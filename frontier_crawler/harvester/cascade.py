"""Fetch one URL: plain HTTP first (JSON, PDF, HTML); when the server refuses it, the same request again with a browser's
TLS fingerprint (curl_cffi); then a browser when the page needs one.

Input contract: result = await cascade.fetch(url, timeout=None). Network and parse failures come back
in the result; result.timed_out says the page timed out with nothing served. Only BlockedAddress is
raised, for a URL the engine must never request.
entries, read, failed = await cascade.sitemap_entries(origin): the (url, lastmod) pairs a site's sitemaps list. Never raises. Fetched bytes are read in worker processes (readers.py);
close the cascade (async with) to stop them.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import ssl
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit

import certifi
import httpx

from frontier_crawler.harvester import readers
from frontier_crawler.harvester.courtesy import crawl_delay, sit_out
from frontier_crawler.harvester.readers import Links, ReaderFailed, ReaderPool, decode, pdf_text, read_html
from frontier_crawler.harvester.throttle import HostThrottle

USER_AGENT = "Mozilla/5.0 (compatible; frontier-crawler/1.0; +https://github.com/howellcolston-hash/frontier-crawler)"
# Sent by both plain tiers, as a browser sends them: some firewalls refuse a request without them. The User-Agent stays our own.
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
IMPERSONATE = "chrome"  # PROVISIONAL: the browser whose TLS and HTTP/2 fingerprint curl_cffi presents (its newest Chrome)
MAX_REDIRECTS = 20      # as httpx's default, so both plain tiers follow the same chains
# PROVISIONAL. Browser-tier requests that carry nothing a page's text or links need (Scrapling's disable_resources, less stylesheets,
# which decide what is hidden, and websockets, which some pages load their data over).
SKIPPED_RESOURCES = frozenset({"font", "image", "media", "beacon", "object", "imageset", "texttrack", "csp_report"})
# PROVISIONAL. Hosts (and their subdomains) that only report on the visitor; the browser tier does not load them.
TRACKER_DOMAINS = frozenset({
    "google-analytics.com", "googletagmanager.com", "doubleclick.net", "googlesyndication.com", "googleadservices.com",
    "facebook.net", "segment.io", "segment.com", "hotjar.com", "clarity.ms", "newrelic.com", "nr-data.net",
    "sentry.io", "datadoghq.com", "intercom.io", "scorecardresearch.com", "quantserve.com", "adnxs.com", "criteo.com",
})
# PROVISIONAL. A larger file is recorded (URL, size, type) and not downloaded, so it can be fetched on purpose later.
MAX_BYTES = 50_000_000
# PROVISIONAL. A page built by JavaScript arrives nearly empty. Under MIN_WORDS of text and
# MIN_LINKS links, the HTML is taken to be such a shell and the browser tier renders it.
MIN_WORDS = 150
MIN_LINKS = 10
# A server that answers 403 or 503 to a plain request sometimes serves a browser.
_BROWSER_RETRY_STATUS = (403, 503)
# Request headers that carry a visitor's credentials. Never copied into a capture or a client spec.
CREDENTIAL_HEADERS = ("cookie", "authorization", "x-api-key", "x-csrf-token", "x-xsrf-token")
PDF_SECONDS = 60.0  # PROVISIONAL: text reading stops here; the stored PDF is complete either way
# PROVISIONAL. A page attempt still running after this long means the shared browser has stopped
# answering; it is discarded. Above the sum of a page's own Playwright waits (the load, NETWORK_IDLE_SECONDS and
# inner_text's 30 s); a longer per-call timeout adds two loads' worth.
BROWSER_HUNG_SECONDS = 120.0
TIMEOUT_SECONDS = 30.0  # PROVISIONAL: HTTP read and browser page-load timeout, unless a call asks for another
CONNECT_SECONDS = 10.0  # PROVISIONAL: a host that will not accept a connection this fast is dead or dropping us
NETWORK_IDLE_SECONDS = 6.0  # PROVISIONAL: after the page has loaded, how long the browser waits for its requests to settle
MAX_SITEMAP_FILES = 10  # PROVISIONAL: sitemap files fetched per site per read, those robots.txt names and those their indexes list together


@dataclass(frozen=True)
class Target:
    """One URL to fetch and where it came from. The crawl fields are stored on its fetch row."""

    url: str
    entity: str = ""
    seed_id: str = ""
    line_id: str = ""
    hops: int = 0
    parent_fetch_id: Optional[str] = None
    fresh: bool = False   # read as new even if unchanged since an earlier fetch: a seed's starting URL fetched as one the first time


@dataclass
class ScrapeResult:
    """What one fetch returned. raw_payload is the body as served (the rendered HTML for the browser tier)."""

    url: str
    http_status: int
    raw_payload: bytes
    markdown_content: Optional[str]
    word_count: int
    tier_used: str  # API | DOM_TRAFILATURA | HTTP_IMPERSONATE (HTML served after a refusal) | PDF | HEADLESS_PLAYWRIGHT
    error_message: Optional[str]
    content_type: str = ""
    too_large_bytes: Optional[int] = None
    timed_out: bool = False  # nothing was served because the request or the page load timed out
    throttle_wait: float = 0.0  # seconds this fetch waited for its host's turn
    links: Optional[Links] = None  # (links, unparseable hrefs) of an HTML page, as its reader found them
    title: Optional[str] = None  # an HTML page's <title>, a PDF's metadata title
    cool_host: bool = False  # the server asked for a pause too long to wait out (courtesy.sit_out): leave its host for the run
    api_responses: int = 0  # browser tier: JSON replies to the page's own background requests (a backend a derived client could call)
    shadow: bool = False  # browser tier: raw_payload includes the page's open shadow roots, which inner_text misses
    etag: Optional[str] = None  # the response's ETag and Last-Modified headers, as served (plain tiers only)
    last_modified: Optional[str] = None


@contextlib.contextmanager
def timed(into: Dict[str, float], key: str) -> Iterator[None]:
    """Add the seconds the block took to into[key]. Measuring must never break what it measures, so its own faults are dropped."""
    start = time.monotonic()
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            into[key] = into.get(key, 0.0) + time.monotonic() - start


class BlockedAddress(Exception):
    """The URL points at this machine or a private network."""


def check_public(url: str) -> None:
    """Refuse localhost and private, loopback and link-local IP addresses.

    Such a link can point at the machine's own services, or at the address where a cloud
    machine serves its credentials (169.254.169.254).
    shortcut: a public name that resolves to a private address is not caught; resolve and check if that is seen.
    """
    host = (urlsplit(url).hostname or "").lower()
    if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
        raise BlockedAddress(url)
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if not address.is_global:
        raise BlockedAddress(url)


def is_tracker(host: str) -> bool:
    """host is in TRACKER_DOMAINS, or a subdomain of one (Scrapling's suffix walk)."""
    parts = host.lower().split(".")
    return any(".".join(parts[i:]) in TRACKER_DOMAINS for i in range(len(parts) - 1))


def word_count(text: str) -> int:
    return len(text.split()) if text else 0


async def read_capped(response: httpx.Response) -> Tuple[Optional[bytes], int]:
    """(body, size) of a streamed response, or (None, size so far or declared) when it is over MAX_BYTES."""
    declared = response.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > MAX_BYTES:
        return None, int(declared)
    chunks = []
    size = 0
    async for chunk in response.aiter_bytes():
        size += len(chunk)
        if size > MAX_BYTES:
            return None, size
        chunks.append(chunk)
    return b"".join(chunks), size


async def _guard_request(request: httpx.Request) -> None:
    check_public(str(request.url))  # runs for every redirect hop too


def _ca_issuers_url(cert) -> Optional[str]:
    from cryptography import x509
    from cryptography.x509.oid import AuthorityInformationAccessOID, ExtensionOID
    try:
        access = cert.extensions.get_extension_for_oid(ExtensionOID.AUTHORITY_INFORMATION_ACCESS).value
    except x509.ExtensionNotFound:
        return None
    for item in access:
        if item.access_method == AuthorityInformationAccessOID.CA_ISSUERS:
            return item.access_location.value
    return None


def context_with_issuers(host: str, port: int = 443) -> Optional[ssl.SSLContext]:
    """Complete a server's certificate chain the way browsers do, by following the
    "CA issuers" links in its certificates, and return a context that still verifies fully.

    Many government and port sites send their certificate without the intermediate that
    links it to a trusted root. The downloaded intermediates are added only as
    chain material: partial-chain trust is switched off, so the chain must still end at a
    root in the standard bundle. Returns None when there is nothing to follow.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding

    with warnings.catch_warnings():  # one bundled root has a non-positive serial number
        warnings.simplefilter("ignore")
        roots = {c.subject for c in x509.load_pem_x509_certificates(Path(certifi.where()).read_bytes())}
    cert = x509.load_pem_x509_certificate(ssl.get_server_certificate((host, port), timeout=10).encode())
    issuers = []
    for _ in range(3):
        url = _ca_issuers_url(cert)
        # Stop once a trusted root issued this one. Root links are often .p7c bundles (Sectigo), which this doesn't read.
        if not url or cert.issuer == cert.subject or cert.issuer in roots:
            break
        check_public(url)
        body = httpx.get(url, timeout=10, follow_redirects=True, headers={"User-Agent": USER_AGENT}).content
        try:
            cert = x509.load_der_x509_certificate(body)
        except ValueError:
            try:
                cert = x509.load_pem_x509_certificate(body)
            except ValueError:
                break
        issuers.append(cert)
    if not issuers:
        return None
    context = ssl.create_default_context(cafile=certifi.where())
    context.verify_flags &= ~getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    context.load_verify_locations(cadata="".join(c.public_bytes(Encoding.PEM).decode() for c in issuers))
    return context


class _ChainCompleter(httpx.AsyncBaseTransport):
    """On "unable to get local issuer certificate", complete the chain and retry, still verified."""

    def __init__(self, complete=context_with_issuers, make=lambda context: httpx.AsyncHTTPTransport(verify=context)) -> None:
        self._complete, self._make = complete, make
        self._default = make(True)
        self._by_host: dict = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host in self._by_host:
            return await self._by_host[host].handle_async_request(request)
        try:
            return await self._default.handle_async_request(request)
        except httpx.ConnectError as exc:
            if "unable to get local issuer certificate" not in str(exc):
                raise
            try:
                context = await asyncio.to_thread(self._complete, host, request.url.port or 443)
                why = "no CA-issuers link to follow"
            except Exception as failure:
                context, why = None, f"{type(failure).__name__}: {failure}"
            if context is None:
                raise httpx.ConnectError(f"{exc} (chain completion failed: {why})", request=request) from exc
        self._by_host[host] = self._make(context)
        return await self._by_host[host].handle_async_request(request)

    async def aclose(self) -> None:
        for transport in [self._default, *self._by_host.values()]:
            await transport.aclose()


class HarvesterCascade:
    """Share one instance across a run so connections are reused."""

    def __init__(
        self,
        timeout_s: float = TIMEOUT_SECONDS,
        client: Optional[httpx.AsyncClient] = None,
        throttle: Optional[HostThrottle] = None,
        browser_slots: int = 4,  # PROVISIONAL: pages rendering at once in the shared browser
    ) -> None:
        self._timeout_s = timeout_s
        self._client = client
        self._owned: Optional[httpx.AsyncClient] = None
        self._throttle = throttle
        self._browser_slots = asyncio.Semaphore(browser_slots)
        self._launch_lock = asyncio.Lock()  # two pages must not both launch
        self._chromium: Optional[Tuple[Any, Any]] = None  # (playwright, browser), shared by every page of the run
        # Measurement only, read by the engine at the end of a run: seconds summed over fetch slots, and counts.
        self.timings: Dict[str, float] = dict.fromkeys(("throttle_wait", "http", "impersonate", "extract_html", "pdf_text", "browser"), 0.0)
        self.counts: Dict[str, int] = dict.fromkeys(
            ("connect_timeouts", "read_timeouts", "browser_timeouts", "browser_launches", "browser_pages", "browser_relaunches", "decoded_non_utf8",
             "reader_crashes", "reader_timeouts", "impersonate_tries", "impersonate_served", "impersonate_failures",
             "browser_requests_skipped", "browser_requests_private", "browser_shadow_pages", "browser_api_pages"), 0
        )
        self._curl: Any = None  # curl_cffi's AsyncSession, opened on the first refusal
        self._readers = ReaderPool(self.counts)
        # Module-level functions, as a worker process must import them. Tests swap in readers that crash or hang.
        self.read_html, self.read_pdf = read_html, pdf_text

    async def __aenter__(self) -> "HarvesterCascade":
        return self

    async def __aexit__(self, *exc: object) -> None:
        try:
            if self._chromium is not None:
                await self._discard(self._chromium[1])
            if self._owned is not None:
                await self._owned.aclose()
                self._owned = None
            if self._curl is not None:
                with contextlib.suppress(Exception):
                    await self._curl.close()
                self._curl = None
        finally:
            self._readers.close()

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        if self._owned is None:
            self._owned = httpx.AsyncClient(
                timeout=self._timeout_s,
                follow_redirects=True,
                headers=REQUEST_HEADERS,
                event_hooks={"request": [_guard_request]},
                transport=_ChainCompleter(),
            )
        return self._owned

    async def fetch(self, url: str, timeout: Optional[float] = None, connect: Optional[float] = None) -> ScrapeResult:
        """timeout: seconds for HTTP reads and for the browser's page load; connect: seconds to connect. The cascade's own when None."""
        waited: Dict[str, float] = {}
        limits = (timeout or self._timeout_s, connect or CONNECT_SECONDS)
        result = await self._fetch(url, limits, waited)
        result.throttle_wait = waited.get("throttle_wait", 0.0)
        return result

    async def _fetch(self, url: str, limits: Tuple[float, float], waited: Dict[str, float]) -> ScrapeResult:
        check_public(url)
        timeout = limits[0]
        try:
            result, wants_browser = await self._http(url, httpx.Timeout(timeout, connect=limits[1]), waited)
            if wants_browser and result.http_status in _BROWSER_RETRY_STATUS:
                result, wants_browser = await self._impersonated(url, limits, waited) or (result, wants_browser)
        except httpx.RemoteProtocolError as exc:
            # A server that hangs up on a plain client's TLS handshake or first request: try a browser's fingerprint once.
            tried = await self._impersonated(url, limits, waited)
            if tried is None:
                return _failed(url, "DOM_TRAFILATURA", f"{type(exc).__name__}: {exc}")
            result, wants_browser = tried
        except httpx.HTTPError as exc:
            # A dead host or a timeout. A browser would meet the same wall.
            failed = _failed(url, "DOM_TRAFILATURA", f"{type(exc).__name__}: {exc}")
            if isinstance(exc, httpx.TimeoutException):
                self.counts["connect_timeouts" if isinstance(exc, httpx.ConnectTimeout) else "read_timeouts"] += 1
                failed.timed_out = True
            return failed
        if not wants_browser:
            return result
        with timed(self.timings, "browser"):  # includes the browser's own throttle wait
            rendered = await self._browser(url, result.error_message, result.http_status, timeout, waited)
        if rendered.raw_payload:
            # Links and title: the browser gave the text, unless shadow roots hold text it missed. Without them the page is still stored.
            with contextlib.suppress(Exception), timed(self.timings, "extract_html"):
                text, _, rendered.links, rendered.title = await self._readers.run(readers.HTML_SECONDS, self.read_html, rendered.raw_payload, rendered.content_type, rendered.url)
                if rendered.shadow and word_count(text or "") > rendered.word_count:
                    rendered.markdown_content, rendered.word_count = text, word_count(text or "")
            return rendered
        # The browser got nothing. Keep what the plain request was served.
        result.error_message = rendered.error_message
        result.timed_out = rendered.timed_out and not result.raw_payload
        return result

    async def _http(self, url: str, timeout: httpx.Timeout, waited: Dict[str, float]) -> Tuple[ScrapeResult, bool]:
        """One GET. Returns (what was served, whether the browser tier should try)."""
        if self._throttle is not None:
            with timed(self.timings, "throttle_wait"), timed(waited, "throttle_wait"):
                await self._throttle.wait(url)
        with timed(self.timings, "http"):
            started = time.monotonic()
            async with self._http_client().stream("GET", url, timeout=timeout) as response:
                answered = time.monotonic() - started
                status = response.status_code
                final_url = str(response.url)
                body, size = await read_capped(response)
        if self._throttle is not None:
            self._throttle.record(url, answered, status)
        return await self._read(url, status, final_url, body, size, response.headers, "DOM_TRAFILATURA")

    async def _impersonated(self, url: str, limits: Tuple[float, float], waited: Dict[str, float]) -> Optional[Tuple[ScrapeResult, bool]]:
        """The GET again, through the throttle, with a browser's TLS and HTTP/2 fingerprint (curl_cffi) and our own headers;
        redirects to private addresses are refused by curl. What _read makes of the reply, or None when it failed."""
        self.counts["impersonate_tries"] += 1
        if self._throttle is not None:
            with timed(self.timings, "throttle_wait"), timed(waited, "throttle_wait"):
                await self._throttle.wait(url)
        try:
            with timed(self.timings, "impersonate"):
                started = time.monotonic()
                status, headers, final_url, body = await self._curl_get(url, limits)
                answered = time.monotonic() - started
            check_public(final_url)
        except BlockedAddress:
            raise
        except Exception:  # not installed, unreachable, over MAX_BYTES (curl stops it), or a redirect curl refused
            self.counts["impersonate_failures"] += 1
            return None
        if self._throttle is not None:
            self._throttle.record(url, answered, status)
        self.counts["impersonate_served"] += status not in _BROWSER_RETRY_STATUS
        return await self._read(url, status, final_url, body, len(body), headers, "HTTP_IMPERSONATE")

    async def _curl_get(self, url: str, limits: Tuple[float, float]) -> Tuple[int, Dict[str, str], str, bytes]:
        """(status, headers with lower-case names, final URL, body). Tests replace this."""
        from curl_cffi import CurlOpt
        from curl_cffi.requests import AsyncSession

        if self._curl is None:
            # "safe": curl refuses a redirect whose address resolves to a private network, checked after DNS.
            self._curl = AsyncSession(impersonate=IMPERSONATE, default_headers=False, headers=REQUEST_HEADERS, allow_redirects="safe",
                                      max_redirects=MAX_REDIRECTS, verify=certifi.where(),
                                      curl_options={CurlOpt.MAXFILESIZE_LARGE: MAX_BYTES})
        reply = await self._curl.get(url, timeout=(limits[1], limits[0]))
        return reply.status_code, {k.lower(): v for k, v in reply.headers.items()}, str(reply.url), reply.content

    async def _read(self, url: str, status: int, final_url: str, body: Optional[bytes], size: int,
                    headers: Mapping[str, str], html_tier: str) -> Tuple[ScrapeResult, bool]:
        """Read what a plain tier was served (headers: httpx's, or a dict with lower-case names). Returns (the result, whether the browser tier should try)."""
        content_type = headers.get("content-type", "")
        pause = sit_out(self._throttle, url, status, headers.get("retry-after"))
        if body is None or size > MAX_BYTES:
            # Not cut short and kept: half a PDF or archive is unreadable.
            result = _failed(final_url, html_tier, "too large", status)
            result.content_type = content_type
            result.too_large_bytes = size
            return result, False

        def served(text: Optional[str], tier: str, note: Optional[str] = None, links: Optional[Links] = None, title: Optional[str] = None) -> ScrapeResult:
            return ScrapeResult(final_url, status, body, text or None, word_count(text or ""), tier, note, content_type, links=links, title=title,
                                cool_host=bool(pause), etag=headers.get("etag"), last_modified=headers.get("last-modified"))

        def undecoded(encoding: Optional[str]) -> Optional[str]:
            """Count a page read in a legacy encoding; the note when no encoding fit."""
            if encoding not in (None, "utf-8"):
                self.counts["decoded_non_utf8"] += 1
            return None if encoding else "text encoding not detected"

        if "json" in content_type.lower() or body.lstrip()[:1] in (b"{", b"["):
            try:
                text, encoding = await self._readers.run(readers.HTML_SECONDS, decode, body, content_type)
            except ReaderFailed as exc:
                return served(None, "API", str(exc)), False
            return served(text, "API", undecoded(encoding)), False
        if "application/pdf" in content_type.lower() or urlsplit(final_url).path.lower().endswith(".pdf"):
            try:
                with timed(self.timings, "pdf_text"):  # the round trip to a reader process
                    text, note, title = await self._readers.run(PDF_SECONDS + readers.MARGIN_SECONDS, self.read_pdf, body, PDF_SECONDS)
            except Exception as exc:  # encrypted, damaged, not a PDF at all, or the reader crashed or hung
                return served(None, "PDF", f"pdf not read: {exc}"), False
            return served(text, "PDF", note, title=title), False
        try:
            with timed(self.timings, "extract_html"):  # the round trip to a reader process: decoding, text and links
                text, encoding, links, title = await self._readers.run(readers.HTML_SECONDS, self.read_html, body, content_type, final_url)
            note = undecoded(encoding)
        except Exception as exc:  # the reader crashed or hung: the page is kept, without text or links, and not rendered
            return served(None, html_tier, f"html not read: {exc}"), False
        words = word_count(text or "")
        # A feed has links but no <a>: its reader's links count too.
        shell = words < MIN_WORDS and body.lower().count(b"<a ") < MIN_LINKS and len(links[0] if links else ()) < MIN_LINKS
        if ((status == 200 and shell) or status in _BROWSER_RETRY_STATUS) and pause is None:   # a server asking for a pause gets one
            return served(text, html_tier, f"plain request: status {status}, {words} words", links, title), True
        return served(text, html_tier, note, links, title), False

    async def get(self, url: str) -> Tuple[int, bytes]:
        """One plain GET through the throttle and the address guard, for a file the crawl reads itself: no tiers, nothing
        read. The body is b"" when it is over MAX_BYTES."""
        check_public(url)
        if self._throttle is not None:
            await self._throttle.wait(url)
        async with self._http_client().stream("GET", url) as response:
            body, _ = await read_capped(response)
        return response.status_code, body or b""

    async def sitemap_entries(self, origin: str) -> Tuple[List[Tuple[str, str]], int, int]:
        """(url, lastmod) from the sitemaps the robots.txt of origin ("https://host") names, or from /sitemap.xml when it names
        none, following sitemap indexes, and indexes they list (the newest sitemaps they list first); then how many sitemap files were read and how many failed.
        At most MAX_SITEMAP_FILES files are fetched. Only discovery and pacing: the robots.txt Crawl-delay for us becomes the
        host's gap in the throttle (its intervals); its rules are not read."""
        try:
            status, body = await self.get(origin + "/robots.txt")
            files = readers.robots_sitemaps(body) if status == 200 else []
            if self._throttle is not None and status < 500:   # a 4xx means no robots.txt, so no delay; a 5xx says nothing
                delay = crawl_delay(body, USER_AGENT) if status == 200 else None
                host = urlsplit(origin).netloc.lower()
                if delay:
                    self._throttle.intervals[host] = delay
                else:
                    self._throttle.intervals.pop(host, None)
        except Exception:
            files = []
        entries: List[Tuple[str, str]] = []
        counts = [0, 0]

        async def read(url: str) -> Tuple[str, List[Tuple[str, str]]]:
            try:
                status, body = await self.get(url)
                if status != 200 or not body:
                    raise ValueError(f"status {status}")
                found = await self._readers.run(readers.HTML_SECONDS, readers.read_sitemap, body)
            except Exception:  # unreachable, refused, not a sitemap, or its reader crashed or hung
                counts[1] += 1
                return "", []
            counts[0] += 1
            return found

        children: List[Tuple[str, str]] = []
        named = (files or [origin + "/sitemap.xml"])[:MAX_SITEMAP_FILES]
        seen = set(named)
        for url in named:
            kind, found = await read(url)
            (children if kind == "index" else entries).extend(found)
        children = readers.newest_first(children)
        while children and sum(counts) < MAX_SITEMAP_FILES:   # indexes inside indexes too, within the same file budget
            url, _ = children.pop(0)
            if url in seen:
                continue
            seen.add(url)
            kind, found = await read(url)
            if kind == "index":
                children = readers.newest_first(children + found)
            else:
                entries.extend(found)
        return entries, counts[0], counts[1]

    async def _browser(
        self, url: str, prior_note: Optional[str], prior_status: int, timeout: Optional[float] = None, waited: Optional[Dict[str, float]] = None,
    ) -> ScrapeResult:
        timeout = timeout or self._timeout_s
        waited = {} if waited is None else waited
        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeout
            from playwright.async_api import async_playwright
        except ImportError as exc:
            return _failed(url, "HEADLESS_PLAYWRIGHT", f"playwright is not installed: {exc}", prior_status)
        self.counts["browser_pages"] += 1
        for _ in range(2):  # a second try only after the browser itself was found dead
            if self._throttle is not None:
                with timed(waited, "throttle_wait"):
                    await self._throttle.wait(url)
            browser = None
            async with self._browser_slots:
                try:
                    browser = await self._shared_browser(async_playwright)
                    hung = BROWSER_HUNG_SECONDS + 2 * max(0.0, timeout - self._timeout_s)
                    return await asyncio.wait_for(self._render(browser, url, prior_note, prior_status, timeout), hung)
                except Exception as exc:
                    failed = _failed(url, "HEADLESS_PLAYWRIGHT", f"{prior_note or ''}; browser: {type(exc).__name__}: {exc}".lstrip("; "), prior_status)
                    if isinstance(exc, PlaywrightTimeout):
                        self.counts["browser_timeouts"] += 1
                        failed.timed_out = True
                    # Navigation errors and page timeouts leave the browser fine. A failed launch, a lost
                    # connection to it or a hang (asyncio's TimeoutError, not Playwright's) do not.
                    if browser is not None and browser.is_connected() and "Connection closed" not in str(exc) and not isinstance(exc, TimeoutError):
                        return failed
                    if browser is not None:
                        await self._discard(browser)
        return failed

    async def _render(self, browser: Any, url: str, prior_note: Optional[str], prior_status: int, timeout: float) -> ScrapeResult:
        """One page in a fresh context, so no cookies or storage carry from one site to the next."""
        from playwright.async_api import Error as PlaywrightError

        timeout_ms = int(timeout * 1000)
        context = await browser.new_context(user_agent=USER_AGENT)
        api_calls: List[str] = []
        try:
            await context.route("**/*", self._screen)
            page = await context.new_page()
            page.on("response", lambda reply: _note_api(reply, api_calls))
            response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # Give scripts a moment to fill the page in. Some pages never stop loading: take what is there.
            with contextlib.suppress(PlaywrightError):
                await page.wait_for_load_state("networkidle", timeout=int(NETWORK_IDLE_SECONDS * 1000))
            check_public(page.url or url)  # a script may have sent the page somewhere else
            shadow = None
            with contextlib.suppress(PlaywrightError):
                shadow = await page.evaluate(SHADOW_SNAPSHOT_JS)
            html = shadow or await page.content()
            # The bytes are kept as rendered; hidden elements are then removed from the live page so its text leaves them out.
            with contextlib.suppress(PlaywrightError):
                await page.evaluate(DROP_HIDDEN_JS)
            text = readers.clean_text(await page.inner_text("body")) or ""
            status = response.status if response is not None else prior_status
            self.counts["browser_shadow_pages"] += bool(shadow)
            self.counts["browser_api_pages"] += bool(api_calls)
            return ScrapeResult(
                page.url or url, status, html.encode("utf-8"), text, word_count(text),
                "HEADLESS_PLAYWRIGHT", prior_note, "text/html; rendered", api_responses=len(api_calls), shadow=bool(shadow),
            )
        finally:
            # An open context keeps its pages' memory. Bounded: a dead or hung browser may never answer.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(context.close(), self._timeout_s)

    async def _screen(self, route: Any) -> None:
        """Every request a rendered page makes: none to a private address (the page itself was checked; what it loads was
        not), and none for images, fonts, media or trackers."""
        request = route.request
        try:
            check_public(request.url)
        except BlockedAddress:
            self.counts["browser_requests_private"] += 1
            await route.abort()
            return
        if request.resource_type in SKIPPED_RESOURCES or is_tracker(urlsplit(request.url).hostname or ""):
            self.counts["browser_requests_skipped"] += 1
            await route.abort()
            return
        await route.continue_()

    async def _shared_browser(self, async_playwright: Any) -> Any:
        """The run's one Chromium: launched on first use, and again once the last one died."""
        if self._chromium is not None and not self._chromium[1].is_connected():
            await self._discard(self._chromium[1])
        async with self._launch_lock:
            if self._chromium is None:
                playwright = await async_playwright().start()
                if self.counts["browser_launches"]:
                    self.counts["browser_relaunches"] += 1
                self.counts["browser_launches"] += 1
                try:
                    browser = await playwright.chromium.launch(headless=True)
                except BaseException:
                    await playwright.stop()  # a failed launch must not leave Playwright running
                    raise
                self._chromium = (playwright, browser)
            return self._chromium[1]

    async def _discard(self, browser: Any) -> None:
        """Close the shared browser and its Playwright, unless another page already replaced it."""
        async with self._launch_lock:
            if self._chromium is None or self._chromium[1] is not browser:
                return
            playwright, self._chromium = self._chromium[0], None
            # A leaked Chromium uses up a small machine's memory. Stopping Playwright ends its Chromium even if close() hung.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(browser.close(), self._timeout_s)
            with contextlib.suppress(Exception):
                await playwright.stop()


def _note_api(reply: Any, into: List[str]) -> None:
    """Note a JSON reply to a page's own background request (Scrapling's network recording, counted rather than kept)."""
    with contextlib.suppress(Exception):
        if reply.request.resource_type in ("xhr", "fetch") and "json" in (reply.headers.get("content-type") or "").lower():
            into.append(reply.url)


# The page's HTML with each open shadow root copied in as a <shadow-root> element, or null when it has none. From Scrapling
# (scrapling/engines/toolbelt/convertor.py, BSD-3-Clause, Copyright (c) 2024 Karim Shoair; licence in licenses/Scrapling-BSD-3-Clause.txt).
SHADOW_SNAPSHOT_JS = r"""
() => {
    if (!Array.from(document.querySelectorAll('*')).some(node => node.shadowRoot)) return null;
    const documentCopy = document.implementation.createHTMLDocument('');
    let roots = 0;
    function copy(node, pierce = true) {
        if (node.nodeType !== Node.ELEMENT_NODE) return node.cloneNode(true);
        const name = node.prefix ? `${node.prefix}:${node.localName}` : node.localName;
        const target = documentCopy.createElementNS(node.namespaceURI, name);
        for (const attr of node.attributes) target.setAttributeNS(attr.namespaceURI, attr.name, attr.value);
        if (pierce && node instanceof HTMLSlotElement && node.getRootNode() instanceof ShadowRoot) {
            const assigned = node.assignedNodes();
            for (const child of assigned.length ? assigned : node.childNodes) target.append(copy(child));
            return target;
        }
        if (pierce && node.shadowRoot) {
            roots++;
            const shadow = documentCopy.createElement('shadow-root');
            for (const child of node.shadowRoot.childNodes) shadow.append(copy(child));
            target.append(shadow);
        } else {
            const template = node instanceof HTMLTemplateElement;
            const source = template ? node.content : node;
            const destination = template ? target.content : target;
            for (const child of source.childNodes) destination.append(copy(child, pierce && !template));
        }
        return target;
    }
    const root = copy(document.documentElement);
    if (!roots) return null;
    document.adoptNode(root);
    const doctype = document.doctype ? new XMLSerializer().serializeToString(document.doctype) : '';
    return doctype + root.outerHTML;
}
"""
# Removes, from the live page, elements a reader cannot see but inner_text keeps: aria-hidden, transparent, zero-size
# text or a zero box that clips its content. The same rule as readers.is_hidden, read from computed style.
DROP_HIDDEN_JS = r"""
() => {
    let dropped = 0;
    for (const el of Array.from(document.querySelectorAll('body *'))) {
        if (!el.isConnected) continue;
        const style = getComputedStyle(el);
        const clipped = (style.height === '0px' || style.width === '0px') && style.overflow === 'hidden';
        if (el.getAttribute('aria-hidden') === 'true' || style.opacity === '0' || parseFloat(style.fontSize) === 0 || clipped) {
            el.remove();
            dropped++;
        }
    }
    return dropped;
}
"""


def _failed(url: str, tier: str, message: str, status: int = 0) -> ScrapeResult:
    return ScrapeResult(url, status, b"", None, 0, tier, message)
