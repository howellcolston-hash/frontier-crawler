"""Fetch one URL: plain HTTP first (JSON, PDF, HTML), then a browser when the page needs one.

Input contract: result = await cascade.fetch(url). Network and parse failures come back in
the result. Only BlockedAddress is raised, for a URL the engine must never request.
"""

from __future__ import annotations

import asyncio
import ipaddress
import ssl
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlsplit

import certifi
import httpx
import trafilatura

from frontier_crawler.harvester.throttle import HostThrottle

USER_AGENT = "Mozilla/5.0 (compatible; frontier-crawler/1.0; +https://github.com/howellcolston-hash/frontier-crawler)"
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


@dataclass(frozen=True)
class Target:
    """One URL to fetch and where it came from. The crawl fields are stored on its fetch row."""

    url: str
    entity: str = ""
    seed_id: str = ""
    line_id: str = ""
    hops: int = 0
    parent_fetch_id: Optional[str] = None


@dataclass
class ScrapeResult:
    """What one fetch returned. raw_payload is the body as served (the rendered HTML for the browser tier)."""

    url: str
    http_status: int
    raw_payload: bytes
    markdown_content: Optional[str]
    word_count: int
    tier_used: str  # API | DOM_TRAFILATURA | PDF | HEADLESS_PLAYWRIGHT
    error_message: Optional[str]
    content_type: str = ""
    too_large_bytes: Optional[int] = None


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


def word_count(text: str) -> int:
    return len(text.split()) if text else 0


def _pdf_text(body: bytes) -> Tuple[str, Optional[str]]:
    """(text of every page, a note when PDF_SECONDS ran out first). Scanned pages have no text."""
    import pypdfium2

    pdf = pypdfium2.PdfDocument(body)
    try:
        parts = []
        deadline = time.monotonic() + PDF_SECONDS
        for index in range(len(pdf)):
            if time.monotonic() > deadline:
                return "\n".join(parts), f"text read for the first {index} of {len(pdf)} pages"
            page = pdf[index]
            textpage = page.get_textpage()
            parts.append(textpage.get_text_bounded())
            textpage.close()
            page.close()
        return "\n".join(parts), None
    finally:
        pdf.close()


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
        timeout_s: float = 15.0,
        client: Optional[httpx.AsyncClient] = None,
        throttle: Optional[HostThrottle] = None,
    ) -> None:
        self._timeout_s = timeout_s
        self._client = client
        self._owned: Optional[httpx.AsyncClient] = None
        self._throttle = throttle
        self._pdf_lock = asyncio.Lock()  # the PDF library must not run in two threads at once

    async def __aenter__(self) -> "HarvesterCascade":
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._owned is not None:
            await self._owned.aclose()
            self._owned = None

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        if self._owned is None:
            self._owned = httpx.AsyncClient(
                timeout=self._timeout_s,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
                event_hooks={"request": [_guard_request]},
                transport=_ChainCompleter(),
            )
        return self._owned

    async def fetch(self, url: str) -> ScrapeResult:
        check_public(url)
        try:
            result, wants_browser = await self._http(url)
        except httpx.HTTPError as exc:
            # A dead host or a timeout. A browser would meet the same wall.
            return _failed(url, "DOM_TRAFILATURA", f"{type(exc).__name__}: {exc}")
        if not wants_browser:
            return result
        rendered = await self._browser(url, result.error_message, result.http_status)
        if rendered.raw_payload:
            return rendered
        # The browser got nothing. Keep what the plain request was served.
        result.error_message = rendered.error_message
        return result

    async def _http(self, url: str) -> Tuple[ScrapeResult, bool]:
        """One GET. Returns (what was served, whether the browser tier should try)."""
        if self._throttle is not None:
            await self._throttle.wait(url)
        async with self._http_client().stream("GET", url) as response:
            status = response.status_code
            content_type = response.headers.get("content-type", "")
            final_url = str(response.url)
            body, size = await read_capped(response)
        if body is None:
            # Not cut short and kept: half a PDF or archive is unreadable.
            result = _failed(final_url, "DOM_TRAFILATURA", "too large", status)
            result.content_type = content_type
            result.too_large_bytes = size
            return result, False

        def served(text: Optional[str], tier: str, note: Optional[str] = None) -> ScrapeResult:
            return ScrapeResult(final_url, status, body, text or None, word_count(text or ""), tier, note, content_type)

        if "json" in content_type.lower() or body.lstrip()[:1] in (b"{", b"["):
            return served(body.decode("utf-8", errors="replace"), "API"), False
        if "application/pdf" in content_type.lower() or urlsplit(final_url).path.lower().endswith(".pdf"):
            try:
                async with self._pdf_lock:
                    text, note = await asyncio.to_thread(_pdf_text, body)
            except Exception as exc:  # encrypted, damaged, or not a PDF at all
                return served(None, "PDF", f"pdf not read: {exc}"), False
            return served(text, "PDF", note), False
        try:
            html = body.decode("utf-8", errors="replace")
            text = await asyncio.to_thread(
                trafilatura.extract, html, output_format="markdown", include_comments=False, include_tables=True
            )
        except Exception:
            text = None
        words = word_count(text or "")
        shell = words < MIN_WORDS and body.lower().count(b"<a ") < MIN_LINKS
        if (status == 200 and shell) or status in _BROWSER_RETRY_STATUS:
            return served(text, "DOM_TRAFILATURA", f"plain request: status {status}, {words} words"), True
        return served(text, "DOM_TRAFILATURA"), False

    async def _browser(self, url: str, prior_note: Optional[str], prior_status: int) -> ScrapeResult:
        try:
            from playwright.async_api import Error as PlaywrightError
            from playwright.async_api import async_playwright
        except ImportError as exc:
            return _failed(url, "HEADLESS_PLAYWRIGHT", f"playwright is not installed: {exc}", prior_status)
        if self._throttle is not None:
            await self._throttle.wait(url)
        playwright = None
        browser = None
        timeout_ms = int(self._timeout_s * 1000)
        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(headless=True)
            page = await (await browser.new_context(user_agent=USER_AGENT)).new_page()
            try:
                response = await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            except PlaywrightError:
                # Some pages never stop loading. Take what is there once the document itself has loaded.
                response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            check_public(page.url or url)  # a script may have sent the page somewhere else
            text = await page.inner_text("body")
            html = await page.content()
            status = response.status if response is not None else prior_status
            return ScrapeResult(
                page.url or url, status, html.encode("utf-8"), text, word_count(text),
                "HEADLESS_PLAYWRIGHT", prior_note, "text/html; rendered",
            )
        except Exception as exc:
            return _failed(url, "HEADLESS_PLAYWRIGHT", f"{prior_note or ''}; browser: {type(exc).__name__}: {exc}".lstrip("; "), prior_status)
        finally:
            # A leaked Chromium uses up a small machine's memory.
            if browser is not None:
                await browser.close()
            if playwright is not None:
                await playwright.stop()


def _failed(url: str, tier: str, message: str, status: int = 0) -> ScrapeResult:
    return ScrapeResult(url, status, b"", None, 0, tier, message)
