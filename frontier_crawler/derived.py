"""Derived API clients: call a site's own backend directly, on a schedule, with no browser.

A client is a JSON spec (the README gives the format), written from a browser session recorded
with the capture command; this module runs specs and says when one
has stopped working. Replies are stored through the engine like any other fetch.

A spec's reply is "json" (the default: a list of records at records_path) or "html" (a list page with no JSON backend,
such as one that pages through form posts). A JSON spec is broken when its first page is not a 200, has no list at
records_path, has fewer than min_records records, or its records have lost a key the fingerprint names. An HTML spec is
broken when its first page is not a 200, lacks a string of the fingerprint's contains, or is under its min_bytes. A
broken client is reported in the run summary and must be derived again: nothing here repairs it.

An HTML spec's follow regex picks the links on its pages (absolute URLs) that are queued for the crawl as ordinary links
of its seed (Frontier.add_followed), so the crawl fetches the detail pages. Requests go through the crawl's throttle,
Crawl-delays included; min_interval_s widens the gap for the spec's host, and a 429 or a 503 with Retry-After is sat
out (harvester/courtesy.py): waited and asked again once when the wait is short, else the host is left for the run.

Input contract:
    specs = load_specs(directory)
    results = await run_derived(engine, specs, fetcher, frontier)   # stores pages, queues followed links, returns {id: result}
    report = await check(spec, client)                       # one or two live calls, stores nothing
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit

import httpx

from frontier_crawler.harvester.cascade import CREDENTIAL_HEADERS, USER_AGENT, ScrapeResult, Target, check_public, read_capped, word_count
from frontier_crawler.harvester.courtesy import sit_out
from frontier_crawler.harvester.readers import decode, extract_links, read_html
from frontier_crawler.harvester.throttle import HostThrottle

FOLLOW_LINKS_PER_PAGE = 5_000   # PROVISIONAL: links read from one list page for follow (the crawl's own cap is lower)
SAT_OUT = "the host asked for a pause"
COOLED = "the host is left alone for the rest of the run"


@dataclass(frozen=True)
class Spec:
    id: str
    entity: str
    seed_id: str
    request: Dict[str, Any]
    paginate: Optional[Dict[str, Any]]
    records_path: str
    keys: Tuple[str, ...]
    min_records: int
    reply: str = "json"
    contains: Tuple[str, ...] = ()   # html: strings a healthy page holds, all of them
    min_bytes: int = 0               # html: a healthy first page is at least this long
    follow: str = ""                 # html: regex for the links to queue for the crawl
    min_interval_s: float = 0.0      # seconds between requests to its host, when longer than the run's gap

    @property
    def pages(self) -> int:
        return int(self.paginate["max_pages"]) if self.paginate else 1


def load_spec(path: Path) -> Spec:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        request = data["request"]
        if data["id"] != Path(path).stem:
            raise ValueError("id must match the file name")
        method = request.get("method", "GET")
        if method not in ("GET", "POST"):
            raise ValueError("method must be GET or POST")
        check_public(request["url"])
        bad = [name for name in request.get("headers") or {} if name.lower() in CREDENTIAL_HEADERS]
        if bad:
            raise ValueError(f"headers must not hold credentials: {bad}")
        paginate = data.get("paginate")
        if "form" in request and (method != "POST" or request.get("json") is not None or not isinstance(request["form"], dict)
                                  or (paginate or {}).get("in") == "json"):
            raise ValueError("form is a dict, sent only by a POST, and not with json")
        if paginate and (paginate["in"] not in ("params", "json", "form") or int(paginate["max_pages"]) < 1):
            raise ValueError("paginate.in must be params, json or form, and max_pages at least 1")
        if paginate and paginate["in"] == "form" and method != "POST":
            raise ValueError("paginate.in form needs a POST")
        reply = data.get("reply", "json")
        fingerprint = data.get("fingerprint") or {}
        contains = tuple(fingerprint.get("contains") or ())
        follow = data.get("follow") or ""
        if reply not in ("json", "html"):
            raise ValueError("reply must be json or html")
        if reply == "html" and (not contains or not all(isinstance(text, str) and text for text in contains)):
            raise ValueError("an html reply needs fingerprint.contains: strings a healthy page holds")
        if follow and reply != "html":
            raise ValueError("follow reads links from html replies only")
        re.compile(follow)
        min_interval_s = float(data.get("min_interval_s") or 0)
        if not math.isfinite(min_interval_s) or min_interval_s < 0:
            raise ValueError("min_interval_s must be a number >= 0")
        return Spec(
            id=data["id"], entity=data["entity"], seed_id=data.get("seed_id") or "", request=request, paginate=paginate,
            records_path=data.get("records_path") or "", keys=tuple(fingerprint.get("keys") or ()),
            min_records=int(fingerprint.get("min_records", 1)), reply=reply, contains=contains,
            min_bytes=int(fingerprint.get("min_bytes") or 0), follow=follow, min_interval_s=min_interval_s,
        )
    except Exception as exc:
        raise ValueError(f"{path}: {type(exc).__name__}: {exc}") from exc


def load_specs(directory: Path) -> List[Spec]:
    return [load_spec(path) for path in sorted(Path(directory).glob("*.json"))]


def _set_path(target: Dict[str, Any], dotted: str, value: Any) -> None:
    *parents, last = dotted.split(".")
    for name in parents:
        target = target.setdefault(name, {})
    target[last] = value


def page_request(spec: Spec, page: int) -> Tuple[str, str, Dict[str, str], Optional[Any], str]:
    """(method, url with query, headers, body, key). The body is the JSON to send, or for a form a str, already
    form-encoded. The key is unique per page and is what the engine stores as the URL."""
    request = copy.deepcopy(spec.request)
    params = dict(request.get("params") or {})
    body = request.get("json")
    form = request.get("form")
    if spec.paginate:
        value = spec.paginate["start"] + page * spec.paginate.get("step", 1)
        if spec.paginate["in"] == "params":
            params[spec.paginate["field"]] = value
        elif spec.paginate["in"] == "form":
            form = {**(form or {}), spec.paginate["field"]: value}
        else:
            body = body if isinstance(body, dict) else {}
            _set_path(body, spec.paginate["field"], value)
    url = request["url"] + ("?" + urlencode(params, doseq=True) if params else "")
    key = url
    if form is not None:
        body = urlencode(form, doseq=True)
        key += "#form-" + hashlib.sha256(body.encode()).hexdigest()[:12]
    elif body is not None:
        # Two POSTs to one address differ only in their body, so the body is part of the key.
        key += "#body-" + hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:12]
    return request.get("method", "GET"), url, dict(request.get("headers") or {}), body, key


def records_of(spec: Spec, body: bytes) -> Optional[List[Any]]:
    """The list of records in a reply, or None when the reply does not have one where the spec says."""
    try:
        node = json.loads(body)
    except ValueError:
        return None
    for name in filter(None, spec.records_path.split(".")):
        if not isinstance(node, dict) or name not in node:
            return None
        node = node[name]
    return node if isinstance(node, list) else None


def missing_markers(spec: Spec, body: bytes) -> List[str]:
    """The strings of the fingerprint's contains that an html reply lacks."""
    text = decode(body, "")[0]
    return [marker for marker in spec.contains if marker not in text]


def shared_keys(records: List[Any], sample: int = 20) -> List[str]:
    """Keys present in every one of the first records: the stable ones, fit for a fingerprint."""
    dicts = [record for record in records[:sample] if isinstance(record, dict)]
    if not dicts:
        return []
    return sorted(set(dicts[0]).intersection(*(set(d) for d in dicts[1:])))


def health(spec: Spec, status: int, body: bytes, error: str = "") -> str:
    """"ok", or "broken: <why>" for a first page that no longer looks like the fingerprint."""
    if status == 0:
        return f"broken: no reply ({error})"
    if status != 200:
        return f"broken: status {status}"
    if spec.reply == "html":
        missing = missing_markers(spec, body)
        if missing:
            return f"broken: page lacks {missing}"
        return f"broken: {len(body)} bytes, expected at least {spec.min_bytes}" if len(body) < spec.min_bytes else "ok"
    records = records_of(spec, body)
    if records is None:
        return f"broken: no list at records_path {spec.records_path!r}"
    if len(records) < spec.min_records:
        return f"broken: {len(records)} records, expected at least {spec.min_records}"
    seen = {key for record in records if isinstance(record, dict) for key in record}
    missing = sorted(set(spec.keys) - seen)
    return f"broken: records lost keys {missing}" if missing else "ok"


class DerivedFetcher:
    """Sends a spec's requests. The engine calls fetch(key) with a key from prepare()."""

    def __init__(self, client: httpx.AsyncClient, throttle: Optional[HostThrottle] = None) -> None:
        self._client = client
        self._throttle = throttle
        self._requests: Dict[str, Tuple[Spec, str, str, Dict[str, str], Optional[Any]]] = {}
        self.last: Dict[str, Tuple[int, bytes, str]] = {}  # key -> (status, body, why there was no reply, or SAT_OUT)
        self.links: Dict[str, List[str]] = {}  # key -> the links on an html reply, for a spec that follows them
        self.cooled: set = set()               # hosts left alone for the rest of the run

    def prepare(self, spec: Spec, page: int) -> str:
        method, url, headers, body, key = page_request(spec, page)
        self._requests[key] = (spec, method, url, headers, body)
        if self._throttle is not None and spec.min_interval_s:
            host = urlsplit(url).netloc.lower()
            self._throttle.intervals[host] = max(self._throttle.intervals.get(host, 0.0), spec.min_interval_s)
        return key

    async def fetch(self, key: str) -> ScrapeResult:
        spec, method, url, headers, body = self._requests[key]
        self.last[key] = (0, b"", "an unexpected error; its fetch row has the detail")  # stands if anything below raises
        check_public(url)
        host = urlsplit(url).netloc.lower()
        if host in self.cooled:
            self.last[key] = (0, b"", COOLED)
            return ScrapeResult(key, 0, b"", None, 0, "API", COOLED)
        if self._throttle is not None:
            await self._throttle.wait(url)
        form = isinstance(body, str)   # page_request form-encodes a form
        try:
            request = self._client.build_request(
                method, url, json=None if form else body, content=body if form else None,
                headers={"User-Agent": USER_AGENT, **({"Content-Type": "application/x-www-form-urlencoded"} if form else {}), **headers})
            response = await self._client.send(request, stream=True, follow_redirects=True)
            try:
                content, size = await read_capped(response)
            finally:
                await response.aclose()
        except httpx.HTTPError as exc:
            self.last[key] = (0, b"", f"{type(exc).__name__}: {exc}")
            return ScrapeResult(key, 0, b"", None, 0, "API", self.last[key][2])
        pause = sit_out(self._throttle, url, response.status_code, response.headers.get("retry-after"))
        if pause:
            self.cooled.add(host)
        note = "" if pause is None else COOLED if pause else SAT_OUT
        content_type = response.headers.get("content-type", "")
        if content is None:
            self.last[key] = (response.status_code, b"", note)
            return ScrapeResult(key, response.status_code, b"", None, 0, "API", "too large", content_type, too_large_bytes=size)
        self.last[key] = (response.status_code, content, note)
        if spec.reply == "html":
            final = str(response.url)
            try:
                text, _, links, title = read_html(content, content_type, final)
            except Exception:  # a page no reader can read is still stored
                text, links, title = None, None, None
            if spec.follow:
                self.links[key] = [link for link, _ in extract_links(content, final, FOLLOW_LINKS_PER_PAGE)[0] if re.search(spec.follow, link)]
            return ScrapeResult(key, response.status_code, content, text, word_count(text or ""), "API", None, content_type, links=links, title=title)
        text = content.decode("utf-8", errors="replace")
        return ScrapeResult(key, response.status_code, content, text, len(text.split()), "API", None, content_type)


async def run_derived(
    engine: Any, specs: List[Spec], fetcher: DerivedFetcher, frontier: Any = None,
    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
) -> Dict[str, Dict[str, Any]]:
    """Fetch every page of every spec through the engine. Returns {spec id: {status, pages, records (json) or followed
    (html with follow), stopped?}}. frontier: the crawl's Frontier, which a spec's followed links join; None to queue none."""
    results: Dict[str, Dict[str, Any]] = {}
    for spec in specs:
        result: Dict[str, Any] = {"status": "ok", "pages": 0, **({"records": 0} if spec.reply == "json" else {}),
                                  **({"followed": 0} if spec.follow else {})}
        results[spec.id] = result
        previous = None
        for page in range(spec.pages):
            key = fetcher.prepare(spec, page)
            target = Target(key, spec.entity, spec.seed_id, f"derived:{spec.id}", page)
            await engine.run([target])
            if fetcher.last[key][2] == SAT_OUT:   # the throttle has waited the server's Retry-After: ask once more
                await engine.run([target])
            # Popped, so a spec of thousands of pages does not hold them all.
            status, body, error = fetcher.last.pop(key)
            links = fetcher.links.pop(key, [])
            if error in (COOLED, SAT_OUT):   # not broken: the server asked us to stay away
                result["stopped"] = f"page {page + 1}: " + (f"status {status}; " if status else "") + error
                break
            if page == 0:
                result["status"] = health(spec, status, body, error)
                if result["status"] != "ok":
                    break
            elif status != 200:
                # The client is not broken, but this run did not get all it asked for.
                result["stopped"] = f"page {page + 1}: " + (f"status {status}" if status else f"no reply ({error})")
                break
            if spec.reply == "html":
                if body == previous or missing_markers(spec, body):
                    break  # past the last page: the site served the same page again, or not a list page
                previous = body
            else:
                records = records_of(spec, body)
                if not records:
                    break  # past the last page
                result["records"] += len(records)
            result["pages"] += 1
            if frontier is not None and links:
                result["followed"] += frontier.add_followed(spec.seed_id, links, now())
    return results


async def check(spec: Spec, client: httpx.AsyncClient) -> Dict[str, Any]:
    """Call the first page (and the second, when paginated) for real and describe what came back. Stores nothing and
    queues nothing."""
    fetcher = DerivedFetcher(client)
    report: Dict[str, Any] = {"id": spec.id}
    first: Optional[List[Any]] = None
    first_body = b""
    for page in range(min(spec.pages, 2)):
        key = fetcher.prepare(spec, page)
        result = await fetcher.fetch(key)
        status, body, error = fetcher.last[key]
        records = records_of(spec, body)
        entry: Dict[str, Any] = {"request": key, "status": status, "bytes": len(body), "error": result.error_message,
                                 "records": None if records is None else len(records)}
        if spec.follow:   # how many links follow picks, and a few of them
            entry["follow_matches"] = len(fetcher.links.get(key, []))
            entry["follow_sample"] = fetcher.links.get(key, [])[:5]
        if page == 0:
            first, first_body = records, body
            entry["health"] = health(spec, status, body, error) if spec.keys or spec.contains else "no fingerprint yet"
            if records:
                entry["first_record"] = json.dumps(records[0], ensure_ascii=False)[:1500]
                # Paste this into the spec once the reply is confirmed to be the real data.
                report["suggested_fingerprint"] = {"keys": shared_keys(records), "min_records": 1}
            elif records is None:
                entry["reply_start"] = body[:600].decode("utf-8", errors="replace")
        elif spec.reply == "html":
            entry["differs_from_first_page"] = bool(body) and body != first_body and not missing_markers(spec, body)
        else:
            entry["differs_from_first_page"] = bool(records) and records != first
        report[f"page_{page + 1}"] = entry
    return report
