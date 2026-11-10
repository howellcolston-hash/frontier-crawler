"""Derived API clients: call a site's own backend directly, on a schedule, with no browser.

A client is a JSON spec (the README gives the format), written from a browser session recorded
with the capture command; this module runs specs and says when one
has stopped working. Replies are stored through the engine like any other fetch.

A spec is broken when its first page is not a 200, has no list at records_path, has fewer than
min_records records, or its records have lost a key the fingerprint names. A broken client is
reported in the run summary and must be derived again: nothing here repairs it.

Input contract:
    specs = load_specs(directory)
    results = await run_derived(engine, specs, client)       # stores pages, returns {id: result}
    report = await check(spec, client)                       # one or two live calls, stores nothing
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

from frontier_crawler.harvester.cascade import CREDENTIAL_HEADERS, USER_AGENT, ScrapeResult, Target, check_public, read_capped
from frontier_crawler.harvester.throttle import HostThrottle



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

    @property
    def pages(self) -> int:
        return int(self.paginate["max_pages"]) if self.paginate else 1


def load_spec(path: Path) -> Spec:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        request = data["request"]
        if data["id"] != Path(path).stem:
            raise ValueError("id must match the file name")
        if request.get("method", "GET") not in ("GET", "POST"):
            raise ValueError("method must be GET or POST")
        check_public(request["url"])
        bad = [name for name in request.get("headers") or {} if name.lower() in CREDENTIAL_HEADERS]
        if bad:
            raise ValueError(f"headers must not hold credentials: {bad}")
        paginate = data.get("paginate")
        if paginate and (paginate["in"] not in ("params", "json") or int(paginate["max_pages"]) < 1):
            raise ValueError("paginate.in must be params or json, and max_pages at least 1")
        fingerprint = data.get("fingerprint") or {}
        return Spec(
            id=data["id"], entity=data["entity"], seed_id=data.get("seed_id") or "", request=request, paginate=paginate,
            records_path=data.get("records_path") or "", keys=tuple(fingerprint.get("keys") or ()),
            min_records=int(fingerprint.get("min_records", 1)),
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
    """(method, url with query, headers, json body, key). The key is unique per page and is what the engine stores as the URL."""
    request = copy.deepcopy(spec.request)
    params = dict(request.get("params") or {})
    body = request.get("json")
    if spec.paginate:
        value = spec.paginate["start"] + page * spec.paginate.get("step", 1)
        if spec.paginate["in"] == "params":
            params[spec.paginate["field"]] = value
        else:
            body = body if isinstance(body, dict) else {}
            _set_path(body, spec.paginate["field"], value)
    url = request["url"] + ("?" + urlencode(params, doseq=True) if params else "")
    key = url
    if body is not None:
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
        self._requests: Dict[str, Tuple[str, str, Dict[str, str], Optional[Any]]] = {}
        self.last: Dict[str, Tuple[int, bytes, str]] = {}  # key -> (status, body, why there was no reply)

    def prepare(self, spec: Spec, page: int) -> str:
        method, url, headers, body, key = page_request(spec, page)
        self._requests[key] = (method, url, headers, body)
        return key

    async def fetch(self, key: str) -> ScrapeResult:
        method, url, headers, body = self._requests[key]
        self.last[key] = (0, b"", "an unexpected error; its fetch row has the detail")  # stands if anything below raises
        check_public(url)
        if self._throttle is not None:
            await self._throttle.wait(url)
        try:
            request = self._client.build_request(method, url, headers={"User-Agent": USER_AGENT, **headers}, json=body)
            response = await self._client.send(request, stream=True, follow_redirects=True)
            try:
                content, size = await read_capped(response)
            finally:
                await response.aclose()
        except httpx.HTTPError as exc:
            self.last[key] = (0, b"", f"{type(exc).__name__}: {exc}")
            return ScrapeResult(key, 0, b"", None, 0, "API", self.last[key][2])
        content_type = response.headers.get("content-type", "")
        if content is None:
            self.last[key] = (response.status_code, b"", "")
            return ScrapeResult(key, response.status_code, b"", None, 0, "API", "too large", content_type, too_large_bytes=size)
        self.last[key] = (response.status_code, content, "")
        text = content.decode("utf-8", errors="replace")
        return ScrapeResult(key, response.status_code, content, text, len(text.split()), "API", None, content_type)


async def run_derived(engine: Any, specs: List[Spec], fetcher: DerivedFetcher) -> Dict[str, Dict[str, Any]]:
    """Fetch every page of every spec through the engine. Returns {spec id: {status, pages, records, stopped?}}."""
    results: Dict[str, Dict[str, Any]] = {}
    for spec in specs:
        result = {"status": "ok", "pages": 0, "records": 0}
        for page in range(spec.pages):
            key = fetcher.prepare(spec, page)
            await engine.run([Target(key, spec.entity, spec.seed_id, f"derived:{spec.id}", page)])
            status, body, error = fetcher.last[key]
            if page == 0:
                result["status"] = health(spec, status, body, error)
                if result["status"] != "ok":
                    break
            elif status != 200:
                # The client is not broken, but this run did not get all it asked for.
                result["stopped"] = f"page {page + 1}: " + (f"status {status}" if status else f"no reply ({error})")
                break
            records = records_of(spec, body)
            if not records:
                break  # past the last page
            result["pages"] += 1
            result["records"] += len(records)
        results[spec.id] = result
    return results


async def check(spec: Spec, client: httpx.AsyncClient) -> Dict[str, Any]:
    """Call the first page (and the second, when paginated) for real and describe what came back. Stores nothing."""
    fetcher = DerivedFetcher(client)
    report: Dict[str, Any] = {"id": spec.id}
    first: Optional[List[Any]] = None
    for page in range(min(spec.pages, 2)):
        key = fetcher.prepare(spec, page)
        result = await fetcher.fetch(key)
        status, body, error = fetcher.last[key]
        records = records_of(spec, body)
        entry: Dict[str, Any] = {"request": key, "status": status, "bytes": len(body), "error": result.error_message,
                                 "records": None if records is None else len(records)}
        if page == 0:
            first = records
            entry["health"] = health(spec, status, body, error) if spec.keys else "no fingerprint yet"
            if records:
                entry["first_record"] = json.dumps(records[0], ensure_ascii=False)[:1500]
                # Paste this into the spec once the reply is confirmed to be the real data.
                report["suggested_fingerprint"] = {"keys": shared_keys(records), "min_records": 1}
            elif records is None:
                entry["reply_start"] = body[:600].decode("utf-8", errors="replace")
        else:
            entry["differs_from_first_page"] = bool(records) and records != first
        report[f"page_{page + 1}"] = entry
    return report
