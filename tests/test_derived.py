"""Checks for derived API clients and the capture helpers. No live network, no browser."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List

import httpx
import pyarrow.parquet as pq
import pytest

from frontier_crawler import derived
from frontier_crawler.capture import is_first_party, is_noise, json_shape, kept_headers
from frontier_crawler.derived import DerivedFetcher, check, health, load_spec, page_request, records_of, run_derived, shared_keys
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import LocalStore

NOW = dt.datetime(2026, 11, 10, 3, 0, tzinfo=dt.timezone.utc)
SPEC = {
    "id": "port-vessels",
    "entity": "Example port vessel calls",
    "seed_id": "example-port",
    "request": {"method": "GET", "url": "https://port.test/api/vessels", "params": {"status": "expected"},
                "headers": {"X-Requested-With": "XMLHttpRequest"}},
    "paginate": {"in": "params", "field": "page", "start": 1, "step": 1, "max_pages": 5},
    "records_path": "data.items",
    "fingerprint": {"keys": ["vessel", "eta"], "min_records": 1},
}


def _spec(tmp_path: Path, **changes: Any) -> derived.Spec:
    data = {**SPEC, **changes}
    path = tmp_path / f"{data['id']}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return load_spec(path)


def _site(pages: int = 3, record: Any = None, status: int = 200) -> Any:
    """A backend with `pages` pages of two records, then empty pages. Records its calls."""
    calls: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        page = int(request.url.params.get("page", "1"))
        items = [] if page > pages else [dict(record or {"vessel": "MV A", "eta": "2026-11-12"}, row=f"{page}-{i}") for i in range(2)]
        return httpx.Response(status, json={"data": {"items": items}})

    handler.calls = calls  # type: ignore[attr-defined]
    return handler


def _run(tmp_path: Path, spec: derived.Spec, handler: Any, run: str = "r1") -> Dict[str, Any]:
    async def go() -> Dict[str, Any]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            fetcher = DerivedFetcher(http)
            engine = Engine(LocalStore(tmp_path / "store"), fetcher, workdir=tmp_path / f"work-{run}", now=lambda: NOW, run_id=run, concurrency=1)
            await engine.start()
            results = await run_derived(engine, [spec], fetcher)
            return {**(await engine.finish()), "clients": results}

    return asyncio.run(go())


def _fetch_rows(tmp_path: Path) -> List[Dict[str, Any]]:
    return [row for path in sorted((tmp_path / "store" / "fetches").rglob("*.parquet")) for row in pq.read_table(path).to_pylist()]


# ---------------------------------------------------------------- specs

def test_spec_must_match_its_file_name_and_hold_no_credentials(tmp_path: Path) -> None:
    assert _spec(tmp_path).keys == ("vessel", "eta")
    (tmp_path / "other.json").write_text(json.dumps(SPEC), encoding="utf-8")
    with pytest.raises(ValueError, match="id must match"):
        load_spec(tmp_path / "other.json")
    with pytest.raises(ValueError, match="credentials"):
        _spec(tmp_path, request={"url": "https://port.test/api", "headers": {"Cookie": "session=abc"}})
    with pytest.raises(ValueError, match="BlockedAddress"):
        _spec(tmp_path, request={"url": "http://169.254.169.254/latest"})


def test_pages_are_built_in_the_query_or_the_body(tmp_path: Path) -> None:
    method, url, headers, body, key = page_request(_spec(tmp_path), 2)
    assert (method, url, body) == ("GET", "https://port.test/api/vessels?status=expected&page=3", None) and key == url
    assert headers == {"X-Requested-With": "XMLHttpRequest"}

    post = _spec(tmp_path, request={"method": "POST", "url": "https://port.test/api/search", "json": {"filter": {"type": "bulk"}}},
                 paginate={"in": "json", "field": "paging.page", "start": 0, "step": 1, "max_pages": 3})
    _, url_a, _, body_a, key_a = page_request(post, 0)
    _, url_b, _, body_b, key_b = page_request(post, 1)
    assert body_b == {"filter": {"type": "bulk"}, "paging": {"page": 1}} and url_a == url_b and key_a != key_b
    assert post.request["json"] == {"filter": {"type": "bulk"}}          # the spec itself is not changed


def test_records_and_stable_keys() -> None:
    spec = derived.Spec("x", "x", "", {}, None, "data.items", (), 1)
    assert records_of(spec, b'{"data": {"items": [1, 2]}}') == [1, 2]
    assert records_of(spec, b'{"data": {}}') is None and records_of(spec, b"<html>") is None
    assert records_of(derived.Spec("x", "x", "", {}, None, "", (), 1), b"[1]") == [1]
    assert shared_keys([{"a": 1, "b": 2}, {"a": 3}, {"a": 4, "c": 5}]) == ["a"]     # only what every record has


@pytest.mark.parametrize("status,body,expected", [
    (200, {"data": {"items": [{"vessel": "A", "eta": "x"}]}}, "ok"),
    (403, {"data": {"items": [{"vessel": "A", "eta": "x"}]}}, "broken: status 403"),
    (200, {"result": []}, "broken: no list at records_path 'data.items'"),
    (200, {"data": {"items": []}}, "broken: 0 records, expected at least 1"),
    (200, {"data": {"items": [{"ship": "A", "eta": "x"}]}}, "broken: records lost keys ['vessel']"),
    (200, {"data": {"items": [{"eta": "x"}, {"vessel": "A", "eta": "y"}]}}, "ok"),          # a key missing from one record is not a break
])
def test_health_says_why_a_client_is_broken(tmp_path: Path, status: int, body: Any, expected: str) -> None:
    assert health(_spec(tmp_path), status, json.dumps(body).encode()) == expected


# ---------------------------------------------------------------- running

def test_a_client_stores_each_page_until_one_is_empty(tmp_path: Path) -> None:
    site = _site(pages=3)
    stats = _run(tmp_path, _spec(tmp_path), site)
    rows = _fetch_rows(tmp_path)
    assert stats["clients"] == {"port-vessels": {"status": "ok", "pages": 3, "records": 6}}
    assert len(site.calls) == 4 and stats["stored"] == 4                 # three pages of records and the empty one that ends it
    first = rows[0]
    assert first["url_requested"] == "https://port.test/api/vessels?status=expected&page=1" and first["tier_used"] == "API"
    assert (first["seed_id"], first["line_id"], first["entity"]) == ("example-port", "derived:port-vessels", "Example port vessel calls")
    assert json.loads(first["body"])["data"]["items"][0]["vessel"] == "MV A"


def test_a_second_run_keeps_no_second_copy_of_unchanged_pages(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    _run(tmp_path, spec, _site(pages=2), "r1")
    stats = _run(tmp_path, spec, _site(pages=2), "r2")
    assert stats["stored"] == 0 and stats["unchanged"] == 3 and stats["clients"]["port-vessels"]["status"] == "ok"


def test_a_changed_backend_is_reported_broken_and_its_reply_is_still_stored(tmp_path: Path) -> None:
    stats = _run(tmp_path, _spec(tmp_path), _site(record={"ship_name": "MV A", "eta": "2026-11-12"}))
    assert stats["clients"]["port-vessels"] == {"status": "broken: records lost keys ['vessel']", "pages": 0, "records": 0}
    assert stats["stored"] == 1                                         # what it did return is kept for the re-derive

    blocked = _run(tmp_path / "b", _spec(tmp_path), _site(status=403), "r1")
    assert blocked["clients"]["port-vessels"]["status"] == "broken: status 403"


def test_a_later_page_failing_is_reported_without_calling_the_client_broken(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") == "2":
            return httpx.Response(429)
        return httpx.Response(200, json={"data": {"items": [{"vessel": "MV A", "eta": "2026-11-12"}]}})

    stats = _run(tmp_path, _spec(tmp_path), handler)
    assert stats["clients"]["port-vessels"] == {"status": "ok", "pages": 1, "records": 1, "stopped": "page 2: status 429"}


def test_a_client_that_gets_no_reply_is_broken_and_the_next_one_still_runs(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "dead.test":
            raise httpx.ConnectTimeout("timed out", request=request)
        if request.url.host == "odd.test":
            raise RuntimeError("something unforeseen")     # not a network error: the engine catches it
        return httpx.Response(200, json={"data": {"items": [{"vessel": "MV A", "eta": "2026-11-12"}]}})

    specs = [
        _spec(tmp_path, id="a-dead", request={"url": "https://dead.test/api"}),
        _spec(tmp_path, id="b-odd", request={"url": "https://odd.test/api"}),
        _spec(tmp_path, id="c-fine", request={"url": "https://fine.test/api"}, paginate=None),
    ]

    async def go() -> Dict[str, Any]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            fetcher = DerivedFetcher(http)
            engine = Engine(LocalStore(tmp_path / "store"), fetcher, workdir=tmp_path / "work", now=lambda: NOW, run_id="r1", concurrency=1)
            await engine.start()
            results = await run_derived(engine, specs, fetcher)
            await engine.finish()
            return results

    results = asyncio.run(go())
    assert results["a-dead"]["status"] == "broken: no reply (ConnectTimeout: timed out)"
    assert results["b-odd"]["status"] == "broken: no reply (an unexpected error; its fetch row has the detail)"
    assert results["c-fine"] == {"status": "ok", "pages": 1, "records": 1}


def test_check_calls_two_pages_and_suggests_a_fingerprint_without_storing(tmp_path: Path) -> None:
    spec = _spec(tmp_path, fingerprint=None)

    async def go() -> Dict[str, Any]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(_site())) as http:
            return await check(spec, http)

    report = asyncio.run(go())
    assert report["suggested_fingerprint"] == {"keys": ["eta", "row", "vessel"], "min_records": 1}
    assert report["page_1"]["records"] == 2 and report["page_1"]["health"] == "no fingerprint yet"
    assert report["page_2"]["differs_from_first_page"] is True and not (tmp_path / "store").exists()


# ---------------------------------------------------------------- capture helpers

def test_capture_helpers_sort_data_requests_from_noise() -> None:
    assert is_noise("https://www.google-analytics.com/g/collect?v=2") and is_noise("https://port.test/collect?e=view")
    assert not is_noise("https://port.test/api/vessels?page=1")
    assert is_first_party("https://api.port.test/v1/x", "https://www.port.test/vessels")
    assert not is_first_party("https://cdn.other.test/x", "https://www.port.test/vessels")
    headers = kept_headers({"Cookie": "session=secret", "Authorization": "Bearer secret", "X-Requested-With": "XMLHttpRequest",
                            "User-Agent": "x", "Accept": "application/json", "X-Forwarded-For": "1.2.3.4"})
    assert headers == {"cookie": "<present, not recorded>", "authorization": "<present, not recorded>",
                       "x-requested-with": "XMLHttpRequest", "accept": "application/json"}
    with pytest.raises(ValueError, match="credentials"):          # the spec loader refuses the same headers
        _spec(Path(__import__("tempfile").mkdtemp()), request={"url": "https://port.test/api", "headers": {"X-XSRF-TOKEN": "abc"}})
    assert "secret" not in json.dumps(headers)
    assert json_shape({"data": {"items": [{"vessel": "A", "berth": 3}], "page": 1}}) == {
        "data": {"items": {"list_len": 1, "item": {"vessel": "str", "berth": "int"}}, "page": "int"}}
