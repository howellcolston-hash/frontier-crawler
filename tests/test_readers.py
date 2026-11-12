"""Checks for reading in worker processes: a reader that crashes or hangs costs one page its text, nothing more. No live network."""

from __future__ import annotations

import asyncio
import ctypes
import datetime as dt
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx
import pyarrow.parquet as pq
import pytest
import trafilatura

from frontier_crawler.harvester import readers
from frontier_crawler.harvester.cascade import HarvesterCascade, ScrapeResult, Target
from frontier_crawler.harvester.readers import ReaderPool, decode, extract_links, read_html
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import LocalStore

TEXT = "The port authority published the berth schedule and the tariff for bulk cargo. " * 15
PAGES = {
    "https://a.test/links": f'<html><body><a href="/one">One</a> <a href="https://b.test/x#y">B</a> <a href="http://[bad">x</a> <p>{TEXT}</p></body></html>'.encode(),
    "https://a.test/legacy": f"<html><head><meta charset='gb18030'></head><body><p>{'港口管理局公布了泊位时间表。' * 40}</p></body></html>".encode("gb18030"),
    "https://a.test/broken": b"<html><body><div><div><table><tr><td><a href='/deep'>deep" + b"<b>" * 5000 + TEXT.encode() + b"<a href=",
}


def crash_or_hang(body: bytes, content_type: str, url: str) -> Any:
    """read_html, except that a page saying CRASH kills its worker with a segfault and one saying HANG never returns.
    Module level, so a worker process can import it."""
    if b"CRASH" in body:
        ctypes.string_at(0)
    if b"HANG" in body:
        time.sleep(600)
    return read_html(body, content_type, url)


def _server(pages: Dict[str, bytes]) -> Any:
    return lambda request: httpx.Response(200, content=pages[str(request.url)], headers={"content-type": "text/html"})


def _fetch_all(pages: Dict[str, bytes], reader: Any = crash_or_hang, after: Tuple[str, ...] = ()) -> Tuple[Dict[str, ScrapeResult], HarvesterCascade]:
    """Fetch every page at once, then the `after` pages one by one, through a cascade whose HTML reader is `reader`."""

    async def run() -> Tuple[Dict[str, ScrapeResult], HarvesterCascade]:
        async with HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(_server(pages)))) as fetcher:
            fetcher.read_html = reader
            first = [url for url in pages if url not in after]
            results = dict(zip(first, await asyncio.gather(*(fetcher.fetch(url) for url in first))))
            for url in after:
                results[url] = await fetcher.fetch(url)
        return results, fetcher

    return asyncio.run(run())


def _read_here(url: str, body: bytes) -> Tuple[Any, Any]:
    """(text, links) read in this process with the functions the reader calls."""
    return trafilatura.extract(decode(body, "text/html")[0], output_format="markdown", include_comments=False, include_tables=True), extract_links(body, url)


def test_a_worker_reads_exactly_what_this_process_would() -> None:
    async def run() -> List[Any]:
        pool = ReaderPool({})
        try:
            return await asyncio.gather(*(pool.run(10, read_html, body, "text/html", url) for url, body in PAGES.items()))
        finally:
            pool.close()

    for (url, body), (text, encoding, links, _) in zip(PAGES.items(), asyncio.run(run())):
        assert (text, links) == _read_here(url, body) and encoding == decode(body, "text/html")[1]
    assert asyncio.run(run())[0][2] == ([("https://a.test/one", "One"), ("https://b.test/x", "B")], ["http://[bad"])


def test_a_page_title_is_read_trimmed_and_cut_and_text_without_html_has_none() -> None:
    long = "Notices " * 60
    body = f"<html><head><title>\n  Tenders &amp; notices \n</title></head><body><p>{TEXT}</p></body></html>".encode()
    assert read_html(body, "text/html", "https://a.test/")[3] == "Tenders & notices"
    assert len(read_html(f"<html><head><title>{long}</title></head></html>".encode(), "text/html", "https://a.test/")[3]) == 300
    legacy = f"<html><head><title>港口管理局</title></head><body><p>{'港口管理局公布了泊位时间表。' * 40}</p></body></html>".encode("gb18030")
    assert read_html(legacy, "text/html; charset=gb18030", "https://a.test/")[3] == "港口管理局"
    assert read_html(b"<html><head><title> </title></head></html>", "text/html", "https://a.test/")[3] is None
    assert read_html(b"a plain <title>x</title> file", "text/plain", "https://a.test/")[2:] == (None, None)


def test_a_reader_that_crashes_costs_its_page_its_text_and_nothing_else() -> None:
    pages = {**PAGES, "https://a.test/crash": b"<html><body>CRASH <a href='/z'>z</a></body></html>", "https://a.test/later": PAGES["https://a.test/links"].replace(b"One", b"Later")}
    results, fetcher = _fetch_all(pages, after=("https://a.test/later",))
    crashed = results["https://a.test/crash"]
    assert crashed.raw_payload == pages["https://a.test/crash"] and crashed.markdown_content is None and crashed.links is None
    assert crashed.error_message == "html not read: reader crashed"
    for url in (*PAGES, "https://a.test/later"):  # in flight beside it, or read after it on a new pool
        assert (results[url].markdown_content, results[url].links) == _read_here(url, pages[url])
    assert fetcher.counts["reader_crashes"] == 1 and fetcher.counts["reader_timeouts"] == 0


def test_a_reader_that_hangs_is_timed_out_and_the_rest_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readers, "HTML_SECONDS", 2.0)
    pages = {**PAGES, "https://a.test/hang": b"<html><body>HANG</body></html>", "https://a.test/later": PAGES["https://a.test/links"]}
    results, fetcher = _fetch_all(pages, after=("https://a.test/later",))
    hung = results["https://a.test/hang"]
    assert hung.raw_payload == pages["https://a.test/hang"] and hung.markdown_content is None and hung.error_message == "html not read: reader timed out"
    for url in (*PAGES, "https://a.test/later"):
        assert (results[url].markdown_content, results[url].links) == _read_here(url, pages[url])
    assert fetcher.counts["reader_timeouts"] == 1 and fetcher.counts["reader_crashes"] == 0


def test_a_crashed_page_is_stored_with_its_bytes_and_the_run_counts_it(tmp_path: Path) -> None:
    pages = {"https://a.test/crash": b"<html><body>CRASH</body></html>", "https://a.test/links": PAGES["https://a.test/links"]}
    now = dt.datetime(2026, 11, 11, 20, tzinfo=dt.timezone.utc)

    async def run() -> Dict[str, Any]:
        async with HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(_server(pages)))) as fetcher:
            fetcher.read_html = crash_or_hang
            engine = Engine(LocalStore(tmp_path / "store"), fetcher, workdir=tmp_path / "work", now=lambda: now, run_id="r1")
            await engine.start()
            await engine.run([Target(url) for url in pages])
            return await engine.finish()

    stats = asyncio.run(run())
    rows = {row["url_final"]: row for path in (tmp_path / "store" / "fetches").rglob("*.parquet") for row in pq.read_table(path).to_pylist()}
    bodies = {row["sha256"] for path in (tmp_path / "store" / "bodies").rglob("*.parquet") for row in pq.read_table(path).to_pylist()}
    crashed = rows["https://a.test/crash"]
    assert crashed["outcome"] == "stored" and crashed["text"] is None and crashed["tier_note"] == "html not read: reader crashed"
    assert crashed["sha256"] in bodies and rows["https://a.test/links"]["text"]
    assert stats["reader_crashes"] == 1 and stats["reader_timeouts"] == 0 and stats["links_written"] == 2


def test_this_process_parses_no_fetched_page(monkeypatch: pytest.MonkeyPatch) -> None:
    import lxml.html

    def refuse(*_: Any, **__: Any) -> Any:
        raise AssertionError("a fetched page was parsed in the crawler's own process")

    monkeypatch.setattr(lxml.html, "fromstring", refuse)
    monkeypatch.setattr(trafilatura, "extract", refuse)
    calls: List[str] = []
    run = ReaderPool.run

    async def counted(self: ReaderPool, limit: float, fn: Any, *args: Any) -> Any:
        calls.append(fn.__name__)
        return await run(self, limit, fn, *args)

    monkeypatch.setattr(ReaderPool, "run", counted)
    results, _ = _fetch_all(PAGES, reader=read_html)
    assert all(result.markdown_content and result.links is not None for result in results.values())
    assert calls == ["read_html"] * len(PAGES)  # one reader call per page, for text and links together


@pytest.mark.parametrize("fail", [False, True])
def test_the_workers_are_stopped_when_the_cascade_closes_even_on_error(fail: bool) -> None:
    processes: List[Any] = []

    async def run() -> None:
        async with HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(_server(PAGES)))) as fetcher:
            await asyncio.gather(*(fetcher.fetch(url) for url in PAGES))
            processes.extend(fetcher._readers._pools["main"]._processes.values())
            if fail:
                raise RuntimeError("the run failed")

    if fail:
        with pytest.raises(RuntimeError):
            asyncio.run(run())
    else:
        asyncio.run(run())
    for process in processes:
        process.join(5)
    assert processes and all(process.exitcode is not None for process in processes)


def test_a_rendered_page_gets_its_links_from_a_worker_and_keeps_the_browsers_text() -> None:
    rendered = f'<html><body><a href="/notice/1">Notice 1</a><a href="https://b.test/">B</a><p>{TEXT}</p></body></html>'.encode()

    async def run() -> ScrapeResult:
        shell = lambda request: httpx.Response(200, content=b'<html><body><div id="app"></div></body></html>', headers={"content-type": "text/html"})  # noqa: E731
        async with HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(shell))) as fetcher:
            async def browser(url: str, *_: Any) -> ScrapeResult:
                return ScrapeResult(url, 200, rendered, "what the browser read", 4, "HEADLESS_PLAYWRIGHT", None, "text/html; rendered")

            fetcher._browser = browser  # type: ignore[method-assign]
            return await fetcher.fetch("https://a.test/app")

    result = asyncio.run(run())
    assert result.tier_used == "HEADLESS_PLAYWRIGHT" and result.markdown_content == "what the browser read"
    assert result.links == extract_links(rendered, "https://a.test/app") and len(result.links[0]) == 2
