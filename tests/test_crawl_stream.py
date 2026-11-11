"""The crawl as a rolling stream: workers, judging and uploads off the fetch slot, the retry lane. No network."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pyarrow.parquet as pq
import pytest

from frontier_crawler import frontier as frontier_module
from frontier_crawler import pipeline
from frontier_crawler.frontier import Frontier, Seed, crawl
from frontier_crawler.harvester.cascade import ScrapeResult
from frontier_crawler.judge import questions as judging
from frontier_crawler.judge.jev import JevClient
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import LocalStore

NOW = dt.datetime(2026, 11, 9, 20, 0, tzinfo=dt.timezone.utc)
TEXT = "The port authority published the berth schedule and the tariff for bulk cargo. " * 12
ROOT = "https://a.test/"
SEEDS = [Seed(id="a", entity="a", urls=(ROOT,))]


class Site:
    """A root page linking to one page on each of n hosts. A page can wait on a gate, sleep, or time out its first
    attempts; fetch records the order pages start and finish, and the timeout each was given."""

    def __init__(self, n: int, delay: float = 0.0, timeouts: Optional[Dict[str, int]] = None, dead: int = 0) -> None:
        self.pages = [f"https://h{i}.test/" for i in range(n)]
        self.dead = [f"https://d{i}.test/" for i in range(dead)]
        self.delay = delay
        self.timeouts = dict(timeouts or {})
        self.started: List[str] = []
        self.finished: List[str] = []
        self.limits: List[tuple] = []
        self.gates: Dict[str, asyncio.Event] = {}
        self.on_start: Any = None

    async def fetch(self, url: str, timeout: Optional[float] = None) -> ScrapeResult:
        self.started.append(url)
        self.limits.append((url, timeout))
        try:
            if self.on_start is not None:
                await self.on_start(self, url)
            await asyncio.sleep(self.delay)
            if self.timeouts.get(url, 0) > 0:
                self.timeouts[url] -= 1
                return ScrapeResult(url, 0, b"", None, 0, "DOM_TRAFILATURA", "ReadTimeout: slow", timed_out=True)
            if url in self.dead:
                return ScrapeResult(url, 404, b"<html>Not found</html>", "Not found", 2, "DOM_TRAFILATURA", None, "text/html")
            links = "".join(f'<a href="{page}">p</a>' for page in self.dead + self.pages) if url == ROOT else ""
            text = f"{url} {TEXT}"
            body = f"<html><body>{links}<p>{text}</p></body></html>".encode()
            return ScrapeResult(url, 200, body, text, len(text.split()), "DOM_TRAFILATURA", None, "text/html")
        finally:
            self.finished.append(url)


def _jev_body(noul: float = 0.9) -> Dict[str, Any]:
    answers = {q.key: ({"type": "noul", "noul": noul} if q.type == "noul" else
                       {"type": "choice", "choice": "record", "confidence": 0.8, "probabilities": {"record": 0.8, "other": 0.2}})
               for q in judging.active()}
    return {"model": "jev-1.13.0", "answers": answers, "usage": {"input_tokens": 700, "output_tokens": 90}}


def _jev(handler: Any) -> JevClient:
    async def no_sleep(_: float) -> None:
        return None

    return JevClient("test-key", httpx.AsyncClient(transport=httpx.MockTransport(handler)), sleep=no_sleep)


def _crawl(tmp_path: Path, site: Any, run: str = "r1", store: Any = None, max_fetches: int = 1000, **kwargs: Any) -> Dict[str, Any]:
    store = store or LocalStore(tmp_path / "store")
    engine_args = {key: kwargs.pop(key) for key in ("jev", "monthly_usd", "concurrency") if key in kwargs}
    engine = Engine(store, site, workdir=tmp_path / run, now=lambda: NOW, run_id=run, **engine_args)

    async def go() -> Dict[str, Any]:
        # A crawl that waited on its slowest page would hang here; fail instead.
        return await asyncio.wait_for(crawl(store, engine, SEEDS, workdir=tmp_path / run, max_fetches=max_fetches, now=lambda: NOW, **kwargs), 20)

    return asyncio.run(go())


def _frontier(tmp_path: Path) -> Frontier:
    return Frontier.load(LocalStore(tmp_path / "store"), tmp_path / "state-check")


def _rows(tmp_path: Path, table: str) -> List[Dict[str, Any]]:
    return [row for path in (tmp_path / "store" / table).rglob("*.parquet") for row in pq.read_table(path).to_pylist()]


def test_a_slow_page_holds_one_worker_while_the_others_keep_fetching(tmp_path: Path) -> None:
    site = Site(12)
    held: List[str] = []

    async def hold_the_first(site: Site, url: str) -> None:
        if url != ROOT and not held:
            held.append(url)
            while len([u for u in site.finished if u != ROOT]) < 8:   # until eight other pages are done
                await asyncio.sleep(0.001)

    site.on_start = hold_the_first
    stats = _crawl(tmp_path, site, concurrency=4)
    others_before = site.finished.index(held[0])
    assert others_before >= 9 and stats["total"] == 13 and stats["stopped_by"] == "queue_empty"   # the root and eight others
    assert stats["workers"] == 4 and stats["peak_in_flight"] == 4 and stats["queued"] == 0


def test_max_fetches_counts_pages_taken_so_none_overshoot(tmp_path: Path) -> None:
    stats = _crawl(tmp_path, Site(20, delay=0.002), max_fetches=6, concurrency=4)
    assert stats["stopped_by"] == "max_fetches" and stats["total"] == 6 and stats["queued"] == 15


def test_the_deadline_stops_new_takes_and_pages_in_flight_finish(tmp_path: Path) -> None:
    clock = [0.0]

    async def slow_then_late(site: Site, url: str) -> None:
        if url != ROOT and len(site.started) == 3:   # the third page started: time runs out while it loads
            clock[0] = 10_000.0
            await asyncio.sleep(0.05)

    site = Site(10)
    site.on_start = slow_then_late
    stats = _crawl(tmp_path, site, concurrency=3, max_minutes=1, clock=lambda: clock[0])
    assert stats["stopped_by"] == "max_minutes" and stats["total"] == len(site.started) == len(site.finished) <= 4
    frontier = _frontier(tmp_path)
    assert all(frontier.entry(url)["status"] == "fetched" for url in site.started)   # recorded, not lost
    assert frontier.count("queued") == 11 - len(site.started)


def test_a_worker_survives_a_fault_in_fetching_in_recording_and_in_judging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    broken_fetch, broken_links, broken_judge = "https://h1.test/", "https://h2.test/", "https://h3.test/"
    original = Frontier.add_links

    def add_links(self: Frontier, entry: Dict[str, Any], row: Dict[str, Any], answers: Any, now: dt.datetime) -> int:
        if entry["url"] == broken_links:
            raise ValueError("a bug after the page was marked")
        return original(self, entry, row, answers, now)

    monkeypatch.setattr(Frontier, "add_links", add_links)

    def jev(request: httpx.Request) -> httpx.Response:
        if broken_judge in json.loads(request.content)["state"]:
            raise RuntimeError("a bug in judging")
        return httpx.Response(200, json=_jev_body())

    site = Site(6)
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "r1", now=lambda: NOW, run_id="r1", jev=_jev(jev), monthly_usd=8)
    fetch_row = engine._fetch_row

    async def faulty(target: Any, timeout: Optional[float] = None) -> Any:
        if target.url == broken_fetch:
            raise RuntimeError("a bug in the engine")
        return await fetch_row(target, timeout)

    engine._fetch_row = faulty  # type: ignore[method-assign]
    stats = asyncio.run(crawl(store, engine, SEEDS, workdir=tmp_path / "r1", max_fetches=100, now=lambda: NOW))
    assert stats["total"] == 6 and stats["judged"] == 4 and stats["queued"] == 0   # every page but the broken fetch, once
    frontier = _frontier(tmp_path)
    assert frontier.entry(broken_fetch)["status"] == "failed"
    assert frontier.entry(broken_links)["status"] == "fetched" and frontier.entry(broken_judge)["status"] == "fetched"
    judged = {call["fetch_id"] for call in _rows(tmp_path, "judgement_calls")}
    # Stored, and left for the backlog: the page Jev failed on, and the one whose bookkeeping failed before it was handed on.
    assert {row["url_requested"] for row in _rows(tmp_path, "fetches") if row["fetch_id"] not in judged} == {broken_judge, broken_links}


def test_a_stuck_judgement_is_abandoned_after_the_grace_period(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(frontier_module, "JUDGE_GRACE_SECONDS", 0.1)
    stuck = "https://h0.test/"

    async def jev(request: httpx.Request) -> httpx.Response:
        if stuck in json.loads(request.content)["state"]:
            await asyncio.sleep(3600)
        return httpx.Response(200, json=_jev_body())

    stats = _crawl(tmp_path, Site(4), jev=_jev(jev), monthly_usd=8)
    assert stats["jev_abandoned"] == 1 and stats["judged"] == 4 and stats["stored"] == 5
    assert stats["seconds"]["judge_wait"] < 5
    unjudged = {row["fetch_id"] for row in _rows(tmp_path, "fetches")} - {call["fetch_id"] for call in _rows(tmp_path, "judgement_calls")}
    assert {row["url_requested"] for row in _rows(tmp_path, "fetches") if row["fetch_id"] in unjudged} == {stuck}


def test_judging_does_not_hold_a_fetch_slot(tmp_path: Path) -> None:
    async def slow_jev(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.2)
        return httpx.Response(200, json=_jev_body())

    site = Site(8)
    stats = _crawl(tmp_path, site, jev=_jev(slow_jev), monthly_usd=8, concurrency=2)
    # Nine pages fetched by two workers would take 0.9 s if each waited for its 0.2 s judgement.
    assert stats["judged"] == 9 and stats["seconds"]["fetching"] < 0.5 and stats["jev"]["seconds_total"] >= 1.8


def test_links_are_queued_at_once_and_re_scored_when_the_judgement_arrives(tmp_path: Path) -> None:
    stats = _crawl(tmp_path, Site(3), jev=_jev(lambda request: httpx.Response(200, json=_jev_body())), monthly_usd=8, max_fetches=1)
    bases = {e["base"] for e in _frontier(tmp_path)._entries.values() if e["parent_fetch_id"]}
    assert stats["judged"] == 1 and stats["links_added"] == 3 and bases == {frontier_module.link_base(_answers())}


def _answers() -> Any:
    class Answers:
        def nouls(self) -> Dict[str, float]:
            return {q.id: 0.9 for q in judging.active() if q.type == "noul"}

    return Answers()


def test_a_failed_periodic_upload_loses_no_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(frontier_module, "FLUSH_EVERY_SECONDS", 0.005)
    monkeypatch.setattr(pipeline, "_UPLOAD_EVERY_S", 0.0)   # every flush uploads what it has

    class FlakyStore(LocalStore):
        calls = 0

        def put_many(self, files: Any, message: str = "") -> None:
            self.calls += 1
            if self.calls <= 2:
                raise OSError("the store is down")
            super().put_many(files, message)

    store = FlakyStore(tmp_path / "store")
    stats = _crawl(tmp_path, Site(30, delay=0.003), store=store, concurrency=2)
    rows = _rows(tmp_path, "fetches")
    assert store.calls > 3 and stats["total"] == 31 == len(rows) == len({row["fetch_id"] for row in rows})
    assert len(stats["shards"]) == len(list((tmp_path / "store" / "fetches").rglob("*.parquet")))


def test_a_timed_out_page_is_retried_once_with_the_long_limit(tmp_path: Path) -> None:
    recovers, fails = "https://h0.test/", "https://h1.test/"
    site = Site(3, timeouts={recovers: 1, fails: 2})
    stats = _crawl(tmp_path, site)
    assert stats["retries"] == 2 and stats["retry_recovered"] == 1 and stats["errors"] == 3
    assert [limit for url, limit in site.limits if url == recovers] == [None, frontier_module.RETRY_TIMEOUT_SECONDS]
    frontier = _frontier(tmp_path)
    assert frontier.entry(recovers)["status"] == "fetched" and frontier.entry(fails)["status"] == "failed"


def test_a_retry_left_at_the_end_of_a_run_is_taken_by_the_next(tmp_path: Path) -> None:
    late = "https://h0.test/"
    clock = [0.0]

    async def time_runs_out(site: Site, url: str) -> None:
        if url == late:
            clock[0] = 10_000.0   # the deadline passes while it times out: no worker is left to retry it

    site = Site(3, timeouts={late: 1})
    site.on_start = time_runs_out
    first = _crawl(tmp_path, site, run="r1", concurrency=1, max_minutes=1, clock=lambda: clock[0])
    assert first["retries"] == 0 and _frontier(tmp_path).entry(late)["status"] == "retry"
    site.limits.clear()
    site.on_start = None
    second = _crawl(tmp_path, site, run="r2", max_fetches=0, concurrency=1)
    assert site.limits == [(late, frontier_module.RETRY_TIMEOUT_SECONDS)] and second["retry_recovered"] == 1


def test_past_max_fetches_every_idle_worker_works_off_the_retries(tmp_path: Path) -> None:
    slow = {f"https://h{i}.test/": 1 for i in range(6)}
    site = Site(6, delay=0.01, timeouts=slow)
    retrying: List[int] = [0, 0]   # now, peak

    async def count_retries(site: Site, url: str) -> None:
        if site.limits[-1][1] is not None:
            retrying[0] += 1
            retrying[1] = max(retrying)
            await asyncio.sleep(0.02)
            retrying[0] -= 1

    site.on_start = count_retries
    stats = _crawl(tmp_path, site, max_fetches=7, concurrency=4)
    assert stats["stopped_by"] == "max_fetches" and stats["retries"] == stats["retry_recovered"] == 6 and stats["total"] == 13
    assert retrying[1] > frontier_module.MAX_RETRY_IN_FLIGHT and _frontier(tmp_path).count("retry") == 0


def test_a_crawl_fails_retries_that_waited_too_long(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    frontier = Frontier()
    frontier.add_seeds(SEEDS, NOW)
    frontier._add("https://h0.test/", "a", "a", 1, 1.0, NOW)
    frontier.mark(frontier.entry("https://h0.test/"), {"outcome": "error", "timed_out": True}, None, NOW - dt.timedelta(days=3))
    store.put_many(frontier.save(tmp_path / "w0"), "state")
    stats = _crawl(tmp_path, Site(0), store=store)
    assert stats["retries_expired"] == 1 and stats["retries"] == 0 and _frontier(tmp_path).entry("https://h0.test/")["status"] == "failed"


def test_with_no_jev_a_line_of_good_pages_keeps_a_high_yield(tmp_path: Path) -> None:
    # Four dead links are taken first (their hosts sort first), then twelve good pages Jev cannot read: those count
    # at the line's relevance, so the yield climbs back. Left out, as before, they would leave it at 0.85^4 = 0.52.
    stats = _crawl(tmp_path, Site(12, dead=4), concurrency=1)
    line = _frontier(tmp_path).line("a")
    assert stats["pending"] == 13 and stats["error_pages"] == 4 and line["fetches"] == 17 and line["yield"] > 0.85
