"""Courtesy pacing: Crawl-delay, Retry-After and sitting out. No network."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any, List, Optional

import httpx
import pyarrow.parquet as pq

from frontier_crawler.harvester.cascade import USER_AGENT, HarvesterCascade, ScrapeResult
from frontier_crawler.harvester.courtesy import MAX_CRAWL_DELAY_S, crawl_delay, retry_after, sit_out
from frontier_crawler.harvester.throttle import HostThrottle
from frontier_crawler.storage.store import LocalStore
from tests.test_crawl_stream import Site, _crawl, _frontier


def test_crawl_delay_is_ours_else_everyones_and_capped() -> None:
    robots = b"User-agent: *\nCrawl-delay: 5\n\nUser-agent: other\nUser-agent: frontier-crawler\nDisallow: /x\nCrawl-delay: 12 # ours\n"
    assert crawl_delay(robots, USER_AGENT) == 12.0
    assert crawl_delay(robots, "SomeoneElse/1.0") == 5.0
    assert crawl_delay(b"User-agent: *\nCrawl-delay: 900\n", USER_AGENT) == MAX_CRAWL_DELAY_S
    assert crawl_delay(b"User-agent: *\nCrawl-delay: soon\nDisallow: /\n", USER_AGENT) is None
    assert crawl_delay(b"Sitemap: https://a.test/s.xml\n", USER_AGENT) is None


def test_retry_after_reads_seconds_or_a_date() -> None:
    now = dt.datetime(2026, 11, 14, 12, 0, tzinfo=dt.timezone.utc)
    assert retry_after("120") == 120.0 and retry_after(None) is None and retry_after("later") is None
    assert retry_after("Sat, 14 Nov 2026 12:01:30 GMT", now) == 90.0
    assert retry_after("Sat, 14 Nov 2026 11:00:00 GMT", now) == 0.0


def test_a_host_sits_out_a_short_wait_and_is_cooled_for_a_long_one_or_a_bare_429() -> None:
    clock = [0.0]
    throttle = HostThrottle(1.0, clock=lambda: clock[0])
    assert sit_out(throttle, "https://a.test/1", 200, "60") is None and sit_out(throttle, "https://a.test/1", 503, None) is None
    assert sit_out(throttle, "https://a.test/1", 503, "60") is False and throttle._next_ok["a.test"] == 60.0
    assert sit_out(throttle, "https://b.test/1", 429, None) is True and "b.test" not in throttle._next_ok
    assert sit_out(throttle, "https://b.test/1", 429, "3600") is True


def test_the_throttle_takes_a_hosts_longer_gap_and_its_holds() -> None:
    clock = [0.0]
    slept: List[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock[0] += seconds

    async def run() -> None:
        throttle = HostThrottle(2.0, clock=lambda: clock[0], sleep=sleep, intervals={"slow.test": 10.0, "fast.test": 0.5})
        for url in ("https://slow.test/1", "https://slow.test/2", "https://fast.test/1", "https://fast.test/2"):
            await throttle.wait(url)
        throttle.hold("https://fast.test/", 30.0)
        await throttle.wait("https://fast.test/3")

    asyncio.run(run())
    assert slept == [10.0, 2.0, 30.0]   # fast.test keeps the run's gap: an override only lengthens it


def _cascade(handler: Any, throttle: HostThrottle, browser: Optional[List[str]] = None) -> HarvesterCascade:
    fetcher = HarvesterCascade(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)), throttle=throttle)
    if browser is not None:
        async def fake_browser(url: str, *_: Any) -> ScrapeResult:
            browser.append(url)
            return ScrapeResult(url, 0, b"", None, 0, "HEADLESS_PLAYWRIGHT", "browser got nothing")
        fetcher._browser = fake_browser  # type: ignore[method-assign]
    return fetcher


def test_the_cascade_cools_on_a_429_and_waits_out_a_503_without_the_browser() -> None:
    replies = {"/busy": (429, {}), "/later": (503, {"retry-after": "20"}), "/down": (503, {})}
    browser: List[str] = []
    throttle = HostThrottle(0.0)

    async def run() -> List[ScrapeResult]:
        handler = lambda request: httpx.Response(replies[request.url.path][0], headers=replies[request.url.path][1], text="<html>busy</html>")  # noqa: E731
        async with _cascade(handler, throttle, browser) as fetcher:
            return [await fetcher.fetch("https://a.test/busy"), await fetcher.fetch("https://b.test/later"), await fetcher.fetch("https://c.test/down")]

    busy, later, down = asyncio.run(run())
    assert busy.cool_host and not later.cool_host and not down.cool_host
    assert "b.test" in throttle._next_ok and browser == ["https://c.test/down"]   # a plain 503 still tries the browser


def test_reading_robots_sets_the_hosts_gap_and_a_missing_robots_clears_it() -> None:
    throttle = HostThrottle(1.0, intervals={"b.test": 9.0})
    files = {"https://a.test/robots.txt": b"User-agent: *\nCrawl-delay: 7\n"}

    async def run() -> None:
        served = lambda request: httpx.Response(200, content=files[str(request.url)]) if str(request.url) in files else httpx.Response(404)  # noqa: E731
        async with _cascade(served, throttle) as fetcher:
            await fetcher.sitemap_entries("https://a.test")
            await fetcher.sitemap_entries("https://b.test")

    asyncio.run(run())
    assert throttle.intervals == {"a.test": 7.0}


def test_a_crawl_keeps_crawl_delays_for_later_runs_and_cools_a_host_that_asks(tmp_path: Path) -> None:
    class Busy(Site):
        async def fetch(self, url: str, timeout: Any = None, connect: Any = None) -> ScrapeResult:
            if url == "https://h0.test/":
                return ScrapeResult(url, 429, b"slow down", "slow down", 2, "DOM_TRAFILATURA", None, "text/plain", cool_host=True)
            return await super().fetch(url, timeout, connect)

    throttle = HostThrottle(0.0, intervals={"h1.test": 4.0})
    stats = _crawl(tmp_path, Busy(3), throttle=throttle)
    assert stats["hosts_cooled"] == 1
    hosts = {row["host"]: row["crawl_delay_s"] for row in pq.read_table(tmp_path / "store" / "state" / "hosts.parquet").to_pylist()}
    assert hosts["h1.test"] == 4.0 and hosts["a.test"] is None and _frontier(tmp_path).crawl_delay == {"h1.test": 4.0}
    later = HostThrottle(0.0)
    _crawl(tmp_path, Site(3), run="r2", store=LocalStore(tmp_path / "store"), throttle=later)
    assert later.intervals == {"h1.test": 4.0}
