"""Checks for the crawl queue: link extraction, how the next URLs are chosen, and a crawl against a fake site. No network."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import heapq
import math
import random
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from frontier_crawler.frontier import (
    GENERATION_DECAY, HOP_DECAY, HOST_ROUND_SHARE, MAX_IN_FLIGHT_PER_HOST, NEW_HOST_SHARE, RETEST_SHARE, Frontier, Seed, crawl, fetch_yield, link_base, load_seeds,
)
from frontier_crawler.harvester.readers import extract_links, normalise
from frontier_crawler.judge import questions as judging
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import LocalStore
from frontier_crawler.harvester.cascade import ScrapeResult

NOW = dt.datetime(2026, 11, 9, 20, 0, tzinfo=dt.timezone.utc)
TEXT = "The port authority published the berth schedule and the tariff for bulk cargo. " * 12


def _seed(sid: str, url: str, **kwargs: Any) -> Seed:
    return Seed(id=sid, entity=sid, urls=(url,), **kwargs)


def test_links_are_absolute_distinct_and_skip_assets_and_share_buttons() -> None:
    html = b"""<html><body>
      <a href="/notices/1.pdf">Notice 1</a> <a href="/notices/1.pdf#page=2">again</a>
      <a href="https://other.test/data?id=7">Data</a> <a href="logo.png">logo</a>
      <a href="mailto:x@a.test">mail</a> <a href="https://www.facebook.com/sharer/sharer.php?u=x">share</a>
      <a href="https://www.facebook.com/SomePortAuthority">page</a> <a href="#top">top</a>
      <a href="http://[bad">broken</a> <a href="//[::1">broken too</a></body></html>"""
    links, unparseable = extract_links(html, "https://A.test/notices/")
    assert unparseable == ["http://[bad", "//[::1"]
    assert links == [
        ("https://a.test/notices/1.pdf", "Notice 1"),
        ("https://other.test/data?id=7", "Data"),
        ("https://www.facebook.com/SomePortAuthority", "page"),
    ]
    assert normalise("ftp://a.test/x") == "" and normalise("HTTPS://A.Test") == "https://a.test/"


def _content(url: str = "", tier: str = "DOM_TRAFILATURA") -> Dict[str, Any]:
    return {"outcome": "stored", "http_status": 200, "url_final": url, "tier_used": tier}


DEAD = {"outcome": "error_page", "http_status": 404, "block_marker": "http 404"}


def _page_row(url: str, links: str) -> Dict[str, Any]:
    """A stored HTML page as Engine.fetch returns it, with its links extracted."""
    return {**_content(url), "fetch_id": "f-" + url, "content_type": "text/html", "links": extract_links(f"<html><body>{links}</body></html>".encode(), url)}


def test_a_round_spreads_across_hosts_and_lines() -> None:
    frontier = Frontier()
    seeds = [_seed("big", "https://big.test/"), _seed("small", "https://small.test/")]
    frontier.add_seeds(seeds, NOW)
    frontier.mark(frontier.entry("https://big.test/"), _content("https://big.test/"), None, NOW)
    for i in range(40):
        frontier._add(f"https://big.test/p{i}", "big", "big", 1, 1.0, NOW)
    for i in range(3):
        frontier._add(f"https://new{i}.test/", "big", "big", 1, 0.2, NOW, crossing=True)
    picks = frontier.select(10, seeds, NOW.date())
    hosts = [entry["host"] for entry in picks]
    # One host may take a tenth of a round (at least 2), so its 40 strong links do not fill the round,
    # and the round is left short rather than padded from that host.
    assert hosts.count("big.test") == 2 and len(picks) == 6
    assert {"new0.test", "new1.test", "new2.test", "small.test"} <= set(hosts)


def test_priority_falls_with_what_is_already_held_and_with_a_fading_yield() -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    frontier._add("https://a.test/1", "s", "s", 1, 1.0, NOW)
    entry = frontier.entry("https://a.test/1")
    full = frontier.priority(entry, 1.0)
    assert frontier.priority(entry, 1.0, extra_host=25) == full / 2
    assert frontier.priority(entry, 1.0, extra_line=100) == full / 2
    assert frontier.priority(entry, 3.0) == 3 * full
    frontier.line("s")["yield"] = 0.5
    assert frontier.priority(entry, 1.0) == full / 2


def test_expired_or_removed_seeds_are_not_followed() -> None:
    frontier = Frontier()
    old = _seed("old", "https://old.test/", until=dt.date(2026, 11, 1))
    live = _seed("live", "https://live.test/")
    frontier.add_seeds([old, live, _seed("gone", "https://gone.test/")], NOW)
    assert [entry["url"] for entry in frontier.select(5, [old, live], NOW.date())] == ["https://live.test/"]


def test_host_counts_record_what_fetching_took(tmp_path: Path) -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    rows = [_content(tier="PDF"), _content(), {"outcome": "error_page", "http_status": 403, "block_marker": "http 403"}, DEAD]
    for i, row in enumerate(rows):
        frontier._add(f"https://a.test/{i}", "s", "s", 0, 1.0, NOW)
        frontier.mark(frontier.entry(f"https://a.test/{i}"), row, None, NOW)
    store = LocalStore(tmp_path / "store")
    store.put_many(frontier.save(tmp_path / "w"), "state")
    import pyarrow.parquet as pq
    (host,) = pq.read_table(tmp_path / "store" / "state" / "hosts.parquet").to_pylist()
    assert host == {"host": "a.test", "fetches": 4, "content": 2, "heavy": 1, "refused": 1, "errors": 0, "error_pages": 1}
    assert frontier.line("s")["content"] == 2


def test_a_new_site_with_content_becomes_its_own_line() -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    root = frontier.entry("https://a.test/")
    row = _page_row("https://a.test/", '<a href="https://b.test/x">b</a> <a href="https://b.test/y">b2</a> <a href="https://c.test/">c</a>')
    frontier.mark(root, row, None, NOW)
    frontier.add_links(root, row, None, NOW)
    bx = frontier.entry("https://b.test/x")
    assert bx["line_id"] == "s" and bx["crossing"]             # not a line yet: nothing is known about the site

    frontier.mark(bx, _content("https://b.test/x"), None, NOW)
    assert frontier.spawned == 1 and bx["line_id"] == "s:b.test" and bx["hops"] == 0
    assert frontier.line("s:b.test")["parent_line"] == "s" and frontier.line("s:b.test")["seed_id"] == "s"
    assert frontier.entry("https://b.test/y")["line_id"] == "s:b.test"   # the other queued link to that site moved with it
    assert frontier.line("s")["fetches"] == 2                  # the find is credited to the line that made it

    frontier.mark(frontier.entry("https://c.test/"), DEAD, None, NOW)
    assert frontier.spawned == 1 and frontier.line("s:c.test") is None   # a dead site starts nothing


def test_a_line_that_stops_paying_sinks_but_is_never_skipped_for_it() -> None:
    frontier = Frontier()
    seeds = [_seed("dry", "https://dry.test/"), _seed("ok", "https://ok.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(30):
        frontier._add(f"https://dry.test/{i}", "dry", "dry", 1, 1.0, NOW)
    for i in range(20):
        frontier.mark(frontier.entry(f"https://dry.test/{i}"), DEAD, None, NOW)
    assert frontier.line("dry")["yield"] < 0.05
    assert [entry["host"] for entry in frontier.select(3, seeds, NOW.date())] == ["ok.test", "dry.test", "dry.test"]
    frontier.mark(frontier.take(seeds, NOW.date()), _content(), None, NOW)    # ok.test first
    assert frontier.take(seeds, NOW.date())["host"] == "dry.test"            # then the sunk line, nothing else being left


def test_lines_saved_frozen_load_active_and_save_without_the_old_columns(tmp_path: Path) -> None:
    old = {"line_id": "s", "seed_id": "s", "parent_line": None, "root_url": "https://a.test/", "created_at": NOW, "fetches": 12,
           "content": 0, "since_thaw": 12, "yield": 0.05, "relevance": 0.1, "status": "frozen", "frozen_at": NOW, "inbound": 2}
    frontier = Frontier(lines=[old])
    seeds = [_seed("s", "https://a.test/")]
    frontier._add("https://a.test/1", "s", "s", 1, 1.0, NOW)
    assert frontier.take(seeds, NOW.date())["url"] == "https://a.test/1"
    import pyarrow.parquet as pq
    (local, _), = [f for f in frontier.save(tmp_path) if f[1] == "state/lines.parquet"]
    (row,) = pq.read_table(local).to_pylist()
    assert set(row) == {"line_id", "seed_id", "parent_line", "root_url", "created_at", "fetches", "content", "yield", "relevance", "inbound", "generation"}
    assert row["inbound"] == 2 and row["generation"] == 0


def _two_lines(yields: Dict[str, float]) -> Frontier:
    """A line per seed, each with its root fetched and a few queued pages, at the given yields."""
    frontier = Frontier()
    frontier.add_seeds([_seed(sid, f"https://{sid}.test/") for sid in yields], NOW)
    for sid, value in yields.items():
        frontier.mark(frontier.entry(f"https://{sid}.test/"), _content(), None, NOW)
        for i in range(3):
            frontier._add(f"https://{sid}.test/{i}", sid, sid, 1, 1.0, NOW)
        frontier.line(sid)["yield"] = value
    return frontier


def test_a_better_line_lifts_another_once_per_run_and_never_lowers_it() -> None:
    frontier = _two_lines({"a": 0.2, "b": 0.6, "c": 0.9})

    def fetch(url: str, links: str) -> None:
        entry, row = frontier.entry(url), _page_row(url, links)
        frontier.mark(entry, row, None, NOW)
        frontier.add_links(entry, row, None, NOW)

    # Into a: a fetched page, a queued one, a new one; and the same again. One lift.
    b_yield = 0.85 * 0.6 + 0.15   # b after the page itself is marked
    fetch("https://b.test/0", "".join(f'<a href="https://a.test/{p}">x</a>' for p in ("", "1", "new", "1")))
    assert frontier.line("a")["yield"] == 0.2 + 0.05 * (b_yield - 0.2)
    assert frontier.line("a")["inbound"] == 1 and frontier.lifts == 1
    # Another page of b into a, in the same run: nothing.
    before = frontier.line("a")["yield"]
    fetch("https://b.test/1", '<a href="https://a.test/2">x</a>')
    assert frontier.line("a")["yield"] == before and frontier.lifts == 1
    # Another line's link into a fetched page alone lifts: the host map knows its line.
    fetch("https://c.test/0", '<a href="https://a.test/">x</a>')
    assert frontier.line("a")["yield"] > before and frontier.lifts == 2 and frontier.lines_lifted == {"a"}
    # From the worse line into a better one: nothing.
    b_now = frontier.line("b")["yield"]
    fetch("https://a.test/2", '<a href="https://b.test/2">x</a>')
    assert frontier.line("b")["yield"] == b_now and frontier.line("b")["inbound"] == 0 and frontier.lifts == 2


def test_a_lift_never_takes_a_line_above_its_own_relevance() -> None:
    frontier = _two_lines({"a": 0.2, "b": 0.9, "c": 0.9})
    frontier.line("a")["relevance"] = 0.3
    b = frontier.entry("https://b.test/")
    frontier.add_links(b, _page_row(b["url"], '<a href="https://a.test/0">x</a>'), None, NOW)
    assert frontier.line("a")["yield"] == 0.2 + 0.05 * (0.3 - 0.2)       # toward its relevance, not b's yield
    frontier.line("a")["yield"] = 0.3
    c = frontier.entry("https://c.test/")
    frontier.add_links(c, _page_row(c["url"], '<a href="https://a.test/0">x</a>'), None, NOW)
    assert frontier.line("a")["yield"] == 0.3 and frontier.line("a")["inbound"] == 1 and frontier.lifts == 1   # at it: nothing


def test_a_lift_into_a_sunk_line_ranks_it_up_for_the_next_take() -> None:
    frontier = _two_lines({"a": 0.1, "c": 0.105, "b": 1.0})
    seeds = [_seed(sid, f"https://{sid}.test/") for sid in ("a", "b", "c")]
    for i in range(3):
        frontier.mark(frontier.entry(f"https://b.test/{i}"), _content(), None, NOW)   # b has nothing left queued
    held = frontier.take(seeds, NOW.date())
    assert held["host"] == "c.test"
    frontier.release(held)
    b = frontier.entry("https://b.test/")
    frontier.add_links(b, _page_row(b["url"], '<a href="https://a.test/0">x</a>'), None, NOW)
    assert frontier.line("a")["yield"] > 0.105 and frontier.take(seeds, NOW.date())["host"] == "a.test"


def _retests(rng_seed: str, takes: int = 500) -> tuple:
    """A strong line on 50 hosts and a sunk one on 200 already-fetched hosts that stay dead; each take fetched at once.
    (take numbers that were re-tests, the hosts of re-tests, the hosts of ordinary takes)."""
    good, sunk = _seed("good", "https://g0.test/"), _seed("sunk", "https://s0.test/")
    frontier = Frontier(hosts=[{"host": f"s{i}.test", "fetches": 1} for i in range(200)], rng_seed=rng_seed)
    frontier.add_seeds([good, sunk], NOW)
    frontier.line("sunk")["yield"] = 0.001
    for i in range(1000):
        frontier._add(f"https://g{i % 50}.test/{i}", "good", "good", 1, 1.0, NOW)
    for i in range(200):
        frontier._add(f"https://s{i}.test/{i}", "sunk", "sunk", 1, 1.0, NOW)
    numbers, retest_hosts, ordinary_hosts = [], [], set()
    for n in range(1, takes + 1):
        entry = frontier.take([good, sunk], NOW.date())
        if entry["url"] in frontier.retests:
            numbers.append(n)
            retest_hosts.append(entry["host"])
        else:
            ordinary_hosts.add(entry["host"])
        frontier.mark(entry, DEAD if entry["seed_id"] == "sunk" else _content(), None, NOW)
    return numbers, retest_hosts, ordinary_hosts


def test_every_fiftieth_take_re_tests_a_group_drawn_at_random() -> None:
    numbers, hosts, ordinary = _retests("2026-11-09T20")
    assert numbers == list(range(50, 501, 50))                              # RETEST_SHARE of takes, no more
    assert _retests("2026-11-09T20")[1] == hosts != _retests("2026-11-09T21")[1]   # the seed fixes the draws
    assert not any(host.startswith("s") for host in ordinary)               # the ranking never reaches the sunk line
    assert any(host.startswith("s") for host in hosts)                      # the re-tests do


def test_lines_and_their_state_survive_a_save_and_load(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    root = frontier.entry("https://a.test/")
    row = _page_row("https://a.test/", '<a href="https://b.test/x">b</a>')
    frontier.mark(root, row, None, NOW)
    frontier.add_links(root, row, None, NOW)
    frontier.mark(frontier.entry("https://b.test/x"), _content("https://b.test/x"), None, NOW)
    store.put_many(frontier.save(tmp_path / "w"), "state")
    again = Frontier.load(store, tmp_path / "w2")
    assert again.line("s:b.test")["root_url"] == "https://b.test/x" and again.line("s")["fetches"] == 2
    assert again.count("fetched") == 2
    assert again._host_line == {"a.test": "s", "b.test": "s:b.test"}


def test_a_starting_url_is_retried_on_its_own_record_not_its_seeds(tmp_path: Path) -> None:
    seed = Seed(id="s", entity="s", urls=("https://a.test/", "https://a.test/main"))
    frontier = Frontier()
    frontier.add_seeds([seed], NOW)
    first = frontier.entry("https://a.test/")
    row = _page_row("https://a.test/", '<a href="https://b.test/">b</a>')
    frontier.mark(first, row, None, NOW)
    frontier.add_links(first, row, None, NOW)
    frontier.mark(frontier.entry("https://b.test/"), _content(), None, NOW)          # another site's first page: content
    frontier.mark(frontier.entry("https://a.test/main"), DEAD, None, NOW)
    assert frontier.line("s")["content"] == 2                       # the seed line has content; its main URL does not
    store = LocalStore(tmp_path / "store")
    store.put_many(frontier.save(tmp_path / "w"), "state")
    again = Frontier.load(store, tmp_path / "w2")
    again.add_seeds([seed], NOW)
    assert again.entry("https://a.test/main")["status"] == "queued" and again.entry("https://a.test/")["status"] == "fetched"
    assert again.seed_urls_requeued == 1
    # A URL fetched before the record existed is tried once; served again unchanged, it is left alone after that.
    again.entry("https://a.test/")["stored"] = None
    again.add_seeds([seed], NOW)
    assert again.entry("https://a.test/")["status"] == "queued" and again.seed_urls_requeued == 2
    again.mark(again.entry("https://a.test/"), {**_content(), "outcome": "unchanged"}, None, NOW)
    again.mark(again.entry("https://a.test/main"), {"outcome": "error"}, None, NOW)
    again.add_seeds([seed], NOW)
    assert again.entry("https://a.test/")["status"] == "fetched" and again.entry("https://a.test/main")["status"] == "queued"
    # A blocked page is not served content.
    again.mark(again.entry("https://a.test/main"), {**_content(), "block_marker": "captcha"}, None, NOW)
    again.add_seeds([seed], NOW)
    assert again.entry("https://a.test/main")["status"] == "queued" and again.seed_urls_requeued == 4


class FakeSite:
    def __init__(self, pages: Dict[str, str]) -> None:
        self.pages = pages
        self.fetched: list = []

    async def fetch(self, url: str) -> ScrapeResult:
        self.fetched.append(url)
        if url not in self.pages:
            return ScrapeResult(url, 404, b"<html>Not found</html>", "Not found", 2, "DOM_TRAFILATURA", None, "text/html")
        body = f"<html><body><h1>{url}</h1>{self.pages[url]}<p>{TEXT}</p></body></html>".encode()
        return ScrapeResult(url, 200, body, TEXT, len(TEXT.split()), "DOM_TRAFILATURA", None, "text/html", links=extract_links(body, url))


def test_crawl_follows_links_and_keeps_its_queue(tmp_path: Path) -> None:
    site = FakeSite({
        "https://a.test/": '<a href="/one">one</a> <a href="/two">two</a> <a href="https://b.test/">b</a> <a href="http://[bad">x</a>',
        "https://a.test/one": '<a href="/deep">deep</a>',
        "https://a.test/two": "",
        "https://b.test/": "",
    })
    store = LocalStore(tmp_path / "store")
    seeds = [_seed("a", "https://a.test/"), _seed("dead", "https://dead.test/")]

    def go(run: str, max_fetches: int) -> Dict[str, Any]:
        engine = Engine(store, site, workdir=tmp_path / run, now=lambda: NOW, run_id=run)
        return asyncio.run(crawl(store, engine, seeds, workdir=tmp_path / run, max_fetches=max_fetches, now=lambda: NOW))

    first = go("r1", 2)
    assert first["total"] == 2 and first["links_added"] == 3 and first["queued"] == 3   # the broken link is skipped, not fatal
    assert first["skipped"] == {"unparseable_link": 1, "hop_limit": 0, "host_queue_full": 0, "too_large": 0, "host_cooled": 0}
    assert sorted(site.fetched) == ["https://a.test/", "https://dead.test/"]
    assert first["stored_by_seed"] == {"a": 1, "dead": 0} and first["seeds_with_nothing"] == ["dead"]
    assert first["seed_url_problems"] == {"https://dead.test/": "error_page, status 404: http 404"}
    assert first["seed_urls_requeued"] == 0

    second = go("r2", 10)
    assert second["seeds_added"] == 0 and second["stored"] == 3 and second["error_pages"] == 2   # /deep, and the dead seed again
    assert sorted(set(site.fetched[2:]) - {"https://dead.test/"}) == ["https://a.test/deep", "https://a.test/one", "https://a.test/two", "https://b.test/"]
    assert second["queued"] == 0
    assert second["lines_spawned"] == 1 and second["lines_active"] == 3   # b.test became a line
    assert second["seeds_with_nothing"] == ["dead"]            # still reported in a later run
    assert site.fetched.count("https://dead.test/") == 2 and "https://dead.test/" in second["seed_url_problems"]   # and tried again
    assert second["seed_urls_requeued"] == 1

    frontier = Frontier.load(store, tmp_path / "w3")
    assert frontier.entry("https://a.test/deep")["hops"] == 2 and frontier.count("fetched") == 6
    broken = frontier.entry("http://[bad")
    assert broken["status"] == "unparseable" and broken["parent_fetch_id"] and broken["seed_id"] == "a"   # kept with the page it came from

    import pyarrow.parquet as pq
    rows = {row["url_requested"]: row for path in (tmp_path / "store" / "fetches").rglob("*.parquet") for row in pq.read_table(path).to_pylist()}
    deep = rows["https://a.test/deep"]
    assert deep["seed_id"] == "a" and deep["line_id"] == "a" and deep["hops"] == 2
    assert deep["parent_fetch_id"] == rows["https://a.test/one"]["fetch_id"]        # every row says which fetch led to it


def test_a_crawl_counts_fetches_by_the_generation_of_their_line(tmp_path: Path) -> None:
    site = FakeSite({
        "https://a.test/": '<a href="https://b.test/">b</a>',
        "https://b.test/": '<a href="/x">x</a> <a href="https://c.test/">c</a>',
        "https://b.test/x": "",
        "https://c.test/": "",
    })
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=10, now=lambda: NOW))
    # a.test/ and the find b.test/ count toward the seed line; b.test/x and the find c.test/ toward the line b.test started.
    assert stats["fetches_by_generation"] == {"0": 2, "1": 2, "2": 0, "3": 0, "4-7": 0, "8+": 0}


def test_the_queue_is_uploaded_once_per_run(tmp_path: Path) -> None:
    class CountingStore(LocalStore):
        queue_uploads = 0

        def put_many(self, files: Any, message: str = "") -> None:
            self.queue_uploads += any(remote == "state/frontier.parquet" for _, remote in files)
            super().put_many(files, message)

    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(6)), **{f"https://a.test/p{i}": "" for i in range(6)}})
    store = CountingStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=7, now=lambda: NOW))
    assert stats["total"] == 7 and store.queue_uploads == 1


def test_a_crawl_records_where_its_time_went(tmp_path: Path) -> None:
    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(6)), **{f"https://a.test/p{i}": "" for i in range(6)}})
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=7, now=lambda: NOW))
    seconds = stats["seconds"]
    assert set(seconds) == {"load_state", "take", "fetching", "judge_wait", "final_save", "total"}
    assert all(value >= 0 for value in seconds.values()) and seconds["total"] >= seconds["fetching"]
    assert stats["slot_seconds"]["waiting"] >= 0 and stats["fetch_seconds_by_tier"]["DOM_TRAFILATURA"]["n"] == 7
    assert stats["workers"] == 4 and 1 <= stats["peak_in_flight"] <= 2   # one host: at most MAX_IN_FLIGHT_PER_HOST at once
    assert set(stats["state_bytes"]) >= {"frontier", "hosts", "lines", "url_state"} and all(stats["state_bytes"].values())


def test_crawl_stops_taking_urls_after_max_minutes(tmp_path: Path) -> None:
    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(8)), **{f"https://a.test/p{i}": "" for i in range(8)}})
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    ticks = iter(range(0, 10_000, 60))   # each clock reading is one minute later than the last
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=100,
                              max_minutes=2, now=lambda: NOW, clock=lambda: float(next(ticks))))
    assert stats["stopped_by"] == "max_minutes" and stats["total"] == 1 and stats["queued"] == 8


def test_example_seed_file_loads_with_unique_ids() -> None:
    seeds = load_seeds(Path(__file__).resolve().parent.parent / "examples" / "config" / "seeds.jsonl")
    assert len({seed.id for seed in seeds}) == len(seeds) >= 2
    assert all(url.startswith("https://") for seed in seeds for url in seed.urls)


def test_a_new_line_starts_one_generation_below_its_parent_and_the_step_lasts() -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    root = frontier.entry("https://a.test/")
    row = _page_row("https://a.test/", "<a href=\"https://b.test/x\">b</a> <a href=\"https://b.test/y\">b2</a> <a href=\"https://b.test/z\">b3</a>")
    frontier.mark(root, row, None, NOW)
    frontier.add_links(root, row, None, NOW)
    frontier.line("s")["yield"] = 0.6
    y = frontier.entry("https://b.test/y")
    frontier.mark(frontier.entry("https://b.test/x"), _content("https://b.test/x"), None, NOW)
    parent_after = frontier.line("s")["yield"]
    child = frontier.line("s:b.test")
    assert child["yield"] == parent_after and child["generation"] == 1 and frontier.line("s")["generation"] == 0
    # At the moment of spawning the line's factor is what it was (parent's yield x HOP_DECAY), now carried by the generation.
    assert child["yield"] * GENERATION_DECAY ** child["generation"] == parent_after * HOP_DECAY
    frontier.mark(y, _content("https://b.test/y"), None, NOW)
    assert child["yield"] == 0.85 * parent_after + 0.15             # it moves with its own fetches
    # The step does not wear off: however well the child's own pages pay, its pages rank GENERATION_DECAY below.
    child["yield"] = 1.0
    z = frontier.entry("https://b.test/z")
    held = frontier.priority(z, 1.0)
    child["generation"] = 0
    assert abs(held - frontier.priority(z, 1.0) * GENERATION_DECAY) < 1e-15


def test_lines_saved_without_a_generation_get_it_from_their_parent_chain() -> None:
    def line(line_id: str, parent: Optional[str], fetches: int = 3, value: float = 0.5, day: int = 0, **extra: Any) -> Dict[str, Any]:
        return {"line_id": line_id, "seed_id": "s", "parent_line": parent, "created_at": NOW + dt.timedelta(day),
                "fetches": fetches, "yield": value, **extra}

    chain = [line("c0", None)] + [line(f"c{i}", f"c{i - 1}") for i in range(1, 150)]   # past any recursion limit
    lines = chain + [
        line("orphan", "gone"), line("o1", "orphan"),                                  # a missing parent
        line("x", "z"), line("y", "x"), line("z", "y"), line("w", "z"),                 # a loop, and a line hanging off it
        line("k", None, generation=0), line("k1", "k", generation=4), line("k2", "k1"),  # a saved generation is kept
        # Not fetched since spawning (at parent's yield x HOP_DECAY then): they restart at their parent's yield.
        line("s", None, value=0.8), line("s:b", "s", fetches=0, value=0.56, day=1), line("s:c", "s:b", fetches=0, value=0.392, day=2),
        line("s:d", "s", value=1.0),
    ]
    frontier = Frontier(lines=lines)
    generation = {line_id: frontier.line(line_id)["generation"] for line_id in frontier._lines}
    assert [generation[f"c{i}"] for i in range(150)] == list(range(150))
    assert generation["orphan"] == 0 and generation["o1"] == 1
    assert sorted(generation[i] for i in "xyz") == [0, 1, 2] and generation["w"] == generation["z"] + 1
    assert generation["k1"] == 4 and generation["k2"] == 5
    assert frontier.line("s:b")["yield"] == frontier.line("s:c")["yield"] == 0.8
    assert frontier.line("s:d")["yield"] == 1.0                 # fetched since it started: its yield is its own
    assert frontier.line("c1")["yield"] == 0.5


class _Answers:
    """Stands in for a JevResult: only nouls() is read."""

    def __init__(self, value: float) -> None:
        self.values = {q.id: value for q in judging.active() if q.type == "noul"}

    def nouls(self) -> Dict[str, float]:
        return self.values


def _readable(row: Dict[str, Any]) -> Dict[str, Any]:
    """The row with text Jev would read."""
    return {**row, "text": TEXT, "word_count": len(TEXT.split())}


def test_what_a_fetch_is_worth_to_its_line() -> None:
    assert fetch_yield(DEAD, None) == 0.0
    assert fetch_yield(_content(), None) == 1.0                              # not something Jev reads
    assert fetch_yield(_content(tier="API"), None) == 1.0
    assert fetch_yield(_readable(_content()), None) is None                  # Jev would read it but did not
    answers = _Answers(0.4)
    assert fetch_yield(_readable(_content()), answers) == judging.relevance(answers.nouls())  # type: ignore[arg-type]


def test_a_page_awaiting_judgement_leaves_the_yield_and_one_never_judged_counts_at_the_lines_relevance() -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    line = frontier.line("s")
    line.update({"yield": 0.6, "relevance": 0.8})
    root = frontier.entry("https://a.test/")
    row = _readable(_page_row("https://a.test/", '<a href="https://b.test/x">b</a>'))
    frontier.expect_judgement(row["fetch_id"], "s")
    frontier.mark(root, row, None, NOW)
    frontier.add_links(root, row, None, NOW)
    x = _readable({**_content("https://b.test/x"), "fetch_id": "f-x"})
    frontier.expect_judgement(x["fetch_id"], "s")
    frontier.mark(frontier.entry("https://b.test/x"), x, None, NOW)
    assert line["yield"] == 0.6 and line["fetches"] == 2 and line["content"] == 2
    assert frontier.spawned == 1 and frontier.line("s:b.test")["yield"] == 0.6
    frontier.judged(row["fetch_id"], None)                       # no judgement came: the line's relevance stands in
    assert line["yield"] == 0.85 * 0.6 + 0.15 * 0.8 and line["relevance"] == 0.8
    answers = _Answers(0.4)
    judged = judging.relevance(answers.nouls())  # type: ignore[arg-type]
    frontier.judged(x["fetch_id"], answers)  # type: ignore[arg-type]
    assert line["relevance"] == 0.85 * 0.8 + 0.15 * judged
    # A page Jev will not read, marked at once, counts the same way.
    frontier._add("https://a.test/n", "s", "s", 1, 1.0, NOW)
    before = line["yield"]
    frontier.mark(frontier.entry("https://a.test/n"), _readable(_content("https://a.test/n")), None, NOW)
    assert line["yield"] == 0.85 * before + 0.15 * line["relevance"]


def test_lines_saved_without_a_relevance_start_it_at_their_yield_and_new_lines_at_one() -> None:
    lines = [{"line_id": "s", "seed_id": "s", "parent_line": None, "created_at": NOW, "since_thaw": 5, "yield": 0.4, "status": "active"}]
    frontier = Frontier(lines=lines)
    frontier.add_seeds([_seed("t", "https://t.test/")], NOW)
    assert frontier.line("s")["relevance"] == 0.4 and frontier.line("t")["relevance"] == 1.0


class _Nouls:
    def __init__(self, **values: float) -> None:
        self.values = values

    def nouls(self) -> Dict[str, float]:
        return self.values


def test_under_sampling_a_line_settles_near_its_live_share_times_its_relevance() -> None:
    """A line of which a share of pages is dead and the rest worth r, with one live page in ten judged: its yield must
    settle near (1 - dead share) x r, not be dragged toward the zeros as when unjudged pages were left out."""
    rng = random.Random(5)
    answers = _Nouls(has_specifics=0.8, junk_page=0.1)
    r = judging.relevance(answers.nouls())
    assert r is not None and abs(r - 0.72) < 1e-9
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    line = frontier.line("s")
    seen = []
    for i in range(3000):
        url = f"https://a.test/{i}"
        frontier._add(url, "s", "s", 1, 1.0, NOW)
        if rng.random() < 0.3:
            frontier.mark(frontier.entry(url), DEAD, None, NOW)
        else:
            row = _readable({**_content(url), "fetch_id": url})
            if rng.random() < 0.1:
                frontier.expect_judgement(url, "s")
                frontier.mark(frontier.entry(url), row, None, NOW)
                frontier.judged(url, answers)  # type: ignore[arg-type]
            else:
                frontier.mark(frontier.entry(url), row, None, NOW)
        seen.append(line["yield"])
    settled = sum(seen[1000:]) / len(seen[1000:])
    assert abs(settled - 0.7 * 0.72) < 0.03


# ---------------------------------------------------------------- the ranked queue against scoring every entry

# select() as it was before the queue was grouped, kept verbatim as the reference the grouped version must match.
def reference_select(self, n: int, seeds: Sequence[Seed], today: dt.date) -> List[Dict[str, Any]]:
    """Up to n queued entries for the next round, from active lines, spread across hosts and lines."""
    weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
    queued = [
        entry for entry in self._entries.values()
        if entry["status"] == "queued" and entry["seed_id"] in weights
        and entry["host"] not in self.cooled   # hosts that served nothing COOL_AFTER times in a row in this run
    ]
    if n <= 0 or not queued:
        return []
    host_cap = max(2, math.ceil(n * HOST_ROUND_SHARE))
    picked: List[Dict[str, Any]] = []
    taken: set = set()
    in_host: Dict[str, int] = {}
    in_line: Dict[str, int] = {}

    def take(entry: Dict[str, Any]) -> None:
        picked.append(entry)
        taken.add(entry["url"])
        in_host[entry["host"]] = in_host.get(entry["host"], 0) + 1
        in_line[entry["line_id"]] = in_line.get(entry["line_id"], 0) + 1

    # First, the share kept for hosts never fetched: the best entry of each, best hosts first.
    best_new: Dict[str, Dict[str, Any]] = {}
    for entry in queued:
        if self._hosts.get(entry["host"], {}).get("fetches", 0) == 0:
            current = best_new.get(entry["host"])
            if current is None or self.priority(entry, weights[entry["seed_id"]]) > self.priority(current, weights[current["seed_id"]]):
                best_new[entry["host"]] = entry
    reserve = math.ceil(n * NEW_HOST_SHARE)
    ranked = sorted(best_new.values(), key=lambda e: (-self.priority(e, weights[e["seed_id"]]), e["url"]))
    for entry in ranked[:reserve]:
        take(entry)

    # Then the rest by priority. Each pick lowers the priority of what shares its host or line,
    # so the heap is re-scored lazily: an entry is taken only if its fresh score still leads.
    def score(entry: Dict[str, Any]) -> float:
        return self.priority(entry, weights[entry["seed_id"]], in_host.get(entry["host"], 0), in_line.get(entry["line_id"], 0))

    heap = [(-score(entry), entry["url"]) for entry in queued if entry["url"] not in taken]
    heapq.heapify(heap)
    while heap and len(picked) < n:
        stale, url = heapq.heappop(heap)
        entry = self._entries[url]
        if in_host.get(entry["host"], 0) >= host_cap:
            continue
        fresh = -score(entry)
        if heap and fresh > heap[0][0]:
            heapq.heappush(heap, (fresh, url))
            continue
        take(entry)
    return picked


def reference_bests(frontier: Frontier, seeds: Sequence[Seed], today: dt.date) -> List[tuple]:
    """((-priority, url), entry) of the best waiting entry of each group take() may hand out, by scoring every entry."""
    weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
    out_host = Counter(host for host, _ in frontier._in_flight.values())
    out_line = Counter(line for _, line in frontier._in_flight.values())
    best: Dict[tuple, tuple] = {}
    for e in frontier._entries.values():
        if (e["status"] == "queued" and e["url"] not in frontier._in_flight and e["seed_id"] in weights
                and out_host[e["host"]] < MAX_IN_FLIGHT_PER_HOST and e["host"] not in frontier.cooled):
            rank = (-frontier.priority(e, weights[e["seed_id"]], out_host[e["host"]], out_line[e["line_id"]]), e["url"])
            group = (e["line_id"], e["host"], e["seed_id"])
            if group not in best or rank < best[group][0]:
                best[group] = (rank, e)
    return list(best.values())


def reference_take(frontier: Frontier, seeds: Sequence[Seed], today: dt.date, new_turn: bool) -> Optional[Dict[str, Any]]:
    """take() by scoring every waiting entry: the best of the groups' bests."""
    bests = reference_bests(frontier, seeds, today)
    if new_turn:
        bests = [b for b in bests if frontier._hosts.get(b[1]["host"], {}).get("fetches", 0) == 0] or bests
    return min(bests, key=lambda b: b[0], default=(None, None))[1]


def _random_frontier(rng: random.Random) -> tuple:
    """A loaded queue with few distinct values, so equal priorities are common: hosts with and without fetches,
    crossing entries, lines saved frozen by the old code (loaded active), inactive and removed seeds, weights."""
    today = NOW.date()
    seeds = [
        Seed(id=f"s{i}", entity=f"s{i}", urls=(f"https://h{i}.test/",), weight=rng.choice([1.0, 1.0, 2.5, 0.4, 0.0]),
             until=rng.choice([None, None, today, today - dt.timedelta(days=1)]))
        for i in range(rng.randint(1, 4))
    ]
    seed_ids = [seed.id for seed in seeds] + ["gone"]   # a seed since removed from the config keeps its queue

    def line(line_id: str, seed_id: str, parent: Optional[str]) -> Dict[str, Any]:
        return {
            "line_id": line_id, "seed_id": seed_id, "parent_line": parent, "root_url": "", "created_at": NOW,
            "fetches": rng.choice([0, 0, 1, 2, 3, 25, 100, rng.randint(0, 300)]), "content": rng.choice([0, 1, 5]),
            "since_thaw": rng.choice([0, 3, 12]), "yield": rng.choice([1.0, 0.6, 0.5, 0.3, 0.05, rng.random()]),
            "status": "frozen" if rng.random() < 0.15 else "active", "frozen_at": None, "inbound": rng.choice([0, 1, 2]),
        }

    lines = [line(sid, sid, None) for sid in seed_ids]
    for i in range(rng.randint(0, 8)):   # chains of lines, so generations differ: from the parent chain, saved, or orphaned
        parent = rng.choice(lines)
        lines.append(line(f"{parent['seed_id']}:x{i}", parent["seed_id"], rng.choice([parent["line_id"]] * 4 + ["gone"])))
        if rng.random() < 0.3:
            lines[-1]["generation"] = rng.randint(0, 9)
    hosts = [f"h{i}.test" for i in range(rng.randint(1, 60))]
    host_rows = [
        {"host": host, "fetches": rng.choice([0, 1, 1, 2, 5, 25]), "content": 0, "heavy": 0, "refused": 0, "errors": 0, "error_pages": 0}
        for host in hosts if rng.random() < 0.5
    ]
    entries = []
    for k in range(rng.choice([0, 1, 5, 40, 300, rng.randint(0, 3000)])):
        home = rng.choice(lines)
        host = rng.choice(hosts)
        entries.append({
            "url": f"https://{host}/{k}", "host": host, "seed_id": home["seed_id"] if rng.random() < 0.9 else rng.choice(seed_ids),
            "line_id": home["line_id"], "crossing": rng.random() < 0.3, "hops": rng.randint(0, 6), "parent_fetch_id": "p",
            "anchor": None, "base": rng.choice([0.3, 0.3, 0.49, 0.5, 0.7, 1.0, 0.05, rng.random()]),   # 0.49 at hop 0 and 1.0 at hop 2 differ by one ulp
            "status": rng.choices(["queued", "fetched", "failed", "alias"], [85, 8, 4, 3])[0], "discovered_at": NOW, "fetched_at": None,
        })
    rng.shuffle(entries)   # queued order differs from url order
    return Frontier(entries, host_rows, lines, rng_seed=rng.random()), seeds


def _churn(frontier: Frontier, seeds: List[Seed], rng: random.Random, later: Optional[list] = None) -> None:
    """Fetches of every kind: pages with links (new, known, into other lines, so lifting them), re-test takes,
    crossing finds that spawn lines, dead runs that sink a line, errors, redirects, and seeds that have stored nothing
    being queued again. With `later`, also judgements in two phases, some arriving in a later call, and link scores
    changed directly."""
    if later is not None:
        for fetch_id, answers in [later.pop(rng.randrange(len(later))) for _ in range(rng.randint(0, len(later)))]:
            frontier.judged(fetch_id, answers)
        queued = [e for e in frontier._entries.values() if e["status"] == "queued"]
        for entry in rng.sample(queued, min(3, len(queued))):
            frontier.rebase(entry, rng.choice([0.05, 0.3, 0.49, 1.0, entry["base"]]))
    for _ in range(rng.randint(1, 6)):
        waiting = [e for e in frontier._entries.values() if e["status"] == "queued" and e["url"] not in frontier._in_flight]
        if not waiting:
            return
        entry = rng.choice(waiting)
        kind = rng.random()
        if kind < 0.1:   # the next take is a re-test turn; what it takes is fetched as a page with links
            frontier._takes = round(1 / RETEST_SHARE) - 1
            entry = frontier.take(seeds, NOW.date())
            if entry is None:
                return
            kind = 0.0
        if kind < 0.3:
            known = rng.sample(list(frontier._entries), min(4, len(frontier._entries)))
            others = [e["url"] for e in waiting if e["line_id"] != entry["line_id"]]
            hosts = [e["host"] for e in waiting]
            targets = known + rng.sample(others, min(3, len(others))) + [f"https://{rng.choice(hosts)}/n{rng.getrandbits(24)}" for _ in range(3)]
            targets += [f"https://new{rng.getrandbits(16)}.test/" for _ in range(2)]
            row = _page_row(entry["url"], "".join(f'<a href="{url}">x</a>' for url in targets))
            answers = rng.choice([None, _Answers(rng.choice([0.1, 0.5, 0.9]))])
            if answers is not None:
                row = _readable(row)
            if answers is not None and later is not None and rng.random() < 0.6:
                frontier.expect_judgement(row["fetch_id"], entry["line_id"])
                frontier.mark(entry, row, None, NOW)
                frontier.add_links(entry, row, None, NOW)
                later.append((row["fetch_id"], answers))
                continue
            frontier.mark(entry, row, answers, NOW)  # type: ignore[arg-type]
            frontier.add_links(entry, row, answers, NOW)  # type: ignore[arg-type]
        elif kind < 0.5:
            crossing = [e for e in waiting if e.get("crossing")] or [entry]
            frontier.mark(rng.choice(crossing), _content(), None, NOW)
        elif kind < 0.7:
            dry = [e for e in waiting if e["line_id"] == entry["line_id"]]
            for other in dry[:12]:
                frontier.mark(other, DEAD, None, NOW)
        elif kind < 0.8:
            frontier.mark(entry, {"outcome": "error"}, None, NOW)
        elif kind < 0.9:
            frontier.mark(entry, _content(f"https://{entry['host']}/moved{rng.getrandbits(24)}"), None, NOW)
        else:
            frontier.add_seeds(seeds, NOW)


def _check_counts(frontier: Frontier) -> None:
    statuses = Counter(e["status"] for e in frontier._entries.values())
    for status in ("queued", "fetched", "failed", "alias", "unparseable"):
        assert frontier.count(status) == statuses[status]
    filled = [g for g in frontier._groups.values() if g.members]
    assert sorted(map(id, frontier._filled)) == sorted(map(id, filled)) and all(frontier._filled[g.slot] is g for g in filled)


def test_select_picks_exactly_what_scoring_every_entry_picked() -> None:
    rng = random.Random(20261111)
    today = NOW.date()
    for case in range(60):
        frontier, seeds = _random_frontier(rng)
        later: list = []
        for step in range(8):
            now_seeds = seeds
            if rng.random() < 0.2:   # the config changes between rounds: a seed reweighted or dropped
                now_seeds = [dataclasses.replace(seed, weight=rng.choice([seed.weight, 0.7, 3.0])) for seed in seeds][: rng.randint(1, len(seeds))]
            for n in (0, 1, 4, 10, 50):
                expected = [e["url"] for e in reference_select(frontier, n, now_seeds, today)]
                assert [e["url"] for e in frontier.select(n, now_seeds, today)] == expected, (case, step, n)
            _check_counts(frontier)
            _churn(frontier, seeds, rng, later if case % 2 else None)


def test_take_hands_out_what_scoring_every_entry_would() -> None:
    rng = random.Random(7)
    today = NOW.date()
    for case in range(40):
        frontier, seeds = _random_frontier(rng)
        later: list = []
        out: List[Dict[str, Any]] = []
        for step in range(60):
            roll = rng.random()
            if roll < 0.55:
                expected = reference_take(frontier, seeds, today, (frontier._takes + 1) % round(1 / NEW_HOST_SHARE) == 0)
                drawn = [b[1] for b in reference_bests(frontier, seeds, today)]
                retest = (frontier._takes + 1) % round(1 / RETEST_SHARE) == 0
                taken = frontier.take(seeds, today)
                # A re-test turn takes the best entry of any group it may draw, or falls back to the ranked take.
                assert taken is expected or (retest and any(taken is e for e in drawn)), (case, step)
                if taken is not None:
                    out.append(taken)
            elif out and roll < 0.8:
                frontier.mark(out.pop(rng.randrange(len(out))), rng.choice([_content(), DEAD, {"outcome": "error"}]), None, NOW)
            elif out and roll < 0.9:
                frontier.release(out.pop(rng.randrange(len(out))))
            else:
                _churn(frontier, seeds, rng, later if case % 2 else None)
        _check_counts(frontier)


def test_take_holds_entries_in_flight_until_mark_or_release() -> None:
    frontier = Frontier()
    seeds = [_seed("a", "https://a.test/"), _seed("b", "https://b.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(3):
        frontier._add(f"https://a.test/{i}", "a", "a", 0, 1.0, NOW)
    frontier.line("b")["yield"] = 0.1
    first, second = frontier.take(seeds, NOW.date()), frontier.take(seeds, NOW.date())
    assert {first["host"], second["host"]} == {"a.test"} and first is not second
    third = frontier.take(seeds, NOW.date())
    assert third["host"] == "b.test"                            # a.test has MAX_IN_FLIGHT_PER_HOST out
    assert frontier.take(seeds, NOW.date()) is None             # everything else is a.test, which is full
    frontier.release(second)
    assert frontier.take(seeds, NOW.date()) is second           # put back, it is the best again
    frontier.mark(first, _content(), None, NOW)
    assert frontier.take(seeds, NOW.date())["host"] == "a.test" and frontier.count("queued") == 4   # out, not yet fetched


def test_a_line_whose_yield_rises_is_ranked_up_for_the_next_take() -> None:
    frontier = Frontier()
    seeds = [_seed("a", "https://a.test/"), _seed("b", "https://b.test/")]
    frontier.add_seeds(seeds, NOW)
    for sid in ("a", "b"):
        for i in range(3):
            frontier._add(f"https://{sid}.test/{i}", sid, sid, 1, 1.0, NOW)
    frontier.line("a")["yield"] = 0.3
    frontier.line("b")["yield"] = 0.32
    held = frontier.take(seeds, NOW.date())
    assert held["host"] == "b.test"
    frontier.release(held)
    frontier.mark(frontier.entry("https://a.test/0"), _content(), None, NOW)   # a's yield rises to 0.405
    assert frontier.take(seeds, NOW.date())["host"] == "a.test"


def test_an_entry_that_rounds_to_its_groups_best_score_is_ranked_by_url() -> None:
    # Base 0.49 at hop 0 and base 1.0 at hop 2 differ by one ulp, and at yield 0.14 round to one priority: the url decides.
    lines = [
        {"line_id": sid, "seed_id": sid, "parent_line": None, "created_at": NOW, "fetches": 0, "since_thaw": 5, "yield": 0.14, "status": "active"}
        for sid in ("s", "t")
    ]
    frontier = Frontier(hosts=[{"host": "b.test", "fetches": 1}], lines=lines)
    seeds = [_seed("s", "https://b.test/"), _seed("t", "https://b.test/")]
    frontier._add("https://b.test/z", "s", "s", 0, 0.49, NOW)
    frontier._add("https://b.test/m", "t", "t", 0, 0.49, NOW)
    assert [e["url"] for e in frontier.select(1, seeds, NOW.date())] == ["https://b.test/m"]
    frontier._add("https://b.test/a", "s", "s", 2, 1.0, NOW)   # joins z's group behind it
    assert [e["url"] for e in frontier.select(1, seeds, NOW.date())] == ["https://b.test/a"]
    assert [e["url"] for e in reference_select(frontier, 3, seeds, NOW.date())] == [e["url"] for e in frontier.select(3, seeds, NOW.date())]


# ---------------------------------------------------------------- two phases: fetched now, judged later

def test_judging_in_two_phases_ends_where_one_call_did() -> None:
    """A scripted crawl of one line, each page judged once its fetch is recorded: the same yields and link scores."""
    pages = [(f"https://a.test/{i}", "".join(f'<a href="https://a.test/{i}/{k}">x</a>' for k in range(3)), 0.2 + 0.15 * i) for i in range(5)]
    frontiers = []
    for two_phase in (False, True):
        frontier = Frontier()
        frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
        for url, _, _ in pages:
            frontier._add(url, "s", "s", 1, 1.0, NOW)
        for url, links, value in pages:
            entry, row, answers = frontier.entry(url), _readable(_page_row(url, links)), _Answers(value)
            if two_phase:
                frontier.expect_judgement(row["fetch_id"], entry["line_id"])
                frontier.mark(entry, row, None, NOW)
                frontier.add_links(entry, row, None, NOW)
                assert {frontier.entry(f"{url}/{k}")["base"] for k in range(3)} == {0.3}   # the default until judged
                frontier.judged(row["fetch_id"], answers)  # type: ignore[arg-type]
            else:
                frontier.mark(entry, row, answers, NOW)  # type: ignore[arg-type]
                frontier.add_links(entry, row, answers, NOW)  # type: ignore[arg-type]
        frontiers.append(frontier)
    old, new = frontiers
    assert old._lines == new._lines and old._judging == new._judging == {}
    assert {u: (e["base"], e["status"]) for u, e in old._entries.items()} == {u: (e["base"], e["status"]) for u, e in new._entries.items()}
    assert [e["url"] for e in old.select(10, [_seed("s", "https://a.test/")], NOW.date())] == [
        e["url"] for e in new.select(10, [_seed("s", "https://a.test/")], NOW.date())]


def test_a_judgement_after_its_links_were_fetched_is_harmless() -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    root = frontier.take(seeds, NOW.date())
    row = _readable(_page_row("https://a.test/", '<a href="/x">x</a> <a href="/y">y</a>'))
    frontier.expect_judgement(row["fetch_id"], "s")
    frontier.mark(root, row, None, NOW)
    assert frontier.add_links(root, row, None, NOW) == 2
    x = frontier.take(seeds, NOW.date())
    frontier.mark(x, _content(x["url"]), None, NOW)
    y = frontier.take(seeds, NOW.date())                          # out, not yet fetched
    frontier.judged(row["fetch_id"], _Answers(0.9))  # type: ignore[arg-type]
    assert x["base"] == 0.3 and x["status"] == "fetched"          # already fetched: left as it was
    assert y["base"] != 0.3                                       # in flight: the new score goes back with it
    frontier.release(y)
    assert frontier.take(seeds, NOW.date()) is y and frontier.count("queued") == 1
    frontier.judged(row["fetch_id"], _Answers(0.1))  # type: ignore[arg-type]   # a second answer for it is ignored
    assert y["base"] != link_base(_Answers(0.1))  # type: ignore[arg-type]


def _timed_out(url: str = "") -> Dict[str, Any]:
    return {"outcome": "error", "http_status": 0, "url_final": url, "timed_out": True}


def test_a_timed_out_page_is_retried_once_through_its_own_lane(tmp_path: Path) -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(4):
        frontier._add(f"https://h{i}.test/", "s", "s", 1, 1.0, NOW)
    first = [frontier.take(seeds, NOW.date()) for _ in range(5)]
    for entry in first[:3]:
        frontier.mark(entry, _timed_out(), None, NOW)
    for entry in first[3:]:
        frontier.mark(entry, _content(), None, NOW)
    assert frontier.count("retry") == 3 and frontier.count("queued") == 0
    frontier._add("https://h9.test/", "s", "s", 1, 1.0, NOW)
    retries = [frontier.take(seeds, NOW.date()) for _ in range(3)]
    # At most MAX_RETRY_IN_FLIGHT at once while there is other work; with none left, the rest too.
    assert [e["status"] for e in retries] == ["retry", "retry", "queued"]
    assert frontier.take(seeds, NOW.date())["url"] == first[2]["url"]
    frontier.release(frontier.entry(first[2]["url"]))
    frontier.mark(retries[2], _content(), None, NOW)
    frontier.mark(retries[0], _timed_out(), None, NOW)                  # a second timeout fails it
    frontier.mark(retries[1], _content(), None, NOW)                    # this one recovered
    assert retries[0]["status"] == "failed" and retries[1]["status"] == "fetched"
    store = LocalStore(tmp_path / "store")
    store.put_many(frontier.save(tmp_path / "w"), "state")
    again = Frontier.load(store, tmp_path / "w2")                       # the one left waiting is kept for the next run
    assert again.count("retry") == 1 and again.expire_retries(NOW + dt.timedelta(days=1)) == 0
    left = again.take(seeds, NOW.date())
    assert left["status"] == "retry" and left["url"] == first[2]["url"]
    again.release(left)
    assert again.take(seeds, NOW.date()) is left


def test_a_retry_that_waits_too_long_is_failed() -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    for i, age in enumerate((3, 1)):
        frontier._add(f"https://h{i}.test/", "s", "s", 1, 1.0, NOW)
        frontier.mark(frontier.entry(f"https://h{i}.test/"), _timed_out(), None, NOW - dt.timedelta(days=age))
    assert frontier.expire_retries(NOW) == 1 and frontier.count("retry") == 1 and frontier.count("failed") == 1
    assert frontier.entry("https://h0.test/")["status"] == "failed"
    assert frontier.take_retry(seeds, NOW.date())["url"] == "https://h1.test/" and frontier.take_retry(seeds, NOW.date()) is None


def test_a_host_that_serves_nothing_three_times_in_a_row_is_left_alone_for_the_run() -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(6):
        frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
    for row in ({"outcome": "error"}, {"outcome": "error"}, _content(), {"outcome": "error"}, _timed_out()):
        frontier.mark(frontier.take(seeds, NOW.date()), row, None, NOW)
    assert frontier.cooled == set()                                   # a success in between clears the count
    frontier.mark(frontier.take(seeds, NOW.date()), {"outcome": "error"}, None, NOW)
    frontier._add("https://b.test/", "s", "s", 1, 0.01, NOW)
    assert frontier.cooled == {"a.test"} and frontier.count("queued") == 3   # left queued, not failed (the sixth take was the retry)
    b = frontier.take(seeds, NOW.date())
    assert b["host"] == "b.test" and frontier.take(seeds, NOW.date()) is None
    frontier.mark(b, _content(), None, NOW)
    assert frontier.select(5, seeds, NOW.date()) == []
    queued, top = frontier.by_seed(seeds, NOW.date())
    assert queued == {"s": 2} and top == {}                           # a.test entries queued, none takeable now
