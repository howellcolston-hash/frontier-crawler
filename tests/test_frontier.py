"""Checks for the crawl queue: link extraction, how a round is chosen, and a crawl against a fake site. No network."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any, Dict

from frontier_crawler.frontier import Frontier, Seed, crawl, extract_links, load_seeds, normalise
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
    return {**_content(url), "fetch_id": "f-" + url, "content_type": "text/html", "body": f"<html><body>{links}</body></html>".encode()}


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


def test_a_line_that_stops_paying_freezes_and_keeps_its_queue() -> None:
    frontier = Frontier()
    seeds = [_seed("dry", "https://dry.test/"), _seed("ok", "https://ok.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(30):
        frontier._add(f"https://dry.test/{i}", "dry", "dry", 1, 1.0, NOW)
    for i in range(20):
        frontier.mark(frontier.entry(f"https://dry.test/{i}"), DEAD, None, NOW)
        if frontier.line("dry")["status"] == "frozen":
            break
    line = frontier.line("dry")
    assert line["status"] == "frozen" and 10 <= line["fetches"] <= 16 and frontier.frozen == 1
    assert frontier.count("queued") > 10                       # the dry line's queue is still there
    assert [entry["host"] for entry in frontier.select(10, seeds, NOW.date())] == ["ok.test"]


def test_a_frozen_line_thaws_when_other_lines_keep_linking_into_it() -> None:
    frontier = Frontier()
    seeds = [_seed("dry", "https://dry.test/"), _seed("ok", "https://ok.test/")]
    frontier.add_seeds(seeds, NOW)
    frontier.line("dry").update({"status": "frozen", "yield": 0.05})
    ok = frontier.entry("https://ok.test/")
    links = "".join(f'<a href="https://dry.test/report{i}">r</a>' for i in range(3))
    row = _page_row("https://ok.test/", links)
    frontier.mark(ok, row, None, NOW)
    assert frontier.add_links(ok, row, None, NOW) == 3
    line = frontier.line("dry")
    assert line["status"] == "active" and line["yield"] == 0.5 and frontier.thawed == 1
    assert frontier.entry("https://dry.test/report0")["line_id"] == "dry"


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


class FakeSite:
    def __init__(self, pages: Dict[str, str]) -> None:
        self.pages = pages
        self.fetched: list = []

    async def fetch(self, url: str) -> ScrapeResult:
        self.fetched.append(url)
        if url not in self.pages:
            return ScrapeResult(url, 404, b"<html>Not found</html>", "Not found", 2, "DOM_TRAFILATURA", None, "text/html")
        body = f"<html><body><h1>{url}</h1>{self.pages[url]}<p>{TEXT}</p></body></html>".encode()
        return ScrapeResult(url, 200, body, TEXT, len(TEXT.split()), "DOM_TRAFILATURA", None, "text/html")


def test_crawl_follows_links_round_by_round_and_keeps_its_queue(tmp_path: Path) -> None:
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
        return asyncio.run(crawl(store, engine, seeds, workdir=tmp_path / run, max_fetches=max_fetches, batch=2, now=lambda: NOW))

    first = go("r1", 2)
    assert first["total"] == 2 and first["links_added"] == 3 and first["queued"] == 3   # the broken link is skipped, not fatal
    assert first["skipped"] == {"unparseable_link": 1, "hop_limit": 0, "host_queue_full": 0, "too_large": 0}
    assert sorted(site.fetched) == ["https://a.test/", "https://dead.test/"]
    assert first["stored_by_seed"] == {"a": 1, "dead": 0} and first["seeds_with_nothing"] == ["dead"]
    assert first["seed_url_problems"] == {"https://dead.test/": "error_page, status 404: http 404"}

    second = go("r2", 10)
    assert second["seeds_added"] == 0 and second["stored"] == 3 and second["error_pages"] == 2   # /deep, and the dead seed again
    assert sorted(set(site.fetched[2:]) - {"https://dead.test/"}) == ["https://a.test/deep", "https://a.test/one", "https://a.test/two", "https://b.test/"]
    assert second["queued"] == 0 and second["rounds"] >= 2
    assert second["lines_spawned"] == 1 and second["lines_frozen"] == 0   # b.test became a line
    assert second["seeds_with_nothing"] == ["dead"]            # still reported in a later run
    assert site.fetched.count("https://dead.test/") == 2 and "https://dead.test/" in second["seed_url_problems"]   # and tried again

    frontier = Frontier.load(store, tmp_path / "w3")
    assert frontier.entry("https://a.test/deep")["hops"] == 2 and frontier.count("fetched") == 6
    broken = frontier.entry("http://[bad")
    assert broken["status"] == "unparseable" and broken["parent_fetch_id"] and broken["seed_id"] == "a"   # kept with the page it came from

    import pyarrow.parquet as pq
    rows = {row["url_requested"]: row for path in (tmp_path / "store" / "fetches").rglob("*.parquet") for row in pq.read_table(path).to_pylist()}
    deep = rows["https://a.test/deep"]
    assert deep["seed_id"] == "a" and deep["line_id"] == "a" and deep["hops"] == 2
    assert deep["parent_fetch_id"] == rows["https://a.test/one"]["fetch_id"]        # every row says which fetch led to it


def test_the_queue_is_uploaded_once_per_run(tmp_path: Path) -> None:
    class CountingStore(LocalStore):
        queue_uploads = 0

        def put_many(self, files: Any, message: str = "") -> None:
            self.queue_uploads += any(remote == "state/frontier.parquet" for _, remote in files)
            super().put_many(files, message)

    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(6)), **{f"https://a.test/p{i}": "" for i in range(6)}})
    store = CountingStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=7, batch=2, now=lambda: NOW))
    assert stats["rounds"] >= 3 and stats["total"] == 7 and store.queue_uploads == 1
    seconds = stats["seconds"]
    assert set(seconds) == {"load_state", "select", "fetch_rounds", "final_save", "total"}
    assert all(value >= 0 for value in seconds.values()) and seconds["total"] >= seconds["fetch_rounds"]
    assert stats["slot_seconds"]["idle"] >= 0 and stats["fetch_seconds_by_tier"]["DOM_TRAFILATURA"]["n"] == 7
    assert set(stats["state_bytes"]) >= {"frontier", "hosts", "lines", "url_state"} and all(stats["state_bytes"].values())


def test_crawl_stops_starting_rounds_after_max_minutes(tmp_path: Path) -> None:
    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(8)), **{f"https://a.test/p{i}": "" for i in range(8)}})
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    ticks = iter(range(0, 10_000, 60))   # each clock reading is one minute later than the last
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=100, batch=2,
                              max_minutes=2, now=lambda: NOW, clock=lambda: float(next(ticks))))
    assert stats["stopped_by"] == "max_minutes" and stats["rounds"] == 1 and stats["total"] == 1 and stats["queued"] == 8


def test_example_seed_file_loads_with_unique_ids() -> None:
    seeds = load_seeds(Path(__file__).resolve().parent.parent / "examples" / "config" / "seeds.jsonl")
    assert len({seed.id for seed in seeds}) == len(seeds) >= 2
    assert all(url.startswith("https://") for seed in seeds for url in seed.urls)
