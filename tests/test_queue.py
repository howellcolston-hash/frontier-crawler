"""Checks for the queue on disk (storage/queue.py, and the hot set in frontier.py): the layout across a save and a load,
the move from the older single file, cold groups, the seen set, site factors, the caps, a crash before the final commit,
and the ranking within the hot set. No network."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import random
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.parquet as pq
import pytest

from frontier_crawler import frontier as frontier_module
from frontier_crawler.frontier import MAX_IN_FLIGHT_PER_HOST, NEW_DOMAIN_SHARE, RETEST_SHARE, Frontier, crawl
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage import queue as queue_module
from frontier_crawler.storage.state import UrlState
from frontier_crawler.storage.store import LocalStore
from test_frontier import DEAD, NOW, FakeSite, _churn, _content, _page_row, _random_frontier, _seed, reference_bests, reference_take


def _reload(frontier: Frontier, tmp_path: Path, name: str) -> Frontier:
    store = LocalStore(tmp_path / "store")
    store.put_many(frontier.save(tmp_path / f"{name}-out"), "state")
    return Frontier.load(store, tmp_path / f"{name}-in")


def _queued(frontier: Frontier) -> Dict[str, tuple]:
    return {url: (e["line_id"], e["host"], e["seed_id"], bool(e["crossing"]), e["hops"], e["base"], e["parent_fetch_id"], e["anchor"])
            for url, e in frontier._entries.items() if e["status"] == "queued"}


def _hot(monkeypatch: pytest.MonkeyPatch, entries: int, per_group: int, sample: int = 0) -> None:
    monkeypatch.setattr(frontier_module, "HOT_ENTRIES", entries)
    monkeypatch.setattr(frontier_module, "HOT_PER_GROUP", per_group)
    monkeypatch.setattr(frontier_module, "COLD_SAMPLE", sample)


def _two_sites() -> tuple:
    """A seed line on a.test with 300 pages queued at three depths, 40 crossings, a fetched root, a retry and a broken link."""
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    root = frontier.entry("https://a.test/")
    row = _page_row("https://a.test/", '<a href="http://[bad">x</a>')
    frontier.mark(root, row, None, NOW)
    frontier.add_links(root, row, None, NOW)
    for i in range(300):
        frontier._add(f"https://a.test/{i}", "s", "s", 1 + i % 3, 1.0, NOW, parent="p", anchor=f"page {i}")
    for i in range(40):
        frontier._add(f"https://x{i}.test/", "s", "s", 1, 0.3, NOW, crossing=True)
    frontier._add("https://slow.test/", "s", "s", 1, 1.0, NOW)
    frontier.mark(frontier.entry("https://slow.test/"), {"outcome": "error", "timed_out": True}, None, NOW)
    return frontier, seeds


def test_the_queue_survives_a_save_and_a_load_and_is_read_only_as_far_as_needed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    frontier, seeds = _two_sites()
    before = _queued(frontier)
    again = _reload(frontier, tmp_path, "r1")
    # Nothing queued is in memory before the first take: only the entries kept whole.
    assert {e["status"] for e in again._entries.values()} == {"fetched", "retry", "unparseable"} and again.count("queued") == len(before) == 340
    assert len(again) == len(frontier) and again.count("retry") == 1 and again.entry("http://[bad")["parent_fetch_id"]
    manifest = json.loads((tmp_path / "store" / "state" / "queue" / "manifest.json").read_text())
    assert sum(manifest["rows"]) == 340 and manifest["dropped"] == {}   # the fetched root, the retry and the broken link are kept
    for path in (tmp_path / "store" / "state" / "queue").glob("b*.parquet"):   # each bucket by group, best first within one
        rows = pq.read_table(path).to_pylist()
        keys = [(r["line_id"], r["host"], r["seed_id"], -frontier_module._static(r, 1.0), r["url"]) for r in rows]
        assert keys == sorted(keys)
    again._ready(seeds, NOW.date())
    group = again._groups[("s", "a.test", "s")]
    assert len(group.members) == frontier_module.HOT_PER_GROUP == 64 and group.disk == 300 - 64   # its best 64 only
    assert again.count("queued") == 340 and all(_queued(again)[url] == before[url] for url in _queued(again))
    _hot(monkeypatch, 10_000, 10_000)   # with room for everything, everything comes back as it was
    whole = _reload(frontier, tmp_path, "r2")
    whole._ready(seeds, NOW.date())
    assert _queued(whole) == before and whole._disk_left == 0


def test_a_hot_set_of_four_takes_what_the_whole_queue_in_memory_takes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Four entries loaded at a time: a group's next rows load as its best are taken, other groups as the hot set's best
    falls below theirs. The ranked takes are those of the whole queue in memory (re-test and new-domain turns, which
    draw from what is loaded, are left out)."""
    monkeypatch.setattr(frontier_module, "RETEST_SHARE", 1e-9)
    monkeypatch.setattr(frontier_module, "NEW_DOMAIN_SHARE", 1e-9)
    frontier, seeds = _two_sites()
    orders = []
    for name, (entries, per_group) in (("whole", (10_000, 10_000)), ("hot", (4, 4))):
        _hot(monkeypatch, entries, per_group)
        again = _reload(frontier, tmp_path, name)
        order = []
        while (entry := again.take(seeds, NOW.date())) is not None:
            order.append(entry["url"])
            again.mark(entry, _content(), None, NOW)
        orders.append((order, again.queue["groups_promoted"]))
    (whole, _), (hot, promoted) = orders
    assert hot == whole and len(hot) == 341 and promoted >= 40   # every crossing was loaded from the reserve


def test_the_older_single_file_is_read_once_into_the_new_layout_and_left_in_place(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    frontier, seeds = _two_sites()
    for i in range(3):   # fetched and failed pages, an alias, and a crossing that became a line
        frontier.mark(frontier.entry(f"https://a.test/{i}"), [_content(), DEAD, {"outcome": "error"}][i], None, NOW)
    frontier.mark(frontier.entry("https://x0.test/"), _content("https://www.x0.test/"), None, NOW)
    store = LocalStore(tmp_path / "store")
    out = frontier.save(tmp_path / "w0")
    store.put_many([f for f in out if f[1] in ("state/hosts.parquet", "state/lines.parquet")], "state")
    old = tmp_path / "old.parquet"   # the queue as the older code saved it: every entry, one file, by url
    pq.write_table(queue_module._table([frontier._entries[url] for url in sorted(frontier._entries)]), old)
    store.put_many([(old, "state/frontier.parquet")], "state")
    reference = Frontier(pq.read_table(old).to_pylist(), *(pq.read_table(local).to_pylist() for local, remote in out if remote in ("state/hosts.parquet", "state/lines.parquet")))
    moved = Frontier.load(store, tmp_path / "w1")
    assert moved.queue["migrated"] and moved._host_line == reference._host_line and len(moved) == len(reference)
    for status in ("queued", "fetched", "failed", "alias", "retry", "unparseable"):
        assert moved.count(status) == reference.count(status), status
    again = _reload(moved, tmp_path, "w2")
    assert not again.queue["migrated"] and again.count("queued") == reference.count("queued") and len(again) == len(reference)
    assert (tmp_path / "store" / "state" / "frontier.parquet").read_bytes() == old.read_bytes()   # left as it was
    monkeypatch.setattr(frontier_module, "RETEST_SHARE", 1e-9)   # its draw depends on the order groups were loaded in
    takes: List[List[str]] = [[], [], []]   # and every run takes what the older queue took
    for _ in range(60):
        for f, out_list in zip((reference, moved, again), takes):
            entry = f.take(seeds, NOW.date())
            out_list.append(entry["url"])
            f.mark(entry, DEAD, None, NOW)
    assert takes[0] == takes[1] == takes[2]


def test_a_link_into_a_cold_group_is_appended_unloaded_and_taken_once_the_group_is_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _hot(monkeypatch, 3, 3)
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/"), _seed("t", "https://c.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(3):
        frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
        frontier._add(f"https://c.test/{i}", "t", "t", 4, 0.3, NOW)
    for url in ("https://a.test/", "https://c.test/"):
        frontier.mark(frontier.entry(url), _content(), None, NOW)
    again = _reload(frontier, tmp_path, "r1")
    again._ready(seeds, NOW.date())
    cold = again._cold((("t", "c.test", "t"), False, 3))
    assert again._groups[("s", "a.test", "s")].hot and not cold.hot and not cold.members
    page = again.take(seeds, NOW.date())
    row = _page_row(page["url"], '<a href="https://c.test/new">c</a>')
    again.mark(page, row, None, NOW)
    assert again.add_links(page, row, None, NOW) == 1 and again.queue["entries_appended_cold"] == 1
    assert [e["url"] for e in cold.pending] == ["https://c.test/new"] and again.count("queued") == 2 + 3 + 1
    taken = []
    while (entry := again.take(seeds, NOW.date())) is not None:   # the hot set runs dry: the cold group is loaded, the new link too
        taken.append(entry["url"])
        again.mark(entry, _content(), None, NOW)
    assert "https://c.test/new" in taken and cold.hot and again.queue["groups_promoted"] >= 1 and again.count("queued") == 0
    # Left cold to the end of a run, the appended link is saved with its group.
    again2 = _reload(frontier, tmp_path, "r2")
    again2._ready(seeds, NOW.date())
    page = again2.take(seeds, NOW.date())
    again2.mark(page, row, None, NOW)
    again2.add_links(page, row, None, NOW)
    third = _reload(again2, tmp_path, "r3")
    third._ready([seeds[1]], NOW.date())
    assert "https://c.test/new" in third._entries and third.count("queued") == 2 + 3 + 1


def test_a_fetched_url_is_never_queued_again(tmp_path: Path) -> None:
    frontier, seeds = _two_sites()
    taken = frontier.take(seeds, NOW.date())
    frontier.mark(taken, _content(), None, NOW)
    again = _reload(frontier, tmp_path, "r1")
    known = len(again)
    assert again.entry(taken["url"]) is None and again._known(taken["url"])   # only its key is left
    page = again.take(seeds, NOW.date())
    row = _page_row(page["url"], f'<a href="{taken["url"]}">old</a> <a href="https://a.test/0">queued</a> <a href="https://a.test/fresh">new</a>')
    again.mark(page, row, None, NOW)
    assert again.add_links(page, row, None, NOW) == 1 and len(again) == known + 1


def test_a_site_steered_to_zero_stays_on_disk_and_comes_back_when_raised(tmp_path: Path) -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    for i in range(5):
        frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
        frontier._add(f"https://b.example.com/{i}", "s", "s", 1, 1.0, NOW, crossing=True)
    held = _reload(frontier, tmp_path, "r1")
    held.steer({"example.com": 0.0})
    taken = []
    while (entry := held.take(seeds, NOW.date())) is not None:
        taken.append(entry)
        held.mark(entry, _content(), None, NOW)
    assert not any(e and e["host"] == "b.example.com" for e in taken) and ("s", "b.example.com", "s") not in held._groups   # not even scored
    assert held.count("queued") == 5 and held.queue_record()["entries_cold"] == 5
    raised = _reload(held, tmp_path, "r2")
    raised.steer({"example.com": 2.0})
    entry = raised.take(seeds, NOW.date())
    assert entry["host"] == "b.example.com" and raised.count("queued") == 5


def test_the_caps_cut_the_lowest_ranked_and_count_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(frontier_module, "MAX_QUEUED_PER_GROUP", 5)
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/"), _seed("t", "https://c.test/")]
    frontier.add_seeds(seeds, NOW)
    for url in ("https://a.test/", "https://c.test/"):
        frontier.mark(frontier.entry(url), _content(), None, NOW)
    for i in range(8):
        frontier._add(f"https://a.test/{i}", "s", "s", 1 + i, 1.0, NOW)   # deeper and deeper
    for i in range(4):
        frontier._add(f"https://c.test/{i}", "t", "t", 1, 1.0, NOW)
    frontier.line("t")["yield"] = 0.01
    again = _reload(frontier, tmp_path, "r1")
    assert frontier.queue["cut_group_cap"] == 3 and again.count("queued") == 5 + 4
    again._ready(seeds, NOW.date())
    assert sorted(_queued(again)) == [f"https://a.test/{i}" for i in range(5)] + [f"https://c.test/{i}" for i in range(4)]
    monkeypatch.setattr(frontier_module, "MAX_QUEUED", 7)
    third = _reload(again, tmp_path, "r2")
    third._ready([seeds[0]], NOW.date())   # t inactive: its group is ranked as if active, and is still the lowest
    fourth = _reload(third, tmp_path, "r3")
    assert third.queue["cut_total_cap"] == 4 and fourth.count("queued") == 5
    assert fourth._known("https://c.test/0") and not fourth.add_sitemap("c.test", [("https://c.test/0", "")], NOW)   # cut stays seen


def test_a_run_that_dies_before_its_final_commit_leaves_the_state_before_it(tmp_path: Path) -> None:
    class Failing(LocalStore):
        fail = False

        def put_many(self, files: Any, message: str = "") -> None:
            if self.fail and any(remote == queue_module.MANIFEST_PATH for _, remote in files):
                raise OSError("the store went away")
            super().put_many(files, message)

    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(30)), **{f"https://a.test/p{i}": "" for i in range(30)}})
    store = Failing(tmp_path / "store")
    seeds = [_seed("a", "https://a.test/")]

    def go(run: str, fetches: int) -> Dict[str, Any]:
        engine = Engine(store, site, workdir=tmp_path / run, now=lambda: NOW, run_id=run)
        return asyncio.run(crawl(store, engine, seeds, workdir=tmp_path / run, max_fetches=fetches, now=lambda: NOW))

    go("r1", 5)
    before = Frontier.load(store, tmp_path / "check1")
    store.fail = True
    with pytest.raises(OSError):
        go("r2", 10)
    after = Frontier.load(store, tmp_path / "check2")
    assert (after.count("queued"), after.count("fetched"), len(after)) == (before.count("queued"), before.count("fetched"), len(before)) == (26, 5, 31)
    store.fail = False
    third = go("r3", 10)
    assert third["queued"] == 16 and third["queue"]["seen"] == 31


def test_a_migration_that_dies_is_done_again_by_the_next_run(tmp_path: Path) -> None:
    frontier, seeds = _two_sites()
    store = LocalStore(tmp_path / "store")
    old = tmp_path / "old.parquet"
    pq.write_table(queue_module._table([frontier._entries[url] for url in sorted(frontier._entries)]), old)
    store.put_many([(old, "state/frontier.parquet")], "state")
    first = Frontier.load(store, tmp_path / "w1")
    first.save(tmp_path / "w1-out")   # written, never uploaded
    second = Frontier.load(store, tmp_path / "w2")
    assert first.queue["migrated"] and second.queue["migrated"] and second.count("queued") == first.count("queued") == 340


class _HotView:
    """The frontier as the reference sees it: only entries in memory and ranked (not those waiting in a cold group)."""

    def __init__(self, frontier: Frontier) -> None:
        self._f = frontier
        groups = frontier._groups
        self._entries = {url: e for url, e in frontier._entries.items()
                         if e["status"] != "queued" or getattr(groups.get((e["line_id"], e["host"], e["seed_id"])), "hot", True)}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._f, name)


@pytest.mark.parametrize("mode", ["fixed", "bandit", "shadow"])
def test_within_the_hot_set_take_hands_out_what_scoring_every_entry_in_memory_would(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    """The random queues of the equivalence tests, saved and loaded with a small hot set: fixed and shadow take exactly what
    scoring every ranked entry in memory takes; bandit the best of a group that may take now."""
    monkeypatch.setattr(frontier_module, "MAX_QUEUED_PER_GROUP", 1 << 30)
    rng = random.Random({"fixed": 5, "bandit": 6, "shadow": 7}[mode])
    today = NOW.date()
    for case in range(10):
        _hot(monkeypatch, rng.choice([1, 5, 40, 400]), rng.choice([1, 2, 7]), rng.choice([0, 3]))
        frontier, seeds = _random_frontier(rng)
        steer = dict(frontier._steer)
        frontier = _reload(frontier, tmp_path / str(case), mode)
        frontier.steer(steer)
        frontier.rank(mode, rng.choice([0.0, 1.0]))
        out: List[Dict[str, Any]] = []
        for step in range(60):
            roll = rng.random()
            if roll < 0.55:
                frontier._ready(seeds, today)
                frontier._refill()
                view = _HotView(frontier)
                expected = reference_take(view, seeds, today, (frontier._takes + 1) % round(1 / NEW_DOMAIN_SHARE) == 0)  # type: ignore[arg-type]
                bests = [b[1] for b in reference_bests(view, seeds, today)]  # type: ignore[arg-type]
                retest = (frontier._takes + 1) % round(1 / RETEST_SHARE) == 0
                taken = frontier.take(seeds, today)
                if mode == "bandit":
                    assert (taken is None and not bests) or any(taken is e for e in bests), (case, step)
                else:
                    assert taken is expected or (retest and any(taken is e for e in bests)), (case, step)
                if taken is not None:
                    out.append(taken)
            elif out and roll < 0.8:
                frontier.mark(out.pop(rng.randrange(len(out))), rng.choice([_content(), DEAD, {"outcome": "error"}]), None, NOW)
            elif out and roll < 0.9:
                frontier.release(out.pop(rng.randrange(len(out))))
            else:
                _churn(frontier, seeds, rng)
            assert all(n <= MAX_IN_FLIGHT_PER_HOST for n in frontier._flight_host.values())
        queued = sum(e["status"] == "queued" for e in frontier._entries.values())
        assert frontier.count("queued") == queued + frontier._disk_left
        filled = [g for g in frontier._groups.values() if g.members]
        assert sorted(map(id, frontier._filled)) == sorted(map(id, filled)) and all(not g.pending for g in frontier._groups.values() if g.hot)


def test_groups_left_on_disk_still_get_re_tests_through_the_sample(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _hot(monkeypatch, 10, 10, 20)
    frontier = Frontier(hosts=[{"host": f"s{i}.test", "fetches": 1} for i in range(200)])
    good, sunk = _seed("good", "https://g0.test/"), _seed("sunk", "https://s0.test/")
    frontier.add_seeds([good, sunk], NOW)
    frontier.line("sunk")["yield"] = 0.001
    for i in range(200):
        frontier._add(f"https://g0.test/{i}", "good", "good", 1, 1.0, NOW)
        frontier._add(f"https://s{i}.test/{i}", "sunk", "sunk", 1, 1.0, NOW)
    again = _reload(frontier, tmp_path, "r1")
    hosts = []
    for _ in range(350):
        entry = again.take([good, sunk], NOW.date())
        if entry["url"] in again.retests:
            hosts.append(entry["host"])
        again.mark(entry, DEAD if entry["seed_id"] == "sunk" else _content(), None, NOW)
    assert again.queue["groups_promoted"] >= 20 and len(hosts) >= 5 and sum(h.startswith("s") for h in hosts) >= len(hosts) - 1   # most groups are sunk ones on disk


def test_a_crossing_left_on_disk_moves_to_its_sites_line_when_loaded(tmp_path: Path) -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/"), _seed("t", "https://t.test/")], NOW)
    frontier._add("https://b.test/1", "s", "s", 1, 0.3, NOW, crossing=True)
    frontier._add("https://b.test/2", "t", "t", 1, 0.3, NOW, crossing=True)
    again = _reload(frontier, tmp_path, "r1")
    again._heat([(again._cold((("s", "b.test", "s"), True, 1)), 1)])
    again.mark(again.entry("https://b.test/1"), _content(), None, NOW + dt.timedelta(hours=1))   # b.test becomes a line
    assert again.line("s:b.test") is not None and not again._cold((("t", "b.test", "t"), True, 1)).hot   # the other one is on disk
    third = _reload(again, tmp_path, "r2")
    third._heat([(third._cold((("t", "b.test", "t"), True, 1)), 1)])
    moved = third.entry("https://b.test/2")
    assert (moved["line_id"], moved["seed_id"], moved["hops"], moved["crossing"]) == ("s:b.test", "t", 1, False)


def test_the_url_state_is_read_once_from_the_older_file_and_kept_as_keys(tmp_path: Path) -> None:
    import pyarrow as pa

    store = LocalStore(tmp_path / "store")
    digest = hashlib.sha256(b"page").hexdigest()
    (tmp_path / "store" / "state").mkdir(parents=True)
    pq.write_table(pa.table({"url": ["https://a.test/"], "sha256": [digest], "last_fetched": pa.array([NOW], pa.timestamp("us", tz="UTC")),
                             "http_status": pa.array([200], pa.int32())}), tmp_path / "store" / "state" / "url_state.parquet")
    state = UrlState.load(store, tmp_path / "w1")
    assert state.same("https://a.test/", digest) and not state.same("https://b.test/", digest)
    store.put_many([state.save(tmp_path / "w1")], "state")
    again = UrlState.load(store, tmp_path / "w2")
    assert again.same("https://a.test/", digest) and (tmp_path / "store" / "state" / "url_state.parquet").exists()
