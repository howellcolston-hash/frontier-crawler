"""Checks for the ranking modes (config/ranking.json): the bandit rule against the fixed one on a synthetic web, shadow
mode, the safeguards in bandit mode, and arm state across saves and modes. No network."""

from __future__ import annotations

import asyncio
import collections
import datetime as dt
import random
from pathlib import Path
from typing import Any, Dict, List, Tuple

from frontier_crawler import frontier as frontier_module
from frontier_crawler.frontier import ARM_PRIOR, MAX_IN_FLIGHT_PER_HOST, Frontier, Seed, crawl
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.store import LocalStore
from test_frontier import (
    DEAD, NOW, FakeSite, _check_counts, _churn, _content, _random_frontier, _seed, reference_bests, reference_take,
)
from frontier_crawler.frontier import NEW_DOMAIN_SHARE, RETEST_SHARE

PAGES = 400   # per site; page i links to pages 3i+1..3i+3 of its site, so MAX_HOPS reaches most of them
DRY = 15      # pages a drying site pays for before it serves nothing of value


class _Nouls:
    def __init__(self, **values: float) -> None:
        self.values = values

    def nouls(self) -> Dict[str, float]:
        return self.values


class _Web:
    """Sites with hidden reward rates. Every page links to three random pages of the poor hubs, so the links favour them;
    the rich sites are three sites from a seed (seed -> l1 -> l2 -> rich) and linked only from l2; the drying sites pay
    well for DRY pages, then nothing. 10% of pages are dead. The link score carries no hint of where a link leads."""

    def __init__(self, seed: int) -> None:
        rng = random.Random(seed)
        self.seed, self.kind, self.rate, self.out = seed, {}, {}, {}
        by: Dict[str, List[str]] = collections.defaultdict(list)
        for kind, count, rate in (("seed", 3, 0.3), ("hub", 20, 0.05), ("l1", 20, 0.2), ("l2", 20, 0.25), ("rich", 8, 0.85), ("dry", 8, 0.9)):
            for i in range(count):
                host = f"{kind}{i}.test"
                self.kind[host], self.rate[host] = kind, rate
                by[kind].append(host)
        links_to = {"seed": (("l1", 7), ("dry", 3)), "hub": (("hub", 3), ("l1", 2)), "l1": (("l2", 2), ("dry", 1)),
                    "l2": (("rich", 1), ("l1", 1)), "rich": (("l2", 1),), "dry": (("l2", 1),)}
        for host, kind in self.kind.items():
            self.out[host] = [other for target, k in links_to[kind] for other in rng.sample(by[target], k)]
        self.hubs = by["hub"]
        self.fetched: collections.Counter = collections.Counter()

    def fetch(self, url: str) -> Tuple[Dict[str, Any], Any, float]:
        host, page = url.split("/")[2], int(url.rsplit("/", 1)[1])
        rng = random.Random(f"{self.seed}:{url}")
        self.fetched[host] += 1
        if rng.random() < 0.1:
            return DEAD, None, 0.0
        rate = 0.0 if self.kind[host] == "dry" and self.fetched[host] > DRY else self.rate[host]
        reward = rng.uniform(0.6, 1.0) if rng.random() < rate else rng.uniform(0.0, 0.1)
        links = [f"https://{host}/{c}" for c in range(3 * page + 1, 3 * page + 4) if c < PAGES]
        links += [f"https://{rng.choice(self.hubs)}/{rng.randrange(PAGES)}" for _ in range(3)]
        links += [f"https://{rng.choice(self.out[host])}/{rng.randrange(PAGES)}"]
        row = {**_content(url), "fetch_id": "f-" + url, "content_type": "text/html", "links": ([(u, "x") for u in links], [])}
        return row, _Nouls(has_specifics=reward, junk_page=0.0, leads_to_records=rng.random()), reward


def _simulate(mode: str, temperature: float = 1.0, seed: int = 1, budget: int = 1500) -> Dict[str, Any]:
    """Crawl the web with up to four pages in flight for a fixed number of takes, each page judged as it is marked."""
    web = _Web(seed)
    frontier = Frontier(rng_seed=seed)
    frontier.rank(mode, temperature)
    seeds = [Seed(id=h, entity=h, urls=(f"https://{h}/0",)) for h, kind in web.kind.items() if kind == "seed"]
    frontier.add_seeds(seeds, NOW)
    out: collections.deque = collections.deque()
    order, total, taken = [], 0.0, 0
    while True:
        entry = frontier.take(seeds, NOW.date()) if taken < budget else None
        if entry is not None:
            out.append(entry)
            order.append(entry["url"])
            taken += 1
        if not out:
            break
        if entry is None or len(out) >= 4:
            done = out.popleft()
            row, answers, reward = web.fetch(done["url"])
            frontier.mark(done, row, answers, NOW)
            frontier.add_links(done, row, answers, NOW)
            total += reward
    fetches = collections.Counter()
    for host, n in web.fetched.items():
        fetches[web.kind[host]] += n
    return {
        "total": total, "fetches": fetches, "order": order, "frontier": frontier,
        "rich_found": sum(1 for host, kind in web.kind.items() if kind == "rich" and web.fetched[host]),
        "after_dry": sum(max(0, web.fetched[host] - DRY) for host, kind in web.kind.items() if kind == "dry") / 8,
    }


def test_the_bandit_rule_collects_more_than_the_fixed_one_on_a_synthetic_web() -> None:
    for seed in (1, 2, 3):
        fixed, bandit, greedy = _simulate("fixed", seed=seed), _simulate("bandit", 1.0, seed=seed), _simulate("bandit", 0.0, seed=seed)
        print(seed, {name: (round(r["total"], 1), dict(r["fetches"]), r["rich_found"], r["after_dry"], r["frontier"].ranking_record()["exploration_takes"],
                            len(r["frontier"].tried)) for name, r in (("fixed", fixed), ("bandit", bandit), ("greedy", greedy))})
        assert bandit["total"] > 1.25 * fixed["total"], seed
        # It finds the far rich sites and stays on them; the near, well-linked poor hubs get far less than under fixed.
        assert bandit["rich_found"] >= 5 and bandit["fetches"]["rich"] > 1.5 * fixed["fetches"]["rich"], seed
        assert bandit["fetches"]["hub"] < 0.6 * fixed["fetches"]["hub"], seed
        # It leaves a site that dries up: a few pages each once its DRY paying pages are gone, of the 385 left.
        assert bandit["after_dry"] < 15, seed
        # Temperature 0 is greedy on the estimates: no exploration takes, fewer arms tried.
        record, greedy_record = bandit["frontier"].ranking_record(), greedy["frontier"].ranking_record()
        assert greedy_record["exploration_takes"] == 0 < record["exploration_takes"]
        assert greedy_record["arms_tried"] < record["arms_tried"]


def test_shadow_mode_takes_exactly_what_fixed_takes_and_records_the_bandit_choice() -> None:
    fixed, shadow = _simulate("fixed", seed=4, budget=600), _simulate("shadow", seed=4, budget=600)
    assert shadow["order"] == fixed["order"] and shadow["total"] == fixed["total"]
    record = shadow["frontier"].ranking_record()
    assert record["shadow_takes"] == 600 and 0 < record["shadow_agreed"] < 600
    assert record["exploration_takes"] == 0 and record["mean_reward_greedy"] is None   # it never takes for itself
    rewarded = shadow["frontier"].rewards
    assert rewarded["agreed"][0] + rewarded["differed"][0] == 600 and record["shadow_mean_reward_differed"] is not None
    assert 0 < record["shadow_mean_estimate_fixed"] <= 1 and 0 < record["shadow_mean_estimate_bandit"] <= 1


def test_the_bandit_rule_keeps_the_safeguards() -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/"), _seed("t", "https://b.example.com/"), _seed("off", "https://off.test/", weight=0.0)]
    frontier.add_seeds(seeds, NOW)
    for i in range(6):
        frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
        frontier._add(f"https://b.example.com/{i}", "t", "t", 1, 1.0, NOW)
    frontier.rank("bandit")
    frontier.steer({"example.com": 0.0})
    frontier.mark(frontier.entry("https://a.test/5"), {"outcome": "error", "timed_out": True}, None, NOW)   # waits for its retry
    first = frontier.take(seeds, NOW.date())
    assert first["status"] == "retry"                                         # the retry lane goes first
    taken = [frontier.take(seeds, NOW.date()) for _ in range(3)]
    assert [e and e["host"] for e in taken] == ["a.test", None, None]         # two in flight per host; b steered to 0; off inactive
    for entry in [first] + taken[:1]:
        frontier.mark(entry, DEAD, None, NOW)
    for _ in range(3):
        frontier.mark(frontier.take(seeds, NOW.date()), {"outcome": "error"}, None, NOW)
    assert frontier.cooled == {"a.test"} and frontier.take(seeds, NOW.date()) is None   # three errors in a row: left alone
    assert frontier.count("queued") == 2 + 7 + 1 and len(frontier._in_flight) == 0   # a.test keeps its other two; b and off all theirs


def test_temperature_scales_how_far_a_draw_strays_from_the_estimate() -> None:
    frontier = Frontier()
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    group = frontier._groups[("s", "a.test", "s")]
    frontier.line("s").update(arm_alpha=2.0, arm_beta=2.0)
    spreads = []
    for temperature in (0.0, 0.5, 1.0):
        frontier.rank("bandit", temperature)
        frontier.take_retry(seeds, NOW.date())   # builds the heaps
        samples = []
        for _ in range(400):
            frontier._draw(group)
            samples.append(next(-score for score, version, _ in frontier._arm_heap if version == group.drawn))
        mean = frontier._arm("s", group.members[0], "a.test")[0]
        spreads.append(max(abs(s - mean) for s in samples))
    assert spreads[0] == 0.0 < spreads[1] < spreads[2]


def test_staleness_widens_an_arm_but_never_moves_its_estimate() -> None:
    frontier = Frontier()
    frontier.add_seeds([_seed("s", "https://a.test/")], NOW)
    for i in range(30):
        frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
        frontier.mark(frontier.entry(f"https://a.test/{i}"), DEAD, None, NOW)   # a line that sank
    first = frontier.entry("https://a.test/")
    widths = []
    for days in (0, 14, 21, 365):
        frontier._today = (NOW + dt.timedelta(days=days)).date()
        widths.append(frontier._arm("s", first, "a.test"))
    means = {round(mean, 12) for mean, _ in widths}
    evidence = [n for _, n in widths]
    assert len(means) == 1 and means.pop() < 0.01                             # a sunk line recovers only through data
    assert abs(evidence[1] - evidence[0] / 2) < 1e-9 and evidence[0] > evidence[1] > evidence[2] > evidence[3] == ARM_PRIOR


def test_arms_follow_every_mode_and_survive_a_save_so_switching_loses_nothing(tmp_path: Path) -> None:
    lines = []
    for mode in ("fixed", "bandit", "shadow"):
        frontier = Frontier()
        frontier.rank(mode)
        seeds = [_seed("s", "https://a.test/")]
        frontier.add_seeds(seeds, NOW)
        for i, row in enumerate((_content(), DEAD, _content(), _content())):
            frontier._add(f"https://a.test/{i}", "s", "s", 1, 1.0, NOW)
            frontier.mark(frontier.entry(f"https://a.test/{i}"), row, _Nouls(has_specifics=0.25 * i, junk_page=0.0) if i else None, NOW)  # type: ignore[arg-type]
        lines.append(frontier._lines)
    assert lines[0] == lines[1] == lines[2]       # the same rewards move the same estimates, whatever the mode
    store = LocalStore(tmp_path / "store")
    store.put_many(frontier.save(tmp_path / "w"), "state")
    again = Frontier.load(store, tmp_path / "w2")
    assert again._lines == lines[2]


def test_a_new_site_starts_from_its_finder_and_the_link_that_found_it() -> None:
    frontier = Frontier()
    frontier.rank("bandit")
    seeds = [_seed("s", "https://a.test/")]
    frontier.add_seeds(seeds, NOW)
    line = frontier.line("s")
    line.update(arm_alpha=3.0, arm_beta=1.0)   # estimate 0.75
    frontier._add("https://b.test/", "s", "s", 1, 0.4, NOW, crossing=True)
    frontier._add("https://c.test/", "s", "s", 1, 0.4, NOW, crossing=True)
    frontier._hosts["c.test"] = dict.fromkeys(("fetches", "content", "heavy", "refused", "errors", "error_pages"), 0)
    frontier._hosts["c.test"]["fetches"] = 3   # tried three times, and it started nothing
    b, c = frontier.entry("https://b.test/"), frontier.entry("https://c.test/")
    mean, n = frontier._arm("s", b, "b.test")
    assert abs(mean - 0.75 * 0.4) < 1e-12 and n == ARM_PRIOR                              # no hop or generation factor
    mean, n = frontier._arm("s", c, "c.test")
    assert n == ARM_PRIOR + 3 and abs(mean - 0.3 * ARM_PRIOR / (ARM_PRIOR + 3)) < 1e-12
    frontier.mark(b, _content(), _Nouls(has_specifics=1.0, junk_page=0.0), NOW)  # type: ignore[arg-type]
    child = frontier.line("s:b.test")
    keep = 1 - 1 / frontier_module.ARM_WINDOW
    assert abs(child["arm_alpha"] - (0.75 * 0.4 * ARM_PRIOR * keep + 1)) < 1e-12 and abs(child["arm_beta"] - (1 - 0.75 * 0.4) * ARM_PRIOR * keep) < 1e-12


def test_a_crawl_reads_the_ranking_mode_and_records_its_figures(tmp_path: Path) -> None:
    site = FakeSite({"https://a.test/": "".join(f'<a href="/p{i}">p</a>' for i in range(6)), **{f"https://a.test/p{i}": "" for i in range(6)}})
    store = LocalStore(tmp_path / "store")
    engine = Engine(store, site, workdir=tmp_path / "w", now=lambda: NOW, run_id="r1")
    stats = asyncio.run(crawl(store, engine, [_seed("a", "https://a.test/")], workdir=tmp_path / "w", max_fetches=7, now=lambda: NOW,
                              ranking=("bandit", 0.5)))
    ranking = stats["ranking"]
    assert stats["total"] == 7 and ranking["mode"] == "bandit" and ranking["temperature"] == 0.5
    assert ranking["greedy_takes"] + ranking["exploration_takes"] == 7 and ranking["arms_tried"] >= 1


def test_shadow_takes_what_scoring_every_entry_would_and_bandit_only_what_may_be_taken() -> None:
    """The random queues of the fixed rule's equivalence test, under shadow and bandit mode: shadow hands out exactly
    the fixed take; bandit hands out only the best entry of a group that may take now; the counts stay right."""
    rng = random.Random(11)
    today = NOW.date()
    for case in range(40):
        frontier, seeds = _random_frontier(rng)
        mode = "shadow" if case % 2 else "bandit"
        frontier.rank(mode, rng.choice([0.0, 0.5, 1.0]))
        later: list = []
        out: List[Dict[str, Any]] = []
        for step in range(60):
            roll = rng.random()
            if roll < 0.55:
                expected = reference_take(frontier, seeds, today, (frontier._takes + 1) % round(1 / NEW_DOMAIN_SHARE) == 0)
                bests = [b[1] for b in reference_bests(frontier, seeds, today)]
                retest = (frontier._takes + 1) % round(1 / RETEST_SHARE) == 0
                retrying = bool(frontier._retry)
                taken = frontier.take(seeds, today)
                if mode == "shadow":
                    assert taken is expected or (retest and any(taken is e for e in bests)) or (retrying and taken["status"] == "retry"), (case, step)
                elif taken is None:
                    assert not bests, (case, step)                        # it never idles while something may be taken
                elif taken["status"] != "retry":
                    assert any(taken is e for e in bests), (case, step)   # an eligible group's best, its host under the limit
                if taken is not None:
                    out.append(taken)
            elif out and roll < 0.8:
                frontier.mark(out.pop(rng.randrange(len(out))), rng.choice([_content(), DEAD, {"outcome": "error"}]), None, NOW)
            elif out and roll < 0.9:
                frontier.release(out.pop(rng.randrange(len(out))))
            else:
                _churn(frontier, seeds, rng, later if case % 4 > 1 else None)
            assert all(n <= MAX_IN_FLIGHT_PER_HOST for n in frontier._flight_host.values())
        _check_counts(frontier)
