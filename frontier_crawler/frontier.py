"""The crawl queue: what to fetch next, how the crawl branches, and when a branch stops.

Seeds (config/seeds.jsonl in the store) are starting points, not permanent tracks. The crawl is
organised into lines. A line is one direction of research: it starts at a seed, or at a
site that another line reached and found content on. Lines multiply as the crawl finds
new sites, each with its own counters, and each one ends when it stops paying.

Branching. A link to a site that no line covers (a crossing) is queued under the line that found
it. If fetching it yields content, that site becomes a new line and the parent line carries on.
A line's record (fetches, content, yield) is its own pages: that first fetch is the new line's,
and a crossing that yields nothing counts toward no line, only its host. What a page is worth as
a route to other sites lives in its links' base, not in its line's yield. A new line is one
generation below the line that found it (a seed line is generation 0). It starts at its parent's
yield and earns its own yield from there; the generation factor in priority keeps the step down
for good, so a line many sites from its seed stays discounted
however well its own pages pay.

Sinking and recovery. Every line keeps a running yield: Jev's relevance for new content it judged, 0 for
a dead link, a duplicate, an unchanged page, a refusal or a thin page (HTML with under THIN_WORDS words:
an outage notice, an empty shell). New content Jev does not judge counts at
the line's relevance: the running mean of the relevance of its judged pages. That covers content Jev
does not read (structured data, a scan with no text) and pages Jev would read but
does not (out of the sample, no key, the ceiling, an outage, a judgement abandoned at the end of a
run). Leaving them out instead would let the zeros outweigh the judged pages by one over the sample
rate. A seed line's relevance starts at 1, a spawned line's at its parent's. A page whose judgement
is still coming leaves the yield until it arrives. Priority carries the yield, so a line that
stops paying sinks; nothing is ever skipped for a low yield alone. A sunk line recovers through
data, never through time: a page of another line that links into it moves its yield INBOUND_LIFT
of the way toward that line's yield, capped at its own relevance (only upward, and only on the
first link from that line into it in a run), and every 1/RETEST_SHARE-th take() fetches the best
entry of a group drawn at random, so cold groups are tried again (the draw ignores priority,
generation included). Growth is bounded by the fetches per run, so lines compete for a fixed capacity.

    priority = seed weight x site factor x base x HOP_DECAY^hops x line yield x GENERATION_DECAY^generation
               / (1 + fetched from this host / HOST_HALF)
               / (1 + fetched from this registered domain, all its hosts / DOMAIN_HALF)
               / (1 + fetched in this line / LINE_HALF)

The site factor comes from the optional config/steer.json, per registered domain (1 where absent). Like the seed weight
it is part of a group's weight, so every ranked path uses it; a re-test draw ignores it but for 0, which, like an
inactive seed, leaves the site's entries queued and never taken, crossings into it and its retries included.
base is 1 for a seed. For a link it comes from Jev's answers about the page it was found on
(half "leads to further records", half relevance), or DEFAULT_LINK_BASE when that page has
not been judged. hops count from the start of the line, generation from the seed. A crossing
is priced as the line it would become: at its finder's generation + 1, so its priority does not
drop when it becomes a line. The registered domain is the name a person registers (example.com
for www.example.com), so a site's subdomains share one damping; it counts fetches done, not
pages out with take() as the host and line factors do. The share of each round kept for
registered domains never fetched (a new subdomain of a fetched one does not qualify) takes the
best entry of each, ranked by this priority too. There are no source tiers: any http(s) link
can be followed.

state/hosts.parquet counts, per host, what fetching it took: fetches, content, fetches that
needed the PDF or browser tier, refusals, failures and dead links; and when its sitemaps were last read.

Sitemaps. At the start of a run, a seed's starting sites and the sites with a line on a domain steered above 1 have
their robots.txt and the sitemaps it names (else /sitemap.xml) read, at most once per SITEMAP_EVERY, and up to
SITEMAP_MAX_URLS new URLs each, newest first, are queued as ordinary links (add_sitemap). Discovery only: robots.txt
rules are not applied.

Watched pages (crawl's watch, from config/watch.json) are fetched before any ranked take when due, outside the queue,
up to WATCH_SHARE of the run's fetches; a changed one's new links are queued as links of its site's line.

A page that timed out is retried once, with a longer limit, through take()'s retry lane; a second timeout fails it,
and so does waiting longer than RETRY_MAX_AGE for the retry.

Input contract:
    frontier = Frontier.load(store, workdir); frontier.add_seeds(seeds, now)
    picks = frontier.select(n, seeds, today)            # or one at a time: entry = frontier.take(seeds, today); frontier.release(entry)
    frontier.mark(entry, fetch_row, answers, now); frontier.add_links(entry, fetch_row, answers, now)
    # or in two phases, so nothing waits on Jev: frontier.expect_judgement(fetch_id, line_id), mark and add_links with
    # answers None, then frontier.judged(fetch_id, answers or None) when the judgement arrives
    files = frontier.save(workdir)          # upload with store.put_many
    stats = await crawl(store, engine, seeds, workdir=..., max_fetches=...)
Every constant below is PROVISIONAL.
"""

from __future__ import annotations

import asyncio
import bisect
import collections
import contextlib
import datetime as dt
import functools
import heapq
import json
import logging
import math
import random
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from tld import get_fld

from frontier_crawler.harvester.cascade import Target, timed
from frontier_crawler.harvester.readers import newest_first, normalise
from frontier_crawler.judge.jev import JevResult
from frontier_crawler.judge.questions import relevance
from frontier_crawler.storage.state import load_watch_times, save_watch_times
from frontier_crawler.storage.store import Store

MAX_HOPS = 5                    # links this many steps from the start of a line are the last ones queued
HOP_DECAY = 0.7
GENERATION_DECAY = 0.7          # PROVISIONAL: priority factor per line between a line and its seed; it does not wear off
DEFAULT_LINK_BASE = 0.3         # for links from a page Jev has not judged
MIN_LINK_BASE = 0.05
HOST_HALF = 100.0               # PROVISIONAL: fetches from one host at which its priority halves
DOMAIN_HALF = 200.0             # PROVISIONAL: fetches from one registered domain, all its hosts, at which its priority halves
LINE_HALF = 100.0               # fetches in one line at which its priority halves
YIELD_ALPHA = 0.15              # weight of the newest fetch in a line's running yield
INBOUND_LIFT = 0.05             # share of the gap a link from a better line closes on the yield of the line it links into
RETEST_SHARE = 0.02             # share of take() calls spent on a group drawn at random
RETEST_DRAWS = 5                # draws a re-test makes past blocked groups before it falls back to the ranked take
NEW_DOMAIN_SHARE = 0.05         # PROVISIONAL: part of each round kept for registered domains never fetched before
HOST_ROUND_SHARE = 0.1          # most of one round that one host may take (at least 2)
MAX_QUEUED_PER_HOST = 2000
MAX_IN_FLIGHT_PER_HOST = 2      # entries handed out by take() and not yet marked, per host
PORTAL_THRESHOLD = 0.7          # Jev's data_portal answer at which a page is listed as a candidate for a derived client
MAX_RETRY_IN_FLIGHT = 2         # timed-out pages fetched again at once while there is other work, so slow hosts cannot take over a run
RETRY_MAX_AGE = dt.timedelta(days=2)   # a retry still waiting this long after its first attempt is failed
RETRY_TIMEOUT_SECONDS = 90.0    # the read limit for that second attempt
RETRY_CONNECT_SECONDS = 20.0    # and its connect limit
COOL_AFTER = 3                  # fetches in a row from one host that served nothing, after which a run leaves the host alone
JEV_SLOTS = 8                   # judgements running at once in a crawl
JUDGE_GRACE_SECONDS = 60.0      # how long the end of a crawl waits for judgements still running; the rest stay unjudged
FLUSH_EVERY_SECONDS = 5.0       # how often a crawl asks the engine to upload what it has buffered
TAKE_WAIT_SECONDS = 0.05        # how long a crawl worker with nothing to take waits before asking again
THIN_WORDS = 20                 # PROVISIONAL: an HTML page served with fewer words is not content: an outage notice, an empty shell
SITEMAP_EVERY = dt.timedelta(days=7)   # PROVISIONAL: how often one site's robots.txt and sitemaps are read
SITEMAP_MAX_URLS = 500          # PROVISIONAL: new URLs queued from one site's sitemaps per read
WATCH_SHARE = 0.2               # PROVISIONAL: most of a run's max_fetches that watched pages may take

logger = logging.getLogger("frontier_crawler.frontier")

FRONTIER_PATH = "state/frontier.parquet"
HOSTS_PATH = "state/hosts.parquet"
LINES_PATH = "state/lines.parquet"

_TS = pa.timestamp("us", tz="UTC")
_FRONTIER_SCHEMA = pa.schema(
    [
        ("url", pa.string()),
        ("host", pa.string()),
        ("seed_id", pa.string()),   # the seed this descends from, however many lines later
        ("line_id", pa.string()),
        ("crossing", pa.bool_()),   # a link to a site no line covered when it was found
        ("hops", pa.int32()),
        ("parent_fetch_id", pa.string()),
        ("anchor", pa.string()),
        ("base", pa.float64()),
        # queued | fetched | failed | retry (timed out once; fetched again later) | alias (a redirect target of a fetched URL) |
        # unparseable (a link as written on its page that could not be read as a URL; never fetched)
        ("status", pa.string()),
        ("discovered_at", _TS),
        ("fetched_at", _TS),
        # its last fetch served its content: stored, unchanged or a duplicate, with no block. Null until fetched.
        ("stored", pa.bool_()),
    ]
)
_HOST_COUNTS = ("fetches", "content", "heavy", "refused", "errors", "error_pages")
# sitemap_read_at: when its robots.txt and sitemaps were last read (null: never; absent from files saved before sitemaps were read).
_HOSTS_SCHEMA = pa.schema([("host", pa.string())] + [(name, pa.int64()) for name in _HOST_COUNTS] + [("sitemap_read_at", _TS)])

_LINES_SCHEMA = pa.schema(
    [
        ("line_id", pa.string()),
        ("seed_id", pa.string()),
        ("parent_line", pa.string()),
        ("root_url", pa.string()),
        ("created_at", _TS),
        ("fetches", pa.int64()),
        ("content", pa.int64()),   # fetches that brought something new
        ("yield", pa.float64()),
        ("relevance", pa.float64()),  # running mean of Jev's relevance for its judged pages, at YIELD_ALPHA
        ("inbound", pa.int64()),  # lifts received from links in other lines
        ("generation", pa.int64()),  # 0 for a seed line, its parent's + 1 for a spawned line
    ]
)

_REFUSED_STATUS = {401, 403, 407, 429, 451, 503}
_HEAVY_TIERS = {"PDF", "HEADLESS_PLAYWRIGHT"}
_HTML_TIERS = {"DOM_TRAFILATURA", "HEADLESS_PLAYWRIGHT"}


@dataclass(frozen=True)
class Seed:
    """A starting point, or an objective added later. weight scales its share; until ends it."""

    id: str
    entity: str
    urls: Tuple[str, ...]
    weight: float = 1.0
    until: Optional[dt.date] = None
    note: str = ""

    def active(self, today: dt.date) -> bool:
        return self.weight > 0 and (self.until is None or today <= self.until)


def load_seeds(path: Path) -> List[Seed]:
    """One JSON object per line: id, entity, urls (or url), and optionally weight, until, note."""
    seeds: List[Seed] = []
    seen = set()
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            data = json.loads(stripped)
            urls = data.get("urls") or [data["url"]]
            seed = Seed(
                id=str(data["id"]).strip(),
                entity=str(data["entity"]).strip(),
                urls=tuple(normalise(url) for url in urls),
                weight=float(data.get("weight", 1.0)),
                until=dt.date.fromisoformat(data["until"]) if data.get("until") else None,
                note=str(data.get("note", "")),
            )
            if not seed.id or not seed.entity or not all(seed.urls) or seed.id in seen:
                raise ValueError("id (unique), entity and urls are required")
        except Exception as exc:
            raise ValueError(f"{path}:{number}: {exc}") from exc
        seen.add(seed.id)
        seeds.append(seed)
    return seeds


@functools.lru_cache(maxsize=None)
def registered_domain(host: str) -> str:
    """The name a person registers (example.com for www.example.com), from tld's bundled public suffix list, offline.
    The host itself where the list cannot say: an IP address, an unknown suffix."""
    return get_fld(host, fix_protocol=True, fail_silently=True) or host


def link_base(answers: Optional[JevResult]) -> float:
    if answers is None:
        return DEFAULT_LINK_BASE
    nouls = answers.nouls()
    leads = nouls.get("leads_to_records")
    if leads is None:
        return DEFAULT_LINK_BASE
    return max(MIN_LINK_BASE, min(1.0, 0.5 * leads + 0.5 * (relevance(nouls) or 0.0)))


def judged_yield(answers: JevResult) -> float:
    """What a judged page was worth to its line: its relevance, or 1 when the questions give none."""
    judged = relevance(answers.nouls())
    return 1.0 if judged is None else max(0.0, min(1.0, judged))


def thin(row: Dict[str, Any]) -> bool:
    """An HTML page served with almost no text. Not JSON or a PDF: a scan has no text until it is read."""
    return (row.get("outcome") in ("stored", "unchanged", "duplicate") and row.get("tier_used") in _HTML_TIERS
            and (row.get("word_count") or 0) < THIN_WORDS)


def fetch_yield(row: Dict[str, Any], answers: Optional[JevResult]) -> Optional[float]:
    """What one fetch was worth to its line: 0 to 1, or None for new content not judged (yet). A thin page is worth 0."""
    if row.get("outcome") != "stored" or row.get("block_marker") or thin(row):
        return 0.0
    return None if answers is None else judged_yield(answers)


def _static(entry: Dict[str, Any], weight: float) -> float:
    """The part of priority() fixed for a queued entry, which priority() starts from: priority() is monotone in it,
    bit for bit, for one group's shared line and host factors. A crossing takes one more generation step here, as the
    line it would become."""
    return weight * entry["base"] * HOP_DECAY ** entry["hops"] * (GENERATION_DECAY if entry.get("crossing") else 1.0)


class _Group:
    """The queued entries that share a line, a host and a seed, and so every factor of priority() but the static one.
    members are kept best first: static score down, then url."""

    __slots__ = ("line", "host", "seed", "weight", "members", "version", "slot")

    def __init__(self, line: str, host: str, seed: str, weight: float) -> None:
        self.line, self.host, self.seed, self.weight = line, host, seed, weight
        self.members: List[Dict[str, Any]] = []
        self.version = 0
        self.slot = -1   # its place in Frontier._filled while it has members

    def key(self, entry: Dict[str, Any]) -> Tuple[float, str]:
        return -_static(entry, self.weight), entry["url"]


class Frontier:
    def __init__(
        self,
        entries: Optional[List[Dict[str, Any]]] = None,
        hosts: Optional[List[Dict[str, Any]]] = None,
        lines: Optional[List[Dict[str, Any]]] = None,
        rng_seed: Any = 0,
    ) -> None:
        self._entries: Dict[str, Dict[str, Any]] = {entry["url"]: entry for entry in (entries or [])}
        self._hosts: Dict[str, Dict[str, int]] = {
            row["host"]: {name: int(row.get(name) or 0) for name in _HOST_COUNTS} for row in (hosts or [])
        }
        self.sitemap_read: Dict[str, dt.datetime] = {row["host"]: row["sitemap_read_at"] for row in (hosts or []) if row.get("sitemap_read_at")}
        # Lines saved before 2026-11-11 may carry status "frozen", since_thaw and frozen_at: every line is active, and those are dropped on save.
        self._lines: Dict[str, Dict[str, Any]] = {line["line_id"]: line for line in (lines or [])}
        for line in self._lines.values():
            if line.get("relevance") is None:   # lines saved before 2026-11-11 have none: their yield is the best guess
                line["relevance"] = line["yield"]
        # Lines saved before generations existed get theirs from the parent chain. Chains run past 100 lines, so walk
        # them without recursion; a missing parent or a loop makes the line where the walk stops generation 0.
        legacy = [line for line in self._lines.values() if line.get("generation") is None]
        for line in legacy:
            chain: List[Dict[str, Any]] = []
            seen: set = set()
            at: Optional[Dict[str, Any]] = line
            while at is not None and at.get("generation") is None and at["line_id"] not in seen:
                seen.add(at["line_id"])
                chain.append(at)
                at = self._lines.get(at.get("parent_line") or "")
            generation = at["generation"] + 1 if at is not None and at.get("generation") is not None else 0
            for link in reversed(chain):
                link["generation"] = generation
                generation += 1
        # Those lines not fetched since they were spawned started at their parent's yield x HOP_DECAY and relevance 1. The
        # generation factor now carries that step, so they restart at their parent's yield and relevance, as new lines do.
        # Oldest first, so parents settle first.
        for line in sorted(legacy, key=lambda l: l["created_at"]):
            parent = self._lines.get(line.get("parent_line") or "")
            if parent is not None and line["fetches"] == 0:
                line["yield"], line["relevance"] = parent["yield"], parent["relevance"]
        self._host_line: Dict[str, str] = {}
        self._queued_per_host: Dict[str, int] = {}
        # The queue, ranked by group: see select(). The heaps are built by the first select() or take(), which bring the seed weights.
        self._groups: Dict[Tuple[str, str, str], _Group] = {}
        self._groups_by_line: Dict[str, List[_Group]] = {}
        self._groups_by_host: Dict[str, List[_Group]] = {}
        self._groups_by_domain: Dict[str, List[_Group]] = {}
        # Fetches per registered domain, from the hosts table, kept current by mark(). A fetch only lowers the groups of
        # its domain, so like a host's own it leaves them for _top() to re-score.
        self._domain_fetches: Counter = Counter()
        for host, host_counts in self._hosts.items():
            self._domain_fetches[registered_domain(host)] += host_counts["fetches"]
        self._weights: Optional[Dict[str, float]] = None      # the active seeds' weights the heaps were built with
        self._steer: Dict[str, float] = {}                     # registered domain -> site factor; see steer()
        self._heap: List[Tuple[float, str, int, _Group]] = []  # (-score, url, version, group) of each group's best entry
        self._new_heap: List[Tuple[float, str, int, _Group]] = []  # the same, for groups on registered domains never fetched
        self._version = 0
        self._crossing: Dict[str, List[Dict[str, Any]]] = {}  # host -> its entries still marked crossing, any status
        self._statuses = Counter(entry["status"] for entry in self._entries.values())
        self._in_flight: Dict[str, Tuple[str, str]] = {}     # url -> (host, line, "" for a crossing) of each entry out with take()
        self._flight_host: Dict[str, int] = {}
        self._flight_line: Dict[str, int] = {}
        self._takes = 0
        self._retry: Dict[str, Dict[str, Any]] = {}   # entries waiting for their second attempt, oldest first
        self._retry_flight = 0
        # In this run only: fetches in a row per host that served nothing, and the hosts left alone for that.
        self._strikes: Dict[str, int] = {}
        self.cooled: set = set()
        # fetch_id -> (the line credited, the urls add_links() queued from it), for pages whose judgement is awaited
        self._judging: Dict[str, Tuple[str, List[str]]] = {}
        # seq is the order entries were queued in: select() breaks ties within a never-fetched domain by it.
        for seq, entry in enumerate(self._entries.values()):
            entry["seq"] = seq
            if entry["status"] == "queued":
                self._queued_per_host[entry["host"]] = self._queued_per_host.get(entry["host"], 0) + 1
                (self._groups.get((entry["line_id"], entry["host"], entry["seed_id"])) or self._group(entry)).members.append(entry)
            elif entry["status"] == "retry":
                self._retry[entry["url"]] = entry
            if entry.get("crossing"):
                self._crossing.setdefault(entry["host"], []).append(entry)
            elif entry["host"]:
                self._host_line.setdefault(entry["host"], entry["line_id"])
        self._seq = len(self._entries)
        # The groups with members, each at its slot, so a re-test draws one uniformly without scanning them.
        self._filled: List[_Group] = list(self._groups.values())
        for slot, group in enumerate(self._filled):
            group.slot = slot
            if len(group.members) > 1:
                group.members.sort(key=group.key)
        self._rng = random.Random(rng_seed)
        self.retests: set = set()   # urls taken as re-tests in this run
        self.new_domain_takes = 0   # takes from the share kept for registered domains never fetched, in this run
        # What the queue declined to take in this run, by reason. Reported in the run summary.
        self.skipped = {"unparseable_link": 0, "hop_limit": 0, "host_queue_full": 0}
        self.spawned = 0
        self.seed_urls_requeued = 0
        self.lifts = 0
        self._linked: set = set()      # (from line, into line) pairs that have had their one link in this run
        self.lines_lifted: set = set()

    # ------------------------------------------------------------ persistence

    @classmethod
    def load(cls, store: Store, workdir: Path, rng_seed: Any = 0) -> "Frontier":
        tables: List[List[Dict[str, Any]]] = []
        for remote in (FRONTIER_PATH, HOSTS_PATH, LINES_PATH):
            local = Path(workdir) / "state-in" / remote.rsplit("/", 1)[1]
            tables.append(pq.read_table(local).to_pylist() if store.get(remote, local) else [])
        return cls(*tables, rng_seed=rng_seed)

    def save(self, workdir: Path) -> List[Tuple[Path, str]]:
        out = Path(workdir) / "state-out"
        out.mkdir(parents=True, exist_ok=True)
        zero = dict.fromkeys(_HOST_COUNTS, 0)
        host_rows = [{"host": host, **self._hosts.get(host, zero), "sitemap_read_at": self.sitemap_read.get(host)}
                     for host in sorted(set(self._hosts) | set(self.sitemap_read))]
        files: List[Tuple[Path, str]] = []
        for remote, schema, rows in (
            (FRONTIER_PATH, _FRONTIER_SCHEMA, [self._entries[url] for url in sorted(self._entries)]),
            (HOSTS_PATH, _HOSTS_SCHEMA, host_rows),
            (LINES_PATH, _LINES_SCHEMA, [self._lines[line_id] for line_id in sorted(self._lines)]),
        ):
            local = out / remote.rsplit("/", 1)[1]
            columns = {name: [row.get(name) for row in rows] for name in schema.names}
            pq.write_table(pa.Table.from_pydict(columns, schema=schema), local, compression="zstd")
            files.append((local, remote))
        return files

    # ------------------------------------------------------------ reading

    def __len__(self) -> int:
        return len(self._entries)

    def entry(self, url: str) -> Optional[Dict[str, Any]]:
        return self._entries.get(url)

    def line(self, line_id: str) -> Optional[Dict[str, Any]]:
        return self._lines.get(line_id)

    def count(self, status: str) -> int:
        return self._statuses[status]

    def steer(self, sites: Dict[str, float]) -> None:
        """Set the site factors, per registered domain. The heaps are rebuilt for them by the next select() or take()."""
        self._steer = dict(sites)
        self._weights = None

    def factor(self, host: str) -> float:
        return self._steer.get(registered_domain(host), 1.0)

    def priority(self, entry: Dict[str, Any], weight: float, extra_host: int = 0, extra_line: int = 0) -> float:
        line = self._lines[entry["line_id"]]
        held_host = self._hosts.get(entry["host"], {}).get("fetches", 0) + extra_host
        return (
            _static(entry, weight) * line["yield"] * GENERATION_DECAY ** line["generation"]
            / (1.0 + held_host / HOST_HALF)
            / (1.0 + self._domain_fetches.get(registered_domain(entry["host"]), 0) / DOMAIN_HALF)
            / (1.0 + (line["fetches"] + extra_line) / LINE_HALF)
        )

    # ------------------------------------------------------------ lines

    def _new_line(
        self, line_id: str, seed_id: str, parent: Optional[Dict[str, Any]], root_url: str, now: dt.datetime,
    ) -> Dict[str, Any]:
        """A seed line (parent None) starts at yield and relevance 1 and generation 0; a spawned line at its parent's
        yield and relevance, one generation down."""
        line = {
            "line_id": line_id, "seed_id": seed_id, "parent_line": parent and parent["line_id"], "root_url": root_url,
            "created_at": now, "fetches": 0, "content": 0, "yield": parent["yield"] if parent else 1.0,
            "relevance": parent["relevance"] if parent else 1.0,
            "inbound": 0, "generation": parent["generation"] + 1 if parent else 0,
        }
        self._lines[line_id] = line
        return line

    def _record_yield(self, line: Dict[str, Any], value: Optional[float]) -> None:
        """value None: new content whose judgement is still coming. It counts as content and leaves the yield where it was."""
        line["fetches"] += 1
        line["content"] += value is None or value > 0
        self._move_yield(line, value)

    def _move_yield(self, line: Dict[str, Any], value: Optional[float]) -> None:
        """One step of the line's running yield toward value (none for None)."""
        if value is not None:
            self._set_yield(line, (1.0 - YIELD_ALPHA) * line["yield"] + YIELD_ALPHA * value)

    def _set_yield(self, line: Dict[str, Any], value: float) -> None:
        """A rise re-ranks the line's groups; a fall is left for _top() to re-score."""
        before, line["yield"] = line["yield"], value
        if value > before:
            self._push_all(self._groups_by_line.get(line["line_id"], ()))

    @staticmethod
    def _note_relevance(line: Dict[str, Any], value: float) -> None:
        line["relevance"] = (1.0 - YIELD_ALPHA) * line["relevance"] + YIELD_ALPHA * value

    def _note_inbound(self, line_id: str, from_line: str) -> None:
        """A page of from_line linked into line_id. The first such link in a run moves line_id's yield INBOUND_LIFT of the
        way toward from_line's, capped at line_id's own relevance, if higher; later ones do nothing. Links alone cannot
        hold a much-linked site above what its judged pages are worth."""
        if (from_line, line_id) in self._linked:
            return
        self._linked.add((from_line, line_id))
        line = self._lines[line_id]
        gap = min(self._lines[from_line]["yield"], line["relevance"]) - line["yield"]
        if gap > 0:
            self._set_yield(line, line["yield"] + INBOUND_LIFT * gap)
            line["inbound"] += 1
            self.lifts += 1
            self.lines_lifted.add(line_id)

    def _spawn(self, entry: Dict[str, Any], now: dt.datetime) -> None:
        """A crossing link yielded content: its site becomes a line, and the queued links to that site move into it."""
        host = entry["host"]
        line_id = self._host_line.get(host)
        if line_id is None:
            line_id = f"{entry['seed_id']}:{host}"
            self._new_line(line_id, entry["seed_id"], self._lines[entry["line_id"]], entry["url"], now)
            self._host_line[host] = line_id
            self.spawned += 1
        self._move(entry, line_id, 0)
        for other in self._crossing.pop(host, ()):
            if other is not entry:
                self._move(other, line_id, 1)

    def _move(self, entry: Dict[str, Any], line_id: str, hops: int) -> None:
        waiting = entry["status"] == "queued" and entry["url"] not in self._in_flight
        if waiting:
            self._dequeue(entry)
        entry.update({"line_id": line_id, "crossing": False, "hops": hops})
        if waiting:
            self._enqueue(entry)

    # ------------------------------------------------------------ adding

    def _add(
        self, url: str, seed_id: str, line_id: str, hops: int, base: float, now: dt.datetime,
        parent: Optional[str] = None, anchor: str = "", crossing: bool = False,
    ) -> bool:
        if not url or url in self._entries:
            return False
        host = urlsplit(url).netloc
        if self._queued_per_host.get(host, 0) >= MAX_QUEUED_PER_HOST:
            self.skipped["host_queue_full"] += 1
            return False
        self._insert({
            "url": url, "host": host, "seed_id": seed_id, "line_id": line_id, "crossing": crossing, "hops": hops,
            "parent_fetch_id": parent, "anchor": anchor or None, "base": base, "status": "queued",
            "discovered_at": now, "fetched_at": None,
        })
        self._queued_per_host[host] = self._queued_per_host.get(host, 0) + 1
        return True

    def _insert(self, entry: Dict[str, Any]) -> None:
        entry["seq"] = self._seq
        self._seq += 1
        self._entries[entry["url"]] = entry
        self._statuses[entry["status"]] += 1
        if entry.get("crossing"):
            self._crossing.setdefault(entry["host"], []).append(entry)
        if entry["status"] == "queued":
            self._enqueue(entry)

    def _set_status(self, entry: Dict[str, Any], status: str) -> None:
        self._statuses.update({entry["status"]: -1, status: 1})
        entry["status"] = status

    def add_seeds(self, seeds: Sequence[Seed], now: dt.datetime) -> int:
        added = 0
        for seed in seeds:
            for url in seed.urls:
                known = self._entries.get(url)
                if known is not None:
                    # A starting URL whose own fetch served nothing (or a thin page) is tried again each run, whatever the rest of
                    # its seed found: the failure may have been temporary, and the run summary then shows the reason.
                    # One fetched before the record existed (null) is tried once more, to find out.
                    if not known.get("stored") and known["status"] in ("fetched", "failed"):
                        self._set_status(known, "queued")
                        self._enqueue(known)
                        self._queued_per_host[known["host"]] = self._queued_per_host.get(known["host"], 0) + 1
                        self.seed_urls_requeued += 1
                    continue
                if seed.id not in self._lines:
                    self._new_line(seed.id, seed.id, None, url, now)
                if self._add(url, seed.id, seed.id, 0, 1.0, now):
                    self._host_line.setdefault(urlsplit(url).netloc, seed.id)
                    added += 1
        return added

    def sitemap_sites(self, seeds: Sequence[Seed], today: dt.date, now: dt.datetime) -> Dict[str, str]:
        """host -> origin of each site whose sitemaps are due: an active seed's starting sites and the sites with a line on a
        registered domain steered above 1, not read in the last SITEMAP_EVERY. A site steered to 0 is not fetched from."""
        sites: Dict[str, str] = {}
        for url in (url for seed in seeds if seed.active(today) for url in seed.urls):
            parts = urlsplit(url)
            sites.setdefault(parts.netloc, f"{parts.scheme}://{parts.netloc}")
        for host in self._host_line:
            if self.factor(host) > 1:
                sites.setdefault(host, f"https://{host}")   # shortcut: https assumed; an http-only site fails and is counted
        return {host: origin for host, origin in sorted(sites.items()) if self.factor(host) > 0
                and (host not in self.sitemap_read or now - self.sitemap_read[host] >= SITEMAP_EVERY)}

    def add_sitemap(self, host: str, entries: Sequence[Tuple[str, str]], now: dt.datetime) -> int:
        """Queue up to SITEMAP_MAX_URLS URLs the queue has not seen from a site's sitemap entries (url, lastmod), newest first,
        as ordinary links: of the line covering their host (else the site's), one step from its start, at DEFAULT_LINK_BASE.
        URLs off the site's registered domain are ignored. Returns how many were queued."""
        domain = registered_domain(host)
        added = 0
        for url, _ in newest_first(list(entries)):
            if added >= SITEMAP_MAX_URLS:
                break
            url_host = urlsplit(url).netloc
            line_id = self._host_line.get(url_host) or self._host_line.get(host)
            if line_id is not None and registered_domain(url_host) == domain:
                added += self._add(url, self._lines[line_id]["seed_id"], line_id, 1, DEFAULT_LINK_BASE, now)
        return added

    def add_links(self, entry: Dict[str, Any], row: Dict[str, Any], answers: Optional[JevResult], now: dt.datetime) -> int:
        """Queue the links of a stored HTML page, row["links"] as the engine extracted them. Returns how many were new."""
        if row.get("outcome") != "stored" or row.get("block_marker") or not row.get("links"):
            return 0
        base = link_base(answers)
        own_line = entry["line_id"]
        added = 0
        awaited = self._judging.get(row.get("fetch_id") or "")
        links, unparseable = row["links"]
        for href in unparseable:
            # Kept as written, with the page it came from, so it can be looked at and repaired later.
            if href not in self._entries:
                self._insert({
                    **entry, "url": href, "host": "", "crossing": False, "parent_fetch_id": row["fetch_id"],
                    "anchor": None, "status": "unparseable", "discovered_at": now, "fetched_at": None,
                })
                self.skipped["unparseable_link"] += 1
        for url, anchor in links:
            host = urlsplit(url).netloc
            covering = self._host_line.get(host)
            # A link into another line's site, queued, fetched or new: the host map knows its line either way.
            if covering is not None and covering != own_line:
                self._note_inbound(covering, own_line)
            if url in self._entries:
                continue
            if covering == own_line:
                hops, line_id, crossing = entry["hops"] + 1, own_line, False
            elif covering is not None:
                # The site already has a line. The link joins it, one step from its start.
                hops, line_id, crossing = 1, covering, False
            else:
                hops, line_id, crossing = entry["hops"] + 1, own_line, True
            # The hop limit holds a line to the neighbourhood of where it started. A link to a
            # site no line covers is exempt: reaching new ground is how the crawl branches.
            if hops > MAX_HOPS and not crossing:
                self.skipped["hop_limit"] += 1
                continue
            if self._add(url, self._lines[line_id]["seed_id"], line_id, hops, base, now, row["fetch_id"], anchor, crossing):
                added += 1
                if awaited is not None:
                    awaited[1].append(url)
        return added

    # ------------------------------------------------------------ after a fetch

    def mark(self, entry: Dict[str, Any], row: Dict[str, Any], answers: Optional[JevResult], now: dt.datetime) -> None:
        flight = self._in_flight.pop(entry["url"], None)
        if flight is not None:
            self._unhold(flight)
            self._retry_flight -= entry["status"] == "retry"
        elif entry["status"] == "queued":
            self._dequeue(entry)
        self._retry.pop(entry["url"], None)
        if entry["status"] == "queued":
            self._queued_per_host[entry["host"]] = max(0, self._queued_per_host.get(entry["host"], 0) - 1)
        outcome = row.get("outcome")
        # A timeout or a connection error (nothing served) is a strike; anything served clears them.
        self._strikes[entry["host"]] = self._strikes.get(entry["host"], 0) + 1 if outcome == "error" else 0
        if self._strikes[entry["host"]] >= COOL_AFTER:
            self.cooled.add(entry["host"])   # its entries stay queued for later runs; the heaps drop its groups
        if outcome != "error":
            self._set_status(entry, "fetched")
        elif row.get("timed_out") and entry["status"] != "retry":
            self._set_status(entry, "retry")
            self._retry[entry["url"]] = entry
        else:
            self._set_status(entry, "failed")
        entry["fetched_at"] = now
        entry["stored"] = outcome in ("stored", "unchanged", "duplicate") and not row.get("block_marker") and not thin(row)
        counts = self._hosts.setdefault(entry["host"], {name: 0 for name in _HOST_COUNTS})
        counts["fetches"] += 1
        self._domain_fetches[registered_domain(entry["host"])] += 1
        marker = row.get("block_marker") or ""
        refused = row.get("http_status") in _REFUSED_STATUS or (bool(marker) and not marker.startswith("http "))
        if outcome == "error":
            counts["errors"] += 1
        elif refused:
            counts["refused"] += 1
        elif outcome == "error_page":
            counts["error_pages"] += 1
        else:
            counts["content"] += 1
            if row.get("tier_used") in _HEAVY_TIERS:
                counts["heavy"] += 1
        # A line's record is its own pages. A crossing that brings content is the first fetch of the line its site
        # becomes; one that brings nothing counts toward no line, only its host.
        value = fetch_yield(row, answers)
        if entry.get("crossing") and (value is None or value > 0):
            self._spawn(entry, now)
        fetch_id = row.get("fetch_id")
        if entry.get("crossing"):
            self._judging.pop(fetch_id, None)
        else:
            line = self._lines[entry["line_id"]]
            if value is None and fetch_id not in self._judging:
                value = line["relevance"]   # not judged, and none coming: it counts at what the line's judged pages were worth
            elif answers is not None and value is not None:
                self._note_relevance(line, value)
            self._record_yield(line, value)
            if fetch_id in self._judging:   # a crossing's judgement moves the line it started, not the one that found it
                self._judging[fetch_id] = (line["line_id"], self._judging[fetch_id][1])
        # A redirect target is the same document. Record it so a later link to it is not fetched again.
        final = normalise(row.get("url_final") or "")
        if final and final not in self._entries:
            self._insert({**entry, "url": final, "host": urlsplit(final).netloc, "anchor": None, "status": "alias"})

    # ------------------------------------------------------------ when a judgement arrives

    def expect_judgement(self, fetch_id: str, line_id: str) -> None:
        """Call before mark() for a page that will be judged later. Its fetch counts now, its yield when judged() is
        called, and the links add_links() queues from it are remembered until then, to be re-scored. mark() moves
        the record of a crossing to the line it starts, or drops it when it starts none."""
        self._judging[fetch_id] = (line_id, [])

    def judged(self, fetch_id: str, answers: Optional[JevResult]) -> None:
        """The judgement of a page passed to expect_judgement(), or None when none will come. Moves the line's yield
        as mark() does (by the page's relevance, or by the line's when none came), and re-scores the links queued
        from it that are still waiting."""
        line_id, urls = self._judging.pop(fetch_id, ("", []))
        if not line_id:
            return
        line = self._lines[line_id]
        if answers is None:
            self._move_yield(line, line["relevance"])   # it will not be judged: it counts as mark() counts such a page
            return
        value = judged_yield(answers)
        self._note_relevance(line, value)
        self._move_yield(line, value)
        base = link_base(answers)
        for url in urls:
            self.rebase(self._entries[url], base)

    def rebase(self, entry: Dict[str, Any], base: float) -> None:
        """Change a queued entry's link score, keeping its group's order and the heaps right."""
        if entry["status"] != "queued" or entry["base"] == base:
            return
        waiting = entry["url"] not in self._in_flight
        if waiting:
            self._dequeue(entry)
        entry["base"] = base
        if waiting:
            self._enqueue(entry)

    # ------------------------------------------------------------ the ranked queue
    #
    # Every factor of priority() but the static one is shared by a group (line, host, seed), so a group ranks by its best
    # entry, and the heaps hold one item per group. An item's score may be stale only by being too high: whatever lowers a
    # group (a fetch, an entry leaving, an entry in flight) is left for _top() to re-score, and whatever raises one (a
    # line's yield rising, an entry put back, a better entry joining) pushes a new item. Older items are skipped by
    # their version.

    def _group(self, entry: Dict[str, Any]) -> _Group:
        key = (entry["line_id"], entry["host"], entry["seed_id"])
        group = self._groups.get(key)
        if group is None:
            group = self._groups[key] = _Group(*key, (self._weights or {}).get(key[2], 1.0) * self.factor(key[1]))
            self._groups_by_line.setdefault(key[0], []).append(group)
            self._groups_by_host.setdefault(key[1], []).append(group)
            self._groups_by_domain.setdefault(registered_domain(key[1]), []).append(group)
        return group

    def _enqueue(self, entry: Dict[str, Any]) -> None:
        group = self._group(entry)
        members = group.members
        i = bisect.bisect_left(members, group.key(entry), key=group.key)
        members.insert(i, entry)
        if len(members) == 1:
            group.slot = len(self._filled)
            self._filled.append(group)
        if self._weights is not None and self._eligible(group):
            extras = self._extras(group)
            # A later entry can still lead: two static scores may round to one score, and then the url decides.
            if i == 0 or self.priority(entry, group.weight, *extras) == self.priority(members[0], group.weight, *extras):
                self._push(group)

    def _dequeue(self, entry: Dict[str, Any]) -> None:
        group = self._groups[(entry["line_id"], entry["host"], entry["seed_id"])]
        i = bisect.bisect_left(group.members, group.key(entry), key=group.key)
        assert group.members[i] is entry
        del group.members[i]
        if not group.members:   # swap the last filled group into its slot
            last = self._filled.pop()
            if last is not group:
                self._filled[group.slot], last.slot = last, group.slot
            group.slot = -1

    def _eligible(self, group: _Group) -> bool:
        return (bool(group.members) and group.seed in (self._weights or {}) and group.line in self._lines
                and group.host not in self.cooled and group.weight > 0)

    def _new_domain(self, host: str) -> bool:
        return self._domain_fetches[registered_domain(host)] == 0

    def _extras(self, group: _Group) -> Tuple[int, int]:
        return self._flight_host.get(group.host, 0), self._flight_line.get(group.line, 0)

    @staticmethod
    def _runs(group: _Group, start: int):
        """(start, end) of each run of members with the same static score, best first."""
        members, weight = group.members, group.weight
        key = lambda e: -_static(e, weight)
        while start < len(members):
            end = start + 1
            if end < len(members) and key(members[end]) == key(members[start]):
                end = bisect.bisect_right(members, key(members[start]), end, key=key)
            yield start, end
            start = end

    def _best(self, group: _Group, extra_host: int, extra_line: int, out: Any = (), start: int = 0) -> Optional[Tuple[float, Dict[str, Any]]]:
        """(score, entry) of the group's best member not in out: highest score, then lowest url. The score never rises
        along the members, but neighbouring static scores can round to one score, so runs are read until it falls."""
        members = group.members
        best = None
        for i, j in self._runs(group, start):
            entry = next((members[k] for k in range(i, j) if members[k]["url"] not in out), None)
            if entry is None:
                continue
            score = self.priority(entry, group.weight, extra_host, extra_line)
            if best is not None and score < best[0]:
                break
            if best is None or entry["url"] < best[1]["url"]:
                best = (score, entry)
        return best

    def _push(self, group: _Group) -> None:
        if self._weights is None or not self._eligible(group):
            return
        score, entry = self._best(group, *self._extras(group))  # type: ignore[misc]
        self._version += 1
        group.version = self._version
        item = (-score, entry["url"], self._version, group)
        heapq.heappush(self._heap, item)
        if self._new_domain(group.host):
            heapq.heappush(self._new_heap, item)

    def _push_all(self, groups: Any) -> None:
        for group in groups:
            self._push(group)

    def _ready(self, seeds: Sequence[Seed], today: dt.date) -> None:
        """Build the heaps for these seed weights, or rebuild them when the weights change or stale items pile up."""
        weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
        if weights == self._weights and len(self._heap) <= 2 * len(self._groups) + 1000:
            return
        self._weights = weights
        self._heap, self._new_heap = [], []
        for group in self._groups.values():
            if group.seed in weights and group.weight != weights[group.seed] * self.factor(group.host):
                group.weight = weights[group.seed] * self.factor(group.host)
                group.members.sort(key=group.key)
            self._push(group)

    def _top(self, heap: List[Tuple[float, str, int, _Group]], new_domains: bool = False) -> Optional[Tuple[float, str, int, _Group]]:
        """The heap's first item, once it is current and exact; stale items are dropped and high ones re-scored."""
        while heap:
            negative, url, version, group = heap[0]
            if version != group.version or not self._eligible(group) or (new_domains and not self._new_domain(group.host)):
                heapq.heappop(heap)
                continue
            score, entry = self._best(group, *self._extras(group))  # type: ignore[misc]
            if -score == negative and entry["url"] == url:
                return heap[0]
            heapq.heappop(heap)
            self._push(group)
        return None

    # ------------------------------------------------------------ choosing

    def select(self, n: int, seeds: Sequence[Seed], today: dt.date) -> List[Dict[str, Any]]:
        """Up to n queued entries for the next round, from active seeds, spread across hosts and lines.

        The result is exactly that of scoring every queued entry and taking them through a lazily re-scored heap
        (tests/test_frontier.py keeps that version and checks this against it). Here a group stands in the heap for its
        entries, and an entry is scored on its own only when it reaches the top. Entries out with take() count as held.
        """
        self._ready(seeds, today)
        if n <= 0:
            return []
        host_cap = max(2, math.ceil(n * HOST_ROUND_SHARE))
        picked: List[Dict[str, Any]] = []
        in_host: Dict[str, int] = {}
        in_line: Dict[str, int] = {}

        def take(entry: Dict[str, Any]) -> None:
            picked.append(entry)
            in_host[entry["host"]] = in_host.get(entry["host"], 0) + 1
            if not entry.get("crossing"):   # held against its line only if its fetch will count toward it, as in take()
                in_line[entry["line_id"]] = in_line.get(entry["line_id"], 0) + 1

        # First, the share kept for registered domains never fetched: the best entry of each, best domains first.
        for entry in self._new_domain_picks(math.ceil(n * NEW_DOMAIN_SHARE)):
            take(entry)

        # Then the rest by priority. Each pick lowers the priority of what shares its host or line, so an entry is
        # taken only if its fresh score still leads every score in the heap, which may be stale.
        def extras(host: str, line: str, host_counts: Dict[str, int], line_counts: Dict[str, int]) -> Tuple[int, int]:
            return self._flight_host.get(host, 0) + host_counts.get(host, 0), self._flight_line.get(line, 0) + line_counts.get(line, 0)

        start_host, start_line = dict(in_host), dict(in_line)   # every entry's first score in this round counts the reserve
        out = {entry["url"] for entry in picked}               # entries no longer waiting in their group this round
        cursor: Dict[_Group, int] = {}
        followed: set = set()   # groups whose entries now come through `local`, not the persistent heap
        local: List[Tuple[float, str, Dict[str, Any], Optional[_Group]]] = []   # a followed group's next entry, or an entry put back
        popped: List[Tuple[float, str, int, _Group]] = []   # persistent items taken off here, put back at the end

        def follow(group: _Group) -> None:
            followed.add(group)
            i = cursor.get(group, 0)
            while i < len(group.members) and group.members[i]["url"] in out:
                i += 1
            cursor[group] = i
            best = self._best(group, *extras(group.host, group.line, start_host, start_line), out, i)
            if best is not None:
                heapq.heappush(local, (-best[0], best[1]["url"], best[1], group))

        def top() -> Optional[tuple]:
            while True:
                item = self._top(self._heap)
                if item is None or (item[3] not in followed and not start_host.get(item[3].host) and not start_line.get(item[3].line)):
                    break
                popped.append(heapq.heappop(self._heap))
                if item[3] not in followed:
                    follow(item[3])
            if local and (item is None or local[0][:2] < item[:2]):
                return local[0]
            return item

        while len(picked) < n:
            item = top()
            if item is None:
                break
            if local and item is local[0]:
                _, url, entry, group = heapq.heappop(local)
            else:
                popped.append(heapq.heappop(self._heap))
                url, group = item[1], item[3]
                entry = self._entries[url]
            if group is not None:
                out.add(url)
                follow(group)
            if in_host.get(entry["host"], 0) >= host_cap:
                continue
            fresh = -self.priority(entry, self._groups[(entry["line_id"], entry["host"], entry["seed_id"])].weight, *extras(entry["host"], entry["line_id"], in_host, in_line))  # type: ignore[index]
            following = top()
            if following is not None and fresh > following[0]:
                heapq.heappush(local, (fresh, url, entry, None))
                continue
            take(entry)
        for item in popped:
            heapq.heappush(self._heap, item)
        return picked

    def _new_domain_picks(self, k: int) -> List[Dict[str, Any]]:
        """The best entry of each of the k best registered domains never fetched, best first. Ties within a domain go
        to the entry queued first, ties between domains to the url."""
        scores: Dict[str, float] = {}
        order: List[str] = []
        popped = []
        while k > 0:
            item = self._top(self._new_heap, new_domains=True)
            if item is None or (len(order) >= k and -item[0] < scores[order[k - 1]]):
                break
            popped.append(heapq.heappop(self._new_heap))
            domain = registered_domain(item[3].host)
            if domain not in scores:
                scores[domain] = -item[0]
                order.append(domain)
        for item in popped:
            heapq.heappush(self._new_heap, item)
        picks = []
        for domain in order:
            first = None
            for group in self._groups_by_domain[domain]:
                if not self._eligible(group):
                    continue
                extras = self._extras(group)
                for i, j in self._runs(group, 0):
                    if self.priority(group.members[i], group.weight, *extras) != scores[domain]:
                        break
                    for entry in group.members[i:j]:
                        if first is None or entry["seq"] < first["seq"]:
                            first = entry
            picks.append(first)
        picks.sort(key=lambda e: (-scores[registered_domain(e["host"])], e["url"]))  # type: ignore[index]
        return picks[:k]  # type: ignore[return-value]

    def take(self, seeds: Sequence[Seed], today: dt.date) -> Optional[Dict[str, Any]]:
        """The next entry to fetch, or None, ranked as select() ranks its main pass. It counts as in flight for its host
        and line until mark() or release() is called for it, and a host has at most MAX_IN_FLIGHT_PER_HOST in flight.
        Every 1/NEW_DOMAIN_SHARE-th call takes from registered domains never fetched while there are any. Every 1/RETEST_SHARE-th call
        is a re-test: the best entry of a group drawn uniformly from the groups with queued entries, under the same
        per-host limits; it falls back to the ranked take after RETEST_DRAWS blocked draws. Before any of that, a page
        waiting for its retry is handed out while fewer than MAX_RETRY_IN_FLIGHT are, and after it, when nothing else can
        be taken, with no such limit; its status is still "retry"."""
        entry = self.take_retry(seeds, today, MAX_RETRY_IN_FLIGHT)
        if entry is not None:
            return entry
        self._takes += 1
        if self._takes % round(1 / RETEST_SHARE) == 0:
            for _ in range(RETEST_DRAWS if self._filled else 0):
                group = self._rng.choice(self._filled)
                if self._eligible(group) and self._flight_host.get(group.host, 0) < MAX_IN_FLIGHT_PER_HOST:
                    entry = self._best(group, *self._extras(group))[1]  # type: ignore[index]
                    self.retests.add(entry["url"])
                    self._dequeue(entry)
                    self._hold(entry)
                    return entry
        heaps = [(self._heap, False)]
        if self._takes % round(1 / NEW_DOMAIN_SHARE) == 0:
            heaps.insert(0, (self._new_heap, True))
        for heap, new_domains in heaps:
            full = []
            while True:
                item = self._top(heap, new_domains)
                if item is None or self._flight_host.get(item[3].host, 0) < MAX_IN_FLIGHT_PER_HOST:
                    break
                full.append(heapq.heappop(heap))
            for kept in full:
                heapq.heappush(heap, kept)
            if item is not None:
                entry = self._entries[item[1]]
                self._dequeue(entry)
                self._hold(entry)
                self.new_domain_takes += new_domains
                return entry
        return self.take_retry(seeds, today)   # nothing else to take: idle workers work off the retries

    def take_retry(self, seeds: Sequence[Seed], today: dt.date, limit: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """The oldest page waiting for its retry whose host has room, while fewer than limit retries are in flight."""
        self._ready(seeds, today)
        if limit is not None and self._retry_flight >= limit:
            return None
        entry = next((e for e in self._retry.values() if e["seed_id"] in self._weights   # type: ignore[operator]
                      and e["host"] not in self.cooled and self.factor(e["host"]) > 0 and self._flight_host.get(e["host"], 0) < MAX_IN_FLIGHT_PER_HOST), None)
        if entry is not None:
            del self._retry[entry["url"]]
            self._retry_flight += 1
            self._hold(entry)
        return entry

    def by_seed(self, seeds: Sequence[Seed], today: dt.date) -> Tuple[Dict[str, int], Dict[str, float]]:
        """(queued entries per seed, the best priority of any of them that could be taken now) for the active seeds."""
        weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
        queued = dict.fromkeys(weights, 0)
        top: Dict[str, float] = {}
        for entry in self._entries.values():
            seed = entry["seed_id"]
            if entry["status"] != "queued" or seed not in weights:
                continue
            queued[seed] += 1
            if entry["host"] not in self.cooled:
                top[seed] = max(top.get(seed, 0.0), round(self.priority(entry, weights[seed] * self.factor(entry["host"])), 4))
        return queued, top

    def expire_retries(self, now: dt.datetime) -> int:
        """Fail the retries still waiting RETRY_MAX_AGE after their first attempt, so the lane cannot grow without bound."""
        old = [e for e in self._retry.values() if e["fetched_at"] is not None and now - e["fetched_at"] > RETRY_MAX_AGE]
        for entry in old:
            del self._retry[entry["url"]]
            self._set_status(entry, "failed")
        return len(old)

    def _hold(self, entry: Dict[str, Any]) -> None:
        # A crossing in flight is held against its host only: its fetch will not count toward the line that found it.
        line = "" if entry.get("crossing") else entry["line_id"]
        self._in_flight[entry["url"]] = (entry["host"], line)
        self._flight_host[entry["host"]] = self._flight_host.get(entry["host"], 0) + 1
        if line:
            self._flight_line[line] = self._flight_line.get(line, 0) + 1

    def _unhold(self, flight: Tuple[str, str]) -> None:
        self._flight_host[flight[0]] -= 1
        if flight[1]:
            self._flight_line[flight[1]] -= 1

    def release(self, entry: Dict[str, Any]) -> None:
        """Return an entry from take() that was not fetched to the queue (or to the retry lane)."""
        host, line = flight = self._in_flight.pop(entry["url"])
        self._unhold(flight)
        if entry["status"] == "retry":
            self._retry_flight -= 1
            self._retry[entry["url"]] = entry
        else:
            self._enqueue(entry)
        self._push_all(self._groups_by_host.get(host, []) + self._groups_by_line.get(line, []))


async def crawl(
    store: Store,
    engine: Any,
    seeds: Sequence[Seed],
    *,
    workdir: Path,
    max_fetches: int,
    max_minutes: Optional[float] = None,
    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
    clock: Callable[[], float] = time.monotonic,
    steer: Optional[Dict[str, float]] = None,
    sitemaps: Optional[Callable[[str], Awaitable[Tuple[List[Tuple[str, str]], int, int]]]] = None,
    watch: Sequence[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """Fetch in a rolling stream until max_fetches new pages are taken and no retry waits, max_minutes has passed,
    or nothing is left to take. steer: the site factors, per registered domain (see the module docstring).
    sitemaps: HarvesterCascade.sitemap_entries, or None to read none. Before the first take, each site sitemap_sites()
    finds due has its sitemaps read and add_sitemap() queues what they list; a failure is counted and the crawl goes on.
    watch: watched pages, {"id", "url", "every_hours"} each. Those never served or last served every_hours ago or more
    are fetched before any ranked take, oldest due first, up to WATCH_SHARE of max_fetches. Each is a fetch row with
    line_id "watch:<id>"; the links of a changed one are queued as links of the line covering its host. When each was
    last served is kept in state/watch.json.

    engine.concurrency workers each take the best URL, fetch it, record it and take the next, so a slow page holds
    one worker and not a round. A page to be judged is judged in its own task, at most JEV_SLOTS at once; its line's
    yield and the score of the links found on it follow when the judgement arrives. A background task uploads rows
    every FLUSH_EVERY_SECONDS. At the time limit no new URL is taken; pages in flight finish, and judgements still
    running get JUDGE_GRACE_SECONDS, after which they are abandoned and their pages stay unjudged.

    The queue is uploaded once, in the engine's final commit. A run that dies before then
    keeps its fetched rows (the engine uploads them as it goes) and loses its queue updates,
    so the next run fetches those URLs again and records them as unchanged.
    """
    workdir = Path(workdir)
    # Wall-clock seconds by phase, for the run record. time.monotonic, not `clock`, which only drives the deadline.
    began = time.monotonic()
    seconds: Dict[str, float] = dict.fromkeys(("load_state", "sitemaps", "take", "fetching", "judge_wait", "final_save"), 0.0)
    with timed(seconds, "load_state"):
        frontier = Frontier.load(store, workdir, rng_seed=now().strftime("%Y-%m-%dT%H"))   # the run's start hour: re-tests reproducible
    frontier.steer(steer or {})
    by_id = {seed.id: seed for seed in seeds}
    retries_expired = frontier.expire_retries(now())
    seeds_added = frontier.add_seeds(seeds, now())
    links_added = 0
    stored_by_seed = {seed.id: 0 for seed in seeds}
    seed_url_problems: Dict[str, str] = {}
    seed_url_redirects: Dict[str, str] = {}   # a starting URL -> the other site it was sent to
    starting = {url for seed in seeds for url in seed.urls}
    # Watched pages due now, oldest due first. With none configured, no watch state is read or written.
    watch_times = load_watch_times(store, workdir) if watch else {}
    never = dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    def due_at(w: Dict[str, Any]) -> dt.datetime:
        last = watch_times.get(w["url"])
        try:
            return never if last is None else last + dt.timedelta(hours=w["every_hours"])
        except OverflowError:   # an interval past the calendar: never due again
            return dt.datetime.max.replace(tzinfo=dt.timezone.utc)

    due = sorted((w for w in watch if due_at(w) <= now()), key=due_at)
    watch_queue = collections.deque(due[:math.floor(max_fetches * WATCH_SHARE)])
    watched = dict.fromkeys(("watch_fetched", "watch_changed", "watch_unchanged", "watch_failed", "watch_links_queued"), 0)
    watch_tally = dict.fromkeys(("taken", "stored", "judged", "relevance"), 0)   # judge() wants one; not reported
    read = dict.fromkeys(("sitemap_sites_read", "sitemaps_read", "sitemap_failures", "sitemap_urls_queued"), 0)
    data_portals: Dict[str, float] = {}
    counts = dict.fromkeys(("taken", "in_flight", "peak_in_flight", "retries", "retry_recovered", "steered_down", "steered_up"), 0)
    by_generation = dict.fromkeys(("0", "1", "2", "3", "4-7", "8+"), 0)   # fetches by the generation they are priced at
    by_host_page = dict.fromkeys(("1", "2-5", "6-20", "21+"), 0)   # fetches by their page number on their host
    # Re-test takes against ordinary ones, so the re-test share can be judged: pages taken, stored and judged, and relevance.
    kinds = {kind: dict.fromkeys(("taken", "stored", "judged", "relevance"), 0) for kind in ("retest", "ordinary")}

    def kind(url: str) -> Dict[str, Any]:
        return kinds["retest" if url in frontier.retests else "ordinary"]
    judging: set = set()
    jev_slots = asyncio.Semaphore(JEV_SLOTS)
    stopped_by = "queue_empty"

    def record(entry: Dict[str, Any], row: Dict[str, Any], awaited: bool) -> None:
        """Phase 1, as soon as the page is fetched: everything that needs no judgement."""
        nonlocal links_added
        generation = frontier.line(entry["line_id"])["generation"] + bool(entry.get("crossing"))   # a crossing: its finder's + 1
        by_generation[str(generation) if generation < 4 else "4-7" if generation < 8 else "8+"] += 1
        page = frontier._hosts.get(entry["host"], {}).get("fetches", 0) + 1
        by_host_page["1" if page == 1 else "2-5" if page <= 5 else "6-20" if page <= 20 else "21+"] += 1
        counts["steered_down"] += frontier.factor(entry["host"]) < 1
        counts["steered_up"] += frontier.factor(entry["host"]) > 1
        if awaited:
            frontier.expect_judgement(row["fetch_id"], entry["line_id"])
        frontier.mark(entry, row, None, now())
        links_added += frontier.add_links(entry, row, None, now())
        kind(entry["url"])["stored"] += row["outcome"] == "stored"   # as the engine counts stored
        if entry["url"] in starting:
            final = urlsplit(normalise(row.get("url_final") or "")).netloc
            if final and registered_domain(final) != registered_domain(entry["host"]):
                seed_url_redirects[entry["url"]] = final
        if row["outcome"] == "stored" and not row["block_marker"] and not thin(row):
            stored_by_seed[entry["seed_id"]] = stored_by_seed.get(entry["seed_id"], 0) + 1
        elif entry["url"] in starting:
            if not row["block_marker"] and thin(row):
                problem = f"thin page: {row.get('word_count') or 0} words"
            else:
                reason = row.get("error") or row.get("block_marker") or row.get("tier_note") or ""
                problem = f"{row['outcome']}, status {row['http_status']}: {reason}"
            seed_url_problems[entry["url"]] = problem[:300]

    async def judge(row: Dict[str, Any], tally: Dict[str, Any]) -> None:
        """Phase 2, in its own task: the page's relevance and its links' scores."""
        answers = None
        try:
            async with jev_slots:
                answers = await engine.judge(row)
            if answers is not None:
                tally["judged"] += 1
                tally["relevance"] += judged_yield(answers)
        except Exception:
            logger.exception("judging failed; the page stays unjudged")
        finally:  # also when abandoned at the end of the run
            try:
                frontier.judged(row["fetch_id"], answers)
                portal = answers.nouls().get("data_portal", 0.0) if answers is not None else 0.0
                if portal >= PORTAL_THRESHOLD:
                    data_portals[row["url_final"]] = round(portal, 2)
            except Exception:
                logger.exception("a judgement was not applied to the queue")

    async def fetch_one(entry: Dict[str, Any]) -> None:
        retry = entry["status"] == "retry"
        fetch_id = None
        try:
            target = Target(entry["url"], by_id[entry["seed_id"]].entity, entry["seed_id"], entry["line_id"], entry["hops"], entry["parent_fetch_id"])
            limits = {"timeout": RETRY_TIMEOUT_SECONDS, "connect": RETRY_CONNECT_SECONDS} if retry else {}
            row = await engine.fetch(target, **limits)
            if engine.sampled(row):
                fetch_id = row["fetch_id"]
            with engine.slot_timer("observe"):
                record(entry, row, fetch_id is not None)
            if retry:
                counts["retries"] += 1
                counts["retry_recovered"] += row["outcome"] in ("stored", "unchanged", "duplicate")
            if fetch_id is not None:
                task = asyncio.create_task(judge({**row, "links": None}, kind(entry["url"])))  # the links are not needed to judge
                judging.add(task)
                task.add_done_callback(judging.discard)
        except Exception:
            logger.exception("a fetch slot failed; its page is marked failed")
            with contextlib.suppress(Exception):  # the worker must outlive any one page
                frontier._judging.pop(fetch_id or "", None)
                if entry["url"] in frontier._in_flight:
                    frontier.mark(entry, {"outcome": "error"}, None, now())

    async def fetch_watched(w: Dict[str, Any]) -> None:
        """A watched page: a fetch row of its own line_id, outside the queue; a changed one's links join the queue."""
        nonlocal links_added
        try:
            host = urlsplit(w["url"]).netloc
            line = frontier.line(frontier._host_line.get(host) or "")
            seed_id = line["seed_id"] if line else ""
            entity = by_id[seed_id].entity if seed_id in by_id else ""
            row = await engine.fetch(Target(w["url"], entity, seed_id, f"watch:{w['id']}"))
            watched["watch_fetched"] += 1
            content = row["outcome"] in ("stored", "duplicate") and not row["block_marker"]
            watched["watch_changed" if content else "watch_unchanged" if row["outcome"] == "unchanged" else "watch_failed"] += 1
            if row["outcome"] != "error":
                watch_times[w["url"]] = now()
            if line is not None:
                # As if it were a page of its site's line, at its start: new links on it are queued, known ones are not.
                as_entry = {"url": w["url"], "host": host, "seed_id": seed_id, "line_id": line["line_id"], "crossing": False, "hops": 0,
                            "parent_fetch_id": None, "anchor": None, "base": DEFAULT_LINK_BASE, "status": "fetched",
                            "discovered_at": now(), "fetched_at": now()}
                queued = frontier.add_links(as_entry, row, None, now())
                watched["watch_links_queued"] += queued
                links_added += queued
            if engine.sampled(row):
                task = asyncio.create_task(judge({**row, "links": None}, watch_tally))
                judging.add(task)
                task.add_done_callback(judging.discard)
        except Exception:
            logger.exception("a watched page failed")
            watched["watch_failed"] += 1

    async def worker() -> None:
        nonlocal stopped_by
        while True:
            if deadline is not None and clock() >= deadline:
                stopped_by = "max_minutes"
                return
            # max_fetches counts new pages only. Past it, workers still work off the retries, until the deadline.
            new_pages = counts["taken"] < max_fetches
            if watch_queue and new_pages:   # watched pages go ahead of every ranked take, and count toward max_fetches
                counts["taken"] += 1
                counts["in_flight"] += 1
                try:
                    await fetch_watched(watch_queue.popleft())
                finally:
                    counts["in_flight"] -= 1
                continue
            with timed(seconds, "take"):
                entry = frontier.take(seeds, now().date()) if new_pages else frontier.take_retry(seeds, now().date())
            if entry is None:
                if not counts["in_flight"]:
                    if not new_pages:
                        stopped_by = "max_fetches"
                    return
                with engine.slot_timer("waiting"):  # pages in flight may bring links, or free a host
                    await asyncio.sleep(TAKE_WAIT_SECONDS)
                continue
            counts["taken"] += entry["status"] != "retry"
            kind(entry["url"])["taken"] += entry["status"] != "retry"
            counts["in_flight"] += 1
            counts["peak_in_flight"] = max(counts["peak_in_flight"], counts["in_flight"])
            try:
                await fetch_one(entry)
            finally:
                counts["in_flight"] -= 1

    async def read_sitemaps() -> None:
        slots = asyncio.Semaphore(engine.concurrency)

        async def one(host: str, origin: str) -> None:
            async with slots:
                if deadline is not None and clock() >= deadline:
                    return
                try:
                    entries, files_read, files_failed = await sitemaps(origin)  # type: ignore[misc]
                except Exception:
                    logger.exception("sitemaps not read")
                    entries, files_read, files_failed = [], 0, 1
            frontier.sitemap_read[host] = now()
            read["sitemap_sites_read"] += 1
            read["sitemaps_read"] += files_read
            read["sitemap_failures"] += files_failed
            read["sitemap_urls_queued"] += frontier.add_sitemap(host, entries, now())

        await asyncio.gather(*(one(host, origin) for host, origin in frontier.sitemap_sites(seeds, now().date(), now()).items()))

    done = asyncio.Event()

    async def uploads() -> None:
        while True:
            try:
                await asyncio.wait_for(done.wait(), FLUSH_EVERY_SECONDS)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await engine.flush()  # uploads only full or old buffers
            except Exception:
                logger.exception("upload failed; the rows are kept and sent with the next one")

    with timed(seconds, "load_state"):
        await engine.start()
    deadline = None if max_minutes is None else clock() + max_minutes * 60
    uploader = asyncio.create_task(uploads())
    workers: List[asyncio.Task] = []
    abandoned = 0
    try:
        with timed(seconds, "sitemaps"):
            if sitemaps is not None:
                await read_sitemaps()
        with timed(seconds, "fetching"):
            workers = [asyncio.create_task(worker()) for _ in range(engine.concurrency)]
            await asyncio.gather(*workers)
        with timed(seconds, "judge_wait"):
            if judging:
                _, unfinished = await asyncio.wait(list(judging), timeout=JUDGE_GRACE_SECONDS)
                for task in unfinished:
                    task.cancel()
                await asyncio.gather(*unfinished, return_exceptions=True)
                abandoned = len(unfinished)
    except BaseException:
        running = workers + list(judging)
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        done.set()
        await asyncio.gather(uploader, return_exceptions=True)
        await engine.flush(force=True)  # keep the rows already fetched
        raise
    done.set()
    await uploader
    with timed(seconds, "final_save"):
        stats = dict(await engine.finish(frontier.save(workdir) + ([save_watch_times(
            {w["url"]: watch_times[w["url"]] for w in watch if w["url"] in watch_times}, workdir)] if watch else [])))
    queued_by_seed, top_priority_by_seed = frontier.by_seed(seeds, now().date())
    seconds["total"] = time.monotonic() - began
    stats.update(
        {
            "stopped_by": stopped_by, "seeds_added": seeds_added, "links_added": links_added,
            "queued": frontier.count("queued"), "known_urls": len(frontier),
            "lines_active": len(frontier._lines), "lines_spawned": frontier.spawned, "inbound_lifts": frontier.lifts,
            "lines_lifted": len(frontier.lines_lifted), "fetches_by_generation": by_generation,
            "fetches_by_host_page": by_host_page, "fetches_new_domain_share": frontier.new_domain_takes,
            "seed_urls_requeued": frontier.seed_urls_requeued, **read,
            "watch_entries": len(watch), "watch_due": len(due), **watched,
            **{name: {"taken": k["taken"], "stored": k["stored"], "judged": k["judged"],
                      "mean_relevance": round(k["relevance"] / k["judged"], 4) if k["judged"] else None} for name, k in kinds.items()},
            "workers": engine.concurrency, "peak_in_flight": counts["peak_in_flight"], "jev_abandoned": abandoned,
            "retries": counts["retries"], "retry_recovered": counts["retry_recovered"], "retries_expired": retries_expired,
            # Seed ids: these go to the private run record only. __main__ prints top-level numbers alone.
            "stored_by_seed": stored_by_seed, "queued_by_seed": queued_by_seed, "top_priority_by_seed": top_priority_by_seed,
            "hosts_cooled": len(frontier.cooled),
            # Site factors: sites with one other than 1, fetches from sites below and above 1, entries held by a factor of 0.
            "sites_steered": sum(f != 1 for f in frontier._steer.values()),
            "fetches_steered_down": counts["steered_down"], "fetches_steered_up": counts["steered_up"],
            "queued_held_by_steer": sum(len(g.members) for g in frontier._groups.values() if frontier.factor(g.host) == 0),
            "seconds": {key: round(value, 1) for key, value in seconds.items()},
            "seed_url_problems": seed_url_problems, "seed_url_redirects": seed_url_redirects,
            # Pages Jev read as data portals whose figures load from a backend: candidates for a derived client.
            "data_portals": dict(sorted(data_portals.items(), key=lambda item: -item[1])[:20]),
            # What the engine chose not to do in this run. Read these when reviewing the limits.
            "skipped": {**frontier.skipped, "too_large": stats["too_large"],
                        "host_cooled": sum(frontier._queued_per_host.get(host, 0) for host in frontier.cooled)},
            # A seed here has been fetched, in this run or an earlier one, and has never stored
            # anything: a dead or blocked starting URL.
            "seeds_with_nothing": sorted(
                seed.id for seed in seeds
                if (frontier.line(seed.id) or {}).get("fetches", 0) > 0 and frontier.line(seed.id)["content"] == 0
            ),
        }
    )
    return stats
