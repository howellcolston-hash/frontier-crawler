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

Two ranking rules for take(), chosen by the optional config/ranking.json (rank(); absent or broken: fixed). Both multiply in
the seed weight and the site factor (0 still holds a site), keep the retry lane, the per-host in-flight limit, cooled hosts
and the queue caps, and take a group's best entry in the order above. select() is the fixed rule's.

    fixed    the priority above, with the re-test and new-domain shares.
    bandit   seed weight x site factor x a draw from the group's arm; no decay, damping, lift or share.
    shadow   takes as fixed does, and records beside each take what bandit would have taken.

The bandit rule. Each group is an arm; its line holds the evidence (the line, not the registered domain: rewards are
already credited per line, a line is one site, and one domain's hosts can differ in worth). A fetch's reward is what the
line's yield is fed. The evidence is Beta counts discounted by 1 - 1/ARM_WINDOW at each reward, so the estimate follows a
site that dries up and diminishing returns show in a site's own record. A crossing group is the arm of the line it would
become: mean its finder line's estimate x the link's score, ARM_PRIOR fetches strong, one failure more per fetch from its
host that started nothing; a seed is that far from its descendants only through this chain. Allocation is Thompson
sampling, mean + temperature x (draw - mean), over the arms that can take now (a sleeping bandit: an empty group, a full or
cooled host sits out). Thompson rather than an upper-confidence index: draws spread concurrent takes and late judgements
over arms where an index would hand every worker the same one, and temperature is then the randomness itself (0: greedy on
the means, 1: Thompson). A draw is kept until its group is taken, its line's evidence changes, or one of ARM_REFRESH arms
drawn at random per take is drawn again: a take reads a heap top, and stale arms get at most ARM_REFRESH new draws per
take however many arms there are. Staleness: an arm's evidence halves every ARM_STALE_DAYS since its line was last
fetched, down to ARM_PRIOR, so long-untested arms widen and their draws re-test them; their means stay, so recovery is
still through data. That replaces the re-test and new-domain shares: untried arms start wide. The counts are kept in every
mode (state/lines.parquet), so switching modes loses nothing.

state/hosts.parquet counts, per host, what fetching it took: fetches, content, fetches that
needed the PDF or browser tier, refusals, failures and dead links; and when its sitemaps were last read.

The queue on disk (storage/queue.py: state/queue/). Queued entries live in bucket files sorted by group; every other URL
the queue has known is a 64-bit key in a seen set (a never-seen URL is wrongly taken as seen with probability n / 2^64 for
n known: 5e-13 a URL at 10 million, so about 3e-6 URLs ever lost to it over the first 10 million). What a fetched URL last
served, for telling an unchanged page, is the engine's state/url_hashes.parquet; when a watched page was last read, its
state/watch.json. A run holds in memory only its hot set. At its first take, a pass over the buckets scores each group by
its best entry as the ranking rule would (fixed: its priority; bandit: one Thompson draw from its arm, staleness widening
included; shadow: both) and loads the best HOT_PER_GROUP entries of the top groups until HOT_ENTRIES are loaded, plus the
best entry of COLD_SAMPLE groups drawn uniformly from the rest. Those are a group's turn at a re-test: a fixed re-test
draws from them in proportion to the groups left on disk, as it drew from every group before; a bandit arm left out is
reachable through its draw, which staleness widens, and through the sample. When a loaded group's entries fall below the
best still on disk, its next ones are loaded; when the hot set runs dry (fixed: its best falls below the best group left
out), the next groups are. A seed's starting group is always loaded. Groups held by a site factor of 0 or an inactive
seed are not scored or loaded, and are back in the ranking once raised or active. A link into a group left on disk is
appended to it unloaded; it joins the ranking when its group next does. A crossing left on disk when its site became a
line (spawned after it was queued) moves to that line when it is loaded, as _spawn would have moved it. At save, a group
keeps its best MAX_QUEUED_PER_GROUP entries, and past MAX_QUEUED the lowest-ranked groups at the run's first take (fixed
priority, a held site or seed ranked as if not held) are cut whole, so the cap acts a run late. What is cut stays in the
seen set and in the links table, which holds every link of every stored page.

Sitemaps. At the start of a run, a seed's starting sites and the sites with a line on a domain steered above 1 have
their robots.txt and the sitemaps it names (else /sitemap.xml) read, at most once per SITEMAP_EVERY, and up to
SITEMAP_MAX_URLS new URLs each, newest first, are queued as ordinary links (add_sitemap). Discovery only: robots.txt
rules are not applied.

Watched pages (crawl's watch, from config/watch.json) are fetched before any ranked take when due, outside the queue,
up to WATCH_SHARE of the run's fetches; a changed one's new links are queued as links of its site's line.

A page that timed out is retried once, with a longer limit, through take()'s retry lane; a second timeout fails it,
and so does waiting longer than RETRY_MAX_AGE for the retry.

Input contract:
    frontier = Frontier.load(store, workdir); frontier.add_seeds(seeds, now); frontier.rank(mode, temperature)   # optional: fixed
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
import itertools
import json
import logging
import math
import random
import resource
import sys
import time
from array import array
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
from frontier_crawler.storage.queue import QueueFiles, group_key, kept as kept_whole
from frontier_crawler.storage.state import load_watch_times, save_watch_times, url_key
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
# The bandit rule (config/ranking.json); see the module docstring.
RANKING_MODES = ("fixed", "bandit", "shadow")
ARM_WINDOW = 6.0                # PROVISIONAL: fetches an arm's estimate remembers (about as many as the yield's YIELD_ALPHA): each reward discounts the older ones by 1 - 1/ARM_WINDOW
ARM_PRIOR = 2.0                 # PROVISIONAL: pseudo-fetches behind a new arm's prior, so it starts wide
ARM_STALE_DAYS = 14.0           # PROVISIONAL: days without a fetch over which an arm's evidence halves, down to ARM_PRIOR
ARM_REFRESH = 8                 # PROVISIONAL: arms drawn at random whose sample is drawn again at each take
# The queue on disk; see the module docstring.
HOT_ENTRIES = 100_000           # PROVISIONAL: entries a run loads at its first take, best groups first: about a dozen runs' takes
HOT_PER_GROUP = 64              # PROVISIONAL: entries of one group loaded at a time; its next ones load when these run out
COLD_SAMPLE = 2_000             # PROVISIONAL: groups outside the hot set drawn uniformly each run, their best entry loaded
MAX_QUEUED_PER_GROUP = 1_000    # PROVISIONAL: a group's lowest-ranked entries past this are cut at save
MAX_QUEUED = 5_000_000          # PROVISIONAL: queued entries in all; past it the lowest-ranked groups are cut at save

logger = logging.getLogger("frontier_crawler.frontier")

HOSTS_PATH = "state/hosts.parquet"
LINES_PATH = "state/lines.parquet"

_TS = pa.timestamp("us", tz="UTC")
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
        # The bandit's estimate of a fetch from it: discounted Beta counts, kept in every mode; when it was last fetched.
        # Null in files saved before they existed: seeded from yield and fetches on load.
        ("arm_alpha", pa.float64()),
        ("arm_beta", pa.float64()),
        ("arm_at", _TS),
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


class _UrlDown(str):
    """A url that sorts in reverse, so a min-heap of (score, url) drops the largest url first among equal scores."""

    __slots__ = ()

    def __lt__(self, other: str) -> bool:
        return str.__gt__(self, other)


class _Group:
    """The queued entries that share a line, a host and a seed, and so every factor of priority() but the static one.
    members are kept best first: static score down, then url. A group with rows on disk is cold until loaded: its new
    entries wait in pending and it is not ranked. Once hot, its members hold its loaded rows and the rest stay on disk."""

    __slots__ = ("line", "host", "seed", "weight", "members", "version", "slot", "drawn",
                 "hot", "bucket", "start", "disk", "taken", "floor", "pending")

    def __init__(self, line: str, host: str, seed: str, weight: float) -> None:
        self.line, self.host, self.seed, self.weight = line, host, seed, weight
        self.members: List[Dict[str, Any]] = []
        self.version = 0
        self.slot = -1   # its place in Frontier._filled while it has members
        self.drawn = 0   # the version of its current items in the arm heaps
        self.hot = True
        self.bucket = -1                 # its bucket file, once known
        self.start = self.disk = 0       # its rows on disk: where they start in the bucket, and how many are not loaded
        self.taken = 0                   # rows loaded so far, the first of its rows on disk
        self.floor = 0.0                 # static score at weight 1 of the last row loaded: none on disk is higher
        self.pending: Optional[List[Dict[str, Any]]] = None   # entries queued while cold

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
        # The queue on disk (see the module docstring); empty for a queue built in memory.
        self._files = QueueFiles(decay=(HOP_DECAY, GENERATION_DECAY))
        self._loc: List[Tuple[Any, Any, Any]] = []   # per bucket: its groups' keys, sorted, and their first rows and row counts
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
        # Lines saved before arms existed: their yield as the mean, their fetches (within ARM_PRIOR..ARM_WINDOW) as the
        # evidence, last fetched when created (so long-lived ones start a little stale).
        for line in self._lines.values():
            if line.get("arm_alpha") is None or line.get("arm_beta") is None:
                n = min(max(float(line.get("fetches") or 0), ARM_PRIOR), ARM_WINDOW)
                line.update(arm_alpha=line["yield"] * n, arm_beta=(1.0 - line["yield"]) * n, arm_at=line.get("created_at"))
        self._host_line: Dict[str, str] = {}
        self._queued_per_host: Dict[str, int] = {}
        # The queue, ranked by group: see select(). The heaps are built by the first select() or take(), which bring the seed weights.
        self._groups: Dict[Tuple[str, str, str], _Group] = {}
        self._groups_by_line: Dict[str, List[_Group]] = {}
        self._groups_by_host: Dict[str, List[_Group]] = {}
        self._groups_by_domain: Dict[str, List[_Group]] = {}
        self._own_by_line: Dict[str, List[_Group]] = {}   # groups made by a line's own (not crossing) entries: its arm's
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
        # The ranking rule (see rank()). The arm heaps hold (-score, version, group): by drawn sample and by mean.
        self._mode, self._temperature = "fixed", 1.0
        self._arm_heap: Optional[List[Tuple[float, int, _Group]]] = None
        self._mean_heap: List[Tuple[float, int, _Group]] = []
        self._drawn = 0
        self._arm_rng = random.Random(f"{rng_seed}:arms")   # its own, so shadow mode leaves the re-test draws alone
        self._today: Optional[dt.date] = None
        self._tags: Dict[str, str] = {}            # url -> what kind of take it was, until its reward is known
        self._tag_fetch: Dict[str, str] = {}       # fetch_id -> the same, for a page whose judgement is awaited
        self.rewards: Dict[str, List[float]] = {}  # kind of take -> [fetches rewarded, reward]
        self.takes_by_kind: Counter = Counter()    # greedy, explore (bandit); agreed, differed (shadow)
        self.tried: set = set()                    # groups taken from in this run
        self.shadow = dict.fromkeys(("takes", "agreed", "estimate_fixed", "estimate_bandit"), 0.0)
        self._disk_left = 0                  # queued rows on disk not loaded
        self._disk_by_seed: Counter = Counter()
        self._cold_groups = 0                # groups with rows on disk, not loaded
        self._inserted = 0                   # URLs first known in this run
        self._reserve: List[Tuple[float, tuple]] = []   # groups left out of the hot set, worst first
        self._beyond = -math.inf             # the best score at the first take of a group left out of the reserve too
        self._spent = False                  # a pass over the disk found nothing more to load
        self._sample: List[_Group] = []      # the groups drawn uniformly from those left out
        self._cut: Dict[Tuple[str, str, str], int] = {}   # groups to cut at save for MAX_QUEUED: rows on disk
        self._cold_top: Dict[str, float] = {}    # per seed, the best priority of its groups left on disk
        self._cold_held = 0                  # rows on disk of sites steered to 0
        self._hot_rng = random.Random(f"{rng_seed}:hot")
        self.queue: Dict[str, Any] = {       # the run record's queue figures: see queue_record()
            "migrated": False, "load_seconds": 0.0, "select_seconds": 0.0, "save_seconds": 0.0, "bytes_in": 0, "bytes_out": 0,
            "scans": 0, "groups_promoted": 0, "entries_appended_cold": 0, "cut_group_cap": 0, "cut_total_cap": 0, "seen": 0,
            "peak_rss_mb_after_select": None,
        }

    # ------------------------------------------------------------ persistence

    @classmethod
    def load(cls, store: Store, workdir: Path, rng_seed: Any = 0) -> "Frontier":
        """The hosts and lines tables whole; the queue's files (state/queue/, or the older state/frontier.parquet), of
        which only the entries kept whole are read into memory here."""
        began = time.monotonic()
        tables: List[List[Dict[str, Any]]] = []
        size = 0
        for remote in (HOSTS_PATH, LINES_PATH):
            local = Path(workdir) / "state-in" / remote.rsplit("/", 1)[1]
            got = store.get(remote, local)
            tables.append(pq.read_table(local).to_pylist() if got else [])
            size += local.stat().st_size if got else 0
        files = QueueFiles.load(store, workdir, (HOP_DECAY, GENERATION_DECAY))
        frontier = cls(files.kept, *tables, rng_seed=rng_seed)
        frontier._attach(files)
        frontier.queue.update(migrated=files.migrated, bytes_in=size + files.bytes_in, load_seconds=time.monotonic() - began)
        return frontier

    def _attach(self, files: QueueFiles) -> None:
        """Take the queue's files: where each group's rows are, and the queued counts per host and seed. Nothing is loaded."""
        self._files = files
        self._host_line.update(files.host_line)
        self._statuses.update(files.dropped)
        for bucket, rows in enumerate(files.rows):
            keys, starts, counts = array("Q"), array("q"), array("q")
            if rows:
                first, first_rows, sizes = files.runs(bucket, ("line_id", "host", "seed_id"))
                hashed = [group_key(*key) for key in zip(first["line_id"], first["host"], first["seed_id"])]
                order = sorted(range(len(hashed)), key=hashed.__getitem__)
                keys, starts, counts = array("Q", (hashed[i] for i in order)), array("q", (first_rows[i] for i in order)), array("q", (sizes[i] for i in order))
                for host, seed, n in zip(first["host"], first["seed_id"], sizes):
                    self._queued_per_host[host] = self._queued_per_host.get(host, 0) + n
                    self._disk_by_seed[seed] += n
                self._disk_left += sum(sizes)
                self._cold_groups += len(sizes)
            self._loc.append((keys, starts, counts))

    def _locate(self, key: Tuple[str, str, str]) -> Optional[Tuple[int, int, int]]:
        """(bucket, first row, rows) of a group's rows on disk, or None. shortcut: two groups sharing a 64-bit key (about
        n^2 / 2^65 for n groups) would be taken for one."""
        if not self._loc:
            return None
        hashed = group_key(*key)
        bucket = hashed % self._files.buckets
        keys, starts, counts = self._loc[bucket]
        i = bisect.bisect_left(keys, hashed)
        return (bucket, starts[i], counts[i]) if i < len(keys) and keys[i] == hashed else None

    def _known(self, url: str) -> bool:
        """Whether the queue has known url: in memory, or in the seen set."""
        return url in self._entries or (bool(self._files.seen) and self._files.has(url_key(url)))

    def save(self, workdir: Path) -> List[Tuple[Path, str]]:
        """The files of the next state, to upload in one commit: hosts, lines and the queue's (only its changed buckets).
        Applies the caps: MAX_QUEUED_PER_GROUP to every bucket written, MAX_QUEUED to the groups chosen at the first take."""
        began = time.monotonic()
        out = Path(workdir) / "state-out"
        out.mkdir(parents=True, exist_ok=True)
        zero = dict.fromkeys(_HOST_COUNTS, 0)
        host_rows = [{"host": host, **self._hosts.get(host, zero), "sitemap_read_at": self.sitemap_read.get(host)}
                     for host in sorted(set(self._hosts) | set(self.sitemap_read))]
        files: List[Tuple[Path, str]] = []
        for remote, schema, rows in (
            (HOSTS_PATH, _HOSTS_SCHEMA, host_rows),
            (LINES_PATH, _LINES_SCHEMA, [self._lines[line_id] for line_id in sorted(self._lines)]),
        ):
            local = out / remote.rsplit("/", 1)[1]
            columns = {name: [row.get(name) for row in rows] for name in schema.names}
            pq.write_table(pa.Table.from_pydict(columns, schema=schema), local, compression="zstd")
            files.append((local, remote))
        adds: Dict[int, List[Dict[str, Any]]] = {}
        drops: Dict[int, List[Tuple[int, int]]] = {}
        cut = {key: n for key, n in self._cut.items() if key not in self._groups or not self._groups[key].hot}
        cut_total = sum(cut.values())
        for key in cut:
            bucket, start, rows = self._locate(key)  # type: ignore[misc]
            drops.setdefault(bucket, []).append((start, start + rows))
        for group in self._groups.values():   # rows loaded are written from memory, if still queued
            if group.taken:
                drops.setdefault(group.bucket, []).append((group.start, group.start + group.taken))
        kept_rows: List[Dict[str, Any]] = []
        kept_counts: Counter = Counter()
        for entry in self._entries.values():
            if entry["status"] == "queued":
                key = (entry["line_id"], entry["host"], entry["seed_id"])
                if key in cut:
                    cut_total += 1
                else:
                    adds.setdefault(self._bucket(key), []).append(entry)
            elif kept_whole(entry):
                kept_rows.append(entry)
                kept_counts[entry["status"]] += 1
        dropped = {status: n - kept_counts[status] for status, n in sorted(self._statuses.items()) if status != "queued" and n > kept_counts[status]}
        host_line = dict(self._host_line)
        for url in sorted(self._entries):   # as a load from one file of every entry mapped them: a host to its first entry's line
            entry = self._entries[url]
            if entry["host"] and not entry.get("crossing"):
                host_line.setdefault(entry["host"], entry["line_id"])
        queue_files, figures = self._files.save(out, adds, drops, self._entries, kept_rows, host_line, dropped, MAX_QUEUED_PER_GROUP)
        files += queue_files
        self.queue.update(cut_group_cap=figures["cut"], cut_total_cap=cut_total, seen=figures["seen"],
                          bytes_out=sum(local.stat().st_size for local, _ in files), save_seconds=time.monotonic() - began)
        return files

    def _bucket(self, key: Tuple[str, str, str]) -> int:
        group = self._groups.get(key)   # none for an entry out with take() when _spawn moved it
        if group is None:
            return group_key(*key) % self._files.buckets
        if group.bucket < 0:
            group.bucket = group_key(*key) % self._files.buckets
        return group.bucket

    def queue_record(self) -> Dict[str, Any]:
        """The run record's queue figures: counts, seconds and bytes only."""
        hot = [group for group in self._groups.values() if group.hot and group.members]
        pending = sum(len(group.pending) for group in self._groups.values() if group.pending)
        return {
            **{key: round(value, 2) if isinstance(value, float) else value for key, value in self.queue.items()},
            "groups_hot": len(hot), "groups_cold": self._cold_groups,
            "entries_hot": sum(len(group.members) for group in hot), "entries_cold": self._disk_left + pending,
        }

    # ------------------------------------------------------------ reading

    def __len__(self) -> int:
        """URLs the queue has known."""
        return len(self._files.seen) + self._inserted if self._files.seen else len(self._entries)

    def entry(self, url: str) -> Optional[Dict[str, Any]]:
        return self._entries.get(url)

    def line(self, line_id: str) -> Optional[Dict[str, Any]]:
        return self._lines.get(line_id)

    def count(self, status: str) -> int:
        return self._statuses[status] + (self._disk_left if status == "queued" else 0)

    def steer(self, sites: Dict[str, float]) -> None:
        """Set the site factors, per registered domain. The heaps are rebuilt for them by the next select() or take()."""
        self._steer = dict(sites)
        self._weights = None

    def rank(self, mode: str, temperature: float = 1.0) -> None:
        """Choose the ranking rule for take(): "fixed", "bandit" or "shadow" (fixed, with bandit's choice recorded beside
        it). temperature scales the bandit's exploration: 0 greedy on its estimates, 1 Thompson sampling."""
        assert mode in RANKING_MODES and temperature >= 0
        self._mode, self._temperature = mode, temperature
        self._weights = None   # every heap is rebuilt for it by the next take()

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
        self, line_id: str, seed_id: str, parent: Optional[Dict[str, Any]], root_url: str, now: dt.datetime, base: float = 1.0,
    ) -> Dict[str, Any]:
        """A seed line (parent None) starts at yield and relevance 1 and generation 0; a spawned line at its parent's
        yield and relevance, one generation down. Its arm starts at the prior of the crossing that found it (base: that
        link's score); a seed line's at 1."""
        alpha, beta = self._prior(parent, base) if parent else (ARM_PRIOR, 0.0)
        line = {
            "line_id": line_id, "seed_id": seed_id, "parent_line": parent and parent["line_id"], "root_url": root_url,
            "created_at": now, "fetches": 0, "content": 0, "yield": parent["yield"] if parent else 1.0,
            "relevance": parent["relevance"] if parent else 1.0,
            "inbound": 0, "generation": parent["generation"] + 1 if parent else 0,
            "arm_alpha": alpha, "arm_beta": beta, "arm_at": None,
        }
        self._lines[line_id] = line
        return line

    def _record_yield(self, line: Dict[str, Any], value: Optional[float]) -> None:
        """value None: new content whose judgement is still coming. It counts as content and leaves the yield where it was."""
        line["fetches"] += 1
        line["content"] += value is None or value > 0
        self._move_yield(line, value)

    def _move_yield(self, line: Dict[str, Any], value: Optional[float]) -> None:
        """One step of the line's running yield toward value (none for None), and the same reward for its arm."""
        if value is not None:
            self._set_yield(line, (1.0 - YIELD_ALPHA) * line["yield"] + YIELD_ALPHA * value)
            keep = 1.0 - 1.0 / ARM_WINDOW
            line["arm_alpha"] = keep * line["arm_alpha"] + value
            line["arm_beta"] = keep * line["arm_beta"] + 1.0 - value
            for group in self._own_by_line.get(line["line_id"], ()):
                self._draw(group)

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
            self._new_line(line_id, entry["seed_id"], self._lines[entry["line_id"]], entry["url"], now, entry["base"])
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
        if not url or self._known(url):
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
        self.queue["entries_appended_cold"] += not self._groups[(line_id, host, seed_id)].hot
        return True

    def _insert(self, entry: Dict[str, Any]) -> None:
        entry["seq"] = self._seq
        self._seq += 1
        self._inserted += 1
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
        """Queue the seeds' starting URLs not yet known, and again those whose own fetch served nothing. An active seed's
        starting group is loaded from disk, so its starting URLs are ranked in this run."""
        added = 0
        for seed in seeds:
            for url in seed.urls:
                known = self._entries.get(url)
                if known is None and self._known(url):   # queued on disk, or fetched by another route
                    continue
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
        if self._loc:
            starting = {(seed.id, urlsplit(url).netloc, seed.id) for seed in seeds if seed.active(now.date()) for url in seed.urls}
            cold = [self._group(dict(zip(("line_id", "host", "seed_id"), key))) for key in sorted(starting) if self.factor(key[1]) > 0]
            cold = [group for group in cold if not group.hot]
            self._heat([(group, HOT_PER_GROUP) for group in cold])
            self.queue["groups_promoted"] += len(cold)
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
            if not self._known(href):
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
            if self._known(url):
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
            for group in self._groups_by_host.get(entry["host"], ()):   # one more fetch that started nothing: its arm is lower
                if group.members and group.members[0].get("crossing"):
                    self._draw(group)
        else:
            line = self._lines[entry["line_id"]]
            if value is None and fetch_id not in self._judging:
                value = line["relevance"]   # not judged, and none coming: it counts at what the line's judged pages were worth
            elif answers is not None and value is not None:
                self._note_relevance(line, value)
            line["arm_at"] = now
            self._record_yield(line, value)
            if fetch_id in self._judging:   # a crossing's judgement moves the line it started, not the one that found it
                self._judging[fetch_id] = (line["line_id"], self._judging[fetch_id][1])
        tag = self._tags.pop(entry["url"], None)
        if tag is not None and value is None:
            self._tag_fetch[fetch_id] = tag   # its reward comes with its judgement
        elif tag is not None:
            self._reward(tag, value)
        # A redirect target is the same document. Record it so a later link to it is not fetched again.
        final = normalise(row.get("url_final") or "")
        if final and not self._known(final):
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
        tag = self._tag_fetch.pop(fetch_id, None)
        if answers is None:
            if tag is not None:
                self._reward(tag, line["relevance"])
            self._move_yield(line, line["relevance"])   # it will not be judged: it counts as mark() counts such a page
            return
        value = judged_yield(answers)
        if tag is not None:
            self._reward(tag, value)
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
            if not entry.get("crossing"):
                self._own_by_line.setdefault(key[0], []).append(group)
            on_disk = self._locate(key)
            if on_disk is not None:
                group.hot, group.pending = False, []
                group.bucket, group.start, group.disk = on_disk
        return group

    def _enqueue(self, entry: Dict[str, Any]) -> None:
        group = self._group(entry)
        if not group.hot:
            group.pending.append(entry)  # type: ignore[union-attr]
            return
        members = group.members
        i = bisect.bisect_left(members, group.key(entry), key=group.key)
        members.insert(i, entry)
        if len(members) == 1:
            group.slot = len(self._filled)
            self._filled.append(group)
        if len(members) == 1 or (i == 0 and entry.get("crossing")):   # a crossing's arm starts from its best link
            self._draw(group)
        if self._weights is not None and self._mode != "bandit" and self._eligible(group):
            extras = self._extras(group)
            # A later entry can still lead: two static scores may round to one score, and then the url decides.
            if i == 0 or self.priority(entry, group.weight, *extras) == self.priority(members[0], group.weight, *extras):
                self._push(group)

    def _dequeue(self, entry: Dict[str, Any]) -> None:
        group = self._groups[(entry["line_id"], entry["host"], entry["seed_id"])]
        if not group.hot:
            group.pending.remove(entry)  # type: ignore[union-attr]
            return
        i = bisect.bisect_left(group.members, group.key(entry), key=group.key)
        assert group.members[i] is entry
        del group.members[i]
        if not group.members:   # swap the last filled group into its slot
            last = self._filled.pop()
            if last is not group:
                self._filled[group.slot], last.slot = last, group.slot
            group.slot = -1
        if group.disk:
            self._top_up(group)

    # ------------------------------------------------------------ the queue on disk

    def _top_up(self, group: _Group) -> None:
        """Load a hot group's next rows while its best entry in memory is below the best left on disk (or it has none)."""
        while group.disk and (not group.members or _static(group.members[0], 1.0) < group.floor):
            rows = self._files.read(group.bucket, [(group.start + group.taken, min(HOT_PER_GROUP, group.disk))])[0]
            self._take_rows(group, rows)

    def _take_rows(self, group: _Group, rows: List[Dict[str, Any]]) -> None:
        group.taken += len(rows)
        group.disk -= len(rows)
        self._disk_left -= len(rows)
        if rows:
            group.floor = _static(rows[-1], 1.0)
        for row in sorted(rows, key=lambda row: row["url"]):   # numbered in url order, as entries read from one file were
            self._disk_by_seed[row["seed_id"]] -= 1
            if row["url"] in self._entries:   # never twice
                continue
            for name in ("host", "line_id", "seed_id"):   # shared by a group's rows
                row[name] = sys.intern(row[name])
            row["seq"] = self._seq
            self._seq += 1
            line = self._lines.get(self._host_line.get(row["host"], ""))
            if row.get("crossing") and line and line.get("parent_line") and line["created_at"] >= (row["discovered_at"] or line["created_at"]):
                row.update(line_id=line["line_id"], crossing=False, hops=1)   # its site became a line since: moved as _spawn moves
            self._entries[row["url"]] = row
            self._statuses["queued"] += 1
            if row.get("crossing"):
                self._crossing.setdefault(row["host"], []).append(row)
            self._enqueue(row)

    def _heat(self, wanted: Sequence[Tuple[_Group, int]]) -> None:
        """Load cold groups: the best n rows of each, one read per bucket, with what was queued for it while cold."""
        reads: Dict[int, List[Tuple[_Group, int]]] = {}
        for group, n in wanted:
            if group.hot:
                continue
            group.hot, pending, group.pending = True, group.pending, None
            self._cold_groups -= 1
            for entry in pending:  # type: ignore[union-attr]
                self._enqueue(entry)
            reads.setdefault(group.bucket, []).append((group, min(n, group.disk)))
        for bucket, groups in reads.items():
            for (group, _), rows in zip(groups, self._files.read(bucket, [(g.start + g.taken, n) for g, n in groups])):
                self._take_rows(group, rows)
        for group, _ in wanted:   # rows moved to their site's line can leave a group with none
            self._top_up(group)

    def _cold(self, item: tuple) -> _Group:
        (line, host, seed), crossing = item[:2]
        return self._group({"line_id": line, "host": host, "seed_id": seed, "crossing": crossing})

    def _sample_arm(self, line: str, best: Dict[str, Any], host: str) -> float:
        """One Thompson draw from a group's arm, as _draw makes, from the hot set's own generator."""
        mean, n = self._arm(line, best, host)
        if self._temperature <= 0:
            return mean
        x = self._hot_rng.betavariate(max(mean * n, 1e-3), max((1.0 - mean) * n, 1e-3))
        return mean + self._temperature * (x - mean)

    def _select(self, weights: Dict[str, float], today: dt.date, again: bool = False) -> int:
        """Score every group on disk by its best entry and load the hot set and the uniform sample (see the module
        docstring); keep the next best as the reserve, and choose the groups MAX_QUEUED cuts at save. Returns the groups
        loaded. again: a later pass, for the groups still on disk, whose hot set counts as promoted."""
        if not self._disk_left:
            return 0
        began = time.monotonic()
        self._today = today
        rules = {"fixed": ("fixed",), "bandit": ("draw",), "shadow": ("fixed", "draw")}[self._mode]
        heaps: Dict[str, list] = {rule: [] for rule in rules}
        totals = dict.fromkeys(rules, 0)
        beyond = -math.inf   # the best score of a group neither loaded nor kept in reserve
        order = itertools.count()
        excess = sum(self._files.rows) - MAX_QUEUED
        cuts: list = []
        cut_rows, eligible = 0, 0
        sample: List[tuple] = []
        self._cold_top, self._cold_held = {}, 0
        for bucket, rows in enumerate(self._files.rows):
            if not rows:
                continue
            first, _, counts = self._files.runs(bucket, ("line_id", "host", "seed_id", "base", "hops", "crossing", "url"))
            for i, count in enumerate(counts):
                line, host, seed = key = (first["line_id"][i], first["host"][i], first["seed_id"][i])
                group = self._groups.get(key)
                if (group is not None and group.hot) or line not in self._lines:
                    continue
                best = {"line_id": line, "host": host, "base": first["base"][i], "hops": first["hops"][i], "crossing": first["crossing"][i]}
                factor = self.factor(host)
                weight = weights.get(seed, 0.0) * factor
                self._cold_held += count if factor == 0 else 0
                if seed in weights:
                    self._cold_top[seed] = max(self._cold_top.get(seed, 0.0), round(self.priority(best, weight), 4))
                if excess > 0:   # keep the lowest-ranked groups that hold the excess, a held site or seed ranked as if not held
                    heapq.heappush(cuts, (-self.priority(best, (weights.get(seed) or 1.0) * (factor or 1.0)), next(order), key, count))
                    cut_rows += count
                    while cut_rows - cuts[0][3] >= excess:
                        cut_rows -= heapq.heappop(cuts)[3]
                if weight <= 0 or host in self.cooled:
                    continue
                item = (key, bool(best["crossing"]), count, first["url"][i])
                for rule in rules:   # the best groups holding the hot set and a reserve as large
                    score = self.priority(best, weight) if rule == "fixed" else weight * self._sample_arm(line, best, host)
                    heapq.heappush(heaps[rule], (score, _UrlDown(item[3]), item))
                    totals[rule] += min(count, HOT_PER_GROUP)
                    while totals[rule] - min(heaps[rule][0][2][2], HOT_PER_GROUP) >= 2 * HOT_ENTRIES:
                        dropped = heapq.heappop(heaps[rule])
                        totals[rule] -= min(dropped[2][2], HOT_PER_GROUP)
                        beyond = max(beyond, dropped[0]) if rule == rules[0] else beyond
                eligible += 1
                if len(sample) < COLD_SAMPLE:   # reservoir sampling
                    sample.append(item)
                elif (j := self._hot_rng.randrange(eligible)) < COLD_SAMPLE:
                    sample[j] = item
        hot: Dict[Tuple[str, str, str], tuple] = {}
        self._reserve = []
        for rule in rules:
            loaded = 0
            for score, _, item in sorted(heaps[rule], key=lambda t: (-t[0], t[2][3])):   # best first, ties by url as take() breaks them
                if loaded < HOT_ENTRIES:
                    hot[item[0]] = item
                    loaded += min(item[2], HOT_PER_GROUP)
                elif rule == rules[0]:
                    self._reserve.append((score, item))
        self._reserve.reverse()
        self._beyond = beyond
        self._heat([(self._cold(item), HOT_PER_GROUP) for item in hot.values()])
        self._sample = [self._cold(item) for item in sample if item[0] not in hot]
        self._heat([(group, 1) for group in self._sample])
        self._cut = {key: count for _, _, key, count in cuts}
        self.queue["groups_promoted"] += len(self._sample) + (len(hot) if again else 0)
        self.queue["scans"] += 1
        self.queue["select_seconds"] += time.monotonic() - began
        self.queue["peak_rss_mb_after_select"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)   # Linux: KB
        return len(hot) + len(self._sample)

    def _refill(self) -> None:
        """Load the next groups left out of the hot set while it has run dry: no group left to take from, or (fixed, shadow)
        its best at or below the best group left out. With the reserve used up, the groups still on disk are scored again."""
        while self._disk_left:
            if self._mode == "bandit":
                heap = self._arm_heap or []
                while heap and (heap[0][1] != heap[0][2].drawn or not self._eligible(heap[0][2])):
                    heapq.heappop(heap)
                if heap:
                    return
            else:
                top = self._top(self._heap)
                if top is not None and -top[0] > (self._reserve[-1][0] if self._reserve else self._beyond):
                    return
            if not self._reserve:
                if self._spent or not self._select(self._weights or {}, self._today or dt.date.today(), again=True):
                    self._spent = True   # none left on disk that may be taken: not scored again in this run
                    return
                continue
            batch: List[Tuple[_Group, int]] = []
            loaded = 0
            while self._reserve and (not batch or loaded < HOT_ENTRIES // 10):
                group = self._cold(self._reserve.pop()[1])
                if not group.hot and group.host not in self.cooled:
                    batch.append((group, HOT_PER_GROUP))
                    loaded += min(group.disk, HOT_PER_GROUP)
            self._heat(batch)
            self.queue["groups_promoted"] += len(batch)

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
        if self._weights is None or self._mode == "bandit" or not self._eligible(group):   # bandit mode reads the arm heaps alone
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
        self._today = today
        weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
        if weights != self._weights or len(self._heap) > 2 * len(self._groups) + 1000:
            if weights != self._weights:
                self._arm_heap = None
                self._select(weights, today)
            self._weights = weights
            self._heap, self._new_heap = [], []
            for group in self._groups.values():
                if group.seed in weights and group.weight != weights[group.seed] * self.factor(group.host):
                    group.weight = weights[group.seed] * self.factor(group.host)
                    group.members.sort(key=group.key)
                self._push(group)
        if self._mode != "fixed" and self._arm_heap is None:
            self._arm_heap, self._mean_heap = [], []
            for group in self._groups.values():
                self._draw(group)
        elif self._arm_heap is not None and len(self._arm_heap) > 4 * len(self._groups) + 1000:   # drop old items, keep draws
            self._arm_heap, self._mean_heap = ([item for item in heap if item[1] == item[2].drawn] for heap in (self._arm_heap, self._mean_heap))
            heapq.heapify(self._arm_heap)
            heapq.heapify(self._mean_heap)

    # ------------------------------------------------------------ the bandit rule
    #
    # Each group is an arm; its line's discounted Beta counts are the evidence (a crossing group's: the prior of the line it
    # would become). An arm's sample is drawn when it gets items and kept until it is taken, its evidence changes, or a
    # refresh draws it again, so a take reads the heap top instead of drawing every arm.

    def _prior(self, parent: Dict[str, Any], base: float, zeros: int = 0) -> Tuple[float, float]:
        """Beta counts for a site found from parent's pages by a link of score base: mean the parent's estimate x base,
        ARM_PRIOR strong, and one failure for each fetch from it that started nothing."""
        mean = parent["arm_alpha"] / (parent["arm_alpha"] + parent["arm_beta"]) * base
        return mean * ARM_PRIOR, (1.0 - mean) * ARM_PRIOR + zeros

    def _arm(self, line_id: str, first: Dict[str, Any], host: str) -> Tuple[float, float]:
        """(mean, evidence) of a fetch from the group of line_id and host whose best entry is first: its evidence halved
        every ARM_STALE_DAYS since its line was last fetched, down to ARM_PRIOR (or what it had, if less)."""
        line = self._lines[line_id]
        if first.get("crossing"):
            alpha, beta = self._prior(line, first["base"], self._hosts.get(host, {}).get("fetches", 0))
            return alpha / (alpha + beta), alpha + beta
        alpha, beta, at = line["arm_alpha"], line["arm_beta"], line.get("arm_at")
        n = alpha + beta
        if at is not None and self._today is not None and self._today > at.date():
            n = max(min(n, ARM_PRIOR), n * 0.5 ** ((self._today - at.date()).days / ARM_STALE_DAYS))
        return alpha / (alpha + beta), n

    def _draw(self, group: _Group) -> None:
        """Draw the group's sample again and push it, and its mean, to the arm heaps."""
        if self._arm_heap is None or not self._eligible(group):
            return
        mean, n = self._arm(group.line, group.members[0], group.host)
        sample = mean
        if self._temperature > 0:
            x = self._arm_rng.betavariate(max(mean * n, 1e-3), max((1.0 - mean) * n, 1e-3))
            sample = mean + self._temperature * (x - mean)
        self._drawn += 1
        group.drawn = self._drawn
        heapq.heappush(self._arm_heap, (-group.weight * sample, self._drawn, group))
        heapq.heappush(self._mean_heap, (-group.weight * mean, self._drawn, group))

    def _arm_top(self, heap: List[Tuple[float, int, _Group]]) -> Optional[_Group]:
        """The best current, eligible group whose host has room, or None."""
        full = []
        while heap:
            _, version, group = heap[0]
            if version != group.drawn or not self._eligible(group):
                heapq.heappop(heap)
            elif self._flight_host.get(group.host, 0) >= MAX_IN_FLIGHT_PER_HOST:
                full.append(heapq.heappop(heap))
            else:
                break
        best = heap[0][2] if heap else None
        for kept in full:
            heapq.heappush(heap, kept)
        return best

    def _reward(self, kind: str, value: float) -> None:
        tally = self.rewards.setdefault(kind, [0, 0.0])
        tally[0] += 1
        tally[1] += value

    def ranking_record(self) -> Dict[str, Any]:
        """The run record's ranking figures: counts and means only."""
        def mean(kind: str) -> Optional[float]:
            n, total = self.rewards.get(kind, (0, 0.0))
            return round(total / n, 4) if n else None

        takes = int(self.shadow["takes"])
        return {
            "mode": self._mode, "temperature": self._temperature, "arms": len(self._filled) + self._cold_groups, "arms_tried": len(self.tried),
            "greedy_takes": self.takes_by_kind["greedy"], "exploration_takes": self.takes_by_kind["explore"],
            "mean_reward_greedy": mean("greedy"), "mean_reward_exploration": mean("explore"),
            "shadow_takes": takes, "shadow_agreed": int(self.shadow["agreed"]),
            "shadow_mean_reward_agreed": mean("agreed"), "shadow_mean_reward_differed": mean("differed"),
            "shadow_mean_estimate_fixed": round(self.shadow["estimate_fixed"] / takes, 4) if takes else None,
            "shadow_mean_estimate_bandit": round(self.shadow["estimate_bandit"] / takes, 4) if takes else None,
        }

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
        The fixed rule's rounds only: in bandit mode the queue hands out one take() at a time.
        """
        assert self._mode != "bandit", "select() ranks by the fixed rule"
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
        be taken, with no such limit; its status is still "retry".

        In bandit mode the ranked take is the bandit rule's, with no re-test or new-domain turns; in shadow mode it is
        the fixed rule's, and what the bandit rule would have taken is recorded beside it."""
        entry = self.take_retry(seeds, today, MAX_RETRY_IN_FLIGHT)
        if entry is not None:
            return entry
        self._refill()
        if self._mode == "bandit":
            entry = self._take_bandit()
        else:
            would = self._arm_top(self._arm_heap) if self._arm_heap is not None else None
            would_entry = self._best(would, *self._extras(would))[1] if would is not None else None  # type: ignore[index]
            entry = self._take_fixed()
            if self._mode == "shadow" and entry is not None:
                agreed = entry is would_entry
                self._tag(entry, "agreed" if agreed else "differed")
                self.shadow["takes"] += 1
                self.shadow["agreed"] += agreed
                self.shadow["estimate_fixed"] += self._arm(entry["line_id"], entry, entry["host"])[0]
                if would is not None:
                    self.shadow["estimate_bandit"] += self._arm(would.line, would_entry, would.host)[0]  # type: ignore[arg-type]
                    self._draw(would)   # its draw is spent, as if it had been taken
                self._draw(self._groups[(entry["line_id"], entry["host"], entry["seed_id"])])
                self._refresh()
        if entry is not None:
            self.tried.add((entry["line_id"], entry["host"], entry["seed_id"]))
            return entry
        return self.take_retry(seeds, today)   # nothing else to take: idle workers work off the retries

    def _take_fixed(self) -> Optional[Dict[str, Any]]:
        self._takes += 1
        if self._takes % round(1 / RETEST_SHARE) == 0:
            for _ in range(RETEST_DRAWS if self._filled else 0):
                pool = self._filled   # the sample stands for the groups left on disk, in proportion to them
                if self._sample and self._rng.random() * (self._cold_groups + len(self._filled)) < self._cold_groups:
                    pool = self._sample
                group = self._rng.choice(pool)
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
        return None

    def _take_bandit(self) -> Optional[Dict[str, Any]]:
        """The best entry of the group with the highest sample. A take that is not the group with the highest mean is
        an exploration take."""
        group = self._arm_top(self._arm_heap)  # type: ignore[arg-type]
        if group is None:
            return None
        greedy = self._arm_top(self._mean_heap)
        entry = self._best(group, *self._extras(group))[1]  # type: ignore[index]
        self._dequeue(entry)
        self._hold(entry)
        self._tag(entry, "greedy" if greedy is group else "explore")
        self._draw(group)   # its next take gets a fresh draw
        self._refresh()
        return entry

    def _tag(self, entry: Dict[str, Any], kind: str) -> None:
        self._tags[entry["url"]] = kind
        self.takes_by_kind[kind] += 1

    def _refresh(self) -> None:
        """Draw ARM_REFRESH arms again, picked uniformly, so an arm that drew low once is not held there."""
        for _ in range(ARM_REFRESH if self._filled else 0):
            self._draw(self._arm_rng.choice(self._filled))

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
        for seed in weights:   # rows on disk, at their best's priority when the hot set was chosen
            queued[seed] += self._disk_by_seed.get(seed, 0)
            if seed in self._cold_top:
                top[seed] = max(top.get(seed, 0.0), self._cold_top[seed])
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
        self._tags.pop(entry["url"], None)
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
    ranking: Tuple[str, float] = ("fixed", 1.0),
) -> Dict[str, Any]:
    """Fetch in a rolling stream until max_fetches new pages are taken and no retry waits, max_minutes has passed,
    or nothing is left to take. steer: the site factors, per registered domain (see the module docstring).
    sitemaps: HarvesterCascade.sitemap_entries, or None to read none. Before the first take, each site sitemap_sites()
    finds due has its sitemaps read and add_sitemap() queues what they list; a failure is counted and the crawl goes on.
    watch: watched pages, {"id", "url", "every_hours"} each. Those never served or last served every_hours ago or more
    are fetched before any ranked take, oldest due first, up to WATCH_SHARE of max_fetches. Each is a fetch row with
    line_id "watch:<id>"; the links of a changed one are queued as links of the line covering its host. When each was
    last served is kept in state/watch.json. ranking: (mode, temperature) for Frontier.rank().

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
    frontier.rank(*ranking)
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
            "queued_held_by_steer": frontier._cold_held + sum(len(g.members) + len(g.pending or ()) for g in frontier._groups.values() if frontier.factor(g.host) == 0),
            # The queue on disk: groups and entries in memory (hot) or not, what was loaded beyond the hot set, appended
            # unloaded, cut by the caps, URLs known, seconds and bytes for its state, and the peak memory once loaded.
            "queue": frontier.queue_record(),
            "seconds": {key: round(value, 1) for key, value in seconds.items()},
            "ranking": frontier.ranking_record(),
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
