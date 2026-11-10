"""The crawl queue: what to fetch next, how the crawl branches, and when a branch stops.

Seeds (config/seeds.jsonl in the store) are starting points, not permanent tracks. The crawl is
organised into lines. A line is one direction of research: it starts at a seed, or at a
site that another line reached and found content on. Lines multiply as the crawl finds
new sites, each with its own counters, and each one ends when it stops paying.

Branching. A link to a site that no line covers is queued under the line that found it.
If fetching it yields content, that site becomes a new line and the parent line carries on.

Freezing. Every line keeps a running yield: 1 for a fetch that brought new content (Jev's
relevance for it once judged), 0 for a dead link, a duplicate, an unchanged page or a
refusal. A line whose yield stays under FREEZE_YIELD after MIN_FETCHES_TO_FREEZE fetches is
frozen: its queue is kept and nothing in it is fetched. A frozen line thaws when other
lines link into it THAW_INBOUND times. Growth is bounded by the fetches per run, so lines
compete for a fixed capacity.

    priority = seed weight x base x HOP_DECAY^hops x line yield
               / (1 + fetched from this host / HOST_HALF)
               / (1 + fetched in this line / LINE_HALF)

base is 1 for a seed. For a link it comes from Jev's answers about the page it was found on
(half "leads to further records", half relevance), or DEFAULT_LINK_BASE when that page has
not been judged. hops count from the start of the line. There are no source tiers: any
http(s) link can be followed.

state/hosts.parquet counts, per host, what fetching it took: fetches, content, fetches that
needed the PDF or browser tier, refusals, failures and dead links.

Input contract:
    frontier = Frontier.load(store, workdir); frontier.add_seeds(seeds, now)
    picks = frontier.select(n, seeds, today)
    frontier.mark(entry, fetch_row, answers, now); frontier.add_links(entry, fetch_row, answers, now)
    files = frontier.save(workdir)          # upload with store.put_many
    stats = await crawl(store, engine, seeds, workdir=..., max_fetches=..., batch=...)
Every constant below is PROVISIONAL.
"""

from __future__ import annotations

import datetime as dt
import heapq
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urldefrag, urljoin, urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq

from frontier_crawler.harvester.cascade import Target
from frontier_crawler.judge.jev import JevResult
from frontier_crawler.judge.questions import relevance
from frontier_crawler.storage.store import Store

MAX_HOPS = 5                    # links this many steps from the start of a line are the last ones queued
HOP_DECAY = 0.7
DEFAULT_LINK_BASE = 0.3         # for links from a page Jev has not judged
MIN_LINK_BASE = 0.05
HOST_HALF = 25.0                # fetches from one host at which its priority halves
LINE_HALF = 100.0               # fetches in one line at which its priority halves
YIELD_ALPHA = 0.15              # weight of the newest fetch in a line's running yield
FREEZE_YIELD = 0.1              # a line is frozen when its running yield falls below this
MIN_FETCHES_TO_FREEZE = 10      # ... and it has had at least this many fetches (since any thaw)
THAW_INBOUND = 3                # links into a frozen line from other lines that thaw it
THAW_YIELD = 0.5                # the running yield a thawed line restarts with
NEW_HOST_SHARE = 0.2            # part of each round kept for hosts never fetched before
HOST_ROUND_SHARE = 0.1          # most of one round that one host may take (at least 2)
MAX_LINKS_PER_PAGE = 300
MAX_QUEUED_PER_HOST = 2000
PORTAL_THRESHOLD = 0.7          # Jev's data_portal answer at which a page is listed as a candidate for a derived client

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
        # queued | fetched | failed | alias (a redirect target of a fetched URL) |
        # unparseable (a link as written on its page that could not be read as a URL; never fetched)
        ("status", pa.string()),
        ("discovered_at", _TS),
        ("fetched_at", _TS),
    ]
)
_HOST_COUNTS = ("fetches", "content", "heavy", "refused", "errors", "error_pages")
_HOSTS_SCHEMA = pa.schema([("host", pa.string())] + [(name, pa.int64()) for name in _HOST_COUNTS])

_LINES_SCHEMA = pa.schema(
    [
        ("line_id", pa.string()),
        ("seed_id", pa.string()),
        ("parent_line", pa.string()),
        ("root_url", pa.string()),
        ("created_at", _TS),
        ("fetches", pa.int64()),
        ("content", pa.int64()),   # fetches that brought something new
        ("since_thaw", pa.int64()),
        ("yield", pa.float64()),
        ("status", pa.string()),  # active | frozen
        ("frozen_at", _TS),
        ("inbound", pa.int64()),  # links from other lines since it was frozen
    ]
)

# Files that are never documents. Everything else, including PDFs and spreadsheets, is followed.
_SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp", ".css", ".js", ".mjs", ".map",
    ".woff", ".woff2", ".ttf", ".eot", ".mp3", ".mp4", ".avi", ".mov", ".webm", ".wav", ".exe", ".dmg", ".apk",
)
# Share buttons point at a form on a social site, not at a source.
_SHARE_MARKERS = ("/sharer", "/share?", "/intent/tweet", "/shareArticle", "wa.me/?text", "api.whatsapp.com/send?text")
_REFUSED_STATUS = {401, 403, 407, 429, 451, 503}
_HEAVY_TIERS = {"PDF", "HEADLESS_PLAYWRIGHT"}


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


def normalise(url: str) -> str:
    """Lower-case scheme and host, drop the fragment. Empty when the URL is not http(s) or does not parse.

    A link that is dropped here is not lost: the page it was on is stored, so a better reader can pick it up later.
    """
    try:
        parts = urlsplit(urldefrag(url.strip())[0])
    except ValueError:
        return ""
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        return ""
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path or "/", parts.query, ""))


def extract_links(body: bytes, base_url: str, limit: int = MAX_LINKS_PER_PAGE) -> Tuple[List[Tuple[str, str]], List[str]]:
    """((url, anchor text) for each distinct link in an HTML page, in page order; links that did not parse, as written)."""
    from lxml import html as lxml_html

    try:
        tree = lxml_html.fromstring(body)
    except Exception:
        return [], []
    own = normalise(base_url)
    found: Dict[str, str] = {}
    unparseable: List[str] = []
    for anchor in tree.iter("a"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        try:
            url = normalise(urljoin(base_url, href))
        except ValueError:
            unparseable.append(href[:500])
            continue
        if not url or url == own or url in found:
            continue
        lowered = url.lower()
        if urlsplit(lowered).path.endswith(_SKIP_EXTENSIONS) or any(marker.lower() in lowered for marker in _SHARE_MARKERS):
            continue
        found[url] = " ".join((anchor.text_content() or "").split())[:200]
        if len(found) >= limit:
            break
    return list(found.items()), unparseable


def _looks_like_html(row: Dict[str, Any]) -> bool:
    content_type = (row.get("content_type") or "").lower()
    if "html" in content_type:
        return True
    return not content_type and (row.get("body") or b"").lstrip()[:1] == b"<"


def link_base(answers: Optional[JevResult]) -> float:
    if answers is None:
        return DEFAULT_LINK_BASE
    nouls = answers.nouls()
    leads = nouls.get("leads_to_records")
    if leads is None:
        return DEFAULT_LINK_BASE
    return max(MIN_LINK_BASE, min(1.0, 0.5 * leads + 0.5 * (relevance(nouls) or 0.0)))


def fetch_yield(row: Dict[str, Any], answers: Optional[JevResult]) -> float:
    """What one fetch was worth to its line: 0 to 1."""
    if row.get("outcome") != "stored" or row.get("block_marker"):
        return 0.0
    if answers is not None:
        judged = relevance(answers.nouls())
        if judged is not None:
            return max(0.0, min(1.0, judged))
    return 1.0


class Frontier:
    def __init__(
        self,
        entries: Optional[List[Dict[str, Any]]] = None,
        hosts: Optional[List[Dict[str, Any]]] = None,
        lines: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self._entries: Dict[str, Dict[str, Any]] = {entry["url"]: entry for entry in (entries or [])}
        self._hosts: Dict[str, Dict[str, int]] = {
            row["host"]: {name: int(row.get(name) or 0) for name in _HOST_COUNTS} for row in (hosts or [])
        }
        self._lines: Dict[str, Dict[str, Any]] = {line["line_id"]: line for line in (lines or [])}
        self._host_line: Dict[str, str] = {}
        self._queued_per_host: Dict[str, int] = {}
        for entry in self._entries.values():
            if entry["status"] == "queued":
                self._queued_per_host[entry["host"]] = self._queued_per_host.get(entry["host"], 0) + 1
            if not entry.get("crossing") and entry["host"]:
                self._host_line.setdefault(entry["host"], entry["line_id"])
        # What the queue declined to take in this run, by reason. Reported in the run summary.
        self.skipped = {"unparseable_link": 0, "hop_limit": 0, "host_queue_full": 0}
        self.spawned = 0
        self.frozen = 0
        self.thawed = 0

    # ------------------------------------------------------------ persistence

    @classmethod
    def load(cls, store: Store, workdir: Path) -> "Frontier":
        tables: List[List[Dict[str, Any]]] = []
        for remote in (FRONTIER_PATH, HOSTS_PATH, LINES_PATH):
            local = Path(workdir) / "state-in" / remote.rsplit("/", 1)[1]
            tables.append(pq.read_table(local).to_pylist() if store.get(remote, local) else [])
        return cls(*tables)

    def save(self, workdir: Path) -> List[Tuple[Path, str]]:
        out = Path(workdir) / "state-out"
        out.mkdir(parents=True, exist_ok=True)
        host_rows = [{"host": host, **self._hosts[host]} for host in sorted(self._hosts)]
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
        return sum(1 for entry in self._entries.values() if entry["status"] == status)

    def count_lines(self, status: str) -> int:
        return sum(1 for line in self._lines.values() if line["status"] == status)

    def priority(self, entry: Dict[str, Any], weight: float, extra_host: int = 0, extra_line: int = 0) -> float:
        line = self._lines[entry["line_id"]]
        held_host = self._hosts.get(entry["host"], {}).get("fetches", 0) + extra_host
        return (
            weight * entry["base"] * HOP_DECAY ** entry["hops"] * line["yield"]
            / (1.0 + held_host / HOST_HALF)
            / (1.0 + (line["fetches"] + extra_line) / LINE_HALF)
        )

    # ------------------------------------------------------------ lines

    def _new_line(self, line_id: str, seed_id: str, parent: Optional[str], root_url: str, now: dt.datetime) -> Dict[str, Any]:
        line = {
            "line_id": line_id, "seed_id": seed_id, "parent_line": parent, "root_url": root_url, "created_at": now,
            "fetches": 0, "content": 0, "since_thaw": 0, "yield": 1.0, "status": "active", "frozen_at": None, "inbound": 0,
        }
        self._lines[line_id] = line
        return line

    def _record_yield(self, line: Dict[str, Any], value: float, now: dt.datetime) -> None:
        line["fetches"] += 1
        line["content"] += value > 0
        line["since_thaw"] += 1
        line["yield"] = (1.0 - YIELD_ALPHA) * line["yield"] + YIELD_ALPHA * value
        if line["status"] == "active" and line["since_thaw"] >= MIN_FETCHES_TO_FREEZE and line["yield"] < FREEZE_YIELD:
            line["status"] = "frozen"
            line["frozen_at"] = now
            line["inbound"] = 0
            self.frozen += 1

    def _note_inbound(self, target: Dict[str, Any], from_line: str) -> None:
        """Another line linked to a URL of this line. Enough of these thaw a frozen line."""
        line = self._lines[target["line_id"]]
        if line["status"] != "frozen" or from_line == line["line_id"] or target["status"] != "queued":
            return
        line["inbound"] += 1
        if line["inbound"] >= THAW_INBOUND:
            line.update({"status": "active", "frozen_at": None, "inbound": 0, "since_thaw": 0, "yield": THAW_YIELD})
            self.thawed += 1

    def _spawn(self, entry: Dict[str, Any], now: dt.datetime) -> None:
        """A crossing link yielded content: its site becomes a line, and the queued links to that site move into it."""
        host = entry["host"]
        parent = entry["line_id"]
        line_id = self._host_line.get(host)
        if line_id is None:
            line_id = f"{entry['seed_id']}:{host}"
            self._new_line(line_id, entry["seed_id"], parent, entry["url"], now)
            self._host_line[host] = line_id
            self.spawned += 1
        entry.update({"line_id": line_id, "crossing": False, "hops": 0})
        for other in self._entries.values():
            if other["host"] == host and other.get("crossing"):
                other.update({"line_id": line_id, "crossing": False, "hops": 1})

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
        self._entries[url] = {
            "url": url, "host": host, "seed_id": seed_id, "line_id": line_id, "crossing": crossing, "hops": hops,
            "parent_fetch_id": parent, "anchor": anchor or None, "base": base, "status": "queued",
            "discovered_at": now, "fetched_at": None,
        }
        self._queued_per_host[host] = self._queued_per_host.get(host, 0) + 1
        return True

    def add_seeds(self, seeds: Sequence[Seed], now: dt.datetime) -> int:
        added = 0
        for seed in seeds:
            line = self._lines.get(seed.id)
            for url in seed.urls:
                known = self._entries.get(url)
                if known is not None:
                    # A seed that has never stored anything gets its starting URLs tried again each run:
                    # the failure may have been temporary, and the run summary then shows the reason.
                    if line and line["content"] == 0 and known["status"] in ("fetched", "failed"):
                        known["status"] = "queued"
                        self._queued_per_host[known["host"]] = self._queued_per_host.get(known["host"], 0) + 1
                    continue
                if seed.id not in self._lines:
                    self._new_line(seed.id, seed.id, None, url, now)
                if self._add(url, seed.id, seed.id, 0, 1.0, now):
                    self._host_line.setdefault(urlsplit(url).netloc, seed.id)
                    added += 1
        return added

    def add_links(self, entry: Dict[str, Any], row: Dict[str, Any], answers: Optional[JevResult], now: dt.datetime) -> int:
        """Queue the links of a stored HTML page. Returns how many were new."""
        if row.get("outcome") != "stored" or row.get("block_marker") or not _looks_like_html(row):
            return 0
        base = link_base(answers)
        own_line = entry["line_id"]
        added = 0
        links, unparseable = extract_links(row["body"], row.get("url_final") or entry["url"])
        for href in unparseable:
            # Kept as written, with the page it came from, so it can be looked at and repaired later.
            if href not in self._entries:
                self._entries[href] = {
                    **entry, "url": href, "host": "", "crossing": False, "parent_fetch_id": row["fetch_id"],
                    "anchor": None, "status": "unparseable", "discovered_at": now, "fetched_at": None,
                }
                self.skipped["unparseable_link"] += 1
        for url, anchor in links:
            known = self._entries.get(url)
            if known is not None:
                self._note_inbound(known, own_line)
                continue
            host = urlsplit(url).netloc
            covering = self._host_line.get(host)
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
                if covering is not None and covering != own_line:
                    self._note_inbound(self._entries[url], own_line)
        return added

    # ------------------------------------------------------------ after a fetch

    def mark(self, entry: Dict[str, Any], row: Dict[str, Any], answers: Optional[JevResult], now: dt.datetime) -> None:
        if entry["status"] == "queued":
            self._queued_per_host[entry["host"]] = max(0, self._queued_per_host.get(entry["host"], 0) - 1)
        outcome = row.get("outcome")
        entry["status"] = "failed" if outcome == "error" else "fetched"
        entry["fetched_at"] = now
        counts = self._hosts.setdefault(entry["host"], {name: 0 for name in _HOST_COUNTS})
        counts["fetches"] += 1
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
        # The fetch counts toward the line that found it, so a line is credited for reaching new ground.
        value = fetch_yield(row, answers)
        self._record_yield(self._lines[entry["line_id"]], value, now)
        if entry.get("crossing") and value > 0:
            self._spawn(entry, now)
        # A redirect target is the same document. Record it so a later link to it is not fetched again.
        final = normalise(row.get("url_final") or "")
        if final and final not in self._entries:
            self._entries[final] = {**entry, "url": final, "host": urlsplit(final).netloc, "anchor": None, "status": "alias"}

    # ------------------------------------------------------------ choosing

    def select(self, n: int, seeds: Sequence[Seed], today: dt.date) -> List[Dict[str, Any]]:
        """Up to n queued entries for the next round, from active lines, spread across hosts and lines."""
        weights = {seed.id: seed.weight for seed in seeds if seed.active(today)}
        queued = [
            entry for entry in self._entries.values()
            if entry["status"] == "queued" and entry["seed_id"] in weights and self._lines[entry["line_id"]]["status"] == "active"
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


async def crawl(
    store: Store,
    engine: Any,
    seeds: Sequence[Seed],
    *,
    workdir: Path,
    max_fetches: int,
    batch: int = 50,
    max_minutes: Optional[float] = None,
    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
    clock: Callable[[], float] = time.monotonic,
) -> Dict[str, Any]:
    """Fetch in rounds until max_fetches is reached, max_minutes has passed, or nothing is queued.
    The queue is re-ranked between rounds. The time limit is checked between rounds, so a run
    can overshoot it by one round; scheduled runs use it to spend a set share of Actions minutes.

    The queue is uploaded once, in the engine's final commit. A run that dies before then
    keeps its fetched rows (the engine uploads them as it goes) and loses its queue updates,
    so the next run fetches those URLs again and records them as unchanged.
    """
    workdir = Path(workdir)
    frontier = Frontier.load(store, workdir)
    by_id = {seed.id: seed for seed in seeds}
    seeds_added = frontier.add_seeds(seeds, now())
    links_added = 0
    stored_by_seed = {seed.id: 0 for seed in seeds}
    seed_url_problems: Dict[str, str] = {}
    data_portals: Dict[str, float] = {}

    def observe(target: Target, row: Dict[str, Any], answers: Optional[JevResult]) -> None:
        nonlocal links_added
        entry = frontier.entry(target.url)
        frontier.mark(entry, row, answers, now())
        links_added += frontier.add_links(entry, row, answers, now())
        portal = answers.nouls().get("data_portal", 0.0) if answers is not None else 0.0
        if portal >= PORTAL_THRESHOLD:
            data_portals[row["url_final"]] = round(portal, 2)
        if row["outcome"] == "stored" and not row["block_marker"]:
            stored_by_seed[entry["seed_id"]] = stored_by_seed.get(entry["seed_id"], 0) + 1
        elif entry["parent_fetch_id"] is None:  # a seed's own starting URL
            reason = row.get("error") or row.get("block_marker") or row.get("tier_note") or ""
            seed_url_problems[target.url] = f"{row['outcome']}, status {row['http_status']}: {reason}"[:300]

    await engine.start()
    rounds = 0
    stopped_by = "queue_empty"
    deadline = None if max_minutes is None else clock() + max_minutes * 60
    try:
        while True:
            if engine.stats["total"] >= max_fetches:
                stopped_by = "max_fetches"
                break
            if deadline is not None and clock() >= deadline:
                stopped_by = "max_minutes"
                break
            picks = frontier.select(min(batch, max_fetches - engine.stats["total"]), seeds, now().date())
            if not picks:
                break
            await engine.run(
                [
                    Target(entry["url"], by_id[entry["seed_id"]].entity, entry["seed_id"], entry["line_id"], entry["hops"], entry["parent_fetch_id"])
                    for entry in picks
                ],
                observe,
            )
            rounds += 1
    except BaseException:
        await engine.flush(force=True)  # keep the rows already fetched
        raise
    stats = dict(await engine.finish(frontier.save(workdir)))
    stats.update(
        {
            "rounds": rounds, "stopped_by": stopped_by, "seeds_added": seeds_added, "links_added": links_added,
            "queued": frontier.count("queued"), "known_urls": len(frontier),
            "lines_active": frontier.count_lines("active"), "lines_frozen": frontier.count_lines("frozen"),
            "lines_spawned": frontier.spawned, "lines_thawed": frontier.thawed,
            "stored_by_seed": stored_by_seed,
            "seed_url_problems": seed_url_problems,
            # Pages Jev read as data portals whose figures load from a backend: candidates for a derived client.
            "data_portals": dict(sorted(data_portals.items(), key=lambda item: -item[1])[:20]),
            # What the engine chose not to do in this run. Read these when reviewing the limits.
            "skipped": {**frontier.skipped, "too_large": stats["too_large"]},
            # A seed here has been fetched, in this run or an earlier one, and has never stored
            # anything: a dead or blocked starting URL.
            "seeds_with_nothing": sorted(
                seed.id for seed in seeds
                if (frontier.line(seed.id) or {}).get("fetches", 0) > 0 and frontier.line(seed.id)["content"] == 0
            ),
        }
    )
    return stats
