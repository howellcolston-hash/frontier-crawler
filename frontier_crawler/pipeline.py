"""Fetch and store, then judge.

Fetching (Engine.fetch): fetch a target and write one row per attempt to the fetches table,
with where it came from; new bytes go to the bodies table, once per content hash across runs, and
a stored HTML page's links to the links table. Nothing here waits on a model: a document is
stored before anything judges it.
Judging (Engine.judge when a Jev client is given, or later through Engine.judge_days): ask Jev
the configured questions (judge/questions.py) about each new document and store every answer.
A document Jev was not asked about (no key, the monthly ceiling reached, Jev unavailable)
stays pending. When the budget cannot cover a run, run judges a sample of its documents, chosen
by content hash, so the allowance is spread over the whole run; the rest are counted not_sampled.
Judging can be re-run; fetching is never repeated for it.

Input contract:
    engine = Engine(store, fetcher, workdir=..., jev=..., monthly_usd=...)
    await engine.start()
    row = await engine.fetch(target)         # fetched, buffered and counted (row["links"]: see fetch); or, a batch at a time:
    answers = await engine.judge(row)        #   await engine.run(targets, observer), which does both
    await engine.flush()                     # uploads full or old buffers; call it now and then
    stats = await engine.finish(extra_files) # final upload: tables, state and extra files in one commit
The fetcher is any object with `async fetch(url) -> ScrapeResult` (and the limits, timeout= and connect=, when fetch is given them).
"""

from __future__ import annotations

import asyncio
import calendar
import contextlib
import datetime as dt
import hashlib
import json
import logging
import math
import resource
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple

import pyarrow.parquet as pq

from frontier_crawler.harvester.cascade import MIN_WORDS as THIN_HTML_WORDS, ScrapeResult, Target, timed
from frontier_crawler.harvester.readers import ReaderPool
from frontier_crawler.judge.excerpt import DEFAULT_MAX_WORDS, build_excerpt
from frontier_crawler.judge.jev import PRICE_PER_MTOK_USD, JevClient, JevRejected, JevResult, JevUnavailable
from frontier_crawler.judge import questions as judging
from frontier_crawler.judge.questions import Question, relevance
from frontier_crawler import ocr
from frontier_crawler.joinkeys import join_keys
from frontier_crawler.storage.shards import (
    ANSWER_SCHEMA, BODY_SCHEMA, CALL_SCHEMA, ENGINE_VERSION, FETCH_SCHEMA, HEAVY_LEVEL, LINK_SCHEMA, OCR_SCHEMA, ShardWriter, bodies_of, day_folder,
)
from frontier_crawler.storage.state import BodyHashes, UrlState, load_spend, save_spend
from frontier_crawler.storage.store import Store

logger = logging.getLogger("frontier_crawler.pipeline")

# PROVISIONAL: phrases that mark a page as a block or error page when the page is short.
_BLOCK_PHRASES = ("access denied", "cloudflare ray id", "attention required", "captcha", "enable javascript", "404 not found")
_BLOCK_PHRASE_MAX_WORDS = 400
# PROVISIONAL: a non-200 response with fewer words than this is an error page: the attempt is
# recorded and its bytes are dropped. With more, it is kept, since some servers send real
# content under an error status.
_ERROR_PAGE_MAX_WORDS = 150
# PROVISIONAL: a page with fewer words than this has nothing for Jev to read.
_MIN_JUDGE_WORDS = 20
# PROVISIONAL: stop calling Jev for the rest of a run after this many refusals in a row (a bad key, a changed API).
_MAX_REJECTIONS = 3
# PROVISIONAL: a crawl run must see at least this many judgeable pages for its count to become the
# forecast of the next run's need, so a short manual run does not shrink the sample.
_MIN_FORECAST_PAGES = 500
# Top-level key in the spend file for that count. The file's other keys are months ("2026-11").
FORECAST_KEY = "judgeable_pages_last_crawl_run"
# PROVISIONAL: a scan's reader process is killed after this long: past read_scan's own limit, which the page in
# progress can overrun by its two Tesseract calls of up to 120 s each.
_OCR_LIMIT_S = ocr.OCR_SECONDS + 300.0
# Rows are uploaded at least this often, so a run that dies keeps what it had fetched.
_UPLOAD_EVERY_S = 600.0

Observer = Callable[[Target, Dict[str, Any], Optional[JevResult]], None]


class Fetcher(Protocol):
    async def fetch(self, url: str) -> ScrapeResult: ...


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def block_marker(status: int, text: str) -> Optional[str]:
    """Why a served page is not content, or None. A flagged page is never judged."""
    if status != 200:
        return f"http {status}"
    if len(text.split()) <= _BLOCK_PHRASE_MAX_WORDS:
        lowered = text.lower()
        for phrase in _BLOCK_PHRASES:
            if phrase in lowered:
                return phrase
    return None


def judgeable(row: Dict[str, Any]) -> bool:
    """New content that reads as a document. JSON from an API is structured data and is not sent to Jev."""
    return (
        row.get("outcome") == "stored"
        and row.get("block_marker") is None
        and row.get("tier_used") != "API"
        and (row.get("word_count") or 0) >= _MIN_JUDGE_WORDS
        and bool(row.get("text"))
    )


def _spread(seconds: List[float]) -> Dict[str, Any]:
    """n, mean, median, 90th percentile (nearest rank) and max of a list of durations."""
    s = sorted(seconds)
    n = len(s)
    return {"n": n, "mean": round(sum(s) / n, 3), "p50": round(s[n // 2], 3),
            "p90": round(s[math.ceil(0.9 * n) - 1], 3), "max": round(s[-1], 3)}


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


class Engine:
    def __init__(
        self,
        store: Store,
        fetcher: Optional[Fetcher] = None,
        *,
        workdir: Path,
        jev: Optional[JevClient] = None,
        monthly_usd: float = 0.0,
        questions: Optional[Sequence[Question]] = None,
        max_excerpt_words: int = DEFAULT_MAX_WORDS,
        concurrency: int = 4,
        now: Callable[[], dt.datetime] = utc_now,
        run_id: Optional[str] = None,
    ) -> None:
        self._store = store
        self._fetcher = fetcher
        self._workdir = Path(workdir)
        self._jev = jev
        self._questions = questions or judging.active()
        self._max_words = max_excerpt_words
        self.concurrency = max(1, concurrency)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._now = now
        started = now()
        # The month's Jev budget accrues steadily through the month: by a given moment at most the elapsed
        # share of it may be spent, so judging never stops for the rest of a month and runs later in a day
        # get their part of that day. Spend is counted from the tokens Jev reports.
        month_start = started.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_seconds = calendar.monthrange(started.year, started.month)[1] * 86400
        share = (started - month_start).total_seconds() / month_seconds
        self._ceiling_tokens = int(monthly_usd * share / PRICE_PER_MTOK_USD * 1_000_000)
        self.run_id = run_id or f"{started.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
        self._day = started.date()
        self._month = started.strftime("%Y-%m")
        self._writers: Dict[Tuple[str, dt.date], ShardWriter] = {}
        self._unsent: List[Tuple[Path, str]] = []   # shards written but not yet uploaded: kept for the next upload
        self._unsent_bodies: List[str] = []         # the hashes in those shards' bodies
        self._bodies_written: set = set()           # hashes given to the bodies writer in this run
        self._flush_lock = asyncio.Lock()
        self._last_upload = time.monotonic()
        self._seen: Dict[str, str] = {}
        self._rejections = 0
        self._sample_rate = 1.0
        self._judgeable_seen = 0
        # Measurement only: seconds summed over fetch slots, per-tier fetch durations, Jev call and upload time.
        # finish() summarises them into stats.
        self._slot: Dict[str, float] = dict.fromkeys(("observe", "waiting"), 0.0)
        self._uploads = 0
        self._upload_seconds = 0.0
        self._fetch_seconds: Dict[str, List[float]] = {}
        self._jev_calls = 0
        self._jev_spent: Dict[str, float] = {"seconds": 0.0}
        self.stats: Dict[str, Any] = {
            "run_id": self.run_id, "total": 0, "stored": 0, "unchanged": 0, "duplicate": 0, "error_pages": 0,
            "too_large": 0, "errors": 0, "blocked": 0, "judged": 0, "pending": 0, "scans_read": 0,
            "jev_input_tokens": 0, "jev_usd_month": 0.0, "jev_stopped": None, "shards": [],
            "not_sampled": 0, "jev_sample_rate": 1.0,
            "stored_bytes": 0, "thin_html": 0, "stored_by_tier": {}, "bytes_by_tier": {}, "shard_bytes": {},
            "bodies_written": 0, "bodies_written_bytes": 0, "bodies_already_stored": 0, "bodies_already_stored_bytes": 0,
            "links_written": 0, "reader_crashes": 0, "reader_timeouts": 0,
        }

    async def start(self) -> None:
        self._state = await asyncio.to_thread(UrlState.load, self._store, self._workdir)
        self._hashes = await asyncio.to_thread(BodyHashes.load, self._store, self._workdir)
        self._spend = await asyncio.to_thread(load_spend, self._store, self._workdir)
        month = self._spend.setdefault(self._month, {"input_tokens": 0, "calls": 0})
        self.stats["jev_usd_month"] = round(month["input_tokens"] * PRICE_PER_MTOK_USD / 1_000_000, 4)
        # The share of this run's documents Jev can read: what may be spent now over what the last sizeable
        # crawl run would cost at the tokens per call of the latest month with calls, so the first run of a
        # month is priced too. With no forecast or no call yet, every document is read.
        pages = self._spend.get(FORECAST_KEY)
        priced = [m for _, m in sorted(self._spend.items()) if isinstance(m, dict) and m.get("calls") and m.get("input_tokens")]
        if pages and priced:
            available = max(0, self._ceiling_tokens - month["input_tokens"])
            self._sample_rate = min(1.0, available / (pages * priced[-1]["input_tokens"] / priced[-1]["calls"]))
        self.stats["jev_sample_rate"] = round(self._sample_rate, 4)

    # ------------------------------------------------------------ writing

    def _writer(self, table: str, day: dt.date) -> ShardWriter:
        key = (table, day)
        if key not in self._writers:
            schema = {"fetches": FETCH_SCHEMA, "bodies": BODY_SCHEMA, "links": LINK_SCHEMA, "judgement_calls": CALL_SCHEMA,
                      "judgements": ANSWER_SCHEMA, "ocr_text": OCR_SCHEMA}[table]
            level = HEAVY_LEVEL if table in ("bodies", "links") else None
            self._writers[key] = ShardWriter(table, schema, self.run_id, day, self._workdir, level=level)
        return self._writers[key]

    async def _cut(self, force: bool) -> None:
        """Write full buffers (all of them when forced) to shards, which wait in _unsent for the next upload."""
        fetches_cut = False
        for (table, _), writer in sorted(self._writers.items(), key=lambda item: item[0][0] != "fetches"):
            # Bodies are cut whenever fetch rows are, so a fetch row never goes up without its bytes.
            batch = writer.take(force or (fetches_cut and table == "bodies"))
            if batch is None:
                continue
            fetches_cut |= table == "fetches"
            hashes = [row["sha256"] for row in batch[1]] if table == "bodies" else []
            local, remote = await asyncio.to_thread(writer.write, batch)
            self._unsent.append((local, remote))
            self._unsent_bodies += hashes
            self.stats["shard_bytes"][table] = self.stats["shard_bytes"].get(table, 0) + _size(local)

    async def flush(self, force: bool = False, extra: Sequence[Tuple[Path, str]] = ()) -> None:
        """Upload full buffers (all of them when forced or when the last upload is old), with any extra files, in one commit."""
        async with self._flush_lock:
            began = time.monotonic()
            await self._cut(force or began - self._last_upload >= _UPLOAD_EVERY_S)
            files = self._unsent
            if not files and not extra:
                return
            # A failed upload leaves the shards in _unsent, so the next flush sends them: no row is lost.
            await asyncio.to_thread(self._store.put_many, files + list(extra), f"engine {self.run_id}: {len(files) + len(extra)} file(s)")
            self._unsent = []
            self._hashes.add(self._unsent_bodies)
            self._unsent_bodies = []
            self.stats["shards"] += [remote for _, remote in files]
            self._last_upload = time.monotonic()
            self._uploads += 1
            self._upload_seconds += self._last_upload - began

    async def finish(self, extra: Sequence[Tuple[Path, str]] = ()) -> Dict[str, Any]:
        """The final upload. State goes up with the last rows, so a URL is never marked fetched without its row,
        and the stored-hash set grows in the same commit as the last bodies, so it never names one the store lacks."""
        if self._judgeable_seen >= _MIN_FORECAST_PAGES:
            self._spend[FORECAST_KEY] = self._judgeable_seen
        async with self._flush_lock:
            await self._cut(force=True)
        state_files = [self._state.save(self._workdir), save_spend(self._spend, self._workdir),
                       *self._hashes.save(self._workdir, self._unsent_bodies)]
        # By file, or by folder for the queue's files (state/queue/...): state/hosts.parquet -> hosts.
        self.stats["state_bytes"] = {}
        for local, remote in state_files + list(extra):
            name = remote.split("/")[1].split(".")[0]
            self.stats["state_bytes"][name] = self.stats["state_bytes"].get(name, 0) + _size(local)
        await self.flush(force=True, extra=state_files + list(extra))
        self._measure()
        logger.info("engine %s", {key: value for key, value in self.stats.items() if key != "shards"})
        return self.stats

    def _measure(self) -> None:
        """Summarise where the run's time went. Counts, seconds and bytes only, never what was fetched."""
        counts = getattr(self._fetcher, "counts", {})
        calls, seconds = self._jev_calls, self._jev_spent["seconds"]
        slot = {**self._slot, **getattr(self._fetcher, "timings", {})}
        self.stats.update(
            {
                "slot_seconds": {key: round(value, 1) for key, value in sorted(slot.items())},
                "flush": {"uploads": self._uploads, "seconds_total": round(self._upload_seconds, 1)},
                "fetch_seconds_by_tier": {tier: _spread(times) for tier, times in sorted(self._fetch_seconds.items())},
                "timeouts": {"connect": counts.get("connect_timeouts", 0), "read": counts.get("read_timeouts", 0), "browser": counts.get("browser_timeouts", 0)},
                **{key: counts.get(key, 0) for key in ("browser_launches", "browser_pages", "browser_relaunches", "decoded_non_utf8")},
                # Pages whose reader process died or hung: the fetcher's, and the OCR command's own.
                **{key: self.stats[key] + counts.get(key, 0) for key in ("reader_crashes", "reader_timeouts")},
                "jev": {"ceiling_usd_today": round(self._ceiling_tokens * PRICE_PER_MTOK_USD / 1_000_000, 4), "calls": calls,
                        "seconds_total": round(seconds, 1), "seconds_mean": round(seconds / calls, 3) if calls else 0.0},
                # shortcut: ru_maxrss is KB on Linux only; macOS reports bytes
                "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
            }
        )

    # ------------------------------------------------------------ fetching

    async def run(self, targets: Sequence[Target], observer: Optional[Observer] = None) -> None:
        """Fetch a batch, judging each page in its fetch slot. observer(target, row, answers) is called once per fetch row, after judging."""

        async def one(target: Target) -> None:
            async with self._semaphore:
                row = await self.fetch(target)
                answers = await self.judge(row)
                if observer is not None:
                    try:
                        with timed(self._slot, "observe"):
                            observer(target, row, answers)
                    except Exception:
                        # The row is stored. A fault in what follows it must not stop the batch.
                        logger.exception("observer failed for %s", target.url)
                await self.flush()

        await asyncio.gather(*(one(target) for target in targets))

    def slot_timer(self, key: str) -> Any:
        """Add the seconds a block takes to the run's slot_seconds under key."""
        return timed(self._slot, key)

    async def fetch(self, target: Target, **limits: float) -> Dict[str, Any]:
        """Fetch one target, buffer its row and count it. Returns the row with two keys that are not stored columns:
        row["links"], (links, unparseable hrefs) of a stored HTML page as its reader found them, else None;
        and row["timed_out"], set when nothing was served because of a timeout. limits (timeout=, connect=) go to the fetcher."""
        took: Dict[str, float] = {}
        with timed(took, "fetch"):
            row, scraped, links = await self._fetch_row(target, limits)
        with contextlib.suppress(Exception):  # measuring must never break a fetch (tested)
            tier = "error" if row["outcome"] == "error" else row.get("tier_used") or "error"
            # Without the wait for the host's turn, which is politeness, not the page.
            self._fetch_seconds.setdefault(tier, []).append(took["fetch"] - (scraped.throttle_wait if scraped else 0.0))
        # The row is buffered before judging starts, so a judging fault cannot drop it.
        self._writer("fetches", self._day).add(row)
        self.stats["total"] += 1
        counted = {"stored": "stored", "unchanged": "unchanged", "duplicate": "duplicate",
                   "error_page": "error_pages", "too_large": "too_large"}
        self.stats[counted.get(row["outcome"], "errors")] += 1
        if row["block_marker"] is not None and row["outcome"] == "stored":
            self.stats["blocked"] += 1
        if judgeable(row):
            self._judgeable_seen += 1
            if not self.sampled(row):
                self.stats["not_sampled"] += 1
        if scraped is not None and scraped.timed_out:
            return {**row, "links": links, "timed_out": True}
        return {**row, "links": links}

    def sampled(self, row: Dict[str, Any]) -> bool:
        """Whether judge() will ask Jev about this row: new readable content within the run's sample."""
        # Uniform by content hash: every line gets the same share of its pages read, and a rerun picks the same ones.
        return judgeable(row) and int(row["sha256"][:8], 16) / 2**32 < self._sample_rate

    async def judge(self, row: Dict[str, Any]) -> Optional[JevResult]:
        """Jev's answers about a row from fetch(), or None: not sampled, or left pending."""
        if not self.sampled(row):
            return None
        return await self._judge(row["fetch_id"], row["sha256"], row["text"], self._day, "new_document")

    async def _fetch_row(self, target: Target, limits: Dict[str, float]) -> Tuple[Dict[str, Any], Optional[ScrapeResult], Any]:
        """(the row, what the fetcher returned: None when it raised, the page's links: see fetch)."""
        row: Dict[str, Any] = {
            "fetch_id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "seed_id": target.seed_id or None,
            "line_id": target.line_id or None,
            "hops": target.hops,
            "parent_fetch_id": target.parent_fetch_id,
            "entity": target.entity or None,
            "url_requested": target.url,
            "url_final": target.url,
            "http_status": 0,
            "outcome": "error",
            "block_marker": None,
            "engine_version": ENGINE_VERSION,
        }
        try:
            scraped = await self._fetcher.fetch(target.url, **limits)
        except Exception as exc:
            logger.warning("fetch failed for %s: %s: %s", target.url, type(exc).__name__, exc)
            return {**row, "fetched_at": self._now(), "error": f"{type(exc).__name__}: {exc}"}, None, None
        when = self._now()
        body = scraped.raw_payload or b""
        row.update(
            {
                "fetched_at": when,
                "url_final": scraped.url or target.url,
                "http_status": scraped.http_status,
                "tier_used": scraped.tier_used,
                "content_type": scraped.content_type or None,
            }
        )
        if scraped.too_large_bytes is not None:
            return {**row, "outcome": "too_large", "size_bytes": scraped.too_large_bytes, "error": scraped.error_message}, scraped, None
        if not body:
            return {**row, "error": scraped.error_message or "nothing was served"}, scraped, None
        text = scraped.markdown_content or ""
        digest = hashlib.sha256(body).hexdigest()
        key = row["url_final"]
        if scraped.http_status != 200 and scraped.word_count < _ERROR_PAGE_MAX_WORDS:
            outcome = "error_page"
        elif not target.fresh and self._state.same(key, digest):
            outcome = "unchanged"
        elif digest in self._seen and self._seen[digest] != key:
            outcome = "duplicate"
        else:
            outcome = "stored"
        self._seen.setdefault(digest, key)
        self._state.record(key, digest)
        row.update(
            {
                "outcome": outcome,
                "sha256": digest,
                "size_bytes": len(body),
                "word_count": scraped.word_count,
                "title": scraped.title,
                "block_marker": block_marker(scraped.http_status, text),
                "tier_note": scraped.error_message,
            }
        )
        links = None
        if outcome == "stored":
            self._keep_body(row, body)
            links = self._links(row, scraped.links)
            self.stats["stored_bytes"] += len(body)
            tier = scraped.tier_used
            self.stats["stored_by_tier"][tier] = self.stats["stored_by_tier"].get(tier, 0) + 1
            # Pages the browser rule left alone although they carry little text: evidence for tuning MIN_LINKS.
            if tier == "DOM_TRAFILATURA" and scraped.word_count < THIN_HTML_WORDS:
                self.stats["thin_html"] += 1
            row["text"] = text or None
            row["join_keys"] = json.dumps(join_keys(text)) if text else None
            sizes = self.stats["bytes_by_tier"].setdefault(tier, {"body": 0, "text": 0})
            sizes["body"] += len(body)
            sizes["text"] += len(text.encode("utf-8"))
        return row, scraped, links

    def _keep_body(self, row: Dict[str, Any], body: bytes) -> None:
        """Buffer the bytes for the bodies table, unless a body with their hash is stored already, in this run or before."""
        digest = row["sha256"]
        if digest in self._bodies_written or digest in self._hashes:
            self.stats["bodies_already_stored"] += 1
            self.stats["bodies_already_stored_bytes"] += len(body)
            return
        self._bodies_written.add(digest)
        self._writer("bodies", self._day).add({
            "sha256": digest, "size_bytes": len(body), "content_type": row["content_type"], "tier_used": row["tier_used"],
            "body": body, "first_fetch_id": row["fetch_id"], "stored_at": row["fetched_at"],
        })
        self.stats["bodies_written"] += 1
        self.stats["bodies_written_bytes"] += len(body)

    def _links(self, row: Dict[str, Any], links: Any) -> Any:
        """Write a stored HTML page's links, as the fetcher's reader found them, to the links table; hand them on for the queue."""
        if links is None:
            return None
        writer = self._writer("links", self._day)
        for position, (url, anchor) in enumerate(links[0]):
            writer.add({"fetch_id": row["fetch_id"], "url": url, "anchor": anchor or None, "position": position})
        self.stats["links_written"] += len(links[0])
        return links

    # ------------------------------------------------------------ judging

    async def _judge(self, fetch_id: str, sha256: str, text: str, day: dt.date, reason: str) -> Optional[JevResult]:
        """Ask Jev about one document and write the call and its answers. None when it stays pending."""
        month = self._spend[self._month]
        if self._jev is None or self.stats["jev_stopped"] or month["input_tokens"] >= self._ceiling_tokens:
            self.stats["pending"] += 1
            return None
        excerpt, method, words = build_excerpt(text, self._max_words)
        call: Dict[str, Any] = {
            "call_id": str(uuid.uuid4()), "fetch_id": fetch_id, "sha256": sha256, "run_id": self.run_id,
            "asked_at": self._now(), "reason": reason, "excerpt_method": method, "excerpt_words": words,
            "relevance_version": judging.relevance_version(),
        }
        calls = self._writer("judgement_calls", day)
        self._jev_calls += 1
        try:
            with timed(self._jev_spent, "seconds"):
                result = await self._jev.ask(excerpt, self._questions)
        except (JevUnavailable, JevRejected) as exc:
            refused = isinstance(exc, JevRejected)
            calls.add({**call, "status": "rejected" if refused else "unavailable", "error": str(exc)})
            self.stats["pending"] += 1
            self._rejections = self._rejections + 1 if refused else 0
            if self._rejections >= _MAX_REJECTIONS:
                self.stats["jev_stopped"] = f"Jev refused {self._rejections} calls in a row ({exc}); judging stopped for this run"
                logger.error(self.stats["jev_stopped"])
            return None
        self._rejections = 0
        month["input_tokens"] += result.input_tokens
        month["calls"] += 1
        self.stats["judged"] += 1
        self.stats["jev_input_tokens"] += result.input_tokens
        self.stats["jev_usd_month"] = round(month["input_tokens"] * PRICE_PER_MTOK_USD / 1_000_000, 4)
        calls.add({**call, "status": "ok", "jev_model": result.model, "input_tokens": result.input_tokens,
                   "relevance": relevance(result.nouls())})
        answers = self._writer("judgements", day)
        for answer in result.answers.values():
            answers.add(
                {
                    "call_id": call["call_id"], "fetch_id": fetch_id, "sha256": sha256,
                    "question_id": answer.question.id, "question_version": answer.question.version,
                    "type": answer.question.type, "value_number": answer.value_number, "value_text": answer.value_text,
                    "probabilities": json.dumps(answer.probabilities, sort_keys=True) if answer.probabilities else None,
                    "confidence": answer.confidence,
                }
            )
        return result

    async def judge_days(self, days: Sequence[dt.date]) -> None:
        """Judge the documents of the given fetch days that have no successful Jev call yet.

        shortcut: each day's fetch files are downloaded whole to read their text; read only the
        needed columns over the network once a day's files run to gigabytes.
        """
        columns = ["fetch_id", "sha256", "outcome", "block_marker", "tier_used", "word_count", "text"]
        for day in days:
            judged = set()
            for remote in await asyncio.to_thread(self._store.list, day_folder("judgement_calls", day)):
                local = self._workdir / "in" / remote
                if await asyncio.to_thread(self._store.get, remote, local):
                    judged |= {call["fetch_id"] for call in pq.read_table(local, columns=["fetch_id", "status"]).to_pylist() if call["status"] == "ok"}
            for remote in await asyncio.to_thread(self._store.list, day_folder("fetches", day)):
                local = self._workdir / "in" / remote
                if not await asyncio.to_thread(self._store.get, remote, local):
                    continue
                todo = [row for row in pq.read_table(local, columns=columns).to_pylist() if judgeable(row) and row["fetch_id"] not in judged]
                local.unlink(missing_ok=True)
                await self._judge_rows(todo, day, "backlog")
            # Scanned PDFs have their text in the ocr_text table.
            for row in await self._read_day("ocr_text", day, ["fetch_id", "sha256", "text", "word_count"]):
                if row["word_count"] >= _MIN_JUDGE_WORDS and row["fetch_id"] not in judged:
                    await self._judge_rows([row], day, "ocr")

    async def _read_day(self, table: str, day: dt.date, columns: List[str]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for remote in await asyncio.to_thread(self._store.list, day_folder(table, day)):
            local = self._workdir / "in" / remote
            if await asyncio.to_thread(self._store.get, remote, local):
                rows += pq.read_table(local, columns=columns).to_pylist()
                local.unlink(missing_ok=True)
        return rows

    async def _judge_rows(self, rows: List[Dict[str, Any]], day: dt.date, reason: str) -> None:
        async def one(row: Dict[str, Any]) -> None:
            async with self._semaphore:
                await self._judge(row["fetch_id"], row["sha256"], row["text"], day, reason)
                await self.flush()

        await asyncio.gather(*(one(row) for row in rows))

    async def ocr_days(self, days: Sequence[dt.date], max_minutes: Optional[float] = None) -> None:
        """Read the scanned PDFs of the given fetch days with Tesseract, one at a time, and judge what was read.

        Stops starting new scans after max_minutes; scans already read are skipped by the next run.
        """
        deadline = None if max_minutes is None else time.monotonic() + max_minutes * 60
        readers = ReaderPool(self.stats, workers=1)
        try:
            await self._ocr_days(days, deadline, readers)
        finally:
            readers.close()

    async def _ocr_days(self, days: Sequence[dt.date], deadline: Optional[float], readers: ReaderPool) -> None:
        for day in days:
            done = {row["fetch_id"] for row in await self._read_day("ocr_text", day, ["fetch_id"])}
            columns = ["fetch_id", "sha256", "outcome", "tier_used", "word_count", "body"]
            scans = [row for row in await self._read_day("fetches", day, columns)
                     if row["outcome"] == "stored" and row["tier_used"] == "PDF" and row["fetch_id"] not in done
                     and (row["word_count"] or 0) < ocr.SCAN_MAX_WORDS]
            found = bodies_of(self._store, self._workdir, scans, day)
            while (item := await asyncio.to_thread(next, found, None)) is not None:
                if deadline is not None and time.monotonic() >= deadline:
                    self.stats["stopped_by"] = "max_minutes"
                    return
                row, body = item
                if body is None:
                    logger.warning("no stored body for %s", row["fetch_id"])
                    continue
                try:
                    text, pages_read, pages_total, langs = await readers.run(_OCR_LIMIT_S, ocr.read_scan, body)
                except Exception as exc:
                    logger.warning("scan not read for %s: %s: %s", row["fetch_id"], type(exc).__name__, exc)
                    continue
                words = len(text.split())
                self._writer("ocr_text", day).add(
                    {"fetch_id": row["fetch_id"], "sha256": row["sha256"], "run_id": self.run_id, "read_at": self._now(),
                     "langs": langs, "pages_read": pages_read, "pages_total": pages_total, "text": text, "word_count": words,
                     "join_keys": json.dumps(join_keys(text)) if text else None}
                )
                self.stats["scans_read"] += 1
                if words >= _MIN_JUDGE_WORDS:
                    await self._judge(row["fetch_id"], row["sha256"], text, day, "ocr")
                await self.flush()
