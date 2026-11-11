"""Parquet shards: the engine's tables. One file holds many rows, so file counts stay low.

Six tables, each partitioned by the UTC day on which the fetching run started:
    fetches/           one row per fetch attempt, with where it came from, the content hash and the text
    bodies/            the bytes served, one row per content hash, stored once across runs (read them with bodies_of)
    links/             one row per distinct link on a stored HTML page, in page order
    ocr_text/          one row per scanned PDF read with Tesseract
    judgement_calls/   one row per Jev call about a fetched document
    judgements/        one row per answer in that call
A judgement is filed in the same day folder as its document's fetch row, not under the day it
was asked, so "which documents of day D are still unjudged" needs only day D's folders.

Input contract: writer.add(row); batch = writer.take(); local, remote = writer.write(batch).
take() is cheap and runs on the event loop. write() makes the Parquet file on disk, blocks, and
belongs in a thread. The caller uploads it, so several tables can go up in one commit.
for row, body in bodies_of(store, workdir, fetch_rows, day): the bytes of each fetch row.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

ENGINE_VERSION = "0.3.0"  # 0.3.0: bytes in bodies, not inline in fetches

_TS = pa.timestamp("us", tz="UTC")

FETCH_SCHEMA = pa.schema(
    [
        ("fetch_id", pa.string()),
        ("run_id", pa.string()),
        # Where the crawl got this URL from: the seed it descends from, its line, the steps
        # from the start of that line, and the fetch whose page linked to it.
        ("seed_id", pa.string()),
        ("line_id", pa.string()),
        ("hops", pa.int32()),
        ("parent_fetch_id", pa.string()),
        ("entity", pa.string()),
        ("url_requested", pa.string()),
        ("url_final", pa.string()),
        ("fetched_at", _TS),
        ("http_status", pa.int32()),
        ("tier_used", pa.string()),
        ("content_type", pa.string()),
        # stored: new bytes kept. unchanged: same bytes as the last fetch of this URL.
        # duplicate: same bytes as another URL in this run. error_page: a non-200 response with
        # little text, bytes not kept. too_large: over the size cap, not downloaded (size_bytes
        # is the size the server declared, or how far the download got). error: nothing was served.
        ("outcome", pa.string()),
        ("sha256", pa.string()),
        ("size_bytes", pa.int64()),
        # The bytes, inline: only in rows from engine 0.2.0 and earlier. Newer rows leave it null and their bytes are
        # in bodies, by sha256. Kept so old and new shards read alike.
        ("body", pa.binary()),
        ("text", pa.string()),
        ("word_count", pa.int32()),
        # Why the page is not content (a non-200 status or a block-page phrase), else null.
        ("block_marker", pa.string()),
        # error: why nothing was served. tier_note: what earlier fetch tiers reported on the way to a result.
        ("error", pa.string()),
        ("tier_note", pa.string()),
        ("engine_version", pa.string()),
    ]
)

# One row per content hash stored. body is the bytes as served (rendered HTML for the browser tier);
# first_fetch_id is the fetch that stored them. A run that died before its final commit may leave a hash twice.
BODY_SCHEMA = pa.schema(
    [
        ("sha256", pa.string()),
        ("size_bytes", pa.int64()),
        ("content_type", pa.string()),
        ("tier_used", pa.string()),
        ("body", pa.binary()),
        ("first_fetch_id", pa.string()),
        ("stored_at", _TS),
    ]
)

# Every distinct link on a stored HTML page (up to readers.MAX_LINKS_PER_PAGE), normalised as the queue normalises it,
# whether or not the queue took it. position is its order on the page, from 0.
LINK_SCHEMA = pa.schema(
    [
        ("fetch_id", pa.string()),
        ("url", pa.string()),
        ("anchor", pa.string()),
        ("position", pa.int32()),
    ]
)

# PROVISIONAL: zstd level for bodies and links. On generated templated HTML, 64 MB a shard: level 1 (the default)
# 3.7x in 0.26 s, 3 6.8x 0.25 s, 6 8.2x 0.55 s, 9 14.2x 0.57 s, 10 14.5x 0.77 s of one core.
HEAVY_LEVEL = 9

CALL_SCHEMA = pa.schema(
    [
        ("call_id", pa.string()),
        ("fetch_id", pa.string()),
        ("sha256", pa.string()),
        ("run_id", pa.string()),
        ("asked_at", _TS),
        ("reason", pa.string()),
        ("status", pa.string()),
        ("error", pa.string()),
        ("jev_model", pa.string()),
        ("input_tokens", pa.int64()),
        ("excerpt_method", pa.string()),
        ("excerpt_words", pa.int32()),
        ("relevance", pa.float64()),
        ("relevance_version", pa.string()),
    ]
)

OCR_SCHEMA = pa.schema(
    [
        ("fetch_id", pa.string()),
        ("sha256", pa.string()),
        ("run_id", pa.string()),
        ("read_at", _TS),
        ("langs", pa.string()),
        ("pages_read", pa.int32()),
        ("pages_total", pa.int32()),
        ("text", pa.string()),
        ("word_count", pa.int32()),
    ]
)

ANSWER_SCHEMA = pa.schema(
    [
        ("call_id", pa.string()),
        ("fetch_id", pa.string()),
        ("sha256", pa.string()),
        ("question_id", pa.string()),
        ("question_version", pa.int32()),
        ("type", pa.string()),
        # noul and score answers are numbers. A choice answer is text.
        ("value_number", pa.float64()),
        ("value_text", pa.string()),
        ("probabilities", pa.string()),
        ("confidence", pa.float64()),
    ]
)


def day_folder(table: str, day: dt.date) -> str:
    return f"{table}/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}"


class ShardWriter:
    """Buffers rows for one table and one fetch day, and writes them as numbered Parquet parts."""

    def __init__(
        self,
        table: str,
        schema: pa.Schema,
        run_id: str,
        day: dt.date,
        workdir: Path,
        max_bytes: int = 64_000_000,
        level: Optional[int] = None,
    ) -> None:
        self._table = table
        self._schema = schema
        self._run_id = run_id
        self._day = day
        self._workdir = Path(workdir)
        self._max_bytes = max_bytes
        self._level = level
        self._rows: List[Dict[str, Any]] = []
        self._bytes = 0
        self._part = 0
        self.written: List[str] = []

    def add(self, row: Dict[str, Any]) -> None:
        unknown = set(row) - set(self._schema.names)
        if unknown:
            raise ValueError(f"{self._table}: unknown columns {sorted(unknown)}")
        self._rows.append(row)
        self._bytes += len(row.get("body") or b"") + len(row.get("text") or "") + 512

    def take(self, force: bool = False) -> Optional[Tuple[int, List[Dict[str, Any]]]]:
        """Hand over the buffer when it is full, or when force is set and it is not empty."""
        if not self._rows or (not force and self._bytes < self._max_bytes):
            return None
        batch = (self._part, self._rows)
        self._part += 1
        self._rows = []
        self._bytes = 0
        return batch

    def write(self, batch: Tuple[int, List[Dict[str, Any]]]) -> Tuple[Path, str]:
        part, rows = batch
        columns = {name: [row.get(name) for row in rows] for name in self._schema.names}
        table = pa.Table.from_pydict(columns, schema=self._schema)
        remote = f"{day_folder(self._table, self._day)}/{self._run_id}-{part:03d}.parquet"
        local = self._workdir / remote
        local.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, local, compression="zstd", compression_level=self._level)
        return local, remote


def bodies_of(store: Any, workdir: Path, rows: Iterable[Dict[str, Any]], day: Optional[dt.date] = None) -> Iterator[Tuple[Dict[str, Any], Optional[bytes]]]:
    """(row, its bytes) for each fetch row: the row's own body when it has one (rows from engine 0.2.0 and earlier),
    else the body with the row's sha256 from the bodies table, or None when no shard holds it.

    The bodies shards of `day` are searched first, then every other day's, newest first, until every hash is found.
    shortcut: a shard is downloaded whole to be searched, so one call costs at most one download of each bodies shard,
    the whole table when a hash is missing; keep a hash -> shard index once reading bytes back is routine.
    """
    wanted: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if row.get("body") is not None:
            yield row, row["body"]
        else:
            wanted.setdefault(row["sha256"], []).append(row)
    if not wanted:
        return
    remotes = list(reversed(store.list("bodies")))
    if day is not None:
        own = day_folder("bodies", day) + "/"
        remotes.sort(key=lambda remote: not remote.startswith(own))
    for remote in remotes:
        if not wanted:
            break
        local = Path(workdir) / "in" / remote
        if not store.get(remote, local):
            continue
        found = pq.read_table(local, columns=["sha256", "body"], filters=[("sha256", "in", list(wanted))]).to_pylist()
        local.unlink(missing_ok=True)
        for hit in found:
            for row in wanted.pop(hit["sha256"], []):
                yield row, hit["body"]
    for missing in wanted.values():
        for row in missing:
            yield row, None
