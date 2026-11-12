"""Small state the engine carries between runs: what each URL last served, which bodies are stored, Jev spend per month,
and when each watched page was last read.

Each is a single file in the store, read at the start of a run and rewritten at the end.
A run that dies before the end leaves the previous state in place: the next run then stores
those documents again, which costs space but loses nothing.
"""

from __future__ import annotations

import bisect
import datetime as dt
import json
from array import array
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from frontier_crawler.storage.store import Store

URL_STATE_PATH = "state/url_state.parquet"
JEV_SPEND_PATH = "state/jev_spend.json"
BODY_HASHES_PATH = "state/body_hashes.parquet"
WATCH_PATH = "state/watch.json"

_URL_SCHEMA = pa.schema(
    [
        ("url", pa.string()),
        ("sha256", pa.string()),
        ("last_fetched", pa.timestamp("us", tz="UTC")),
        ("http_status", pa.int32()),
    ]
)


class UrlState:
    """url -> the hash of what it last served."""

    def __init__(self, rows: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        self._rows: Dict[str, Dict[str, Any]] = rows or {}

    @classmethod
    def load(cls, store: Store, workdir: Path) -> "UrlState":
        local = Path(workdir) / "state-in" / "url_state.parquet"
        if not store.get(URL_STATE_PATH, local):
            return cls()
        return cls({row["url"]: row for row in pq.read_table(local).to_pylist()})

    def last_hash(self, url: str) -> Optional[str]:
        row = self._rows.get(url)
        return row["sha256"] if row else None

    def record(self, url: str, sha256: str, when: dt.datetime, status: int) -> None:
        self._rows[url] = {"url": url, "sha256": sha256, "last_fetched": when, "http_status": status}

    def __len__(self) -> int:
        return len(self._rows)

    def save(self, workdir: Path) -> Tuple[Path, str]:
        local = Path(workdir) / "state-out" / "url_state.parquet"
        local.parent.mkdir(parents=True, exist_ok=True)
        rows = [self._rows[url] for url in sorted(self._rows)]
        columns = {name: [row.get(name) for row in rows] for name in _URL_SCHEMA.names}
        pq.write_table(pa.Table.from_pydict(columns, schema=_URL_SCHEMA), local, compression="zstd")
        return local, URL_STATE_PATH


class BodyHashes:
    """The content hashes the bodies table holds, so a body is stored once across runs.

    Each is kept as its first 8 bytes, an unsigned 64-bit integer: 8 MB per million in memory (a sorted array, searched by
    bisection) and about 6 MB per million on disk (one sorted, delta-encoded Parquet column). Two different bodies share
    those 8 bytes with probability n / 2^64 for n stored, about 5e-12 per new body at 100 million; a false match means
    one body is not stored. The file grows only in the commit that uploads the shards holding those bodies, so it never
    names a body the store lacks; a run that dies leaves its uploaded bodies unnamed, and they are stored again later.
    shortcut: the array is read and written in this machine's byte order, little-endian on every runner used.
    """

    def __init__(self, known: Optional[array] = None) -> None:
        self._known = known if known is not None else array("Q")
        self._new: set = set()

    @staticmethod
    def _key(sha256: str) -> int:
        return int(sha256[:16], 16)

    @classmethod
    def load(cls, store: Store, workdir: Path) -> "BodyHashes":
        local = Path(workdir) / "state-in" / "body_hashes.parquet"
        known = array("Q")
        if store.get(BODY_HASHES_PATH, local):
            column = pq.read_table(local).column("h").combine_chunks()
            if len(column):
                known.frombytes(column.buffers()[1].slice(column.offset * 8, len(column) * 8))
        return cls(known)

    def __contains__(self, sha256: str) -> bool:
        key = self._key(sha256)
        at = bisect.bisect_left(self._known, key)
        return key in self._new or (at < len(self._known) and self._known[at] == key)

    def add(self, hashes: Iterable[str]) -> None:
        self._new.update(map(self._key, hashes))

    def save(self, workdir: Path, pending: Iterable[str] = ()) -> List[Tuple[Path, str]]:
        """The file to upload, naming also the pending hashes (bodies in the same commit); none when nothing is new."""
        new = self._new | set(map(self._key, pending))
        if not new:
            return []
        known = pa.Array.from_buffers(pa.uint64(), len(self._known), [None, pa.py_buffer(self._known)])
        keys = pa.concat_arrays([known, pa.array(sorted(new), pa.uint64())])
        local = Path(workdir) / "state-out" / "body_hashes.parquet"
        local.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"h": keys.take(pc.sort_indices(keys))}), local, compression="zstd",
                       use_dictionary=False, column_encoding={"h": "DELTA_BINARY_PACKED"})
        return [(local, BODY_HASHES_PATH)]


def load_spend(store: Store, workdir: Path) -> Dict[str, Any]:
    """{"2026-11": {"input_tokens": n, "calls": m}, "judgeable_pages_last_crawl_run": k} (k once a sizeable run is recorded)."""
    local = Path(workdir) / "state-in" / "jev_spend.json"
    if not store.get(JEV_SPEND_PATH, local):
        return {}
    data = json.loads(local.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def save_spend(spend: Dict[str, Any], workdir: Path) -> Tuple[Path, str]:
    local = Path(workdir) / "state-out" / "jev_spend.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(json.dumps(spend, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    return local, JEV_SPEND_PATH


def load_watch_times(store: Store, workdir: Path) -> Dict[str, dt.datetime]:
    """{watched url: when it was last served}. A missing or unreadable file is no history: every page is due."""
    local = Path(workdir) / "state-in" / "watch.json"
    try:
        if not store.get(WATCH_PATH, local):
            return {}
        return {str(url): dt.datetime.fromisoformat(when) for url, when in json.loads(local.read_text(encoding="utf-8")).items()}
    except Exception:
        return {}


def save_watch_times(times: Dict[str, dt.datetime], workdir: Path) -> Tuple[Path, str]:
    local = Path(workdir) / "state-out" / "watch.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(json.dumps({url: when.isoformat() for url, when in sorted(times.items())}, indent=1) + "\n", encoding="utf-8")
    return local, WATCH_PATH
