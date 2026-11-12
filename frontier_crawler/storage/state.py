"""Small state the engine carries between runs: what each URL last served, which bodies are stored, Jev spend per month,
and when each watched page was last read.

Each is a single file in the store, read at the start of a run and rewritten at the end.
A run that dies before the end leaves the previous state in place: the next run then stores
those documents again, which costs space but loses nothing.
"""

from __future__ import annotations

import bisect
import datetime as dt
import hashlib
import json
from array import array
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from frontier_crawler.storage.store import Store

URL_STATE_PATH = "state/url_state.parquet"     # before 2026-11-12: url, sha256, last_fetched, http_status; read once, left in place
URL_HASHES_PATH = "state/url_hashes.parquet"
JEV_SPEND_PATH = "state/jev_spend.json"
BODY_HASHES_PATH = "state/body_hashes.parquet"
WATCH_PATH = "state/watch.json"


def url_key(text: str) -> int:
    """A URL's 64-bit key: the first 8 bytes of its BLAKE2b hash. Two of n URLs share one with probability about n^2 / 2^65."""
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=8).digest(), "little")


def u64(values: array) -> pa.Array:
    """An array("Q") as an Arrow uint64 array, without copying."""
    return pa.Array.from_buffers(pa.uint64(), len(values), [None, pa.py_buffer(values)])


def to_array(values: pa.Array) -> array:
    """An Arrow uint64 array as an array("Q").
    shortcut: read and written in this machine's byte order, little-endian on every runner used."""
    values, out = values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values, array("Q")
    if len(values):
        out.frombytes(values.buffers()[1].slice(values.offset * 8, len(values) * 8))
    return out


def unique_sorted(values: pa.Array) -> pa.Array:
    values = values.take(pc.sort_indices(values))
    return values.filter(last_of_runs(values))


def read_u64(local: Path, names: Sequence[str]) -> List[array]:
    """uint64 columns of a Parquet file as array("Q")s."""
    table = pq.read_table(local, columns=list(names))
    return [to_array(table.column(name)) for name in names]


def write_u64(local: Path, columns: Dict[str, pa.Array], sorted_name: str) -> None:
    """Write uint64 columns, the sorted one delta-encoded (about 5 bytes a value at a few million random keys)."""
    local.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), local, compression="zstd", use_dictionary=False,
                   column_encoding={name: "DELTA_BINARY_PACKED" if name == sorted_name else "PLAIN" for name in columns})


def last_of_runs(keys: pa.Array) -> pa.Array:
    """True at the last of each run of equal values in a sorted array."""
    if not len(keys):
        return pa.array([], pa.bool_())
    return pa.concat_arrays([pc.not_equal(keys.slice(0, len(keys) - 1), keys.slice(1)), pa.array([True])])


class UrlState:
    """url -> what it last served, to tell an unchanged page: the URL's 64-bit key -> the first 8 bytes of its content's SHA-256,
    16 bytes a URL in memory (two sorted arrays, searched by bisection) instead of a row of Python objects. A page is
    wrongly called unchanged only if two URLs share a key and their contents those 8 bytes: never, in practice. A store
    with only the older url_state.parquet is read from that once; the older file is left in place."""

    def __init__(self, keys: Optional[array] = None, values: Optional[array] = None) -> None:
        self._keys = keys if keys is not None else array("Q")
        self._values = values if values is not None else array("Q")
        self._new: Dict[int, int] = {}

    @classmethod
    def load(cls, store: Store, workdir: Path) -> "UrlState":
        local = Path(workdir) / "state-in" / "url_hashes.parquet"
        if store.get(URL_HASHES_PATH, local):
            return cls(*read_u64(local, ("k", "h")))
        state, old = cls(), Path(workdir) / "state-in" / "url_state.parquet"
        if store.get(URL_STATE_PATH, old):
            for batch in pq.ParquetFile(old).iter_batches(columns=["url", "sha256"]):
                for url, sha256 in zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()):
                    if url and sha256:
                        state.record(url, sha256)
        return state

    def same(self, url: str, sha256: str) -> bool:
        """Whether url last served content with this hash."""
        key = url_key(url)
        value = self._new.get(key)
        if value is None:
            at = bisect.bisect_left(self._keys, key)
            value = self._values[at] if at < len(self._keys) and self._keys[at] == key else None
        return value == int(sha256[:16], 16)

    def record(self, url: str, sha256: str) -> None:
        self._new[url_key(url)] = int(sha256[:16], 16)

    def save(self, workdir: Path) -> Tuple[Path, str]:
        local = Path(workdir) / "state-out" / "url_hashes.parquet"
        keys = pa.concat_arrays([u64(self._keys), pa.array(list(self._new), pa.uint64())])
        values = pa.concat_arrays([u64(self._values), pa.array(list(self._new.values()), pa.uint64())])
        order = pc.sort_indices(keys)   # stable: for a key in both, the old value comes first and this run's last
        keys, values = keys.take(order), values.take(order)
        last = last_of_runs(keys)
        write_u64(local, {"k": keys.filter(last), "h": values.filter(last)}, "k")
        return local, URL_HASHES_PATH


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
        return cls(read_u64(local, ("h",))[0] if store.get(BODY_HASHES_PATH, local) else array("Q"))

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
        keys = pa.concat_arrays([u64(self._known), pa.array(sorted(new), pa.uint64())])
        local = Path(workdir) / "state-out" / "body_hashes.parquet"
        write_u64(local, {"h": keys.take(pc.sort_indices(keys))}, "h")
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
