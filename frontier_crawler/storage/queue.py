"""The crawl queue on disk, so a run holds in memory only the part of it the run will use.

    state/queue/manifest.json         {"version": 1, "buckets": B, "rows": [rows per bucket], "dropped": {status: count}}
    state/queue/b000.parquet ...      the queued entries, one file per bucket of groups (a group: line, host, seed)
    state/queue/seen.parquet          the 64-bit key (storage.state.url_key) of every URL the queue has known, sorted
    state/queue/kept.parquet          entries kept whole that are not queued: retries, unparseable links, fetched starting URLs
    state/queue/host_lines.parquet    host -> the line covering it

A group's bucket is its 64-bit key mod B. Each bucket file is sorted by group and, within one, best first (static score down,
then url), in row groups of ROW_GROUP rows, so a group is rows [start, start + count) of its bucket and its best n are read
from the row groups holding them, not the whole file. One file per bucket rather than one file with an index: a save
rewrites a bucket whole, so it never holds more than 1/B of the queue, and a bucket no row of which changed is neither
rewritten nor uploaded. Every other URL ever known is only its key in the seen set: 8 bytes, and enough that no page is
queued twice. "dropped" counts those by status, so count("fetched") still adds up.

Atomic: a run writes every file in its final commit (one Hub commit); until then the store holds the previous state. The
local store, for tests and runs without the Hub, copies file by file. A store holding only the older state/frontier.parquet
(every URL, any status, one row each) is read from that, streamed in batches, and the new layout is written by that run's
save; the older file is left in place.
"""

from __future__ import annotations

import bisect
import json
from array import array
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from frontier_crawler.storage.state import read_u64, to_array, u64, unique_sorted, url_key, write_u64
from frontier_crawler.storage.store import Store

QUEUE_BUCKETS = 32     # PROVISIONAL: bucket files; at 3 million queued, about 94,000 rows and 1.6 MB each
ROW_GROUP = 4096       # rows per Parquet row group in a bucket: the unit read for one group
QUEUE_DIR = "state/queue"
MANIFEST_PATH = f"{QUEUE_DIR}/manifest.json"
SEEN_PATH = f"{QUEUE_DIR}/seen.parquet"
KEPT_PATH = f"{QUEUE_DIR}/kept.parquet"
HOST_LINES_PATH = f"{QUEUE_DIR}/host_lines.parquet"
OLD_FRONTIER_PATH = "state/frontier.parquet"

_TS = pa.timestamp("us", tz="UTC")
ENTRY_SCHEMA = pa.schema(
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
_HOST_LINES_SCHEMA = pa.schema([("host", pa.string()), ("line_id", pa.string())])


def group_key(line_id: str, host: str, seed_id: str) -> int:
    return url_key(f"{line_id}\x1f{host}\x1f{seed_id}")


def kept(entry: Dict[str, Any]) -> bool:
    """Whether an entry that is not queued is kept whole: a retry, an unparseable link (kept to be repaired), or a seed's
    starting URL once fetched (whether its own fetch stored anything decides if it is queued again)."""
    status = entry["status"]
    return status in ("retry", "unparseable") or (
        status in ("fetched", "failed") and entry["hops"] == 0 and entry["line_id"] == entry["seed_id"] and not entry.get("crossing"))


def _table(rows: Sequence[Dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pydict({name: [row.get(name) for row in rows] for name in ENTRY_SCHEMA.names}, schema=ENTRY_SCHEMA)


def _sorted(table: pa.Table, cap: int, decay: Tuple[float, float]) -> Tuple[pa.Table, int]:
    """The rows by group, best first within one (the queue's static score at weight 1: base x hop decay^hops x the
    generation decay for a crossing; then url), at most cap a group: (table, rows cut)."""
    hop, generation = decay
    static = pc.multiply(pc.multiply(table["base"], pc.power(hop, pc.cast(table["hops"], pa.float64()))),
                         pc.if_else(pc.fill_null(table["crossing"], False), generation, 1.0))
    table = table.append_column("_static", static)
    table = table.take(pc.sort_indices(table, [("line_id", "ascending"), ("host", "ascending"), ("seed_id", "ascending"),
                                                ("_static", "descending"), ("url", "ascending")])).drop_columns(["_static"])
    starts, _ = _runs(table)
    n = table.num_rows
    if n and max(b - a for a, b in zip(starts, starts[1:] + [n])) > cap:
        first = array("q", [0] * n)
        for start in starts:
            first[start] = start
        index = pa.array(range(n), pa.int64())
        rank = pc.subtract(index, pc.cumulative_max(pa.Array.from_buffers(pa.int64(), n, [None, pa.py_buffer(first)])))
        keep = pc.less(rank, cap)
        return table.filter(keep), n - pc.sum(keep).as_py()
    return table, 0


def _runs(table: pa.Table) -> Tuple[List[int], List[int]]:
    """(first row, rows) of each group in a table sorted by group."""
    n = table.num_rows
    if not n:
        return [], []
    changed = None
    for name in ("line_id", "host", "seed_id"):
        column = table[name]
        step = pc.fill_null(pc.not_equal(column.slice(0, n - 1), column.slice(1)), True)
        changed = step if changed is None else pc.or_(changed, step)
    starts = [0] + [i + 1 for i in pc.indices_nonzero(changed).to_pylist()]
    return starts, [b - a for a, b in zip(starts, starts[1:] + [n])]


class QueueFiles:
    """This run's copy of the queue's files, and writing the next one."""

    def __init__(self, local: Optional[Path] = None, decay: Tuple[float, float] = (1.0, 1.0), buckets: int = QUEUE_BUCKETS) -> None:
        self.local = local                # folder holding this run's copies of the bucket files
        self.decay = decay                # (hop, generation) decay of the queue's static score, which orders a group
        self.buckets = buckets
        self.rows = [0] * buckets         # rows in each bucket file
        self.seen = array("Q")
        self.kept: List[Dict[str, Any]] = []
        self.host_line: Dict[str, str] = {}
        self.dropped: Dict[str, int] = {}
        self.bytes_in = 0
        self.migrated = False

    def path(self, bucket: int) -> Path:
        return self.local / f"b{bucket:03d}.parquet"   # type: ignore[operator]

    @classmethod
    def load(cls, store: Store, workdir: Path, decay: Tuple[float, float]) -> "QueueFiles":
        files = cls(Path(workdir) / "state-in" / "queue", decay)
        manifest = files.local / "manifest.json"   # type: ignore[operator]
        if store.get(MANIFEST_PATH, manifest):
            data = json.loads(manifest.read_text(encoding="utf-8"))
            if data.get("version") != 1:
                raise ValueError("unknown queue version")
            files.buckets, files.rows, files.dropped = data["buckets"], data["rows"], data["dropped"]
            got = [manifest]
            for bucket, rows in enumerate(files.rows):
                if rows and not store.get(f"{QUEUE_DIR}/{files.path(bucket).name}", files.path(bucket)):
                    raise RuntimeError("the queue manifest names a bucket file the store lacks")   # never pass over a lost queue
                got += [files.path(bucket)] if rows else []
            for remote in (SEEN_PATH, KEPT_PATH, HOST_LINES_PATH):
                local = files.local / remote.rsplit("/", 1)[1]   # type: ignore[operator]
                if not store.get(remote, local):
                    raise RuntimeError("the queue manifest has no matching state file")
                got.append(local)
            files.seen = read_u64(got[-3], ("h",))[0]
            files.kept = pq.read_table(got[-2]).to_pylist()
            files.host_line = dict(zip(*(pq.read_table(got[-1]).column(name).to_pylist() for name in ("host", "line_id"))))
            files.bytes_in = sum(path.stat().st_size for path in got)
            return files
        old = Path(workdir) / "state-in" / "frontier.parquet"
        if store.get(OLD_FRONTIER_PATH, old):
            files.bytes_in = old.stat().st_size
            files._migrate(old)
        return files

    def _migrate(self, old: Path) -> None:
        """Read the older single file in batches: queued rows to bucket files, other URLs to the seen set (whole, if kept)."""
        self.migrated = True
        self.local.mkdir(parents=True, exist_ok=True)   # type: ignore[union-attr]
        source = pq.ParquetFile(old)
        names = [name for name in ENTRY_SCHEMA.names if name in source.schema_arrow.names]
        writers: Dict[int, pq.ParquetWriter] = {}
        dropped: Counter = Counter()
        for batch in source.iter_batches(columns=names, batch_size=65536):
            table = pa.Table.from_batches([batch])
            for field in ENTRY_SCHEMA:
                if field.name not in names:
                    table = table.append_column(field, pa.nulls(table.num_rows, field.type))
            table = table.select(ENTRY_SCHEMA.names).cast(ENTRY_SCHEMA)
            urls, hosts, lines, seeds, crossing, status = (table[name].to_pylist() for name in
                                                          ("url", "host", "line_id", "seed_id", "crossing", "status"))
            self.seen.extend(map(url_key, urls))
            for host, line, cross in zip(hosts, lines, crossing):   # as the queue built it from its entries, in file order
                if host and not cross:
                    self.host_line.setdefault(host, line)
            other = [i for i, s in enumerate(status) if s != "queued"]
            for row in table.take(pa.array(other, pa.int64())).to_pylist():
                if kept(row):
                    self.kept.append(row)
                else:
                    dropped[row["status"]] += 1
            queued = [i for i, s in enumerate(status) if s == "queued"]
            buckets = pa.array([group_key(lines[i], hosts[i], seeds[i]) % self.buckets for i in queued], pa.int64())
            queued_rows = table.take(pa.array(queued, pa.int64()))
            for bucket in set(buckets.to_pylist()):
                if bucket not in writers:
                    writers[bucket] = pq.ParquetWriter(self.path(bucket).with_suffix(".part"), ENTRY_SCHEMA)
                writers[bucket].write_table(queued_rows.filter(pc.equal(buckets, bucket)))
        for bucket, writer in writers.items():
            writer.close()
            part = self.path(bucket).with_suffix(".part")
            table, _ = _sorted(pq.read_table(part), 1 << 62, self.decay)   # caps are applied by the save, which counts them
            pq.write_table(table, self.path(bucket), compression="zstd", row_group_size=ROW_GROUP)
            part.unlink()
            self.rows[bucket] = table.num_rows
        self.dropped = dict(dropped)
        self.seen = to_array(unique_sorted(u64(self.seen)))

    def runs(self, bucket: int, columns: Sequence[str]) -> Tuple[Dict[str, List[Any]], List[int], List[int]]:
        """({column: values of each group's first row}, first rows, row counts) of the groups in a bucket."""
        table = pq.read_table(self.path(bucket), columns=sorted(set(columns) | {"line_id", "host", "seed_id"}))
        starts, counts = _runs(table)
        first = table.take(starts)
        return {name: first[name].to_pylist() for name in columns}, starts, counts

    def read(self, bucket: int, ranges: Sequence[Tuple[int, int]]) -> List[List[Dict[str, Any]]]:
        """The rows [start, start + n) of a bucket for each (start, n), reading only the row groups that hold them."""
        source = pq.ParquetFile(self.path(bucket))
        firsts = [0]   # the first row of each row group
        for i in range(source.metadata.num_row_groups):
            firsts.append(firsts[-1] + source.metadata.row_group(i).num_rows)
        groups = lambda start, n: range(bisect.bisect_right(firsts, start) - 1, bisect.bisect_right(firsts, start + n - 1))
        needed = sorted({group for start, n in ranges if n for group in groups(start, n)})
        table = source.read_row_groups(needed) if needed else None
        at, offset = {}, 0   # where each needed row group starts in table
        for group in needed:
            at[group], offset = offset, offset + firsts[group + 1] - firsts[group]
        out = []
        for start, n in ranges:
            if n:
                group = groups(start, n)[0]
                out.append(table.slice(at[group] + start - firsts[group], n).to_pylist())   # type: ignore[union-attr]
            else:
                out.append([])
        return out

    def has(self, key: int) -> bool:
        at = bisect.bisect_left(self.seen, key)
        return at < len(self.seen) and self.seen[at] == key

    def save(
        self, out: Path, adds: Dict[int, List[Dict[str, Any]]], drops: Dict[int, List[Tuple[int, int]]], urls: Iterable[str],
        kept_rows: List[Dict[str, Any]], host_line: Dict[str, str], dropped: Dict[str, int], cap: int,
    ) -> Tuple[List[Tuple[Path, str]], Dict[str, int]]:
        """Write the next state: each bucket with its dropped row ranges taken out, its added rows in and at most cap rows a
        group, when any of that changes it; the seen set with these urls; the kept entries, host lines and manifest.
        Returns (files to upload, {"cut": rows cut by cap, "rows": queued rows, "seen": keys}).
        """
        folder = Path(out) / "queue"
        folder.mkdir(parents=True, exist_ok=True)
        files: List[Tuple[Path, str]] = []
        rows, cut = list(self.rows), 0
        for bucket in range(self.buckets):
            if not adds.get(bucket) and not drops.get(bucket) and not (self.migrated and self.rows[bucket]):
                continue   # unchanged, and in the store already
            parts, at = [], 0
            old = pq.read_table(self.path(bucket)) if self.rows[bucket] else None
            for start, end in sorted(drops.get(bucket, ())):
                parts.append(old.slice(at, start - at))   # type: ignore[union-attr]
                at = end
            if old is not None:
                parts.append(old.slice(at))
            parts.append(_table(adds.get(bucket, [])))
            table, gone = _sorted(pa.concat_tables(parts), cap, self.decay)
            cut += gone
            local = folder / f"b{bucket:03d}.parquet"
            pq.write_table(table, local, compression="zstd", row_group_size=ROW_GROUP)
            files.append((local, f"{QUEUE_DIR}/{local.name}"))
            rows[bucket] = table.num_rows
        keys = unique_sorted(pa.concat_arrays([u64(self.seen), pa.array([url_key(url) for url in urls], pa.uint64())]))
        write_u64(folder / "seen.parquet", {"h": keys}, "h")
        pq.write_table(_table(kept_rows), folder / "kept.parquet", compression="zstd")
        lines = sorted(host_line.items())
        pq.write_table(pa.table({"host": [h for h, _ in lines], "line_id": [l for _, l in lines]}, schema=_HOST_LINES_SCHEMA),
                       folder / "host_lines.parquet", compression="zstd")
        (folder / "manifest.json").write_text(json.dumps({"version": 1, "buckets": self.buckets, "rows": rows, "dropped": dropped},
                                                        sort_keys=True) + "\n", encoding="utf-8")
        files += [(folder / remote.rsplit("/", 1)[1], remote) for remote in (SEEN_PATH, KEPT_PATH, HOST_LINES_PATH, MANIFEST_PATH)]
        return files, {"cut": cut, "rows": sum(rows), "seen": len(keys)}
