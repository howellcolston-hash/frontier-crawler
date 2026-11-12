"""Memory and time of the queue on disk at scale, on a synthetic queue. Not part of the test run.

    python tools/queue_bench.py QUEUED FETCHED [--workdir DIR] [--skip-whole]

Writes, in a local store under the work folder, the older single-file state (state/frontier.parquet) with QUEUED queued
and FETCHED fetched entries, the hosts and lines tables and state/url_state.parquet; then runs each phase in a fresh
process, so that each peak resident memory is its own:

    whole     the older way, for comparison: every entry read into memory, as the older code loaded it
    migrate   the first run on the new code: load (reading the older file), the first take, save
    run       a later run: load, the first take, 8,000 takes, each marked and adding 20 links (14 on its own site, 4 to
              other sites, 2 already known), save; with the URL state of the fetched pages loaded beside it

The shape: 60% of queued entries in groups of the line's own site (up to 1,000 each), 40% as crossings to other sites
in groups of 1 to 3. Prints counts, seconds, megabytes and bytes only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import random
import resource
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from frontier_crawler.frontier import Frontier, Seed, _HOSTS_SCHEMA, _LINES_SCHEMA   # noqa: E402
from frontier_crawler.storage.queue import ENTRY_SCHEMA   # noqa: E402
from frontier_crawler.storage.state import UrlState   # noqa: E402
from frontier_crawler.storage.store import LocalStore   # noqa: E402

NOW = dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)
SEEDS = 60
TAKES = 8_000


def _mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def build(store_dir: Path, queued: int, fetched: int) -> None:
    rng = random.Random(1)
    lines_n = max(SEEDS, queued // 100)
    lines = []
    for i in range(lines_n):
        seed = f"s{i % SEEDS}"
        line_id = seed if i < SEEDS else f"{seed}:l{i}.test"
        lines.append({"line_id": line_id, "seed_id": seed, "parent_line": None if i < SEEDS else f"s{i % SEEDS}", "root_url": f"https://l{i}.test/",
                      "created_at": NOW, "fetches": rng.randint(0, 300), "content": 1, "yield": rng.random(), "relevance": rng.random(),
                      "inbound": 0, "generation": 0 if i < SEEDS else rng.randint(1, 8), "arm_alpha": None, "arm_beta": None, "arm_at": None})
    home = [f"l{i}.test" for i in range(lines_n)]
    state = store_dir / "state"
    state.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(lines, schema=_LINES_SCHEMA), state / "lines.parquet")
    hosts = [{"host": h, "fetches": rng.randint(0, 200), "content": 0, "heavy": 0, "refused": 0, "errors": 0, "error_pages": 0, "sitemap_read_at": None} for h in home]
    pq.write_table(pa.Table.from_pylist(hosts, schema=_HOSTS_SCHEMA), state / "hosts.parquet")
    writer = pq.ParquetWriter(state / "frontier.parquet", ENTRY_SCHEMA)
    batch, n = [], 0

    def flush() -> None:
        writer.write_table(pa.Table.from_pylist(batch, schema=ENTRY_SCHEMA))
        batch.clear()

    def row(url: str, host: str, line: dict, crossing: bool, status: str, hops: int) -> None:
        batch.append({"url": url, "host": host, "seed_id": line["seed_id"], "line_id": line["line_id"], "crossing": crossing, "hops": hops,
                      "parent_fetch_id": "00000000-0000-4000-8000-%012d" % (n % 10**12), "anchor": "a link", "base": rng.choice([0.3, 0.3, 0.5, 0.8, 1.0]),
                      "status": status, "discovered_at": NOW, "fetched_at": NOW if status != "queued" else None, "stored": True if status != "queued" else None})
        if len(batch) >= 50_000:
            flush()

    own = int(queued * 0.6)
    while n < own:   # own-site groups, sizes skewed, at most 1,000
        i = rng.randrange(lines_n)
        for _ in range(min(1000, int(rng.paretovariate(1.2) * 5), own - n)):
            row(f"https://{home[i]}/page/{n}?q={rng.getrandbits(20)}", home[i], lines[i], False, "queued", rng.randint(1, 5))
            n += 1
    while n < queued:   # crossings, groups of 1 to 3, to a pool of other sites
        i, host = rng.randrange(lines_n), f"x{rng.randrange(queued // 5)}.example.org"
        for _ in range(min(rng.randint(1, 3), queued - n)):
            row(f"https://{host}/{n}", host, lines[i], True, "queued", rng.randint(1, 6))
            n += 1
    urls = []
    for k in range(fetched):
        i = rng.randrange(lines_n)
        url = f"https://{home[i]}/done/{k}"
        row(url, home[i], lines[i], False, "fetched", rng.randint(1, 5))
        urls.append(url)
        n += 1
    flush()
    writer.close()
    pq.write_table(pa.table({"url": urls, "sha256": [hashlib.sha256(u.encode()).hexdigest() for u in urls],
                             "last_fetched": pa.array([NOW] * len(urls), pa.timestamp("us", tz="UTC")), "http_status": pa.array([200] * len(urls), pa.int32())}),
                   state / "url_state.parquet")


def phase(name: str, workdir: Path) -> dict:
    store = LocalStore(workdir / "store")
    seeds = [Seed(id=f"s{i}", entity=f"s{i}", urls=(f"https://l{i}.test/",)) for i in range(SEEDS)]
    out: dict = {"phase": name}
    began = time.monotonic()
    if name == "whole":
        local = workdir / "store" / "state"
        rows = pq.read_table(local / "frontier.parquet").to_pylist()
        Frontier(rows, pq.read_table(local / "hosts.parquet").to_pylist(), pq.read_table(local / "lines.parquet").to_pylist())
        out.update(load_seconds=round(time.monotonic() - began, 1), peak_rss_mb=_mb())
        return out
    run = workdir / f"run-{name}"
    frontier = Frontier.load(store, run)
    url_state = UrlState.load(store, run) if name == "run" else None
    out["after_load_rss_mb"] = _mb()
    frontier.add_seeds(seeds, NOW)
    rng = random.Random(2)
    first = time.monotonic()
    entry = frontier.take(seeds, NOW.date())
    out["first_take_seconds"] = round(time.monotonic() - first, 1)
    takes = TAKES if name == "run" else 0
    known = [f"https://l{i}.test/done/{i}" for i in range(100)]
    fetching = time.monotonic()
    for k in range(takes):
        if entry is None:
            break
        page = {"outcome": "stored", "http_status": 200, "url_final": entry["url"], "tier_used": "DOM_TRAFILATURA", "word_count": 300,
                "fetch_id": f"f{k}", "block_marker": None}
        links = [(f"https://{entry['host']}/new/{k}/{j}", "x") for j in range(14)]
        links += [(f"https://x{rng.randrange(1_000_000)}.example.org/n{k}{j}", "x") for j in range(2)]
        links += [(f"https://l{rng.randrange(SEEDS, 1000)}.test/n{k}{j}", "x") for j in range(2)]
        links += [(rng.choice(known), "x") for _ in range(2)]
        page["links"] = (links, [])
        frontier.mark(entry, page, None, NOW)
        frontier.add_links(entry, page, None, NOW)
        if url_state is not None:
            url_state.same(entry["url"], "0" * 64)
            url_state.record(entry["url"], hashlib.sha256(entry["url"].encode()).hexdigest())
        entry = frontier.take(seeds, NOW.date())
    out["takes_seconds"] = round(time.monotonic() - fetching, 1)
    files = frontier.save(run)
    if url_state is not None:
        files.append(url_state.save(run))
    copied = time.monotonic()
    store.put_many(files, "bench")
    out["copy_seconds"] = round(time.monotonic() - copied, 1)
    record = frontier.queue_record()
    out.update({key: record[key] for key in ("migrated", "load_seconds", "select_seconds", "save_seconds", "bytes_in", "bytes_out",
                                             "groups_hot", "groups_cold", "entries_hot", "entries_cold", "groups_promoted",
                                             "entries_appended_cold", "cut_group_cap", "cut_total_cap", "seen")})
    out.update(queue_files_out=sum(1 for _, remote in files if remote.startswith("state/queue/b")), queued=frontier.count("queued"),
               peak_rss_mb=_mb(), total_seconds=round(time.monotonic() - began, 1))
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("queued", type=int)
    parser.add_argument("fetched", type=int)
    parser.add_argument("--workdir", default="/tmp/queue-bench")
    parser.add_argument("--phase")
    parser.add_argument("--skip-whole", action="store_true", help="leave out the older way (about 1.7 KB an entry in memory)")
    args = parser.parse_args()
    workdir = Path(args.workdir)
    if args.phase:
        print(json.dumps(phase(args.phase, workdir)))
        return
    import shutil

    shutil.rmtree(workdir, ignore_errors=True)
    began = time.monotonic()
    build(workdir / "store", args.queued, args.fetched)
    print(json.dumps({"built": {"queued": args.queued, "fetched": args.fetched, "seconds": round(time.monotonic() - began, 1),
                                "old_file_bytes": (workdir / "store" / "state" / "frontier.parquet").stat().st_size}}))
    for name in ("migrate", "run") if args.skip_whole else ("whole", "migrate", "run"):
        result = subprocess.run([sys.executable, __file__, str(args.queued), str(args.fetched), "--workdir", str(workdir), "--phase", name],
                                capture_output=True, text=True)
        print(result.stdout.strip() or result.stderr[-2000:])


if __name__ == "__main__":
    main()
