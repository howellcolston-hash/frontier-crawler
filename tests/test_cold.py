"""The size rule, raw bodies shards on an rclone remote, and the cold tier's read-through. Offline: a fake rclone executable
maps a remote `fake:<folder>` onto a local directory."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any, Dict, List

import pyarrow.parquet as pq
import pytest

from frontier_crawler import pipeline
from frontier_crawler.harvester.cascade import ScrapeResult, Target
from frontier_crawler.pipeline import Engine
from frontier_crawler.storage.cold import ColdTier
from frontier_crawler.storage.shards import BODY_SCHEMA, ShardWriter, bodies_of, day_folder
from frontier_crawler.storage.store import LocalStore

NOW = dt.datetime(2026, 11, 25, 12, 0, tzinfo=dt.timezone.utc)
DAY = NOW.date()
WORDS = "The tender board published the award and the bidders for road works. " * 20   # well over 150 words

FAKE_RCLONE = r'''
import hashlib, json, os, shutil, sys
from pathlib import Path
root, args = Path(os.environ["FAKE_RCLONE_DIR"]), sys.argv[1:]
def local(p): return root / p.split(":", 1)[1] if ":" in p else Path(p)
if os.environ.get("FAKE_RCLONE_FAIL") == args[0]: sys.exit(1)
if args[0] == "copyto":
    src, dst = local(args[1]), local(args[2])
    if not src.is_file(): sys.exit(3)
    dst.parent.mkdir(parents=True, exist_ok=True); shutil.copyfile(src, dst)
    if os.environ.get("FAKE_RCLONE_CORRUPT") and ":" in args[2]: dst.write_bytes(dst.read_bytes()[:-1] + b"x")
elif args[0] == "lsjson":
    f = local(args[-1])
    if not f.is_file(): sys.exit(3)
    data = f.read_bytes()
    print(json.dumps([{"Path": f.name, "Size": len(data), "Hashes": {"md5": hashlib.md5(data).hexdigest(), "sha1": hashlib.sha1(data).hexdigest(), "sha256": hashlib.sha256(data).hexdigest()}}]))
'''


@pytest.fixture
def rclone(tmp_path: Path, monkeypatch: Any) -> str:
    script = tmp_path / "bin" / "rclone"
    script.parent.mkdir()
    script.write_text(f"#!{sys.executable}\n{FAKE_RCLONE}")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("FAKE_RCLONE_DIR", str(tmp_path / "drive"))
    return str(script)


class Cascade:
    def __init__(self, pages: Dict[str, ScrapeResult]) -> None:
        self.pages = pages

    async def fetch(self, url: str) -> ScrapeResult:
        return self.pages[url]


def _html(url: str, size: int, text: str = WORDS, note: Any = None) -> ScrapeResult:
    raw = (f"<html>{text}</html>".encode() + b" " * size)[:max(size, 1)]
    return ScrapeResult(url, 200, raw, text or None, len(text.split()), "DOM_TRAFILATURA", note, "text/html", links=([], 0))  # type: ignore[arg-type]


def _run(tmp_path: Path, pages: Dict[str, ScrapeResult], run: str, **kwargs: Any) -> Dict[str, Any]:
    async def go() -> Dict[str, Any]:
        engine = Engine(LocalStore(tmp_path / "store"), Cascade(pages), workdir=tmp_path / f"work-{run}", now=lambda: NOW, run_id=run, **kwargs)
        await engine.start()
        await engine.run([Target(url, "Example", "example", "example:a.test", 0, None) for url in pages])
        return await engine.finish()

    return asyncio.run(go())


def _hashes(root: Path) -> set:
    return {row["sha256"] for path in sorted(root.glob("bodies/**/*.parquet")) for row in pq.read_table(path, columns=["sha256"]).to_pylist()}


def _fetches(tmp_path: Path) -> Dict[str, Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in (tmp_path / "store" / day_folder("fetches", DAY)).glob("*.parquet"):
        rows += pq.read_table(path).to_pylist()
    return {row["url_final"]: row for row in rows}


# ---------------------------------------------------------------- the size rule

def test_a_large_page_whose_text_was_read_keeps_text_only_and_a_lossy_one_keeps_its_bytes(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(pipeline, "RAW_MAX_BYTES", 3000)
    pages = {"https://a.test/big": _html("https://a.test/big", 5000),
             "https://a.test/thin": _html("https://a.test/thin", 5000, text="Loading " * 20),          # a shell: data in scripts
             "https://a.test/noted": _html("https://a.test/noted", 5000, text="noted " + WORDS, note="html not read: timeout"),
             "https://a.test/small": _html("https://a.test/small", 2000)}
    stats = _run(tmp_path, pages, "r1")
    rows, kept = _fetches(tmp_path), _hashes(tmp_path / "store")
    assert rows["https://a.test/big"]["sha256"] not in kept and rows["https://a.test/big"]["text"] == WORDS
    assert {rows[u]["sha256"] for u in ("https://a.test/thin", "https://a.test/noted", "https://a.test/small")} == kept
    assert (stats["large_text_only"], stats["large_text_only_bytes"], stats["bodies_written"]) == (1, 5000, 3)


def test_the_size_rule_leaves_pdfs_to_pdf_bodies(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(pipeline, "RAW_MAX_BYTES", 10)
    pdf = ScrapeResult("https://a.test/a.pdf", 200, b"%PDF " * 100, WORDS, len(WORDS.split()), "PDF", None, "application/pdf")
    stats = _run(tmp_path, {pdf.url: pdf}, "r1", pdf_bodies="keep")
    assert stats["bodies_written"] == 1 and stats["large_text_only"] == 0


# ---------------------------------------------------------------- raw bodies on the remote

def test_raw_shards_go_to_the_remote_checked_and_listed_and_read_back_through_the_cold_tier(tmp_path: Path, rclone: str) -> None:
    pages = {f"https://a.test/{n}": _html(f"https://a.test/{n}", 500, text=f"{n} {WORDS}") for n in range(3)}
    stats = _run(tmp_path, pages, "r1", cold=ColdTier("fake:raw", rclone=rclone))
    store = tmp_path / "store"
    assert _hashes(store) == set() and stats["bodies_written"] == 3
    (line,) = [json.loads(t) for t in (store / "cold" / "raw-r1.jsonl").read_text().splitlines()]
    on_drive = tmp_path / "drive" / "raw" / line["path"]
    assert line["path"].startswith(day_folder("bodies", DAY)) and line["remote"] == "fake:raw" and line["run"] == "r1"
    assert on_drive.stat().st_size == line["size"] and pq.read_table(on_drive).num_rows == 3
    assert stats["raw_remote"] == {"status": "on", "shards": 1, "bytes": line["size"], "failed": 0, "failed_bytes": 0}
    rows = list(_fetches(tmp_path).values())
    found = dict((row["fetch_id"], body) for row, body in bodies_of(LocalStore(store), tmp_path / "read", rows, DAY, ColdTier(rclone=rclone)))
    assert all(found[row["fetch_id"]].startswith(f"<html>{row['url_final'][-1]} ".encode()) for row in rows)
    # Without rclone the cold tier is not searched: the bytes read as missing, and nothing raises.
    assert [body for _, body in bodies_of(LocalStore(store), tmp_path / "read", rows[:1])] == [None]


@pytest.mark.parametrize("trouble", ["FAKE_RCLONE_FAIL", "FAKE_RCLONE_CORRUPT"])
def test_a_copy_the_remote_does_not_take_goes_to_the_store_and_three_stop_the_remote(tmp_path: Path, rclone: str, monkeypatch: Any,
                                                                                     trouble: str) -> None:
    monkeypatch.setenv(trouble, "copyto" if trouble == "FAKE_RCLONE_FAIL" else "1")
    monkeypatch.setattr(pipeline, "_UPLOAD_EVERY_S", 0.0)   # one shard per flush, so the run makes several
    pages = {f"https://a.test/{n}": _html(f"https://a.test/{n}", 500, text=f"{n} {WORDS}") for n in range(5)}
    stats = _run(tmp_path, pages, "r1", cold=ColdTier("fake:raw", rclone=rclone), concurrency=1)
    assert _hashes(tmp_path / "store") == {row["sha256"] for row in _fetches(tmp_path).values()}   # no body lost
    assert not (tmp_path / "store" / "cold").exists()
    raw = stats["raw_remote"]
    assert raw["status"] == "stopped" and raw["failed"] == 3 and raw["shards"] == 0 and raw["failed_bytes"] > 0


def test_a_shard_on_the_remote_is_not_copied_again_when_the_store_commit_failed(tmp_path: Path, rclone: str) -> None:
    class Flaky(LocalStore):
        fail = True

        def put_many(self, files: Any, message: str = "") -> None:
            if Flaky.fail:
                Flaky.fail = False
                raise RuntimeError("hub down")
            super().put_many(files, message)

    cold = ColdTier("fake:raw", rclone=rclone)
    puts: List[str] = []
    real_put = cold.put
    cold.put = lambda local, path, run, now: puts.append(path) or real_put(local, path, run, now)  # type: ignore[method-assign]

    async def go() -> None:
        url = "https://a.test/x"
        engine = Engine(Flaky(tmp_path / "store"), Cascade({url: _html(url, 500)}), workdir=tmp_path / "w", now=lambda: NOW, run_id="r1", cold=cold)
        await engine.start()
        await engine.run([Target(url, "Example", "example", "example:a.test", 0, None)])
        with pytest.raises(RuntimeError):
            await engine.flush(force=True)
        await engine.finish()

    asyncio.run(go())
    assert len(puts) == 1 and len((tmp_path / "store" / "cold" / "raw-r1.jsonl").read_text().splitlines()) == 1


# ---------------------------------------------------------------- the read-through

def _shard(root: Path, day: dt.date, run: str, bodies: Dict[str, bytes]) -> str:
    writer = ShardWriter("bodies", BODY_SCHEMA, run, day, root)
    for sha, body in bodies.items():
        writer.add({"sha256": sha, "body": body})
    return writer.write(writer.take(force=True))[1]


class FakeCold:
    """Reads manifest lines from a local folder; records each get."""

    def __init__(self, folder: Path, lines: Dict[str, Dict[str, Any]]) -> None:
        self.folder, self._lines, self.got = folder, lines, []

    def lines(self, store: Any, workdir: Path) -> Dict[str, Dict[str, Any]]:
        return self._lines

    def get(self, line: Dict[str, Any], local: Path) -> bool:
        self.got.append(line["path"])
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes((self.folder / line["path"]).read_bytes())
        return True


def test_the_cold_tier_is_searched_after_the_store_its_own_day_first(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    held = _shard(tmp_path / "store", DAY, "r0", {"a" * 64: b"in the store"})
    older = DAY - dt.timedelta(days=3)
    cold_new = _shard(tmp_path / "moved", DAY - dt.timedelta(days=1), "r1", {"b" * 64: b"newer copy"})
    cold_own = _shard(tmp_path / "moved", older, "r2", {"b" * 64: b"own day copy"})
    cold = FakeCold(tmp_path / "moved", {cold_new: {"path": cold_new}, cold_own: {"path": cold_own}, held: {"path": held}})
    got = list(bodies_of(store, tmp_path / "w", [{"sha256": "a" * 64}], DAY, cold))
    assert got == [({"sha256": "a" * 64}, b"in the store")] and cold.got == []       # in the store: the remote is not asked
    got = list(bodies_of(store, tmp_path / "w", [{"sha256": "b" * 64}], older, cold))
    assert got == [({"sha256": "b" * 64}, b"own day copy")] and cold.got == [cold_own]
    assert list(bodies_of(store, tmp_path / "w", [{"sha256": "c" * 64}], None, cold)) == [({"sha256": "c" * 64}, None)]


def test_from_env_needs_rclone_and_a_remote_path(tmp_path: Path, monkeypatch: Any, rclone: str) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "nothing"))
    monkeypatch.setenv("CRAWLER_RAW_REMOTE", "fake:raw")
    assert ColdTier.from_env() is None
    monkeypatch.setenv("PATH", str(Path(rclone).parent) + os.pathsep + os.environ.get("PATH", ""))
    assert ColdTier.from_env().root == "fake:raw"
    monkeypatch.delenv("CRAWLER_RAW_REMOTE")
    assert ColdTier.from_env().root is None   # reads only
    with pytest.raises(ValueError):
        ColdTier("no-colon")


def test_a_read_that_does_not_match_its_line_is_refused(tmp_path: Path, rclone: str) -> None:
    (tmp_path / "drive" / "raw" / "bodies").mkdir(parents=True)
    (tmp_path / "drive" / "raw" / "bodies" / "x.parquet").write_bytes(b"changed")
    local = tmp_path / "w" / "x.parquet"
    assert not ColdTier(rclone=rclone).get({"remote": "fake:raw", "path": "bodies/x.parquet", "sha256": "0" * 64}, local)
    assert not local.exists()
    assert not ColdTier(rclone=rclone).get({"remote": "fake:raw", "path": "../x", "sha256": "0" * 64}, local)
