"""The cold tier: bodies shards kept on an rclone remote (Google Drive, later any rclone backend) instead of the store.

Two ways in, one manifest format, so one read-through serves both:
    - the crawl writes its own bodies shards there when CRAWLER_RAW_REMOTE is set (pipeline.Engine.flush), and
    - an operator may move old shards there by hand.
Each shard on the remote is listed by one JSON line in the store under cold/*.jsonl:
    {"path", "size", "sha256", "sha1", "md5", "remote", "run", "copied_at"}
path is the shard's store path (bodies/year=.../<run>-000.parquet), kept unchanged under the remote root `remote`.

Input contract:
    cold = ColdTier.from_env()                      None when rclone is not on PATH; root (where put writes) from CRAWLER_RAW_REMOTE
    line = cold.put(local, path, run, now)          copy, then check size and every hash the remote reports; None if it failed
    cold.get(line, local) -> bool                   False when the copy is absent, unreadable or not the bytes listed
    cold.lines(store, workdir) -> {path: line}      every manifest line in the store, the later manifest winning
The remote's credentials stay with rclone: its config file, or RCLONE_CONFIG_<NAME>_* environment variables. Nothing here
logs a path, a remote or rclone's output: counts and exit codes only.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from frontier_crawler.storage.store import _check

logger = logging.getLogger("frontier_crawler.cold")

MANIFESTS = "cold"
HASHES = ("sha256", "sha1", "md5")
RCLONE_TIMEOUT_S = 900.0   # PROVISIONAL: one copy of a shard (up to about 64 MB); rclone retries inside it


def sums(path: Path) -> Tuple[int, Dict[str, str]]:
    digests = {name: hashlib.new(name) for name in HASHES}
    size = 0
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            size += len(chunk)
            for digest in digests.values():
                digest.update(chunk)
    return size, {name: digest.hexdigest() for name, digest in digests.items()}


class ColdTier:
    def __init__(self, root: Optional[str] = None, rclone: str = "rclone") -> None:
        if root is not None and ":" not in root:
            raise ValueError("the raw remote must be an rclone remote path, name:folder")
        self.root = root.rstrip("/") if root else None
        self._rclone = rclone
        self._lines: Optional[Dict[str, Dict[str, Any]]] = None

    @classmethod
    def from_env(cls) -> Optional["ColdTier"]:
        root = os.environ.get("CRAWLER_RAW_REMOTE", "").strip() or None
        rclone = shutil.which("rclone")
        if rclone is None:
            if root:
                logger.warning("CRAWLER_RAW_REMOTE is set but rclone is not installed: bodies go to the store")
            return None
        return cls(root, rclone)

    def _run(self, *args: str) -> str:
        return subprocess.run([self._rclone, *args], check=True, capture_output=True, text=True, timeout=RCLONE_TIMEOUT_S).stdout

    def put(self, local: Path, path: str, run: str, now: dt.datetime) -> Optional[Dict[str, Any]]:
        """Copy one file to <root>/<path> and check the copy: same size, and every hash the remote keeps (Drive: MD5,
        SHA-1, SHA-256) matches. The manifest line, or None when the copy failed or a remote keeps none of them."""
        size, local_sums = sums(local)
        target = f"{self.root}/{_check(path)}"
        try:
            self._run("copyto", str(local), target)
            found = json.loads(self._run("lsjson", "--hash", "--files-only", target) or "[]")
        except (subprocess.SubprocessError, OSError, ValueError) as exc:
            logger.warning("raw remote: copy failed (%s)", getattr(exc, "returncode", type(exc).__name__))
            return None
        if len(found) != 1 or int(found[0].get("Size", -1)) != size:
            logger.warning("raw remote: the copy is missing or its size differs")
            return None
        remote = {k.lower().replace("-", ""): str(v).lower() for k, v in (found[0].get("Hashes") or {}).items()}
        shared = [name for name in HASHES if remote.get(name)]
        if not shared or any(remote[name] != local_sums[name] for name in shared):
            logger.warning("raw remote: the copy's hashes %s", "differ" if shared else "are not reported")
            return None
        return {"path": path, "size": size, **local_sums, "remote": self.root, "run": run, "copied_at": now.isoformat()}

    def get(self, line: Dict[str, Any], local: Path) -> bool:
        # shortcut: reads go to the remote each line names; moving the files to another home needs the lines rewritten.
        local = Path(local)
        local.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._run("copyto", f"{str(line['remote']).rstrip('/')}/{_check(line['path'])}", str(local))
            ok = local.is_file() and sums(local)[1]["sha256"] == line["sha256"]
        except (subprocess.SubprocessError, OSError, KeyError, ValueError) as exc:
            logger.warning("cold tier: read failed (%s)", getattr(exc, "returncode", type(exc).__name__))
            ok = False
        if not ok:
            local.unlink(missing_ok=True)
        return ok

    def lines(self, store: Any, workdir: Path) -> Dict[str, Dict[str, Any]]:
        """{store path: its manifest line}, read once per ColdTier."""
        if self._lines is None:
            self._lines = cold_lines(store, workdir)
        return self._lines


def cold_lines(store: Any, workdir: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for remote in store.list(MANIFESTS):
        local = Path(workdir) / "in" / remote
        if not remote.endswith(".jsonl") or not store.get(remote, local):
            continue
        for text in local.read_text(encoding="utf-8").splitlines():
            if text.strip():
                line = json.loads(text)
                out[line["path"]] = line
        local.unlink(missing_ok=True)
    return out
