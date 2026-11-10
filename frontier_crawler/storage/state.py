"""Small state the engine carries between runs: what each URL last served, and Jev spend per month.

Both are single files in the store, read at the start of a run and rewritten at the end.
A run that dies before the end leaves the previous state in place: the next run then stores
those documents again, which costs space but loses nothing.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from frontier_crawler.storage.store import Store

URL_STATE_PATH = "state/url_state.parquet"
JEV_SPEND_PATH = "state/jev_spend.json"

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


def load_spend(store: Store, workdir: Path) -> Dict[str, Dict[str, int]]:
    """{"2026-11": {"input_tokens": n, "calls": m}}"""
    local = Path(workdir) / "state-in" / "jev_spend.json"
    if not store.get(JEV_SPEND_PATH, local):
        return {}
    data = json.loads(local.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def save_spend(spend: Dict[str, Dict[str, int]], workdir: Path) -> Tuple[Path, str]:
    local = Path(workdir) / "state-out" / "jev_spend.json"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(json.dumps(spend, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    return local, JEV_SPEND_PATH
