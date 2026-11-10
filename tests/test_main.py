import json
from pathlib import Path

import pytest

from frontier_crawler import __main__ as cli
from frontier_crawler.storage.store import LocalStore


def test_a_run_prints_counts_only_and_records_the_rest_in_the_store(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "open_store", lambda: LocalStore(tmp_path))

    async def job(args, ctx):
        ctx["stats"] = {"stored": 3, "stopped_by": "max_minutes", "seed_url_problems": {"https://secret.test/": "error"}}
        raise RuntimeError("could not reach https://secret.test/")

    with pytest.raises(SystemExit) as stopped:
        cli._run(type("Args", (), {"command": "crawl"})(), {"max_minutes": 1}, job)
    printed = capsys.readouterr()
    assert "secret.test" not in printed.out + printed.err + str(stopped.value)
    assert json.loads(printed.out) == {"status": "failed", "stopped_by": "max_minutes", "stored": 3}
    (record,) = [json.loads(p.read_text()) for p in (tmp_path / "runs").glob("*-crawl.json")]
    assert record["status"] == "failed" and "secret.test" in record["notes"][0] and record["stats"]["stored"] == 3
