import json
from pathlib import Path

import pytest

from frontier_crawler import __main__ as cli
from frontier_crawler.storage.store import LocalStore


def test_a_steering_file_is_optional_and_a_broken_one_is_ignored_and_counted(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    assert cli._steer(store, tmp_path / "w0") == ({}, 0)
    for n, text in enumerate([
        '{"version": 1, "generated": "2026-11-12T00:00:00Z", "sites": {"example.com": 0.5, "example.org": 0}}',
        '{"version": 2, "sites": {"example.com": 0.5}}', '{"version": 1, "sites": {"example.com": -1}}',
        '{"version": 1, "sites": {"example.com": "high"}}', '{"version": 1}', "[1]", "not json",
    ]):
        (tmp_path / "store" / "config").mkdir(parents=True, exist_ok=True)
        (tmp_path / "store" / "config" / "steer.json").write_text(text)
        expected = ({"example.com": 0.5, "example.org": 0.0}, 0) if n == 0 else ({}, 1)
        assert cli._steer(store, tmp_path / f"w{n + 1}") == expected, text


def test_a_ranking_file_is_optional_and_a_broken_one_runs_the_fixed_rule_and_is_counted(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    assert cli._ranking(store, tmp_path / "w0") == (("fixed", 1.0), 0)
    (tmp_path / "store" / "config").mkdir(parents=True)
    for n, (text, expected) in enumerate([
        ('{"version": 1, "mode": "bandit", "temperature": 0.5}', (("bandit", 0.5), 0)), ('{"version": 1, "mode": "shadow"}', (("shadow", 1.0), 0)),
        ('{"version": 1, "mode": "fixed", "temperature": 0}', (("fixed", 0.0), 0)),
        ('{"version": 1, "mode": "greedy"}', (("fixed", 1.0), 1)), ('{"version": 2, "mode": "bandit"}', (("fixed", 1.0), 1)),
        ('{"version": 1, "mode": "bandit", "temperature": -1}', (("fixed", 1.0), 1)), ('{"version": 1, "mode": "bandit", "temperature": "NaN"}', (("fixed", 1.0), 1)),
        ('{"version": 1}', (("fixed", 1.0), 1)), ("[1]", (("fixed", 1.0), 1)), ("not json", (("fixed", 1.0), 1)),
    ]):
        (tmp_path / "store" / "config" / "ranking.json").write_text(text)
        assert cli._ranking(store, tmp_path / f"w{n + 1}") == expected, text


def test_a_watch_file_is_optional_and_a_broken_one_or_bad_entries_are_ignored_and_counted(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    assert cli._watch(store, tmp_path / "w0") == ([], 0, 0)
    good = {"id": "notices", "url": "https://A.test/notices#top", "every_hours": 6}
    bad = [{"id": "notices", "url": "https://a.test/again", "every_hours": 1}, {"id": "rel", "url": "/notices", "every_hours": 1},
           {"id": "zero", "url": "https://a.test/z", "every_hours": 0}, {"id": "nan", "url": "https://a.test/n", "every_hours": "nan"},
           {"id": "a b", "url": "https://a.test/s", "every_hours": 1}, {"id": 7, "url": "https://a.test/i", "every_hours": 1},
           {"url": "https://a.test/missing"}, "not an object"]
    (tmp_path / "store" / "config").mkdir(parents=True)
    for n, (text, expected) in enumerate([
        (json.dumps({"version": 1, "generated": "2026-11-12T00:00:00Z", "urls": [good, *bad]}),
         ([{"id": "notices", "url": "https://a.test/notices", "every_hours": 6.0}], 0, len(bad))),
        ('{"version": 2, "urls": []}', ([], 1, 0)), ('{"version": 1, "urls": {}}', ([], 1, 0)), ("[1]", ([], 1, 0)), ("not json", ([], 1, 0)),
    ]):
        (tmp_path / "store" / "config" / "watch.json").write_text(text)
        assert cli._watch(store, tmp_path / f"w{n + 1}") == expected, text


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
