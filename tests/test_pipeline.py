"""Checks for the engine: fetch rows, state, stores, the Jev client, the ceiling and the backlog. No live network."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pyarrow.parquet as pq
import pytest

from frontier_crawler.harvester.cascade import ScrapeResult, Target
from frontier_crawler.harvester.readers import extract_links
from frontier_crawler.harvester.throttle import HostThrottle
from frontier_crawler.judge.excerpt import build_excerpt
from frontier_crawler.judge.jev import PRICE_PER_MTOK_USD, JevClient, JevRejected, JevUnavailable
from frontier_crawler.judge import questions as judging
from frontier_crawler.judge.questions import relevance

QUESTIONS_V1 = judging.active()  # the built-in example set
from frontier_crawler.pipeline import FORECAST_KEY, Engine, _spread, block_marker
from frontier_crawler.storage.shards import FETCH_SCHEMA, ShardWriter, bodies_of, day_folder
from frontier_crawler.storage.state import BODY_HASHES_PATH
from frontier_crawler.storage.store import HubStore, LocalStore

NOW = dt.datetime(2026, 11, 9, 20, 0, tzinfo=dt.timezone.utc)
DAY = NOW.date()
DOC = "The port authority published the berth schedule and the tariff for bulk cargo. " * 10


def _job(url: str) -> Target:
    return Target(url, "Example port", "example-port", "example-port:a.test", 2, "parent-fetch")


class FakeCascade:
    """Serves canned results by URL. A missing URL raises, like a bug inside one fetch."""

    def __init__(self, pages: Dict[str, ScrapeResult]) -> None:
        self.pages = pages

    async def fetch(self, url: str) -> ScrapeResult:
        return self.pages[url]


def _page(url: str, text: str = DOC, status: int = 200, tier: str = "DOM_TRAFILATURA", body: Optional[bytes] = None) -> ScrapeResult:
    raw = body if body is not None else f"<html>{text}</html>".encode()
    links = None if tier == "API" else extract_links(raw, url)  # as the fetcher's reader finds them
    return ScrapeResult(url, status, raw, text or None, len(text.split()), tier, None, "text/html", links=links)  # type: ignore[arg-type]


def _rows(store_root: Path, table: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for path in sorted((store_root / day_folder(table, DAY)).glob("*.parquet")):
        out += pq.read_table(path).to_pylist()
    return out


def _jev_body(questions: Any = QUESTIONS_V1, noul: float = 0.9) -> Dict[str, Any]:
    answers: Dict[str, Any] = {}
    for q in questions:
        if q.type == "noul":
            answers[q.key] = {"type": "noul", "noul": 0.05 if q.id == "junk_page" else noul}
        else:
            answers[q.key] = {"type": "choice", "choice": "record", "confidence": 0.8,
                              "probabilities": {"record": 0.8, "other": 0.2}}
    return {"model": "jev-1.13.0", "answers": answers, "usage": {"input_tokens": 700, "output_tokens": 90}}


def _jev(handler: Any, **kwargs: Any) -> JevClient:
    async def no_sleep(_: float) -> None:
        return None

    return JevClient("test-key", httpx.AsyncClient(transport=httpx.MockTransport(handler)), sleep=no_sleep, **kwargs)


class _Run:
    """Start, one batch, finish."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    async def run(self, targets: Any) -> Dict[str, Any]:
        await self.engine.start()
        await self.engine.run(targets)
        return await self.engine.finish()


def _engine(tmp_path: Path, cascade: Any, run: str, store: Any = None, **kwargs: Any) -> _Run:
    store = store or LocalStore(tmp_path / "store")
    return _Run(Engine(store, cascade, workdir=tmp_path / f"work-{run}", now=lambda: NOW, run_id=run, **kwargs))


# ---------------------------------------------------------------- fetch and store

def test_fetch_row_carries_provenance_and_its_bytes_go_to_bodies(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x?session=1")})
    stats = asyncio.run(_engine(tmp_path, cascade, "r1").run([_job("https://a.test/x")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    (body,) = _rows(tmp_path / "store", "bodies")
    assert stats["stored"] == 1 and stats["pending"] == 1 and stats["judged"] == 0
    assert row["url_requested"] == "https://a.test/x"
    assert row["url_final"] == "https://a.test/x?session=1"
    assert row["fetched_at"] == NOW and row["http_status"] == 200 and row["tier_used"] == "DOM_TRAFILATURA"
    assert row["body"] is None and body["body"].startswith(b"<html>") and row["size_bytes"] == body["size_bytes"] == len(body["body"])
    assert (body["sha256"], body["first_fetch_id"], body["stored_at"]) == (row["sha256"], row["fetch_id"], NOW)
    assert (body["content_type"], body["tier_used"]) == ("text/html", "DOM_TRAFILATURA")
    assert stats["stored_bytes"] == stats["bodies_written_bytes"] == len(body["body"]) and stats["stored_by_tier"] == {"DOM_TRAFILATURA": 1}
    assert stats["bodies_written"] == 1 and stats["bodies_already_stored"] == 0
    assert len(row["sha256"]) == 64 and row["text"] == DOC and row["outcome"] == "stored"
    assert row["entity"] == "Example port" and row["run_id"] == "r1"
    assert (row["seed_id"], row["line_id"], row["hops"], row["parent_fetch_id"]) == ("example-port", "example-port:a.test", 2, "parent-fetch")


def test_a_fetch_row_carries_the_pages_title(tmp_path: Path) -> None:
    page = dataclasses.replace(_page("https://a.test/x"), title="Berth schedule")
    asyncio.run(_engine(tmp_path, FakeCascade({"https://a.test/x": page, "https://a.test/y": _page("https://a.test/y")}), "r1").run(
        [_job("https://a.test/x"), _job("https://a.test/y")]))
    assert {row["url_final"]: row["title"] for row in _rows(tmp_path / "store", "fetches")} == {"https://a.test/x": "Berth schedule", "https://a.test/y": None}


def test_second_run_marks_unchanged_and_keeps_no_second_copy(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x")})
    asyncio.run(_engine(tmp_path, cascade, "r1").run([_job("https://a.test/x")]))
    stats = asyncio.run(_engine(tmp_path, cascade, "r2").run([_job("https://a.test/x")]))
    second = [row for row in _rows(tmp_path / "store", "fetches") if row["run_id"] == "r2"]
    assert stats["unchanged"] == 1 and stats["stored"] == 0
    assert second[0]["outcome"] == "unchanged" and second[0]["body"] is None and second[0]["sha256"]


def test_same_bytes_at_two_urls_is_a_duplicate(tmp_path: Path) -> None:
    cascade = FakeCascade({
        "https://a.test/x": _page("https://a.test/x", body=b"same"),
        "https://a.test/y": _page("https://a.test/y", body=b"same"),
    })
    engine = _engine(tmp_path, cascade, "r1", concurrency=1)
    stats = asyncio.run(engine.run([_job("https://a.test/x"), _job("https://a.test/y")]))
    assert stats["stored"] == 1 and stats["duplicate"] == 1


def _bodies(store_root: Path) -> List[Dict[str, Any]]:
    return [row for path in sorted((store_root / "bodies").rglob("*.parquet")) for row in pq.read_table(path).to_pylist()]


def _day_engine(tmp_path: Path, pages: Dict[str, ScrapeResult], run: str, day: int = 0) -> Engine:
    when = NOW + dt.timedelta(days=day)
    return Engine(LocalStore(tmp_path / "store"), FakeCascade(pages), workdir=tmp_path / f"work-{run}", now=lambda: when, run_id=run, concurrency=1)


def _stored_body_counts_add_up(stats: Dict[str, Any]) -> None:
    assert stats["bodies_written"] + stats["bodies_already_stored"] == stats["stored"]
    assert stats["bodies_written_bytes"] + stats["bodies_already_stored_bytes"] == stats["stored_bytes"]


def test_the_same_bytes_are_stored_once_in_a_run_and_across_runs(tmp_path: Path) -> None:
    same = {url: _page(url, body=b"<html>same bytes</html>") for url in ("https://a.test/x", "https://a.test/y")}
    first = asyncio.run(_Run(_day_engine(tmp_path, same, "r1")).run([_job(url) for url in same]))
    assert (first["stored"], first["duplicate"], first["bodies_written"]) == (1, 1, 1)
    later = {"https://b.test/z": _page("https://b.test/z", body=b"<html>same bytes</html>"), "https://b.test/new": _page("https://b.test/new", body=b"<html>new</html>")}
    second = asyncio.run(_Run(_day_engine(tmp_path, later, "r2", day=1)).run([_job(url) for url in later]))
    # A new URL serving bytes an earlier run stored: stored for its URL, with its text, and no second body.
    assert (second["stored"], second["bodies_written"], second["bodies_already_stored"]) == (2, 1, 1)
    assert second["bodies_already_stored_bytes"] == len(b"<html>same bytes</html>")
    rows = [row for day in (DAY, DAY + dt.timedelta(days=1)) for path in (tmp_path / "store" / day_folder("fetches", day)).glob("*.parquet")
            for row in pq.read_table(path).to_pylist()]
    assert len(rows) == 4 and all(row["body"] is None for row in rows)
    assert sorted(row["sha256"] for row in _bodies(tmp_path / "store")) == sorted({row["sha256"] for row in rows})
    for stats in (first, second):
        _stored_body_counts_add_up(stats)


def test_a_run_killed_before_its_final_commit_leaves_the_hash_set_alone_and_the_next_run_stores_again(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"<html>doc {i}</html>".encode()) for i in range(2)}
    killed = _day_engine(tmp_path, pages, "killed")

    async def die() -> None:
        await killed.start()
        await killed.fetch(_job("https://a.test/0"))
        await killed.flush(force=True)            # the periodic upload: the first body goes up
        await killed.fetch(_job("https://a.test/1"))   # buffered, then the run dies

    asyncio.run(die())
    assert len(_bodies(tmp_path / "store")) == 1 and not (tmp_path / "store" / BODY_HASHES_PATH).exists()
    stats = asyncio.run(_Run(_day_engine(tmp_path, pages, "next")).run([_job(url) for url in pages]))
    # Both are stored again: the uploaded one twice (harmless), the lost one now.
    assert stats["bodies_written"] == 2 and len(_bodies(tmp_path / "store")) == 3
    again = asyncio.run(_Run(_day_engine(tmp_path, {"https://c.test/": _page("https://c.test/", body=b"<html>doc 1</html>")}, "again")).run([_job("https://c.test/")]))
    assert (again["bodies_written"], again["bodies_already_stored"]) == (0, 1)


def test_fetch_rows_never_go_up_without_their_bodies(tmp_path: Path) -> None:
    pages = {"https://a.test/x": _page("https://a.test/x")}
    engine = _day_engine(tmp_path, pages, "r1")

    async def go() -> None:
        await engine.start()
        await engine.fetch(_job("https://a.test/x"))
        engine._writer("fetches", DAY)._max_bytes = 1   # the fetch rows' shard is full; the bodies' is not
        await engine.flush()

    asyncio.run(go())
    assert len(_rows(tmp_path / "store", "fetches")) == len(_bodies(tmp_path / "store")) == 1


def _old_format_shard(tmp_path: Path, rows: List[Dict[str, Any]], day: dt.date) -> None:
    """Fetch rows as engine 0.2.0 wrote them, with the bytes inline and no title column."""
    writer = ShardWriter("fetches", FETCH_SCHEMA.remove(FETCH_SCHEMA.get_field_index("title")), "old", day, tmp_path / "work-old")
    for row in rows:
        writer.add(row)
    LocalStore(tmp_path / "store").put_many([writer.write(writer.take(force=True))])


def test_the_body_reader_finds_inline_bodies_and_bodies_stored_on_earlier_days(tmp_path: Path) -> None:
    earlier, today = DAY - dt.timedelta(days=3), DAY
    _old_format_shard(tmp_path, [{"fetch_id": "old-1", "outcome": "stored", "sha256": "ab" * 32, "body": b"inline bytes"}], earlier)
    asyncio.run(_Run(_day_engine(tmp_path, {"https://a.test/x": _page("https://a.test/x", body=b"<html>kept</html>")}, "r1", day=-2)).run([_job("https://a.test/x")]))
    pages = {"https://b.test/x": _page("https://b.test/x", body=b"<html>kept</html>"), "https://b.test/y": _page("https://b.test/y", body=b"<html>today</html>")}
    asyncio.run(_Run(_day_engine(tmp_path, pages, "r2")).run([_job(url) for url in pages]))
    old = pq.read_table(tmp_path / "store" / day_folder("fetches", earlier) / "old-000.parquet").to_pylist()
    new = _rows(tmp_path / "store", "fetches")
    found = {row["fetch_id"]: body for row, body in bodies_of(LocalStore(tmp_path / "store"), tmp_path / "read", old + new, today)}
    by_url = {row["url_final"]: found[row["fetch_id"]] for row in new}
    assert found["old-1"] == b"inline bytes"
    assert by_url == {"https://b.test/x": b"<html>kept</html>", "https://b.test/y": b"<html>today</html>"}
    assert list(bodies_of(LocalStore(tmp_path / "store"), tmp_path / "read", [{"sha256": "cd" * 32}])) == [({"sha256": "cd" * 32}, None)]


def test_the_body_command_writes_one_stored_body(tmp_path: Path, monkeypatch: Any) -> None:
    from frontier_crawler import __main__ as cli

    asyncio.run(_engine(tmp_path, FakeCascade({"https://a.test/x": _page("https://a.test/x", body=b"<html>wanted</html>")}), "r1").run([_job("https://a.test/x")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    monkeypatch.setattr(cli, "open_store", lambda: LocalStore(tmp_path / "store"))
    cli.main(["body", row["sha256"].upper(), "--out", str(tmp_path / "out.html")])
    assert (tmp_path / "out.html").read_bytes() == b"<html>wanted</html>"
    with pytest.raises(SystemExit, match="no stored body"):
        cli.main(["body", "0" * 64])
    with pytest.raises(SystemExit, match="64 hex"):
        cli.main(["body", "../../etc"])


def test_one_failed_job_does_not_stop_the_rest(tmp_path: Path) -> None:
    cascade = FakeCascade({
        "https://a.test/ok": _page("https://a.test/ok"),
        "https://a.test/empty": ScrapeResult("https://a.test/empty", 0, b"", None, 0, "HEADLESS_PLAYWRIGHT", "timed out"),
    })
    jobs = [_job("https://a.test/missing"), _job("https://a.test/empty"), _job("https://a.test/ok")]
    stats = asyncio.run(_engine(tmp_path, cascade, "r1").run(jobs))
    rows = {row["url_requested"]: row for row in _rows(tmp_path / "store", "fetches")}
    assert stats["errors"] == 2 and stats["stored"] == 1 and len(rows) == 3
    assert "KeyError" in rows["https://a.test/missing"]["error"]
    assert rows["https://a.test/empty"]["error"] == "timed out"


def test_blocked_page_is_stored_but_not_judged(tmp_path: Path) -> None:
    calls: List[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json=_jev_body())

    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x", text=DOC * 2, status=403)})
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", jev=_jev(handler), monthly_usd=8).run([_job("https://a.test/x")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    assert row["outcome"] == "stored" and row["block_marker"] == "http 403" and stats["bodies_written"] == 1
    assert stats["blocked"] == 1 and stats["judged"] == 0 and not calls


def test_error_page_is_recorded_without_its_bytes(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/gone": _page("https://a.test/gone", text="Page not found", status=404)})
    stats = asyncio.run(_engine(tmp_path, cascade, "r1").run([_job("https://a.test/gone")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    assert stats["error_pages"] == 1 and stats["stored"] == 0 and stats["errors"] == 0
    assert row["outcome"] == "error_page" and row["http_status"] == 404 and row["block_marker"] == "http 404"
    assert row["body"] is None and row["text"] is None and row["sha256"] and row["size_bytes"] > 0
    assert stats["bodies_written"] == 0 and _rows(tmp_path / "store", "bodies") == []


def test_block_marker_only_reads_short_pages() -> None:
    assert block_marker(200, "Access denied") == "access denied"
    assert block_marker(200, "the minister said access denied to the port " + "word " * 500) is None
    assert block_marker(404, "anything") == "http 404"


def test_throttle_spaces_requests_per_host() -> None:
    clock = [0.0]
    slept: List[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock[0] += seconds

    async def run() -> None:
        throttle = HostThrottle(2.0, clock=lambda: clock[0], sleep=sleep)
        await throttle.wait("https://a.test/1")
        await throttle.wait("https://b.test/1")
        await throttle.wait("https://a.test/2")

    asyncio.run(run())
    assert slept == [2.0]


# ---------------------------------------------------------------- stores

def test_local_store_lists_and_rejects_escapes(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "s")
    source = tmp_path / "f.txt"
    source.write_text("x", encoding="utf-8")
    store.put_many([(source, "a/b/f.txt")], "m")
    assert store.list("a") == ["a/b/f.txt"] and store.list("nothing") == []
    assert not store.get("a/b/missing.txt", tmp_path / "out.txt")
    with pytest.raises(ValueError):
        store.put_many([(source, "../f.txt")], "m")


def test_hub_store_sends_one_commit_for_many_files(tmp_path: Path) -> None:
    class FakeApi:
        def __init__(self) -> None:
            self.commits: List[Dict[str, Any]] = []

        def create_commit(self, **kwargs: Any) -> None:
            self.commits.append(kwargs)

    api = FakeApi()
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_text("1", encoding="utf-8")
    b.write_text("2", encoding="utf-8")
    HubStore("owner/repo", "token", api=api).put_many([(a, "x/a"), (b, "x/b")], "engine r1")
    (commit,) = api.commits
    assert commit["repo_id"] == "owner/repo" and commit["repo_type"] == "dataset"
    assert [op.path_in_repo for op in commit["operations"]] == ["x/a", "x/b"]


# ---------------------------------------------------------------- Jev

def test_jev_request_and_reply_follow_the_documented_shape() -> None:
    sent: Dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent["auth"] = request.headers["authorization"]
        sent["body"] = json.loads(request.content)
        return httpx.Response(200, json=_jev_body())

    result = asyncio.run(_jev(handler).ask("some text", QUESTIONS_V1))
    assert sent["auth"] == "Bearer test-key"
    assert set(sent["body"]) == {"model", "state", "questions"} and sent["body"]["state"] == "some text"
    assert sent["body"]["questions"]["has_specifics_v1"]["type"] == "noul"
    assert set(sent["body"]["questions"]["has_specifics_v1"]["criteria"]) == {"true", "false"}
    assert result.model == "jev-1.13.0" and result.input_tokens == 700
    assert result.answers["has_specifics"].value_number == 0.9
    assert result.answers["doc_type"].value_text == "record"
    assert result.answers["doc_type"].confidence == 0.8


def test_jev_retries_rate_limits_then_answers() -> None:
    statuses = [429, 529, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        return httpx.Response(status, json=_jev_body() if status == 200 else {"error": "busy"})

    result = asyncio.run(_jev(handler).ask("text", QUESTIONS_V1))
    assert result.input_tokens == 700 and not statuses


def test_jev_outage_and_refusal_are_different_errors() -> None:
    attempts: List[int] = []

    def down(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(503)

    with pytest.raises(JevUnavailable):
        asyncio.run(_jev(down, retries=2).ask("text", QUESTIONS_V1))
    assert len(attempts) == 3

    refused: List[int] = []

    def unauthorised(request: httpx.Request) -> httpx.Response:
        refused.append(1)
        return httpx.Response(401, json={"error": "bad key test-key"})

    with pytest.raises(JevRejected) as caught:
        asyncio.run(_jev(unauthorised).ask("text", QUESTIONS_V1))
    assert len(refused) == 1 and "test-key" not in str(caught.value)


def test_jev_rejects_a_boolean_noul() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = _jev_body()
        body["answers"]["junk_page_v1"] = {"type": "noul", "noul": True}
        return httpx.Response(200, json=body)

    with pytest.raises(JevRejected):
        asyncio.run(_jev(handler).ask("text", QUESTIONS_V1))


def test_relevance_and_threshold() -> None:
    assert relevance({"has_specifics": 0.5, "junk_page": 0.1}) == pytest.approx(0.5 * 0.9)
    assert relevance({"has_specifics": 0.8}) is None
    assert judging.keep_threshold() == pytest.approx(1 / 11)


def test_questions_come_from_configuration() -> None:
    config = {"questions": [{"id": q, "version": 2, "type": "noul", "instructions": q} for q in ("a", "b", "c", "d")],
              "relevance": {"version": "r2", "all": ["a"], "any": ["b", "c"], "none": ["d"], "miss_to_junk_cost": 4}}
    try:
        judging.configure(config)
        assert [q.key for q in judging.active()] == ["a_v2", "b_v2", "c_v2", "d_v2"] and judging.relevance_version() == "r2"
        assert relevance({"a": 0.8, "b": 0.2, "c": 0.5, "d": 0.1}) == pytest.approx(0.8 * 0.5 * 0.9)
        assert judging.keep_threshold() == pytest.approx(1 / 5)
        with pytest.raises(ValueError):
            judging.configure({**config, "relevance": {**config["relevance"], "all": ["missing"]}})
    finally:
        judging.configure(judging.EXAMPLE)


def test_excerpt_is_full_when_short_and_sampled_across_a_long_text() -> None:
    assert build_excerpt("a b c", 10) == ("a b c", "full", 3)
    body = " ".join(f"w{i}" for i in range(10_000))
    text = "# Title\n" + body[: len(body) // 2] + "\n## Annex B tariffs\n" + body[len(body) // 2 :]
    excerpt, method, words = build_excerpt(text, 1000)
    assert method == "sampled" and words <= 1010
    assert excerpt.startswith("# Title w0 w1 ") and excerpt.endswith("w9999")
    assert "## Annex B tariffs" in excerpt and excerpt.count("# Title") == 1
    middle = [int(word[1:]) for word in excerpt.split() if word[1:].isdigit() and 1000 < int(word[1:]) < 9000]
    assert min(middle) < 3500 and max(middle) > 6500


# ---------------------------------------------------------------- judging in the pipeline

def test_new_document_is_judged_and_every_answer_is_stored(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x")})
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", jev=jev, monthly_usd=8).run([_job("https://a.test/x")]))
    (fetch,) = _rows(tmp_path / "store", "fetches")
    (call,) = _rows(tmp_path / "store", "judgement_calls")
    answers = _rows(tmp_path / "store", "judgements")
    assert stats["judged"] == 1 and stats["pending"] == 0 and stats["jev_input_tokens"] == 700
    assert call["fetch_id"] == fetch["fetch_id"] and call["status"] == "ok" and call["jev_model"] == "jev-1.13.0"
    assert call["relevance"] == pytest.approx(0.9 * 0.95) and call["excerpt_method"] == "full"
    assert {a["question_id"] for a in answers} == {q.id for q in QUESTIONS_V1}
    assert all(a["question_version"] == 1 and a["call_id"] == call["call_id"] for a in answers)
    spend = json.loads((tmp_path / "store" / "state" / "jev_spend.json").read_text(encoding="utf-8"))
    assert spend == {"2026-11": {"calls": 1, "input_tokens": 700}}


def test_api_json_is_not_sent_to_jev(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/api": _page("https://a.test/api", tier="API", body=b'{"rate": 12.0}')})
    jev = _jev(lambda request: pytest.fail("Jev was called for structured data"))
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", jev=jev, monthly_usd=8).run([_job("https://a.test/api")]))
    assert stats["stored"] == 1 and stats["judged"] == 0 and stats["pending"] == 0


def test_ceiling_leaves_documents_pending_and_the_backlog_picks_them_up(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(3)}
    jobs = [_job(url) for url in pages]
    answering = lambda request: httpx.Response(200, json=_jev_body())  # noqa: E731

    # No money: nothing is asked, everything is stored.
    stats = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", jev=_jev(answering), monthly_usd=0).run(jobs))
    assert stats["stored"] == 3 and stats["judged"] == 0 and stats["pending"] == 3

    def backlog(run: str) -> Dict[str, Any]:
        engine = Engine(LocalStore(tmp_path / "store"), workdir=tmp_path / f"work-{run}", jev=_jev(answering), monthly_usd=8, now=lambda: NOW, run_id=run)

        async def go() -> Dict[str, Any]:
            await engine.start()
            await engine.judge_days([DAY])
            return await engine.finish()

        return asyncio.run(go())

    assert backlog("j1")["judged"] == 3 and len(_rows(tmp_path / "store", "judgement_calls")) == 3
    assert backlog("j2")["judged"] == 0          # nothing is asked twice
    # A shard written before the title column is read alongside the new ones.
    _old_format_shard(tmp_path, [{"fetch_id": "old-1", "outcome": "stored", "tier_used": "DOM_TRAFILATURA", "sha256": "ab" * 32,
                                  "text": DOC, "word_count": len(DOC.split())}], DAY)
    assert backlog("j3")["judged"] == 1


def test_the_ceiling_stops_calls_once_the_month_has_spent_it(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(4)}
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    # 3,333 tokens a month at the list price, and NOW is 8 days 20 hours into a 30-day month, so 981 may be spent by now.
    # Each call reports 700, so the second call passes the ceiling.
    stats = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", jev=jev, monthly_usd=0.00014, concurrency=1).run([_job(url) for url in pages]))
    assert stats["stored"] == 4 and stats["judged"] == 2 and stats["pending"] == 2


def test_jev_outage_keeps_the_document_and_marks_it_for_later(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x")})
    jev = _jev(lambda request: httpx.Response(503), retries=0)
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", jev=jev, monthly_usd=8).run([_job("https://a.test/x")]))
    (call,) = _rows(tmp_path / "store", "judgement_calls")
    assert stats["stored"] == 1 and stats["pending"] == 1 and call["status"] == "unavailable"
    assert _rows(tmp_path / "store", "judgements") == []


def test_repeated_refusals_stop_judging_for_the_run(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(6)}
    hits: List[int] = []

    def unauthorised(request: httpx.Request) -> httpx.Response:
        hits.append(1)
        return httpx.Response(401)

    engine = _engine(tmp_path, FakeCascade(pages), "r1", jev=_jev(unauthorised), monthly_usd=8, concurrency=1)
    stats = asyncio.run(engine.run([_job(url) for url in pages]))
    assert stats["stored"] == 6 and len(hits) == 3 and "refused 3 calls" in stats["jev_stopped"]


def test_a_run_uploads_its_tables_and_state_in_one_commit(tmp_path: Path) -> None:
    class CountingStore(LocalStore):
        commits: List[List[str]] = []

        def put_many(self, files: Any, message: str = "") -> None:
            self.commits.append(sorted(remote for _, remote in files))
            super().put_many(files, message)

    store = CountingStore(tmp_path / "store")
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x")})
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", store=store, jev=jev, monthly_usd=8).run([_job("https://a.test/x")]))
    (commit,) = store.commits
    assert [path.split("/")[0] for path in commit] == ["bodies", "fetches", "judgement_calls", "judgements", "state", "state", "state"]
    assert "state/body_hashes.parquet" in commit and len(stats["shards"]) == 4


def test_a_file_over_the_size_cap_is_a_row_without_bytes(tmp_path: Path) -> None:
    big = ScrapeResult("https://a.test/dump.csv", 200, b"", None, 0, "DOM_TRAFILATURA", "too large", "text/csv", too_large_bytes=900_000_000)
    stats = asyncio.run(_engine(tmp_path, FakeCascade({"https://a.test/dump.csv": big}), "r1").run([_job("https://a.test/dump.csv")]))
    (row,) = _rows(tmp_path / "store", "fetches")
    assert stats["too_large"] == 1 and stats["errors"] == 0
    assert row["outcome"] == "too_large" and row["size_bytes"] == 900_000_000 and row["content_type"] == "text/csv" and row["body"] is None


def test_a_fault_after_a_row_is_stored_does_not_stop_the_batch(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(3)}
    engine = Engine(LocalStore(tmp_path / "store"), FakeCascade(pages), workdir=tmp_path / "work", now=lambda: NOW, run_id="r1")

    def faulty(target: Target, row: Dict[str, Any], answers: Any) -> None:
        raise ValueError("Invalid IPv6 URL")

    async def go() -> Dict[str, Any]:
        await engine.start()
        await engine.run([_job(url) for url in pages], faulty)
        return await engine.finish()

    assert asyncio.run(go())["stored"] == 3 and len(_rows(tmp_path / "store", "fetches")) == 3


class InProcessReaders:
    """Runs a reader in this process, so a test can replace ocr.read_scan with a closure. test_readers covers the processes."""

    def __init__(self, counts: Dict[str, Any], workers: int = 1) -> None:
        pass

    async def run(self, limit: float, fn: Any, *args: Any) -> Any:
        return fn(*args)

    def close(self) -> None:
        pass


def test_scanned_pdfs_are_read_once_and_then_judged(tmp_path: Path, monkeypatch: Any) -> None:
    from frontier_crawler import ocr
    from frontier_crawler import pipeline

    monkeypatch.setattr(pipeline, "ReaderPool", InProcessReaders)

    scan = ScrapeResult("https://a.test/scan.pdf", 200, b"%PDF scan", None, 0, "PDF", None, "application/pdf")
    texty = ScrapeResult("https://a.test/text.pdf", 200, b"%PDF text", DOC, len(DOC.split()), "PDF", None, "application/pdf")
    pages = {"https://a.test/scan.pdf": scan, "https://a.test/text.pdf": texty}
    asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1").run([_job(url) for url in pages]))

    read: List[bytes] = []

    def fake_read(body: bytes) -> Any:
        read.append(body)
        return DOC, 4, 9, "ara+eng"

    monkeypatch.setattr(ocr, "read_scan", fake_read)

    def later(run: str, method: str, *limit: float) -> Dict[str, Any]:
        jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
        engine = Engine(LocalStore(tmp_path / "store"), workdir=tmp_path / f"work-{run}", jev=jev, monthly_usd=8, now=lambda: NOW, run_id=run)

        async def go() -> Dict[str, Any]:
            await engine.start()
            await getattr(engine, method)([DAY], *limit)
            return await engine.finish()

        return asyncio.run(go())

    out_of_time = later("o0", "ocr_days", 0)
    assert read == [] and out_of_time["scans_read"] == 0 and out_of_time["stopped_by"] == "max_minutes"
    first = later("o1", "ocr_days")
    (row,) = _rows(tmp_path / "store", "ocr_text")
    assert read == [b"%PDF scan"] and first["scans_read"] == 1 and first["judged"] == 1    # the PDF that had text is left alone
    assert (row["langs"], row["pages_read"], row["pages_total"], row["text"]) == ("ara+eng", 4, 9, DOC)
    assert later("o2", "ocr_days")["scans_read"] == 0                                      # not read twice
    assert later("j1", "judge_days")["judged"] == 1                                        # only the text PDF was still unjudged
    assert later("j2", "judge_days")["judged"] == 0


def test_scans_are_read_from_inline_bodies_and_from_bodies_stored_on_an_earlier_day(tmp_path: Path, monkeypatch: Any) -> None:
    from frontier_crawler import ocr
    from frontier_crawler import pipeline

    monkeypatch.setattr(pipeline, "ReaderPool", InProcessReaders)

    _old_format_shard(tmp_path, [{"fetch_id": "old-scan", "outcome": "stored", "tier_used": "PDF", "word_count": 0,
                                  "sha256": "ab" * 32, "body": b"%PDF old"}], DAY)
    scan = lambda url: ScrapeResult(url, 200, b"%PDF moved", None, 0, "PDF", None, "application/pdf")  # noqa: E731
    asyncio.run(_Run(_day_engine(tmp_path, {"https://a.test/s.pdf": scan("https://a.test/s.pdf")}, "r0", day=-1)).run([_job("https://a.test/s.pdf")]))
    asyncio.run(_Run(_day_engine(tmp_path, {"https://b.test/s.pdf": scan("https://b.test/s.pdf")}, "r1")).run([_job("https://b.test/s.pdf")]))
    read: List[bytes] = []
    monkeypatch.setattr(ocr, "read_scan", lambda body: read.append(body) or (DOC, 1, 1, "eng"))
    engine = Engine(LocalStore(tmp_path / "store"), workdir=tmp_path / "work-ocr", now=lambda: NOW, run_id="o1")

    async def go() -> Dict[str, Any]:
        await engine.start()
        await engine.ocr_days([DAY])
        return await engine.finish()

    assert asyncio.run(go())["scans_read"] == 2 and sorted(read) == [b"%PDF moved", b"%PDF old"]


# ---------------------------------------------------------------- measurements

def test_a_run_records_where_its_time_and_bytes_went(tmp_path: Path) -> None:
    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x"), "https://a.test/api": _page("https://a.test/api", tier="API", body=b'{"a": 1}')})
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    stats = asyncio.run(_engine(tmp_path, cascade, "r1", jev=jev, monthly_usd=8).run(
        [_job("https://a.test/x"), _job("https://a.test/api"), _job("https://a.test/missing")]))
    slot = stats["slot_seconds"]
    assert set(slot) >= {"observe", "waiting"} and all(isinstance(v, float) and v >= 0 for v in slot.values())
    assert stats["flush"]["uploads"] == 1 and stats["flush"]["seconds_total"] >= 0
    tiers = stats["fetch_seconds_by_tier"]
    assert set(tiers) == {"DOM_TRAFILATURA", "API", "error"} and tiers["error"]["n"] == 1
    assert stats["bytes_by_tier"]["DOM_TRAFILATURA"] == {"body": len(f"<html>{DOC}</html>"), "text": len(DOC.encode())}
    assert stats["shard_bytes"]["fetches"] > 0 and stats["shard_bytes"]["judgements"] > 0
    assert stats["state_bytes"]["url_hashes"] > 0 and stats["state_bytes"]["jev_spend"] > 0
    assert stats["jev"]["calls"] == 1 and stats["jev"]["ceiling_usd_today"] > 0 and stats["jev"]["seconds_mean"] >= 0
    assert stats["timeouts"] == {"connect": 0, "read": 0, "browser": 0} and stats["browser_launches"] == 0
    assert stats["peak_rss_mb"] > 0
    json.dumps(stats)   # the run record is JSON


def test_spread_reads_the_90th_percentile_by_nearest_rank() -> None:
    assert _spread([float(i) for i in range(10, 0, -1)]) == {"n": 10, "mean": 5.5, "p50": 6.0, "p90": 9.0, "max": 10.0}


def test_fetch_seconds_leave_out_the_wait_for_the_hosts_turn(tmp_path: Path) -> None:
    class Polite:
        async def fetch(self, url: str) -> ScrapeResult:
            await asyncio.sleep(0.05)   # all of it spent waiting for the host's turn
            return dataclasses.replace(_page(url), throttle_wait=0.05)

    stats = asyncio.run(_engine(tmp_path, Polite(), "r1").run([_job("https://a.test/x")]))
    assert stats["fetch_seconds_by_tier"]["DOM_TRAFILATURA"]["max"] < 0.03


def test_a_fault_in_measuring_does_not_break_a_fetch(tmp_path: Path) -> None:
    class Broken(dict):
        def __setitem__(self, key: Any, value: Any) -> None:
            raise RuntimeError("timer fault")

        def setdefault(self, *args: Any) -> Any:
            raise RuntimeError("timer fault")

    cascade = FakeCascade({"https://a.test/x": _page("https://a.test/x")})
    run = _engine(tmp_path, cascade, "r1")
    run.engine._slot = Broken()
    run.engine._fetch_seconds = Broken()
    stats = asyncio.run(run.run([_job("https://a.test/x")]))
    assert stats["stored"] == 1 and len(_rows(tmp_path / "store", "fetches")) == 1


# ---------------------------------------------------------------- spreading the budget over a run

def _write_spend(store_root: Path, spend: Dict[str, Any]) -> None:
    (store_root / "state").mkdir(parents=True, exist_ok=True)
    (store_root / "state" / "jev_spend.json").write_text(json.dumps(spend), encoding="utf-8")


def _usd_for(tokens_per_month: float) -> float:
    return tokens_per_month * PRICE_PER_MTOK_USD / 1_000_000


def test_the_ceiling_accrues_through_the_month_not_by_whole_days(tmp_path: Path) -> None:
    def ceiling(when: dt.datetime) -> int:
        return Engine(LocalStore(tmp_path / "store"), workdir=tmp_path / "w", monthly_usd=_usd_for(720_000), now=lambda: when)._ceiling_tokens

    # 720,000 tokens over a 30-day month is 1,000 an hour. NOW is 212 hours in.
    assert abs(ceiling(NOW) - 212_000) <= 1
    assert abs(ceiling(NOW.replace(hour=0)) - 192_000) <= 1
    assert abs(ceiling(NOW.replace(hour=6)) - 198_000) <= 1


def test_every_document_is_judged_with_no_forecast_or_with_budget_to_spare(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(3)}
    jev = lambda: _jev(lambda request: httpx.Response(200, json=_jev_body()))  # noqa: E731
    # A forecast is on file but no call in any month: nothing to price it with.
    _write_spend(tmp_path / "store", {FORECAST_KEY: 1000})
    first = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", jev=jev(), monthly_usd=8).run([_job(url) for url in pages]))
    assert first["jev_sample_rate"] == 1.0 and first["judged"] == 3 and first["not_sampled"] == 0
    # 1,000 pages at 700 tokens each is far under what $8 a month allows by now.
    pages = {f"https://a.test/n{i}": _page(f"https://a.test/n{i}", body=f"new {i}".encode()) for i in range(3)}
    second = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r2", jev=jev(), monthly_usd=8).run([_job(url) for url in pages]))
    assert second["jev_sample_rate"] == 1.0 and second["judged"] == 3 and second["not_sampled"] == 0


def test_a_run_the_budget_cannot_cover_judges_an_even_sample_and_leaves_none_pending(tmp_path: Path) -> None:
    # One call of 700 tokens so far; the last crawl run saw 1,000 judgeable pages, so a run needs 700,000.
    # The ceiling by NOW leaves a quarter of that: 175,700 tokens less the 700 spent.
    _write_spend(tmp_path / "store", {"2026-11": {"input_tokens": 700, "calls": 1}, FORECAST_KEY: 1000})
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(200)}
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    engine = _engine(tmp_path, FakeCascade(pages), "r1", jev=jev, monthly_usd=_usd_for(175_700 * 720 / 212))
    stats = asyncio.run(engine.run([_job(url) for url in pages]))
    assert stats["jev_sample_rate"] == pytest.approx(0.25, abs=1e-4)
    rate = engine.engine._sample_rate
    picked = [row for row in _rows(tmp_path / "store", "fetches") if int(row["sha256"][:8], 16) / 2**32 < rate]
    assert stats["judged"] == len(picked) and 30 <= len(picked) <= 70
    assert stats["not_sampled"] == 200 - len(picked) and stats["pending"] == 0
    assert {call["fetch_id"] for call in _rows(tmp_path / "store", "judgement_calls")} == {row["fetch_id"] for row in picked}


def test_the_first_run_of_a_month_is_priced_by_the_last_month_with_calls(tmp_path: Path) -> None:
    # As above, but the one call was last month: the sample rate is still a quarter, not everything.
    _write_spend(tmp_path / "store", {"2026-09": {"input_tokens": 9000, "calls": 1}, "2026-10": {"input_tokens": 700, "calls": 1}, FORECAST_KEY: 1000})
    engine = _engine(tmp_path, FakeCascade({}), "r1", monthly_usd=_usd_for((175_700 - 700) * 720 / 212))
    asyncio.run(engine.engine.start())
    assert engine.engine.stats["jev_sample_rate"] == pytest.approx(0.25, abs=1e-4)


def test_the_forecast_is_kept_from_the_last_sizeable_crawl_run(tmp_path: Path) -> None:
    def spend() -> Dict[str, Any]:
        return json.loads((tmp_path / "store" / "state" / "jev_spend.json").read_text(encoding="utf-8"))

    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(500)}
    asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1").run([_job(url) for url in pages]))
    assert spend()[FORECAST_KEY] == 500 and spend()["2026-11"] == {"input_tokens": 0, "calls": 0}
    few = {f"https://a.test/f{i}": _page(f"https://a.test/f{i}", body=f"few {i}".encode()) for i in range(3)}
    asyncio.run(_engine(tmp_path, FakeCascade(few), "r2").run([_job(url) for url in few]))
    assert spend()[FORECAST_KEY] == 500   # a short run does not replace it
