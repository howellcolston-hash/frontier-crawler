"""The judge encoder: its arithmetic, loading and falling back, and the engine and crawl using it where Jev does not judge.
No model file: the encoder is a stub, or the real class around a fake session and tokenizer."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

import httpx
import numpy as np
import pytest

from frontier_crawler import frontier as frontier_module
from frontier_crawler.__main__ import open_encoder
from frontier_crawler.frontier import Seed, crawl
from frontier_crawler.judge import questions as judging
from frontier_crawler.judge import encoder as encoder_module
from frontier_crawler.judge.encoder import Encoder, spearman, windows
from frontier_crawler.judge.jev import Answer, JevClient, JevResult
from frontier_crawler.pipeline import ENCODER_AUDIT_SHARE, Engine
from frontier_crawler.storage.store import LocalStore
from tests.test_crawl_stream import Site
from tests.test_pipeline import NOW, FakeCascade, _engine, _jev_body, _job, _page, _rows

NOULS = [q for q in judging.active() if q.type == "noul"]
CHOICE = next(q for q in judging.active() if q.type == "choice")


class StubEncoder:
    """Answers every yes/no question with p; raises when told to."""

    stamp = "20261123T074638Z"

    def __init__(self, p: float = 0.6, fail: bool = False) -> None:
        self.p, self.fail, self.calls = p, fail, 0

    def ask(self, excerpt: str) -> JevResult:
        self.calls += 1
        if self.fail:
            raise RuntimeError("onnx went wrong")
        answers = {q.id: Answer(q, 0.05 if q.id == "junk_page" else self.p, None, None, None) for q in NOULS}
        answers[CHOICE.id] = Answer(CHOICE, None, "record", {"record": 0.7, "news_or_commentary": 0.2, "other": 0.1}, 0.7)
        return JevResult(answers, f"encoder-{self.stamp}", 0)


def _jev(handler: Any) -> JevClient:
    async def no_sleep(_: float) -> None:
        return None

    return JevClient("test-key", httpx.AsyncClient(transport=httpx.MockTransport(handler)), sleep=no_sleep)


def _in(sha: str, rate: float) -> bool:
    return int(sha[:8], 16) / 2**32 < rate


def test_the_encoder_scores_what_jev_does_not_judge_and_an_audit_slice_of_what_it_does(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(400)}
    stub = StubEncoder()
    try:
        judging.configure({**judging.EXAMPLE, "jev_sample": 0.25})
        jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
        stats = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", jev=jev, monthly_usd=8, encoder=stub, encoder_status="on").run(
            [_job(url) for url in pages]))
    finally:
        judging.configure(judging.EXAMPLE)
    fetches = _rows(tmp_path / "store", "fetches")
    sampled = {r["fetch_id"] for r in fetches if _in(r["sha256"], 0.25)}
    audit = {r["fetch_id"] for r in fetches if _in(r["sha256"], 0.25 * ENCODER_AUDIT_SHARE)}
    scores = _rows(tmp_path / "store", "encoder_scores")
    by_id = {r["fetch_id"]: r for r in scores}
    assert audit and len(scores) == len(by_id) == 400 - len(sampled) + len(audit)
    assert set(by_id) == ({r["fetch_id"] for r in fetches} - sampled) | audit
    assert {c["fetch_id"] for c in _rows(tmp_path / "store", "judgement_calls")} == sampled   # Jev still reads its sample only
    enc = stats["encoder"]
    assert (enc["status"], enc["model"], enc["scored"], enc["audited"]) == ("on", stub.stamp, 400 - len(sampled), len(audit))
    assert stats["judged"] == len(sampled) and stats["not_sampled"] == 400 - len(sampled)
    row = by_id[next(iter(audit))]
    assert row["jev_relevance"] == pytest.approx(0.9 * 0.95) and row["relevance"] == pytest.approx(0.6 * 0.95)
    assert row["model"] == f"encoder-{stub.stamp}" and row["relevance_version"] == "relevance_v1"
    assert json.loads(row["answers"])["doc_type"] == {"value": "record", "probabilities": {"record": 0.7, "news_or_commentary": 0.2, "other": 0.1}}
    assert all(r["jev_relevance"] is None for f, r in by_id.items() if f not in audit)
    assert enc["audit_mean_abs_diff"] == pytest.approx(0.3 * 0.95) and enc["audit_spearman"] is None   # constant scores: no ranks


def test_with_no_jev_key_the_encoder_judges_every_page(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(5)}
    stats = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", encoder=StubEncoder()).run([_job(url) for url in pages]))
    assert stats["pending"] == 5 and stats["encoder"]["scored"] == 5 and len(_rows(tmp_path / "store", "encoder_scores")) == 5


def test_an_encoder_failure_turns_it_off_and_leaves_pages_as_before(tmp_path: Path) -> None:
    pages = {f"https://a.test/{i}": _page(f"https://a.test/{i}", body=f"doc {i}".encode()) for i in range(5)}
    stub = StubEncoder(fail=True)
    stats = asyncio.run(_engine(tmp_path, FakeCascade(pages), "r1", encoder=stub, encoder_status="on").run([_job(url) for url in pages]))
    assert stub.calls == 1 and stats["stored"] == 5 and stats["encoder"]["scored"] == 0
    assert stats["encoder"]["stopped"].startswith("RuntimeError;") and "went wrong" not in stats["encoder"]["stopped"]
    assert _rows(tmp_path / "store", "encoder_scores") == []


def test_a_page_waiting_for_the_encoder_holds_no_jev_slot(tmp_path: Path) -> None:
    jev = _jev(lambda request: httpx.Response(200, json=_jev_body()))
    engine = Engine(LocalStore(tmp_path / "store"), FakeCascade({}), workdir=tmp_path / "w", now=lambda: NOW, run_id="r1", jev=jev,
                    monthly_usd=8, encoder=StubEncoder(), encoder_status="on")
    sha = "0" * 64   # inside every share: Jev's sample and the audit slice

    async def go() -> Any:
        await engine.start()
        slots = asyncio.Semaphore(1)
        async with engine._encoder_lock:   # the encoder busy with other pages
            task = asyncio.create_task(engine._judge("f1", sha, "some words here " * 50, NOW.date(), "new_document", slots))
            for _ in range(500):
                if engine._jev_calls:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)
            # Jev has answered; the audit waits for the encoder without the slot, so Jev's next page can go.
            assert engine._jev_calls == 1 and not task.done() and not slots.locked()
        return await task

    result = asyncio.run(go())
    assert result is not None and not result.model.startswith("encoder-") and engine.stats["encoder"]["audited"] == 1


@pytest.mark.skipif(sys.platform != "linux", reason="niceness is per thread on Linux only")
def test_the_scoring_thread_runs_below_the_crawl(tmp_path: Path) -> None:
    pool = ThreadPoolExecutor(1, initializer=encoder_module._lower_priority)
    try:
        mine = os.getpriority(os.PRIO_PROCESS, 0)
        assert pool.submit(os.getpriority, os.PRIO_PROCESS, 0).result() == min(19, mine + encoder_module.NICE)
        assert os.getpriority(os.PRIO_PROCESS, 0) == mine   # this thread, the event loop's, is untouched
    finally:
        pool.shutdown()


def test_in_a_crawl_the_encoders_answers_move_the_lines_and_its_queue_waits_until_the_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(frontier_module, "JUDGE_GRACE_SECONDS", 0.01)

    class Slow(StubEncoder):
        def ask(self, excerpt: str) -> JevResult:
            import time
            time.sleep(0.05)
            return super().ask(excerpt)

    store = LocalStore(tmp_path / "store")
    engine = Engine(store, Site(6), workdir=tmp_path / "w", now=lambda: NOW, run_id="r1", encoder=Slow(p=0.2))
    seeds = [Seed(id="a", entity="a", urls=("https://a.test/",))]
    stats = asyncio.run(asyncio.wait_for(crawl(store, engine, seeds, workdir=tmp_path / "w", max_fetches=100, max_minutes=1,
                                               now=lambda: NOW, scope="open", run_share=None), 20))
    assert stats["encoder"]["scored"] == stats["stored"] == 7 and stats["jev_abandoned"] == 0
    root = frontier_module.Frontier.load(store, tmp_path / "check").line("a")
    assert root["relevance"] < 1.0   # the encoder's 0.2 x 0.95 moved it, where with no judgement it would stay at 1


def test_open_encoder_falls_back_with_no_model_or_a_model_for_other_questions(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "store")
    assert open_encoder(store, tmp_path / "w") == (None, "off: no model")
    folder = tmp_path / "m"
    folder.mkdir()
    (folder / "manifest.json").write_text(json.dumps({"date": "20261123T074638Z", "encoder": {"format": "onnx"}, "window": 512,
                                                      "nouls": ["something_else"], "doc_types": [], "question_versions": {}}))
    store.put_many([(folder / "manifest.json", "models/judge/20261123T074638Z/manifest.json")])
    encoder, status = open_encoder(store, tmp_path / "w2")
    assert encoder is None and status.startswith("off: load failed: ValueError") and "another question set" in status


class FakeTokenizer:
    def encode(self, text: str, add_special_tokens: bool = True) -> Any:
        class Ids:
            ids = [len(word) for word in text.split()]
        return Ids()


class FakeSession:
    """Each window's vector is (mean id, number of real tokens), unnormalised: enough to check the pooling and the windows."""

    def __init__(self) -> None:
        self.batches: List[np.ndarray] = []

    def run(self, _: Any, feed: Dict[str, np.ndarray]) -> List[np.ndarray]:
        ids, mask = feed["input_ids"], feed["attention_mask"]
        self.batches.append(ids)
        return [np.stack([(ids * mask).sum(1) / mask.sum(1), mask.sum(1)], axis=1).astype(np.float32)]


def test_the_encoder_windows_pools_and_applies_its_head_as_the_bake_off_does() -> None:
    assert windows(list(range(10)), [100], [], [101], 6, 3) == [[100, 0, 1, 2, 3, 101], [100, 3, 4, 5, 6, 101], [100, 6, 7, 8, 9, 101]]
    assert windows([1, 2], [100], [], [101], 6, 3) == [[100, 1, 2, 101]]
    enc = Encoder.__new__(Encoder)
    enc._np, enc.stamp, enc.window, enc.max_windows = np, "s", 6, 3
    enc.tokenizer, enc._pad, enc._prefix, enc._head, enc._tail = FakeTokenizer(), 0, [], [9], [9]
    enc._session = FakeSession()
    enc._nouls, enc._choices = [q.id for q in NOULS], list(CHOICE.criteria)  # type: ignore[arg-type]
    enc._questions, enc._choice = {q.id: q for q in NOULS}, CHOICE
    rng = np.random.default_rng(0)
    w = {"w1": rng.normal(size=(2, 4)), "b1": rng.normal(size=4), "w2": rng.normal(size=(4, len(NOULS))), "b2": rng.normal(size=len(NOULS)),
         "w3": rng.normal(size=(4, len(enc._choices))), "b3": rng.normal(size=len(enc._choices))}
    enc._head_w = w
    text = "a bb ccc dddd eeeee f gg hhh iiii jjjjj"   # ten tokens: three windows of four
    vectors = [np.array([np.mean([9, *ids, 9]), 6.0]) for ids in ([1, 2, 3, 4], [4, 5, 1, 2], [2, 3, 4, 5])]
    x = np.mean(vectors, axis=0)
    x /= np.linalg.norm(x)
    hidden = np.maximum(x @ w["w1"] + w["b1"], 0)
    p = 1 / (1 + np.exp(-(hidden @ w["w2"] + w["b2"])))
    q = np.exp(hidden @ w["w3"] + w["b3"])
    q /= q.sum()
    result = enc.ask(text)
    assert [result.answers[n.id].value_number for n in NOULS] == pytest.approx(list(p), rel=1e-5)
    assert list(result.answers[CHOICE.id].probabilities.values()) == pytest.approx(list(q), rel=1e-5)
    assert result.answers[CHOICE.id].value_text == list(CHOICE.criteria)[int(q.argmax())] and result.model == "encoder-s"  # type: ignore[arg-type]
    assert len(enc._session.batches) == 1 and enc._session.batches[0].shape == (3, 6)


def test_spearman_ranks_ties_at_their_mean() -> None:
    assert spearman([(1, 1), (2, 2), (3, 3)]) == 1.0 and spearman([(1, 3), (2, 2), (3, 1)]) == -1.0
    assert spearman([(1, 1), (2, 2)]) is None and spearman([(1, 1), (1, 2), (1, 3)]) is None
    assert spearman([(1, 1), (2, 2), (2, 3), (4, 4)]) == pytest.approx(0.9487, abs=1e-4)
