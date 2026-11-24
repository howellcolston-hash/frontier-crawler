"""The judge encoder: a small model trained on Jev's answers, run on the CPU in the crawl for the pages Jev does not read.

A bake-off (Horizon's private distil job) saves it to the store as models/judge/<stamp>/: encoder.onnx (token ids and mask
-> one pooled, normalised vector per window), tokenizer.json and tokenizer_config.json, head.npz and manifest.json.
load() takes the newest stamp. A text is cut into up to max_windows evenly spaced windows of `window` tokens, special
tokens included; their vectors are averaged and normalised, and the head turns that vector into one probability per yes/no
question and a distribution over the choice question's options. ask() returns them as a JevResult, so everything that
reads Jev's answers reads the encoder's the same way; relevance comes from the configured rule, as for Jev.

Input contract:
    encoder = load(store, workdir)          # raises when there is no model, or it was trained on other questions
    result = encoder.ask(excerpt)           # blocking, CPU-bound: call it in a thread, one at a time
onnxruntime and tokenizers are imported by load(), so a crawl without them runs as before.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Sequence

from frontier_crawler.judge import questions as judging
from frontier_crawler.judge.jev import Answer, JevResult

MODELS = "models/judge"
BATCH_TOKENS = 8192  # PROVISIONAL: padded tokens per forward pass, as the bake-off embedded


def windows(ids: List[int], head: List[int], prefix: List[int], tail: List[int], window: int, max_windows: int) -> List[List[int]]:
    """A text's token ids as up to max_windows evenly spaced windows of at most `window` tokens, special tokens included."""
    body = window - len(head) - len(tail) - len(prefix)
    count = min(max_windows, max(1, math.ceil(len(ids) / body)))
    starts = [0] if count == 1 else [round(i * (len(ids) - body) / (count - 1)) for i in range(count)]
    return [head + prefix + ids[s : s + body] + tail for s in starts]


class Encoder:
    def __init__(self, folder: Path, manifest: Dict[str, Any]) -> None:
        import numpy as np
        import onnxruntime
        from tokenizers import Tokenizer

        if manifest["encoder"].get("format") != "onnx":
            raise ValueError(f"model {manifest.get('date')} holds a {manifest['encoder'].get('format')} encoder, not ONNX")
        self._np = np
        self.stamp = manifest["date"]
        self._nouls: List[str] = manifest["nouls"]
        self._choices: List[str] = manifest["doc_types"]
        # Its outputs are columns in the order it was trained on; another question set would be read under the wrong names.
        nouls = [q for q in judging.active() if q.type == "noul"]
        choice = [q for q in judging.active() if q.type == "choice"]
        if ([q.id for q in nouls] != self._nouls or manifest["question_versions"] != {q.id: q.version for q in judging.active()}
                or len(choice) != 1 or list(choice[0].criteria or ()) != self._choices):
            raise ValueError(f"model {self.stamp} was trained on another question set than the configured one")
        self._questions = {q.id: q for q in nouls}
        self._choice = choice[0]
        self.window, self.max_windows = int(manifest["window"]), int(manifest.get("max_windows", 3))
        self.tokenizer = Tokenizer.from_file(str(folder / "tokenizer.json"))
        self.tokenizer.no_truncation()   # as transformers calls it: windows are cut here, never by the tokenizer
        self.tokenizer.no_padding()
        config = json.loads((folder / "tokenizer_config.json").read_text(encoding="utf-8"))
        self._pad = self.tokenizer.token_to_id(config["pad_token"])
        prefix = manifest.get("prefix") or ""
        self._prefix = self.tokenizer.encode(prefix, add_special_tokens=False).ids if prefix else []
        special = self.tokenizer.encode("").ids
        self._head, self._tail = special[:1], special[1:]
        options = onnxruntime.SessionOptions()
        options.intra_op_num_threads = os.cpu_count() or 1
        options.add_session_config_entry("session.intra_op.allow_spinning", "0")   # idle threads sleep, leaving the crawl its CPU
        self._session = onnxruntime.InferenceSession(str(folder / "encoder.onnx"), options, providers=["CPUExecutionProvider"])
        self._head_w = {k: v for k, v in np.load(folder / "head.npz").items()}

    def embed(self, text: str) -> Any:
        """One normalised vector for the text: its windows' vectors averaged."""
        np = self._np
        ids = self.tokenizer.encode(text, add_special_tokens=False).ids
        items = sorted(windows(ids, self._head, self._prefix, self._tail, self.window, self.max_windows), key=len)
        chunks: List[List[List[int]]] = [[]]
        for item in items:  # ascending length, so the newest window sets the padded width
            if chunks[-1] and (len(chunks[-1]) + 1) * len(item) > BATCH_TOKENS:
                chunks.append([])
            chunks[-1].append(item)
        total = None
        for chunk in chunks:
            width = len(chunk[-1])
            batch = np.array([w + [self._pad] * (width - len(w)) for w in chunk], dtype=np.int64)
            mask = np.array([[1] * len(w) + [0] * (width - len(w)) for w in chunk], dtype=np.int64)
            vectors = self._session.run(None, {"input_ids": batch, "attention_mask": mask})[0].astype(np.float64)
            total = vectors.sum(0) if total is None else total + vectors.sum(0)
        mean = total / len(items)
        return (mean / max(float(np.linalg.norm(mean)), 1e-12)).astype(np.float32)

    def ask(self, excerpt: str) -> JevResult:
        np, w = self._np, self._head_w
        hidden = np.maximum(self.embed(excerpt) @ w["w1"] + w["b1"], 0.0)
        p_noul = 1.0 / (1.0 + np.exp(-(hidden @ w["w2"] + w["b2"])))
        logits = hidden @ w["w3"] + w["b3"]
        p_choice = np.exp(logits - logits.max())
        p_choice /= p_choice.sum()
        answers = {qid: Answer(self._questions[qid], float(p), None, None, None) for qid, p in zip(self._nouls, p_noul)}
        probs = {key: float(p) for key, p in zip(self._choices, p_choice)}
        best = max(probs, key=probs.__getitem__)
        answers[self._choice.id] = Answer(self._choice, None, best, probs, probs[best])
        return JevResult(answers, f"encoder-{self.stamp}", 0)


def load(store: Any, workdir: Path) -> Encoder:
    """The newest models/judge/<stamp>/ in the store. Raises FileNotFoundError when there is none."""
    stamps = sorted({r.split("/")[2] for r in store.list(MODELS) if r.endswith("/manifest.json") and r.count("/") == 3})
    if not stamps:
        raise FileNotFoundError(f"no {MODELS}/<stamp>/manifest.json in the store")
    folder = Path(workdir) / "model" / stamps[-1]
    for remote in store.list(f"{MODELS}/{stamps[-1]}"):
        store.get(remote, folder / remote.split(f"{stamps[-1]}/", 1)[1])
    return Encoder(folder, json.loads((folder / "manifest.json").read_text(encoding="utf-8")))


def spearman(pairs: Sequence[Any]) -> Any:
    """Rank correlation of (a, b) pairs, ties at their mean rank; None for fewer than 3 pairs or a constant side."""
    if len(pairs) < 3:
        return None

    def ranks(values: List[float]) -> List[float]:
        order = sorted(range(len(values)), key=values.__getitem__)
        out = [0.0] * len(values)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
                j += 1
            for k in range(i, j + 1):
                out[order[k]] = (i + j) / 2
            i = j + 1
        return out

    a, b = ranks([p[0] for p in pairs]), ranks([p[1] for p in pairs])
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    var = math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))
    return round(cov / var, 4) if var else None
