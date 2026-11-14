"""Client for Jev (TypeSafe's decision model).

API as documented at docs.typesafe.ai on 2026-11-09:
    POST https://api.typesafe.ai/v1/systemone   Authorization: Bearer <key>
    {"model": "jev-latest", "state": "<text>", "questions": {"<key>": {"type", "instructions", "criteria"?}}}
    -> {"model": "jev-1.13.0", "answers": {"<key>": {...}}, "usage": {"input_tokens": n, "output_tokens": m}}
A noul answer is {"type": "noul", "noul": p}: the probability of yes, with no confidence field.
choice and score answers carry their value, "probabilities" and "confidence".

Input contract: await client.ask(state, questions) -> JevResult.
JevUnavailable means try again later (network, 429, 5xx, 529). JevRejected means the request
or the key is wrong and a retry will not help. Neither is a judgement about the document.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

import httpx

from frontier_crawler.judge.questions import Question

JEV_URL = "https://api.typesafe.ai/v1/systemone"
# A fixed version, not the jev-latest alias, so the judge only changes when this line does.
# Re-check stored answers against the labelled set before moving it.
JEV_MODEL = "jev-1.14.0"
# PROVISIONAL: the early-access price, USD per million input tokens. Output tokens are not billed.
PRICE_PER_MTOK_USD = 0.042


class JevUnavailable(Exception):
    """Transport trouble or rate limiting. The document stays unjudged and is picked up later."""


class JevRejected(Exception):
    """Jev refused the request (bad key, bad body, or an answer missing from the reply).

    status is the HTTP status Jev returned, or None when the reply (or the request) was malformed.
    """

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Answer:
    question: Question
    value_number: Optional[float]
    value_text: Optional[str]
    probabilities: Optional[Dict[str, float]]
    confidence: Optional[float]


@dataclass(frozen=True)
class JevResult:
    answers: Dict[str, Answer]  # by question id
    model: str
    input_tokens: int

    def nouls(self) -> Dict[str, float]:
        return {
            qid: answer.value_number
            for qid, answer in self.answers.items()
            if answer.question.type == "noul" and answer.value_number is not None
        }


def _parse_answer(question: Question, raw: Any) -> Answer:
    if not isinstance(raw, dict):
        raise JevRejected(f"answer for {question.key} is not an object")
    probabilities = raw.get("probabilities")
    if probabilities is not None:
        probabilities = {str(key): float(value) for key, value in probabilities.items()}
    confidence = raw.get("confidence")
    confidence = float(confidence) if confidence is not None else None
    if question.type == "noul":
        value = raw.get("noul")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise JevRejected(f"noul answer for {question.key} is not a number")
        return Answer(question, float(value), None, probabilities, confidence)
    if question.type == "choice":
        value = raw.get("choice")
        if not isinstance(value, str):
            raise JevRejected(f"choice answer for {question.key} is not text")
        return Answer(question, None, value, probabilities, confidence)
    if question.type == "score":
        value = raw.get("score")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise JevRejected(f"score answer for {question.key} is not a number")
        return Answer(question, float(value), None, probabilities, confidence)
    raise JevRejected(f"unknown question type {question.type}")


class JevClient:
    def __init__(
        self,
        api_key: str,
        client: httpx.AsyncClient,
        url: str = JEV_URL,
        model: str = JEV_MODEL,
        retries: int = 3,
        backoff_s: float = 2.0,
        timeout_s: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not api_key:
            raise ValueError("Jev API key is empty")
        self._key = api_key
        self._client = client
        self._url = url
        self._model = model
        self._retries = retries
        self._backoff_s = backoff_s
        self._timeout_s = timeout_s
        self._sleep = sleep

    async def ask(self, state: str, questions: Sequence[Question]) -> JevResult:
        if not state.strip():
            raise JevRejected("state is empty")
        payload = {
            "model": self._model,
            "state": state,
            "questions": {question.key: question.payload() for question in questions},
        }
        body = await self._post(payload)
        answers = body.get("answers")
        if not isinstance(answers, dict):
            raise JevRejected("reply has no answers object")
        parsed: Dict[str, Answer] = {}
        for question in questions:
            if question.key not in answers:
                raise JevRejected(f"reply is missing {question.key}")
            parsed[question.id] = _parse_answer(question, answers[question.key])
        usage = body.get("usage") if isinstance(body.get("usage"), dict) else {}
        return JevResult(parsed, str(body.get("model") or ""), int(usage.get("input_tokens") or 0))

    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # The key goes in the header only. Error text below never includes the request.
        headers = {"Authorization": f"Bearer {self._key}"}
        last = "no attempt made"
        for attempt in range(self._retries + 1):
            wait = self._backoff_s * (attempt + 1)
            try:
                response = await self._client.post(self._url, json=payload, headers=headers, timeout=self._timeout_s)
            except httpx.TransportError as exc:
                last = f"{type(exc).__name__}: {exc}"
            else:
                status = response.status_code
                if status == 200:
                    try:
                        body = response.json()
                    except ValueError as exc:
                        raise JevRejected("reply was not JSON") from exc
                    if not isinstance(body, dict):
                        raise JevRejected("reply was not a JSON object")
                    return body
                if status == 429 or status >= 500:
                    last = f"HTTP {status}"
                    retry_after = response.headers.get("retry-after", "")
                    if retry_after.isdigit():
                        wait = min(float(retry_after), 60.0)
                else:
                    raise JevRejected(f"HTTP {status}", status)
            if attempt < self._retries:
                await self._sleep(wait)
        raise JevUnavailable(last)
