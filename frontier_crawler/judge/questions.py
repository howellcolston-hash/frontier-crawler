"""What the engine asks Jev about a document, and how the answers combine.

Rules for questions:
  * Each asks about one observable property of the text. Jev knows nothing about the project,
    so "is this relevant?" is not asked. Relevance is computed here from the answers.
  * Wording is neutral. How lenient the engine is lives in miss_to_junk_cost, one number.
  * A reworded question gets a new version. Answers from different versions are never mixed.
Every answer is stored as a probability, so the formula and the threshold below can be
changed later and applied to old documents without asking Jev again.

The questions are configuration, not code: `configure` takes them as a JSON object (the store's
config/questions.json). EXAMPLE shows the format and is what runs until `configure` is called.
The crawl reads two answers by name when they are asked: `leads_to_records` (how much a page's
links are worth) and `data_portal` (candidates for a derived client).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

Criteria = Union[Dict[str, str], List[str]]


@dataclass(frozen=True)
class Question:
    id: str
    version: int
    type: str  # noul | choice | score
    instructions: str
    criteria: Optional[Criteria] = None

    @property
    def key(self) -> str:
        """The name sent to Jev and returned in its answers."""
        return f"{self.id}_v{self.version}"

    def payload(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {"type": self.type, "instructions": self.instructions}
        if self.criteria is not None:
            body["criteria"] = self.criteria
        return body


EXAMPLE: Dict[str, Any] = {
    "questions": [
        {"id": "has_specifics", "version": 1, "type": "noul",
         "instructions": "The text gives specific facts: figures, dates, names or terms.",
         "criteria": {"true": "States concrete figures, dates, terms or named items", "false": "General description only"}},
        {"id": "junk_page", "version": 1, "type": "noul",
         "instructions": "The text has no content of its own.",
         "criteria": {"true": "An error page, login or consent wall, bare navigation or menu, advertising, or placeholder text",
                      "false": "Carries its own information, however short"}},
        {"id": "leads_to_records", "version": 1, "type": "noul",
         "instructions": "The page lists or links to further records of the same kind: an archive, register, search results or data portal."},
        {"id": "data_portal", "version": 1, "type": "noul",
         "instructions": "The page is an interactive data portal or dashboard whose figures are loaded from a backend service, not written in the page."},
        {"id": "doc_type", "version": 1, "type": "choice", "instructions": "What kind of document this is.",
         "criteria": {"record": "An official or first-hand record", "news_or_commentary": "News reporting, analysis or opinion", "other": "None of the above"}},
    ],
    # relevance = product of `all` x the largest of `any` x product of (1 - each of `none`).
    # A missed relevant document is taken to be miss_to_junk_cost times worse than a stored junk one.
    "relevance": {"version": "relevance_v1", "all": [], "any": ["has_specifics"], "none": ["junk_page"], "miss_to_junk_cost": 10},
}

_questions: Sequence[Question] = ()
_rule: Dict[str, Any] = {}


def configure(config: Mapping[str, Any]) -> None:
    """Set the questions and the relevance rule. Raises ValueError on a config that could not work."""
    global _questions, _rule
    questions = tuple(Question(q["id"], int(q["version"]), q["type"], q["instructions"], q.get("criteria")) for q in config["questions"])
    rule = dict(config["relevance"])
    nouls = {q.id for q in questions if q.type == "noul"}
    unknown = [name for part in ("all", "any", "none") for name in rule.get(part, []) if name not in nouls]
    if unknown or len({q.id for q in questions}) != len(questions) or float(rule["miss_to_junk_cost"]) <= 0:
        raise ValueError(f"questions config: duplicate ids, a bad cost, or relevance names that are not noul questions: {unknown}")
    _questions, _rule = questions, rule


def active() -> Sequence[Question]:
    return _questions


def relevance_version() -> str:
    return _rule["version"]


def keep_threshold() -> float:
    """PROVISIONAL. A document is worth keeping when its relevance is at least this."""
    return 1.0 / (1.0 + float(_rule["miss_to_junk_cost"]))


def relevance(nouls: Mapping[str, float]) -> Optional[float]:
    """PROVISIONAL. Probability-like score that a document is worth following up.

    Returns None when an answer it needs is missing.
    """
    try:
        score = 1.0
        for name in _rule.get("all", []):
            score *= nouls[name]
        if _rule.get("any"):
            score *= max(nouls[name] for name in _rule["any"])
        for name in _rule.get("none", []):
            score *= 1.0 - nouls[name]
    except KeyError:
        return None
    return score


configure(EXAMPLE)
