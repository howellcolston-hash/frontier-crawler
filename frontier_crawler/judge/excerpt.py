"""What part of a document Jev reads. The whole document is stored either way.

Input contract: build_excerpt(text, max_words) -> (excerpt, method, words).
Up to max_words the full text is sent ("full"). A longer text is sampled ("sampled"):
the opening, the headings that follow it, passages from three points in the middle, and the end,
so a document that is good all the way through is judged on all of it.
"""

from __future__ import annotations

from typing import List, Tuple

# PROVISIONAL. Jev bills the document once per call however many questions follow, so the cap
# is about cost per document, not per question.
DEFAULT_MAX_WORDS = 1500
_GAP = "\n[...]\n"


def build_excerpt(text: str, max_words: int = DEFAULT_MAX_WORDS) -> Tuple[str, str, int]:
    words = text.split()
    if len(words) <= max_words:
        return text.strip(), "full", len(words)

    # A tenth of the allowance is held for headings from beyond the opening passage, which
    # show what the rest of the document covers.
    allowance = max_words // 10
    remaining = max_words - allowance
    head = int(remaining * 0.40)
    tail = int(remaining * 0.15)
    window = (remaining - head - tail) // 3

    headings: List[str] = []
    position = 0
    for line in text.splitlines():
        stripped = line.strip()
        cost = len(stripped.split())
        if stripped.startswith("#") and position >= head and cost <= allowance:
            headings.append(stripped)
            allowance -= cost
        position += cost

    parts: List[str] = [" ".join(words[:head])]
    if headings:
        parts.append("Headings in the rest of the document:\n" + "\n".join(headings))
    middle = len(words) - head - tail
    for i in range(3):
        start = head + int(middle * (i + 1) / 4) - window // 2
        parts.append(" ".join(words[start : start + window]))
    parts.append(" ".join(words[len(words) - tail :]))
    excerpt = _GAP.join(part for part in parts if part)
    return excerpt, "sampled", len(excerpt.split())
