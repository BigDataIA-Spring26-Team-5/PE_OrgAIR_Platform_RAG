"""Output guardrails for the RAG chatbot pipeline."""
from __future__ import annotations

import re
from typing import Any, List

from app.guardrails.input_guards import GuardResult


_ANSWER_MIN = 20
_ANSWER_MAX = 2000

_CITATION_RE = re.compile(
    r"per SEC|per the|\[sec|\[job",
    re.IGNORECASE,
)

_REFUSAL_PREFIXES = (
    "I cannot",
    "I'm unable",
    "As an AI",
)

_GROUNDING_DISCLAIMER = (
    "\n\n[Note: No supporting evidence was retrieved for this response. "
    "The answer above may not be grounded in verified filings or disclosures.]"
)

_REFUSAL_FALLBACK = (
    "The system was unable to generate an answer for this question. "
    "Please rephrase or try a different query."
)


def check_answer_length(answer: str) -> GuardResult:
    if len(answer) < _ANSWER_MIN:
        return GuardResult(passed=False, reason="Answer too short.")
    if len(answer) > _ANSWER_MAX:
        return GuardResult(passed=False, reason="Answer too long.")
    return GuardResult(passed=True)


def check_answer_grounded(answer: str, evidence: List[Any]) -> str:
    """Append a disclaimer when evidence is empty but answer cites sources."""
    if evidence:
        return answer
    if _CITATION_RE.search(answer):
        return answer + _GROUNDING_DISCLAIMER
    return answer


def check_no_refusal(answer: str) -> str:
    """Convert LLM refusal responses to a structured fallback string."""
    for prefix in _REFUSAL_PREFIXES:
        if answer.startswith(prefix):
            return _REFUSAL_FALLBACK
    return answer
