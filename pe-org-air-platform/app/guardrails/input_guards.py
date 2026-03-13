"""Input guardrails for the RAG chatbot pipeline."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from app.services.integration.cs3_client import DIMENSIONS


@dataclass
class GuardResult:
    passed: bool
    reason: Optional[str] = None  # human-readable block reason


_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9\.\-]{0,9}$")

_INJECTION_PATTERNS = re.compile(
    r"ignore (previous|all|above) instructions?"
    r"|you are (now|actually)"
    r"|forget (everything|your instructions)"
    r"|system prompt"
    r"|<\|im_start\|>"
    r"|\[INST\]",
    re.IGNORECASE,
)

_QUESTION_MIN = 10
_QUESTION_MAX = 500


def validate_ticker(ticker: str) -> GuardResult:
    if not _TICKER_RE.match(ticker):
        return GuardResult(
            passed=False,
            reason=(
                "Invalid ticker format. Must start with a letter, "
                "contain only uppercase letters, digits, '.', or '-', "
                "and be 1–10 characters long."
            ),
        )
    return GuardResult(passed=True)


def validate_question(question: str) -> GuardResult:
    if len(question) < _QUESTION_MIN:
        return GuardResult(
            passed=False,
            reason=f"Question too short (minimum {_QUESTION_MIN} characters).",
        )
    if len(question) > _QUESTION_MAX:
        return GuardResult(
            passed=False,
            reason=f"Question too long (maximum {_QUESTION_MAX} characters).",
        )
    if _INJECTION_PATTERNS.search(question):
        return GuardResult(
            passed=False,
            reason="Question contains disallowed content.",
        )
    return GuardResult(passed=True)


def validate_dimension(dimension: Optional[str]) -> GuardResult:
    if dimension is None:
        return GuardResult(passed=True)
    if dimension not in DIMENSIONS:
        return GuardResult(
            passed=False,
            reason=(
                f"Invalid dimension '{dimension}'. "
                f"Allowed values: {', '.join(DIMENSIONS)}."
            ),
        )
    return GuardResult(passed=True)
