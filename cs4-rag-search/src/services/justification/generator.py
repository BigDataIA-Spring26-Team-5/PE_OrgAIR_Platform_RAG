"""
generator.py — CS4 RAG Search
src/services/justification/generator.py

Structured IC memo generation.
Produces a JSON-structured investment committee memo grounded in retrieved evidence.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import structlog

from ..llm.router import chat
from ..retrieval.hybrid import RetrievedChunk

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

@dataclass
class DimensionJustification:
    dimension: str
    score: Optional[float]
    level_label: Optional[str]
    narrative: str
    supporting_quotes: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)


@dataclass
class ICMemo:
    ticker: str
    company_name: str
    overall_assessment: str
    org_air_score: Optional[float]
    confidence_interval: Optional[tuple[float, float]]
    dimension_justifications: List[DimensionJustification] = field(default_factory=list)
    key_risks: List[str] = field(default_factory=list)
    key_opportunities: List[str] = field(default_factory=list)
    recommendation: str = ""
    generated_by_model: str = ""


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are a senior PE investment analyst writing an IC memo section.
You will be given:
  - Company ticker and name
  - CS3 dimension scores (0–100 scale, 7 dimensions)
  - Retrieved evidence chunks from SEC filings, job postings, and analyst notes

Write a structured JSON memo with the following fields:
{
  "overall_assessment": "<2-3 sentence executive summary>",
  "dimension_justifications": [
    {
      "dimension": "<dimension name>",
      "narrative": "<2-3 sentences grounded in evidence>",
      "supporting_quotes": ["<verbatim quote from evidence>", ...],
      "risk_flags": ["<specific risk>", ...]
    },
    ...
  ],
  "key_risks": ["<risk>", ...],
  "key_opportunities": ["<opportunity>", ...],
  "recommendation": "<BUY / HOLD / PASS with one sentence rationale>"
}

Be specific. Quote evidence directly. Flag gaps where evidence is absent.
Output only valid JSON — no prose outside the JSON block."""


# ---------------------------------------------------------------------------
# JustificationGenerator
# ---------------------------------------------------------------------------

class JustificationGenerator:
    """Generates a structured IC memo from dimension scores + retrieved chunks."""

    def __init__(self, model: str = "gpt-4o", temperature: float = 0.2) -> None:
        self._model = model
        self._temperature = temperature

    async def generate(
        self,
        ticker: str,
        company_name: str,
        dimension_scores: List[Dict[str, Any]],
        evidence_chunks: List[RetrievedChunk],
        org_air_score: Optional[float] = None,
        confidence_interval: Optional[tuple[float, float]] = None,
    ) -> ICMemo:
        """Generate an IC memo grounded in retrieved evidence chunks."""
        logger.info("generating IC memo", ticker=ticker, chunks=len(evidence_chunks))

        user_content = self._build_user_message(
            ticker, company_name, dimension_scores, evidence_chunks,
            org_air_score, confidence_interval,
        )
        raw = await chat(
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            model=self._model,
            temperature=self._temperature,
        )

        return self._parse_response(raw, ticker, company_name, org_air_score, confidence_interval)

    def _build_user_message(
        self,
        ticker: str,
        company_name: str,
        dimension_scores: List[Dict[str, Any]],
        chunks: List[RetrievedChunk],
        org_air_score: Optional[float],
        ci: Optional[tuple[float, float]],
    ) -> str:
        scores_text = "\n".join(
            f"  {s.get('dimension', '?')}: {s.get('score', 'N/A')} "
            f"(confidence {s.get('confidence', '?')})"
            for s in dimension_scores
        )
        evidence_text = "\n\n".join(
            f"[{i+1}] Source={c.source} Ticker={c.ticker}\n{c.text[:600]}"
            for i, c in enumerate(chunks[:12])
        )
        org_line = (
            f"Org-AI-R Score: {org_air_score:.1f} "
            f"(95% CI: {ci[0]:.1f}–{ci[1]:.1f})"
            if org_air_score is not None and ci
            else ""
        )
        return (
            f"Company: {company_name} ({ticker})\n"
            f"{org_line}\n\n"
            f"Dimension Scores:\n{scores_text}\n\n"
            f"Evidence Chunks:\n{evidence_text}"
        )

    def _parse_response(
        self,
        raw: str,
        ticker: str,
        company_name: str,
        org_air_score: Optional[float],
        ci: Optional[tuple[float, float]],
    ) -> ICMemo:
        # Strip markdown code fences if present
        clean = raw.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        try:
            data = json.loads(clean)
        except json.JSONDecodeError:
            logger.warning("failed to parse LLM JSON", raw=raw[:200])
            data = {}

        dim_justifications = [
            DimensionJustification(
                dimension=dj.get("dimension", ""),
                score=None,
                level_label=None,
                narrative=dj.get("narrative", ""),
                supporting_quotes=dj.get("supporting_quotes", []),
                risk_flags=dj.get("risk_flags", []),
            )
            for dj in data.get("dimension_justifications", [])
        ]

        return ICMemo(
            ticker=ticker,
            company_name=company_name,
            overall_assessment=data.get("overall_assessment", ""),
            org_air_score=org_air_score,
            confidence_interval=ci,
            dimension_justifications=dim_justifications,
            key_risks=data.get("key_risks", []),
            key_opportunities=data.get("key_opportunities", []),
            recommendation=data.get("recommendation", ""),
            generated_by_model=self._model,
        )
