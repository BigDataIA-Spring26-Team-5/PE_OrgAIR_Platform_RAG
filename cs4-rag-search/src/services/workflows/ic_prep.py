"""
ic_prep.py — CS4 RAG Search
src/services/workflows/ic_prep.py

IC preparation workflow: orchestrates CS1/CS2/CS3 clients → hybrid retrieval → IC memo.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import structlog

from ..integration.cs1_client import CS1Client, Company
from ..integration.cs2_client import CS2Client, CS2Evidence
from ..integration.cs3_client import CS3Client, CompanyAssessment
from ..retrieval.hybrid import HybridRetriever, RetrievedChunk
from ..justification.generator import JustificationGenerator, ICMemo

logger = structlog.get_logger(__name__)


@dataclass
class ICPrepResult:
    ticker: str
    company: Optional[Company]
    assessment: Optional[CompanyAssessment]
    evidence_chunks: List[RetrievedChunk]
    memo: Optional[ICMemo]
    errors: List[str]


class ICPrepWorkflow:
    """
    End-to-end IC prep for a single ticker:
      1. Fetch company metadata (CS1)
      2. Fetch evidence signals (CS2)
      3. Fetch dimension scores / assessment (CS3)
      4. Retrieve relevant chunks via hybrid search
      5. Generate IC memo
    """

    def __init__(
        self,
        cs1: CS1Client,
        cs2: CS2Client,
        cs3: CS3Client,
        retriever: HybridRetriever,
        generator: JustificationGenerator,
    ) -> None:
        self._cs1 = cs1
        self._cs2 = cs2
        self._cs3 = cs3
        self._retriever = retriever
        self._generator = generator

    async def run(self, ticker: str, assessment_id: Optional[str] = None) -> ICPrepResult:
        errors: List[str] = []
        company: Optional[Company] = None
        assessment: Optional[CompanyAssessment] = None
        evidence: List[CS2Evidence] = []

        # Step 1 — company metadata
        try:
            company = await self._cs1.get_company(ticker)
            logger.info("fetched company", ticker=ticker, name=company.name)
        except Exception as exc:
            errors.append(f"CS1 error: {exc}")
            logger.warning("cs1 fetch failed", ticker=ticker, error=str(exc))

        # Step 2 — evidence signals
        try:
            evidence = await self._cs2.get_evidence(ticker)
            logger.info("fetched evidence", ticker=ticker, count=len(evidence))
        except Exception as exc:
            errors.append(f"CS2 error: {exc}")
            logger.warning("cs2 fetch failed", ticker=ticker, error=str(exc))

        # Step 3 — assessment + dimension scores
        if assessment_id:
            try:
                assessment = await self._cs3.get_assessment(assessment_id)
                logger.info("fetched assessment", ticker=ticker, id=assessment_id)
            except Exception as exc:
                errors.append(f"CS3 error: {exc}")
                logger.warning("cs3 fetch failed", ticker=ticker, error=str(exc))

        # Step 4 — hybrid retrieval
        query = f"{ticker} AI readiness digital transformation organizational capability"
        chunks = self._retriever.search(query, top_k=10)

        # Step 5 — IC memo generation
        memo: Optional[ICMemo] = None
        if company and assessment:
            dim_scores = [
                {
                    "dimension": ds.dimension.value,
                    "score": ds.score,
                    "confidence": ds.confidence,
                }
                for ds in assessment.dimension_scores
            ]
            try:
                memo = await self._generator.generate(
                    ticker=ticker,
                    company_name=company.name,
                    dimension_scores=dim_scores,
                    evidence_chunks=chunks,
                    org_air_score=assessment.v_r_score,
                    confidence_interval=(
                        (assessment.confidence_lower, assessment.confidence_upper)
                        if assessment.confidence_lower and assessment.confidence_upper
                        else None
                    ),
                )
            except Exception as exc:
                errors.append(f"memo generation error: {exc}")
                logger.error("memo generation failed", ticker=ticker, error=str(exc))

        return ICPrepResult(
            ticker=ticker,
            company=company,
            assessment=assessment,
            evidence_chunks=chunks,
            memo=memo,
            errors=errors,
        )
