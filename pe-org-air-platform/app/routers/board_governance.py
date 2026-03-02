"""
Board Governance API Router (CS3 Task 5.0d)
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.pipelines.board_analyzer import (
    CompanyRegistry,
    GovernanceSignal,
)
from app.services.board_governance_service import get_board_governance_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/board-governance", tags=["Board Governance"])


# ────────────────────────────────────────────────────────────────
# Response Models
# ────────────────────────────────────────────────────────────────

class BoardMemberOut(BaseModel):
    name: str
    title: str
    is_independent: bool
    tenure_years: int
    committees: List[str]


class GovernanceOut(BaseModel):
    company_id: str
    ticker: str
    governance_score: float
    confidence: float
    independent_ratio: float
    tech_expertise_count: int
    has_tech_committee: bool
    has_ai_expertise: bool
    has_data_officer: bool
    has_risk_tech_oversight: bool
    has_ai_in_strategy: bool
    ai_experts: List[str]
    relevant_committees: List[str]
    board_members: List[BoardMemberOut]
    score_breakdown: Optional[dict] = None
    confidence_detail: Optional[dict] = None
    s3_key: Optional[str] = None


class BatchOut(BaseModel):
    total: int
    succeeded: int
    failed: int
    results: List[GovernanceOut]
    errors: List[dict] = []


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def _to_response(
    signal: GovernanceSignal,
    trail: dict,
    s3_key: Optional[str] = None,
) -> GovernanceOut:
    conf = float(signal.confidence)
    return GovernanceOut(
        company_id=signal.company_id,
        ticker=signal.ticker,
        governance_score=float(signal.governance_score),
        confidence=conf,
        independent_ratio=float(signal.independent_ratio),
        tech_expertise_count=signal.tech_expertise_count,
        has_tech_committee=signal.has_tech_committee,
        has_ai_expertise=signal.has_ai_expertise,
        has_data_officer=signal.has_data_officer,
        has_risk_tech_oversight=signal.has_risk_tech_oversight,
        has_ai_in_strategy=signal.has_ai_in_strategy,
        ai_experts=signal.ai_experts,
        relevant_committees=signal.relevant_committees,
        board_members=[BoardMemberOut(**m) for m in signal.board_members],
        score_breakdown=trail,
        confidence_detail={
            "value": conf,
            "formula": "min(0.50 + board_members_count / 20, 0.95)",
            "board_members_extracted": len(signal.board_members),
            "interpretation": "High" if conf >= 0.80 else "Medium" if conf >= 0.65 else "Low",
        },
        s3_key=s3_key,
    )


# ────────────────────────────────────────────────────────────────
# POST /analyze/{ticker}  — single company
# ────────────────────────────────────────────────────────────────

@router.post("/analyze/{ticker}", response_model=GovernanceOut)
async def analyze_ticker(ticker: str):
    """Analyze board governance for a single company and persist results."""
    ticker = ticker.upper()
    try:
        CompanyRegistry.get(ticker)
    except ValueError:
        raise HTTPException(404, f"Ticker '{ticker}' not registered. Available: {CompanyRegistry.all_tickers()}")

    try:
        signal, trail, s3_key = get_board_governance_service().analyze(ticker)
    except Exception as e:
        logger.error(f"[{ticker}] Analysis failed: {e}")
        raise HTTPException(500, f"Analysis failed: {e}")

    return _to_response(signal, trail, s3_key)


# ────────────────────────────────────────────────────────────────
# POST /analyze  — all 5 CS3 companies
# ────────────────────────────────────────────────────────────────

@router.post("/analyze", response_model=BatchOut)
async def analyze_all():
    """Analyze board governance for all 5 CS3 companies (NVDA, JPM, WMT, GE, DG)."""
    tickers = CompanyRegistry.all_tickers()
    svc = get_board_governance_service()

    results: List[GovernanceOut] = []
    errors: List[dict] = []

    for ticker in tickers:
        try:
            signal, trail, s3_key = svc.analyze(ticker)
            results.append(_to_response(signal, trail, s3_key))
        except Exception as e:
            logger.error(f"[{ticker}] Failed: {e}")
            errors.append({"ticker": ticker, "error": str(e)})

    return BatchOut(
        total=len(tickers),
        succeeded=len(results),
        failed=len(errors),
        results=results,
        errors=errors,
    )


# ────────────────────────────────────────────────────────────────
# GET /score/{ticker}  — latest from S3 (or live)
# ────────────────────────────────────────────────────────────────

@router.get("/score/{ticker}")
async def get_governance_score(ticker: str):
    """
    Get latest governance signal for a ticker.

    Tries S3 first (most recent stored result). Falls back to live analysis.
    """
    ticker = ticker.upper()
    try:
        CompanyRegistry.get(ticker)
    except ValueError:
        raise HTTPException(404, f"Ticker '{ticker}' not registered")

    svc = get_board_governance_service()

    # Try S3 first
    data, _ = svc.get(ticker)
    if data is not None:
        return data

    # Fallback: live analysis
    try:
        signal, trail, s3_key = svc.analyze(ticker)
        return _to_response(signal, trail, s3_key)
    except Exception as e:
        raise HTTPException(500, f"Analysis failed: {e}")


# ────────────────────────────────────────────────────────────────
# GET /scores  — latest for all 5 companies
# ────────────────────────────────────────────────────────────────

@router.get("/scores")
async def get_all_governance_scores():
    """
    Get latest governance signals for all 5 CS3 companies.

    Tries S3 first per ticker. Falls back to live analysis for any missing.
    """
    tickers = CompanyRegistry.all_tickers()
    svc = get_board_governance_service()
    results = []
    errors = []

    for ticker in tickers:
        # Try S3
        data, _ = svc.get(ticker)
        if data is not None:
            results.append(data)
            continue

        # Fallback: live
        try:
            signal, trail, s3_key = svc.analyze(ticker)
            results.append(_to_response(signal, trail, s3_key).model_dump())
        except Exception as e:
            logger.error(f"[{ticker}] Failed: {e}")
            errors.append({"ticker": ticker, "error": str(e)})

    return {"total": len(tickers), "results": results, "errors": errors}
