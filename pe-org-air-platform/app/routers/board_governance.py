"""
Board Governance API Router (CS3 Task 5.0d)
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.pipelines.board_analyzer import (
    BoardCompositionAnalyzer,
    CompanyRegistry,
    GovernanceSignal,
    save_signal,
    save_signal_to_s3,
)
from app.services.s3_storage import get_s3_service
from app.repositories.document_repository import get_document_repository
from app.repositories.company_repository import CompanyRepository as get_company_repository
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

def _get_analyzer() -> BoardCompositionAnalyzer:
    try:
        s3 = get_s3_service()
    except Exception:
        s3 = None
    try:
        doc_repo = get_document_repository()
    except Exception:
        doc_repo = None
    return BoardCompositionAnalyzer(s3=s3, doc_repo=doc_repo)


def _resolve_company_id(ticker: str) -> str:
    try:
        repo = get_company_repository()
        company = repo.get_by_ticker(ticker.upper())
        if company:
            return company["id"]
    except Exception:
        pass
    return ticker.upper()


def _to_response(
    signal: GovernanceSignal,
    trail: dict,
    s3_key: Optional[str] = None,
) -> GovernanceOut:
    from decimal import Decimal

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


def _analyze_one(analyzer: BoardCompositionAnalyzer, ticker: str) -> tuple[GovernanceSignal, dict, Optional[str]]:
    """Run analysis, save locally + S3, return (signal, trail, s3_key)."""
    company_id = _resolve_company_id(ticker)
    signal = analyzer.scrape_and_analyze(ticker=ticker, company_id=company_id)
    trail = analyzer.get_last_evidence_trail()

    save_signal(signal)

    s3_key = None
    try:
        s3_key = save_signal_to_s3(signal, evidence_trail=trail)
    except Exception as e:
        logger.warning(f"[{ticker}] S3 save failed: {e}")

    return signal, trail, s3_key


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

    analyzer = _get_analyzer()
    try:
        signal, trail, s3_key = _analyze_one(analyzer, ticker)
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
    analyzer = _get_analyzer()

    results: List[GovernanceOut] = []
    errors: List[dict] = []

    for ticker in tickers:
        try:
            signal, trail, s3_key = _analyze_one(analyzer, ticker)
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

    # Try S3 first
    try:
        s3 = get_s3_service()
        keys = s3.list_files(f"signals/board_composition/{ticker}/")
        if keys:
            latest = sorted(keys)[-1]
            data = s3.get_file(latest)
            if data:
                import json
                return json.loads(data.decode("utf-8"))
    except Exception as e:
        logger.warning(f"[{ticker}] S3 read failed, running live analysis: {e}")

    # Fallback: live analysis (don't persist on GET)
    analyzer = _get_analyzer()
    try:
        signal = analyzer.scrape_and_analyze(ticker=ticker, company_id=_resolve_company_id(ticker))
        trail = analyzer.get_last_evidence_trail()
        return _to_response(signal, trail)
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
    results = []
    errors = []

    s3 = None
    try:
        s3 = get_s3_service()
    except Exception:
        pass

    analyzer = None  # lazy init only if needed

    for ticker in tickers:
        # Try S3
        if s3:
            try:
                keys = s3.list_files(f"signals/board_composition/{ticker}/")
                if keys:
                    latest = sorted(keys)[-1]
                    data = s3.get_file(latest)
                    if data:
                        import json
                        results.append(json.loads(data.decode("utf-8")))
                        continue
            except Exception as e:
                logger.warning(f"[{ticker}] S3 read failed: {e}")

        # Fallback: live
        try:
            if analyzer is None:
                analyzer = _get_analyzer()
            signal = analyzer.scrape_and_analyze(ticker=ticker, company_id=_resolve_company_id(ticker))
            trail = analyzer.get_last_evidence_trail()
            results.append(_to_response(signal, trail).model_dump())
        except Exception as e:
            logger.error(f"[{ticker}] Failed: {e}")
            errors.append({"ticker": ticker, "error": str(e)})

    return {"total": len(tickers), "results": results, "errors": errors}