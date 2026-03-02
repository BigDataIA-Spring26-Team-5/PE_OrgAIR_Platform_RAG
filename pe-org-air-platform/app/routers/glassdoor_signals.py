"""
Glassdoor Culture Signals API Router (CS3)

Endpoints:
  POST /api/v1/glassdoor-signals/{ticker}          — Collect culture reviews & save to S3 + Snowflake
  GET  /api/v1/glassdoor-signals/{ticker}           — Full score breakdown for one company
  GET  /api/v1/glassdoor-signals/portfolio/all      — Score breakdowns for all 5 CS3 companies

Data sources: Glassdoor, Indeed, CareerBliss (via CultureCollector pipeline)
S3 paths:
  glassdoor_signals/raw/{TICKER}/{timestamp}_raw.json
  glassdoor_signals/output/{TICKER}/{timestamp}_culture.json
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.culture_signal_service import get_culture_signal_service, CultureCollectResult

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/glassdoor-signals", tags=["Glassdoor Culture Signals"])

CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]


# =====================================================================
# Response Models
# =====================================================================

class ReviewOut(BaseModel):
    """Single review from raw collection."""
    ticker: str
    source: str
    review_id: str
    rating: Optional[float] = None
    title: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None
    advice_to_management: Optional[str] = None
    is_current_employee: Optional[bool] = None
    job_title: Optional[str] = None
    review_date: Optional[str] = None


class CollectCultureResponse(BaseModel):
    """Response for POST /{ticker} — collection endpoint."""
    ticker: str
    status: str
    review_count: int
    sources_collected: Dict[str, int] = Field(
        default_factory=dict,
        description="Reviews collected per source (glassdoor, indeed, careerbliss)",
    )
    s3_raw_key: Optional[str] = None
    s3_output_key: Optional[str] = None
    snowflake_upserted: bool = False
    culture_scores: Optional[Dict[str, float]] = None
    raw_reviews: List[ReviewOut] = Field(default_factory=list)
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class CultureSignalDetailOut(BaseModel):
    """Full score breakdown for one company."""
    ticker: str
    company_id: Optional[str] = None
    overall_score: Optional[float] = None
    innovation_score: Optional[float] = None
    data_driven_score: Optional[float] = None
    change_readiness_score: Optional[float] = None
    ai_awareness_score: Optional[float] = None
    review_count: Optional[int] = None
    avg_rating: Optional[float] = None
    current_employee_ratio: Optional[float] = None
    confidence: Optional[float] = None
    source_breakdown: Optional[Dict[str, int]] = None
    positive_keywords_found: Optional[List[str]] = None
    negative_keywords_found: Optional[List[str]] = None
    run_timestamp: Optional[str] = None
    s3_source: Optional[str] = None


class PortfolioCultureResponse(BaseModel):
    """Response for GET /portfolio/all."""
    status: str
    companies_found: int
    companies_missing: int
    results: List[CultureSignalDetailOut]
    summary_table: List[Dict[str, Any]]


# =====================================================================
# POST /api/v1/glassdoor-signals/{ticker} — Collect + save to S3 + Snowflake
# =====================================================================

@router.post(
    "/{ticker}",
    response_model=CollectCultureResponse,
    summary="Collect culture reviews from Glassdoor/Indeed/CareerBliss",
    description="""
    Runs the full CultureCollector pipeline for a single ticker:

    1. Scrapes reviews from Glassdoor (RapidAPI), Indeed, and CareerBliss
    2. Analyzes reviews → CultureSignal (innovation, data-driven, AI awareness, change readiness)
    3. Uploads raw reviews to S3: glassdoor_signals/raw/{TICKER}/
    4. Uploads scored output to S3: glassdoor_signals/output/{TICKER}/
    5. Upserts glassdoor_reviews row into Snowflake signal_dimension_mapping
    6. Returns the raw review data extracted from all sources

    Valid tickers: NVDA, JPM, WMT, GE, DG
    """,
)
async def collect_culture_signal(ticker: str):
    """Collect and analyze culture reviews for one company."""
    start = time.time()
    ticker = ticker.upper()

    if ticker not in CS3_PORTFOLIO:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ticker '{ticker}'. Must be one of: {', '.join(CS3_PORTFOLIO)}",
        )

    try:
        logger.info("=" * 60)
        logger.info("GLASSDOOR COLLECTION: %s", ticker)
        logger.info("=" * 60)

        result: CultureCollectResult = get_culture_signal_service().collect(ticker)

        raw_reviews = [
            ReviewOut(
                ticker=r.get("ticker", ticker),
                source=r.get("source", "unknown"),
                review_id=r.get("review_id", ""),
                rating=r.get("rating"),
                title=r.get("title"),
                pros=r.get("pros"),
                cons=r.get("cons"),
                advice_to_management=r.get("advice_to_management"),
                is_current_employee=r.get("is_current_employee"),
                job_title=r.get("job_title"),
                review_date=r.get("review_date"),
            )
            for r in result.raw_reviews
        ]

        return CollectCultureResponse(
            ticker=ticker,
            status="success",
            review_count=len(raw_reviews),
            sources_collected=result.source_counts,
            s3_raw_key=result.raw_s3_key,
            s3_output_key=result.output_s3_key,
            snowflake_upserted=result.snowflake_ok,
            culture_scores={
                "overall_score": result.signal_dict.get("overall_score"),
                "innovation_score": result.signal_dict.get("innovation_score"),
                "data_driven_score": result.signal_dict.get("data_driven_score"),
                "ai_awareness_score": result.signal_dict.get("ai_awareness_score"),
                "change_readiness_score": result.signal_dict.get("change_readiness_score"),
            },
            raw_reviews=raw_reviews,
            duration_seconds=round(time.time() - start, 2),
        )

    except Exception as e:
        logger.error("Culture collection failed for %s: %s", ticker, e, exc_info=True)
        return CollectCultureResponse(
            ticker=ticker,
            status="failed",
            review_count=0,
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =====================================================================
# GET /api/v1/glassdoor-signals/{ticker} — Full score breakdown
# =====================================================================

@router.get(
    "/{ticker}",
    response_model=CultureSignalDetailOut,
    summary="Fetch full culture score breakdown for one company",
    description="""
    Returns the complete culture signal breakdown from S3 including:
    overall_score, innovation_score, data_driven_score, change_readiness_score,
    ai_awareness_score, keyword analysis, source breakdown, and more.
    """,
)
async def get_culture_signal(ticker: str):
    """Return the full Glassdoor culture signal breakdown for a single ticker."""
    ticker = ticker.upper()
    data, s3_key = get_culture_signal_service().get(ticker)

    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No culture signal found in S3 for ticker '{ticker}'. "
                   f"Run POST /api/v1/glassdoor-signals/{ticker} first to collect data.",
        )

    return CultureSignalDetailOut(
        ticker=ticker,
        company_id=data.get("company_id"),
        overall_score=data.get("overall_score"),
        innovation_score=data.get("innovation_score"),
        data_driven_score=data.get("data_driven_score"),
        change_readiness_score=data.get("change_readiness_score"),
        ai_awareness_score=data.get("ai_awareness_score"),
        review_count=data.get("review_count"),
        avg_rating=data.get("avg_rating"),
        current_employee_ratio=data.get("current_employee_ratio"),
        confidence=data.get("confidence"),
        source_breakdown=data.get("source_breakdown"),
        positive_keywords_found=data.get("positive_keywords_found"),
        negative_keywords_found=data.get("negative_keywords_found"),
        run_timestamp=data.get("run_timestamp"),
        s3_source=s3_key,
    )


# =====================================================================
# GET /api/v1/glassdoor-signals/portfolio/all — All 5 CS3 companies
# =====================================================================

@router.get(
    "/portfolio/all",
    response_model=PortfolioCultureResponse,
    summary="Fetch culture score breakdowns for all 5 CS3 portfolio companies",
    description="Returns the full culture signal breakdown for each of: NVDA, JPM, WMT, GE, DG.",
)
async def get_all_culture_signals():
    """Return culture signal breakdowns for the entire CS3 portfolio."""
    svc = get_culture_signal_service()
    results: List[CultureSignalDetailOut] = []
    summary: List[Dict[str, Any]] = []
    found = 0
    missing = 0

    for ticker in CS3_PORTFOLIO:
        data, s3_key = svc.get(ticker)

        if data is not None:
            found += 1
            detail = CultureSignalDetailOut(
                ticker=ticker,
                company_id=data.get("company_id"),
                overall_score=data.get("overall_score"),
                innovation_score=data.get("innovation_score"),
                data_driven_score=data.get("data_driven_score"),
                change_readiness_score=data.get("change_readiness_score"),
                ai_awareness_score=data.get("ai_awareness_score"),
                review_count=data.get("review_count"),
                avg_rating=data.get("avg_rating"),
                current_employee_ratio=data.get("current_employee_ratio"),
                confidence=data.get("confidence"),
                source_breakdown=data.get("source_breakdown"),
                positive_keywords_found=data.get("positive_keywords_found"),
                negative_keywords_found=data.get("negative_keywords_found"),
                run_timestamp=data.get("run_timestamp"),
                s3_source=s3_key,
            )
            results.append(detail)
            summary.append({
                "ticker": ticker,
                "overall_score": data.get("overall_score"),
                "innovation_score": data.get("innovation_score"),
                "data_driven_score": data.get("data_driven_score"),
                "ai_awareness_score": data.get("ai_awareness_score"),
                "change_readiness_score": data.get("change_readiness_score"),
                "review_count": data.get("review_count"),
                "avg_rating": data.get("avg_rating"),
                "confidence": data.get("confidence"),
            })
        else:
            missing += 1
            results.append(CultureSignalDetailOut(ticker=ticker))
            summary.append({"ticker": ticker, "status": "not_found"})

    return PortfolioCultureResponse(
        status="success" if missing == 0 else "partial",
        companies_found=found,
        companies_missing=missing,
        results=results,
        summary_table=summary,
    )
