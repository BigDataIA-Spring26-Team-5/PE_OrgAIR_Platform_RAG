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

import json
import logging
import time
from dataclasses import asdict
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.s3_storage import get_s3_service

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
# Helpers
# =====================================================================

def _load_latest_culture_json(ticker: str) -> tuple[Optional[Dict], Optional[str]]:
    """
    Load the latest culture signal JSON from S3 for a given ticker.

    Tries:
      1. glassdoor_signals/output/{TICKER}/ → pick the latest file by name sort
      2. glassdoor_signals/output/{TICKER}_culture.json (flat fallback)

    Returns (data_dict, s3_key) or (None, None).
    """
    s3 = get_s3_service()
    ticker_upper = ticker.upper()

    # Attempt 1: timestamped subfolder (latest file)
    prefix = f"glassdoor_signals/output/{ticker_upper}/"
    keys = s3.list_files(prefix)
    if keys:
        latest_key = sorted(keys)[-1]
        raw = s3.get_file(latest_key)
        if raw is not None:
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            return data, latest_key

    # Attempt 2: flat file
    flat_key = f"glassdoor_signals/output/{ticker_upper}_culture.json"
    raw = s3.get_file(flat_key)
    if raw is not None:
        data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        return data, flat_key

    return None, None


def _load_latest_raw_json(ticker: str) -> tuple[Optional[Dict], Optional[str]]:
    """Load the latest raw reviews JSON from S3."""
    s3 = get_s3_service()
    ticker_upper = ticker.upper()

    prefix = f"glassdoor_signals/raw/{ticker_upper}/"
    keys = s3.list_files(prefix)
    if keys:
        latest_key = sorted(keys)[-1]
        raw = s3.get_file(latest_key)
        if raw is not None:
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            return data, latest_key

    return None, None

def _upsert_culture_to_snowflake(ticker: str, signal_data: Dict) -> bool:
    """
    Upsert the glassdoor_reviews row into signal_dimension_mapping in Snowflake.

    CS3 Table 1 weights for glassdoor_reviews:
      talent_skills = 0.10, leadership_vision = 0.10, culture_change = 0.80
    """
    try:
        from app.repositories.scoring_repository import get_scoring_repository
        repo = get_scoring_repository()

        overall = signal_data.get("overall_score", 0)
        confidence = signal_data.get("confidence", 0)
        review_count = signal_data.get("review_count", 0)

        repo.upsert_mapping_row(
            ticker=ticker.upper(),
            source="glassdoor_reviews",
            raw_score=float(overall) if overall else None,
            confidence=float(confidence) if confidence else None,
            evidence_count=int(review_count),
            data_infrastructure=None,
            ai_governance=None,
            technology_stack=None,
            talent_skills=0.100,
            leadership_vision=0.100,
            use_case_portfolio=None,
            culture_change=0.800,
        )
        logger.info(f"[{ticker}] Upserted glassdoor_reviews to Snowflake signal_dimension_mapping")
        return True

    except Exception as e:
        logger.error(f"[{ticker}] Snowflake upsert failed: {e}", exc_info=True)
        return False

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
        from app.pipelines.glassdoor_collector import CultureCollector

        logger.info(f"{'=' * 60}")
        logger.info(f"GLASSDOOR COLLECTION: {ticker}")
        logger.info(f"{'=' * 60}")

        collector = CultureCollector()

        try:
            # Run full collection: scrape + analyze + upload to S3
            signal = collector.collect_and_analyze(
                ticker=ticker,
                sources=["glassdoor", "indeed", "careerbliss"],
                use_cache=True,
            )
        finally:
            collector.close_browser()

        # Convert signal to dict for scores
        signal_dict = {}
        for k, v in asdict(signal).items():
            signal_dict[k] = float(v) if isinstance(v, Decimal) else v

        # Upsert to Snowflake
        sf_ok = _upsert_culture_to_snowflake(ticker, signal_dict)

        # Load the raw data that was just uploaded to S3
        raw_data, raw_s3_key = _load_latest_raw_json(ticker)
        raw_reviews: List[ReviewOut] = []
        source_counts: Dict[str, int] = {}

        if raw_data and "reviews" in raw_data:
            for r in raw_data["reviews"]:
                raw_reviews.append(ReviewOut(
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
                ))
                src = r.get("source", "unknown")
                source_counts[src] = source_counts.get(src, 0) + 1

        # Find the output S3 key
        _, output_s3_key = _load_latest_culture_json(ticker)

        return CollectCultureResponse(
            ticker=ticker,
            status="success",
            review_count=len(raw_reviews),
            sources_collected=source_counts,
            s3_raw_key=raw_s3_key,
            s3_output_key=output_s3_key,
            snowflake_upserted=sf_ok,
            culture_scores={
                "overall_score": signal_dict.get("overall_score"),
                "innovation_score": signal_dict.get("innovation_score"),
                "data_driven_score": signal_dict.get("data_driven_score"),
                "ai_awareness_score": signal_dict.get("ai_awareness_score"),
                "change_readiness_score": signal_dict.get("change_readiness_score"),
            },
            raw_reviews=raw_reviews,
            duration_seconds=round(time.time() - start, 2),
        )

    except Exception as e:
        logger.error(f"Culture collection failed for {ticker}: {e}", exc_info=True)
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
    data, s3_key = _load_latest_culture_json(ticker)

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
    results: List[CultureSignalDetailOut] = []
    summary: List[Dict[str, Any]] = []
    found = 0
    missing = 0

    for ticker in CS3_PORTFOLIO:
        data, s3_key = _load_latest_culture_json(ticker)

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
