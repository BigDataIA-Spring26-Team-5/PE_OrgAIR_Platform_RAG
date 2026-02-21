# """
# Signals API Router
# app/routers/signals.py
#
# Endpoints:
#   DELETE /api/v1/signals/reset                        - Delete all signals for all companies
#   DELETE /api/v1/signals/reset/{ticker}               - Delete all signals for a company
#   DELETE /api/v1/signals/reset/{ticker}/{category}    - Delete signals by category
#   POST  /api/v1/signals/collect                       - Trigger signal collection (background)
#   GET   /api/v1/signals/tasks/{task_id}               - Get task status
#   GET   /api/v1/signals/detailed                      - List signals (filterable)
#   POST  /api/v1/signals/score/{ticker}/hiring         - Score technology hiring
#   POST  /api/v1/signals/score/{ticker}/digital        - Score digital presence
#   POST  /api/v1/signals/score/{ticker}/innovation     - Score innovation activity
#   POST  /api/v1/signals/score/{ticker}/leadership     - Score leadership signals
#   POST  /api/v1/signals/score/{ticker}/all            - Score all categories
#   GET   /api/v1/signals/{ticker}/current-scores       - Get current scores
# """

import logging
import os
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

import boto3
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import get_company_search_name, get_job_search_names
from app.repositories.company_repository import CompanyRepository
from app.repositories.signal_repository import get_signal_repository
from app.repositories.signal_scores_repository import SignalScoresRepository
from app.services.job_data_service import get_job_data_service
from app.services.job_signal_service import get_job_signal_service
from app.services.leadership_service import get_leadership_service
from app.services.patent_signal_service import get_patent_signal_service
from app.services.s3_storage import get_s3_service
from app.services.tech_signal_service import get_tech_signal_service

logger = logging.getLogger(__name__)

# In-memory task status store (in production, use Redis or database)
_task_store: Dict[str, Dict[str, Any]] = {}

# S3 config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "pe-orgair-platform-group5")


# =============================================================================
# S3 Helpers
# =============================================================================

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def delete_s3_prefix(prefix: str) -> int:
    """Delete all S3 objects under a given prefix. Returns count deleted."""
    s3 = get_s3_client()
    deleted_count = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects = page.get("Contents", [])
            if not objects:
                continue
            delete_keys = [{"Key": obj["Key"]} for obj in objects]
            s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": delete_keys})
            deleted_count += len(delete_keys)
            logger.info(f"Deleted {len(delete_keys)} S3 objects under prefix '{prefix}'")
    except Exception as e:
        logger.error(f"Error deleting S3 prefix '{prefix}': {e}")
    return deleted_count


# =============================================================================
# Enums & Models
# =============================================================================

class SignalCategory(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


VALID_CATEGORIES = [c.value for c in SignalCategory]

COMPOSITE_WEIGHTS = {
    "technology_hiring": 0.30,
    "innovation_activity": 0.25,
    "digital_presence": 0.25,
    "leadership_signals": 0.20,
}


class CollectionRequest(BaseModel):
    company_id: str = Field(..., description="Company ID or ticker symbol")
    categories: List[str] = Field(
        default=VALID_CATEGORIES,
        description="Signal categories to collect",
    )
    years_back: int = Field(default=5, ge=1, le=10, description="Years back for patent search")
    force_refresh: bool = Field(default=False, description="Force refresh cached data")


class CollectionResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SingleSignalResponse(BaseModel):
    ticker: str
    category: str
    status: str
    score: Optional[float] = None
    confidence: Optional[float] = None
    breakdown: Optional[Dict[str, Any]] = None
    data_source: Optional[str] = None
    evidence_count: Optional[int] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


class AllSignalsResponse(BaseModel):
    ticker: str
    company_name: Optional[str] = None
    results: Dict[str, SingleSignalResponse]
    composite_score: Optional[float] = None
    total_duration_seconds: Optional[float] = None


class CompanyScoreStatus(BaseModel):
    ticker: str
    company_name: Optional[str] = None
    company_id: Optional[str] = None
    technology_hiring: Optional[Dict[str, Any]] = None
    digital_presence: Optional[Dict[str, Any]] = None
    innovation_activity: Optional[Dict[str, Any]] = None
    leadership_signals: Optional[Dict[str, Any]] = None
    composite_score: Optional[float] = None
    last_updated: Optional[str] = None


# =============================================================================
# Router
# =============================================================================

router = APIRouter(prefix="/api/v1", tags=["Signals"])


# =============================================================================
# Helper: look up company or 404
# =============================================================================

def _get_company_or_404(ticker: str) -> dict:
    company = CompanyRepository().get_by_ticker(ticker.upper())
    if not company:
        raise HTTPException(status_code=404, detail=f"Company not found: {ticker}")
    return company


# =============================================================================
# Reset Endpoints
# =============================================================================

@router.delete(
    "/signals/reset",
    summary="Delete all signals for all companies",
    description="Deletes all signal records from Snowflake and all signal-related files from S3.",
)
async def reset_all_signals():
    """Delete all signals for all companies from Snowflake and S3."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()

    # Snowflake: delete all signals
    snowflake_deleted = 0
    try:
        if hasattr(repo, "delete_all"):
            snowflake_deleted = repo.delete_all() or 0
        else:
            for company in company_repo.get_all():
                cid = str(company.get("id"))
                if hasattr(repo, "delete_by_company"):
                    snowflake_deleted += repo.delete_by_company(cid) or 0
    except Exception as e:
        logger.error(f"Error deleting signals from Snowflake: {e}")

    # Snowflake: delete all summaries
    summary_deleted = 0
    try:
        if hasattr(repo, "delete_all_summaries"):
            summary_deleted = repo.delete_all_summaries() or 0
    except Exception as e:
        logger.error(f"Error deleting signal summaries: {e}")

    # S3: delete all signal-related files
    s3_deleted = 0
    for prefix in ["signals/", "sec/signals/"]:
        s3_deleted += delete_s3_prefix(prefix)

    logger.info(f"Reset all signals: snowflake={snowflake_deleted}, summaries={summary_deleted}, s3={s3_deleted}")
    return {
        "message": "All signal data deleted for all companies",
        "snowflake_signals_deleted": snowflake_deleted,
        "snowflake_summaries_deleted": summary_deleted,
        "s3_objects_deleted": s3_deleted,
    }


@router.delete(
    "/signals/reset/{ticker}",
    summary="Delete all signals for a company",
    description=(
        "Deletes all signal data for a company across Snowflake and S3.\n\n"
        "**Snowflake tables cleared:** EXTERNAL_SIGNALS, COMPANY_SIGNAL_SUMMARIES, SIGNAL_SCORES\n\n"
        "**S3 prefixes cleared:** signals/jobs/{TICKER}/, signals/patents/{TICKER}/, signals/techstack/{TICKER}/"
    ),
)
async def reset_signals_by_ticker(ticker: str):
    """Delete all signals for a specific company."""
    ticker = ticker.upper()
    company = _get_company_or_404(ticker)
    company_id = str(company["id"])

    result = {"ticker": ticker, "snowflake": {}, "s3": {"deleted_keys": [], "errors": []}}

    # Snowflake
    signal_repo = get_signal_repository()
    scores_repo = SignalScoresRepository()

    result["snowflake"]["external_signals_deleted"] = signal_repo.delete_signals_by_company(company_id)
    result["snowflake"]["signal_summary_deleted"] = signal_repo.delete_summary(company_id)
    result["snowflake"]["signal_scores_deleted"] = scores_repo.delete_by_ticker(ticker)

    # S3
    s3_prefixes = [
        f"signals/jobs/{ticker}/",
        f"signals/patents/{ticker}/",
        f"signals/techstack/{ticker}/",
    ]
    try:
        s3 = get_s3_service()
        for prefix in s3_prefixes:
            for key in s3.list_files(prefix):
                if s3.delete_file(key):
                    result["s3"]["deleted_keys"].append(key)
                else:
                    result["s3"]["errors"].append(key)
    except Exception as e:
        logger.error(f"S3 cleanup error for {ticker}: {e}")
        result["s3"]["errors"].append(str(e))

    sf = result["snowflake"]
    logger.info(
        f"Signal reset for {ticker}: "
        f"snowflake(signals={sf['external_signals_deleted']}, "
        f"summary={sf['signal_summary_deleted']}, "
        f"scores={sf['signal_scores_deleted']}) "
        f"s3({len(result['s3']['deleted_keys'])} files deleted)"
    )
    return result


@router.delete(
    "/signals/reset/{ticker}/{category}",
    summary="Delete signals by category for a company",
    description="Deletes signal records for a specific category from Snowflake and S3.",
)
async def reset_signals_by_category(ticker: str, category: str):
    """Delete signals for a company filtered by category."""
    if category not in VALID_CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category: {category}. Valid: {VALID_CATEGORIES}",
        )

    ticker = ticker.upper()
    company = _get_company_or_404(ticker)
    company_id = str(company["id"])

    # Snowflake
    repo = get_signal_repository()
    snowflake_deleted = 0
    try:
        if hasattr(repo, "delete_by_category"):
            snowflake_deleted = repo.delete_by_category(company_id, category) or 0
    except Exception as e:
        logger.error(f"Error deleting {category} signals for {ticker}: {e}")

    # S3
    s3_deleted = 0
    for prefix in [f"signals/{ticker}/{category}/", f"sec/signals/{ticker}/{category}/"]:
        s3_deleted += delete_s3_prefix(prefix)

    logger.info(f"Reset {category} signals for {ticker}: snowflake={snowflake_deleted}, s3={s3_deleted}")
    return {
        "ticker": ticker,
        "company_name": company.get("name", ticker),
        "category": category,
        "message": f"{category} signals deleted for {ticker}",
        "snowflake_signals_deleted": snowflake_deleted,
        "s3_objects_deleted": s3_deleted,
    }


# =============================================================================
# Collection (Background Task)
# =============================================================================

@router.post(
    "/signals/collect",
    response_model=CollectionResponse,
    summary="Trigger signal collection for a company",
    description=(
        "Trigger signal collection for a company. Runs asynchronously in the background.\n\n"
        "**Categories:** technology_hiring, innovation_activity, digital_presence, leadership_signals\n\n"
        "Returns a task_id to check status via GET /api/v1/signals/tasks/{task_id}"
    ),
)
async def collect_signals(request: CollectionRequest, background_tasks: BackgroundTasks):
    """Trigger signal collection for a company."""
    task_id = str(uuid4())
    _task_store[task_id] = {
        "task_id": task_id,
        "status": "queued",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "progress": {
            "total_categories": len(request.categories),
            "completed_categories": 0,
            "current_category": None,
        },
        "result": None,
        "error": None,
    }

    background_tasks.add_task(
        run_signal_collection,
        task_id=task_id,
        company_id=request.company_id,
        categories=request.categories,
        years_back=request.years_back,
        force_refresh=request.force_refresh,
    )

    logger.info(f"Signal collection queued: task_id={task_id}, company={request.company_id}")
    return CollectionResponse(
        task_id=task_id,
        status="queued",
        message=f"Signal collection started for company {request.company_id}",
    )


async def run_signal_collection(
    task_id: str,
    company_id: str,
    categories: List[str],
    years_back: int,
    force_refresh: bool,
):
    """Background task for signal collection."""
    logger.info(f"Starting signal collection: task_id={task_id}, company={company_id}")
    _task_store[task_id]["status"] = "running"

    company_repo = CompanyRepository()
    company = company_repo.get_by_ticker(company_id.upper())
    if not company:
        companies = company_repo.get_all()
        company = next((c for c in companies if str(c.get("id")) == company_id), None)

    if not company:
        _task_store[task_id]["status"] = "failed"
        _task_store[task_id]["error"] = f"Company not found: {company_id}"
        _task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        return

    ticker = company.get("ticker")
    result = {
        "company_id": str(company.get("id")),
        "company_name": company.get("name"),
        "ticker": ticker,
        "signals": {},
        "errors": [],
    }

    category_handlers = {
        "technology_hiring": lambda: get_job_data_service().analyze_company(ticker, force_refresh=force_refresh),
        "innovation_activity": lambda: get_patent_signal_service().analyze_company(ticker, years_back=years_back),
        "digital_presence": lambda: get_tech_signal_service().analyze_company(ticker, force_refresh=force_refresh),
        "leadership_signals": lambda: get_leadership_service().analyze_company(ticker),
    }

    for i, category in enumerate(categories):
        _task_store[task_id]["progress"]["current_category"] = category
        try:
            handler = category_handlers.get(category)
            if handler:
                signal_result = await handler()
                result["signals"][category] = {
                    "status": "success",
                    "score": signal_result.get("normalized_score"),
                    "details": signal_result,
                }
        except Exception as e:
            logger.error(f"Error collecting {category} signals: {e}")
            result["signals"][category] = {"status": "failed", "error": str(e)}
            result["errors"].append(f"{category}: {str(e)}")

        _task_store[task_id]["progress"]["completed_categories"] = i + 1

    _task_store[task_id]["status"] = "completed" if not result["errors"] else "completed_with_errors"
    _task_store[task_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
    _task_store[task_id]["result"] = result
    _task_store[task_id]["progress"]["current_category"] = None
    logger.info(f"Signal collection completed: task_id={task_id}")


# =============================================================================
# List / Query Signals
# =============================================================================

@router.get(
    "/signals/detailed",
    summary="List signals with details (filterable)",
    description="List all signals with optional filters by category, ticker, min_score, and limit.",
)
async def list_signals(
    category: Optional[str] = Query(None, description="Filter by category"),
    ticker: Optional[str] = Query(None, description="Filter by company ticker"),
    min_score: Optional[float] = Query(None, ge=0, le=100, description="Minimum score"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
):
    """List signals with optional filters."""
    repo = get_signal_repository()
    company_repo = CompanyRepository()
    results = []

    if ticker:
        company = _get_company_or_404(ticker)
        company_id = str(company["id"])
        results = (
            repo.get_signals_by_category(company_id, category)
            if category
            else repo.get_signals_by_company(company_id)
        )
    else:
        for company in company_repo.get_all():
            company_id = str(company.get("id"))
            signals = (
                repo.get_signals_by_category(company_id, category)
                if category
                else repo.get_signals_by_company(company_id)
            )
            results.extend(signals)

    if min_score is not None:
        results = [s for s in results if (s.get("normalized_score") or 0) >= min_score]

    results = results[:limit]
    return {
        "total": len(results),
        "filters": {"category": category, "ticker": ticker, "min_score": min_score},
        "signals": results,
    }


# =============================================================================
# Individual Signal Scoring
# =============================================================================

@router.post(
    "/signals/score/{ticker}/hiring",
    response_model=SingleSignalResponse,
    summary="Score technology hiring for a company",
    description=(
        "Run the technology_hiring signal pipeline.\n\n"
        "**Source:** Job postings from LinkedIn, Indeed, Glassdoor (via JobSpy)\n\n"
        "**CS3 feeds:** Talent (0.70), Tech Stack (0.20), Culture (0.10)"
    ),
)
async def score_hiring(ticker: str, force_refresh: bool = False):
    """Score technology hiring signal for one company."""
    start = time.time()
    try:
        service = get_job_signal_service()
        result = await service.analyze_company(ticker.upper(), force_refresh=force_refresh)
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="technology_hiring",
            status="success",
            score=result.get("normalized_score"),
            confidence=result.get("confidence"),
            breakdown=result.get("breakdown"),
            data_source="jobspy (linkedin, indeed, glassdoor)",
            evidence_count=result.get("job_postings_analyzed", 0),
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Hiring scoring failed for {ticker}: {e}")
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="technology_hiring",
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


@router.post(
    "/signals/score/{ticker}/digital",
    response_model=SingleSignalResponse,
    summary="Score digital presence for a company",
    description=(
        "Run the digital_presence signal pipeline.\n\n"
        "**Source:** BuiltWith Free API + Wappalyzer\n\n"
        "**CS3 feeds:** Data Infrastructure (0.60), Technology Stack (0.40)"
    ),
)
async def score_digital_presence(ticker: str, force_refresh: bool = False):
    """Score digital presence signal for one company."""
    start = time.time()
    try:
        service = get_tech_signal_service()
        result = await service.analyze_company(ticker.upper(), force_refresh=force_refresh)
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="digital_presence",
            status="success",
            score=result.get("normalized_score"),
            confidence=result.get("confidence"),
            breakdown=result.get("breakdown"),
            data_source=", ".join(result.get("data_sources", ["builtwith", "wappalyzer"])),
            evidence_count=result.get("tech_metrics", {}).get("total_technologies", 0),
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Digital presence scoring failed for {ticker}: {e}")
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="digital_presence",
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


@router.post(
    "/signals/score/{ticker}/innovation",
    response_model=SingleSignalResponse,
    summary="Score innovation activity for a company",
    description=(
        "Run the innovation_activity signal pipeline.\n\n"
        "**Source:** PatentsView API (USPTO patent data)\n\n"
        "**CS3 feeds:** Technology Stack (0.50), Use Case Portfolio (0.30), Data Infra (0.20)"
    ),
)
async def score_innovation(ticker: str, years_back: int = 5):
    """Score innovation activity signal for one company."""
    start = time.time()
    try:
        service = get_patent_signal_service()
        result = await service.analyze_company(ticker.upper(), years_back=years_back)
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="innovation_activity",
            status="success",
            score=result.get("normalized_score"),
            confidence=result.get("confidence"),
            breakdown=result.get("breakdown"),
            data_source="patentsview (USPTO)",
            evidence_count=result.get("patent_metrics", {}).get(
                "total_patents", result.get("total_patents", 0)
            ),
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Innovation scoring failed for {ticker}: {e}")
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="innovation_activity",
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


@router.post(
    "/signals/score/{ticker}/leadership",
    response_model=SingleSignalResponse,
    summary="Score leadership signals for a company",
    description=(
        "Run the leadership_signals pipeline.\n\n"
        "**Source:** SEC DEF-14A proxy statements\n\n"
        "**CS3 feeds:** Leadership (0.60), AI Governance (0.25), Culture (0.15)"
    ),
)
async def score_leadership(ticker: str):
    """Score leadership signals for one company."""
    start = time.time()
    try:
        service = get_leadership_service()
        result = await service.analyze_company(ticker.upper())
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="leadership_signals",
            status="success",
            score=result.get("normalized_score"),
            confidence=result.get("confidence"),
            breakdown=result.get("breakdown"),
            data_source="sec_edgar (DEF 14A proxy statements)",
            evidence_count=result.get("filing_count_analyzed", result.get("filings_analyzed", 0)),
            duration_seconds=round(time.time() - start, 2),
        )
    except Exception as e:
        logger.error(f"Leadership scoring failed for {ticker}: {e}")
        return SingleSignalResponse(
            ticker=ticker.upper(),
            category="leadership_signals",
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


# =============================================================================
# Score All + Status
# =============================================================================

@router.post(
    "/signals/score/{ticker}/all",
    response_model=AllSignalsResponse,
    summary="Score ALL signal categories for a company",
    description=(
        "Run all 4 signal pipelines sequentially.\n\n"
        "**Composite = 0.30×hiring + 0.25×innovation + 0.25×digital + 0.20×leadership**"
    ),
)
async def score_all_signals(ticker: str, force_refresh: bool = False):
    """Score all signal categories for one company."""
    overall_start = time.time()
    ticker = ticker.upper()

    company = CompanyRepository().get_by_ticker(ticker)
    company_name = company.get("name", ticker) if company else ticker

    results = {}

    logger.info(f"{'=' * 60}")
    logger.info(f"SCORING ALL SIGNALS FOR: {ticker}")
    logger.info(f"{'=' * 60}")

    logger.info("[1/4] Technology Hiring...")
    results["technology_hiring"] = await score_hiring(ticker, force_refresh)

    logger.info("[2/4] Digital Presence...")
    results["digital_presence"] = await score_digital_presence(ticker, force_refresh)

    logger.info("[3/4] Innovation Activity...")
    results["innovation_activity"] = await score_innovation(ticker)

    logger.info("[4/4] Leadership Signals...")
    results["leadership_signals"] = await score_leadership(ticker)

    # Calculate composite
    scores = {cat: results[cat].score for cat in COMPOSITE_WEIGHTS}
    composite = None
    if all(s is not None for s in scores.values()):
        composite = round(
            sum(COMPOSITE_WEIGHTS[cat] * scores[cat] for cat in COMPOSITE_WEIGHTS), 2
        )

    # Persist summary
    if company:
        try:
            get_signal_repository().upsert_summary(
                company_id=str(company["id"]),
                ticker=ticker,
                hiring_score=scores["technology_hiring"],
                digital_score=scores["digital_presence"],
                innovation_score=scores["innovation_activity"],
                leadership_score=scores["leadership_signals"],
            )
        except Exception as e:
            logger.warning(f"Failed to update composite summary: {e}")

    total_duration = round(time.time() - overall_start, 2)

    logger.info(f"{'=' * 60}")
    logger.info(f"ALL SIGNALS COMPLETE FOR: {ticker}")
    for cat, score in scores.items():
        logger.info(f"  {cat}: {score}")
    logger.info(f"  COMPOSITE: {composite}")
    logger.info(f"  Duration: {total_duration}s")
    logger.info(f"{'=' * 60}")

    return AllSignalsResponse(
        ticker=ticker,
        company_name=company_name,
        results=results,
        composite_score=composite,
        total_duration_seconds=total_duration,
    )


@router.get(
    "/signals/{ticker}/current-scores",
    response_model=CompanyScoreStatus,
    summary="Get current scores for a company",
    description="Get the latest stored scores for all signal categories (does NOT trigger new collection).",
)
async def get_score_status(ticker: str):
    """Get current signal scores for a company."""
    ticker = ticker.upper()
    company = _get_company_or_404(ticker)
    company_id = str(company["id"])

    signal_repo = get_signal_repository()
    summary = signal_repo.get_summary_by_ticker(ticker)

    category_data = {}
    for cat in VALID_CATEGORIES:
        signals = signal_repo.get_signals_by_category(company_id, cat)
        if signals:
            latest = signals[0]
            category_data[cat] = {
                "score": latest.get("normalized_score"),
                "confidence": latest.get("confidence"),
                "source": latest.get("source"),
                "signal_date": latest.get("signal_date"),
                "evidence_count": latest.get("evidence_count"),
                "raw_value": latest.get("raw_value"),
            }
        else:
            category_data[cat] = None

    return CompanyScoreStatus(
        ticker=ticker,
        company_name=company.get("name"),
        company_id=company_id,
        technology_hiring=category_data.get("technology_hiring"),
        digital_presence=category_data.get("digital_presence"),
        innovation_activity=category_data.get("innovation_activity"),
        leadership_signals=category_data.get("leadership_signals"),
        composite_score=summary.get("composite_score") if summary else None,
        last_updated=summary.get("updated_at") if summary else None,
    )