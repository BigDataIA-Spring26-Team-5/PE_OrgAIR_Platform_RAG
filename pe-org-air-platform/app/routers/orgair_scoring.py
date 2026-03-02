"""
routers/orgair_scoring.py — CS3 Task 6.4 Endpoints

Endpoints:
  POST /api/v1/scoring/orgair/{ticker}        — Compute Org-AI-R for one company
  POST /api/v1/scoring/orgair/portfolio       — Compute Org-AI-R for all 5 CS3 companies
  POST /api/v1/scoring/orgair/results         — Generate results/*.json for submission
  GET  /api/v1/scoring/orgair/portfolio       — Read portfolio from Snowflake
  GET  /api/v1/scoring/orgair/{ticker}        — Read one from Snowflake
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import logging
import time

from app.repositories.composite_scoring_repository import get_composite_scoring_repo
from app.services.composite_scoring_service import (
    get_composite_scoring_service,
    OrgAIRResponse,
    OrgAIRBreakdown,
    OrgAIRValidation,
    CIBreakdown,
    CS3_PORTFOLIO,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 Org-AI-R"])


# =====================================================================
# Response Models (portfolio + GET — stay in router)
# =====================================================================

class PortfolioOrgAIRResponse(BaseModel):
    status: str
    companies_scored: int
    companies_failed: int
    results: List[OrgAIRResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


class ResultsGenerationResponse(BaseModel):
    status: str
    files_generated: int
    local_files: List[str]
    s3_files: List[str]
    summary: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# POST /api/v1/scoring/orgair/results — Generate results/*.json
# =====================================================================

@router.post(
    "/orgair/results",
    response_model=ResultsGenerationResponse,
    summary="Generate results/*.json files for CS3 submission",
    description="""
    Runs the full Org-AI-R pipeline for all 5 companies, then generates
    individual JSON result files (nvda.json, jpm.json, etc.) saved both
    locally in results/ and to S3 under scoring/results/.

    Each JSON contains: final Org-AI-R score, V^R, H^R, synergy,
    7 dimension scores, TC, PF, confidence intervals, job analysis,
    and validation against CS3 Table 5 expected ranges.
    """,
)
async def generate_results():
    """Generate results JSON files for CS3 submission."""
    result = get_composite_scoring_service().compute_full_pipeline(CS3_PORTFOLIO)
    return ResultsGenerationResponse(
        status="success",
        files_generated=result["files_generated"],
        local_files=result["local_files"],
        s3_files=result["s3_files"],
        summary=result["summary"],
        duration_seconds=result["duration_seconds"],
    )


# =====================================================================
# POST /api/v1/scoring/orgair/portfolio
# =====================================================================

@router.post(
    "/orgair/portfolio",
    response_model=PortfolioOrgAIRResponse,
    summary="Calculate Org-AI-R for all 5 CS3 portfolio companies",
)
async def score_portfolio_orgair():
    """Calculate Org-AI-R for all 5 companies."""
    start = time.time()
    svc = get_composite_scoring_service()

    logger.info("=" * 70)
    logger.info("Org-AI-R PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = svc.compute_orgair(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1

    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("Org-AI-R SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(
        f"{'Ticker':<8} {'V^R':>8} {'H^R':>8} {'Synergy':>9} "
        f"{'Org-AI-R':>10} {'Range':>15} {'✓':>3}"
    )
    logger.info("-" * 70)

    for r in results:
        if r.status == "success" and r.breakdown:
            b = r.breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.orgair_expected if r.validation else "—"
            logger.info(
                f"{r.ticker:<8} {b.vr_score:>8.2f} {b.hr_score:>8.2f} "
                f"{b.synergy_score:>9.2f} {b.org_air_score:>10.2f} "
                f"{range_str:>15} {val_status:>3}"
            )
            summary.append({
                "ticker": r.ticker,
                "vr_score": b.vr_score,
                "hr_score": b.hr_score,
                "synergy_score": b.synergy_score,
                "org_air_score": b.org_air_score,
                "weighted_base": b.weighted_base,
                "synergy_contribution": b.synergy_contribution,
                "orgair_in_expected_range": (
                    r.validation.orgair_in_range if r.validation else None
                ),
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})

    logger.info("-" * 70)
    orgair_pass = sum(1 for r in results if r.validation and r.validation.orgair_in_range)
    orgair_total = sum(1 for r in results if r.validation)
    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"Org-AI-R Validation: {orgair_pass}/{orgair_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)

    return PortfolioOrgAIRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/orgair/{ticker}
# =====================================================================

@router.post(
    "/orgair/{ticker}",
    response_model=OrgAIRResponse,
    summary="Calculate Org-AI-R for one company",
)
async def score_orgair(ticker: str):
    """Calculate Org-AI-R for one company."""
    return get_composite_scoring_service().compute_orgair(ticker.upper())


# =====================================================================
# GET endpoints (Snowflake reads)
# =====================================================================

class OrgAIRScoringRecord(BaseModel):
    ticker: str
    org_air: Optional[float] = None
    scored_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioOrgAIRScoringResponse(BaseModel):
    status: str
    results: List[OrgAIRScoringRecord]
    message: Optional[str] = None


def _fetch_orgair_row(ticker: str) -> Optional[OrgAIRScoringRecord]:
    row = get_composite_scoring_repo().fetch_orgair_row(ticker)
    if not row:
        return None
    return OrgAIRScoringRecord(
        ticker=row["TICKER"],
        org_air=row.get("ORG_AIR"),
        scored_at=str(row["SCORED_AT"]) if row.get("SCORED_AT") else None,
        updated_at=str(row["UPDATED_AT"]) if row.get("UPDATED_AT") else None,
    )


@router.get(
    "/orgair/portfolio",
    response_model=PortfolioOrgAIRScoringResponse,
    summary="Get last computed Org-AI-R for all 5 CS3 companies (from Snowflake)",
)
async def get_portfolio_orgair():
    results = []
    for ticker in CS3_PORTFOLIO:
        try:
            row = _fetch_orgair_row(ticker)
            results.append(row if row else OrgAIRScoringRecord(ticker=ticker))
        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch SCORING row: {e}")
            results.append(OrgAIRScoringRecord(ticker=ticker))

    scored = sum(1 for r in results if r.org_air is not None)
    return PortfolioOrgAIRScoringResponse(
        status="ok",
        results=results,
        message=f"{scored}/{len(CS3_PORTFOLIO)} companies have stored Org-AI-R scores",
    )


@router.get(
    "/orgair/{ticker}",
    response_model=OrgAIRScoringRecord,
    summary="Get last computed Org-AI-R for one company (from Snowflake)",
)
async def get_orgair(ticker: str):
    row = _fetch_orgair_row(ticker.upper())
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No scoring record for {ticker.upper()}. Run POST first.",
        )
    return row
