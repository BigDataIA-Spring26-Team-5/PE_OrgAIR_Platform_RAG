"""
routers/hr_scoring.py — CS3 Task 6.1 Endpoints

Endpoints:
  POST /api/v1/scoring/hr/{ticker}        — Compute H^R for one company
  POST /api/v1/scoring/hr/portfolio       — Compute H^R for all 5 companies
  POST /api/v1/scoring/hr/portfolio/report — Download report as .md

Already registered in main.py as hr_router.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import logging
import time
from fastapi.responses import StreamingResponse
import io

from app.repositories.composite_scoring_repository import get_composite_scoring_repo
from app.services.composite_scoring_service import (
    get_composite_scoring_service,
    HRResponse,
    HRBreakdown,
    HRValidation,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 H^R (Human Readiness)"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]


# =====================================================================
# Response Models (portfolio + GET — stay in router)
# =====================================================================

class PortfolioHRResponse(BaseModel):
    """Portfolio H^R response."""
    status: str
    companies_scored: int
    companies_failed: int
    results: List[HRResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# POST /api/v1/scoring/hr/portfolio — Calculate H^R for all 5 companies
# =====================================================================

@router.post(
    "/hr/portfolio",
    response_model=PortfolioHRResponse,
    summary="Calculate H^R for all 5 CS3 portfolio companies",
    description="""
    Runs Task 6.1 (H^R calculation) for all 5 companies: NVDA, JPM, WMT, GE, DG.

    Pipeline for each company:
    1. Get Position Factor from PF endpoint (Task 6.0a)
    2. Get sector baseline H^R
    3. Calculate H^R = HR_base × (1 + 0.15 × PF)
    4. Validate against expected ranges

    Returns individual breakdowns + summary comparison table.
    """,
)
async def score_portfolio_hr():
    """Calculate H^R for all 5 companies."""
    start = time.time()
    svc = get_composite_scoring_service()

    logger.info("=" * 70)
    logger.info("🚀 H^R PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = svc.compute_hr(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1

    # Build summary table
    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 H^R SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(
        f"{'Ticker':<8} {'Sector':<20} {'HR Base':>9} {'PF':>8} "
        f"{'Adj':>8} {'H^R':>8} {'Range':>15} {'✓':>3}"
    )
    logger.info("-" * 70)

    for r in results:
        if r.status == "success" and r.hr_breakdown:
            b = r.hr_breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.hr_expected if r.validation else "—"

            logger.info(
                f"{r.ticker:<8} {b.sector:<20} {b.hr_base:>9.2f} "
                f"{b.position_factor:>8.4f} {b.position_adjustment:>8.4f} "
                f"{b.hr_score:>8.2f} {range_str:>15} {val_status:>3}"
            )

            summary.append({
                "ticker": r.ticker,
                "sector": b.sector,
                "hr_base": b.hr_base,
                "position_factor": b.position_factor,
                "position_adjustment": b.position_adjustment,
                "hr_score": b.hr_score,
                "interpretation": b.interpretation,
                "hr_in_expected_range": r.validation.hr_in_range if r.validation else None,
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})

    logger.info("-" * 70)

    hr_pass = sum(1 for r in results if r.validation and r.validation.hr_in_range)
    hr_total = sum(1 for r in results if r.validation)

    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"H^R Validation: {hr_pass}/{hr_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)

    return PortfolioHRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/hr/{ticker} — Calculate H^R for one company
# =====================================================================

@router.post(
    "/hr/{ticker}",
    response_model=HRResponse,
    summary="Calculate H^R for one company",
    description="""
    Runs Task 6.1 (H^R calculation) for a single ticker.

    Pipeline:
    1. Get Position Factor from PF endpoint (Task 6.0a)
    2. Get sector baseline H^R
    3. Calculate H^R = HR_base × (1 + 0.15 × PF)
    4. Validate against expected range

    H^R interpretation:
    - 75-100: Highly Ready
    - 60-75: Moderately Ready
    - 45-60: Developing
    - 0-45: Not Ready
    """,
)
async def score_hr(ticker: str):
    """Calculate H^R for one company. Saves to S3 + Snowflake SCORING table."""
    return get_composite_scoring_service().compute_hr(ticker.upper())


# =====================================================================
# POST /api/v1/scoring/hr/portfolio/report — Download MD Report
# =====================================================================

def _generate_hr_report(portfolio: PortfolioHRResponse) -> str:
    """Generate a markdown report from H^R results."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = []
    lines.append("# H^R (Human Readiness) Scoring — CS3 Portfolio Report")
    lines.append("")
    lines.append(f"**Generated:** {now}")
    lines.append(
        f"**Companies:** {portfolio.companies_scored} scored, "
        f"{portfolio.companies_failed} failed"
    )
    lines.append(f"**Duration:** {portfolio.duration_seconds}s")
    lines.append("")

    # ---- Summary Table ----
    lines.append("## Portfolio Summary Table")
    lines.append("")
    lines.append(
        "| Ticker | Sector | HR Base | PF | Adj (δ×PF) | H^R | Expected Range | Status |"
    )
    lines.append("|--------|--------|---------|-----|-----------|------|----------------|--------|")

    hr_pass = 0
    hr_total = 0

    for r in portfolio.results:
        if r.status != "success":
            lines.append(f"| {r.ticker} | — | — | — | — | — | — | ❌ |")
            continue

        b = r.hr_breakdown
        val = r.validation

        if val:
            hr_total += 1
            if val.hr_in_range:
                hr_pass += 1

        status = val.status if val else "—"
        range_str = val.hr_expected if val else "—"

        lines.append(
            f"| {r.ticker} | {b.sector} | {b.hr_base:.1f} | {b.position_factor:.4f} "
            f"| {b.position_adjustment:.4f} | {b.hr_score:.2f} | {range_str} | {status} |"
        )

    lines.append("")

    # ---- Scorecard ----
    lines.append("## Validation Scorecard")
    lines.append("")
    lines.append(f"- **H^R:** {hr_pass}/{hr_total} ✅ within expected range")
    lines.append("")

    # ---- Interpretation ----
    lines.append("## H^R Interpretation by Company")
    lines.append("")

    for r in portfolio.results:
        if r.status == "success" and r.hr_breakdown:
            b = r.hr_breakdown
            lines.append(f"### {r.ticker} — H^R = {b.hr_score:.2f}")
            lines.append(f"**{b.interpretation}**")
            lines.append("")
            lines.append(f"- Sector: {b.sector}")
            lines.append(f"- Base readiness: {b.hr_base:.1f}")
            lines.append(
                f"- Position adjustment: {b.position_adjustment:+.4f} "
                f"(δ×PF = 0.15 × {b.position_factor:.4f})"
            )
            lines.append("")

    # ---- Ordering ----
    scored = [r for r in portfolio.results if r.status == "success"]
    scored_sorted = sorted(scored, key=lambda r: r.hr_score or 0, reverse=True)
    ordering = " > ".join(f"{r.ticker} ({r.hr_score:.1f})" for r in scored_sorted)
    lines.append(f"**Relative ordering:** {ordering}")
    lines.append("")

    # ---- Footer ----
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by CS3 H^R Scoring Pipeline*")
    lines.append(f"*Formula: H^R = HR_base × (1 + δ × PF), where δ = 0.15*")

    return "\n".join(lines)


@router.post(
    "/hr/portfolio/report",
    summary="Generate & download H^R portfolio report as .md file",
    description="""
    Calculates H^R for all 5 CS3 companies, then generates a
    downloadable Markdown report with:
    - Summary table with validation
    - Validation scorecard
    - Interpretation by company
    - Relative ordering

    Returns a downloadable `.md` file.
    """,
    responses={
        200: {
            "content": {"text/markdown": {}},
            "description": "Downloadable Markdown report file",
        }
    },
)
async def download_hr_report():
    """Calculate H^R for all 5 companies, generate MD report, return as downloadable file."""
    start = time.time()
    svc = get_composite_scoring_service()

    logger.info("📝 Generating downloadable H^R Portfolio Report")

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = svc.compute_hr(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1

    portfolio = PortfolioHRResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=[],
        duration_seconds=round(time.time() - start, 2),
    )

    md_content = _generate_hr_report(portfolio)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"cs3_hr_report_{ts}.md"

    buffer = io.BytesIO(md_content.encode("utf-8"))
    buffer.seek(0)

    logger.info(f"📝 H^R report ready — {len(md_content)} chars, file={filename}")

    return StreamingResponse(
        content=buffer,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(md_content.encode("utf-8"))),
        },
    )


# =====================================================================
# Response models for GET endpoints
# =====================================================================

class HRScoringRecord(BaseModel):
    """H^R score stored in the SCORING table for one company."""
    ticker: str
    hr: Optional[float] = None
    scored_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioHRScoringResponse(BaseModel):
    """SCORING table HR rows for all CS3 portfolio companies."""
    status: str
    results: List[HRScoringRecord]
    message: Optional[str] = None


def _fetch_hr_row(ticker: str) -> Optional[HRScoringRecord]:
    """Read hr column from the Snowflake SCORING table for one ticker."""
    row = get_composite_scoring_repo().fetch_hr_row(ticker)
    if not row:
        return None
    return HRScoringRecord(
        ticker=row["TICKER"],
        hr=row.get("HR"),
        scored_at=str(row["SCORED_AT"]) if row.get("SCORED_AT") else None,
        updated_at=str(row["UPDATED_AT"]) if row.get("UPDATED_AT") else None,
    )


# =====================================================================
# GET /api/v1/scoring/hr/portfolio — Read all 5 from Snowflake
# =====================================================================

@router.get(
    "/hr/portfolio",
    response_model=PortfolioHRScoringResponse,
    summary="Get last computed H^R for all 5 CS3 companies (from Snowflake)",
    description="""
    Reads the latest H^R scores for all 5 CS3 portfolio companies from the
    Snowflake SCORING table. No computation is performed.

    Use POST /hr/portfolio to (re)compute and refresh the stored scores.
    """,
)
async def get_portfolio_hr():
    """Return last stored H^R for all 5 portfolio companies."""
    results = []
    for ticker in CS3_PORTFOLIO:
        try:
            row = _fetch_hr_row(ticker)
            results.append(row if row else HRScoringRecord(ticker=ticker))
        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch SCORING row: {e}")
            results.append(HRScoringRecord(ticker=ticker))

    scored = sum(1 for r in results if r.hr is not None)
    return PortfolioHRScoringResponse(
        status="ok",
        results=results,
        message=f"{scored}/{len(CS3_PORTFOLIO)} companies have stored H^R scores",
    )


# =====================================================================
# GET /api/v1/scoring/hr/{ticker} — Read one from Snowflake
# =====================================================================

@router.get(
    "/hr/{ticker}",
    response_model=HRScoringRecord,
    summary="Get last computed H^R for one company (from Snowflake)",
    description="""
    Reads the latest H^R score for a single ticker from the Snowflake SCORING table.
    No computation is performed.

    Use POST /hr/{ticker} to (re)compute and refresh the stored score.
    """,
)
async def get_hr(ticker: str):
    """Return last stored H^R for one company."""
    row = _fetch_hr_row(ticker.upper())
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No scoring record found for {ticker.upper()}. "
                f"Run POST /hr/{ticker} first."
            ),
        )
    return row
