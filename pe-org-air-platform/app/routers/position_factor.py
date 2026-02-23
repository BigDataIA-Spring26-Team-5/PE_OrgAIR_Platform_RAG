"""
routers/position_factor.py — CS3 Task 6.0a Endpoints

Endpoints:
  POST /api/v1/scoring/pf/{ticker}        — Compute Position Factor for one company
  POST /api/v1/scoring/pf/portfolio       — Compute PF for all 5 CS3 companies
  POST /api/v1/scoring/pf/portfolio/report — Download report as .md file

Already registered in main.py as pf_router.
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
    PFResponse,
    PFBreakdown,
    PFValidation,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 Position Factor"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Expected PF ranges from CS3 Table 5 (used by report helper)
EXPECTED_PF_RANGES = {
    "NVDA": (0.7, 1.0),
    "JPM": (0.3, 0.7),
    "WMT": (0.1, 0.5),
    "GE": (-0.2, 0.2),
    "DG": (-0.5, -0.1),
}


# =====================================================================
# Response Models (portfolio + GET — stay in router)
# =====================================================================

class PortfolioPFResponse(BaseModel):
    """Portfolio Position Factor response."""
    status: str
    companies_scored: int
    companies_failed: int
    results: List[PFResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# POST /api/v1/scoring/pf/portfolio — Calculate PF for all 5 companies
# =====================================================================

@router.post(
    "/pf/portfolio",
    response_model=PortfolioPFResponse,
    summary="Calculate Position Factor for all 5 CS3 portfolio companies",
    description="""
    Runs Task 6.0a (Position Factor) for all 5 companies: NVDA, JPM, WMT, GE, DG.

    Pipeline for each company:
    1. Get VR score from TC+VR endpoint (Task 5.2)
    2. Get market cap percentile (manual input)
    3. Calculate PF = 0.6 × VR_component + 0.4 × MCap_component
    4. Validate against CS3 Table 5 expected ranges

    Returns individual breakdowns + summary comparison table.
    """,
)
async def score_portfolio_pf():
    """Calculate Position Factor for all 5 companies."""
    start = time.time()
    svc = get_composite_scoring_service()

    logger.info("=" * 70)
    logger.info("🚀 POSITION FACTOR PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = svc.compute_pf(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1

    # Build summary table
    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 POSITION FACTOR SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(
        f"{'Ticker':<8} {'VR':>6} {'Sector Avg':>11} {'VR Comp':>9} "
        f"{'MCap %ile':>10} {'MCap Comp':>10} {'PF':>8} {'Range':>12} {'✓':>3}"
    )
    logger.info("-" * 70)

    for r in results:
        if r.status == "success" and r.pf_breakdown:
            b = r.pf_breakdown
            val_status = r.validation.status if r.validation else "—"
            range_str = r.validation.pf_expected if r.validation else "—"

            logger.info(
                f"{r.ticker:<8} {b.vr_score:>6.2f} {b.sector_avg_vr:>11.2f} "
                f"{b.vr_component:>9.4f} {b.market_cap_percentile:>10.2f} "
                f"{b.mcap_component:>10.4f} {b.position_factor:>8.4f} "
                f"{range_str:>12} {val_status:>3}"
            )

            summary.append({
                "ticker": r.ticker,
                "vr_score": b.vr_score,
                "sector_avg_vr": b.sector_avg_vr,
                "vr_component": b.vr_component,
                "market_cap_percentile": b.market_cap_percentile,
                "mcap_component": b.mcap_component,
                "position_factor": b.position_factor,
                "pf_in_expected_range": r.validation.pf_in_range if r.validation else None,
            })
        else:
            logger.info(f"{r.ticker:<8} FAILED: {r.error}")
            summary.append({"ticker": r.ticker, "status": "failed", "error": r.error})

    logger.info("-" * 70)

    pf_pass = sum(1 for r in results if r.validation and r.validation.pf_in_range)
    pf_total = sum(1 for r in results if r.validation)

    logger.info(f"Scored: {scored}  Failed: {failed}")
    logger.info(f"PF Validation: {pf_pass}/{pf_total} within expected range")
    logger.info(f"Duration: {time.time() - start:.2f}s")
    logger.info("=" * 70)

    return PortfolioPFResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=summary,
        duration_seconds=round(time.time() - start, 2),
    )


# =====================================================================
# POST /api/v1/scoring/pf/{ticker} — Calculate PF for one company
# =====================================================================

@router.post(
    "/pf/{ticker}",
    response_model=PFResponse,
    summary="Calculate Position Factor for one company",
    description="""
    Runs Task 6.0a (Position Factor) for a single ticker.

    Pipeline:
    1. Get VR score from TC+VR endpoint (Task 5.2)
    2. Get market cap percentile (manual input from config)
    3. Calculate PF = 0.6 × VR_component + 0.4 × MCap_component
    4. Validate against CS3 Table 5 expected range
    5. Save result to S3 and upsert PF into Snowflake SCORING table

    Position Factor interpretation:
    - PF > +0.7: Dominant leader
    - PF +0.3 to +0.7: Strong player
    - PF -0.3 to +0.3: Average/peer
    - PF < -0.3: Laggard
    """,
)
async def score_pf(ticker: str):
    """Calculate Position Factor for one company. Saves to S3 + Snowflake SCORING table."""
    return get_composite_scoring_service().compute_pf(ticker.upper())


# =====================================================================
# POST /api/v1/scoring/pf/portfolio/report — Download MD Report
# =====================================================================

def _generate_pf_report(portfolio: PortfolioPFResponse) -> str:
    """Generate a markdown report from Position Factor results."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = []
    lines.append("# Position Factor (PF) Scoring — CS3 Portfolio Report")
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
        "| Ticker | VR | Sector Avg | VR Comp | MCap %ile | MCap Comp | PF "
        "| Expected Range | Status |"
    )
    lines.append(
        "|--------|------|------------|---------|-----------|-----------|------|----------------|--------|"
    )

    pf_pass = 0
    pf_total = 0

    for r in portfolio.results:
        if r.status != "success":
            lines.append(f"| {r.ticker} | — | — | — | — | — | — | — | ❌ |")
            continue

        b = r.pf_breakdown
        val = r.validation

        if val:
            pf_total += 1
            if val.pf_in_range:
                pf_pass += 1

        status = val.status if val else "—"
        range_str = val.pf_expected if val else "—"

        lines.append(
            f"| {r.ticker} | {b.vr_score:.2f} | {b.sector_avg_vr:.2f} "
            f"| {b.vr_component:.4f} | {b.market_cap_percentile:.2f} "
            f"| {b.mcap_component:.4f} | {b.position_factor:.4f} "
            f"| {range_str} | {status} |"
        )

    lines.append("")

    # ---- Scorecard ----
    lines.append("## Validation Scorecard")
    lines.append("")
    lines.append(f"- **Position Factor:** {pf_pass}/{pf_total} ✅ within expected range")
    lines.append("")

    # ---- Position Factor Interpretation ----
    lines.append("## Position Factor Interpretation")
    lines.append("")
    lines.append("| PF Range | Interpretation | Companies |")
    lines.append("|----------|----------------|-----------|")

    leaders = [
        r.ticker for r in portfolio.results
        if r.status == "success" and r.position_factor and r.position_factor >= 0.7
    ]
    strong = [
        r.ticker for r in portfolio.results
        if r.status == "success" and r.position_factor and 0.3 <= r.position_factor < 0.7
    ]
    average = [
        r.ticker for r in portfolio.results
        if r.status == "success" and r.position_factor and -0.3 <= r.position_factor < 0.3
    ]
    laggards = [
        r.ticker for r in portfolio.results
        if r.status == "success" and r.position_factor and r.position_factor < -0.3
    ]

    lines.append(
        f"| +0.7 to +1.0 | **Dominant Leader** | {', '.join(leaders) if leaders else '—'} |"
    )
    lines.append(
        f"| +0.3 to +0.7 | **Strong Player** | {', '.join(strong) if strong else '—'} |"
    )
    lines.append(
        f"| -0.3 to +0.3 | **Average/Peer** | {', '.join(average) if average else '—'} |"
    )
    lines.append(
        f"| -1.0 to -0.3 | **Laggard** | {', '.join(laggards) if laggards else '—'} |"
    )
    lines.append("")

    # ---- Ordering ----
    scored = [r for r in portfolio.results if r.status == "success"]
    scored_sorted = sorted(scored, key=lambda r: r.position_factor or 0, reverse=True)
    ordering = " > ".join(
        f"{r.ticker} ({r.position_factor:.2f})" for r in scored_sorted
    )
    lines.append(f"**Relative ordering:** {ordering}")
    lines.append("")

    # ---- Footer ----
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by CS3 Position Factor Scoring Pipeline*")
    lines.append(
        f"*Formula: PF = 0.6 × (VR - Sector_Avg)/50 + 0.4 × (MCap_%ile - 0.5) × 2*"
    )

    return "\n".join(lines)


@router.post(
    "/pf/portfolio/report",
    summary="Generate & download Position Factor portfolio report as .md file",
    description="""
    Calculates Position Factor for all 5 CS3 companies, then generates a
    downloadable Markdown report with:
    - Summary table with VR, sector avg, components, and PF
    - Validation scorecard
    - Position interpretation (leader/strong/average/laggard)
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
async def download_pf_report():
    """Calculate PF for all 5 companies, generate MD report, return as downloadable file."""
    start = time.time()
    svc = get_composite_scoring_service()

    logger.info("📝 Generating downloadable Position Factor Portfolio Report")

    results = []
    scored = 0
    failed = 0

    for ticker in CS3_PORTFOLIO:
        result = svc.compute_pf(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
        else:
            failed += 1

    portfolio = PortfolioPFResponse(
        status="success" if failed == 0 else "partial",
        companies_scored=scored,
        companies_failed=failed,
        results=results,
        summary_table=[],
        duration_seconds=round(time.time() - start, 2),
    )

    md_content = _generate_pf_report(portfolio)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"cs3_position_factor_report_{ts}.md"

    buffer = io.BytesIO(md_content.encode("utf-8"))
    buffer.seek(0)

    logger.info(
        f"📝 Position Factor report ready — {len(md_content)} chars, file={filename}"
    )

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

class PFScoringRecord(BaseModel):
    """PF score stored in the SCORING table for one company."""
    ticker: str
    pf: Optional[float] = None
    scored_at: Optional[str] = None
    updated_at: Optional[str] = None


class PortfolioPFScoringResponse(BaseModel):
    """SCORING table PF rows for all CS3 portfolio companies."""
    status: str
    results: List[PFScoringRecord]
    message: Optional[str] = None


def _fetch_pf_row(ticker: str) -> Optional[PFScoringRecord]:
    """Read pf column from the Snowflake SCORING table for one ticker."""
    row = get_composite_scoring_repo().fetch_pf_row(ticker)
    if not row:
        return None
    return PFScoringRecord(
        ticker=row["TICKER"],
        pf=row.get("PF"),
        scored_at=str(row["SCORED_AT"]) if row.get("SCORED_AT") else None,
        updated_at=str(row["UPDATED_AT"]) if row.get("UPDATED_AT") else None,
    )


# =====================================================================
# GET /api/v1/scoring/pf/portfolio — Read all 5 from Snowflake
# =====================================================================

@router.get(
    "/pf/portfolio",
    response_model=PortfolioPFScoringResponse,
    summary="Get last computed Position Factor for all 5 CS3 companies (from Snowflake)",
    description="""
    Reads the latest PF scores for all 5 CS3 portfolio companies from the
    Snowflake SCORING table. No computation is performed.

    Use POST /pf/portfolio to (re)compute and refresh the stored scores.
    """,
)
async def get_portfolio_pf():
    """Return last stored Position Factor for all 5 portfolio companies."""
    results = []
    for ticker in CS3_PORTFOLIO:
        try:
            row = _fetch_pf_row(ticker)
            results.append(row if row else PFScoringRecord(ticker=ticker))
        except Exception as e:
            logger.warning(f"[{ticker}] Failed to fetch SCORING row: {e}")
            results.append(PFScoringRecord(ticker=ticker))

    scored = sum(1 for r in results if r.pf is not None)
    return PortfolioPFScoringResponse(
        status="ok",
        results=results,
        message=f"{scored}/{len(CS3_PORTFOLIO)} companies have stored PF scores",
    )


# =====================================================================
# GET /api/v1/scoring/pf/{ticker} — Read one from Snowflake
# =====================================================================

@router.get(
    "/pf/{ticker}",
    response_model=PFScoringRecord,
    summary="Get last computed Position Factor for one company (from Snowflake)",
    description="""
    Reads the latest PF score for a single ticker from the Snowflake SCORING table.
    No computation is performed.

    Use POST /pf/{ticker} to (re)compute and refresh the stored score.
    """,
)
async def get_pf(ticker: str):
    """Return last stored Position Factor for one company."""
    row = _fetch_pf_row(ticker.upper())
    if not row:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No scoring record found for {ticker.upper()}. "
                f"Run POST /pf/{ticker} first."
            ),
        )
    return row
