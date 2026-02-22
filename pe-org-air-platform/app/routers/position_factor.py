"""
routers/position_factor.py — CS3 Task 6.0a Endpoints

Endpoints:
  POST /api/v1/scoring/pf/{ticker}        — Compute Position Factor for one company
  POST /api/v1/scoring/pf/portfolio       — Compute PF for all 5 CS3 companies
  POST /api/v1/scoring/pf/portfolio/report — Download report as .md file

Already registered in main.py as pf_router.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal
import logging
import time
from fastapi.responses import StreamingResponse
import io

from app.repositories.composite_scoring_repository import get_composite_scoring_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scoring", tags=["CS3 Position Factor"])

# The 5 CS3 portfolio companies
CS3_PORTFOLIO = ["NVDA", "JPM", "WMT", "GE", "DG"]

# Manual market cap percentiles (from your config)
MARKET_CAP_PERCENTILES = {
    "NVDA": 0.95,  # Top 5% in tech
    "JPM": 0.85,   # Largest US bank
    "WMT": 0.60,   # Large retailer (Amazon bigger)
    "GE": 0.50,    # Middle of manufacturing
    "DG": 0.30,    # Smaller retailer
}

# Sector assignments
COMPANY_SECTORS = {
    "NVDA": "technology",
    "JPM": "financial_services",
    "WMT": "retail",
    "GE": "manufacturing",
    "DG": "retail",
}

# Expected PF ranges from CS3 Table 5
EXPECTED_PF_RANGES = {
    "NVDA": (0.7, 1.0),
    "JPM": (0.3, 0.7),
    "WMT": (0.1, 0.5),
    "GE": (-0.2, 0.2),
    "DG": (-0.5, -0.1),
}


# =====================================================================
# Response Models
# =====================================================================

class PFBreakdown(BaseModel):
    """Position Factor calculation breakdown."""
    vr_score: float
    sector_avg_vr: float
    vr_diff: float
    vr_component: float
    market_cap_percentile: float
    mcap_component: float
    position_factor: float


class PFValidation(BaseModel):
    """Validation against expected ranges."""
    pf_in_range: bool
    pf_expected: str
    status: str  # "✅", "⚠️", or "—"


class PFResponse(BaseModel):
    """Single company Position Factor response."""
    ticker: str
    status: str  # "success" or "failed"
    
    # Position Factor outputs
    position_factor: Optional[float] = None
    pf_breakdown: Optional[PFBreakdown] = None
    
    # Validation
    validation: Optional[PFValidation] = None
    
    # Metadata
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    scored_at: Optional[str] = None


class PortfolioPFResponse(BaseModel):
    """Portfolio Position Factor response."""
    status: str
    companies_scored: int
    companies_failed: int
    results: List[PFResponse]
    summary_table: List[Dict[str, Any]]
    duration_seconds: float


# =====================================================================
# TEMPORARY: _compute_tc_vr duplicate — pending Phase 4 extraction
# into CompositeScoringService.  Delete this entire section once
# CompositeScoringService is created.
# =====================================================================

import json as _json
from pydantic import BaseModel as _BaseModel
from typing import Optional as _Optional, List as _List, Dict as _Dict

_TC_VR_EXPECTED_RANGES_PF = {
    "NVDA": {"tc": (0.05, 0.20), "pf": (0.7, 1.0),  "vr": (80, 100)},
    "JPM":  {"tc": (0.10, 0.25), "pf": (0.3, 0.7),  "vr": (60, 80)},
    "WMT":  {"tc": (0.12, 0.28), "pf": (0.1, 0.5),  "vr": (50, 70)},
    "GE":   {"tc": (0.18, 0.35), "pf": (-0.2, 0.2), "vr": (40, 60)},
    "DG":   {"tc": (0.22, 0.40), "pf": (-0.5, -0.1),"vr": (30, 50)},
}


class _JobAnalysisOutput(_BaseModel):
    total_ai_jobs: int
    senior_ai_jobs: int
    mid_ai_jobs: int
    entry_ai_jobs: int
    unique_skills: _List[str]


class _TCBreakdown(_BaseModel):
    leadership_ratio: float
    team_size_factor: float
    skill_concentration: float
    individual_factor: float


class _VRBreakdownOutput(_BaseModel):
    vr_score: float
    weighted_dim_score: float
    talent_risk_adj: float


class _ValidationOutput(_BaseModel):
    tc_in_range: bool
    tc_expected: str
    vr_in_range: bool
    vr_expected: str


class _TCVRResponse(_BaseModel):
    ticker: str
    status: str
    talent_concentration: _Optional[float] = None
    tc_breakdown: _Optional[_TCBreakdown] = None
    job_analysis: _Optional[_JobAnalysisOutput] = None
    individual_mentions: _Optional[int] = None
    review_count: _Optional[int] = None
    ai_mentions: _Optional[int] = None
    vr_result: _Optional[_VRBreakdownOutput] = None
    dimension_scores: _Optional[_Dict[str, float]] = None
    validation: _Optional[_ValidationOutput] = None
    duration_seconds: _Optional[float] = None
    error: _Optional[str] = None
    scored_at: _Optional[str] = None


def _load_jobs_s3_pf(ticker: str, s3) -> list:
    """Load job postings from S3 — mirrors tc_vr_scoring._load_jobs_s3."""
    prefix = f"signals/jobs/{ticker}/"
    try:
        keys = s3.list_files(prefix)
        for key in sorted(keys, reverse=True):
            raw = s3.get_file(key)
            if raw is None:
                continue
            data = _json.loads(raw)
            postings = data.get("job_postings", [])
            if postings:
                for p in postings:
                    if "ai_skills_found" not in p:
                        p["ai_skills_found"] = p.get("ai_keywords_found", [])
                return postings
    except Exception as exc:
        logger.warning(f"[{ticker}] Job S3 load failed: {exc}")
    return []


# TEMPORARY: This is a duplicate pending Phase 4 extraction.
# Delete this function once CompositeScoringService is created.
def _compute_tc_vr_local(ticker: str) -> _TCVRResponse:
    """
    Compute TC + V^R without cross-router imports.
    Duplicated from tc_vr_scoring._compute_tc_vr to remove cross-router import.
    """
    start = time.time()
    ticker = ticker.upper()
    try:
        from app.services.scoring_service import get_scoring_service
        scoring_svc = get_scoring_service()
        base_result = scoring_svc.score_company(ticker)
        dim_scores_list = base_result.get("dimension_scores", [])

        from app.scoring.talent_concentration import TalentConcentrationCalculator
        tc_calc = TalentConcentrationCalculator()
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()

        job_postings = _load_jobs_s3_pf(ticker, s3)
        job_analysis = tc_calc.analyze_job_postings(job_postings)
        glassdoor_reviews = tc_calc.load_glassdoor_reviews(ticker, s3)
        indiv_mentions, rev_count = tc_calc.count_individual_mentions(glassdoor_reviews)
        ai_mentions, _ = tc_calc.count_ai_mentions(glassdoor_reviews)
        tc = tc_calc.calculate_tc(job_analysis, indiv_mentions, rev_count)

        total = job_analysis.total_ai_jobs
        senior = job_analysis.senior_ai_jobs
        leadership_ratio = senior / total if total > 0 else 0.5
        team_size_factor = min(1.0, 1.0 / (total ** 0.5 + 0.1)) if total > 0 else min(1.0, 1.0 / 0.1)
        skill_concentration = max(0.0, 1.0 - len(job_analysis.unique_skills) / 15)
        individual_factor = indiv_mentions / rev_count if rev_count > 0 else 0.5

        from app.scoring.vr_calculator import VRCalculator
        vr_calc = VRCalculator()
        dim_score_dict = {row["dimension"]: row["score"] for row in dim_scores_list}
        vr_result = vr_calc.calculate(dim_score_dict, float(tc))

        validation = None
        if ticker in _TC_VR_EXPECTED_RANGES_PF:
            exp = _TC_VR_EXPECTED_RANGES_PF[ticker]
            tc_ok = exp["tc"][0] <= float(tc) <= exp["tc"][1]
            vr_ok = exp["vr"][0] <= float(vr_result.vr_score) <= exp["vr"][1]
            validation = _ValidationOutput(
                tc_in_range=tc_ok,
                tc_expected=f"{exp['tc'][0]:.2f} - {exp['tc'][1]:.2f}",
                vr_in_range=vr_ok,
                vr_expected=f"{exp['vr'][0]} - {exp['vr'][1]}",
            )

        from datetime import datetime, timezone as _tz
        return _TCVRResponse(
            ticker=ticker, status="success",
            talent_concentration=float(tc),
            tc_breakdown=_TCBreakdown(
                leadership_ratio=round(leadership_ratio, 4),
                team_size_factor=round(team_size_factor, 4),
                skill_concentration=round(skill_concentration, 4),
                individual_factor=round(individual_factor, 4),
            ),
            job_analysis=_JobAnalysisOutput(
                total_ai_jobs=job_analysis.total_ai_jobs,
                senior_ai_jobs=job_analysis.senior_ai_jobs,
                mid_ai_jobs=job_analysis.mid_ai_jobs,
                entry_ai_jobs=job_analysis.entry_ai_jobs,
                unique_skills=sorted(job_analysis.unique_skills),
            ),
            individual_mentions=indiv_mentions, review_count=rev_count, ai_mentions=ai_mentions,
            vr_result=_VRBreakdownOutput(
                vr_score=float(vr_result.vr_score),
                weighted_dim_score=float(vr_result.weighted_dim_score),
                talent_risk_adj=float(vr_result.talent_risk_adj),
            ),
            dimension_scores=dim_score_dict, validation=validation,
            duration_seconds=round(time.time() - start, 2),
            scored_at=datetime.now(_tz.utc).isoformat(),
        )
    except Exception as e:
        logger.error(f"TC+VR scoring failed for {ticker}: {e}", exc_info=True)
        return _TCVRResponse(
            ticker=ticker, status="failed", error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )

# =====================================================================
# END TEMPORARY SECTION
# =====================================================================


# =====================================================================
# Helper: Compute Position Factor
# =====================================================================

def _compute_position_factor(ticker: str) -> PFResponse:
    """
    Core logic: 
    1. Get VR score from TC+VR endpoint
    2. Get market cap percentile (manual input)
    3. Calculate Position Factor
    4. Validate against expected range
    """
    start = time.time()
    ticker = ticker.upper()
    
    try:
        logger.info("=" * 60)
        logger.info(f"📍 POSITION FACTOR CALCULATION: {ticker}")
        logger.info("=" * 60)
        
        # ---- 1. Get VR score (local compute — no cross-router import) ----
        tc_vr_result = _compute_tc_vr_local(ticker)

        if tc_vr_result.status != "success":
            raise ValueError(f"TC+VR scoring failed: {tc_vr_result.error}")

        vr_score = tc_vr_result.vr_result.vr_score
        logger.info(f"[{ticker}] VR Score: {vr_score:.2f}")
        
        # ---- 2. Get manual inputs ----
        market_cap_percentile = MARKET_CAP_PERCENTILES.get(ticker)
        if market_cap_percentile is None:
            raise ValueError(f"No market cap percentile defined for {ticker}")
        
        sector = COMPANY_SECTORS.get(ticker)
        if sector is None:
            raise ValueError(f"No sector defined for {ticker}")
        
        logger.info(f"[{ticker}] Market Cap Percentile (manual): {market_cap_percentile:.2f}")
        logger.info(f"[{ticker}] Sector: {sector}")
        
        # ---- 3. Calculate Position Factor ----
        from app.scoring.position_factor import PositionFactorCalculator
        pf_calc = PositionFactorCalculator()
        
        pf = pf_calc.calculate_position_factor(
            vr_score=float(vr_score),
            sector=sector,
            market_cap_percentile=market_cap_percentile
        )
        
        # Get breakdown components for logging
        sector_avg = pf_calc.SECTOR_AVG_VR.get(sector.lower(), 50.0)
        vr_diff = vr_score - sector_avg
        vr_component = max(-1, min(1, vr_diff / 50))
        mcap_component = (market_cap_percentile - 0.5) * 2
        
        logger.info(f"[{ticker}] Position Factor Breakdown:")
        logger.info(f"  VR Score           = {vr_score:.2f}")
        logger.info(f"  Sector Avg VR      = {sector_avg:.2f}")
        logger.info(f"  VR Difference      = {vr_diff:.2f}")
        logger.info(f"  VR Component       = {vr_component:.4f}  (× 0.60 = {0.6 * vr_component:.4f})")
        logger.info(f"  MCap Percentile    = {market_cap_percentile:.2f}")
        logger.info(f"  MCap Component     = {mcap_component:.4f}  (× 0.40 = {0.4 * mcap_component:.4f})")
        logger.info(f"  ───────────────────────────────────────")
        logger.info(f"  Position Factor    = {float(pf):.4f}")
        
        # ---- 4. Validate against expected range ----
        validation = None
        if ticker in EXPECTED_PF_RANGES:
            exp_lo, exp_hi = EXPECTED_PF_RANGES[ticker]
            pf_ok = exp_lo <= float(pf) <= exp_hi
            status = "✅" if pf_ok else "⚠️"
            
            validation = PFValidation(
                pf_in_range=pf_ok,
                pf_expected=f"{exp_lo:.1f} to {exp_hi:.1f}",
                status=status
            )
            
            logger.info(f"[{ticker}] Validation (CS3 Table 5):")
            logger.info(f"  PF = {float(pf):.4f}  expected [{exp_lo:.1f}, {exp_hi:.1f}]  {status}")
        
        logger.info("=" * 60)
        
        return PFResponse(
            ticker=ticker,
            status="success",
            position_factor=float(pf),
            pf_breakdown=PFBreakdown(
                vr_score=vr_score,
                sector_avg_vr=sector_avg,
                vr_diff=vr_diff,
                vr_component=vr_component,
                market_cap_percentile=market_cap_percentile,
                mcap_component=mcap_component,
                position_factor=float(pf),
            ),
            validation=validation,
            duration_seconds=round(time.time() - start, 2),
            scored_at=datetime.now(timezone.utc).isoformat(),
        )
        
    except Exception as e:
        logger.error(f"Position Factor calculation failed for {ticker}: {e}", exc_info=True)
        return PFResponse(
            ticker=ticker,
            status="failed",
            error=str(e),
            duration_seconds=round(time.time() - start, 2),
        )


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
    
    logger.info("=" * 70)
    logger.info("🚀 POSITION FACTOR PORTFOLIO SCORING — 5 COMPANIES")
    logger.info("=" * 70)
    
    results = []
    scored = 0
    failed = 0
    
    for ticker in CS3_PORTFOLIO:
        result = _compute_position_factor(ticker)
        results.append(result)
        if result.status == "success":
            scored += 1
            _save_pf_result(result)
        else:
            failed += 1

    # Build summary table
    summary = []
    logger.info("")
    logger.info("=" * 70)
    logger.info("📊 POSITION FACTOR SUMMARY TABLE")
    logger.info("=" * 70)
    logger.info(f"{'Ticker':<8} {'VR':>6} {'Sector Avg':>11} {'VR Comp':>9} "
                f"{'MCap %ile':>10} {'MCap Comp':>10} {'PF':>8} {'Range':>12} {'✓':>3}")
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
    
    # Count validations
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
    result = _compute_position_factor(ticker.upper())
    if result.status == "success":
        _save_pf_result(result)
    return result


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
    lines.append(f"**Companies:** {portfolio.companies_scored} scored, {portfolio.companies_failed} failed")
    lines.append(f"**Duration:** {portfolio.duration_seconds}s")
    lines.append("")
    
    # ---- Summary Table ----
    lines.append("## Portfolio Summary Table")
    lines.append("")
    lines.append("| Ticker | VR | Sector Avg | VR Comp | MCap %ile | MCap Comp | PF | Expected Range | Status |")
    lines.append("|--------|------|------------|---------|-----------|-----------|------|----------------|--------|")
    
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
            f"| {r.ticker} | {b.vr_score:.2f} | {b.sector_avg_vr:.2f} | {b.vr_component:.4f} "
            f"| {b.market_cap_percentile:.2f} | {b.mcap_component:.4f} | {b.position_factor:.4f} "
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
    
    leaders = [r.ticker for r in portfolio.results if r.status == "success" and r.position_factor and r.position_factor >= 0.7]
    strong = [r.ticker for r in portfolio.results if r.status == "success" and r.position_factor and 0.3 <= r.position_factor < 0.7]
    average = [r.ticker for r in portfolio.results if r.status == "success" and r.position_factor and -0.3 <= r.position_factor < 0.3]
    laggards = [r.ticker for r in portfolio.results if r.status == "success" and r.position_factor and r.position_factor < -0.3]
    
    lines.append(f"| +0.7 to +1.0 | **Dominant Leader** | {', '.join(leaders) if leaders else '—'} |")
    lines.append(f"| +0.3 to +0.7 | **Strong Player** | {', '.join(strong) if strong else '—'} |")
    lines.append(f"| -0.3 to +0.3 | **Average/Peer** | {', '.join(average) if average else '—'} |")
    lines.append(f"| -1.0 to -0.3 | **Laggard** | {', '.join(laggards) if laggards else '—'} |")
    lines.append("")
    
    # ---- Ordering ----
    scored = [r for r in portfolio.results if r.status == "success"]
    scored_sorted = sorted(scored, key=lambda r: r.position_factor or 0, reverse=True)
    ordering = " > ".join(f"{r.ticker} ({r.position_factor:.2f})" for r in scored_sorted)
    lines.append(f"**Relative ordering:** {ordering}")
    lines.append("")
    
    # ---- Footer ----
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by CS3 Position Factor Scoring Pipeline*")
    lines.append(f"*Formula: PF = 0.6 × (VR - Sector_Avg)/50 + 0.4 × (MCap_%ile - 0.5) × 2*")
    
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
    
    logger.info("📝 Generating downloadable Position Factor Portfolio Report")
    
    # Run portfolio scoring
    results = []
    scored = 0
    failed = 0
    
    for ticker in CS3_PORTFOLIO:
        result = _compute_position_factor(ticker)
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
    
    # Generate markdown content
    md_content = _generate_pf_report(portfolio)
    
    # Build filename with timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"cs3_position_factor_report_{ts}.md"
    
    # Stream as downloadable file
    buffer = io.BytesIO(md_content.encode("utf-8"))
    buffer.seek(0)
    
    logger.info(f"📝 Position Factor report ready — {len(md_content)} chars, file={filename}")
    
    return StreamingResponse(
        content=buffer,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(md_content.encode("utf-8"))),
        },
    )


# =====================================================================
# Snowflake + S3 persistence helpers
# =====================================================================

def _save_pf_result(result: PFResponse) -> None:
    """Save PF result to S3 JSON and upsert into Snowflake SCORING and PF_SCORING tables."""
    ticker = result.ticker
    try:
        from app.services.s3_storage import get_s3_service
        s3 = get_s3_service()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"scoring/pf/{ticker}/{ts}.json"
        s3.upload_json(result.model_dump(), s3_key)
        logger.info(f"[{ticker}] PF result saved to S3: {s3_key}")
    except Exception as e:
        logger.warning(f"[{ticker}] S3 save failed (non-fatal): {e}")

    try:
        get_composite_scoring_repo().upsert_scoring_pf(ticker, result.position_factor)
        logger.info(f"[{ticker}] SCORING table upserted: PF={result.position_factor}")
    except Exception as e:
        logger.warning(f"[{ticker}] Snowflake SCORING upsert failed (non-fatal): {e}")

    # PF_SCORING — breakdown detail table
    try:
        bd = result.pf_breakdown
        val = result.validation
        get_composite_scoring_repo().upsert_pf_result(
            ticker,
            position_factor=result.position_factor,
            vr_score_used=bd.vr_score if bd else None,
            sector=COMPANY_SECTORS.get(ticker.upper()),
            sector_avg_vr=bd.sector_avg_vr if bd else None,
            vr_diff=bd.vr_diff if bd else None,
            vr_component=bd.vr_component if bd else None,
            market_cap_percentile=bd.market_cap_percentile if bd else None,
            mcap_component=bd.mcap_component if bd else None,
            pf_in_range=val.pf_in_range if val else None,
            pf_expected=val.pf_expected if val else None,
        )
        logger.info(f"[{ticker}] PF_SCORING table upserted")
    except Exception as e:
        logger.warning(f"[{ticker}] PF_SCORING upsert failed (non-fatal): {e}")


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
    from fastapi import HTTPException
    row = _fetch_pf_row(ticker.upper())
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No scoring record found for {ticker.upper()}. Run POST /pf/{ticker} first.",
        )
    return row