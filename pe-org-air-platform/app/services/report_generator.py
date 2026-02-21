"""
CS3 Report Generator Service
app/services/report_generator.py

Generates markdown scoring reports per company from Snowflake data.
Auto-pulls CS2 signal summaries and includes glassdoor/board placeholders.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Company name mapping
COMPANY_NAMES = {
    "CAT": "Caterpillar Inc.", "DE": "Deere & Company",
    "UNH": "UnitedHealth Group", "HCA": "HCA Healthcare",
    "ADP": "Automatic Data Processing", "PAYX": "Paychex Inc.",
    "WMT": "Walmart Inc.", "TGT": "Target Corporation",
    "JPM": "JPMorgan Chase & Co.", "GS": "Goldman Sachs Group",
    "NVDA": "NVIDIA Corporation", "GE": "General Electric Company",
    "DG": "Dollar General Corporation",
}

SECTOR_MAP = {
    "CAT": "Industrials", "DE": "Industrials",
    "UNH": "Healthcare", "HCA": "Healthcare",
    "ADP": "Business Services", "PAYX": "Business Services",
    "WMT": "Consumer Retail", "TGT": "Consumer Retail",
    "JPM": "Financial Services", "GS": "Financial Services",
    "NVDA": "Technology", "GE": "Manufacturing",
    "DG": "Consumer Retail",
}

DIM_DISPLAY = {
    "data_infrastructure": "Data Infrastructure",
    "ai_governance": "AI Governance",
    "technology_stack": "Technology Stack",
    "talent_skills": "Talent & Skills",
    "leadership_vision": "Leadership Vision",
    "use_case_portfolio": "Use Case Portfolio",
    "culture_change": "Culture & Change",
}

DIM_SHORT = {
    "data_infrastructure": "Data",
    "ai_governance": "Gov",
    "technology_stack": "Tech",
    "talent_skills": "Talent",
    "leadership_vision": "Lead",
    "use_case_portfolio": "Use",
    "culture_change": "Culture",
}


def _level_label(score: float) -> str:
    if score >= 80: return "Excellent"
    if score >= 60: return "Good"
    if score >= 40: return "Adequate"
    if score >= 20: return "Developing"
    return "Nascent"


# =====================================================================
# Single Company Report
# =====================================================================

def generate_company_report(
    ticker: str,
    matrix_rows: List[Dict],
    dimension_rows: List[Dict],
    signal_summary: Optional[Dict] = None,
) -> str:
    """Generate markdown report for a single company."""

    name = COMPANY_NAMES.get(ticker, ticker)
    sector = SECTOR_MAP.get(ticker, "Unknown")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    dim_keys = ["data_infrastructure", "ai_governance", "technology_stack",
                 "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"]

    lines = []
    lines.append(f"# {ticker} — CS3 Scoring Report")
    lines.append(f"")
    lines.append(f"> **{name}** | Sector: {sector} | Scored: {now}")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    # ── CS2 Signal Scores (if provided) ──
    if signal_summary:
        lines.append(f"## CS2 Signal Scores (Input)")
        lines.append(f"")
        lines.append(f"| Signal | Score |")
        lines.append(f"|:---|---:|")
        lines.append(f"| Technology Hiring | {_fmt(signal_summary.get('technology_hiring_score'))} |")
        lines.append(f"| Innovation Activity | {_fmt(signal_summary.get('innovation_activity_score'))} |")
        lines.append(f"| Digital Presence | {_fmt(signal_summary.get('digital_presence_score'))} |")
        lines.append(f"| Leadership Signals | {_fmt(signal_summary.get('leadership_signals_score'))} |")
        lines.append(f"| **Composite** | **{_fmt(signal_summary.get('composite_score'))}** |")
        lines.append(f"")

    # ── Mapping Matrix ──
    lines.append(f"## Mapping Matrix")
    lines.append(f"")

    header = "| Source | Score | " + " | ".join(DIM_SHORT[d] for d in dim_keys) + " |"
    separator = "|:---|---:|" + "|".join(["---:" for _ in dim_keys]) + "|"
    lines.append(header)
    lines.append(separator)

    # Active rows (with scores)
    active_rows = [r for r in matrix_rows if r.get("raw_score") is not None]
    for row in active_rows:
        _append_matrix_row(lines, row, dim_keys)

    # Placeholder rows for glassdoor/board (show as [NEW] with null)
    glassdoor_row = next((r for r in matrix_rows if r.get("source") == "glassdoor_reviews"), None)
    board_row = next((r for r in matrix_rows if r.get("source") == "board_composition"), None)

    if glassdoor_row and glassdoor_row.get("raw_score") is None:
        _append_placeholder_row(lines, "glassdoor_reviews [NEW]", glassdoor_row, dim_keys)
    if board_row and board_row.get("raw_score") is None:
        _append_placeholder_row(lines, "board_composition [NEW]", board_row, dim_keys)

    lines.append(f"")

    # ── SEC Rubric Scores ──
    sec_rows = [r for r in matrix_rows
                if r.get("source", "").startswith("sec_") and r.get("raw_score") is not None]

    if sec_rows:
        lines.append(f"## SEC Rubric Scores")
        lines.append(f"")
        lines.append(f"| Section | Score | Level | Confidence |")
        lines.append(f"|:---|---:|:---|---:|")

        for row in sec_rows:
            source = row.get("source", "")
            score = _to_float(row.get("raw_score", 0))
            conf = _to_float(row.get("confidence", 0))
            lines.append(f"| {_sec_display(source)} | {score:.1f} | {_level_label(score)} | {conf:.3f} |")

        lines.append(f"")

    # ── Dimension Scores ──
    lines.append(f"## Dimension Scores")
    lines.append(f"")
    lines.append(f"| Dimension | Score | Level | Conf | Sources |")
    lines.append(f"|:---|---:|:---|---:|:---|")

    for row in dimension_rows:
        dim = row.get("dimension", "")
        score = _to_float(row.get("score", 0))
        conf = _to_float(row.get("confidence", 0))
        sources = row.get("sources", "")
        lines.append(f"| {DIM_DISPLAY.get(dim, dim)} | {score:.1f} | {_level_label(score)} | {conf:.3f} | {sources} |")

    lines.append(f"")

    # ── Key Findings ──
    lines.append(f"## Key Findings")
    lines.append(f"")

    sorted_dims = sorted(dimension_rows, key=lambda r: _to_float(r.get("score", 0)), reverse=True)
    if sorted_dims:
        top = sorted_dims[0]
        bottom = sorted_dims[-1]
        top_name = DIM_DISPLAY.get(top["dimension"], top["dimension"])
        bot_name = DIM_DISPLAY.get(bottom["dimension"], bottom["dimension"])
        top_score = _to_float(top.get("score", 0))
        bot_score = _to_float(bottom.get("score", 0))

        lines.append(f"**Strongest:** {top_name} ({top_score:.1f}) — {_level_label(top_score)}")
        lines.append(f"")
        lines.append(f"**Weakest:** {bot_name} ({bot_score:.1f}) — {_level_label(bot_score)}")
        lines.append(f"")

        avg = sum(_to_float(r.get("score", 0)) for r in dimension_rows) / max(len(dimension_rows), 1)
        lines.append(f"**Average Dimension Score:** {avg:.1f}")
        lines.append(f"")

    lines.append(f"---")
    lines.append(f"")

    return "\n".join(lines)


# =====================================================================
# Portfolio Summary Report
# =====================================================================

def generate_portfolio_summary(
    all_dimension_scores: List[Dict],
    all_signal_summaries: Optional[List[Dict]] = None,
) -> str:
    """Generate portfolio comparison markdown with CS2 inputs and CS3 outputs."""

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group dimension scores by ticker
    companies: Dict[str, List[Dict]] = {}
    for row in all_dimension_scores:
        t = row.get("ticker", "")
        if t not in companies:
            companies[t] = []
        companies[t].append(row)

    dim_keys = ["data_infrastructure", "ai_governance", "technology_stack",
                 "talent_skills", "leadership_vision", "use_case_portfolio", "culture_change"]

    lines = []
    lines.append(f"# CS3 Portfolio Scoring Summary")
    lines.append(f"")
    lines.append(f"> Generated: {now} | Companies: {len(companies)} | Pipeline: CS3 Tasks 5.0a + 5.0b")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    # ── CS2 Signal Scores (Input) ──
    if all_signal_summaries:
        lines.append(f"## CS2 Signal Scores (Input)")
        lines.append(f"")
        lines.append(f"Source: `company_signal_summaries`")
        lines.append(f"")
        lines.append(f"| Ticker | Hiring | Innovation | Digital | Leadership | Composite | Signals |")
        lines.append(f"|:---|---:|---:|---:|---:|---:|---:|")

        for s in sorted(all_signal_summaries, key=lambda x: x.get("ticker", "")):
            t = s.get("ticker", "")
            lines.append(
                f"| **{t}** "
                f"| {_fmt(s.get('technology_hiring_score'))} "
                f"| {_fmt(s.get('innovation_activity_score'))} "
                f"| {_fmt(s.get('digital_presence_score'))} "
                f"| {_fmt(s.get('leadership_signals_score'))} "
                f"| {_fmt(s.get('composite_score'))} "
                f"| {s.get('signal_count', 0)} |"
            )

        lines.append(f"")

    # ── Dimension Scores by Company ──
    lines.append(f"## Dimension Scores by Company")
    lines.append(f"")

    header = "| Ticker | Sector | " + " | ".join(DIM_SHORT[d] for d in dim_keys) + " | Avg |"
    sep = "|:---|:---|" + "|".join(["---:" for _ in dim_keys]) + "|---:|"
    lines.append(header)
    lines.append(sep)

    ticker_avgs = []
    for ticker in sorted(companies.keys()):
        dims = companies[ticker]
        sector = SECTOR_MAP.get(ticker, "—")
        dim_lookup = {r["dimension"]: _to_float(r.get("score", 0)) for r in dims}

        cells = [f"{dim_lookup.get(d, 50.0):.0f}" for d in dim_keys]
        avg = sum(dim_lookup.get(d, 50.0) for d in dim_keys) / 7
        ticker_avgs.append((ticker, avg))

        lines.append(f"| **{ticker}** | {sector} | " + " | ".join(cells) + f" | **{avg:.0f}** |")

    lines.append(f"")

    # ── Rankings ──
    lines.append(f"## Rankings (by Average Score)")
    lines.append(f"")
    lines.append(f"| Rank | Ticker | Company | Sector | Avg Score | Level |")
    lines.append(f"|---:|:---|:---|:---|---:|:---|")

    ticker_avgs.sort(key=lambda x: x[1], reverse=True)
    for i, (ticker, avg) in enumerate(ticker_avgs, 1):
        name = COMPANY_NAMES.get(ticker, ticker)
        sector = SECTOR_MAP.get(ticker, "—")
        lines.append(f"| {i} | **{ticker}** | {name} | {sector} | {avg:.1f} | {_level_label(avg)} |")

    lines.append(f"")

    # ── Dimension Leaders ──
    lines.append(f"## Dimension Leaders")
    lines.append(f"")
    lines.append(f"| Dimension | Leader | Score | Laggard | Score |")
    lines.append(f"|:---|:---|---:|:---|---:|")

    for d in dim_keys:
        scores = []
        for ticker, dims in companies.items():
            for row in dims:
                if row.get("dimension") == d:
                    scores.append((ticker, _to_float(row.get("score", 0))))
        if scores:
            scores.sort(key=lambda x: x[1], reverse=True)
            leader, laggard = scores[0], scores[-1]
            lines.append(f"| {DIM_DISPLAY.get(d, d)} | **{leader[0]}** | {leader[1]:.0f} | {laggard[0]} | {laggard[1]:.0f} |")

    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    # ── Snowflake Verification Queries ──
    lines.append(f"## Snowflake Verification Queries")
    lines.append(f"")
    lines.append(f"```sql")
    lines.append(f"-- CS3 Table 1 mapping matrix for a company")
    lines.append(f"SELECT ticker, source, raw_score, confidence,")
    lines.append(f"       data_infrastructure, ai_governance, technology_stack,")
    lines.append(f"       talent_skills, leadership_vision, use_case_portfolio, culture_change")
    lines.append(f"FROM signal_dimension_mapping")
    lines.append(f"WHERE ticker = 'JPM'")
    lines.append(f"ORDER BY CASE source")
    lines.append(f"    WHEN 'technology_hiring' THEN 1 WHEN 'innovation_activity' THEN 2")
    lines.append(f"    WHEN 'digital_presence' THEN 3 WHEN 'leadership_signals' THEN 4")
    lines.append(f"    WHEN 'sec_item_1' THEN 5 WHEN 'sec_item_1a' THEN 6")
    lines.append(f"    WHEN 'sec_item_7' THEN 7 WHEN 'glassdoor_reviews' THEN 8")
    lines.append(f"    WHEN 'board_composition' THEN 9")
    lines.append(f"END;")
    lines.append(f"")
    lines.append(f"-- All dimension scores")
    lines.append(f"SELECT ticker, dimension, score, confidence, source_count, sources")
    lines.append(f"FROM evidence_dimension_scores")
    lines.append(f"ORDER BY ticker, dimension;")
    lines.append(f"")
    lines.append(f"-- Rankings")
    lines.append(f"SELECT ticker, ROUND(AVG(score), 1) as avg_score")
    lines.append(f"FROM evidence_dimension_scores GROUP BY ticker ORDER BY avg_score DESC;")
    lines.append(f"")
    lines.append(f"-- Row counts (expect 117 mapping rows, 91 dimension scores)")
    lines.append(f"SELECT COUNT(*) as rows, COUNT(DISTINCT ticker) as companies FROM signal_dimension_mapping;")
    lines.append(f"SELECT COUNT(*) as rows, COUNT(DISTINCT ticker) as companies FROM evidence_dimension_scores;")
    lines.append(f"```")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    lines.append(f"*Generated by CS3 Scoring Engine (Tasks 5.0a + 5.0b) | PE Org-AI-R Platform*")

    return "\n".join(lines)


# =====================================================================
# Helpers
# =====================================================================

def _to_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def _fmt(val) -> str:
    """Format a score value: number or 'null' for None."""
    if val is None:
        return "—"
    return f"{_to_float(val):.2f}"

def _source_display(source: str) -> str:
    return {
        "technology_hiring": "technology_hiring",
        "innovation_activity": "innovation_activity",
        "digital_presence": "digital_presence",
        "leadership_signals": "leadership_signals",
        "sec_item_1": "sec_item_1 (Business)",
        "sec_item_1a": "sec_item_1a (Risk)",
        "sec_item_7": "sec_item_7 (MD&A)",
        "glassdoor_reviews": "glassdoor_reviews [NEW]",
        "board_composition": "board_composition [NEW]",
    }.get(source, source)

def _sec_display(source: str) -> str:
    return {
        "sec_item_1": "Item 1 — Business",
        "sec_item_1a": "Item 1A — Risk Factors",
        "sec_item_7": "Item 7 — MD&A",
    }.get(source, source)

def _append_matrix_row(lines: List[str], row: Dict, dim_keys: List[str]):
    """Append an active matrix row (has score)."""
    source = row.get("source", "")
    score = row.get("raw_score")
    score_str = f"{_to_float(score):.1f}" if score is not None else "—"

    weights = {}
    for d in dim_keys:
        w = row.get(d)
        if w is not None:
            weights[d] = _to_float(w)

    primary_dim = max(weights, key=weights.get) if weights else None

    cells = []
    for d in dim_keys:
        w = row.get(d)
        if w is not None:
            w_str = f"{_to_float(w):.2f}"
            if d == primary_dim:
                w_str = f"**{w_str}**"
            cells.append(w_str)
        else:
            cells.append("—")

    lines.append(f"| {_source_display(source)} | {score_str} | " + " | ".join(cells) + " |")

def _append_placeholder_row(lines: List[str], label: str, row: Dict, dim_keys: List[str]):
    """Append a placeholder row for glassdoor/board (no score yet)."""
    cells = []
    for d in dim_keys:
        w = row.get(d)
        if w is not None:
            cells.append(f"{_to_float(w):.2f}")
        else:
            cells.append("—")

    lines.append(f"| {label} | *null* | " + " | ".join(cells) + " |")