"""
Company Resolver — app/utils/company_resolver.py

Resolves a ticker, company name, or CIK into full company metadata.

Sources (in order of reliability):
  1. yfinance      → name, sector, revenue, employees, market cap
  2. SEC EDGAR API → CIK number, fiscal year end, SIC code
  3. Groq LLM      → maps sector → industry_id, estimates position_factor

Usage:
    from app.utils.company_resolver import resolve_company
    result = resolve_company("GOOGL")
    result = resolve_company("Google")
    result = resolve_company("0001652044")
"""
from __future__ import annotations

import os
import re
import logging
import requests
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# ── Your 6 industry IDs from Snowflake seed data ─────────────────
INDUSTRY_MAP: Dict[str, str] = {
    "manufacturing":      "550e8400-e29b-41d4-a716-446655440001",
    "healthcare":         "550e8400-e29b-41d4-a716-446655440002",
    "business_services":  "550e8400-e29b-41d4-a716-446655440003",
    "retail":             "550e8400-e29b-41d4-a716-446655440004",
    "financial":          "550e8400-e29b-41d4-a716-446655440005",
    "technology":         "550e8400-e29b-41d4-a716-446655440006",
}

# ── Sector → industry mapping (covers yfinance sector names) ─────
SECTOR_TO_INDUSTRY: Dict[str, str] = {
    # Technology
    "technology":                    "technology",
    "communication services":        "technology",
    "information technology":        "technology",

    # Financial
    "financial services":            "financial",
    "financials":                    "financial",
    "banking":                       "financial",
    "insurance":                     "financial",

    # Healthcare
    "healthcare":                    "healthcare",
    "health care":                   "healthcare",
    "pharmaceuticals":               "healthcare",
    "biotechnology":                 "healthcare",

    # Manufacturing / Industrials
    "industrials":                   "manufacturing",
    "manufacturing":                 "manufacturing",
    "materials":                     "manufacturing",
    "energy":                        "manufacturing",
    "utilities":                     "manufacturing",

    # Retail / Consumer
    "consumer cyclical":             "retail",
    "consumer defensive":            "retail",
    "retail":                        "retail",
    "consumer staples":              "retail",
    "consumer discretionary":        "retail",

    # Business Services
    "real estate":                   "business_services",
    "services":                      "business_services",
    "business services":             "business_services",
}

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.1-8b-instant"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

SEC_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt=2020-01-01&enddt=2026-01-01&forms=10-K"
SEC_EDGAR_COMPANY = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_EDGAR_TICKERS = "https://www.sec.gov/files/company_tickers.json"


@dataclass
class ResolvedCompany:
    """Full company metadata ready for POST /api/v1/companies."""
    # Required for API
    name: str
    ticker: str
    industry_id: str
    position_factor: float = 0.0

    # Enriched fields (filled by Groq background task in CS1)
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    market_cap_percentile: Optional[float] = None
    fiscal_year_end: Optional[str] = None

    # Extra context (not stored in companies table)
    cik: Optional[str] = None
    market_cap: Optional[float] = None
    description: Optional[str] = None
    website: Optional[str] = None
    country: Optional[str] = None

    # Resolution metadata
    resolved_from: str = ""  # "yfinance", "sec_edgar", "groq"
    confidence: float = 1.0
    warnings: list = field(default_factory=list)


def _lookup_yfinance(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch company info from yfinance."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker.upper())
        info = t.info
        if not info or not info.get("longName"):
            return None
        return info
    except Exception as e:
        logger.warning("yfinance_failed ticker=%s error=%s", ticker, e)
        return None


def _lookup_sec_cik(ticker: str) -> Optional[str]:
    """Look up CIK number from SEC EDGAR company tickers file."""
    try:
        resp = requests.get(
            SEC_EDGAR_TICKERS,
            headers={"User-Agent": "PE-OrgAIR-Platform research@quantuniversity.com"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        ticker_upper = ticker.upper()
        for _, company in data.items():
            if company.get("ticker", "").upper() == ticker_upper:
                cik = str(company["cik_str"]).zfill(10)
                return cik
    except Exception as e:
        logger.warning("sec_cik_lookup_failed ticker=%s error=%s", ticker, e)
    return None


def _lookup_sec_by_name(company_name: str) -> Optional[str]:
    """Search SEC EDGAR for a company by name, return ticker if found."""
    try:
        resp = requests.get(
            f"https://efts.sec.gov/LATEST/search-index?q=%22{requests.utils.quote(company_name)}%22&forms=10-K",
            headers={"User-Agent": "PE-OrgAIR-Platform research@quantuniversity.com"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            source = hits[0].get("_source", {})
            return source.get("file_date", None)
    except Exception as e:
        logger.warning("sec_name_search_failed name=%s error=%s", company_name, e)
    return None


def _ticker_from_name_groq(company_name: str) -> Optional[str]:
    """Use Groq to resolve company name → ticker symbol."""
    if not GROQ_API_KEY:
        return None
    try:
        resp = requests.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [{
                    "role": "user",
                    "content": (
                        f"What is the NYSE/NASDAQ ticker symbol for '{company_name}'? "
                        f"Reply with ONLY the ticker symbol, nothing else. "
                        f"Example: GOOGL or MSFT or AAPL"
                    ),
                }],
                "max_tokens": 10,
                "temperature": 0.1,
            },
            timeout=10,
        )
        resp.raise_for_status()
        ticker = resp.json()["choices"][0]["message"]["content"].strip().upper()
        # Validate — ticker should be 1-5 uppercase letters
        if re.match(r'^[A-Z]{1,5}$', ticker):
            return ticker
    except Exception as e:
        logger.warning("groq_ticker_resolution_failed name=%s error=%s", company_name, e)
    return None


def _map_sector_to_industry(sector: str) -> tuple[str, str]:
    """
    Map yfinance sector string to your 6 industry categories.
    Returns (industry_key, industry_id).
    """
    sector_lower = sector.lower().strip()

    # Direct match
    if sector_lower in SECTOR_TO_INDUSTRY:
        key = SECTOR_TO_INDUSTRY[sector_lower]
        return key, INDUSTRY_MAP[key]

    # Partial match
    for s, key in SECTOR_TO_INDUSTRY.items():
        if s in sector_lower or sector_lower in s:
            return key, INDUSTRY_MAP[key]

    # Default to business_services if no match
    logger.warning("sector_not_mapped sector=%s defaulting to business_services", sector)
    return "business_services", INDUSTRY_MAP["business_services"]


def _calculate_position_factor(
    market_cap: Optional[float],
    sector: str,
) -> float:
    """
    Estimate position factor from market cap.
    Simple percentile-based approach using rough sector thresholds.
    Returns value in [-1, 1].
    """
    if not market_cap:
        return 0.0

    # Rough market cap thresholds by tier (USD)
    MEGA_CAP = 500_000_000_000   # >500B → top tier
    LARGE_CAP = 100_000_000_000  # >100B → large
    MID_CAP = 10_000_000_000     # >10B  → mid
    SMALL_CAP = 1_000_000_000    # >1B   → small

    if market_cap >= MEGA_CAP:
        return 0.9
    elif market_cap >= LARGE_CAP:
        return 0.6
    elif market_cap >= MID_CAP:
        return 0.3
    elif market_cap >= SMALL_CAP:
        return 0.0
    else:
        return -0.3


def _calculate_market_cap_percentile(market_cap: Optional[float]) -> Optional[float]:
    """Rough market cap percentile (0-1) based on absolute value."""
    if not market_cap:
        return None
    MEGA_CAP = 500_000_000_000
    LARGE_CAP = 100_000_000_000
    MID_CAP = 10_000_000_000
    SMALL_CAP = 1_000_000_000

    if market_cap >= MEGA_CAP:
        return 0.99
    elif market_cap >= LARGE_CAP:
        return 0.85
    elif market_cap >= MID_CAP:
        return 0.60
    elif market_cap >= SMALL_CAP:
        return 0.35
    else:
        return 0.10


def resolve_company(input_str: str) -> ResolvedCompany:
    """
    Resolve a ticker, company name, or CIK to full company metadata.

    Examples:
        resolve_company("GOOGL")
        resolve_company("Google")
        resolve_company("Alphabet Inc")
        resolve_company("0001652044")

    Returns ResolvedCompany with all fields populated.
    """
    input_str = input_str.strip()
    warnings = []
    ticker = None
    cik = None

    # ── Detect input type ─────────────────────────────────────────
    # CIK: 10-digit number
    if re.match(r'^\d{7,10}$', input_str):
        cik = input_str.zfill(10)
        # Try to get ticker from SEC
        try:
            resp = requests.get(
                SEC_EDGAR_COMPANY.format(cik=cik),
                headers={"User-Agent": "PE-OrgAIR-Platform research@quantuniversity.com"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                ticker = data.get("tickers", [None])[0]
                if ticker:
                    ticker = ticker.upper()
        except Exception:
            pass
        if not ticker:
            warnings.append(f"Could not resolve ticker from CIK {cik}")
            return ResolvedCompany(
                name=f"Company CIK {cik}",
                ticker=cik,
                industry_id=INDUSTRY_MAP["business_services"],
                cik=cik,
                resolved_from="sec_edgar",
                warnings=warnings,
            )

    # Ticker: 1-5 uppercase letters (or can be converted)
    elif re.match(r'^[A-Za-z]{1,5}$', input_str):
        ticker = input_str.upper()

    # Company name: resolve via Groq
    else:
        ticker = _ticker_from_name_groq(input_str)
        if not ticker:
            warnings.append(
                f"Could not resolve ticker for '{input_str}'. "
                "Try entering the ticker directly (e.g. GOOGL)"
            )
            return ResolvedCompany(
                name=input_str,
                ticker=input_str.upper()[:10],
                industry_id=INDUSTRY_MAP["business_services"],
                resolved_from="groq",
                confidence=0.3,
                warnings=warnings,
            )

    # ── Fetch from yfinance ───────────────────────────────────────
    info = _lookup_yfinance(ticker)

    if not info:
        warnings.append(
            f"yfinance returned no data for {ticker}. "
            "Company may not be publicly listed."
        )
        # Return minimal company with just ticker
        return ResolvedCompany(
            name=ticker,
            ticker=ticker,
            industry_id=INDUSTRY_MAP["business_services"],
            resolved_from="yfinance_failed",
            confidence=0.3,
            warnings=warnings,
        )

    # ── Map sector → industry ─────────────────────────────────────
    yf_sector = info.get("sector", "") or ""
    industry_key, industry_id = _map_sector_to_industry(yf_sector)

    # ── Calculate financials ──────────────────────────────────────
    market_cap = info.get("marketCap")
    total_revenue = info.get("totalRevenue")
    revenue_millions = round(total_revenue / 1_000_000, 1) if total_revenue else None
    employee_count = info.get("fullTimeEmployees")
    position_factor = _calculate_position_factor(market_cap, yf_sector)
    market_cap_percentile = _calculate_market_cap_percentile(market_cap)

    # ── Get CIK from SEC if not already known ────────────────────
    if not cik:
        cik = _lookup_sec_cik(ticker)

    # ── Fiscal year end ───────────────────────────────────────────
    fiscal_year_end = None
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        cal = t.calendar
        if cal is not None and hasattr(cal, 'get'):
            fiscal_year_end = cal.get("Earnings Date", [None])[0]
            fiscal_year_end = None  # calendar gives earnings, not FY end
        # Use financials to infer FY end month
        fin = t.financials
        if fin is not None and not fin.empty:
            last_col = fin.columns[0]
            fiscal_year_end = last_col.strftime("%B")  # e.g. "December"
    except Exception:
        pass

    # ── Build result ──────────────────────────────────────────────
    return ResolvedCompany(
        name=info.get("longName", ticker),
        ticker=ticker,
        industry_id=industry_id,
        position_factor=round(position_factor, 3),
        sector=industry_key,
        sub_sector=info.get("industry", ""),
        revenue_millions=revenue_millions,
        employee_count=employee_count,
        market_cap_percentile=market_cap_percentile,
        fiscal_year_end=fiscal_year_end,
        cik=cik,
        market_cap=market_cap,
        description=info.get("longBusinessSummary", "")[:500] if info.get("longBusinessSummary") else "",
        website=info.get("website", ""),
        country=info.get("country", ""),
        resolved_from="yfinance",
        confidence=0.95,
        warnings=warnings,
    )


def format_resolution_preview(company: ResolvedCompany) -> str:
    """Format resolved company for display in Streamlit."""
    lines = [
        f"**{company.name}** ({company.ticker})",
        f"Sector: {company.sector or 'Unknown'} | "
        f"Sub-sector: {company.sub_sector or 'Unknown'}",
    ]
    if company.revenue_millions:
        lines.append(f"Revenue: ${company.revenue_millions:,.0f}M")
    if company.employee_count:
        lines.append(f"Employees: {company.employee_count:,}")
    if company.cik:
        lines.append(f"SEC CIK: {company.cik}")
    if company.warnings:
        for w in company.warnings:
            lines.append(f"⚠️ {w}")
    return "\n".join(lines)
