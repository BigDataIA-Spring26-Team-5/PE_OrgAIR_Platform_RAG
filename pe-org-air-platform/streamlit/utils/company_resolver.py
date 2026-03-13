"""
Company Resolver — streamlit/utils/company_resolver.py

Resolves a ticker, company name, or CIK into full company metadata.

Sources (in order of reliability):
  1. yfinance      → name, sector, revenue, employees, market cap
  2. SEC EDGAR API → CIK number, fiscal year end, SIC code
  3. Groq LLM      → maps sector → industry_id, estimates position_factor

Name-to-ticker resolution order:
  1. yfinance.Search (fast, no API key needed)
  2. SEC EDGAR full-text search
  3. Groq LLM fallback

Usage:
    from streamlit.utils.company_resolver import resolve_company
    result = resolve_company("GOOGL")
    result = resolve_company("Apple")
    result = resolve_company("Apple Inc")
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

SECTOR_TO_INDUSTRY: Dict[str, str] = {
    "technology":               "technology",
    "communication services":   "technology",
    "information technology":   "technology",
    "financial services":       "financial",
    "financials":               "financial",
    "banking":                  "financial",
    "insurance":                "financial",
    "healthcare":               "healthcare",
    "health care":              "healthcare",
    "pharmaceuticals":          "healthcare",
    "biotechnology":            "healthcare",
    "industrials":              "manufacturing",
    "manufacturing":            "manufacturing",
    "materials":                "manufacturing",
    "energy":                   "manufacturing",
    "utilities":                "manufacturing",
    "consumer cyclical":        "retail",
    "consumer defensive":       "retail",
    "retail":                   "retail",
    "consumer staples":         "retail",
    "consumer discretionary":   "retail",
    "real estate":              "business_services",
    "services":                 "business_services",
    "business services":        "business_services",
}

GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL     = "llama-3.1-8b-instant"
GROQ_API_URL   = "https://api.groq.com/openai/v1/chat/completions"

SEC_EDGAR_COMPANY = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_EDGAR_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_HEADERS       = {"User-Agent": "PE-OrgAIR-Platform research@quantuniversity.com"}


@dataclass
class ResolvedCompany:
    """Full company metadata ready for POST /api/v1/companies."""
    name: str
    ticker: str
    industry_id: str
    position_factor: float = 0.0

    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    market_cap_percentile: Optional[float] = None
    fiscal_year_end: Optional[str] = None

    cik: Optional[str] = None
    market_cap: Optional[float] = None
    description: Optional[str] = None
    website: Optional[str] = None
    country: Optional[str] = None

    resolved_from: str = ""
    confidence: float = 1.0
    warnings: list = field(default_factory=list)


# ── Name → Ticker resolution (3 strategies) ───────────────────────

def _ticker_from_yfinance_search(query: str) -> Optional[str]:
    """
    Use yfinance.Search to find a ticker by company name.
    This works without any API key — it hits Yahoo Finance search.
    e.g. "Apple" → "AAPL", "Microsoft" → "MSFT"
    """
    try:
        import yfinance as yf
        # yfinance >= 0.2.28 has Search
        search = yf.Search(query, max_results=5, news_count=0)
        quotes = search.quotes
        if not quotes:
            return None
        # Prefer EQUITY type (not ETF/index/crypto)
        for q in quotes:
            q_type = q.get("quoteType", "").upper()
            symbol = q.get("symbol", "")
            if q_type == "EQUITY" and symbol and re.match(r'^[A-Z]{1,5}$', symbol):
                logger.info("yfinance_search query=%s → %s", query, symbol)
                return symbol
        # Fallback: first result regardless of type
        symbol = quotes[0].get("symbol", "")
        if symbol:
            return symbol.upper()
    except AttributeError:
        # yfinance version too old — no Search class
        logger.warning("yfinance.Search not available, skipping")
    except Exception as e:
        logger.warning("yfinance_search_failed query=%s error=%s", query, e)
    return None


def _ticker_from_sec_name(company_name: str) -> Optional[str]:
    """
    Search SEC EDGAR company tickers file for a name match.
    Downloads the full tickers list and fuzzy-matches by name.
    e.g. "Apple" → "AAPL"
    """
    try:
        resp = requests.get(SEC_EDGAR_TICKERS, headers=SEC_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        name_lower = company_name.lower().strip()
        best_ticker = None
        best_score  = 0

        for _, co in data.items():
            co_name  = co.get("title", "").lower()
            co_tick  = co.get("ticker", "")
            # Exact match
            if co_name == name_lower:
                return co_tick.upper()
            # Name starts with query (e.g. "apple" matches "apple inc")
            if co_name.startswith(name_lower) and len(name_lower) > best_score:
                best_ticker = co_tick.upper()
                best_score  = len(name_lower)
            # Query starts with co_name (e.g. "apple inc" matches "apple")
            elif name_lower.startswith(co_name) and len(co_name) > best_score:
                best_ticker = co_tick.upper()
                best_score  = len(co_name)

        if best_ticker:
            logger.info("sec_name_search query=%s → %s", company_name, best_ticker)
        return best_ticker

    except Exception as e:
        logger.warning("sec_name_search_failed name=%s error=%s", company_name, e)
    return None


def _ticker_from_name_groq(company_name: str) -> Optional[str]:
    """Use Groq LLM to resolve company name → ticker symbol."""
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
                        "Reply with ONLY the ticker symbol, nothing else. "
                        "Example: GOOGL or MSFT or AAPL"
                    ),
                }],
                "max_tokens": 10,
                "temperature": 0.1,
            },
            timeout=10,
        )
        resp.raise_for_status()
        ticker = resp.json()["choices"][0]["message"]["content"].strip().upper()
        if re.match(r'^[A-Z]{1,5}$', ticker):
            logger.info("groq_ticker_resolution name=%s → %s", company_name, ticker)
            return ticker
    except Exception as e:
        logger.warning("groq_ticker_resolution_failed name=%s error=%s", company_name, e)
    return None


def _resolve_name_to_ticker(input_str: str) -> tuple[Optional[str], str]:
    """
    Try all 3 name-to-ticker strategies in order.
    Returns (ticker, source_used).
    """
    # Strategy 1: yfinance Search (fast, no API key)
    ticker = _ticker_from_yfinance_search(input_str)
    if ticker:
        return ticker, "yfinance_search"

    # Strategy 2: SEC EDGAR company name match
    ticker = _ticker_from_sec_name(input_str)
    if ticker:
        return ticker, "sec_edgar_name"

    # Strategy 3: Groq LLM fallback
    ticker = _ticker_from_name_groq(input_str)
    if ticker:
        return ticker, "groq"

    return None, "failed"


# ── yfinance data fetch ───────────────────────────────────────────

def _lookup_yfinance(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch company info from yfinance."""
    try:
        import yfinance as yf
        t    = yf.Ticker(ticker.upper())
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
        resp = requests.get(SEC_EDGAR_TICKERS, headers=SEC_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        ticker_upper = ticker.upper()
        for _, company in data.items():
            if company.get("ticker", "").upper() == ticker_upper:
                return str(company["cik_str"]).zfill(10)
    except Exception as e:
        logger.warning("sec_cik_lookup_failed ticker=%s error=%s", ticker, e)
    return None


def _map_sector_to_industry(sector: str) -> tuple[str, str]:
    sector_lower = sector.lower().strip()
    if sector_lower in SECTOR_TO_INDUSTRY:
        key = SECTOR_TO_INDUSTRY[sector_lower]
        return key, INDUSTRY_MAP[key]
    for s, key in SECTOR_TO_INDUSTRY.items():
        if s in sector_lower or sector_lower in s:
            return key, INDUSTRY_MAP[key]
    logger.warning("sector_not_mapped sector=%s defaulting to business_services", sector)
    return "business_services", INDUSTRY_MAP["business_services"]


def _calculate_position_factor(market_cap: Optional[float], sector: str = "") -> float:
    if not market_cap:
        return 0.0
    if market_cap >= 500_000_000_000:  return 0.9
    if market_cap >= 100_000_000_000:  return 0.6
    if market_cap >= 10_000_000_000:   return 0.3
    if market_cap >= 1_000_000_000:    return 0.0
    return -0.3


def _calculate_market_cap_percentile(market_cap: Optional[float]) -> Optional[float]:
    if not market_cap:
        return None
    if market_cap >= 500_000_000_000:  return 0.99
    if market_cap >= 100_000_000_000:  return 0.85
    if market_cap >= 10_000_000_000:   return 0.60
    if market_cap >= 1_000_000_000:    return 0.35
    return 0.10


# ── Main entry point ──────────────────────────────────────────────

def resolve_company(input_str: str) -> ResolvedCompany:
    """
    Resolve a ticker, company name, or CIK to full company metadata.

    Examples:
        resolve_company("GOOGL")
        resolve_company("Apple")
        resolve_company("Apple Inc")
        resolve_company("Microsoft Corporation")
        resolve_company("0001652044")
    """
    input_str  = input_str.strip()
    warnings   = []
    ticker     = None
    cik        = None
    name_source = "yfinance"

    # ── Detect input type ─────────────────────────────────────────

    if re.match(r'^\d{7,10}$', input_str):
        # CIK number
        cik = input_str.zfill(10)
        try:
            resp = requests.get(
                SEC_EDGAR_COMPANY.format(cik=cik),
                headers=SEC_HEADERS, timeout=10,
            )
            if resp.status_code == 200:
                data   = resp.json()
                ticker = (data.get("tickers") or [None])[0]
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

    elif re.match(r'^[A-Za-z]{1,5}$', input_str) and input_str.upper() == input_str.upper():
        # Looks like a ticker — try it directly first
        candidate = input_str.upper()
        test_info = _lookup_yfinance(candidate)
        if test_info:
            ticker = candidate
        else:
            # Could be a short company name like "Apple" or "Meta"
            # Try name resolution before giving up
            logger.info("ticker_lookup_failed for %s, trying name resolution", candidate)
            resolved_ticker, name_source = _resolve_name_to_ticker(input_str)
            if resolved_ticker:
                ticker = resolved_ticker
            else:
                # Last resort: use as-is and let yfinance error surface
                ticker = candidate
                warnings.append(
                    f"'{input_str}' not found as a ticker. "
                    "Try the full company name or official ticker symbol."
                )

    else:
        # Company name — resolve to ticker
        ticker, name_source = _resolve_name_to_ticker(input_str)
        if not ticker:
            warnings.append(
                f"Could not resolve '{input_str}' to a ticker symbol. "
                "Try entering the ticker directly (e.g. AAPL for Apple)."
            )
            return ResolvedCompany(
                name=input_str,
                ticker=input_str.upper()[:10].replace(" ", ""),
                industry_id=INDUSTRY_MAP["business_services"],
                resolved_from="failed",
                confidence=0.2,
                warnings=warnings,
            )

    # ── Fetch from yfinance ───────────────────────────────────────
    info = _lookup_yfinance(ticker)

    if not info:
        warnings.append(
            f"yfinance returned no data for ticker '{ticker}'. "
            "The company may not be publicly traded or the ticker may be incorrect."
        )
        return ResolvedCompany(
            name=ticker,
            ticker=ticker,
            industry_id=INDUSTRY_MAP["business_services"],
            resolved_from="yfinance_failed",
            confidence=0.3,
            warnings=warnings,
        )

    # ── Map sector → industry ─────────────────────────────────────
    yf_sector    = info.get("sector", "") or ""
    industry_key, industry_id = _map_sector_to_industry(yf_sector)

    # ── Financials ────────────────────────────────────────────────
    market_cap        = info.get("marketCap")
    total_revenue     = info.get("totalRevenue")
    revenue_millions  = round(total_revenue / 1_000_000, 1) if total_revenue else None
    employee_count    = info.get("fullTimeEmployees")
    position_factor   = _calculate_position_factor(market_cap, yf_sector)
    mcp               = _calculate_market_cap_percentile(market_cap)

    # ── CIK from SEC ──────────────────────────────────────────────
    if not cik:
        cik = _lookup_sec_cik(ticker)

    # ── Fiscal year end ───────────────────────────────────────────
    fiscal_year_end = None
    try:
        import yfinance as yf
        fin = yf.Ticker(ticker).financials
        if fin is not None and not fin.empty:
            fiscal_year_end = fin.columns[0].strftime("%B")
    except Exception:
        pass

    return ResolvedCompany(
        name=info.get("longName", ticker),
        ticker=ticker,
        industry_id=industry_id,
        position_factor=round(position_factor, 3),
        sector=industry_key,
        sub_sector=info.get("industry", ""),
        revenue_millions=revenue_millions,
        employee_count=employee_count,
        market_cap_percentile=mcp,
        fiscal_year_end=fiscal_year_end,
        cik=cik,
        market_cap=market_cap,
        description=(info.get("longBusinessSummary", "") or "")[:500],
        website=info.get("website", ""),
        country=info.get("country", ""),
        resolved_from=f"yfinance+{name_source}",
        confidence=0.95,
        warnings=warnings,
    )


def format_resolution_preview(company: ResolvedCompany) -> str:
    """Format resolved company for display in Streamlit."""
    lines = [f"**{company.name}** ({company.ticker})"]
    if company.sector:
        lines.append(f"Sector: {company.sector} | Sub-sector: {company.sub_sector or 'Unknown'}")
    if company.revenue_millions:
        lines.append(f"Revenue: ${company.revenue_millions:,.0f}M")
    if company.employee_count:
        lines.append(f"Employees: {company.employee_count:,}")
    if company.cik:
        lines.append(f"SEC CIK: {company.cik}")
    for w in company.warnings:
        lines.append(f"⚠️ {w}")
    return "\n".join(lines)