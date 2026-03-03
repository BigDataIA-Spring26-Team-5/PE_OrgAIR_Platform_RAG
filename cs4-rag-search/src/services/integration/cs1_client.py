"""
cs1_client.py — CS4 RAG Search
src/services/integration/cs1_client.py

HTTP client for the CS1 company layer (pe-org-air-platform /companies/* endpoints).
Data models derived from:
  - app/models/company.py              (CompanyResponse)
  - app/pipelines/board_analyzer.py    (CompanyRegistry.COMPANIES sector strings)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Sector(str, Enum):
    """
    Sector classifications extracted from CompanyRegistry.COMPANIES
    (app/pipelines/board_analyzer.py lines 62–85).
    """
    TECHNOLOGY          = "technology"
    FINANCIAL_SERVICES  = "financial_services"
    RETAIL              = "retail"
    MANUFACTURING       = "manufacturing"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class Company:
    """
    Company record.
    Core fields map from CompanyResponse (app/models/company.py):
      company_id  ← id
      name        ← name
      ticker      ← ticker_symbol
    Seven additional optional fields for PE context (not yet in platform API).
    """
    company_id: str
    name: str
    ticker: str
    # Optional fields not in CompanyResponse — default to None
    sector: Optional[Sector] = None
    sub_sector: Optional[str] = None
    market_cap_percentile: Optional[float] = None   # 0–100
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    fiscal_year_end: Optional[date] = None
    industry_id: Optional[str] = None
    position_factor: Optional[float] = None         # -1.0 to 1.0

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any]) -> "Company":
        ticker_raw = data.get("ticker_symbol") or data.get("ticker", "")
        return cls(
            company_id=str(data.get("id", "")),
            name=data.get("name", ""),
            ticker=ticker_raw.upper() if ticker_raw else "",
            industry_id=str(data.get("industry_id")) if data.get("industry_id") else None,
            position_factor=data.get("position_factor"),
        )


@dataclass
class Portfolio:
    """
    A named collection of companies for batch IC prep.
    CS4 new — no platform equivalent.
    """
    name: str
    companies: List[Company] = field(default_factory=list)
    created_at: Optional[datetime] = None
    description: Optional[str] = None

    def tickers(self) -> List[str]:
        return [c.ticker for c in self.companies]


# ---------------------------------------------------------------------------
# CS1Client
# ---------------------------------------------------------------------------

class CS1Client:
    """
    Async HTTP client for the CS1 company endpoints.
    Base URL defaults to CS1_BASE_URL env var (fallback: http://localhost:8000).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = (base_url or os.getenv("CS1_BASE_URL", "http://localhost:8000")).rstrip("/")
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)
        self._log = logger.bind(client="cs1")

    async def get_company(self, ticker: str) -> Company:
        """
        GET /companies?ticker={ticker}
        Platform returns paginated list; we pick the first match.
        """
        self._log.info("fetching company", ticker=ticker)
        resp = await self._client.get("/companies", params={"ticker": ticker.upper()})
        resp.raise_for_status()
        data = resp.json()
        items: List[Dict[str, Any]] = data.get("items", [data] if "id" in data else [])
        if not items:
            raise ValueError(f"Company not found for ticker: {ticker}")
        return Company.from_api_dict(items[0])

    async def list_companies(self, page: int = 1, page_size: int = 50) -> List[Company]:
        """GET /companies — paginated."""
        self._log.info("listing companies", page=page)
        resp = await self._client.get("/companies", params={"page": page, "page_size": page_size})
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        return [Company.from_api_dict(row) for row in items]

    async def get_portfolio_companies(self, portfolio_name: str) -> Portfolio:
        """
        Fetch all companies belonging to a named portfolio.
        Stubbed — no /portfolios endpoint exists on the platform yet.
        Returns a Portfolio with all active companies as a fallback.
        """
        self._log.warning(
            "get_portfolio_companies: no platform endpoint — returning all companies",
            portfolio_name=portfolio_name,
        )
        companies = await self.list_companies()
        return Portfolio(name=portfolio_name, companies=companies)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CS1Client":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
