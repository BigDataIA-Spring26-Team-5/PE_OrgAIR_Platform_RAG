"""CS1 Platform API client for company data.

All endpoints call the single pe-org-air-platform FastAPI at localhost:8000.
"""
import httpx
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum
import structlog

logger = structlog.get_logger()


class Sector(str, Enum):
    TECHNOLOGY = "technology"
    FINANCIAL_SERVICES = "financial_services"
    HEALTHCARE = "healthcare"
    MANUFACTURING = "manufacturing"
    RETAIL = "retail"
    BUSINESS_SERVICES = "business_services"
    CONSUMER = "consumer"


@dataclass
class Company:
    """Company from CS1 Platform."""
    company_id: str           # UUID from the platform (field 'id' in API response)
    ticker: str
    name: str
    sector: Optional[Sector]  # populated by Groq enrichment; may be None briefly after creation
    sub_sector: Optional[str]
    market_cap_percentile: Optional[float]  # 0-1, for position_factor
    revenue_millions: Optional[float]
    employee_count: Optional[int]
    fiscal_year_end: Optional[str]  # "December", "March", etc.


@dataclass
class Portfolio:
    """PE portfolio from CS1."""
    portfolio_id: str
    name: str
    company_ids: List[str] = field(default_factory=list)
    fund_vintage: Optional[int] = None


def _parse_company(data: dict) -> Company:
    """Parse a CompanyResponse dict from the platform API into a Company dataclass."""
    raw_sector = data.get("sector")
    try:
        sector = Sector(raw_sector) if raw_sector else None
    except ValueError:
        sector = None
    return Company(
        company_id=str(data["id"]),
        ticker=data.get("ticker", ""),
        name=data["name"],
        sector=sector,
        sub_sector=data.get("sub_sector"),
        market_cap_percentile=data.get("market_cap_percentile"),
        revenue_millions=data.get("revenue_millions"),
        employee_count=data.get("employee_count"),
        fiscal_year_end=data.get("fiscal_year_end"),
    )


class CS1Client:
    """Client for CS1 Platform API — pe-org-air-platform at localhost:8000."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)

    async def get_company(self, ticker: str) -> Company:
        """Fetch company by ticker symbol. Calls GET /api/v1/companies/{ticker}."""
        response = await self.client.get(f"{self.base_url}/api/v1/companies/{ticker}")
        response.raise_for_status()
        return _parse_company(response.json())

    async def list_companies(
        self,
        sector: Optional[Sector] = None,
        min_revenue: Optional[float] = None,
    ) -> List[Company]:
        """List all companies, optionally filtered by sector or minimum revenue.
        Calls GET /api/v1/companies/all and filters client-side.
        """
        response = await self.client.get(f"{self.base_url}/api/v1/companies/all")
        response.raise_for_status()
        companies = [_parse_company(c) for c in response.json().get("items", [])]

        if sector:
            companies = [c for c in companies if c.sector == sector]
        if min_revenue is not None:
            companies = [c for c in companies if c.revenue_millions is not None and c.revenue_millions >= min_revenue]
        return companies

    async def get_portfolio_companies(self, portfolio_id: str) -> List[Company]:
        """Get all companies in a PE portfolio. Calls GET /api/v1/portfolios/{portfolio_id}/companies."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/portfolios/{portfolio_id}/companies"
        )
        response.raise_for_status()
        data = response.json()
        return [_parse_company(c) for c in data.get("companies", [])]

    async def get_portfolio(self, portfolio_id: str) -> Portfolio:
        """Get portfolio metadata and its company IDs."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/portfolios/{portfolio_id}/companies"
        )
        response.raise_for_status()
        data = response.json()
        return Portfolio(
            portfolio_id=data["portfolio_id"],
            name=data["name"],
            fund_vintage=data.get("fund_vintage"),
            company_ids=[str(c["id"]) for c in data.get("companies", [])],
        )

    async def close(self):
        await self.client.aclose()
