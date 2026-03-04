"""CS1 Platform API client for company data."""
import httpx
from dataclasses import dataclass
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
    company_id: str
    ticker: str
    name: str
    sector: Sector
    sub_sector: str
    market_cap_percentile: float  # 0-1, for position_factor
    revenue_millions: float
    employee_count: int
    fiscal_year_end: str  # "December", "March", etc.

@dataclass
class Portfolio:
    """PE portfolio from CS1."""
    portfolio_id: str
    name: str
    company_ids: List[str]
    fund_vintage: int

class CS1Client:
    """Client for CS1 Platform API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)

    async def get_company(self, ticker: str) -> Company:
        """Fetch company by ticker symbol."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/companies/{ticker}"
        )
        response.raise_for_status()
        data = response.json()
        return Company(
            company_id=data["company_id"],
            ticker=data["ticker"],
            name=data["name"],
            sector=Sector(data["sector"]),
            sub_sector=data["sub_sector"],
            market_cap_percentile=data["market_cap_percentile"],
            revenue_millions=data["revenue_millions"],
            employee_count=data["employee_count"],
            fiscal_year_end=data["fiscal_year_end"],
        )

    async def list_companies(
        self,
        sector: Optional[Sector] = None,
        min_revenue: Optional[float] = None,
    ) -> List[Company]:
        """List companies with optional filters."""
        params = {}
        if sector:
            params["sector"] = sector.value
        if min_revenue:
            params["min_revenue"] = min_revenue

        response = await self.client.get(
            f"{self.base_url}/api/v1/companies",
            params=params
        )
        response.raise_for_status()
        return [Company(**c) for c in response.json()]

    async def get_portfolio_companies(
        self,
        portfolio_id: str,
    ) -> List[Company]:
        """Get all companies in a PE portfolio."""
        response = await self.client.get(
            f"{self.base_url}/api/v1/portfolios/{portfolio_id}/companies"
        )
        response.raise_for_status()
        return [Company(**c) for c in response.json()]

    async def close(self):
        await self.client.aclose()
