from pydantic import BaseModel, Field, field_validator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional, List


class CompanyBase(BaseModel):
    """
    Base Pydantic model for Company.
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Company name"
    )

    ticker_symbol: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=10,
        description="Optional stock ticker symbol (uppercase)"
    )

    industry_id: UUID = Field(
        ...,
        description="Foreign key reference to Industry"
    )

    position_factor: float = Field(
        default=0.0,
        ge=-1.0,
        le=1.0,
        description="Adjustment factor between -1.0 and 1.0"
    )

    # CS4 enriched fields (populated by Groq on company creation)
    sector: Optional[str] = Field(
        default=None,
        description="High-level sector (e.g. technology, retail)"
    )
    sub_sector: Optional[str] = Field(
        default=None,
        description="Sub-sector within the sector"
    )
    market_cap_percentile: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Market cap percentile within sector peers (0-1)"
    )
    revenue_millions: Optional[float] = Field(
        default=None,
        description="Annual revenue in USD millions"
    )
    employee_count: Optional[int] = Field(
        default=None,
        description="Total headcount"
    )
    fiscal_year_end: Optional[str] = Field(
        default=None,
        description="Month of fiscal year end (e.g. December, March)"
    )

    @field_validator("ticker_symbol")
    @classmethod
    def uppercase_ticker(cls, value: Optional[str]) -> Optional[str]:
        return value.upper() if value else None


class CompanyCreate(CompanyBase):
    """
    Model for creating a new company.
    """
    pass


class CompanyUpdate(CompanyBase):
    """
    Model for updating an existing company.
    """
    pass


class CompanyResponse(CompanyBase):
    """
    Model returned in API responses.
    """

    id: UUID = Field(
        default_factory=uuid4,
        description="Unique company identifier"
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Record creation timestamp"
    )

    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Record last update timestamp"
    )

    class Config:
        from_attributes = True


class PaginatedCompanyResponse(BaseModel):
    """
    Paginated response for listing companies.
    """

    items: List[CompanyResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class PortfolioResponse(BaseModel):
    """Response model for a PE portfolio."""
    portfolio_id: str
    name: str
    fund_vintage: Optional[int] = None
    company_ids: List[str] = Field(default_factory=list)
