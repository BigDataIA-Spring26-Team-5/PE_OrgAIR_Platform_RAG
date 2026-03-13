"""
Company Router - PE Org-AI-R Platform
app/routers/companies.py

Handles company CRUD operations with Redis caching.
"""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Query, status
from pydantic import BaseModel, Field, root_validator

from app.core.dependencies import get_company_repository, get_industry_repository
from app.core.exceptions import raise_error
from app.repositories.company_repository import CompanyRepository
from app.repositories.industry_repository import IndustryRepository
from app.services.cache import (
    CacheInfo,
    TTL_COMPANY,
    cached_query,
    create_cache_info,
    get_cache,
)
from app.services.groq_enrichment import enrich_company_metadata, enrich_portfolio_metadata, get_dimension_keywords

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Companies"])



#  Schemas


class CompanyBase(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=10)
    industry_id: Optional[UUID] = None
    position_factor: float = Field(default=0.0, ge=-1.0, le=1.0)

    @root_validator(pre=True)
    @classmethod
    def uppercase_ticker(cls, values):
        if 'ticker' in values and values['ticker']:
            values['ticker'] = values['ticker'].upper()
        return values


class CompanyCreate(CompanyBase):
    name: str = Field(..., min_length=1, max_length=255)
    industry_id: UUID


class CompanyUpdate(CompanyBase):
    pass


class CompanyResponse(BaseModel):
    id: UUID
    name: str
    ticker: Optional[str] = None
    industry_id: UUID
    position_factor: float
    # CS4 enriched fields
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    market_cap_percentile: Optional[float] = None
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    fiscal_year_end: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    cache: Optional[CacheInfo] = None

    class Config:
        from_attributes = True


class PortfolioCompaniesResponse(BaseModel):
    portfolio_id: str
    name: str
    fund_vintage: Optional[int] = None
    companies: List[CompanyResponse]
    total: int


class DimensionKeywordsResponse(BaseModel):
    ticker: str
    dimension: str
    keywords: List[str]


class CompanyListResponse(BaseModel):
    """Response for get all companies (no pagination)."""
    items: list[CompanyResponse]
    total: int
    cache: Optional[CacheInfo] = None


class PaginatedCompanyResponse(BaseModel):
    items: list[CompanyResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
    cache: Optional[CacheInfo] = None


BACKFILL_DEFAULT_TICKERS = [
        "CAT","DE","UNH","HCA","ADP","PAYX","WMT","TGT","DG","JPM","GS","NVDA","GE","NFLX","GOOGL","AAPL"
]


class BackfillRequest(BaseModel):
    tickers: List[str] = Field(
        default=BACKFILL_DEFAULT_TICKERS,
        description="Ticker symbols to backfill from yfinance",
        example=BACKFILL_DEFAULT_TICKERS,
    )


class BackfillResult(BaseModel):
    ticker: str
    status: str  # "created" | "updated" | "skipped" | "failed"
    company: Optional[CompanyResponse] = None
    message: str = ""


class BackfillResponse(BaseModel):
    results: List[BackfillResult]
    created: int
    updated: int
    skipped: int
    failed: int



#  Exception Helpers


def raise_company_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "COMPANY_NOT_FOUND", "Company not found")

def raise_industry_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "INDUSTRY_NOT_FOUND", "Industry does not exist")

def raise_company_deleted():
    raise_error(status.HTTP_410_GONE, "COMPANY_DELETED", "Company has been deleted")

def raise_duplicate_company():
    raise_error(status.HTTP_409_CONFLICT, "DUPLICATE_COMPANY", "Company already exists in this industry")

def raise_validation_error(msg: str):
    raise_error(status.HTTP_422_UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", msg)



#  Cache Helpers


CACHE_KEY_COMPANY_PREFIX = "company:"
CACHE_KEY_COMPANIES_LIST_PREFIX = "companies:list:"
CACHE_KEY_COMPANIES_ALL = "companies:all"
CACHE_KEY_COMPANIES_BY_INDUSTRY = "companies:industry:"


def get_company_cache_key(company_id: UUID) -> str:
    return f"{CACHE_KEY_COMPANY_PREFIX}{company_id}"


def get_companies_list_cache_key(page: int, page_size: int, industry_id: Optional[UUID], min_revenue: Optional[float] = None) -> str:
    return f"{CACHE_KEY_COMPANIES_LIST_PREFIX}page:{page}:size:{page_size}:industry:{industry_id}:min_revenue:{min_revenue}"


def get_companies_by_industry_cache_key(industry_id: UUID) -> str:
    return f"{CACHE_KEY_COMPANIES_BY_INDUSTRY}{industry_id}"


def invalidate_company_cache(company_id: Optional[UUID] = None) -> None:
    """Invalidate company cache entries in Redis."""
    cache = get_cache()
    if cache:
        try:
            if company_id:
                cache.delete(get_company_cache_key(company_id))
            cache.delete_pattern(f"{CACHE_KEY_COMPANIES_LIST_PREFIX}*")
            cache.delete(CACHE_KEY_COMPANIES_ALL)
            cache.delete_pattern(f"{CACHE_KEY_COMPANIES_BY_INDUSTRY}*")
        except Exception:
            pass



#  Helper Functions


def row_to_response(row: dict, cache_info: Optional[CacheInfo] = None) -> CompanyResponse:
    return CompanyResponse(
        id=UUID(row["id"]),
        name=row["name"],
        ticker=row["ticker"],
        industry_id=UUID(row["industry_id"]),
        position_factor=float(row["position_factor"]),
        sector=row.get("sector"),
        sub_sector=row.get("sub_sector"),
        market_cap_percentile=float(row["market_cap_percentile"]) if row.get("market_cap_percentile") is not None else None,
        revenue_millions=float(row["revenue_millions"]) if row.get("revenue_millions") is not None else None,
        employee_count=int(row["employee_count"]) if row.get("employee_count") is not None else None,
        fiscal_year_end=row.get("fiscal_year_end"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        cache=cache_info,
    )



#  Identifier Resolver


def resolve_company_identifier(
    ticker: str, company_repo: CompanyRepository
) -> dict:
    """Accept either a UUID string or a ticker; return the company row or raise 404."""
    import uuid as _uuid
    try:
        company_id = _uuid.UUID(ticker)
        company = company_repo.get_by_id(company_id)
    except ValueError:
        company = company_repo.get_by_ticker(ticker)
    if company is None:
        raise_error(status.HTTP_404_NOT_FOUND, "COMPANY_NOT_FOUND", f"Company '{ticker}' not found")
    return company


#  Routes


def _enrich_company_in_background(company_id: UUID, ticker: str, name: str, company_repo: CompanyRepository) -> None:
    """Background task: call Groq to fill in enriched fields, create portfolio, and persist."""
    try:
        # 1. Enrich company metadata fields
        enriched = enrich_company_metadata(ticker, name)
        if enriched:
            company_repo.update_enriched_fields(company_id, **enriched)
            logger.info("Groq company enrichment complete for %s", ticker)

        # 2. Enrich portfolio metadata and create portfolio entry
        portfolio_data = enrich_portfolio_metadata(ticker, name)
        portfolio_id = company_repo.create_portfolio(
            name=portfolio_data["portfolio_name"],
            fund_vintage=portfolio_data.get("fund_vintage"),
        )
        company_repo.add_company_to_portfolio(portfolio_id, str(company_id))
        logger.info("Portfolio created for %s: id=%s name='%s'", ticker, portfolio_id, portfolio_data["portfolio_name"])
    except Exception as exc:
        logger.warning("Groq enrichment background task failed for %s: %s", ticker, exc)


def _resolve_and_backfill_ticker(ticker: str, company_repo: CompanyRepository) -> BackfillResult:
    """Resolve ticker via yfinance (app.utils.company_resolver) and upsert into Snowflake.
    Only fills NULL columns — never overwrites existing data.
    """
    from app.utils.company_resolver import resolve_company
    try:
        resolved = resolve_company(ticker)
        existing = company_repo.get_by_ticker(resolved.ticker)

        if existing is None:
            data = company_repo.create(
                name=resolved.name,
                industry_id=UUID(resolved.industry_id),
                ticker=resolved.ticker,
                position_factor=resolved.position_factor,
                sector=resolved.sector,
                sub_sector=resolved.sub_sector,
                market_cap_percentile=resolved.market_cap_percentile,
                revenue_millions=resolved.revenue_millions,
                employee_count=resolved.employee_count,
                fiscal_year_end=resolved.fiscal_year_end,
            )
            invalidate_company_cache()
            return BackfillResult(
                ticker=resolved.ticker, status="created",
                company=row_to_response(data),
                message=f"Created (confidence={resolved.confidence})",
            )

        # Exists — only fill NULL enriched columns
        updates = {}
        for field_name in ["sector", "sub_sector", "market_cap_percentile",
                           "revenue_millions", "employee_count", "fiscal_year_end"]:
            if existing.get(field_name) is None:
                val = getattr(resolved, field_name)
                if val is not None:
                    updates[field_name] = val

        if not updates:
            return BackfillResult(
                ticker=resolved.ticker, status="skipped",
                company=row_to_response(existing),
                message="All enriched fields already populated",
            )

        company_id = UUID(str(existing["id"]))
        company_repo.update_enriched_fields(company_id, **updates)
        invalidate_company_cache(company_id)
        updated_data = company_repo.get_by_id(company_id)
        return BackfillResult(
            ticker=resolved.ticker, status="updated",
            company=row_to_response(updated_data),
            message=f"Filled null fields: {', '.join(updates.keys())}",
        )
    except Exception as exc:
        logger.warning("backfill_failed ticker=%s error=%s", ticker, exc)
        return BackfillResult(ticker=ticker, status="failed", message=str(exc))


@router.post(
    "/companies",
    response_model=CompanyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new company",
    description=(
        "Creates a new company. Validates schema, checks industry existence, and enforces uniqueness. "
        "Kicks off a background Groq enrichment to fill sub_sector, market_cap_percentile, "
        "revenue_millions, employee_count, and fiscal_year_end for any ticker."
    ),
)
async def create_company(
    company: CompanyCreate,
    background_tasks: BackgroundTasks,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyResponse:
    if not industry_repo.exists(company.industry_id):
        raise_industry_not_found()

    if company_repo.check_duplicate(company.name, company.industry_id):
        raise_duplicate_company()

    company_data = company_repo.create(
        name=company.name,
        industry_id=company.industry_id,
        ticker=company.ticker,
        position_factor=company.position_factor,
    )

    # Kick off Groq enrichment in the background so the response returns immediately
    if company.ticker:
        background_tasks.add_task(
            _enrich_company_in_background,
            UUID(str(company_data["id"])),
            company.ticker,
            company.name,
            company_repo,
        )

    invalidate_company_cache()

    return row_to_response(company_data)


@router.post(
    "/companies/backfill",
    response_model=BackfillResponse,
    summary="Bulk backfill companies from yfinance",
    description=(
        "Fetches financial metadata from yfinance for each ticker and upserts into Snowflake. "
        "Creates the company if it doesn't exist; if it exists, only fills NULL columns. "
        "Already-populated fields are never overwritten."
    ),
)
async def backfill_companies(
    request: BackfillRequest,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> BackfillResponse:
    results = [_resolve_and_backfill_ticker(t.upper(), company_repo) for t in request.tickers]
    return BackfillResponse(
        results=results,
        created=sum(1 for r in results if r.status == "created"),
        updated=sum(1 for r in results if r.status == "updated"),
        skipped=sum(1 for r in results if r.status == "skipped"),
        failed=sum(1 for r in results if r.status == "failed"),
    )


@router.post(
    "/companies/{ticker}/backfill",
    response_model=BackfillResult,
    summary="Backfill a single company from yfinance",
    description=(
        "Fetches yfinance metadata for the given ticker and upserts into Snowflake. "
        "Creates the company if absent; otherwise fills only NULL columns."
    ),
)
async def backfill_company(
    ticker: str,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> BackfillResult:
    return _resolve_and_backfill_ticker(ticker.upper(), company_repo)


@router.get(
    "/companies/all",
    response_model=CompanyListResponse,
    summary="Get all companies",
    description="Returns all companies without pagination. Cached for 5 minutes.",
)
async def get_all_companies(
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> CompanyListResponse:
    cache_key = CACHE_KEY_COMPANIES_ALL

    def _fetch():
        companies = company_repo.get_all()
        return CompanyListResponse(items=[row_to_response(c) for c in companies], total=len(companies))

    result, hit, latency = cached_query(cache_key, TTL_COMPANY, CompanyListResponse, _fetch)
    result.cache = create_cache_info(hit, cache_key, latency, TTL_COMPANY)
    return result


@router.get(
    "/companies/industry/{industry_id}",
    response_model=CompanyListResponse,
    summary="Get companies by industry",
    description="Returns all companies for a specific industry. Cached for 5 minutes.",
)
async def get_companies_by_industry(
    industry_id: UUID,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyListResponse:
    # Check industry exists
    if not industry_repo.exists(industry_id):
        raise_industry_not_found()

    cache_key = get_companies_by_industry_cache_key(industry_id)

    def _fetch():
        companies = company_repo.get_by_industry(industry_id)
        return CompanyListResponse(items=[row_to_response(c) for c in companies], total=len(companies))

    result, hit, latency = cached_query(cache_key, TTL_COMPANY, CompanyListResponse, _fetch)
    result.cache = create_cache_info(hit, cache_key, latency, TTL_COMPANY)
    return result


@router.get(
    "/companies",
    response_model=PaginatedCompanyResponse,
    summary="List companies (paginated)",
    description="Returns a paginated list of companies. Optionally filter by industry and/or minimum revenue. Cached for 5 minutes.",
)
async def list_companies(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    industry_id: Optional[UUID] = Query(default=None),
    min_revenue: Optional[float] = Query(default=None, description="Minimum annual revenue in USD millions"),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> PaginatedCompanyResponse:
    cache_key = get_companies_list_cache_key(page, page_size, industry_id, min_revenue)

    def _fetch():
        all_companies = company_repo.get_by_industry(industry_id) if industry_id else company_repo.get_all()
        if min_revenue is not None:
            all_companies = [
                c for c in all_companies
                if c.get("revenue_millions") is not None and float(c["revenue_millions"]) >= min_revenue
            ]
        total = len(all_companies)
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        start_idx = (page - 1) * page_size
        companies = all_companies[start_idx:start_idx + page_size]
        return PaginatedCompanyResponse(
            items=[row_to_response(c) for c in companies],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
        )

    result, hit, latency = cached_query(cache_key, TTL_COMPANY, PaginatedCompanyResponse, _fetch)
    result.cache = create_cache_info(hit, cache_key, latency, TTL_COMPANY)
    return result


@router.get(
    "/companies/{ticker}",
    response_model=CompanyResponse,
    summary="Get company by ID or ticker",
    description="Retrieves a company by UUID or ticker symbol (case-insensitive). Cached for 5 minutes.",
)
async def get_company(
    ticker: str,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> CompanyResponse:
    company = resolve_company_identifier(ticker, company_repo)
    company_id = UUID(str(company["id"]))

    cache_key = get_company_cache_key(company_id)

    def _fetch():
        return row_to_response(company_repo.get_by_id(company_id))

    result, hit, latency = cached_query(cache_key, TTL_COMPANY, CompanyResponse, _fetch)
    result.cache = create_cache_info(hit, cache_key, latency, TTL_COMPANY)
    return result


@router.put(
    "/companies/{ticker}",
    response_model=CompanyResponse,
    summary="Update company",
    description="Updates company data by UUID or ticker symbol (case-insensitive). Invalidates cache.",
)
async def update_company(
    ticker: str,
    company: CompanyUpdate,
    company_repo: CompanyRepository = Depends(get_company_repository),
    industry_repo: IndustryRepository = Depends(get_industry_repository),
) -> CompanyResponse:
    existing = resolve_company_identifier(ticker, company_repo)
    id = UUID(str(existing["id"]))

    if existing.get("is_deleted"):
        raise_company_deleted()

    update_data = company.model_dump(exclude_unset=True)

    if not update_data:
        return row_to_response(existing)

    if "industry_id" in update_data and str(update_data["industry_id"]) != existing["industry_id"]:
        if not industry_repo.exists(update_data["industry_id"]):
            raise_industry_not_found()

    new_name = update_data.get("name", existing["name"])
    new_industry_id = update_data.get("industry_id", UUID(existing["industry_id"]))

    if new_name != existing["name"] or str(new_industry_id) != existing["industry_id"]:
        if company_repo.check_duplicate(new_name, new_industry_id, exclude_id=id):
            raise_duplicate_company()

    updated = company_repo.update(
        company_id=id,
        name=update_data.get("name"),
        ticker=update_data.get("ticker"),
        industry_id=update_data.get("industry_id"),
        position_factor=update_data.get("position_factor"),
    )

    invalidate_company_cache(id)

    return row_to_response(updated)


@router.delete(
    "/companies/{ticker}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Soft delete company",
    description="Marks a company as deleted by UUID or ticker symbol (case-insensitive). Invalidates cache.",
)
async def delete_company(
    ticker: str,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> None:
    existing = resolve_company_identifier(ticker, company_repo)
    id = UUID(str(existing["id"]))

    if existing.get("is_deleted"):
        raise_company_deleted()

    company_repo.soft_delete(id)
    invalidate_company_cache(id)


@router.get(
    "/portfolios/{portfolio_id}/companies",
    response_model=PortfolioCompaniesResponse,
    summary="Get all companies in a PE portfolio",
    description="Returns all active companies belonging to the given portfolio UUID.",
)
async def get_portfolio_companies(
    portfolio_id: str,
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> PortfolioCompaniesResponse:
    portfolio = company_repo.get_portfolio(portfolio_id)
    if not portfolio:
        raise_error(status.HTTP_404_NOT_FOUND, "PORTFOLIO_NOT_FOUND", f"Portfolio '{portfolio_id}' not found")

    companies = company_repo.get_by_portfolio(portfolio_id)
    return PortfolioCompaniesResponse(
        portfolio_id=str(portfolio["id"]),
        name=portfolio["name"],
        fund_vintage=portfolio.get("fund_vintage"),
        companies=[row_to_response(c) for c in companies],
        total=len(companies),
    )


# Default rubric keywords per dimension — expanded dynamically by Groq
_BASE_DIMENSION_KEYWORDS = {
    "data_infrastructure": ["data lake", "data warehouse", "ETL", "data pipeline", "real-time data", "cloud storage"],
    "ai_governance": ["AI ethics", "model governance", "responsible AI", "bias detection", "explainability", "AI policy"],
    "technology_stack": ["machine learning platform", "MLOps", "Kubernetes", "cloud-native", "microservices", "API gateway"],
    "talent": ["machine learning engineer", "data scientist", "AI researcher", "NLP", "computer vision", "deep learning"],
    "leadership": ["Chief AI Officer", "AI strategy", "digital transformation", "technology roadmap", "innovation lab"],
    "use_case_portfolio": ["AI use case", "automation", "predictive analytics", "recommendation system", "computer vision"],
    "culture": ["data-driven", "experimentation", "agile", "innovation culture", "AI adoption", "continuous learning"],
}


@router.get(
    "/companies/{ticker}/dimension-keywords",
    response_model=DimensionKeywordsResponse,
    summary="Get Groq-expanded scoring keywords for a company and dimension",
    description=(
        "Returns base rubric keywords expanded with company-specific synonyms via Groq. "
        "Used by the CS4 RAG search to improve evidence retrieval quality."
    ),
)
async def get_dimension_keywords_endpoint(
    ticker: str,
    dimension: str = Query(..., description="One of the 7 V^R dimensions, e.g. data_infrastructure"),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> DimensionKeywordsResponse:
    ticker = ticker.upper()
    base_keywords = _BASE_DIMENSION_KEYWORDS.get(dimension, [])

    company = company_repo.get_by_ticker(ticker)
    if not company:
        # Return base keywords even if company not yet in DB (e.g. during onboarding)
        return DimensionKeywordsResponse(ticker=ticker, dimension=dimension, keywords=base_keywords)

    expanded = get_dimension_keywords(
        ticker=ticker,
        company_name=company["name"],
        dimension=dimension,
        base_keywords=base_keywords,
    )
    return DimensionKeywordsResponse(ticker=ticker, dimension=dimension, keywords=expanded)
