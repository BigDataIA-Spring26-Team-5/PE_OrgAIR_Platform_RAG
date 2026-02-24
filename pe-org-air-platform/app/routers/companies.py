"""
Company Router - PE Org-AI-R Platform
app/routers/companies.py

Handles company CRUD operations with Redis caching.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
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
)

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
    created_at: datetime
    updated_at: datetime
    cache: Optional[CacheInfo] = None

    class Config:
        from_attributes = True


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


def get_companies_list_cache_key(page: int, page_size: int, industry_id: Optional[UUID]) -> str:
    return f"{CACHE_KEY_COMPANIES_LIST_PREFIX}page:{page}:size:{page_size}:industry:{industry_id}"


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


@router.post(
    "/companies",
    response_model=CompanyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new company",
    description="Creates a new company. Validates schema, checks industry existence, and enforces uniqueness.",
)
async def create_company(
    company: CompanyCreate,
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

    invalidate_company_cache()

    return row_to_response(company_data)


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
    description="Returns a paginated list of companies. Cached for 5 minutes.",
)
async def list_companies(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    industry_id: Optional[UUID] = Query(default=None),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> PaginatedCompanyResponse:
    cache_key = get_companies_list_cache_key(page, page_size, industry_id)

    def _fetch():
        all_companies = company_repo.get_by_industry(industry_id) if industry_id else company_repo.get_all()
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