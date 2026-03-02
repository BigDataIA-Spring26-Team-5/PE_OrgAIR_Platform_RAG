"""
Industry Router - PE Org-AI-R Platform
app/routers/industries.py

Handles industry-related endpoints with Redis caching.
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from app.core.dependencies import get_industry_repository
from app.core.exceptions import raise_error
from app.repositories.industry_repository import IndustryRepository
from app.services.cache import CacheInfo, TTL_INDUSTRY, cached_query, create_cache_info

router = APIRouter(prefix="/api/v1", tags=["Industries"])





#  Schemas


class ErrorResponse(BaseModel):
    error_code: str
    message: str
    details: Optional[dict] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IndustryResponse(BaseModel):
    id: UUID
    name: str
    sector: str
    h_r_base: float
    cache: Optional[CacheInfo] = None  # Cache info for debugging

    class Config:
        from_attributes = True


class IndustryListResponse(BaseModel):
    items: list[IndustryResponse]
    total: int
    cache: Optional[CacheInfo] = None  # Cache info for debugging



#  Exception Helpers


def raise_industry_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "INDUSTRY_NOT_FOUND", "Industry not found")


def raise_internal_error():
    raise_error(status.HTTP_500_INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "Unexpected server error")



#  Cache Helpers


CACHE_KEY_INDUSTRY_LIST = "industry:list"
CACHE_KEY_INDUSTRY_PREFIX = "industry:"


def get_industry_cache_key(industry_id: UUID) -> str:
    """Generate cache key for a single industry."""
    return f"{CACHE_KEY_INDUSTRY_PREFIX}{industry_id}"


def invalidate_industry_cache(industry_id: Optional[UUID] = None) -> None:
    """Invalidate industry cache entries."""
    cache = get_cache()
    if cache:
        try:
            cache.delete(CACHE_KEY_INDUSTRY_LIST)
            if industry_id:
                cache.delete(get_industry_cache_key(industry_id))
        except Exception:
            pass



#  Helper Functions


def row_to_response(row: dict, cache_info: Optional[CacheInfo] = None) -> IndustryResponse:
    """Convert database row to response model."""
    return IndustryResponse(
        id=UUID(row["id"]),
        name=row["name"],
        sector=row["sector"],
        h_r_base=float(row["h_r_base"]),
        cache=cache_info,
    )



#  Routes


@router.get(
    "/industries",
    response_model=IndustryListResponse,
    responses={
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T01:19:36.806Z",
                    }
                }
            },
        },
    },
    summary="List industries",
    description="Returns all available industries. Cached for 1 hour.",
)
async def list_industries(
    repo: IndustryRepository = Depends(get_industry_repository),
) -> IndustryListResponse:
    def _fetch():
        rows = repo.get_all()
        items = [row_to_response(ind) for ind in rows]
        return IndustryListResponse(items=items, total=len(items))

    result, hit, latency = cached_query(CACHE_KEY_INDUSTRY_LIST, TTL_INDUSTRY, IndustryListResponse, _fetch)
    result.cache = create_cache_info(hit, CACHE_KEY_INDUSTRY_LIST, latency, TTL_INDUSTRY)
    return result


@router.get(
    "/industries/{id}",
    response_model=IndustryResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Industry not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INDUSTRY_NOT_FOUND",
                        "message": "Industry not found",
                        "details": None,
                        "timestamp": "2026-01-28T01:19:36.803Z",
                    }
                }
            },
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "ID must be a valid UUID format",
                        "details": {"field": "id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T01:19:36.805Z",
                    }
                }
            },
        },
        500: {
            "model": ErrorResponse,
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INTERNAL_SERVER_ERROR",
                        "message": "Unexpected server error",
                        "details": None,
                        "timestamp": "2026-01-28T01:19:36.806Z",
                    }
                }
            },
        },
    },
    summary="Get industry by ID",
    description="Retrieves a single industry by UUID. Cached for 1 hour.",
)
async def get_industry(
    id: UUID,
    repo: IndustryRepository = Depends(get_industry_repository),
) -> IndustryResponse:
    cache_key = get_industry_cache_key(id)

    def _fetch():
        industry = repo.get_by_id(id)
        if not industry:
            raise_industry_not_found()
        return row_to_response(industry)

    result, hit, latency = cached_query(cache_key, TTL_INDUSTRY, IndustryResponse, _fetch)
    result.cache = create_cache_info(hit, cache_key, latency, TTL_INDUSTRY)
    return result