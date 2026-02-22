"""
Assessment Router - PE Org-AI-R Platform
app/routers/assessments.py

Handles assessment CRUD operations with Snowflake storage and Redis caching.
"""

from datetime import datetime, timezone
from typing import Dict, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Query, status

from app.core.dependencies import get_assessment_repository, get_company_repository
from app.core.exceptions import raise_error
from app.models.assessment import (
    AssessmentCreate,
    AssessmentResponse,
    ErrorResponse,
    PaginatedAssessmentResponse,
    StatusUpdate,
)
from app.models.enumerations import AssessmentStatus, AssessmentType
from app.repositories.assessment_repository import AssessmentRepository
from app.repositories.company_repository import CompanyRepository
from app.services.cache import get_cache, TTL_ASSESSMENT, invalidate_assessment_cache

router = APIRouter(prefix="/api/v1/assessments", tags=["Assessments"])




#  Exception Helpers


def raise_bad_request(msg: str = "Malformed JSON request"):
    raise_error(status.HTTP_400_BAD_REQUEST, "INVALID_REQUEST", msg)


def raise_assessment_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "ASSESSMENT_NOT_FOUND", "Assessment not found")


def raise_company_not_found():
    raise_error(status.HTTP_404_NOT_FOUND, "COMPANY_NOT_FOUND", "Company does not exist")


def raise_validation_error(msg: str):
    raise_error(status.HTTP_422_UNPROCESSABLE_ENTITY, "VALIDATION_ERROR", msg)


def raise_internal_error():
    raise_error(status.HTTP_500_INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "Unexpected server error")


#  Routes

@router.post(
    "",
    response_model=AssessmentResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Invalid request",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "INVALID_REQUEST",
                        "message": "Malformed JSON request body",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        404: {
            "model": ErrorResponse,
            "description": "Company not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "COMPANY_NOT_FOUND",
                        "message": "Company does not exist",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Company ID must be a valid UUID format",
                        "details": {"field": "company_id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
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
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Create a new assessment",
    description="Creates a new assessment for a company. Validates that the company exists and sets initial status to 'draft'.",
)
async def create_assessment(
    payload: AssessmentCreate,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
    company_repo: CompanyRepository = Depends(get_company_repository),
) -> AssessmentResponse:
    # Company existence check
    if not company_repo.exists_active(payload.company_id):
        raise_company_not_found()

    # Create assessment in Snowflake
    assessment_data = assessment_repo.create(
        company_id=payload.company_id,
        assessment_type=payload.assessment_type,
        assessment_date=payload.assessment_date,
        primary_assessor=payload.primary_assessor,
        secondary_assessor=payload.secondary_assessor,
    )

    return AssessmentResponse(**assessment_data)


@router.get(
    "",
    response_model=PaginatedAssessmentResponse,
    responses={
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Page must be greater than or equal to 1",
                        "details": {"field": "page", "type": "greater_than_equal"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
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
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="List assessments",
    description="Returns a paginated list of assessments with optional filtering by company_id, assessment_type, and status.",
)
async def list_assessments(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    company_id: Optional[UUID] = Query(default=None),
    assessment_type: Optional[AssessmentType] = Query(default=None),
    status_filter: Optional[AssessmentStatus] = Query(default=None, alias="status"),
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> PaginatedAssessmentResponse:
    # Fetch from Snowflake with filters
    assessments, total = assessment_repo.get_all(
        page=page,
        page_size=page_size,
        company_id=company_id,
        assessment_type=assessment_type,
        status=status_filter,
    )

    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    return PaginatedAssessmentResponse(
        items=[AssessmentResponse(**a) for a in assessments],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


@router.get(
    "/{assessment_id}",
    response_model=AssessmentResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Assessment not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "ASSESSMENT_NOT_FOUND",
                        "message": "Assessment not found",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Assessment ID must be a valid UUID format",
                        "details": {"field": "assessment_id", "type": "uuid_parsing"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
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
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Get assessment by ID",
    description="Retrieves a single assessment by its UUID, including associated dimension scores.",
)
async def get_assessment(
    assessment_id: UUID,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> AssessmentResponse:
    cache_key = f"assessment:{assessment_id}"
    cache = get_cache()

    # Try cache first (with graceful failure)
    if cache:
        try:
            cached = cache.get(cache_key, AssessmentResponse)
            if cached:
                return cached  # Cache hit!
        except Exception:
            pass  # Redis failed, continue to database

    # Cache miss - query Snowflake
    assessment_data = assessment_repo.get_by_id(assessment_id)

    if not assessment_data:
        raise_assessment_not_found()

    assessment = AssessmentResponse(**assessment_data)

    # Cache the result
    if cache:
        try:
            cache.set(cache_key, assessment, TTL_ASSESSMENT)
        except Exception:
            pass  # Don't fail if cache write fails

    return assessment


@router.patch(
    "/{assessment_id}/status",
    response_model=AssessmentResponse,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Assessment not found",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "ASSESSMENT_NOT_FOUND",
                        "message": "Assessment not found",
                        "details": None,
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
        422: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "example": {
                        "error_code": "VALIDATION_ERROR",
                        "message": "Status must be one of: draft, in_progress, submitted, approved, superseded",
                        "details": {"field": "status", "type": "enum"},
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
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
                        "timestamp": "2026-01-28T12:00:00Z"
                    }
                }
            }
        },
    },
    summary="Update assessment status",
    description="Updates the status of an existing assessment. Valid transitions depend on current status.",
)
async def update_assessment_status(
    assessment_id: UUID,
    payload: StatusUpdate,
    assessment_repo: AssessmentRepository = Depends(get_assessment_repository),
) -> AssessmentResponse:
    # Check if assessment exists
    if not assessment_repo.exists(assessment_id):
        raise_assessment_not_found()

    # Update status in Snowflake
    updated_data = assessment_repo.update_status(assessment_id, payload.status)

    # Invalidate cache after update
    invalidate_assessment_cache(assessment_id)

    return AssessmentResponse(**updated_data)
