"""Analyst Notes API Router (CS4 — Task 8.0d)

Endpoints:
  POST /api/v1/analyst-notes/{ticker}/interview    — Submit interview transcript
  POST /api/v1/analyst-notes/{ticker}/dd-finding   — Submit DD finding
  POST /api/v1/analyst-notes/{ticker}/data-room    — Submit data room summary
  GET  /api/v1/analyst-notes/{ticker}              — List all notes for company
  GET  /api/v1/analyst-notes/{ticker}/{note_id}    — Get single note by ID
  POST /api/v1/analyst-notes/{ticker}/load         — Restore cache from Snowflake
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.repositories.company_repository import CompanyRepository
from app.services.collection.analyst_notes import AnalystNotesCollector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyst-notes", tags=["Analyst Notes"])

# Module-level singleton
_collector: Optional[AnalystNotesCollector] = None


def get_collector() -> AnalystNotesCollector:
    global _collector
    if _collector is None:
        _collector = AnalystNotesCollector()
    return _collector


# =====================================================================
# Request Models
# =====================================================================

class SubmitInterviewRequest(BaseModel):
    interviewee: str
    interviewee_title: str
    transcript: str
    assessor: str
    dimensions_discussed: Optional[List[str]] = None


class SubmitDDFindingRequest(BaseModel):
    title: str
    finding: str
    dimension: str
    severity: str = Field(..., description="One of: critical, high, medium, low")
    assessor: str


class SubmitDataRoomRequest(BaseModel):
    document_name: str
    summary: str
    dimension: str
    assessor: str


# =====================================================================
# Response Models
# =====================================================================

class NoteSubmittedResponse(BaseModel):
    note_id: str
    company_id: str
    note_type: str
    dimension: str
    assessor: str
    created_at: str
    s3_key: Optional[str] = None


class AnalystNoteOut(BaseModel):
    note_id: str
    company_id: str
    note_type: str
    content: str
    dimension: str
    assessor: str
    confidence: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    s3_key: Optional[str] = None


class ListNotesResponse(BaseModel):
    company_id: str
    count: int
    notes: List[AnalystNoteOut]


# =====================================================================
# POST /{ticker}/interview — Submit interview transcript
# =====================================================================

@router.post(
    "/{ticker}/interview",
    response_model=NoteSubmittedResponse,
    summary="Submit interview transcript",
    description="Index an interview transcript into ChromaDB, Snowflake, and S3.",
)
async def submit_interview(ticker: str, body: SubmitInterviewRequest):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        collector = get_collector()
        note_id = collector.submit_interview(
            company_id=company_id,
            interviewee=body.interviewee,
            interviewee_title=body.interviewee_title,
            transcript=body.transcript,
            assessor=body.assessor,
            dimensions_discussed=body.dimensions_discussed,
        )
        note = collector.get_note(note_id)
        return NoteSubmittedResponse(
            note_id=note_id,
            company_id=company_id,
            note_type="interview_transcript",
            dimension=note.dimension if note else "",
            assessor=body.assessor,
            created_at=note.created_at if note else "",
            s3_key=note.s3_key if note else None,
        )
    except Exception as e:
        logger.error("submit_interview failed company_id=%s: %s", company_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# POST /{ticker}/dd-finding — Submit DD finding
# =====================================================================

@router.post(
    "/{ticker}/dd-finding",
    response_model=NoteSubmittedResponse,
    summary="Submit due diligence finding",
    description="Index a DD finding into ChromaDB, Snowflake, and S3.",
)
async def submit_dd_finding(ticker: str, body: SubmitDDFindingRequest):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        collector = get_collector()
        note_id = collector.submit_dd_finding(
            company_id=company_id,
            title=body.title,
            finding=body.finding,
            dimension=body.dimension,
            severity=body.severity,
            assessor=body.assessor,
        )
        note = collector.get_note(note_id)
        return NoteSubmittedResponse(
            note_id=note_id,
            company_id=company_id,
            note_type="dd_finding",
            dimension=body.dimension,
            assessor=body.assessor,
            created_at=note.created_at if note else "",
            s3_key=note.s3_key if note else None,
        )
    except Exception as e:
        logger.error("submit_dd_finding failed company_id=%s: %s", company_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# POST /{ticker}/data-room — Submit data room summary
# =====================================================================

@router.post(
    "/{ticker}/data-room",
    response_model=NoteSubmittedResponse,
    summary="Submit data room document summary",
    description="Index a data room summary into ChromaDB, Snowflake, and S3.",
)
async def submit_data_room(ticker: str, body: SubmitDataRoomRequest):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        collector = get_collector()
        note_id = collector.submit_data_room_summary(
            company_id=company_id,
            document_name=body.document_name,
            summary=body.summary,
            dimension=body.dimension,
            assessor=body.assessor,
        )
        note = collector.get_note(note_id)
        return NoteSubmittedResponse(
            note_id=note_id,
            company_id=company_id,
            note_type="data_room_summary",
            dimension=body.dimension,
            assessor=body.assessor,
            created_at=note.created_at if note else "",
            s3_key=note.s3_key if note else None,
        )
    except Exception as e:
        logger.error("submit_data_room failed company_id=%s: %s", company_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /{ticker} — List all notes for company
# =====================================================================

@router.get(
    "/{ticker}",
    response_model=ListNotesResponse,
    summary="List all analyst notes for a company",
    description="Returns all notes from memory cache (with Snowflake fallback).",
)
async def list_notes(ticker: str):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        notes = get_collector().list_notes(company_id)
        return ListNotesResponse(
            company_id=company_id,
            count=len(notes),
            notes=[
                AnalystNoteOut(
                    note_id=n.note_id,
                    company_id=n.company_id,
                    note_type=n.note_type,
                    content=n.content,
                    dimension=n.dimension,
                    assessor=n.assessor,
                    confidence=n.confidence,
                    metadata=n.metadata,
                    created_at=n.created_at,
                    s3_key=n.s3_key,
                )
                for n in notes
            ],
        )
    except Exception as e:
        logger.error("list_notes failed company_id=%s: %s", company_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================================
# GET /{ticker}/{note_id} — Get single note
# =====================================================================

@router.get(
    "/{ticker}/{note_id}",
    response_model=AnalystNoteOut,
    summary="Get a single analyst note by ID",
    description="Fetches from memory cache first, then Snowflake fallback.",
)
async def get_note(ticker: str, note_id: str):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        note = get_collector().get_note(note_id)
    except Exception as e:
        logger.error("get_note failed note_id=%s: %s", note_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if note is None:
        raise HTTPException(status_code=404, detail=f"Note '{note_id}' not found.")

    return AnalystNoteOut(
        note_id=note.note_id,
        company_id=note.company_id,
        note_type=note.note_type,
        content=note.content,
        dimension=note.dimension,
        assessor=note.assessor,
        confidence=note.confidence,
        metadata=note.metadata,
        created_at=note.created_at,
        s3_key=note.s3_key,
    )


# =====================================================================
# POST /{ticker}/load — Restore cache from Snowflake
# =====================================================================

@router.post(
    "/{ticker}/load",
    response_model=ListNotesResponse,
    summary="Restore analyst notes cache from Snowflake",
    description=(
        "Loads all notes for the company from Snowflake + S3 into the in-memory cache "
        "and re-indexes them in ChromaDB. Call this after a server restart."
    ),
)
async def load_from_snowflake(ticker: str):
    ticker = ticker.upper()
    company = CompanyRepository().get_by_ticker(ticker)
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company '{ticker}' not found.")
    company_id = str(company["id"])
    try:
        notes = get_collector().load_from_snowflake(company_id)
        return ListNotesResponse(
            company_id=company_id,
            count=len(notes),
            notes=[
                AnalystNoteOut(
                    note_id=n.note_id,
                    company_id=n.company_id,
                    note_type=n.note_type,
                    content=n.content,
                    dimension=n.dimension,
                    assessor=n.assessor,
                    confidence=n.confidence,
                    metadata=n.metadata,
                    created_at=n.created_at,
                    s3_key=n.s3_key,
                )
                for n in notes
            ],
        )
    except Exception as e:
        logger.error("load_from_snowflake failed company_id=%s: %s", company_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
