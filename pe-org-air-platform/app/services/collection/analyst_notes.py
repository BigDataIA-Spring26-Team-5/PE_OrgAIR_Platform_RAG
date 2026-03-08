"""Analyst Notes Collector — Post-LOI DD notes indexer."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any

from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from app.services.retrieval.dimension_mapper import DimensionMapper

NOTE_TYPES = [
    "interview_transcript",
    "management_meeting",
    "site_visit",
    "dd_finding",
    "data_room_summary",
]

SEVERITY_LEVELS = ["critical", "high", "medium", "low"]


@dataclass
class AnalystNote:
    note_id: str
    company_id: str
    note_type: str
    content: str
    dimension: str
    assessor: str
    confidence: float = 1.0  # Primary sources = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class AnalystNotesCollector:
    """Indexes post-LOI DD notes into the hybrid retriever."""

    def __init__(self, retriever: Optional[HybridRetriever] = None):
        self.retriever = retriever or HybridRetriever()
        self.mapper = DimensionMapper()
        self._notes: Dict[str, AnalystNote] = {}

    def submit_interview(
        self,
        company_id: str,
        interviewee: str,
        interviewee_title: str,
        transcript: str,
        assessor: str,
        dimensions_discussed: Optional[List[str]] = None,
    ) -> str:
        """Index an interview transcript. Returns note_id."""
        note_id = str(uuid.uuid4())
        primary_dim = (dimensions_discussed or ["leadership"])[0]
        meta = {
            "interviewee": interviewee,
            "interviewee_title": interviewee_title,
            "note_type": "interview_transcript",
            "assessor": assessor,
            "dimensions_discussed": ",".join(dimensions_discussed or []),
            "company_id": company_id,
            "dimension": primary_dim,
            "confidence": 1.0,
        }
        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type="interview_transcript",
            content=transcript,
            dimension=primary_dim,
            assessor=assessor,
            metadata=meta,
        )
        self._notes[note_id] = note
        self._index_note(note)
        return note_id

    def submit_dd_finding(
        self,
        company_id: str,
        title: str,
        finding: str,
        dimension: str,
        severity: str,
        assessor: str,
    ) -> str:
        """Index a DD finding. Returns note_id."""
        note_id = str(uuid.uuid4())
        content = f"[{severity.upper()}] {title}\n\n{finding}"
        meta = {
            "title": title,
            "severity": severity,
            "note_type": "dd_finding",
            "assessor": assessor,
            "company_id": company_id,
            "dimension": dimension,
            "confidence": 1.0,
        }
        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type="dd_finding",
            content=content,
            dimension=dimension,
            assessor=assessor,
            metadata=meta,
        )
        self._notes[note_id] = note
        self._index_note(note)
        return note_id

    def submit_data_room_summary(
        self,
        company_id: str,
        document_name: str,
        summary: str,
        dimension: str,
        assessor: str,
    ) -> str:
        """Index a data room document summary. Returns note_id."""
        note_id = str(uuid.uuid4())
        content = f"Data Room Document: {document_name}\n\n{summary}"
        meta = {
            "document_name": document_name,
            "note_type": "data_room_summary",
            "assessor": assessor,
            "company_id": company_id,
            "dimension": dimension,
            "confidence": 1.0,
        }
        note = AnalystNote(
            note_id=note_id,
            company_id=company_id,
            note_type="data_room_summary",
            content=content,
            dimension=dimension,
            assessor=assessor,
            metadata=meta,
        )
        self._notes[note_id] = note
        self._index_note(note)
        return note_id

    def _index_note(self, note: AnalystNote):
        doc = RetrievedDocument(
            doc_id=note.note_id,
            content=note.content,
            metadata=note.metadata,
            score=1.0,
            retrieval_method="direct",
        )
        self.retriever.index_documents([doc])

    def get_note(self, note_id: str) -> Optional[AnalystNote]:
        return self._notes.get(note_id)

    def list_notes(self, company_id: str) -> List[AnalystNote]:
        return [n for n in self._notes.values() if n.company_id == company_id]
