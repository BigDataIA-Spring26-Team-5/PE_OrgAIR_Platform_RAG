"""Collect and index analyst notes and interview transcripts."""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum
from services.retrieval.hybrid import HybridRetriever
from services.retrieval.dimension_mapper import DimensionMapper

class NoteType(str, Enum):
    INTERVIEW_TRANSCRIPT = "interview_transcript"
    MANAGEMENT_MEETING = "management_meeting"
    SITE_VISIT = "site_visit"
    DD_FINDING = "dd_finding"
    DATA_ROOM_SUMMARY = "data_room_summary"

@dataclass
class AnalystNote:
    """Analyst-generated evidence."""
    note_id: str
    company_id: str
    note_type: NoteType
    title: str
    content: str

    # Interview metadata
    interviewee: Optional[str] = None
    interviewee_title: Optional[str] = None

    # Assessment context
    dimensions_discussed: List[str] = field(default_factory=list)
    key_findings: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)

    # Provenance
    assessor: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    confidence: float = 1.0  # Primary source = high confidence

class AnalystNotesCollector:
    """API for analysts to submit and index notes."""

    def __init__(self, retriever: HybridRetriever):
        self.retriever = retriever
        self.mapper = DimensionMapper()

    async def submit_interview(
        self, company_id: str, interviewee: str, interviewee_title: str,
        transcript: str, assessor: str, dimensions_discussed: List[str],
    ) -> str:
        """Submit interview transcript for indexing."""
        note_id = f"interview_{company_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Determine primary dimension
        primary_dim = dimensions_discussed[0] if dimensions_discussed else "leadership"

        doc = {
            "doc_id": note_id,
            "content": f"Interview: {interviewee_title}\n\n{transcript}",
            "metadata": {
                "company_id": company_id,
                "source_type": NoteType.INTERVIEW_TRANSCRIPT.value,
                "dimension": primary_dim,
                "confidence": 1.0,  # Primary source
                "assessor": assessor,
                "interviewee_title": interviewee_title,
            }
        }
        self.retriever.index_documents([doc])
        return note_id

    async def submit_dd_finding(
        self, company_id: str, title: str, finding: str,
        dimension: str, severity: str, assessor: str,
    ) -> str:
        """Submit due diligence finding."""
        note_id = f"dd_{company_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        doc = {
            "doc_id": note_id,
            "content": f"{title}\n\n{finding}",
            "metadata": {
                "company_id": company_id,
                "source_type": NoteType.DD_FINDING.value,
                "dimension": dimension,
                "confidence": 1.0,
                "assessor": assessor,
                "severity": severity,
            }
        }
        self.retriever.index_documents([doc])
        return note_id

    async def submit_data_room_summary(
        self, company_id: str, document_name: str, summary: str,
        dimension: str, assessor: str,
    ) -> str:
        """Submit data room document summary."""
        note_id = f"dataroom_{company_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        doc = {
            "doc_id": note_id,
            "content": f"Data Room: {document_name}\n\n{summary}",
            "metadata": {
                "company_id": company_id,
                "source_type": NoteType.DATA_ROOM_SUMMARY.value,
                "dimension": dimension,
                "confidence": 1.0,
                "assessor": assessor,
                "document_name": document_name,
            }
        }
        self.retriever.index_documents([doc])
        return note_id
