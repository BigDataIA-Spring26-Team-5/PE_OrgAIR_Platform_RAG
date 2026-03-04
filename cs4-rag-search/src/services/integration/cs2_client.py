"""CS2 Evidence Collection API client."""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import httpx

class SourceType(str, Enum):
    """Evidence source types from CS2."""
    SEC_10K_ITEM_1 = "sec_10k_item_1"      # Business description
    SEC_10K_ITEM_1A = "sec_10k_item_1a"    # Risk factors
    SEC_10K_ITEM_7 = "sec_10k_item_7"      # MD&A
    JOB_POSTING_LINKEDIN = "job_posting_linkedin"
    JOB_POSTING_INDEED = "job_posting_indeed"
    PATENT_USPTO = "patent_uspto"
    PRESS_RELEASE = "press_release"
    GLASSDOOR_REVIEW = "glassdoor_review"   # From CS3 Task 5.0c
    BOARD_PROXY_DEF14A = "board_proxy_def14a"  # From CS3 Task 5.0d
    ANALYST_INTERVIEW = "analyst_interview"  # NEW: DD interviews
    DD_DATA_ROOM = "dd_data_room"           # NEW: Data room docs

class SignalCategory(str, Enum):
    """Signal categories from CS2 collectors."""
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    CULTURE_SIGNALS = "culture_signals"
    GOVERNANCE_SIGNALS = "governance_signals"

@dataclass
class ExtractedEntity:
    """Entity extracted from evidence text."""
    entity_type: str  # "ai_investment", "technology", "person", etc.
    text: str
    char_start: int
    char_end: int
    confidence: float
    attributes: Dict[str, Any] = field(default_factory=dict)

@dataclass
class CS2Evidence:
    """Evidence item from CS2 Evidence Collection."""
    evidence_id: str
    company_id: str
    source_type: SourceType
    signal_category: SignalCategory
    content: str
    extracted_at: datetime
    confidence: float

    # Optional metadata
    fiscal_year: Optional[int] = None
    source_url: Optional[str] = None
    page_number: Optional[int] = None
    extracted_entities: List[ExtractedEntity] = field(default_factory=list)

    # Indexing status
    indexed_in_cs4: bool = False
    indexed_at: Optional[datetime] = None

class CS2Client:
    """Client for CS2 Evidence Collection API."""

    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=60.0)

    async def get_evidence(
        self,
        company_id: str,
        source_types: Optional[List[SourceType]] = None,
        signal_categories: Optional[List[SignalCategory]] = None,
        min_confidence: float = 0.0,
        indexed: Optional[bool] = None,
        since: Optional[datetime] = None,
    ) -> List[CS2Evidence]:
        """
        Fetch evidence for a company with filters.

        Args:
            company_id: Company ticker or ID
            source_types: Filter by source types
            signal_categories: Filter by signal categories
            min_confidence: Minimum confidence threshold
            indexed: Filter by indexing status (None=all)
            since: Only evidence extracted after this date
        """
        params = {"company_id": company_id}
        if source_types:
            params["source_types"] = ",".join(s.value for s in source_types)
        if signal_categories:
            params["signal_categories"] = ",".join(s.value for s in signal_categories)
        if min_confidence > 0:
            params["min_confidence"] = min_confidence
        if indexed is not None:
            params["indexed"] = indexed
        if since:
            params["since"] = since.isoformat()

        response = await self.client.get(
            f"{self.base_url}/api/v1/evidence",
            params=params
        )
        response.raise_for_status()

        evidence_list = []
        for e in response.json():
            entities = [ExtractedEntity(**ent) for ent in e.get("extracted_entities", [])]
            evidence_list.append(CS2Evidence(
                evidence_id=e["evidence_id"],
                company_id=e["company_id"],
                source_type=SourceType(e["source_type"]),
                signal_category=SignalCategory(e["signal_category"]),
                content=e["content"],
                extracted_at=datetime.fromisoformat(e["extracted_at"]),
                confidence=e["confidence"],
                fiscal_year=e.get("fiscal_year"),
                source_url=e.get("source_url"),
                page_number=e.get("page_number"),
                extracted_entities=entities,
                indexed_in_cs4=e.get("indexed_in_cs4", False),
            ))
        return evidence_list

    async def mark_indexed(self, evidence_ids: List[str]) -> int:
        """Mark evidence as indexed in CS4."""
        response = await self.client.post(
            f"{self.base_url}/api/v1/evidence/mark-indexed",
            json={"evidence_ids": evidence_ids}
        )
        response.raise_for_status()
        return response.json()["updated_count"]

    async def close(self):
        await self.client.aclose()
