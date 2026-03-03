"""
cs2_client.py — CS4 RAG Search
src/services/integration/cs2_client.py

HTTP client for the CS2 evidence layer (pe-org-air-platform /evidence/* endpoints).
Data models derived from:
  - app/models/signal.py   (SignalCategory, SignalSource)
  - app/models/evidence.py (SignalEvidence fields)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SignalCategory(str, Enum):
    """
    CS2 signal categories.
    Renamed per CS4 spec:
      CULTURE_SIGNALS   (platform: GLASSDOOR_CULTURE)
      GOVERNANCE_SIGNALS (platform: BOARD_GOVERNANCE)
    """
    TECHNOLOGY_HIRING  = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE   = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    CULTURE_SIGNALS    = "culture_signals"     # was GLASSDOOR_CULTURE
    GOVERNANCE_SIGNALS = "governance_signals"  # was BOARD_GOVERNANCE


class SourceType(str, Enum):
    """
    All evidence sources.
    Derived from platform's SignalSource enum with CS4 naming convention.
    Added: ANALYST_INTERVIEW, DD_DATA_ROOM (CS4 new).
    """
    # Job sources
    JOB_POSTING_LINKEDIN  = "linkedin"
    JOB_POSTING_INDEED    = "indeed"
    # Culture / review source
    GLASSDOOR_REVIEW      = "glassdoor"
    # Patent source
    USPTO_PATENT          = "uspto"
    # Tech stack sources
    TECH_STACK_BUILTWITH  = "builtwith"
    TECH_STACK_WAPPALYZER = "wappalyzer"
    TECH_STACK_COMBINED   = "builtwith_wappalyzer"
    # SEC / public sources
    SEC_10K_ITEM_1        = "sec_item_1"
    SEC_10K_ITEM_1A       = "sec_item_1a"
    SEC_10K_ITEM_7        = "sec_item_7"
    SEC_FILING            = "sec_filing"
    PRESS_RELEASE         = "press_release"
    COMPANY_WEBSITE       = "company_website"
    # Governance
    BOARD_PROXY           = "board_proxy"
    # CS4 new sources
    ANALYST_INTERVIEW     = "analyst_interview"
    DD_DATA_ROOM          = "dd_data_room"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ExtractedEntity:
    """A named entity or key phrase extracted from evidence text."""
    text: str
    entity_type: str        # e.g. "PERSON", "ORG", "TECH", "SKILL"
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CS2Evidence:
    """
    A single evidence item from CS2.
    Fields mirror SignalEvidence (app/models/evidence.py) with CS4 additions:
      indexed_in_cs4, indexed_at, extracted_entities.
    """
    id: str
    category: str
    source: str
    signal_date: Optional[datetime] = None
    raw_value: Optional[str] = None
    normalized_score: Optional[float] = None
    confidence: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    # CS4 additions
    indexed_in_cs4: bool = False
    indexed_at: Optional[datetime] = None
    extracted_entities: List[ExtractedEntity] = field(default_factory=list)

    @classmethod
    def from_api_dict(cls, data: Dict[str, Any]) -> "CS2Evidence":
        """Construct from raw API response dict."""
        return cls(
            id=data["id"],
            category=data.get("category", ""),
            source=data.get("source", ""),
            signal_date=_parse_dt(data.get("signal_date")),
            raw_value=data.get("raw_value"),
            normalized_score=data.get("normalized_score"),
            confidence=data.get("confidence"),
            metadata=data.get("metadata"),
            created_at=_parse_dt(data.get("created_at")),
        )


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# CS2Client
# ---------------------------------------------------------------------------

class CS2Client:
    """
    Async HTTP client for the CS2 evidence endpoints.
    Base URL defaults to CS2_BASE_URL env var (fallback: http://localhost:8000).
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = (base_url or os.getenv("CS2_BASE_URL", "http://localhost:8000")).rstrip("/")
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)
        self._log = logger.bind(client="cs2")

    async def get_evidence(
        self,
        ticker: str,
        category: Optional[SignalCategory] = None,
    ) -> List[CS2Evidence]:
        """
        Fetch evidence items for a company ticker.
        Maps to GET /companies/{ticker}/evidence on the platform.
        """
        url = f"/companies/{ticker}/evidence"
        params: Dict[str, str] = {}
        if category is not None:
            params["category"] = category.value

        self._log.info("fetching evidence", ticker=ticker, category=category)
        response = await self._client.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        signals: List[Dict[str, Any]] = data.get("signals", [])
        return [CS2Evidence.from_api_dict(s) for s in signals]

    async def mark_indexed(self, evidence_id: str, indexed_at: Optional[datetime] = None) -> None:
        """
        Mark an evidence item as indexed in CS4 (local state only — no platform endpoint).
        In production this would update a local index-tracking store.
        """
        ts = indexed_at or datetime.utcnow()
        self._log.info("marking evidence indexed", evidence_id=evidence_id, indexed_at=ts.isoformat())

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "CS2Client":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
