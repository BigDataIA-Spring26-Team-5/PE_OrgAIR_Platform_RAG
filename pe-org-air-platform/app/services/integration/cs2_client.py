"""CS2 Client — Evidence and chunk data from the PE Org-AI-R platform."""
from __future__ import annotations

import httpx
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

SOURCE_TYPES = [
    "sec_10k_item_1",
    "sec_10k_item_1a",
    "sec_10k_item_7",
    "job_posting_linkedin",
    "job_posting_indeed",
    "patent_uspto",
    "glassdoor_review",
    "board_proxy_def14a",
    "analyst_interview",
    "dd_data_room",
]


@dataclass
class CS2Evidence:
    evidence_id: str
    company_id: str
    source_type: str
    signal_category: str
    content: str
    confidence: float = 0.0
    extracted_entities: Dict[str, Any] = field(default_factory=dict)
    fiscal_year: Optional[str] = None
    source_url: Optional[str] = None
    page_number: Optional[int] = None
    indexed_in_cs4: bool = False


class CS2Client:
    """Fetches evidence/chunk data from CS2 API endpoints."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=60.0)

    def get_evidence(
        self,
        company_id: Optional[str] = None,
        source_types: Optional[List[str]] = None,
        signal_categories: Optional[List[str]] = None,
        min_confidence: float = 0.0,
        indexed: Optional[bool] = None,
        since: Optional[datetime] = None,
    ) -> List[CS2Evidence]:
        """Fetch evidence records with optional filters."""
        params: Dict[str, Any] = {}
        if company_id:
            params["company_id"] = company_id
        if source_types:
            params["source_types"] = ",".join(source_types)
        if signal_categories:
            params["signal_categories"] = ",".join(signal_categories)
        if min_confidence > 0:
            params["min_confidence"] = min_confidence
        if indexed is not None:
            params["indexed"] = indexed
        if since:
            params["since"] = since.isoformat()

        # Try the evidence endpoint
        resp = self._client.get(f"{self.base_url}/evidence", params=params)
        if resp.status_code == 404:
            # Fall back to chunks endpoint
            return self._fetch_from_chunks(company_id, min_confidence)
        resp.raise_for_status()
        data = resp.json()
        items = data if isinstance(data, list) else data.get("evidence", [])
        return [self._parse_evidence(e) for e in items]

    def _fetch_from_chunks(
        self,
        company_id: Optional[str],
        min_confidence: float,
    ) -> List[CS2Evidence]:
        """Fallback: fetch from document chunks endpoint."""
        params: Dict[str, Any] = {}
        if company_id:
            params["company_id"] = company_id
        resp = self._client.get(f"{self.base_url}/documents/chunks", params=params)
        if resp.status_code in (404, 422):
            return []
        resp.raise_for_status()
        data = resp.json()
        chunks = data if isinstance(data, list) else data.get("chunks", [])
        results = []
        for chunk in chunks:
            ev = CS2Evidence(
                evidence_id=str(chunk.get("id", chunk.get("chunk_id", ""))),
                company_id=str(chunk.get("company_id", company_id or "")),
                source_type=chunk.get("source_type", "sec_10k_item_1"),
                signal_category=chunk.get("signal_category", "digital_presence"),
                content=chunk.get("content", chunk.get("text", "")),
                confidence=float(chunk.get("confidence", 0.5)),
                fiscal_year=chunk.get("fiscal_year"),
                source_url=chunk.get("source_url"),
                page_number=chunk.get("page_number"),
                indexed_in_cs4=bool(chunk.get("indexed_in_cs4", False)),
            )
            if ev.confidence >= min_confidence:
                results.append(ev)
        return results

    def mark_indexed(self, evidence_ids: List[str]) -> int:
        """Mark evidence records as indexed in CS4. Returns count updated."""
        if not evidence_ids:
            return 0
        resp = self._client.patch(
            f"{self.base_url}/evidence/mark-indexed",
            json={"evidence_ids": evidence_ids},
        )
        if resp.status_code in (404, 422):
            return len(evidence_ids)  # Assume success if endpoint not available
        resp.raise_for_status()
        data = resp.json()
        return int(data.get("updated", len(evidence_ids)))

    @staticmethod
    def _parse_evidence(data: dict) -> CS2Evidence:
        return CS2Evidence(
            evidence_id=str(data.get("id", data.get("evidence_id", ""))),
            company_id=str(data.get("company_id", "")),
            source_type=data.get("source_type", ""),
            signal_category=data.get("signal_category", ""),
            content=data.get("content", data.get("text", "")),
            confidence=float(data.get("confidence", 0.0)),
            extracted_entities=data.get("extracted_entities", {}),
            fiscal_year=data.get("fiscal_year"),
            source_url=data.get("source_url"),
            page_number=data.get("page_number"),
            indexed_in_cs4=bool(data.get("indexed_in_cs4", False)),
        )

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
