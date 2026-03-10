"""CS2 Client — Evidence from S3 (jobs, patents, techstack, glassdoor, SEC chunks)."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.utils.id_utils import stable_evidence_id

SOURCE_TYPES = [
    "sec_10k_item_1",
    "sec_10k_item_1a",
    "sec_10k_item_7",
    "job_posting_linkedin",
    "job_posting_indeed",
    "patent_uspto",
    "glassdoor_review",
    "board_proxy_def14a",
    "digital_presence",
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


def _section_to_source_type(section: str) -> str:
    s = (section or "").lower().replace(" ", "_")
    if "item_1a" in s or "1a" in s:
        return "sec_10k_item_1a"
    if "item_7" in s or "7" in s:
        return "sec_10k_item_7"
    return "sec_10k_item_1"


def _section_to_signal_category(section: str) -> str:
    s = (section or "").lower().replace(" ", "_")  # match _section_to_source_type() normalization
    if "def14a" in s or "proxy" in s or "governance" in s:
        return "governance_signals"
    if "item_1a" in s or "1a" in s:               # item_1a BEFORE item_1 to avoid substring match
        return "digital_presence"
    if "item_1" in s or "item_7" in s or "business" in s or "management" in s:
        return "leadership_signals"
    return "digital_presence"


class CS2Client:
    """Fetches evidence directly from S3, mirroring vr_scoring_service._load_jobs_from_s3()."""

    def __init__(self):
        from app.services.s3_storage import get_s3_service
        self._s3 = get_s3_service()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_evidence(
        self,
        company_id: Optional[str] = None,
        ticker: Optional[str] = None,
        source_types: Optional[List[str]] = None,
        signal_categories: Optional[List[str]] = None,
        min_confidence: float = 0.0,
        indexed: Optional[bool] = None,
        since: Optional[datetime] = None,
    ) -> List[CS2Evidence]:
        """Fetch evidence from S3 for the given ticker/company."""
        resolved_ticker = ticker or self._resolve_ticker(company_id) or company_id or ""
        resolved_ticker = resolved_ticker.upper()

        fetchers = {
            "technology_hiring": self._fetch_jobs,
            "innovation_activity": self._fetch_patents,
            "digital_presence": self._fetch_techstack,
            "culture_signals": self._fetch_glassdoor,
            "sec_chunks": self._fetch_sec_chunks,
        }

        requested = set(signal_categories) if signal_categories else set(fetchers.keys())
        # sec_chunks is always included unless caller explicitly restricts to other cats
        if signal_categories and "sec_chunks" not in signal_categories:
            requested.discard("sec_chunks")

        all_evidence: List[CS2Evidence] = []
        for cat, fn in fetchers.items():
            if cat not in requested:
                continue
            try:
                all_evidence.extend(fn(resolved_ticker))
            except Exception:
                pass  # partial failure — skip, don't crash

        result = [e for e in all_evidence if e.confidence >= min_confidence]
        if source_types:
            result = [e for e in result if e.source_type in source_types]
        return result

    def mark_indexed(self, evidence_ids: List[str]) -> int:
        """Mark evidence as indexed. Returns count (always succeeds — no endpoint needed)."""
        return len(evidence_ids)

    # ------------------------------------------------------------------
    # S3 fetchers
    # ------------------------------------------------------------------

    def _fetch_jobs(self, ticker: str) -> List[CS2Evidence]:
        prefix = f"signals/jobs/{ticker}/"
        keys = sorted(self._s3.list_files(prefix), reverse=True)
        postings = []
        for key in keys:
            raw = self._s3.get_file(key)
            if raw is None:
                continue
            data = json.loads(raw)
            postings = data.get("jobs", data.get("job_postings", []))
            if postings:
                break

        results = []
        for p in postings:
            title = p.get("title", "")
            desc = p.get("description", "")
            content = f"{title} — {desc}".strip(" —")
            if not content:
                continue
            source = p.get("source", "")
            source_type = (
                "job_posting_linkedin" if "linkedin" in source.lower()
                else "job_posting_indeed"
            )
            results.append(CS2Evidence(
                evidence_id=p.get("job_id") or stable_evidence_id(ticker, source_type, content),
                company_id=ticker,
                source_type=source_type,
                signal_category="technology_hiring",
                content=content,
                confidence=0.7,
                fiscal_year=None,
            ))
        return results

    def _fetch_patents(self, ticker: str) -> List[CS2Evidence]:
        prefix = f"signals/patents/{ticker}/"
        keys = sorted(self._s3.list_files(prefix), reverse=True)
        patents = []
        for key in keys:
            raw = self._s3.get_file(key)
            if raw is None:
                continue
            data = json.loads(raw)
            patents = data.get("patents", [])
            if patents:
                break

        results = []
        for p in patents:
            title = p.get("title", "")
            abstract = p.get("abstract", "")
            categories = ", ".join(p.get("ai_categories", []))
            cat_str = f" | AI Categories: {categories}" if categories else ""
            content = f"[Patent] {title} — {abstract}{cat_str}".strip()
            if not content or content == "[Patent]":
                continue
            patent_num = p.get("patent_number") or p.get("patent_id", "")
            evidence_id = f"patent_{ticker}_{patent_num}" if patent_num else stable_evidence_id(ticker, "patent_uspto", content)
            results.append(CS2Evidence(
                evidence_id=evidence_id,
                company_id=ticker,
                source_type="patent_uspto",
                signal_category="innovation_activity",
                content=content,
                confidence=0.8,
            ))
        return results

    def _fetch_techstack(self, ticker: str) -> List[CS2Evidence]:
        prefix = f"signals/digital/{ticker}/"
        keys = sorted(self._s3.list_files(prefix), reverse=True)
        data = {}
        for key in keys:
            raw = self._s3.get_file(key)
            if raw is None:
                continue
            data = json.loads(raw)
            if data:
                break

        ai_techs: List[str] = data.get("ai_technologies_detected", [])
        wap_techs: List[str] = data.get("wappalyzer_techs", [])
        all_techs = ai_techs + [t for t in wap_techs if t not in ai_techs]
        if not all_techs:
            return []

        content = "Detected technologies: " + ", ".join(all_techs[:50])
        return [CS2Evidence(
            evidence_id=stable_evidence_id(ticker, "digital_presence", content),
            company_id=ticker,
            source_type="digital_presence",
            signal_category="digital_presence",
            content=content,
            confidence=0.6,
        )]

    def _fetch_glassdoor(self, ticker: str) -> List[CS2Evidence]:
        prefix = f"glassdoor_signals/raw/{ticker}/"
        keys = sorted(self._s3.list_files(prefix), reverse=True)
        raw = None
        for key in keys:
            raw = self._s3.get_file(key)
            if raw is not None:
                break
        if raw is None:
            return []
        wrapper = json.loads(raw)
        reviews = wrapper if isinstance(wrapper, list) else wrapper.get("reviews", [])

        results = []
        for r in reviews:
            title = r.get("title", "")
            pros = r.get("pros", "")
            cons = r.get("cons", "")
            content = f"{title} — Pros: {pros} Cons: {cons}".strip()
            if not content or content == "— Pros:  Cons:":
                continue
            results.append(CS2Evidence(
                evidence_id=r.get("review_id") or stable_evidence_id(ticker, "glassdoor_review", content),
                company_id=ticker,
                source_type="glassdoor_review",
                signal_category="culture_signals",
                content=content,
                confidence=0.65,
            ))
        return results

    def _fetch_sec_chunks(self, ticker: str) -> List[CS2Evidence]:
        results = []
        for filing_type in ("10-K", "DEF14A"):
            prefix = f"sec/chunks/{ticker}/{filing_type}/"
            keys = self._s3.list_files(prefix)
            for key in keys:
                raw = self._s3.get_file(key)
                if raw is None:
                    continue
                data = json.loads(raw)
                chunks = data if isinstance(data, list) else data.get("chunks", [])
                for chunk in chunks:
                    text = chunk.get("text") or chunk.get("content", "")
                    if not text:
                        continue
                    section = chunk.get("section", "")
                    if filing_type == "DEF14A":
                        source_type = "board_proxy_def14a"
                        signal_cat = "governance_signals"
                    else:
                        source_type = _section_to_source_type(section)
                        signal_cat = _section_to_signal_category(section)
                    results.append(CS2Evidence(
                        evidence_id=chunk.get("chunk_id") or stable_evidence_id(ticker, source_type, text),
                        company_id=ticker,
                        source_type=source_type,
                        signal_category=signal_cat,
                        content=text,
                        confidence=0.9,
                        page_number=chunk.get("page_number"),
                        fiscal_year=chunk.get("fiscal_year"),
                    ))
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_ticker(self, company_id: Optional[str]) -> Optional[str]:
        if not company_id:
            return None
        # company_id may already be a ticker (e.g. "NVDA") or a UUID
        try:
            from app.repositories.company_repository import CompanyRepository
            repo = CompanyRepository()
            company = repo.get_by_id(company_id)
            if company:
                return company.get("ticker") or company.get("symbol")
        except Exception:
            pass
        # If lookup fails, treat company_id as ticker directly
        return company_id
