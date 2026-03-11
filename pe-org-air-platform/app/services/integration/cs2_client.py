"""CS2 Client — Evidence from S3 (jobs, patents, techstack, glassdoor, SEC chunks)."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

import httpx

from app.utils.id_utils import stable_evidence_id

logger = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.1-8b-instant"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

SIGNAL_KEYWORDS: Dict[str, List[str]] = {
    "technology_hiring": [
        "machine learning", "data science", "AI engineer", "software engineer",
        "cloud", "python", "MLOps", "LLM", "deep learning", "NLP",
    ],
    "innovation_activity": [
        "patent", "R&D", "invention", "USPTO", "intellectual property",
        "innovation", "research", "technology development",
    ],
    "digital_presence": [
        "cloud infrastructure", "AWS", "Azure", "GCP", "tech stack",
        "digital transformation", "AI platform", "data platform", "SaaS",
    ],
    "leadership_signals": [
        "CEO", "CTO", "CDO", "board", "executive", "strategy",
        "AI governance", "digital strategy", "technology leadership",
    ],
    "glassdoor_culture": [
        "culture", "innovation", "data-driven", "AI awareness",
        "change readiness", "employee", "work environment",
    ],
    "board_governance": [
        "board committee", "tech committee", "AI expertise", "independent director",
        "risk oversight", "governance", "proxy statement", "DEF 14A",
    ],
}

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


async def expand_keywords_with_groq(ticker: str, category: str) -> List[str]:
    """
    Use Groq LLM to expand keywords for a given signal category and company.
    Falls back to the static SIGNAL_KEYWORDS list if Groq is unavailable.
    """
    if not GROQ_API_KEY:
        return SIGNAL_KEYWORDS.get(category, [])

    base_keywords = SIGNAL_KEYWORDS.get(category, [])
    prompt = (
        f"You are a financial analyst. For the company with ticker '{ticker}', "
        f"generate 10 additional specific keywords or short phrases (comma-separated) "
        f"that would appear in SEC filings, job postings, or analyst reports when "
        f"evaluating the '{category}' dimension. "
        f"Base keywords: {', '.join(base_keywords)}. "
        f"Return ONLY the comma-separated keywords, nothing else."
    )
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.3,
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()
            expanded = [kw.strip() for kw in text.split(",") if kw.strip()]
            return list(set(base_keywords + expanded))
    except Exception as e:
        logger.warning("groq_keyword_expansion_failed ticker=%s category=%s error=%s", ticker, category, e)
        return base_keywords


async def get_groq_signal_summary(ticker: str, category: str, raw_data: Dict[str, Any]) -> Optional[str]:
    """
    Use Groq to generate a short natural language summary of a signal result.
    Returns None if Groq is unavailable.
    """
    if not GROQ_API_KEY:
        return None
    prompt = (
        f"Summarize the following {category} signal data for ticker '{ticker}' "
        f"in 2-3 sentences for an investment committee memo. "
        f"Data: {json.dumps(raw_data, default=str)[:1500]}"
    )
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 300,
                    "temperature": 0.4,
                },
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning("groq_summary_failed ticker=%s category=%s error=%s", ticker, category, e)
        return None


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
    # Groq-enhanced async methods
    # ------------------------------------------------------------------

    async def get_keywords_for_category(self, ticker: str, category: str) -> List[str]:
        """Groq-expanded keywords for a signal category; falls back to static list."""
        return await expand_keywords_with_groq(ticker, category)

    async def get_full_evidence_with_keywords(self, ticker: str, category: str) -> Dict[str, Any]:
        """Evidence from S3 for ticker/category + Groq-expanded keywords + IC summary.

        Returns: {ticker, category, keywords, evidence, groq_summary}
        """
        evidence = self.get_evidence(
            ticker=ticker,
            signal_categories=[category] if category else None,
        )
        keywords = await expand_keywords_with_groq(ticker, category)
        summary = await get_groq_signal_summary(ticker, category, {
            "evidence_count": len(evidence),
            "category": category,
        })
        return {
            "ticker": ticker.upper(),
            "category": category,
            "keywords": keywords,
            "evidence": evidence,
            "groq_summary": summary,
        }

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
