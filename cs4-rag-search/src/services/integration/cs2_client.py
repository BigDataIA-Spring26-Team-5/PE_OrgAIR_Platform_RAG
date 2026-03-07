"""CS2 Evidence Collection API client.

Maps to the pe-org-air-platform FastAPI at localhost:8000.

Covers:
  - Document collection, parsing, chunking (SEC EDGAR pipeline)
  - Signal scoring: technology_hiring, digital_presence, innovation_activity, leadership_signals
  - Glassdoor culture signals
  - Board governance signals
  - Evidence summary and stats
  - Groq API keyword expansion for signal category matching

All endpoints call localhost:8000 (the pe-org-air-platform).
"""
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import httpx
import structlog

logger = structlog.get_logger()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.1-8b-instant"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

# Signal category keywords used for matching / expanding Groq queries
SIGNAL_KEYWORDS = {
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


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SourceType(str, Enum):
    """Evidence source types — mirrors the platform's signal categories and filing types."""
    SEC_10K_ITEM_1 = "sec_10k_item_1"
    SEC_10K_ITEM_1A = "sec_10k_item_1a"
    SEC_10K_ITEM_7 = "sec_10k_item_7"
    JOB_POSTING_LINKEDIN = "job_posting_linkedin"
    JOB_POSTING_INDEED = "job_posting_indeed"
    PATENT_USPTO = "patent_uspto"
    PRESS_RELEASE = "press_release"
    GLASSDOOR_REVIEW = "glassdoor_review"
    BOARD_PROXY_DEF14A = "board_proxy_def14a"
    ANALYST_INTERVIEW = "analyst_interview"
    DD_DATA_ROOM = "dd_data_room"
    UNKNOWN = "unknown"


class SignalCategory(str, Enum):
    """Signal categories — mirrors the platform's company_signals.category values."""
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    CULTURE_SIGNALS = "culture_signals"
    GOVERNANCE_SIGNALS = "governance_signals"
    UNKNOWN = "unknown"


class FilingType(str, Enum):
    FORM_10K = "10-K"
    FORM_10Q = "10-Q"
    FORM_8K = "8-K"
    DEF_14A = "DEF 14A"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ExtractedEntity:
    """Entity extracted from evidence text."""
    entity_type: str
    text: str
    char_start: int
    char_end: int
    confidence: float
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CS2Evidence:
    """Evidence item derived from the platform's CompanyEvidenceResponse."""
    evidence_id: str
    company_id: str
    source_type: SourceType
    signal_category: SignalCategory
    content: str          # signal raw_value or serialised metadata
    extracted_at: datetime
    confidence: float

    fiscal_year: Optional[int] = None
    source_url: Optional[str] = None
    page_number: Optional[int] = None
    extracted_entities: List[ExtractedEntity] = field(default_factory=list)

    indexed_in_cs4: bool = False
    indexed_at: Optional[datetime] = None


@dataclass
class DocumentInfo:
    """Metadata for a collected SEC document."""
    document_id: str
    ticker: str
    filing_type: str
    filing_date: Optional[str]
    status: str
    word_count: int = 0
    chunk_count: int = 0


@dataclass
class SignalScore:
    """Score for a single signal category."""
    ticker: str
    category: str
    status: str
    score: Optional[float] = None
    confidence: Optional[float] = None
    breakdown: Optional[Dict[str, Any]] = None
    data_source: Optional[str] = None
    evidence_count: Optional[int] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


@dataclass
class AllSignalScores:
    """All signal scores for a company."""
    ticker: str
    company_name: Optional[str]
    scores: Dict[str, SignalScore]
    composite_score: Optional[float] = None
    total_duration_seconds: Optional[float] = None


@dataclass
class CultureSignal:
    """Glassdoor culture signal for a company."""
    ticker: str
    overall_score: Optional[float] = None
    innovation_score: Optional[float] = None
    data_driven_score: Optional[float] = None
    change_readiness_score: Optional[float] = None
    ai_awareness_score: Optional[float] = None
    review_count: Optional[int] = None
    avg_rating: Optional[float] = None
    confidence: Optional[float] = None
    positive_keywords_found: List[str] = field(default_factory=list)
    negative_keywords_found: List[str] = field(default_factory=list)


@dataclass
class GovernanceSignal:
    """Board governance signal for a company."""
    ticker: str
    governance_score: Optional[float] = None
    confidence: Optional[float] = None
    independent_ratio: Optional[float] = None
    tech_expertise_count: int = 0
    has_tech_committee: bool = False
    has_ai_expertise: bool = False
    has_data_officer: bool = False
    has_risk_tech_oversight: bool = False
    has_ai_in_strategy: bool = False
    ai_experts: List[str] = field(default_factory=list)
    relevant_committees: List[str] = field(default_factory=list)
    score_breakdown: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _map_category(raw: str) -> SignalCategory:
    try:
        return SignalCategory(raw)
    except ValueError:
        return SignalCategory.UNKNOWN


def _map_source(raw: str) -> SourceType:
    try:
        return SourceType(raw)
    except ValueError:
        return SourceType.UNKNOWN


def _signal_to_evidence(sig: dict, company_id: str) -> CS2Evidence:
    """Convert a platform SignalEvidence dict to CS2Evidence."""
    raw_content = sig.get("raw_value")
    if raw_content is None:
        meta = sig.get("metadata")
        raw_content = json.dumps(meta) if meta else ""

    extracted_at_raw = sig.get("signal_date") or sig.get("created_at")
    try:
        extracted_at = datetime.fromisoformat(str(extracted_at_raw)) if extracted_at_raw else datetime.utcnow()
    except (ValueError, TypeError):
        extracted_at = datetime.utcnow()

    return CS2Evidence(
        evidence_id=str(sig.get("id", "")),
        company_id=company_id,
        source_type=_map_source(sig.get("source", "")),
        signal_category=_map_category(sig.get("category", "")),
        content=str(raw_content),
        extracted_at=extracted_at,
        confidence=float(sig.get("confidence") or 0.5),
        source_url=sig.get("source_url"),
    )


def _parse_doc(d: dict) -> DocumentInfo:
    return DocumentInfo(
        document_id=str(d.get("id", d.get("document_id", ""))),
        ticker=d.get("ticker", ""),
        filing_type=d.get("filing_type", ""),
        filing_date=str(d.get("filing_date", "")) if d.get("filing_date") else None,
        status=d.get("status", "unknown"),
        word_count=d.get("word_count") or 0,
        chunk_count=d.get("chunk_count") or 0,
    )


def _parse_signal_score(d: dict) -> SignalScore:
    return SignalScore(
        ticker=d.get("ticker", ""),
        category=d.get("category", ""),
        status=d.get("status", "unknown"),
        score=d.get("score"),
        confidence=d.get("confidence"),
        breakdown=d.get("breakdown"),
        data_source=d.get("data_source"),
        evidence_count=d.get("evidence_count"),
        error=d.get("error"),
        duration_seconds=d.get("duration_seconds"),
    )


def _parse_governance(d: dict) -> GovernanceSignal:
    return GovernanceSignal(
        ticker=d.get("ticker", ""),
        governance_score=d.get("governance_score"),
        confidence=d.get("confidence"),
        independent_ratio=d.get("independent_ratio"),
        tech_expertise_count=d.get("tech_expertise_count", 0),
        has_tech_committee=d.get("has_tech_committee", False),
        has_ai_expertise=d.get("has_ai_expertise", False),
        has_data_officer=d.get("has_data_officer", False),
        has_risk_tech_oversight=d.get("has_risk_tech_oversight", False),
        has_ai_in_strategy=d.get("has_ai_in_strategy", False),
        ai_experts=d.get("ai_experts", []),
        relevant_committees=d.get("relevant_committees", []),
        score_breakdown=d.get("score_breakdown"),
    )


# ---------------------------------------------------------------------------
# Groq helper
# ---------------------------------------------------------------------------

async def expand_keywords_with_groq(ticker: str, category: str) -> List[str]:
    """
    Use Groq LLM to expand keywords for a given signal category and company.
    Returns a list of relevant keywords/phrases for matching evidence.
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
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
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
        logger.warning("groq_keyword_expansion_failed", ticker=ticker, category=category, error=str(e))
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
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                },
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
        logger.warning("groq_summary_failed", ticker=ticker, category=category, error=str(e))
        return None


# ---------------------------------------------------------------------------
# Main CS2Client
# ---------------------------------------------------------------------------

class CS2Client:
    """
    Client for CS2 Evidence Collection — reads from pe-org-air-platform at localhost:8000.

    Sections:
      1. Document Collection (SEC EDGAR)
      2. Document Parsing
      3. Document Chunking
      4. Document Management & Reports
      5. Signal Scoring (hiring, digital, innovation, leadership)
      6. Glassdoor Culture Signals
      7. Board Governance Signals
      8. Evidence Summary & Stats
      9. Groq-enhanced helpers
    """

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=120.0)

    # -----------------------------------------------------------------------
    # 1. Document Collection
    # -----------------------------------------------------------------------

    async def collect_documents(
        self,
        ticker: str,
        filing_types: Optional[List[FilingType]] = None,
        years_back: int = 3,
    ) -> Dict[str, Any]:
        """
        Collect SEC filings for a single company.
        POST /api/v1/documents/collect

        Downloads 10-K, 10-Q, 8-K, DEF 14A filings from SEC EDGAR,
        uploads raw files to S3, and saves metadata to Snowflake.
        """
        if filing_types is None:
            filing_types = [FilingType.FORM_10K, FilingType.FORM_10Q,
                            FilingType.FORM_8K, FilingType.DEF_14A]

        payload = {
            "ticker": ticker.upper(),
            "filing_types": [ft.value for ft in filing_types],
            "years_back": years_back,
        }
        response = await self.client.post(
            f"{self.base_url}/api/v1/documents/collect",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    async def collect_all_documents(
        self,
        filing_types: Optional[List[FilingType]] = None,
        years_back: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Collect SEC filings for all 10 target companies.
        POST /api/v1/documents/collect/all
        """
        if filing_types is None:
            filing_types = [FilingType.FORM_10K, FilingType.FORM_10Q,
                            FilingType.FORM_8K, FilingType.DEF_14A]

        params = {
            "filing_types": [ft.value for ft in filing_types],
            "years_back": years_back,
        }
        response = await self.client.post(
            f"{self.base_url}/api/v1/documents/collect/all",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # 2. Document Parsing
    # -----------------------------------------------------------------------

    async def parse_documents(self, ticker: str) -> Dict[str, Any]:
        """
        Parse all collected SEC filings for a company.
        POST /api/v1/documents/parse/{ticker}

        Downloads raw docs from S3, extracts text/tables,
        identifies key sections (Risk Factors, MD&A), uploads parsed JSON to S3.
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/documents/parse/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    async def parse_all_documents(self) -> Dict[str, Any]:
        """
        Parse documents for all 10 target companies.
        POST /api/v1/documents/parse
        """
        response = await self.client.post(f"{self.base_url}/api/v1/documents/parse")
        response.raise_for_status()
        return response.json()

    async def get_parsed_document(self, document_id: str) -> Dict[str, Any]:
        """
        Get parsed content of a document from S3.
        GET /api/v1/documents/parsed/{document_id}

        Returns text preview, tables, section keys, and word/table counts.
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/parsed/{document_id}"
        )
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # 3. Document Chunking
    # -----------------------------------------------------------------------

    async def chunk_documents(
        self,
        ticker: str,
        chunk_size: int = 750,
        chunk_overlap: int = 50,
    ) -> Dict[str, Any]:
        """
        Split parsed documents into overlapping chunks for LLM processing.
        POST /api/v1/documents/chunk/{ticker}

        Uploads chunks to S3 (sec/chunks/{ticker}/...) and saves metadata to Snowflake.
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/documents/chunk/{ticker.upper()}",
            params={"chunk_size": chunk_size, "chunk_overlap": chunk_overlap},
        )
        response.raise_for_status()
        return response.json()

    async def chunk_all_documents(
        self,
        chunk_size: int = 750,
        chunk_overlap: int = 50,
    ) -> Dict[str, Any]:
        """
        Chunk documents for all 10 target companies.
        POST /api/v1/documents/chunk
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/documents/chunk",
            params={"chunk_size": chunk_size, "chunk_overlap": chunk_overlap},
        )
        response.raise_for_status()
        return response.json()

    async def get_document_chunks(self, document_id: str) -> Dict[str, Any]:
        """
        Get all chunks for a specific document.
        GET /api/v1/documents/chunks/{document_id}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/chunks/{document_id}"
        )
        response.raise_for_status()
        return response.json()

    async def get_chunk_stats(self, ticker: str) -> Dict[str, Any]:
        """
        Get chunk statistics for a company.
        GET /api/v1/documents/chunk/stats/{ticker}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/chunk/stats/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # 4. Document Management & Reports
    # -----------------------------------------------------------------------

    async def list_documents(
        self,
        ticker: Optional[str] = None,
        filing_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List all documents with optional filters.
        GET /api/v1/documents
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if ticker:
            params["ticker"] = ticker.upper()
        if filing_type:
            params["filing_type"] = filing_type
        if status:
            params["status"] = status

        response = await self.client.get(
            f"{self.base_url}/api/v1/documents",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def get_document(self, document_id: str) -> DocumentInfo:
        """
        Get document metadata by ID.
        GET /api/v1/documents/{document_id}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/{document_id}"
        )
        response.raise_for_status()
        return _parse_doc(response.json())

    async def get_document_stats(self, ticker: str) -> Dict[str, Any]:
        """
        Get document statistics for a company.
        GET /api/v1/documents/stats/{ticker}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/stats/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    async def get_evidence_report(self) -> Dict[str, Any]:
        """
        Get comprehensive evidence collection report (JSON).
        GET /api/v1/documents/report
        """
        response = await self.client.get(f"{self.base_url}/api/v1/documents/report")
        response.raise_for_status()
        return response.json()

    async def get_section_analysis(self, ticker: str) -> Dict[str, Any]:
        """
        Get section word counts and keyword mentions for a single company.
        GET /api/v1/documents/analysis/{ticker}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/documents/analysis/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    async def get_all_section_analysis(self) -> Dict[str, Any]:
        """
        Get section analysis tables for all 10 companies.
        GET /api/v1/documents/analysis
        """
        response = await self.client.get(f"{self.base_url}/api/v1/documents/analysis")
        response.raise_for_status()
        return response.json()

    async def reset_company_documents(self, ticker: str) -> Dict[str, Any]:
        """
        Delete all data for a company (S3 raw/parsed/chunks + Snowflake records).
        DELETE /api/v1/documents/reset/{ticker}

        For demo/testing only.
        """
        response = await self.client.delete(
            f"{self.base_url}/api/v1/documents/reset/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # 5. Signal Scoring
    # -----------------------------------------------------------------------

    async def collect_signals(
        self,
        company_id: str,
        categories: Optional[List[str]] = None,
        years_back: int = 5,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Trigger signal collection for a company (runs in background).
        POST /api/v1/signals/collect

        Returns a task_id. Poll get_signal_task_status() to check progress.
        Categories: technology_hiring, innovation_activity, digital_presence, leadership_signals
        """
        if categories is None:
            categories = [c.value for c in SignalCategory
                          if c not in (SignalCategory.UNKNOWN, SignalCategory.CULTURE_SIGNALS,
                                       SignalCategory.GOVERNANCE_SIGNALS)]

        payload = {
            "company_id": company_id,
            "categories": categories,
            "years_back": years_back,
            "force_refresh": force_refresh,
        }
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/collect",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    async def get_signal_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get status of a background signal collection task.
        GET /api/v1/signals/tasks/{task_id}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/signals/tasks/{task_id}"
        )
        response.raise_for_status()
        return response.json()

    async def list_signals(
        self,
        ticker: Optional[str] = None,
        category: Optional[str] = None,
        min_score: Optional[float] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        List all signals with optional filters.
        GET /api/v1/signals/detailed
        """
        params: Dict[str, Any] = {"limit": limit}
        if ticker:
            params["ticker"] = ticker.upper()
        if category:
            params["category"] = category
        if min_score is not None:
            params["min_score"] = min_score

        response = await self.client.get(
            f"{self.base_url}/api/v1/signals/detailed",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def score_hiring(self, ticker: str, force_refresh: bool = False) -> SignalScore:
        """
        Score technology hiring signal for a company.
        POST /api/v1/signals/score/{ticker}/hiring

        Source: Job postings from LinkedIn, Indeed, Glassdoor (via JobSpy)
        CS3 feeds: Talent (0.70), Tech Stack (0.20), Culture (0.10)
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/score/{ticker.upper()}/hiring",
            params={"force_refresh": force_refresh},
        )
        response.raise_for_status()
        return _parse_signal_score(response.json())

    async def score_digital_presence(self, ticker: str, force_refresh: bool = False) -> SignalScore:
        """
        Score digital presence signal for a company.
        POST /api/v1/signals/score/{ticker}/digital

        Source: BuiltWith Free API + Wappalyzer
        CS3 feeds: Data Infrastructure (0.60), Technology Stack (0.40)
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/score/{ticker.upper()}/digital",
            params={"force_refresh": force_refresh},
        )
        response.raise_for_status()
        return _parse_signal_score(response.json())

    async def score_innovation(self, ticker: str, years_back: int = 5) -> SignalScore:
        """
        Score innovation activity signal for a company.
        POST /api/v1/signals/score/{ticker}/innovation

        Source: PatentsView API (USPTO patent data)
        CS3 feeds: Technology Stack (0.50), Use Case Portfolio (0.30), Data Infra (0.20)
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/score/{ticker.upper()}/innovation",
            params={"years_back": years_back},
        )
        response.raise_for_status()
        return _parse_signal_score(response.json())

    async def score_leadership(self, ticker: str) -> SignalScore:
        """
        Score leadership signals for a company.
        POST /api/v1/signals/score/{ticker}/leadership

        Source: SEC DEF-14A proxy statements
        CS3 feeds: Leadership (0.60), AI Governance (0.25), Culture (0.15)
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/score/{ticker.upper()}/leadership"
        )
        response.raise_for_status()
        return _parse_signal_score(response.json())

    async def score_all_signals(self, ticker: str, force_refresh: bool = False) -> AllSignalScores:
        """
        Score all 4 signal categories for a company sequentially.
        POST /api/v1/signals/score/{ticker}/all

        Composite = 0.30*hiring + 0.25*innovation + 0.25*digital + 0.20*leadership
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/signals/score/{ticker.upper()}/all",
            params={"force_refresh": force_refresh},
        )
        response.raise_for_status()
        data = response.json()
        raw_results = data.get("results", {})
        return AllSignalScores(
            ticker=data.get("ticker", ticker.upper()),
            company_name=data.get("company_name"),
            scores={cat: _parse_signal_score(v) for cat, v in raw_results.items()},
            composite_score=data.get("composite_score"),
            total_duration_seconds=data.get("total_duration_seconds"),
        )

    async def get_current_scores(self, ticker: str) -> Dict[str, Any]:
        """
        Get the latest stored scores for all signal categories (no new collection).
        GET /api/v1/signals/{ticker}/current-scores
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/signals/{ticker.upper()}/current-scores"
        )
        response.raise_for_status()
        return response.json()

    async def reset_signals(self, ticker: str) -> Dict[str, Any]:
        """
        Delete all signals for a company from Snowflake and S3.
        DELETE /api/v1/signals/reset/{ticker}
        """
        response = await self.client.delete(
            f"{self.base_url}/api/v1/signals/reset/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    async def reset_signals_by_category(self, ticker: str, category: str) -> Dict[str, Any]:
        """
        Delete signals for a company filtered by category.
        DELETE /api/v1/signals/reset/{ticker}/{category}
        """
        response = await self.client.delete(
            f"{self.base_url}/api/v1/signals/reset/{ticker.upper()}/{category}"
        )
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # 6. Glassdoor Culture Signals
    # -----------------------------------------------------------------------

    async def collect_culture_signal(self, ticker: str) -> Dict[str, Any]:
        """
        Collect and analyze culture reviews for one company.
        POST /api/v1/glassdoor-signals/{ticker}

        Scrapes Glassdoor/Indeed/CareerBliss, analyzes to produce CultureSignal,
        uploads to S3, upserts to Snowflake.
        Works for any ticker registered in Snowflake.
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/glassdoor-signals/{ticker.upper()}"
        )
        response.raise_for_status()
        return response.json()

    async def get_culture_signal(self, ticker: str) -> CultureSignal:
        """
        Get full Glassdoor culture score breakdown for a company.
        GET /api/v1/glassdoor-signals/{ticker}

        Returns overall_score, innovation_score, data_driven_score,
        change_readiness_score, ai_awareness_score, keyword analysis.
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/glassdoor-signals/{ticker.upper()}"
        )
        response.raise_for_status()
        d = response.json()
        return CultureSignal(
            ticker=d.get("ticker", ticker.upper()),
            overall_score=d.get("overall_score"),
            innovation_score=d.get("innovation_score"),
            data_driven_score=d.get("data_driven_score"),
            change_readiness_score=d.get("change_readiness_score"),
            ai_awareness_score=d.get("ai_awareness_score"),
            review_count=d.get("review_count"),
            avg_rating=d.get("avg_rating"),
            confidence=d.get("confidence"),
            positive_keywords_found=d.get("positive_keywords_found") or [],
            negative_keywords_found=d.get("negative_keywords_found") or [],
        )

    async def get_all_culture_signals(self) -> List[CultureSignal]:
        """
        Get culture signal breakdowns for all 5 CS3 portfolio companies.
        GET /api/v1/glassdoor-signals/portfolio/all
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/glassdoor-signals/portfolio/all"
        )
        response.raise_for_status()
        data = response.json()
        results = []
        for d in data.get("results", []):
            results.append(CultureSignal(
                ticker=d.get("ticker", ""),
                overall_score=d.get("overall_score"),
                innovation_score=d.get("innovation_score"),
                data_driven_score=d.get("data_driven_score"),
                change_readiness_score=d.get("change_readiness_score"),
                ai_awareness_score=d.get("ai_awareness_score"),
                review_count=d.get("review_count"),
                avg_rating=d.get("avg_rating"),
                confidence=d.get("confidence"),
                positive_keywords_found=d.get("positive_keywords_found") or [],
                negative_keywords_found=d.get("negative_keywords_found") or [],
            ))
        return results

    # -----------------------------------------------------------------------
    # 7. Board Governance Signals
    # -----------------------------------------------------------------------

    async def analyze_board_governance(self, ticker: str) -> GovernanceSignal:
        """
        Analyze board governance for a single company and persist results.
        POST /api/v1/board-governance/analyze/{ticker}

        Extracts board composition, tech committee, AI expertise from DEF 14A proxy.
        Works for any ticker registered in Snowflake.
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/board-governance/analyze/{ticker.upper()}"
        )
        response.raise_for_status()
        return _parse_governance(response.json())

    async def analyze_all_board_governance(self) -> List[GovernanceSignal]:
        """
        Analyze board governance for all 5 CS3 companies (NVDA, JPM, WMT, GE, DG).
        POST /api/v1/board-governance/analyze
        """
        response = await self.client.post(f"{self.base_url}/api/v1/board-governance/analyze")
        response.raise_for_status()
        data = response.json()
        return [_parse_governance(r) for r in data.get("results", [])]

    async def get_governance_score(self, ticker: str) -> GovernanceSignal:
        """
        Get latest governance signal for a ticker (tries S3 first, then live).
        GET /api/v1/board-governance/score/{ticker}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/board-governance/score/{ticker.upper()}"
        )
        response.raise_for_status()
        return _parse_governance(response.json())

    async def get_all_governance_scores(self) -> List[GovernanceSignal]:
        """
        Get latest governance signals for all 5 CS3 companies.
        GET /api/v1/board-governance/scores
        """
        response = await self.client.get(f"{self.base_url}/api/v1/board-governance/scores")
        response.raise_for_status()
        data = response.json()
        return [_parse_governance(r) for r in data.get("results", [])]

    # -----------------------------------------------------------------------
    # 8. Evidence Summary & Stats
    # -----------------------------------------------------------------------

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
        Fetch evidence for a company.
        GET /api/v1/companies/{ticker}/evidence

        Returns aggregated SEC filing statistics and signal evidence.
        Client-side filters applied for source_types, signal_categories,
        min_confidence, and since (date).
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/companies/{company_id}/evidence"
        )
        response.raise_for_status()
        payload = response.json()

        cid = payload.get("company_id", company_id)
        evidence_list: List[CS2Evidence] = [
            _signal_to_evidence(sig, cid)
            for sig in payload.get("signals", [])
        ]

        if source_types:
            st_set = {s.value for s in source_types}
            evidence_list = [e for e in evidence_list if e.source_type.value in st_set]
        if signal_categories:
            sc_set = {s.value for s in signal_categories}
            evidence_list = [e for e in evidence_list if e.signal_category.value in sc_set]
        if min_confidence > 0:
            evidence_list = [e for e in evidence_list if e.confidence >= min_confidence]
        if since:
            evidence_list = [e for e in evidence_list if e.extracted_at >= since]

        return evidence_list

    async def get_evidence_stats(self) -> dict:
        """
        Fetch overall evidence collection statistics.
        GET /api/v1/evidence/stats

        Returns document counts, signal scores, category breakdowns for all companies.
        """
        response = await self.client.get(f"{self.base_url}/api/v1/evidence/stats")
        response.raise_for_status()
        return response.json()

    async def trigger_backfill(
        self,
        skip_recent_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Trigger full evidence backfill (SEC docs + signals) for all 10 companies.
        POST /api/v1/evidence/backfill

        Returns a task_id. Poll get_backfill_status() to check progress.
        """
        params = {"skip_recent_hours": skip_recent_hours, "force": force}
        response = await self.client.post(
            f"{self.base_url}/api/v1/evidence/backfill",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def get_backfill_status(self, task_id: str) -> Dict[str, Any]:
        """
        Check progress of a backfill task.
        GET /api/v1/evidence/backfill/tasks/{task_id}
        """
        response = await self.client.get(
            f"{self.base_url}/api/v1/evidence/backfill/tasks/{task_id}"
        )
        response.raise_for_status()
        return response.json()

    async def cancel_backfill(self, task_id: str) -> Dict[str, Any]:
        """
        Cancel a running backfill task (stops after current company finishes).
        POST /api/v1/evidence/backfill/tasks/{task_id}/cancel
        """
        response = await self.client.post(
            f"{self.base_url}/api/v1/evidence/backfill/tasks/{task_id}/cancel"
        )
        response.raise_for_status()
        return response.json()

    async def mark_indexed(self, evidence_ids: List[str]) -> int:
        """
        Mark evidence IDs as indexed in CS4.
        NOTE: The platform does not currently expose a mark-indexed endpoint.
        This is a no-op stub that returns the count for interface compatibility.
        """
        return len(evidence_ids)

    # -----------------------------------------------------------------------
    # 9. Groq-enhanced helpers
    # -----------------------------------------------------------------------

    async def get_keywords_for_category(self, ticker: str, category: str) -> List[str]:
        """
        Get Groq-expanded keywords for a signal category and company.
        Uses static fallback if Groq is unavailable.
        """
        return await expand_keywords_with_groq(ticker, category)

    async def get_signal_summary(self, ticker: str, category: str) -> Optional[str]:
        """
        Get a Groq-generated IC-ready summary for a signal category.

        Fetches latest scores from the platform, then asks Groq to summarize.
        Returns None if no data or Groq is unavailable.
        """
        try:
            scores = await self.get_current_scores(ticker)
            category_data = scores.get(category.replace("_", "_")) or {}
            if not category_data:
                return None
            return await get_groq_signal_summary(ticker, category, category_data)
        except Exception as e:
            logger.warning("signal_summary_failed", ticker=ticker, category=category, error=str(e))
            return None

    async def get_full_evidence_with_keywords(
        self,
        ticker: str,
        category: str,
    ) -> Dict[str, Any]:
        """
        Get evidence for a ticker filtered by category, with Groq-expanded keywords
        returned alongside for use in downstream RAG matching.

        Returns:
          {
            "ticker": str,
            "category": str,
            "keywords": List[str],
            "evidence": List[CS2Evidence],
            "groq_summary": Optional[str],
          }
        """
        try:
            sc = SignalCategory(category)
        except ValueError:
            sc = SignalCategory.UNKNOWN

        evidence = await self.get_evidence(
            company_id=ticker,
            signal_categories=[sc] if sc != SignalCategory.UNKNOWN else None,
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

    async def close(self):
        await self.client.aclose()
