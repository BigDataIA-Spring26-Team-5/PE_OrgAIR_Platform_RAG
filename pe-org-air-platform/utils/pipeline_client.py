"""
Pipeline Client — utils/pipeline_client.py

Orchestrates the full CS1→CS2→CS3→CS4 pipeline via HTTP calls to FastAPI.
Used by the Streamlit app to trigger and monitor pipeline progress.

Pipeline steps:
  1. Check/Create company in CS1 (upsert with yfinance data)
  2. Collect SEC filings via CS2 (1 year back)
  3. Collect external signals via CS2 (1 year back)
  4. Score company via CS3
  5. Index evidence into ChromaDB via CS4

All steps return PipelineStepResult with status, message, and data.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable
import requests

logger = logging.getLogger(__name__)

# Default FastAPI base URL
DEFAULT_BASE_URL = "http://localhost:8000"

# Pipeline configuration
SEC_YEARS_BACK = 2       # 2 years = enough context for strong RAG citations
SIGNALS_YEARS_BACK = 1   # Signals only need recent data (1 year is sufficient)
SEC_FILING_TYPES = ["10-K", "10-Q", "DEF 14A", "8-K"]  # Full coverage — 10-Q adds ~1.5min but no score risk


@dataclass
class PipelineStepResult:
    """Result of a single pipeline step."""
    step: int
    name: str
    status: str          # "success", "skipped", "error", "running"
    message: str
    data: Dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: Optional[str] = None


@dataclass
class PipelineResult:
    """Full pipeline result for a company."""
    ticker: str
    company_name: str
    company_id: Optional[str]
    steps: List[PipelineStepResult] = field(default_factory=list)
    overall_status: str = "pending"  # "success", "partial", "error"
    total_duration_seconds: float = 0.0

    @property
    def is_success(self) -> bool:
        return self.overall_status == "success"

    @property
    def failed_steps(self) -> List[PipelineStepResult]:
        return [s for s in self.steps if s.status == "error"]

    @property
    def org_air_score(self) -> Optional[float]:
        for step in self.steps:
            if step.name == "Scoring" and step.status == "success":
                return step.data.get("org_air_score")
        return None

    @property
    def indexed_count(self) -> int:
        for step in self.steps:
            if step.name == "Index Evidence" and step.status == "success":
                return step.data.get("indexed_count", 0)
        return 0


class PipelineClient:
    """
    Orchestrates the full pipeline via HTTP calls to FastAPI.

    Usage:
        client = PipelineClient()
        result = client.run_pipeline(
            resolved_company,
            on_step_complete=lambda step: st.write(step.message)
        )
    """

    def __init__(self, base_url: str = DEFAULT_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _get(self, path: str, params: dict = None) -> requests.Response:
        return self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=300,
        )

    def _post(self, path: str, body: dict = None) -> requests.Response:
        return self.session.post(
            f"{self.base_url}{path}",
            json=body or {},
            timeout=300,
        )

    def _put(self, path: str, body: dict = None) -> requests.Response:
        return self.session.put(
            f"{self.base_url}{path}",
            json=body or {},
            timeout=300,
        )

    # ── Step 1: Check or Create Company ──────────────────────────

    def check_or_create_company(self, resolved) -> PipelineStepResult:
        """
        Upsert company into CS1:
          - EXISTS + enriched → skip
          - EXISTS + null fields → update enriched fields
          - NOT EXISTS → create with full data
        """
        start = time.time()
        ticker = resolved.ticker

        try:
            # Check if company exists
            resp = self._get(f"/api/v1/companies/{ticker}")

            if resp.status_code == 200:
                existing = resp.json()
                company_id = str(existing["id"])

                # Check if enriched fields are missing
                needs_update = (
                    not existing.get("sector") or
                    not existing.get("revenue_millions") or
                    not existing.get("employee_count")
                )

                if needs_update:
                    # Update missing enriched fields
                    update_resp = self._put(
                        f"/api/v1/companies/{ticker}",
                        {
                            "sector": resolved.sector,
                            "sub_sector": resolved.sub_sector,
                            "market_cap_percentile": resolved.market_cap_percentile,
                            "revenue_millions": resolved.revenue_millions,
                            "employee_count": resolved.employee_count,
                            "fiscal_year_end": resolved.fiscal_year_end,
                            "position_factor": resolved.position_factor,
                        }
                    )
                    return PipelineStepResult(
                        step=1,
                        name="Company Setup",
                        status="success",
                        message=f"✅ {resolved.name} ({ticker}) — enriched fields updated",
                        data={"company_id": company_id, "action": "updated"},
                        duration_seconds=time.time() - start,
                    )
                else:
                    return PipelineStepResult(
                        step=1,
                        name="Company Setup",
                        status="skipped",
                        message=f"✅ {resolved.name} ({ticker}) — already exists in platform",
                        data={"company_id": company_id, "action": "skipped"},
                        duration_seconds=time.time() - start,
                    )

            elif resp.status_code == 404:
                # Company doesn't exist — create it
                # First get industry list to find correct industry_id
                create_resp = self._post(
                    "/api/v1/companies",
                    {
                        "name": resolved.name,
                        "ticker": ticker,
                        "industry_id": resolved.industry_id,
                        "position_factor": resolved.position_factor,
                    }
                )
                create_resp.raise_for_status()
                created = create_resp.json()
                company_id = str(created["id"])

                return PipelineStepResult(
                    step=1,
                    name="Company Setup",
                    status="success",
                    message=f"✅ {resolved.name} ({ticker}) — created in platform",
                    data={
                        "company_id": company_id,
                        "action": "created",
                        "sector": resolved.sector,
                        "revenue_millions": resolved.revenue_millions,
                        "employee_count": resolved.employee_count,
                    },
                    duration_seconds=time.time() - start,
                )
            else:
                resp.raise_for_status()

        except Exception as e:
            return PipelineStepResult(
                step=1,
                name="Company Setup",
                status="error",
                message=f"❌ Failed to setup company {ticker}",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    # ── Step 2: Collect SEC Filings ───────────────────────────────

    def collect_sec_filings(self, ticker: str) -> PipelineStepResult:
        """Collect SEC filings via CS2 (1 year back for speed)."""
        start = time.time()
        try:
            resp = self._post(
                "/api/v1/documents/collect",
                {
                    "ticker": ticker,
                    "filing_types": SEC_FILING_TYPES,
                    "years_back": SEC_YEARS_BACK,
                }
            )
            resp.raise_for_status()
            data = resp.json()

            doc_count = data.get("document_count", data.get("total_documents", 0))
            chunk_count = data.get("chunk_count", data.get("total_chunks", 0))

            return PipelineStepResult(
                step=2,
                name="SEC Filings",
                status="success",
                message=(
                    f"✅ SEC filings collected — "
                    f"{doc_count} documents, {chunk_count} chunks"
                ),
                data=data,
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=2,
                name="SEC Filings",
                status="error",
                message=f"❌ SEC filing collection failed for {ticker}",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    # ── Step 3: Collect External Signals ─────────────────────────

    def collect_signals(self, company_id: str, ticker: str) -> PipelineStepResult:
        """Collect external signals (jobs, patents, tech stack) via CS2."""
        start = time.time()
        try:
            resp = self._post(
                "/api/v1/signals/collect",
                {
                    "company_id": company_id,
                    "categories": [
                        "technology_hiring",
                        "innovation_activity",
                        "digital_presence",
                        "leadership_signals",
                    ],
                    "years_back": SIGNALS_YEARS_BACK,
                    "force_refresh": False,
                }
            )
            resp.raise_for_status()
            data = resp.json()

            signal_count = data.get("signal_count", data.get("total_signals", 0))
            scores = data.get("scores", {})

            msg_parts = [f"✅ Signals collected — {signal_count} signals"]
            if scores:
                hiring = scores.get("technology_hiring", 0)
                innovation = scores.get("innovation_activity", 0)
                msg_parts.append(f"Hiring: {hiring:.0f} | Innovation: {innovation:.0f}")

            return PipelineStepResult(
                step=3,
                name="External Signals",
                status="success",
                message=" | ".join(msg_parts),
                data=data,
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=3,
                name="External Signals",
                status="error",
                message=f"❌ Signal collection failed for {ticker}",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    # ── Step 4: Score Company ─────────────────────────────────────

    def score_company(self, ticker: str) -> PipelineStepResult:
        """Run CS3 scoring pipeline for the company."""
        start = time.time()
        try:
            resp = self._post(f"/api/v1/scoring/{ticker}")
            resp.raise_for_status()
            data = resp.json()

            org_air = data.get("org_air_score", data.get("final_score", 0))
            vr = data.get("vr_score", 0)
            hr = data.get("hr_score", 0)

            # Extract top dimension scores for display
            dim_scores = data.get("dimension_scores", [])
            if isinstance(dim_scores, list):
                top_dims = sorted(
                    dim_scores,
                    key=lambda x: x.get("score", 0),
                    reverse=True
                )[:3]
                top_str = " | ".join(
                    f"{d['dimension'].replace('_', ' ').title()}: {d['score']:.0f}"
                    for d in top_dims
                )
            else:
                top_str = ""

            return PipelineStepResult(
                step=4,
                name="Scoring",
                status="success",
                message=(
                    f"✅ Scoring complete — "
                    f"Org-AI-R: {org_air:.1f} | V^R: {vr:.1f} | H^R: {hr:.1f}"
                    + (f"\n   Top: {top_str}" if top_str else "")
                ),
                data=data,
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=4,
                name="Scoring",
                status="error",
                message=f"❌ Scoring failed for {ticker}",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    # ── Step 5: Index Evidence into ChromaDB ─────────────────────

    def index_evidence(self, ticker: str) -> PipelineStepResult:
        """Index CS2 evidence into ChromaDB via CS4 RAG endpoint."""
        start = time.time()
        try:
            resp = self._post(
                f"/rag/index/{ticker}",
            )
            resp.raise_for_status()
            data = resp.json()

            indexed_count = data.get("indexed_count", 0)
            source_counts = data.get("source_counts", {})

            # Format source breakdown
            source_str = " | ".join(
                f"{k.replace('_', ' ').title()}: {v}"
                for k, v in source_counts.items()
                if v > 0
            )

            return PipelineStepResult(
                step=5,
                name="Index Evidence",
                status="success",
                message=(
                    f"✅ {indexed_count} evidence pieces indexed into ChromaDB"
                    + (f"\n   {source_str}" if source_str else "")
                    + "\n   💬 Chatbot is now ready!"
                ),
                data=data,
                duration_seconds=time.time() - start,
            )
        except Exception as e:
            return PipelineStepResult(
                step=5,
                name="Index Evidence",
                status="error",
                message=f"❌ Evidence indexing failed for {ticker}",
                error=str(e),
                duration_seconds=time.time() - start,
            )

    # ── Full Pipeline ─────────────────────────────────────────────

    def run_pipeline(
        self,
        resolved,
        on_step_start: Optional[Callable] = None,
        on_step_complete: Optional[Callable] = None,
    ) -> PipelineResult:
        """
        Run the full CS1→CS2→CS3→CS4 pipeline.

        Args:
            resolved:          ResolvedCompany from company_resolver.py
            on_step_start:     Callback(step_name) called before each step
            on_step_complete:  Callback(PipelineStepResult) called after each step

        Returns:
            PipelineResult with all step results
        """
        pipeline_start = time.time()
        result = PipelineResult(
            ticker=resolved.ticker,
            company_name=resolved.name,
            company_id=None,
        )

        steps = [
            ("Company Setup",     lambda: self.check_or_create_company(resolved)),
            ("SEC Filings",       lambda: self.collect_sec_filings(resolved.ticker)),
            ("External Signals",  lambda: None),  # needs company_id — filled below
            ("Scoring",           lambda: self.score_company(resolved.ticker)),
            ("Index Evidence",    lambda: self.index_evidence(resolved.ticker)),
        ]

        company_id = None

        for i, (step_name, step_fn) in enumerate(steps):
            if on_step_start:
                on_step_start(step_name)

            # Step 3 needs company_id from Step 1
            if step_name == "External Signals":
                if not company_id:
                    step_result = PipelineStepResult(
                        step=3,
                        name="External Signals",
                        status="skipped",
                        message="⚠️ Skipped — company_id not available",
                    )
                else:
                    step_result = self.collect_signals(company_id, resolved.ticker)
            else:
                step_result = step_fn()

            # Extract company_id from Step 1 result
            if step_name == "Company Setup" and step_result.status in ("success", "skipped"):
                company_id = step_result.data.get("company_id")
                result.company_id = company_id

            result.steps.append(step_result)

            if on_step_complete:
                on_step_complete(step_result)

            # Stop pipeline on critical errors (Step 1 or 2 failure)
            if step_result.status == "error" and i < 2:
                result.overall_status = "error"
                result.total_duration_seconds = time.time() - pipeline_start
                return result

        # Determine overall status
        errors = [s for s in result.steps if s.status == "error"]
        if not errors:
            result.overall_status = "success"
        elif len(errors) < len(result.steps):
            result.overall_status = "partial"
        else:
            result.overall_status = "error"

        result.total_duration_seconds = time.time() - pipeline_start
        return result

    def get_company_status(self, ticker: str) -> Dict[str, Any]:
        """
        Check current status of a company in the platform.
        Used to determine if chatbot is available.
        """
        status = {
            "ticker": ticker,
            "in_cs1": False,
            "has_scores": False,
            "indexed_in_chroma": False,
            "chatbot_ready": False,
            "company_id": None,
            "org_air_score": None,
            "indexed_documents": 0,
        }

        # Check CS1
        try:
            resp = self._get(f"/api/v1/companies/{ticker}")
            if resp.status_code == 200:
                data = resp.json()
                status["in_cs1"] = True
                status["company_id"] = str(data["id"])
        except Exception:
            pass

        # Check scores
        try:
            resp = self._get(f"/api/v1/scoring/{ticker}/dimensions")
            if resp.status_code == 200:
                data = resp.json()
                # Scoring returns dimension_scores as list, no composite org_air_score
                # Endpoint returns key 'scores' (not 'dimension_scores')
                dim_scores = data.get("scores", data.get("dimension_scores", []))
                status["has_scores"] = bool(dim_scores)
                if dim_scores and isinstance(dim_scores, list):
                    score_vals = [d.get("score", 0) for d in dim_scores if d.get("score")]
                    status["org_air_score"] = round(sum(score_vals) / len(score_vals), 1) if score_vals else None
                else:
                    status["org_air_score"] = data.get("org_air_score")
        except Exception:
            pass

        # Check ChromaDB index via status endpoint
        try:
            resp = self._get("/rag/status")
            if resp.status_code == 200:
                data = resp.json()
                total = data.get("indexed_documents", 0)
                # Check if this specific ticker is indexed via search
                search_resp = self._post("/rag/search", {
                    "query": "AI capabilities",
                    "ticker": ticker,
                    "top_k": 1,
                })
                if search_resp.status_code == 200:
                    results = search_resp.json()
                    ticker_count = len(results)
                    status["indexed_documents"] = ticker_count
                    status["indexed_in_chroma"] = ticker_count > 0
                else:
                    # Fallback: if total > 0, assume indexed
                    status["indexed_documents"] = total
                    status["indexed_in_chroma"] = total > 0
        except Exception:
            pass

        status["chatbot_ready"] = (
            status["in_cs1"] and
            status["indexed_in_chroma"]
        )

        return status