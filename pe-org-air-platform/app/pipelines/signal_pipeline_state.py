"""
Signal Pipeline State
app/pipelines/signal_pipeline_state.py

State container for the external signals collection pipeline
(jobs, patents). Tracks companies, postings, scores, and errors.

All storage is S3 + Snowflake. No local file paths.

Renamed from pipeline2_state.py for clarity.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class SignalPipelineState:
    """State container for job + patent signal collection."""

    # Configuration
    request_delay: float = 6.0
    results_per_company: int = 50
    mode: str = "jobs"  # "jobs", "patents", or "both"

    # Patents config
    patents_years_back: int = 5
    patents_api_key: Optional[str] = None

    # Company data (from Snowflake or manual)
    companies: List[Dict[str, Any]] = field(default_factory=list)

    # Collected data
    job_postings: List[Dict[str, Any]] = field(default_factory=list)
    patents: List[Dict[str, Any]] = field(default_factory=list)

    # Per-company data
    company_job_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    company_patent_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Scores (company_id -> score)
    job_market_scores: Dict[str, float] = field(default_factory=dict)
    patent_scores: Dict[str, float] = field(default_factory=dict)

    # Analyses (company_id -> analysis dict)
    job_market_analyses: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # S3 loaded data
    loaded_s3_data: Dict[str, Any] = field(
        default_factory=lambda: {"jobs": {}, "patents": {}}
    )

    # Step tracking
    steps_completed: List[str] = field(default_factory=list)
    step_history: List[Dict[str, Any]] = field(default_factory=list)

    # Summary
    summary: Dict[str, Any] = field(
        default_factory=lambda: {
            "companies_processed": 0,
            "job_postings_collected": 0,
            "ai_jobs_found": 0,
            "patents_collected": 0,
            "ai_patents_found": 0,
            "s3_files_uploaded": 0,
            "s3_files_read": 0,
            "snowflake_records_inserted": 0,
            "snowflake_scores_updated": 0,
            "errors": [],
            "started_at": None,
            "completed_at": None,
        }
    )

    # Compat stats
    stats: Dict[str, Any] = field(
        default_factory=lambda: {
            "downloaded": 0,
            "parsed": 0,
            "duplicates_skipped": 0,
            "unique_filings": 0,
            "total_chunks": 0,
            "items_extracted": 0,
            "errors": 0,
            "error_details": [],
        }
    )

    # ----- Error handling -----

    def add_error(
        self, step: str, error: str, company_id: Optional[str] = None
    ) -> None:
        entry: Dict[str, Any] = {
            "step": step,
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if company_id:
            entry["company_id"] = company_id
        self.summary["errors"].append(entry)
        self.stats["errors"] += 1
        self.stats["error_details"].append(entry)

    # ----- Step tracking -----

    def mark_step_complete(self, step_name: str) -> None:
        if step_name not in self.steps_completed:
            self.steps_completed.append(step_name)
        self.step_history.append({
            "step": step_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def is_step_complete(self, step_name: str) -> bool:
        return step_name in self.steps_completed

    # ----- Lifecycle -----

    def mark_started(self) -> None:
        self.summary["started_at"] = datetime.now(timezone.utc).isoformat()

    def mark_completed(self) -> None:
        self.summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.summary["companies_processed"] = len(self.companies)

        self.summary["ai_jobs_found"] = sum(
            1
            for jd in self.company_job_data.values()
            for j in jd.get("jobs", [])
            if j.get("is_ai_role")
        )
        self.summary["ai_patents_found"] = sum(
            1
            for pd in self.company_patent_data.values()
            for p in pd.get("patents", [])
            if p.get("is_ai_patent")
        )
        self.summary["job_postings_collected"] = sum(
            len(d.get("jobs", [])) for d in self.company_job_data.values()
        )
        self.summary["patents_collected"] = sum(
            len(d.get("patents", [])) for d in self.company_patent_data.values()
        )

    def reset(self) -> None:
        self.steps_completed.clear()
        self.step_history.clear()
        self.stats = {
            "downloaded": 0, "parsed": 0, "duplicates_skipped": 0,
            "unique_filings": 0, "total_chunks": 0,
            "items_extracted": 0, "errors": 0, "error_details": [],
        }
        self.summary["errors"].clear()

    # ----- Data helpers -----

    def add_company_job_data(self, company_id: str, job_data: Dict[str, Any]) -> None:
        self.company_job_data[company_id] = job_data
        self.job_postings.extend(job_data.get("jobs", []))
        self.summary["job_postings_collected"] += len(job_data.get("jobs", []))

    def add_company_patent_data(self, company_id: str, patent_data: Dict[str, Any]) -> None:
        self.company_patent_data[company_id] = patent_data
        self.patents.extend(patent_data.get("patents", []))
        self.summary["patents_collected"] += len(patent_data.get("patents", []))

    def get_company_name(self, company_id: str) -> str:
        for c in self.companies:
            if c.get("id") == company_id:
                return c.get("name", company_id)
        return company_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config": {
                "request_delay": self.request_delay,
                "results_per_company": self.results_per_company,
                "mode": self.mode,
                "patents_years_back": self.patents_years_back,
            },
            "companies": self.companies,
            "steps_completed": self.steps_completed,
            "scores": {
                "job_market": self.job_market_scores,
                "patent": self.patent_scores,
            },
            "summary": self.summary,
            "stats": self.stats,
        }