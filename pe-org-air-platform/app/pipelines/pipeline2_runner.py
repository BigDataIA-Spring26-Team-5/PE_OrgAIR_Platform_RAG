"""
Pipeline 2 Runner - Job and Patent Collection
app/pipelines/pipeline2_runner.py

Scrapes job postings and fetches patents for companies.
All storage goes to S3 + Snowflake. No local file writes.

Tech stack / digital_presence is handled separately by tech_signals.py
(BuiltWith + Wappalyzer) and is NOT part of this pipeline.

Examples:
  python -m app.pipelines.pipeline2_runner --companies CAT DE UNH
  python -m app.pipelines.pipeline2_runner --companies JPM --mode patents
  python -m app.pipelines.pipeline2_runner --companies WMT --mode both
  python -m app.pipelines.pipeline2_runner --companies GS --step extract
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from app.pipelines.signal_pipeline_state import SignalPipelineState
from app.pipelines.job_signals import run_job_signals
from app.pipelines.patent_signals import run_patent_signals
from app.pipelines.utils import Company, safe_filename
from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService

logger = logging.getLogger(__name__)
load_dotenv()


class Pipeline2Runner:
    """Pipeline 2 runner — jobs + patents → S3 + Snowflake."""

    def __init__(self):
        self.state = SignalPipelineState()
        self.s3 = S3Storage()
        self.snowflake = None

    def _init_snowflake(self):
        if self.snowflake is None:
            self.snowflake = SnowflakeService()

    def _close_snowflake(self):
        if self.snowflake:
            self.snowflake.close()
            self.snowflake = None

    # ------------------------------------------------------------------
    # STEP 1: EXTRACT DATA
    # ------------------------------------------------------------------

    async def step_extract_data(
        self,
        *,
        companies: List[str],
        mode: str = "jobs",
        jobs_request_delay: float = 6.0,
        patents_request_delay: float = 1.5,
        jobs_results_per_company: int = 50,
        patents_results_per_company: int = 100,
        patents_years_back: int = 5,
        patents_api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        print("=" * 60)
        print("Step 1: Extract Data")
        print("=" * 60)

        if not companies:
            return {"status": "error", "message": "No companies provided"}

        company_list = [
            Company.from_name(name, i).to_dict()
            for i, name in enumerate(companies)
        ]
        self.state.companies = company_list

        print(f"\nCompanies: {len(self.state.companies)}")
        for c in self.state.companies:
            print(f"  - {c['name']}")

        # Jobs
        if mode in ("jobs", "both"):
            print("\n" + "-" * 60)
            print("Extracting Job Postings")
            print("-" * 60)
            self.state.request_delay = jobs_request_delay
            self.state.results_per_company = jobs_results_per_company
            self.state = await run_job_signals(self.state, skip_storage=True)

        # Patents
        if mode in ("patents", "both"):
            print("\n" + "-" * 60)
            print("Extracting Patent Data")
            print("-" * 60)
            self.state.request_delay = patents_request_delay
            self.state.results_per_company = patents_results_per_company
            self.state = await run_patent_signals(
                self.state,
                years_back=patents_years_back,
                results_per_company=patents_results_per_company,
                api_key=patents_api_key,
                skip_storage=True,
            )

        self.state.mark_step_complete("extract")

        return {
            "status": "success",
            "companies_processed": len(self.state.companies),
            "job_postings": self.state.summary.get("job_postings_collected", 0),
            "patents_collected": self.state.summary.get("patents_collected", 0),
            "mode": mode,
        }

    # ------------------------------------------------------------------
    # STEP 2: VALIDATE
    # ------------------------------------------------------------------

    def step_validate_data(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 2: Validate Extracted Data")
        print("=" * 60)

        if not self.state.is_step_complete("extract"):
            return {"status": "error", "message": "Extract step not complete"}

        job_counts: Dict[str, int] = {}
        patent_counts: Dict[str, int] = {}

        for p in self.state.job_postings:
            cid = p.get("company_id", "unknown")
            job_counts[cid] = job_counts.get(cid, 0) + 1

        for p in self.state.patents:
            cid = p.get("company_id", "unknown")
            patent_counts[cid] = patent_counts.get(cid, 0) + 1

        print("\nData Summary:")
        for c in self.state.companies:
            cid = c.get("id", "")
            name = c.get("name", cid)
            print(f"  ✓ {name}: {job_counts.get(cid, 0)} jobs, {patent_counts.get(cid, 0)} patents")

        self.state.mark_step_complete("validate")

        return {
            "status": "success",
            "total_jobs": len(self.state.job_postings),
            "total_patents": len(self.state.patents),
        }

    # ------------------------------------------------------------------
    # STEP 3: VERIFY SCORES
    # ------------------------------------------------------------------

    def step_verify_scores(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 3: Verify Scores")
        print("=" * 60)

        if not self.state.is_step_complete("validate"):
            return {"status": "error", "message": "Validate step not complete"}

        if self.state.job_market_scores:
            print("\nJob Market Scores:")
            for c in self.state.companies:
                cid = c.get("id", "")
                name = c.get("name", cid)
                score = self.state.job_market_scores.get(cid, 0)
                if score > 0:
                    print(f"  ✓ {name}: {score:.1f}/100")

        if self.state.patent_scores:
            print("\nPatent Portfolio Scores:")
            for c in self.state.companies:
                cid = c.get("id", "")
                name = c.get("name", cid)
                score = self.state.patent_scores.get(cid, 0)
                if score > 0:
                    print(f"  ✓ {name}: {score:.1f}/100")

        self.state.mark_step_complete("score")

        return {
            "status": "success",
            "job_scores": len(self.state.job_market_scores),
            "patent_scores": len(self.state.patent_scores),
        }

    # ------------------------------------------------------------------
    # STEP 4: UPLOAD TO S3
    # ------------------------------------------------------------------

    def step_upload_to_s3(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 4: Upload to S3")
        print("=" * 60)

        if not self.state.is_step_complete("score"):
            return {"status": "error", "message": "Score step not complete"}

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uploads = 0

        company_jobs = defaultdict(list)
        company_patents = defaultdict(list)

        for p in self.state.job_postings:
            company_jobs[p.get("company_id", "unknown")].append(p)
        for p in self.state.patents:
            company_patents[p.get("company_id", "unknown")].append(p)

        # Upload jobs
        for cid, jobs in company_jobs.items():
            name = self._get_company_name(cid)
            ticker = self._get_ticker(cid) or safe_filename(name).upper()
            key = f"signals/jobs/{ticker}/{timestamp}.json"
            data = {
                "company_id": cid,
                "company_name": name,
                "ticker": ticker,
                "collection_date": timestamp,
                "total_jobs": len(jobs),
                "ai_jobs": sum(1 for j in jobs if j.get("is_ai_role")),
                "job_market_score": self.state.job_market_scores.get(cid, 0),
                "job_market_analysis": self.state.job_market_analyses.get(cid, {}),
                "jobs": jobs,
            }
            self.s3.upload_json(data, key)
            uploads += 1
            print(f"  📤 {key}")

        # Upload patents
        for cid, patents in company_patents.items():
            name = self._get_company_name(cid)
            ticker = self._get_ticker(cid) or safe_filename(name).upper()
            key = f"signals/patents/{ticker}/{timestamp}.json"
            data = {
                "company_id": cid,
                "company_name": name,
                "ticker": ticker,
                "collection_date": timestamp,
                "total_patents": len(patents),
                "ai_patents": sum(1 for p in patents if p.get("is_ai_patent")),
                "patent_score": self.state.patent_scores.get(cid, 0),
                "patents": patents,
            }
            self.s3.upload_json(data, key)
            uploads += 1
            print(f"  📤 {key}")

        self.state.mark_step_complete("s3_upload")
        print(f"\n✅ Uploaded {uploads} files to S3")

        return {"status": "success", "files_uploaded": uploads}

    # ------------------------------------------------------------------
    # STEP 5: WRITE TO SNOWFLAKE
    # ------------------------------------------------------------------

    def step_write_to_snowflake(self) -> Dict[str, Any]:
        print("\n" + "=" * 60)
        print("Step 5: Write to Snowflake")
        print("=" * 60)

        if not self.state.is_step_complete("s3_upload"):
            return {"status": "error", "message": "S3 upload step not complete"}

        self._init_snowflake()

        try:
            inserts = 0
            for c in self.state.companies:
                cid = c.get("id", "")
                name = c.get("name", "")
                job_score = self.state.job_market_scores.get(cid, 0)
                patent_score = self.state.patent_scores.get(cid, 0)

                if job_score > 0 or patent_score > 0:
                    try:
                        scores = [s for s in (job_score, patent_score) if s > 0]
                        total = sum(scores) / len(scores) if scores else 0

                        self.snowflake.insert_company_signal_summary(
                            company_id=cid,
                            company_name=name,
                            job_market_score=job_score,
                            patent_portfolio_score=patent_score,
                            techstack_score=0,  # Now handled by tech_signals.py separately
                            total_score=total,
                            calculated_at=datetime.now(timezone.utc),
                        )
                        inserts += 1
                        print(f"  ✓ {name}: Job={job_score:.1f}, Patent={patent_score:.1f}")
                    except Exception as e:
                        print(f"  ✗ {name}: {e}")

            self.state.mark_step_complete("snowflake_write")
            return {"status": "success", "inserts": inserts}
        finally:
            self._close_snowflake()

    # ------------------------------------------------------------------
    # COMPLETE PIPELINE
    # ------------------------------------------------------------------

    async def run_pipeline(
        self,
        *,
        companies: List[str],
        mode: str = "jobs",
        jobs_request_delay: float = 6.0,
        patents_request_delay: float = 1.5,
        jobs_results_per_company: int = 50,
        patents_results_per_company: int = 100,
        patents_years_back: int = 5,
        patents_api_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        results = {}

        results["step1_extract"] = await self.step_extract_data(
            companies=companies,
            mode=mode,
            jobs_request_delay=jobs_request_delay,
            patents_request_delay=patents_request_delay,
            jobs_results_per_company=jobs_results_per_company,
            patents_results_per_company=patents_results_per_company,
            patents_years_back=patents_years_back,
            patents_api_key=patents_api_key,
        )
        results["step2_validate"] = self.step_validate_data()
        results["step3_score"] = self.step_verify_scores()
        results["step4_s3"] = self.step_upload_to_s3()
        results["step5_snowflake"] = self.step_write_to_snowflake()

        self._print_summary()
        return results

    def _print_summary(self):
        print("\n" + "=" * 60)
        print("Pipeline 2 Complete")
        print("=" * 60)
        print(f"Steps: {', '.join(self.state.steps_completed)}")

        if self.state.job_market_scores:
            print("\nJob Market Scores:")
            for cid, score in self.state.job_market_scores.items():
                print(f"  {self._get_company_name(cid)}: {score:.1f}/100")

        if self.state.patent_scores:
            print("\nPatent Scores:")
            for cid, score in self.state.patent_scores.items():
                print(f"  {self._get_company_name(cid)}: {score:.1f}/100")

        errs = self.state.summary.get("errors", [])
        print(f"\nErrors: {len(errs)}")
        print("\nStorage: S3 + Snowflake (no local files)")

    def _get_company_name(self, company_id: str) -> str:
        for c in self.state.companies:
            if c.get("id") == company_id:
                return c.get("name", company_id)
        return company_id

    def _get_ticker(self, company_id: str) -> Optional[str]:
        for c in self.state.companies:
            if c.get("id") == company_id:
                return c.get("ticker", "").upper() or None
        return None


# ------------------------------------------------------------------
# Convenience function
# ------------------------------------------------------------------

async def run_pipeline2(
    *,
    companies: Optional[List[str]] = None,
    mode: str = "jobs",
    **kwargs,
) -> SignalPipelineState:
    runner = Pipeline2Runner()
    await runner.run_pipeline(companies=companies or [], mode=mode, **kwargs)
    return runner.state


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Pipeline 2: Jobs + Patents → S3 + Snowflake")
    parser.add_argument("--companies", nargs="+", required=True)
    parser.add_argument("--mode", choices=["jobs", "patents", "both"], default="jobs")
    parser.add_argument("--step", choices=["extract", "validate", "score", "s3", "snowflake", "all"], default="all")
    parser.add_argument("--jobs-delay", type=float, default=6.0, dest="jobs_delay")
    parser.add_argument("--patents-delay", type=float, default=1.5, dest="patents_delay")
    parser.add_argument("--jobs-results", type=int, default=50, dest="jobs_results")
    parser.add_argument("--patents-results", type=int, default=100, dest="patents_results")
    parser.add_argument("--years", type=int, default=5, dest="years")
    parser.add_argument("--api-key", default=None, dest="api_key")

    args = parser.parse_args()
    runner = Pipeline2Runner()

    if args.step == "all":
        await runner.run_pipeline(
            companies=args.companies,
            mode=args.mode,
            jobs_request_delay=args.jobs_delay,
            patents_request_delay=args.patents_delay,
            jobs_results_per_company=args.jobs_results,
            patents_results_per_company=args.patents_results,
            patents_years_back=args.years,
            patents_api_key=args.api_key,
        )
    else:
        if args.step == "extract":
            await runner.step_extract_data(
                companies=args.companies, mode=args.mode,
                jobs_request_delay=args.jobs_delay,
                patents_request_delay=args.patents_delay,
                jobs_results_per_company=args.jobs_results,
                patents_results_per_company=args.patents_results,
                patents_years_back=args.years,
                patents_api_key=args.api_key,
            )
        elif args.step == "validate":
            runner.step_validate_data()
        elif args.step == "score":
            runner.step_verify_scores()
        elif args.step == "s3":
            runner.step_upload_to_s3()
        elif args.step == "snowflake":
            runner.step_write_to_snowflake()


if __name__ == "__main__":
    asyncio.run(main())