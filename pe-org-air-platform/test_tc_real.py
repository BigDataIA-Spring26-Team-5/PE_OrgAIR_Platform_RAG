#!/usr/bin/env python3
"""
test_tc_real.py
---------------
Runs TalentConcentrationCalculator against raw data fetched directly from S3:
  - Job postings  : s3://pe-orgair-platform/signals/jobs/{TICKER}/latest.json
  - Glassdoor reviews : s3://pe-orgair-platform/glassdoor_signals/raw/{TICKER}/latest_raw.json

No local files, no live scraping, no Snowflake.

Usage:
    cd pe-org-air-platform
    python test_tc_real.py
"""

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load .env so AWS credentials are available without starting the full app
load_dotenv(Path(__file__).parent / ".env")

# Allow importing from app/ without installing the package
sys.path.insert(0, str(Path(__file__).parent))

from app.scoring.talent_concentration import (
    GlassdoorReview,
    TalentConcentrationCalculator,
)


class _S3Client:
    """
    Minimal S3 wrapper that mirrors the list_files / get_file interface of
    app.services.S3StorageService — without triggering the app's circular
    import chain through app/services/__init__.py.
    """

    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        self._bucket = os.environ.get("S3_BUCKET", "pe-orgair-platform")

    def list_files(self, prefix: str) -> list[str]:
        try:
            resp = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        except ClientError as exc:
            print(f"  [warn] S3 list failed for {prefix}: {exc}")
            return []

    def get_file(self, key: str) -> bytes | None:
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
            return resp["Body"].read()
        except ClientError as exc:
            print(f"  [warn] S3 get failed for {key}: {exc}")
            return None

TICKERS = ["JPM", "NVDA"]


# ── S3 loaders ────────────────────────────────────────────────────────────────

def load_glassdoor_from_s3(ticker: str, s3) -> list[GlassdoorReview]:
    """
    Fetch the Glassdoor raw snapshot from S3 for *ticker*.

    S3 path : glassdoor_signals/raw/{TICKER}_raw.json
    JSON    : {"reviews": [{review_id, rating, pros, cons, ...}], ...}
    """
    key = f"glassdoor_signals/raw/{ticker}_raw.json"
    try:
        raw = s3.get_file(key)
        if raw is None:
            print(f"  [warn] No Glassdoor snapshot found at {key}")
            return []

        wrapper = json.loads(raw)
        reviews: list[GlassdoorReview] = []
        for r in wrapper.get("reviews", []):
            reviews.append(GlassdoorReview(
                review_id=r.get("review_id", ""),
                rating=float(r.get("rating") or 0.0),
                title=r.get("title") or "",
                pros=r.get("pros") or "",
                cons=r.get("cons") or "",
                advice_to_management=r.get("advice_to_management"),
                is_current_employee=bool(r.get("is_current_employee", False)),
                job_title=r.get("job_title") or "",
                review_date=r.get("review_date"),
                source=r.get("source", "unknown"),
            ))
        print(f"  [{ticker}] Glassdoor: {len(reviews)} reviews from {key}")
        return reviews

    except Exception as exc:
        print(f"  [warn] Glassdoor S3 load failed for {ticker}: {exc}")
        return []


def load_jobs_from_s3(ticker: str, s3) -> list[dict]:
    """
    Fetch the most recent job postings snapshot from S3 for *ticker*.

    S3 path : signals/jobs/{TICKER}/{TIMESTAMP}.json
    JSON    : {"job_postings": [{title, is_ai_role, ai_keywords_found, ...}], ...}
    """
    prefix = f"signals/jobs/{ticker}/"
    try:
        keys = s3.list_files(prefix)
        if not keys:
            print(f"  [warn] No job postings found at {prefix}")
            return []

        # Iterate newest → oldest; pick first file that contains at least one job.
        # (A newer file may be an empty/failed run.)
        for key in sorted(keys, reverse=True):
            raw = s3.get_file(key)
            if raw is None:
                continue
            data = json.loads(raw)
            postings = data.get("job_postings", [])
            if postings:
                # Normalize: TalentConcentrationCalculator expects ai_skills_found;
                # the S3 payload stores the same data under ai_keywords_found.
                for p in postings:
                    if "ai_skills_found" not in p:
                        p["ai_skills_found"] = p.get("ai_keywords_found", [])
                print(f"  [{ticker}] Jobs: {len(postings)} postings from {key}")
                return postings
            print(f"  [{ticker}] Skipping empty file {key}")

        print(f"  [warn] All job files are empty for {ticker}")
        return []

    except Exception as exc:
        print(f"  [warn] Job S3 load failed for {ticker}: {exc}")
        return []


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    s3 = _S3Client()
    calc = TalentConcentrationCalculator()

    header = (
        f"{'Ticker':<8} {'Jobs':>6} {'AI Jobs':>8} "
        f"{'Reviews':>8} {'AI Mentions':>14} {'TC Score':>10}"
    )
    rule = "-" * len(header)

    rows: list[tuple] = []

    for ticker in TICKERS:
        print(f"\nFetching {ticker}...")
        reviews = load_glassdoor_from_s3(ticker, s3)
        jobs    = load_jobs_from_s3(ticker, s3)

        if not reviews and not jobs:
            print(f"  [skip] No data available for {ticker}")
            continue

        job_analysis = calc.analyze_job_postings(jobs)
        ai_mentions, total_reviews = calc.count_ai_mentions(reviews)
        indiv_mentions, _ = calc.count_individual_mentions(reviews)
        tc: Decimal = calc.calculate_tc(job_analysis, indiv_mentions, total_reviews)

        rows.append((
            ticker,
            len(jobs),
            job_analysis.total_ai_jobs,
            total_reviews,
            f"{ai_mentions}/{total_reviews}",
            str(tc),
        ))

    print()
    print("Talent Concentration — S3 Real Data Run")
    print("=" * len(header))
    print(header)
    print(rule)
    for ticker, n_jobs, ai_jobs, n_reviews, mention_str, tc_str in rows:
        print(
            f"{ticker:<8} {n_jobs:>6} {ai_jobs:>8} "
            f"{n_reviews:>8} {mention_str:>14} {tc_str:>10}"
        )
    print(rule)
    print()
    print("Done.")
    print()
    print("Notes:")
    print("  TC formula : 0.4×leadership_ratio + 0.3×team_size_factor")
    print("             + 0.2×skill_concentration + 0.1×individual_factor")
    print("  All data   : fetched live from S3 bucket pe-orgair-platform")


if __name__ == "__main__":
    main()
