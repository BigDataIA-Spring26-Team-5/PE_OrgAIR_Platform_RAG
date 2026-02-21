"""
Signals Storage Service (S3-only)
app/services/signals_storage.py

All signal data stored in S3. No local filesystem writes.

S3 layout:
    signals/jobs/{TICKER}/{timestamp}.json
    signals/patents/{TICKER}/{timestamp}.json
    signals/techstack/{TICKER}/{timestamp}.json
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class S3SignalsStorage:
    """S3 storage backend for all signal data."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
    ):
        self.bucket = bucket_name or os.getenv("S3_BUCKET")
        self.region = region or os.getenv("AWS_REGION", "us-east-2")
        self._client = None
        self._enabled = bool(self.bucket)

    @property
    def client(self):
        if self._client is None and self._enabled:
            try:
                self._client = boto3.client("s3", region_name=self.region)
            except (NoCredentialsError, Exception) as e:
                print(f"[S3SignalsStorage] Failed to init S3: {e}")
                self._enabled = False
        return self._client

    @property
    def is_enabled(self) -> bool:
        return self._enabled and self.client is not None

    def upload_json(self, data: Dict[str, Any], s3_key: str) -> Optional[str]:
        """Upload JSON data to S3. Returns key on success, None on failure."""
        if not self.is_enabled:
            return None
        try:
            body = json.dumps(data, indent=2, default=str).encode("utf-8")
            self.client.put_object(
                Bucket=self.bucket, Key=s3_key,
                Body=body, ContentType="application/json",
            )
            return s3_key
        except (ClientError, Exception) as e:
            print(f"[S3SignalsStorage] Upload failed {s3_key}: {e}")
            return None

    def download_json(self, s3_key: str) -> Optional[Dict[str, Any]]:
        """Download JSON from S3."""
        if not self.is_enabled:
            return None
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=s3_key)
            return json.loads(resp["Body"].read().decode("utf-8"))
        except (ClientError, Exception) as e:
            print(f"[S3SignalsStorage] Download failed {s3_key}: {e}")
            return None

    def list_keys(self, prefix: str) -> List[str]:
        """List all S3 keys under a prefix."""
        if not self.is_enabled:
            return []
        keys: List[str] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
        except Exception as e:
            print(f"[S3SignalsStorage] List failed {prefix}: {e}")
        return keys

    def delete_key(self, s3_key: str) -> bool:
        """Delete an S3 object."""
        if not self.is_enabled:
            return False
        try:
            self.client.delete_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False


class SignalsStorage:
    """
    High-level service for storing / retrieving signal data.

    All data lives in S3. Provides typed helpers for jobs, patents, techstack.
    """

    S3_PREFIX = "signals"

    def __init__(self):
        self._s3 = S3SignalsStorage()

    @property
    def s3_enabled(self) -> bool:
        return self._s3.is_enabled

    # ----- Jobs -----

    def save_job_signals(
        self,
        company_id: str,
        company_name: str,
        ticker: str,
        job_postings: List[Dict[str, Any]],
        job_market_score: Optional[float],
    ) -> Optional[str]:
        """Save job signal data to S3. Returns S3 key or None."""
        ticker = ticker.upper()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        total = len(job_postings)
        ai = sum(1 for j in job_postings if j.get("is_ai_role", False))

        data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "total_count": total,
            "ai_count": ai,
            "job_market_score": job_market_score,
            "job_postings": job_postings,
        }
        key = f"{self.S3_PREFIX}/jobs/{ticker}/{ts}.json"
        return self._s3.upload_json(data, key)

    # ----- Patents -----

    def save_patent_signals(
        self,
        company_id: str,
        company_name: str,
        ticker: str,
        patents: List[Dict[str, Any]],
        patent_score: Optional[float],
    ) -> Optional[str]:
        """Save patent signal data to S3."""
        ticker = ticker.upper()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        total = len(patents)
        ai = sum(1 for p in patents if p.get("is_ai_patent", False))

        data = {
            "company_id": company_id,
            "company_name": company_name,
            "ticker": ticker,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "total_count": total,
            "ai_count": ai,
            "patent_portfolio_score": patent_score,
            "patents": patents,
        }
        key = f"{self.S3_PREFIX}/patents/{ticker}/{ts}.json"
        return self._s3.upload_json(data, key)

    # ----- Tech Stack -----

    def save_techstack_signals(
        self,
        ticker: str,
        data: Dict[str, Any],
    ) -> Optional[str]:
        """Save tech stack (digital presence) data to S3."""
        ticker = ticker.upper()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"{self.S3_PREFIX}/techstack/{ticker}/{ts}.json"
        return self._s3.upload_json(data, key)

    # ----- Retrieval -----

    def get_latest(self, signal_type: str, ticker: str) -> Optional[Dict[str, Any]]:
        """Get the most recent signal data for a company by type."""
        prefix = f"{self.S3_PREFIX}/{signal_type}/{ticker.upper()}/"
        keys = self._s3.list_keys(prefix)
        if not keys:
            return None
        # Keys are timestamped, so last alphabetically = most recent
        latest_key = sorted(keys)[-1]
        return self._s3.download_json(latest_key)

    def list_companies_with_signals(self, signal_type: str) -> List[str]:
        """List tickers that have signal data for a given type."""
        prefix = f"{self.S3_PREFIX}/{signal_type}/"
        keys = self._s3.list_keys(prefix)
        tickers = set()
        for k in keys:
            parts = k.replace(prefix, "").split("/")
            if parts:
                tickers.add(parts[0])
        return sorted(tickers)


@lru_cache
def get_signals_storage_service() -> SignalsStorage:
    """Singleton factory for SignalsStorage."""
    return SignalsStorage()