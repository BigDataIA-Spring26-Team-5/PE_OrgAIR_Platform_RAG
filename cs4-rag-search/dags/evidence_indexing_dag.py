"""
evidence_indexing_dag.py — CS4 RAG Search
dags/evidence_indexing_dag.py

Airflow DAG for nightly evidence indexing into ChromaDB.
Pulls CS2 evidence from the platform and indexes it for RAG retrieval.

Usage:
  Place this file in your Airflow dags/ folder (or mount via docker-compose).
  Requires: apache-airflow>=2.9, httpx, chromadb, sentence-transformers
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

# Guard: only import Airflow if available (avoids import errors in plain Python)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    _AIRFLOW_AVAILABLE = True
except ImportError:
    _AIRFLOW_AVAILABLE = False

PLATFORM_URL = os.getenv("PLATFORM_URL", "http://localhost:8000")
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8001"))
TARGET_TICKERS = os.getenv("TARGET_TICKERS", "NVDA,JPM,WMT,GE,DG").split(",")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def fetch_and_index_evidence(ticker: str, **context: object) -> int:
    """
    Fetch CS2 evidence for ticker and upsert into ChromaDB.
    Returns number of documents indexed.
    """
    import httpx
    import chromadb
    from sentence_transformers import SentenceTransformer

    print(f"[{ticker}] fetching evidence from {PLATFORM_URL}")

    # Fetch evidence
    with httpx.Client(base_url=PLATFORM_URL, timeout=60) as client:
        resp = client.get(f"/companies/{ticker}/evidence")
        resp.raise_for_status()
        signals = resp.json().get("signals", [])

    if not signals:
        print(f"[{ticker}] no signals found — skipping")
        return 0

    # Prepare documents
    texts = [s.get("raw_value") or s.get("id", "") for s in signals]
    ids = [s["id"] for s in signals]
    metadatas = [
        {
            "ticker": ticker,
            "category": s.get("category", ""),
            "source": s.get("source", ""),
            "normalized_score": str(s.get("normalized_score", "")),
        }
        for s in signals
    ]

    # Embed and upsert
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = encoder.encode(texts, show_progress_bar=False).tolist()

    chroma = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    collection = chroma.get_or_create_collection("cs4_evidence")
    collection.upsert(documents=texts, ids=ids, embeddings=embeddings, metadatas=metadatas)

    print(f"[{ticker}] indexed {len(ids)} documents")
    return len(ids)


def verify_index(**context: object) -> None:
    """Verify the ChromaDB collection count after indexing."""
    import chromadb
    chroma = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    collection = chroma.get_or_create_collection("cs4_evidence")
    count = collection.count()
    print(f"ChromaDB cs4_evidence collection: {count} documents")


# ---------------------------------------------------------------------------
# DAG definition (only if Airflow is available)
# ---------------------------------------------------------------------------

if _AIRFLOW_AVAILABLE:
    default_args = {
        "owner": "cs4",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    }

    with DAG(
        dag_id="evidence_indexing",
        description="Nightly CS2 evidence indexing into ChromaDB for RAG",
        schedule_interval="0 2 * * *",  # 2 AM daily
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["cs4", "rag", "evidence"],
    ) as dag:

        index_tasks = [
            PythonOperator(
                task_id=f"index_{ticker.lower()}",
                python_callable=fetch_and_index_evidence,
                op_kwargs={"ticker": ticker},
            )
            for ticker in TARGET_TICKERS
        ]

        verify_task = PythonOperator(
            task_id="verify_index",
            python_callable=verify_index,
        )

        # All index tasks run in parallel, then verify
        index_tasks >> verify_task  # type: ignore[operator]
