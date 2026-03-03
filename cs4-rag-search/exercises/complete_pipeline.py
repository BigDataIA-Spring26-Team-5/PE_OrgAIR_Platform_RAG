"""
complete_pipeline.py — CS4 RAG Search
exercises/complete_pipeline.py

End-to-end exercise scaffold: runs a full IC prep workflow for a single ticker.
Run with: python exercises/complete_pipeline.py

Prerequisites:
  1. pip install -r requirements.txt
  2. ChromaDB running: docker compose up chromadb
  3. pe-org-air-platform running on http://localhost:8000
  4. OPENAI_API_KEY (or ANTHROPIC_API_KEY) set in environment
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

# Allow running from project root without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

load_dotenv()

from src.services.integration.cs1_client import CS1Client
from src.services.integration.cs2_client import CS2Client
from src.services.integration.cs3_client import CS3Client
from src.services.retrieval.hybrid import HybridRetriever
from src.services.retrieval.dimension_mapper import DimensionMapper, SignalCategory
from src.services.justification.generator import JustificationGenerator
from src.services.workflows.ic_prep import ICPrepWorkflow
from src.services.collection.analyst_notes import AnalystNoteIngester, AnalystNote


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TICKER = os.getenv("TARGET_TICKER", "NVDA")
ASSESSMENT_ID = os.getenv("ASSESSMENT_ID", None)  # optional
PLATFORM_URL = os.getenv("PLATFORM_URL", "http://localhost:8000")
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8001"))
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")


# ---------------------------------------------------------------------------
# Exercise helpers
# ---------------------------------------------------------------------------

def exercise_dimension_mapper() -> None:
    """Exercise 1: verify DimensionMapper routing."""
    print("\n=== Exercise 1: DimensionMapper ===")
    dm = DimensionMapper()
    for cat in [SignalCategory.TECHNOLOGY_HIRING, SignalCategory.GOVERNANCE_SIGNALS]:
        weights = dm.get_dimension_weights(cat)
        primary = dm.get_primary_dimension(cat)
        dims = dm.get_all_dimensions_for_evidence(cat, min_weight=0.1)
        print(f"\n{cat.value}:")
        print(f"  primary dimension : {primary.value}")
        print(f"  all weights       : { {d.value: float(w) for d, w in weights.items()} }")
        print(f"  dims >= 0.10 wt   : {[d.value for d in dims]}")


async def exercise_clients() -> None:
    """Exercise 2: verify client imports and basic API connectivity."""
    print("\n=== Exercise 2: Client Connectivity ===")
    async with CS1Client(base_url=PLATFORM_URL) as cs1:
        try:
            companies = await cs1.list_companies(page_size=5)
            print(f"CS1: found {len(companies)} companies (first 5)")
            for c in companies[:3]:
                print(f"  {c.ticker}: {c.name}")
        except Exception as exc:
            print(f"CS1: {exc} (is the platform running?)")


async def exercise_full_pipeline() -> None:
    """Exercise 3: full IC prep for TICKER."""
    print(f"\n=== Exercise 3: Full IC Prep — {TICKER} ===")

    retriever = HybridRetriever(
        collection_name="cs4_evidence",
        chroma_host=CHROMA_HOST,
        chroma_port=CHROMA_PORT,
    )

    # Seed the retriever with a stub document so search doesn't fail on empty index
    ingester = AnalystNoteIngester(retriever)
    ingester.ingest_text(
        content=(
            f"{TICKER} has demonstrated strong AI integration across engineering teams. "
            "Recent 10-K disclosures highlight significant R&D investment in machine learning "
            "infrastructure and data platform modernisation."
        ),
        ticker=TICKER,
        source_type="analyst_interview",
        author="exercise_seed",
    )

    async with (
        CS1Client(base_url=PLATFORM_URL) as cs1,
        CS2Client(base_url=PLATFORM_URL) as cs2,
        CS3Client(base_url=PLATFORM_URL) as cs3,
    ):
        workflow = ICPrepWorkflow(
            cs1=cs1,
            cs2=cs2,
            cs3=cs3,
            retriever=retriever,
            generator=JustificationGenerator(model=LLM_MODEL),
        )

        result = await workflow.run(ticker=TICKER, assessment_id=ASSESSMENT_ID)

    print(f"\nTicker    : {result.ticker}")
    print(f"Company   : {result.company.name if result.company else 'N/A'}")
    print(f"Chunks    : {len(result.evidence_chunks)}")
    print(f"Errors    : {result.errors}")

    if result.memo:
        print(f"\n--- IC Memo ({result.memo.generated_by_model}) ---")
        print(f"Assessment  : {result.memo.overall_assessment}")
        print(f"Org-AI-R    : {result.memo.org_air_score}")
        print(f"Recommendation: {result.memo.recommendation}")
        print(f"Key risks   : {result.memo.key_risks[:3]}")
    else:
        print("\nNo memo generated (missing company/assessment or LLM key not set).")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    exercise_dimension_mapper()
    await exercise_clients()
    await exercise_full_pipeline()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
