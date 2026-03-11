"""
CS4 Complete Pipeline Exercise — NVDA Data Infrastructure

End-to-end demonstration:
  CS1 company lookup → CS3 score → CS2 evidence fetch → index → justify

Run from pe-org-air-platform/:
    python -m exercises.complete_pipeline
"""
from __future__ import annotations

import asyncio
import json
import sys

from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client
from app.services.search.vector_store import VectorStore
from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument
from app.services.retrieval.dimension_mapper import DimensionMapper
from app.services.retrieval.hyde import HyDERetriever
from app.services.justification.generator import JustificationGenerator
from app.services.llm.router import ModelRouter


def run_pipeline(ticker: str = "NVDA", dimension: str = "data_infrastructure"):
    print(f"\n{'='*60}")
    print(f"CS4 RAG Pipeline — {ticker} / {dimension}")
    print(f"{'='*60}\n")

    # ── Step 1: CS1 — Company Lookup ─────────────────────────────
    print("[1/6] Fetching company metadata from CS1...")
    cs1 = CS1Client()
    # company = cs1.get_company(ticker)
    company = asyncio.run(cs1.get_company(ticker))
    if company:
        print(f"  Found: {company.name} ({company.ticker})")
        print(f"    Sector: {company.sector} | Revenue: ${company.revenue_millions:.0f}M")
        print(f"    Employees: {company.employee_count:,}")
    else:
        print(f"  Company {ticker} not found in CS1 — using placeholder")
        from app.services.integration.cs1_client import Company
        company = Company(
            company_id=ticker,
            ticker=ticker,
            name="NVIDIA Corporation",
            sector="Semiconductors",
            sub_sector="AI/GPU",
            revenue_millions=60922.0,
            employee_count=29600,
        )

    # ── Step 2: CS3 — Score Lookup ────────────────────────────────
    print(f"\n[2/6] Fetching {dimension} score from CS3...")
    cs3 = CS3Client()
    dim_score = cs3.get_dimension_score(company.company_id, dimension)
    if dim_score:
        print(f"  Score: {dim_score.score:.1f}/100 (Level {dim_score.level} — {dim_score.level_name})")
        rubric = cs3.get_rubric(dimension, dim_score.level)
        print(f"  Rubric: {rubric[0].criteria[:80]}..." if rubric else "  No rubric available")
    else:
        print(f"  No score found for {dimension} — using placeholder score of 78")

    # ── Step 3: CS2 — Evidence Fetch ─────────────────────────────
    print(f"\n[3/6] Fetching evidence from CS2...")
    cs2 = CS2Client()
    evidence = cs2.get_evidence(company_id=company.company_id, min_confidence=0.3)
    print(f"  Fetched {len(evidence)} evidence records")

    # ── Step 4: Index into Vector Store ──────────────────────────
    print(f"\n[4/6] Indexing evidence into ChromaDB...")
    mapper = DimensionMapper()
    vs = VectorStore(persist_dir="./chroma_data")
    indexed = vs.index_cs2_evidence(evidence, mapper)
    print(f"  Indexed {indexed} documents (total in store: {vs.count()})")

    # Also index into hybrid retriever
    retriever = HybridRetriever(persist_dir="./chroma_data")
    hybrid_docs = [
        RetrievedDocument(
            doc_id=ev.evidence_id,
            content=ev.content,
            metadata={
                "company_id": ev.company_id,
                "source_type": ev.source_type,
                "signal_category": ev.signal_category,
                "dimension": mapper.get_primary_dimension(ev.signal_category),
                "confidence": ev.confidence,
            },
            score=ev.confidence,
            retrieval_method="direct",
        )
        for ev in evidence
        if ev.content
    ]
    retriever.index_documents(hybrid_docs)
    print(f"  Hybrid retriever indexed {len(hybrid_docs)} documents")

    # ── Step 5: HyDE Search ───────────────────────────────────────
    print(f"\n[5/6] Running HyDE-enhanced retrieval for '{dimension}'...")
    llm_router = ModelRouter()
    hyde = HyDERetriever(retriever, llm_router)

    query = f"NVIDIA {dimension} infrastructure capabilities AI data platform"
    context = f"{company.name} is a {company.sector} company with {company.employee_count:,} employees"
    results = hyde.retrieve(
        query,
        k=5,
        filters={"company_id": company.company_id} if evidence else None,
        dimension=dimension,
        company_context=context,
    )
    print(f"  Retrieved {len(results)} documents via HyDE")
    for i, r in enumerate(results[:3], 1):
        print(f"    [{i}] score={r.score:.3f} | {r.content[:100]}...")

    # ── Step 6: Justification Generation ─────────────────────────
    print(f"\n[6/6] Generating IC justification for {ticker}/{dimension}...")
    gen = JustificationGenerator(cs3=cs3, retriever=retriever, router=llm_router)
    justification = gen.generate_justification(company.company_id, dimension)

    print(f"\n{'─'*60}")
    print(f"SCORE JUSTIFICATION")
    print(f"{'─'*60}")
    print(f"Company: {ticker}")
    print(f"Dimension: {dimension}")
    print(f"Score: {justification.score:.1f}/100 (Level {justification.level} — {justification.level_name})")
    print(f"Evidence Strength: {justification.evidence_strength}")
    print(f"Supporting Evidence: {len(justification.supporting_evidence)} pieces")
    print(f"\nGenerated Summary:")
    print(justification.generated_summary)
    if justification.gaps_identified:
        print(f"\nGaps Identified:")
        for gap in justification.gaps_identified:
            print(f"  - {gap}")

    return justification


if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "NVDA"
    dimension = sys.argv[2] if len(sys.argv) > 2 else "data_infrastructure"
    run_pipeline(ticker, dimension)
