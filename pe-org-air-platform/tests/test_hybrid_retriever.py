"""Tests for HybridRetriever — RRF fusion logic."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.services.retrieval.hybrid import HybridRetriever, RetrievedDocument


def make_doc(doc_id: str, content: str = "test content", score: float = 0.5) -> RetrievedDocument:
    return RetrievedDocument(
        doc_id=doc_id,
        content=content,
        metadata={"company_id": "test"},
        score=score,
        retrieval_method="test",
    )


def test_rrf_fusion_combines_results():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc("a", score=0.9), make_doc("b", score=0.8), make_doc("c", score=0.7)]
    sparse = [make_doc("b", score=5.0), make_doc("d", score=4.0), make_doc("a", score=3.0)]

    result = retriever._rrf_fusion(dense, sparse, k=4)
    assert len(result) <= 4
    doc_ids = [r.doc_id for r in result]
    # "a" and "b" appear in both — should rank high
    assert "a" in doc_ids
    assert "b" in doc_ids


def test_rrf_fusion_respects_weights():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 1.0
    retriever.sparse_weight = 0.0
    retriever.rrf_k = 60

    dense = [make_doc("x", score=0.95), make_doc("y", score=0.5)]
    sparse = [make_doc("z", score=9.0), make_doc("w", score=8.0)]

    result = retriever._rrf_fusion(dense, sparse, k=4)
    # With dense_weight=1.0, dense results should dominate
    assert result[0].doc_id == "x"


def test_rrf_fusion_empty_inputs():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    result = retriever._rrf_fusion([], [], k=5)
    assert result == []


def test_rrf_fusion_single_source():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc(f"doc{i}", score=1.0 - i * 0.1) for i in range(5)]
    result = retriever._rrf_fusion(dense, [], k=3)
    assert len(result) == 3


def test_rrf_scores_decrease_with_rank():
    retriever = HybridRetriever.__new__(HybridRetriever)
    retriever.dense_weight = 0.6
    retriever.sparse_weight = 0.4
    retriever.rrf_k = 60

    dense = [make_doc(f"d{i}") for i in range(5)]
    sparse = [make_doc(f"s{i}") for i in range(5)]
    result = retriever._rrf_fusion(dense, sparse, k=10)

    scores = [r.score for r in result]
    assert scores == sorted(scores, reverse=True)


def test_matches_filter():
    assert HybridRetriever._matches_filter(
        {"company_id": "nvda", "dimension": "talent"},
        {"company_id": "nvda"},
    )
    assert not HybridRetriever._matches_filter(
        {"company_id": "aapl"},
        {"company_id": "nvda"},
    )


def test_build_where_single():
    where = HybridRetriever._build_where({"company_id": "nvda"})
    assert where == {"company_id": {"$eq": "nvda"}}


def test_build_where_multiple():
    where = HybridRetriever._build_where({"company_id": "nvda", "dimension": "talent"})
    assert "$and" in where
    assert len(where["$and"]) == 2


# ---------------------------------------------------------------------------
# _flatten_filter
# ---------------------------------------------------------------------------

def test_flatten_filter_simple_dict():
    flat = HybridRetriever._flatten_filter({"ticker": "nvda"})
    assert flat == {"ticker": "nvda"}


def test_flatten_filter_nested_and():
    filt = {"$and": [{"ticker": "nvda"}, {"dimension": "talent"}]}
    flat = HybridRetriever._flatten_filter(filt)
    assert flat == {"ticker": "nvda", "dimension": "talent"}


def test_flatten_filter_with_in_list():
    filt = {"$and": [{"source_type": {"$in": ["sec_10k_item_1", "job_posting_indeed"]}}]}
    flat = HybridRetriever._flatten_filter(filt)
    assert flat["source_type"] == ["sec_10k_item_1", "job_posting_indeed"]


def test_flatten_filter_none_returns_empty():
    assert HybridRetriever._flatten_filter(None) == {}


# ---------------------------------------------------------------------------
# seed_from_evidence
# ---------------------------------------------------------------------------

from types import SimpleNamespace
from unittest.mock import patch as _patch


def make_evidence(evidence_id: str, content: str = "tech AI data pipeline", company_id: str = "nvda"):
    return SimpleNamespace(
        evidence_id=evidence_id,
        content=content,
        signal_category="technology_hiring",
        company_id=company_id,
        source_type="job_posting_indeed",
        confidence=0.8,
    )


def make_empty_retriever():
    r = HybridRetriever.__new__(HybridRetriever)
    r.dense_weight = 0.6
    r.sparse_weight = 0.4
    r.rrf_k = 60
    r._bm25 = None
    r._doc_store = []
    r._tokenized_corpus = []
    r._seeded_tickers = set()
    r._vector_store = MagicMock()
    return r


def test_seed_from_evidence_adds_to_bm25():
    """seed_from_evidence with 3 evidence items → doc_store has 3 docs, BM25 built."""
    r = make_empty_retriever()
    evidence = [make_evidence(f"ev_{i:03d}") for i in range(3)]
    # DimensionMapper is pure Python (no network), let it run naturally
    r.seed_from_evidence(evidence)
    assert len(r._doc_store) == 3
    assert r._bm25 is not None


def test_seed_from_evidence_deduplicates():
    """Calling seed_from_evidence twice with same IDs → no duplicate docs."""
    r = make_empty_retriever()
    evidence = [make_evidence("ev_001"), make_evidence("ev_002")]
    r.seed_from_evidence(evidence)
    r.seed_from_evidence(evidence)  # second call with same IDs
    doc_ids = [d.doc_id for d in r._doc_store]
    assert len(doc_ids) == len(set(doc_ids))  # no duplicates
    assert len(r._doc_store) == 2


def test_seed_from_evidence_marks_tickers_seeded():
    r = make_empty_retriever()
    evidence = [make_evidence("ev_001", company_id="nvda")]
    r.seed_from_evidence(evidence)
    assert "nvda" in r._seeded_tickers


def test_seed_ticker_is_noop():
    """_seed_ticker only adds to _seeded_tickers, never calls VectorStore."""
    r = make_empty_retriever()
    r._seed_ticker("nvda")
    assert "nvda" in r._seeded_tickers
    r._vector_store.search.assert_not_called()
