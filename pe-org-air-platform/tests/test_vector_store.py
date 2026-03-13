"""Unit tests for VectorStore — all I/O mocked, no real ChromaDB/network."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.services.search.vector_store import VectorStore, MULTI_DIM_MIN_WEIGHT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_vs(use_cloud: bool = False, collection_id: str | None = None) -> VectorStore:
    """Create a VectorStore instance without running __init__ (no network)."""
    store = VectorStore.__new__(VectorStore)
    store._use_cloud = use_cloud
    store._collection_id = collection_id
    store._local_collection = None
    store._encoder = None
    store._api_key = "test-api-key" if use_cloud else ""
    store._tenant = "test-tenant" if use_cloud else ""
    store._database = "test-db"
    return store


def make_ev(
    content: str = "some evidence content",
    signal_category: str = "technology_hiring",
    evidence_id: str = "ev_001",
    company_id: str = "nvda",
    source_type: str = "job_posting_indeed",
    confidence: float = 0.8,
    fiscal_year: str = "2024",
    source_url: str = "https://example.com",
    page_number: int = 1,
) -> SimpleNamespace:
    return SimpleNamespace(
        content=content,
        signal_category=signal_category,
        evidence_id=evidence_id,
        company_id=company_id,
        source_type=source_type,
        confidence=confidence,
        fiscal_year=fiscal_year,
        source_url=source_url,
        page_number=page_number,
    )


def make_mapper(weights: dict, primary: str = "talent") -> MagicMock:
    m = MagicMock()
    m.get_dimension_weights.return_value = weights
    m.get_primary_dimension.return_value = primary
    return m


# ---------------------------------------------------------------------------
# Encode fallback
# ---------------------------------------------------------------------------

def test_encode_fallback_returns_zero_vectors():
    store = make_vs()
    result = store._encode(["foo", "bar"])
    assert result == [[0.0] * 384, [0.0] * 384]


# ---------------------------------------------------------------------------
# index_cs2_evidence — no-backend path
# ---------------------------------------------------------------------------

def test_index_cs2_evidence_empty_list():
    store = make_vs()
    mapper = make_mapper({"talent": 1.0})
    assert store.index_cs2_evidence([], mapper) == 0


def test_index_cs2_evidence_skips_empty_content():
    store = make_vs()
    mapper = make_mapper({"talent": 1.0})
    ev = make_ev(content="")
    assert store.index_cs2_evidence([ev], mapper) == 0


# ---------------------------------------------------------------------------
# index_cs2_evidence — deduplication
# ---------------------------------------------------------------------------

def test_index_cs2_evidence_deduplicates_content():
    """Two evidences with same content produce only 1 indexed entry."""
    store = make_vs(use_cloud=True, collection_id="col-id")
    mapper = make_mapper({"ai_governance": 1.0}, primary="ai_governance")

    ev1 = make_ev(content="Identical content here", evidence_id="ev_001")
    ev2 = make_ev(content="Identical content here", evidence_id="ev_002")

    captured_ids: list[str] = []

    def fake_upsert(ids, documents, embeddings, metadatas):
        captured_ids.extend(ids)
        return True

    store._cloud_upsert = fake_upsert
    count = store.index_cs2_evidence([ev1, ev2], mapper)
    assert count == 1
    assert len(captured_ids) == 1


# ---------------------------------------------------------------------------
# index_cs2_evidence — multi-dimension expansion
# ---------------------------------------------------------------------------

def test_index_cs2_evidence_multi_dimension_expansion():
    """technology_hiring: talent=0.70, tech_stack=0.20, culture=0.10.
    Only talent and technology_stack are >= MULTI_DIM_MIN_WEIGHT (0.15).
    Expect 2 vectors with __talent and __technology_stack suffixes.
    """
    store = make_vs(use_cloud=True, collection_id="col-id")
    mapper = make_mapper(
        {"talent": 0.70, "technology_stack": 0.20, "culture": 0.10},
        primary="talent",
    )
    ev = make_ev(evidence_id="ev_001")

    captured_ids: list[str] = []

    def fake_upsert(ids, documents, embeddings, metadatas):
        captured_ids.extend(ids)
        return True

    store._cloud_upsert = fake_upsert
    count = store.index_cs2_evidence([ev], mapper)

    assert count == 2
    assert "ev_001__talent" in captured_ids
    assert "ev_001__technology_stack" in captured_ids
    assert all("culture" not in doc_id for doc_id in captured_ids)


def test_index_cs2_evidence_single_dim_no_suffix():
    """Single relevant dimension → ID gets no __ suffix."""
    store = make_vs(use_cloud=True, collection_id="col-id")
    # Only talent is above the threshold
    mapper = make_mapper(
        {"talent": 0.90, "technology_stack": 0.05},
        primary="talent",
    )
    ev = make_ev(evidence_id="ev_solo")

    captured_ids: list[str] = []

    def fake_upsert(ids, documents, embeddings, metadatas):
        captured_ids.extend(ids)
        return True

    store._cloud_upsert = fake_upsert
    count = store.index_cs2_evidence([ev], mapper)

    assert count == 1
    # No suffix when only one relevant dimension
    assert "ev_solo" in captured_ids
    assert "ev_solo__talent" not in captured_ids


# ---------------------------------------------------------------------------
# search / count / delete — no backend
# ---------------------------------------------------------------------------

def test_search_returns_empty_when_no_backend():
    store = make_vs()
    results = store.search("AI infrastructure", top_k=5)
    assert results == []


def test_count_returns_zero_when_no_backend():
    store = make_vs()
    assert store.count() == 0


def test_delete_by_filter_returns_zero_when_no_backend():
    store = make_vs()
    assert store.delete_by_filter({"ticker": {"$eq": "nvda"}}) == 0


# ---------------------------------------------------------------------------
# Header / URL helpers
# ---------------------------------------------------------------------------

def test_cloud_headers_format():
    store = make_vs(use_cloud=True)
    headers = store._headers()
    assert headers["x-chroma-token"] == "test-api-key"
    assert headers["Content-Type"] == "application/json"


def test_base_url_format():
    store = make_vs(use_cloud=True)
    url = store._base_url()
    assert "test-tenant" in url
    assert "test-db" in url


# ---------------------------------------------------------------------------
# Cloud upsert / count
# ---------------------------------------------------------------------------

def test_cloud_upsert_success():
    store = make_vs(use_cloud=True, collection_id="col-id")
    with patch("app.services.search.vector_store.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        result = store._cloud_upsert(["id1"], ["doc1"], [[0.0] * 384], [{}])
    assert result is True


def test_cloud_upsert_failure():
    store = make_vs(use_cloud=True, collection_id="col-id")
    with patch("app.services.search.vector_store.requests.post") as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.text = "Server Error"
        result = store._cloud_upsert(["id1"], ["doc1"], [[0.0] * 384], [{}])
    assert result is False


def test_cloud_count_success():
    store = make_vs(use_cloud=True, collection_id="col-id")
    with patch("app.services.search.vector_store.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = 42
        result = store._cloud_count()
    assert result == 42


# ---------------------------------------------------------------------------
# Cloud query — no where key
# ---------------------------------------------------------------------------

def test_cloud_query_passes_no_where():
    """_cloud_query must NOT include 'where' in the body (Chroma Cloud workaround)."""
    store = make_vs(use_cloud=True, collection_id="col-id")
    with patch("app.services.search.vector_store.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {}
        store._cloud_query([0.0] * 384, 5)
        body = mock_post.call_args[1]["json"]
        assert "where" not in body


# ---------------------------------------------------------------------------
# Client-side filtering in search()
# ---------------------------------------------------------------------------

def _make_cloud_query_result(tickers: list[str], dimension: str = "talent") -> dict:
    ids = [f"doc_{i}" for i in range(len(tickers))]
    docs = [f"content {i}" for i in range(len(tickers))]
    metas = [{"ticker": t, "dimension": dimension, "source_type": "sec", "confidence": 0.8}
             for t in tickers]
    dists = [0.1 * (i + 1) for i in range(len(tickers))]
    return {
        "ids": [ids],
        "documents": [docs],
        "metadatas": [metas],
        "distances": [dists],
    }


def test_search_client_side_filter_ticker():
    store = make_vs(use_cloud=True, collection_id="col-id")
    cloud_data = _make_cloud_query_result(["nvda", "aapl", "nvda"])

    with patch.object(store, "_cloud_count", return_value=5), \
         patch.object(store, "_cloud_query", return_value=cloud_data):
        results = store.search("AI talent", top_k=10, ticker="nvda")

    assert len(results) == 2
    assert all(r.metadata["ticker"] == "nvda" for r in results)


def test_search_client_side_filter_dimension():
    store = make_vs(use_cloud=True, collection_id="col-id")
    cloud_data = _make_cloud_query_result(["nvda"] * 3, dimension="talent")
    # Override middle doc's dimension
    cloud_data["metadatas"][0][1]["dimension"] = "leadership"

    with patch.object(store, "_cloud_count", return_value=5), \
         patch.object(store, "_cloud_query", return_value=cloud_data):
        results = store.search("AI talent", top_k=10, dimension="talent")

    assert all(r.metadata["dimension"] == "talent" for r in results)
    assert len(results) == 2


def test_search_respects_top_k():
    """Even if cloud returns many docs, output is capped at top_k."""
    store = make_vs(use_cloud=True, collection_id="col-id")
    tickers = ["nvda"] * 20
    cloud_data = _make_cloud_query_result(tickers)

    with patch.object(store, "_cloud_count", return_value=20), \
         patch.object(store, "_cloud_query", return_value=cloud_data):
        results = store.search("AI", top_k=5)

    assert len(results) == 5
