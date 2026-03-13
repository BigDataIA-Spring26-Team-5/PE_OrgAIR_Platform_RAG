"""RAG FastAPI endpoint tests — all services mocked, no network/DB required."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import app.routers.rag as rag_module
from app.main import app
from app.services.retrieval.hybrid import RetrievedDocument


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_retrieved_doc(
    doc_id: str = "ev001",
    content: str = "AI infrastructure content",
    score: float = 0.8,
    source_type: str = "sec_10k_item_1",
    ticker: str = "nvda",
    dimension: str = "talent",
) -> RetrievedDocument:
    return RetrievedDocument(
        doc_id=doc_id,
        content=content,
        metadata={"ticker": ticker, "dimension": dimension, "source_type": source_type},
        score=score,
        retrieval_method="hybrid",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_vs():
    vs = MagicMock()
    vs.count.return_value = 42
    vs.index_cs2_evidence.return_value = 6
    vs.delete_by_filter.return_value = 0
    return vs


@pytest.fixture
def mock_retriever():
    r = MagicMock()
    r.retrieve.return_value = [
        make_retrieved_doc("ev001", score=0.9),
        make_retrieved_doc("ev002", score=0.8),
    ]
    r.refresh_sparse_index.return_value = None
    r.seed_from_evidence.return_value = None
    return r


@pytest.fixture
def mock_router_llm():
    r = MagicMock()
    r.complete.return_value = "Mock LLM answer."
    return r


@pytest.fixture
def mock_mapper():
    m = MagicMock()
    m.get_dimension_weights.return_value = {"talent": 1.0}
    m.get_primary_dimension.return_value = "talent"
    return m


@pytest.fixture
def rag_client(mock_vs, mock_retriever, mock_router_llm, mock_mapper):
    """TestClient with mocked RAG module globals (no real services).

    Uses TestClient without context manager to skip startup/shutdown events
    (the startup event tries to install signal handlers which fail in threads).
    """
    # Pre-populate module globals to prevent lazy init of real services
    rag_module._vector_store = mock_vs
    rag_module._retriever = mock_retriever
    rag_module._router_llm = mock_router_llm
    rag_module._mapper = mock_mapper

    # raise_server_exceptions=False prevents test failures on startup errors;
    # we skip the context manager to avoid firing the signal-handler startup event.
    c = TestClient(app, raise_server_exceptions=True)
    yield c

    # Reset so other test modules get a clean state
    rag_module._vector_store = None
    rag_module._retriever = None
    rag_module._router_llm = None
    rag_module._mapper = None


# ---------------------------------------------------------------------------
# GET /rag/status
# ---------------------------------------------------------------------------

def test_rag_status_returns_200(rag_client):
    response = rag_client.get("/rag/status")
    assert response.status_code == 200


def test_rag_status_response_shape(rag_client):
    response = rag_client.get("/rag/status")
    data = response.json()
    assert "status" in data
    assert "indexed_documents" in data
    assert data["status"] == "operational"


# ---------------------------------------------------------------------------
# POST /rag/search
# ---------------------------------------------------------------------------

def test_search_returns_results(rag_client):
    payload = {"query": "AI talent hiring", "ticker": "nvda", "top_k": 5}
    response = rag_client.post("/rag/search", json=payload)
    assert response.status_code == 200
    results = response.json()
    assert isinstance(results, list)


def test_search_missing_query_422(rag_client):
    """Omitting required 'query' field → 422 validation error."""
    response = rag_client.post("/rag/search", json={"ticker": "nvda"})
    assert response.status_code == 422


def test_search_respects_top_k(rag_client, mock_retriever):
    """top_k is forwarded to retriever as k; response contains exactly what retriever returns."""
    mock_retriever.retrieve.return_value = [make_retrieved_doc("ev0")]
    payload = {"query": "AI talent", "top_k": 1}
    response = rag_client.post("/rag/search", json=payload)
    assert response.status_code == 200
    # Retriever returned 1 item → response has 1 item
    assert len(response.json()) == 1
    # Verify retriever was called with k=1
    call_k = mock_retriever.retrieve.call_args[1].get("k") or mock_retriever.retrieve.call_args[0][1]
    assert call_k == 1


def test_search_response_schema(rag_client):
    """Each result must have doc_id, content, score, metadata, retrieval_method."""
    payload = {"query": "AI talent hiring", "ticker": "nvda", "top_k": 5}
    response = rag_client.post("/rag/search", json=payload)
    assert response.status_code == 200
    for item in response.json():
        assert "doc_id" in item
        assert "content" in item
        assert "score" in item
        assert "metadata" in item
        assert "retrieval_method" in item


# ---------------------------------------------------------------------------
# POST /rag/index/{ticker}
# ---------------------------------------------------------------------------

def make_cs2_evidence(n: int = 3):
    return [
        SimpleNamespace(
            evidence_id=f"ev_{i:03d}",
            content=f"Evidence content for document {i}",
            signal_category="technology_hiring",
            company_id="nvda",
            source_type="job_posting_indeed",
            confidence=0.8,
            fiscal_year="2024",
            source_url="https://example.com",
            page_number=i,
        )
        for i in range(n)
    ]


def test_index_ticker_returns_200(rag_client, mock_vs, mock_retriever):
    with patch("app.routers.rag.CS2Client") as MockCS2:
        mock_cs2 = MagicMock()
        MockCS2.return_value = mock_cs2
        mock_cs2.get_evidence.return_value = make_cs2_evidence(3)
        mock_vs.index_cs2_evidence.return_value = 6

        response = rag_client.post("/rag/index/nvda")

    assert response.status_code == 200


def test_index_ticker_response_contains_count(rag_client, mock_vs):
    with patch("app.routers.rag.CS2Client") as MockCS2:
        mock_cs2 = MagicMock()
        MockCS2.return_value = mock_cs2
        mock_cs2.get_evidence.return_value = make_cs2_evidence(3)
        mock_vs.index_cs2_evidence.return_value = 6

        response = rag_client.post("/rag/index/nvda")

    data = response.json()
    assert "indexed_count" in data
    assert data["ticker"] == "nvda"


# ---------------------------------------------------------------------------
# GET /rag/justify/{ticker}/{dimension}
# ---------------------------------------------------------------------------

def _make_mock_justification():
    from app.services.justification.generator import ScoreJustification
    return ScoreJustification(
        company_id="nvda",
        dimension="talent",
        score=75.0,
        level=4,
        level_name="Good",
        confidence_interval=(70.0, 80.0),
        rubric_criteria="Strong ML engineering team",
        rubric_keywords=["machine learning", "data scientist"],
        supporting_evidence=[],
        gaps_identified=["Missing deep learning specialization"],
        generated_summary="NVDA demonstrates strong talent at Level 4.",
        evidence_strength="strong",
    )


def test_justify_returns_200(rag_client):
    with patch("app.routers.rag.JustificationGenerator") as MockGen:
        mock_gen = MagicMock()
        MockGen.return_value = mock_gen
        mock_gen.generate_justification.return_value = _make_mock_justification()

        response = rag_client.get("/rag/justify/nvda/talent")

    assert response.status_code == 200


def test_justify_response_has_required_fields(rag_client):
    with patch("app.routers.rag.JustificationGenerator") as MockGen:
        mock_gen = MagicMock()
        MockGen.return_value = mock_gen
        mock_gen.generate_justification.return_value = _make_mock_justification()

        response = rag_client.get("/rag/justify/nvda/talent")

    data = response.json()
    assert "score" in data
    assert "level" in data
    assert "generated_summary" in data
    assert "supporting_evidence" in data


# ---------------------------------------------------------------------------
# Dimension detection helper — pure logic tests (no HTTP)
# ---------------------------------------------------------------------------

from app.routers.rag import _detect_dimension_scored


def test_detect_dimension_data_infrastructure_keywords():
    """Query with 'snowflake' (a discriminator) → data_infrastructure."""
    dim, confidence = _detect_dimension_scored("data warehouse snowflake pipeline")
    assert dim == "data_infrastructure"
    assert confidence > 0


def test_detect_dimension_talent_keywords():
    """'hiring' is in _TALENT_TRIGGERS → talent via priority 2."""
    dim, confidence = _detect_dimension_scored("machine learning engineer hiring")
    assert dim == "talent"
    assert confidence > 0


def test_detect_dimension_governance_keywords():
    """'AI ethics board policy' → ai_governance via keyword scoring."""
    dim, confidence = _detect_dimension_scored("AI ethics board policy governance")
    assert dim == "ai_governance"
    assert confidence > 0


def test_detect_dimension_low_confidence_fallback():
    """Generic query with no matching keywords → (None, 0.0)."""
    dim, confidence = _detect_dimension_scored("hello world completely unrelated text xyz")
    # No dimension keywords matched → confidence below any useful threshold
    assert confidence < 0.12 or dim is None
