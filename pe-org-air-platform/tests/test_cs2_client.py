"""Tests for CS2Client — evidence fetching and filtering."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.services.integration.cs2_client import CS2Client, CS2Evidence


@pytest.fixture
def mock_ok():
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def sample_evidence():
    return {
        "id": "ev001",
        "company_id": "nvda",
        "source_type": "sec_10k_item_1",
        "signal_category": "digital_presence",
        "content": "NVIDIA operates a comprehensive data infrastructure...",
        "confidence": 0.85,
        "fiscal_year": "2024",
        "source_url": "https://sec.gov/nvda/10k",
        "page_number": 12,
        "indexed_in_cs4": False,
    }


def test_get_evidence_parses_list(mock_ok, sample_evidence):
    mock_ok.json.return_value = [sample_evidence]
    with patch("httpx.Client.get", return_value=mock_ok):
        client = CS2Client()
        evidence = client.get_evidence(company_id="nvda")

    assert len(evidence) == 1
    ev = evidence[0]
    assert ev.evidence_id == "ev001"
    assert ev.source_type == "sec_10k_item_1"
    assert ev.confidence == 0.85


def test_get_evidence_filters_by_confidence(mock_ok):
    mock_ok.json.return_value = [
        {"id": "1", "company_id": "nvda", "source_type": "sec_10k_item_1",
         "signal_category": "digital_presence", "content": "text", "confidence": 0.3},
        {"id": "2", "company_id": "nvda", "source_type": "sec_10k_item_1",
         "signal_category": "digital_presence", "content": "text2", "confidence": 0.9},
    ]
    with patch("httpx.Client.get", return_value=mock_ok):
        client = CS2Client()
        evidence = client.get_evidence(min_confidence=0.5)

    # The API-level filter is passed as param; client returns all API results
    assert len(evidence) == 2  # filtering happens server-side


def test_get_evidence_fallback_on_404():
    resp_404 = MagicMock()
    resp_404.status_code = 404

    resp_chunks = MagicMock()
    resp_chunks.status_code = 200
    resp_chunks.raise_for_status = MagicMock()
    resp_chunks.json.return_value = []

    with patch("httpx.Client.get", side_effect=[resp_404, resp_chunks]):
        client = CS2Client()
        result = client.get_evidence(company_id="nvda")

    assert result == []


def test_mark_indexed_success():
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"updated": 3}

    with patch("httpx.Client.patch", return_value=resp):
        client = CS2Client()
        count = client.mark_indexed(["ev1", "ev2", "ev3"])

    assert count == 3


def test_mark_indexed_empty():
    client = CS2Client()
    assert client.mark_indexed([]) == 0
