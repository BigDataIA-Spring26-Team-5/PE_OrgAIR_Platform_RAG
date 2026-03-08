"""Tests for CS2Client — S3-based evidence fetching."""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_s3(files: dict):
    """Return a mock S3 service where list_files and get_file use `files` dict."""
    s3 = MagicMock()
    s3.list_files.side_effect = lambda prefix: [k for k in files if k.startswith(prefix)]
    s3.get_file.side_effect = lambda key: files.get(key)
    return s3


def _client(s3_mock):
    with patch("app.services.s3_storage.get_s3_service", return_value=s3_mock):
        from app.services.integration.cs2_client import CS2Client
        return CS2Client()


# ---------------------------------------------------------------------------
# Job postings
# ---------------------------------------------------------------------------

def test_fetch_jobs_returns_evidence():
    jobs_json = json.dumps({"job_postings": [
        {"job_id": "j1", "title": "AI Engineer", "description": "Build ML models", "source": "linkedin"},
        {"job_id": "j2", "title": "Data Scientist", "description": "Analyze data", "source": "indeed"},
    ]}).encode()

    s3 = _make_s3({"signals/jobs/NVDA/2024-01.json": jobs_json})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["technology_hiring"])

    assert len(ev) == 2
    assert ev[0].source_type == "job_posting_linkedin"
    assert ev[1].source_type == "job_posting_indeed"
    assert "AI Engineer" in ev[0].content
    assert ev[0].signal_category == "technology_hiring"


def test_fetch_jobs_skips_empty_files():
    empty = json.dumps({"job_postings": []}).encode()
    real = json.dumps({"job_postings": [
        {"job_id": "j1", "title": "ML Ops", "description": "Infra", "source": "linkedin"},
    ]}).encode()
    # sorted desc: b comes before a
    s3 = _make_s3({
        "signals/jobs/CAT/2024-02.json": empty,
        "signals/jobs/CAT/2024-01.json": real,
    })
    client = _client(s3)
    ev = client.get_evidence(ticker="CAT", signal_categories=["technology_hiring"])
    assert len(ev) == 1
    assert "ML Ops" in ev[0].content


# ---------------------------------------------------------------------------
# Patents
# ---------------------------------------------------------------------------

def test_fetch_patents_returns_evidence():
    data = json.dumps({"patents": [
        {"patent_id": "p1", "title": "Neural net chip", "abstract": "A chip for AI inference."},
    ]}).encode()
    s3 = _make_s3({"signals/patents/NVDA/2024.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["innovation_activity"])

    assert len(ev) == 1
    assert ev[0].source_type == "patent_uspto"
    assert "Neural net chip" in ev[0].content
    assert "A chip for AI inference" in ev[0].content


# ---------------------------------------------------------------------------
# Tech stack
# ---------------------------------------------------------------------------

def test_fetch_techstack_builds_content():
    data = json.dumps({
        "ai_technologies_detected": ["TensorFlow", "PyTorch"],
        "wappalyzer_techs": ["React", "Kubernetes"],
    }).encode()
    s3 = _make_s3({"signals/techstack/NVDA/2024.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["digital_presence"])

    assert len(ev) == 1
    assert "TensorFlow" in ev[0].content
    assert "React" in ev[0].content
    assert ev[0].signal_category == "digital_presence"


def test_fetch_techstack_empty_returns_nothing():
    data = json.dumps({}).encode()
    s3 = _make_s3({"signals/techstack/NVDA/2024.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["digital_presence"])
    assert ev == []


# ---------------------------------------------------------------------------
# Glassdoor
# ---------------------------------------------------------------------------

def test_fetch_glassdoor_returns_reviews():
    data = json.dumps({"reviews": [
        {"review_id": "r1", "title": "Great culture", "pros": "Flexible hours", "cons": "Busy"},
        {"review_id": "r2", "title": "Fast paced", "pros": "Innovative", "cons": "Long hours"},
    ]}).encode()
    s3 = _make_s3({"glassdoor_signals/raw/NVDA_raw.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["culture_signals"])

    assert len(ev) == 2
    assert ev[0].source_type == "glassdoor_review"
    assert "Great culture" in ev[0].content
    assert "Flexible hours" in ev[0].content


def test_fetch_glassdoor_missing_file_returns_empty():
    s3 = _make_s3({})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["culture_signals"])
    assert ev == []


# ---------------------------------------------------------------------------
# SEC chunks
# ---------------------------------------------------------------------------

def test_fetch_sec_chunks_10k():
    data = json.dumps({"chunks": [
        {"chunk_id": "c1", "text": "NVIDIA operates data centers globally.", "section": "item_1"},
        {"chunk_id": "c2", "text": "Risk factors include supply chain.", "section": "item_1a"},
    ]}).encode()
    s3 = _make_s3({"sec/chunks/NVDA/10-K/2024.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["sec_chunks"])

    assert len(ev) == 2
    assert ev[0].source_type == "sec_10k_item_1"
    assert ev[1].source_type == "sec_10k_item_1a"
    assert ev[0].confidence == 0.9


def test_fetch_sec_chunks_def14a():
    data = json.dumps({"chunks": [
        {"chunk_id": "g1", "text": "Board consists of 9 members.", "section": "governance"},
    ]}).encode()
    s3 = _make_s3({"sec/chunks/NVDA/DEF14A/2024.json": data})
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA", signal_categories=["sec_chunks"])

    assert len(ev) == 1
    assert ev[0].source_type == "board_proxy_def14a"
    assert ev[0].signal_category == "governance_signals"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def test_get_evidence_all_categories():
    files = {
        "signals/jobs/NVDA/2024.json": json.dumps({"job_postings": [
            {"job_id": "j1", "title": "SWE", "description": "Build things", "source": "linkedin"},
        ]}).encode(),
        "signals/patents/NVDA/2024.json": json.dumps({"patents": [
            {"patent_id": "p1", "title": "GPU", "abstract": "Compute"},
        ]}).encode(),
        "signals/techstack/NVDA/2024.json": json.dumps({"ai_technologies_detected": ["CUDA"]}).encode(),
        "glassdoor_signals/raw/NVDA_raw.json": json.dumps({"reviews": [
            {"review_id": "r1", "title": "Good", "pros": "Pay", "cons": "Hours"},
        ]}).encode(),
        "sec/chunks/NVDA/10-K/2024.json": json.dumps({"chunks": [
            {"chunk_id": "c1", "text": "Revenue grew 122%.", "section": "item_7"},
        ]}).encode(),
    }
    s3 = _make_s3(files)
    client = _client(s3)
    ev = client.get_evidence(ticker="NVDA")

    assert len(ev) == 5
    cats = {e.signal_category for e in ev}
    assert "technology_hiring" in cats
    assert "innovation_activity" in cats
    assert "culture_signals" in cats


def test_get_evidence_min_confidence_filter():
    data = json.dumps({"job_postings": [
        {"job_id": "j1", "title": "AI Eng", "description": "ML", "source": "indeed"},
    ]}).encode()
    s3 = _make_s3({"signals/jobs/NVDA/2024.json": data})
    client = _client(s3)

    # Jobs have confidence=0.7; filter above that → empty
    ev = client.get_evidence(ticker="NVDA", signal_categories=["technology_hiring"], min_confidence=0.8)
    assert ev == []

    # Filter at 0.7 → included
    ev2 = client.get_evidence(ticker="NVDA", signal_categories=["technology_hiring"], min_confidence=0.7)
    assert len(ev2) == 1


def test_s3_failure_does_not_crash():
    s3 = MagicMock()
    s3.list_files.side_effect = Exception("S3 unavailable")
    s3.get_file.side_effect = Exception("S3 unavailable")
    client = _client(s3)
    # Should return empty list, not raise
    ev = client.get_evidence(ticker="NVDA")
    assert ev == []


# ---------------------------------------------------------------------------
# mark_indexed
# ---------------------------------------------------------------------------

def test_mark_indexed_returns_count():
    s3 = _make_s3({})
    client = _client(s3)
    assert client.mark_indexed(["ev1", "ev2", "ev3"]) == 3


def test_mark_indexed_empty():
    s3 = _make_s3({})
    client = _client(s3)
    assert client.mark_indexed([]) == 0
