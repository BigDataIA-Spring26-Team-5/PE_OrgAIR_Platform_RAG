"""Tests for CS1Client — company metadata fetching."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.services.integration.cs1_client import CS1Client, Company


@pytest.fixture
def mock_response_200():
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def sample_company_data():
    return {
        "id": "abc123",
        "ticker": "NVDA",
        "name": "NVIDIA Corporation",
        "sector": "Semiconductors",
        "sub_sector": "AI/GPU",
        "market_cap_percentile": 98.5,
        "revenue_millions": 60922.0,
        "employee_count": 29600,
        "fiscal_year_end": "2025-01-31",
    }


def test_get_company_parses_response(mock_response_200, sample_company_data):
    mock_response_200.json.return_value = sample_company_data
    with patch("httpx.Client.get", return_value=mock_response_200):
        client = CS1Client()
        company = client.get_company("NVDA")

    assert company is not None
    assert company.ticker == "NVDA"
    assert company.name == "NVIDIA Corporation"
    assert company.sector == "Semiconductors"
    assert company.revenue_millions == 60922.0
    assert company.employee_count == 29600
    assert company.company_id == "abc123"


def test_get_company_returns_none_on_404():
    resp = MagicMock()
    resp.status_code = 404
    with patch("httpx.Client.get", return_value=resp):
        client = CS1Client()
        result = client.get_company("UNKNOWN")
    assert result is None


def test_list_companies_parses_list(mock_response_200, sample_company_data):
    mock_response_200.json.return_value = [sample_company_data]
    with patch("httpx.Client.get", return_value=mock_response_200):
        client = CS1Client()
        companies = client.list_companies()

    assert len(companies) == 1
    assert isinstance(companies[0], Company)
    assert companies[0].ticker == "NVDA"


def test_list_companies_parses_dict_wrapper(mock_response_200, sample_company_data):
    mock_response_200.json.return_value = {"companies": [sample_company_data]}
    with patch("httpx.Client.get", return_value=mock_response_200):
        client = CS1Client()
        companies = client.list_companies()

    assert len(companies) == 1


def test_parse_company_with_missing_fields():
    data = {"ticker": "TEST", "name": "Test Co"}
    company = CS1Client._parse_company(data)
    assert company.ticker == "TEST"
    assert company.revenue_millions == 0.0
    assert company.employee_count == 0
    assert company.sector == ""


def test_context_manager():
    with CS1Client() as client:
        assert client is not None
