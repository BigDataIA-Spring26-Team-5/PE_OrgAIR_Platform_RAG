"""
data_loader.py — Centralized data fetching for Streamlit dashboard.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# API_BASE = os.getenv("FASTAPI_URL", "http://localhost:8000")
# API_BASE = os.getenv("FASTAPI_URL", "https://pe-orgair-platform-scoring-engine.onrender.com")
API_BASE = st.secrets.get("FASTAPI_URL", os.getenv("FASTAPI_URL", "http://localhost:8000"))
RESULTS_DIR = Path(__file__).parent.parent / "results"

CS3_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]
COMPANY_NAMES = {
    "NVDA": "NVIDIA Corporation", "JPM": "JPMorgan Chase",
    "WMT": "Walmart Inc.", "GE": "GE Aerospace", "DG": "Dollar General",
}
SECTORS = {
    "NVDA": "Technology", "JPM": "Financial Services",
    "WMT": "Retail", "GE": "Manufacturing", "DG": "Retail",
}
EXPECTED_RANGES = {
    "NVDA": (85, 95), "JPM": (65, 75), "WMT": (55, 65),
    "GE": (45, 55), "DG": (35, 45),
}
DIMENSION_LABELS = {
    "data_infrastructure": "Data Infrastructure",
    "ai_governance": "AI Governance",
    "technology_stack": "Technology Stack",
    "talent_skills": "Talent & Skills",
    "leadership_vision": "Leadership Vision",
    "use_case_portfolio": "Use Case Portfolio",
    "culture_change": "Culture & Change",
}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def _api(path: str, timeout: int = 30) -> Optional[Dict]:
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


@st.cache_data(ttl=300)
def get_table_counts() -> Dict[str, int]:
    data = _api("/health/table-counts")
    if data is None:
        tables = [
            "INDUSTRIES", "COMPANIES", "ASSESSMENTS", "DIMENSION_SCORES",
            "DOCUMENTS", "DOCUMENT_CHUNKS", "EXTERNAL_SIGNALS",
            "COMPANY_SIGNAL_SUMMARIES", "SIGNAL_SCORES",
            "SIGNAL_DIMENSION_MAPPING", "EVIDENCE_DIMENSION_SCORES",
        ]
        return {t: -1 for t in tables}
    return data


# ---------------------------------------------------------------------------
# Results JSON
# ---------------------------------------------------------------------------
@st.cache_data(ttl=120)
def load_result(ticker: str) -> Optional[Dict]:
    path = RESULTS_DIR / f"{ticker.lower()}.json"
    if path.exists():
        return json.loads(path.read_text())
    return None


@st.cache_data(ttl=120)
def load_all_results() -> Dict[str, Dict]:
    results = {}
    for t in CS3_TICKERS:
        r = load_result(t)
        if r:
            results[t] = r
    return results


# ---------------------------------------------------------------------------
# DataFrame builders
# ---------------------------------------------------------------------------
def build_portfolio_df() -> pd.DataFrame:
    results = load_all_results()
    if not results:
        return pd.DataFrame()
    rows = []
    for ticker in CS3_TICKERS:
        r = results.get(ticker)
        if not r:
            continue
        exp = EXPECTED_RANGES.get(ticker, (0, 100))
        score = r.get("org_air_score", 0)
        rows.append({
            "Ticker": ticker,
            "Company": COMPANY_NAMES.get(ticker, ticker),
            "Sector": SECTORS.get(ticker, ""),
            "Org-AI-R": score,
            "V^R": r.get("vr_score", 0),
            "H^R": r.get("hr_score", 0),
            "Synergy": r.get("synergy_score", 0),
            "TC": r.get("talent_concentration", 0),
            "PF": r.get("position_factor", 0),
            "Expected Low": exp[0],
            "Expected High": exp[1],
            "In Range": "✅" if exp[0] <= score <= exp[1] else "⚠️",
        })
    return pd.DataFrame(rows)


def build_dimensions_df() -> pd.DataFrame:
    results = load_all_results()
    if not results:
        return pd.DataFrame()
    rows = []
    for ticker in CS3_TICKERS:
        r = results.get(ticker)
        if not r or not r.get("dimension_scores"):
            continue
        dims = r["dimension_scores"]
        if not isinstance(dims, dict):
            continue
        row = {"Ticker": ticker, "Company": COMPANY_NAMES.get(ticker, ticker)}
        for dim_key, label in DIMENSION_LABELS.items():
            row[label] = dims.get(dim_key, 0)
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Signal summaries
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_signal_summaries() -> pd.DataFrame:
    rows = []
    for ticker in CS3_TICKERS:
        data = _api(f"/api/v1/signals/{ticker}/current-scores")
        if not data:
            continue
        rows.append({
            "TICKER": ticker,
            "TECHNOLOGY_HIRING_SCORE":  (data.get("technology_hiring")  or {}).get("score") or 0,
            "INNOVATION_ACTIVITY_SCORE":(data.get("innovation_activity") or {}).get("score") or 0,
            "DIGITAL_PRESENCE_SCORE":   (data.get("digital_presence")   or {}).get("score") or 0,
            "LEADERSHIP_SIGNALS_SCORE": (data.get("leadership_signals") or {}).get("score") or 0,
            "COMPOSITE_SCORE": data.get("composite_score") or 0,
            "SIGNAL_COUNT": 0,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Document stats
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def get_document_stats() -> pd.DataFrame:
    data = _api("/api/v1/documents/report")
    if not data:
        return pd.DataFrame()
    filing_map = [
        ("10-K",    "form_10k"),
        ("10-Q",    "form_10q"),
        ("8-K",     "form_8k"),
        ("DEF 14A", "def_14a"),
    ]
    rows = []
    for company in data.get("documents_by_company", []):
        ticker = (company.get("ticker") or "").upper()
        if ticker not in CS3_TICKERS:
            continue
        for filing_type, key in filing_map:
            count = company.get(key) or 0
            if count > 0:
                rows.append({"TICKER": ticker, "FILING_TYPE": filing_type, "DOC_COUNT": count})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def check_health() -> Optional[Dict]:
    return _api("/health")


# ---------------------------------------------------------------------------
# TC breakdown helpers (result JSONs store tc_breakdown as raw strings)
# ---------------------------------------------------------------------------
def _parse_kv_string(s: str) -> dict:
    """Parse 'key=value key2=value2 ...' string, ignoring list-valued fields."""
    result = {}
    if not s or not isinstance(s, str):
        return result
    # stop before unique_skills=[...] list which breaks simple split
    clean = s.split("unique_skills=")[0].strip()
    for part in clean.split():
        if "=" in part:
            k, _, v = part.partition("=")
            try:
                result[k.strip()] = float(v.strip())
            except ValueError:
                pass
    return result


@st.cache_data(ttl=120)
def build_tc_breakdown_df() -> pd.DataFrame:
    """DataFrame of TC sub-components parsed from all 5 result JSONs."""
    results = load_all_results()
    rows = []
    for ticker in CS3_TICKERS:
        r = results.get(ticker)
        if not r:
            continue
        bd = _parse_kv_string(r.get("tc_breakdown", ""))
        rows.append({
            "Ticker": ticker,
            "leadership_ratio":    bd.get("leadership_ratio",    0.0),
            "team_size_factor":    bd.get("team_size_factor",    0.0),
            "skill_concentration": bd.get("skill_concentration", 0.0),
            "individual_factor":   bd.get("individual_factor",   0.0),
        })
    return pd.DataFrame(rows)