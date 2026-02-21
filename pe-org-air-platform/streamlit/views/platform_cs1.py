"""Page: Platform Foundation (CS1). Infrastructure, schema, API layer."""

import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
from streamlit_mermaid import st_mermaid

from data_loader import API_BASE, check_health, get_table_counts


def render():
    st.title("🏗️ Platform Foundation (CS1)")
    st.caption("Building the shell: API layer, data models, and persistence that every other case study builds on")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1. Architecture Overview
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 1. Architecture Overview")
    st.markdown(
        "The platform is built on four connected services. The **FastAPI** backend is the single entry point. "
        "every scoring request flows through it. It reads/writes structured data to **Snowflake**, "
        "stores raw documents and timestamped result history in **AWS S3**, and uses **Redis** "
        "to cache repeated computations (like SEC filing parses) across scoring runs."
    )

    from pathlib import Path
    _arch_img = Path(__file__).parent.parent / "screenshots" / "cs1_architecture.png"
    if _arch_img.exists():
        st.image(str(_arch_img), caption="Platform Architecture. FastAPI → Snowflake / S3 / Redis")
    else:
        st.info(f"Architecture image not found at `screenshots/cs1_architecture.png`")

    st.markdown(
        "**Data flow:** Client triggers a scoring run → FastAPI orchestrates evidence collection (CS2) "
        "and scoring (CS3) → results are written to both Snowflake (latest state, queryable) "
        "and S3 (timestamped key, full history preserved)."
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2. Live System Health
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 2. Live System Health")
    st.markdown(
        "Real-time status of all four infrastructure dependencies. "
        "The `/health` endpoint checks each service and reports its status. "
        "All four must be ✅ for the scoring pipeline to run end-to-end."
    )

    if st.button("🔄 Refresh Status"):
        st.cache_data.clear()
        st.rerun()

    # API_BASE_URL = "http://localhost:8000"
    from data_loader import API_BASE
    API_BASE_URL = API_BASE
    api_healthy = False
    health_data = None
    try:
        resp = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if resp.status_code == 200:
            api_healthy = True
            health_data = resp.json()
    except Exception:
        pass

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("##### FastAPI")
        if api_healthy:
            st.success("✅ Running")
        else:
            st.error("❌ Not Running")
            st.caption("Run: `uvicorn app.main:app --reload`")

    with col2:
        st.markdown("##### Snowflake")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            sf_status = deps.get("snowflake", "unknown")
            if "healthy" in str(sf_status):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")

    with col3:
        st.markdown("##### Redis")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            redis_status = deps.get("redis", "unknown")
            if "healthy" in str(redis_status):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")

    with col4:
        st.markdown("##### AWS S3")
        if health_data and isinstance(health_data, dict):
            deps = health_data.get("dependencies", {})
            s3_status = deps.get("s3", "unknown")
            if "healthy" in str(s3_status):
                st.success("✅ Connected")
            else:
                st.error("❌ Not Connected")
        else:
            st.warning("⚠️ Check API first")

    if health_data:
        with st.expander("📋 View Full Health Response"):
            st.json(health_data)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3. Snowflake Data Schema
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 3. Snowflake Data Schema")
    st.markdown(
        "The Snowflake warehouse (`pe_orgair_db.platform`) holds **11 tables** organized into 3 layers. "
        "Each layer is built by a different case study. CS1 creates the entity framework, "
        "CS2 populates evidence, and CS3 stores scoring outputs."
    )

    counts = get_table_counts()

    # --- Schema table ---
    _schema_rows = [
        {"Layer": "CS1. Foundation", "Table": "COMPANIES",               "Rows": counts.get("COMPANIES", 0),               "Purpose": "Core entity. 5 CS3 + 10 CS2 companies"},
        {"Layer": "CS1. Foundation", "Table": "INDUSTRIES",              "Rows": counts.get("INDUSTRIES", 0),              "Purpose": "Sector reference data with H^R baselines"},
        {"Layer": "CS1. Foundation", "Table": "ASSESSMENTS",             "Rows": counts.get("ASSESSMENTS", 0),             "Purpose": "Assessment records with status tracking"},
        {"Layer": "CS1. Foundation", "Table": "DIMENSION_SCORES",        "Rows": counts.get("DIMENSION_SCORES", 0),        "Purpose": "7-dimension scores per assessment"},
        {"Layer": "CS2. Evidence",   "Table": "DOCUMENTS",               "Rows": counts.get("DOCUMENTS", 0),               "Purpose": "SEC filing metadata (10-K, DEF 14A)"},
        {"Layer": "CS2. Evidence",   "Table": "DOCUMENT_CHUNKS",         "Rows": counts.get("DOCUMENT_CHUNKS", 0),         "Purpose": "Semantic chunks with S3 references"},
        {"Layer": "CS2. Evidence",   "Table": "EXTERNAL_SIGNALS",        "Rows": counts.get("EXTERNAL_SIGNALS", 0),        "Purpose": "Individual signal observations"},
        {"Layer": "CS2. Evidence",   "Table": "COMPANY_SIGNAL_SUMMARIES","Rows": counts.get("COMPANY_SIGNAL_SUMMARIES", 0),"Purpose": "Aggregated signal scores per company"},
        {"Layer": "CS3. Scoring",    "Table": "SCORING",                 "Rows": counts.get("SCORING", 0),                 "Purpose": "Final Org-AI-R composite scores"},
        {"Layer": "CS3. Scoring",    "Table": "SIGNAL_DIMENSION_MAPPING","Rows": counts.get("SIGNAL_DIMENSION_MAPPING", 0),"Purpose": "9×7 weight matrix (Table 1)"},
        {"Layer": "CS3. Scoring",    "Table": "EVIDENCE_DIMENSION_SCORES","Rows": counts.get("EVIDENCE_DIMENSION_SCORES", 0),"Purpose": "7-dimension scores from evidence mapper"},
    ]
    _schema_df = pd.DataFrame(_schema_rows)
    st.dataframe(_schema_df, use_container_width=True, hide_index=True)

    # --- Layer metrics + bar chart side by side ---
    _cs1_rows = sum(counts.get(t, 0) for t in ["COMPANIES", "INDUSTRIES", "ASSESSMENTS", "DIMENSION_SCORES"])
    _cs2_rows = sum(counts.get(t, 0) for t in ["DOCUMENTS", "DOCUMENT_CHUNKS", "EXTERNAL_SIGNALS", "COMPANY_SIGNAL_SUMMARIES"])
    _cs3_rows = sum(counts.get(t, 0) for t in ["SCORING", "SIGNAL_DIMENSION_MAPPING", "EVIDENCE_DIMENSION_SCORES"])
    _total_rows = _cs1_rows + _cs2_rows + _cs3_rows

    col_m, col_chart = st.columns([1, 1])
    with col_m:
        st.metric("Total Rows Across All Tables", f"{_total_rows:,}")
        _k1, _k2, _k3 = st.columns(3)
        _k1.metric("CS1 Layer", f"{_cs1_rows:,}", help="COMPANIES, INDUSTRIES, ASSESSMENTS, DIMENSION_SCORES")
        _k2.metric("CS2 Layer", f"{_cs2_rows:,}", help="DOCUMENTS, DOCUMENT_CHUNKS, EXTERNAL_SIGNALS, COMPANY_SIGNAL_SUMMARIES")
        _k3.metric("CS3 Layer", f"{_cs3_rows:,}", help="SCORING, SIGNAL_DIMENSION_MAPPING, EVIDENCE_DIMENSION_SCORES")

    # with col_chart:
    #     _layer_fig = go.Figure()
    #     _layers = ["CS1. Foundation", "CS2. Evidence", "CS3. Scoring"]
    #     _layer_counts = [_cs1_rows, _cs2_rows, _cs3_rows]
    #     _layer_colors = ["#6366f1", "#0ea5e9", "#10b981"]
    #     _layer_fig.add_trace(go.Bar(
    #         x=_layers, y=_layer_counts,
    #         marker_color=_layer_colors,
    #         text=[f"{v:,}" for v in _layer_counts],
    #         textposition="outside", textfont=dict(size=13),
    #     ))
    #     _layer_fig.update_layout(
    #         title="Row Count by Data Layer",
    #         yaxis=dict(title="Rows"),
    #         height=300, margin=dict(t=50, b=40, l=50, r=30),
    #         showlegend=False, plot_bgcolor="white",
    #     )
    #     st.plotly_chart(_layer_fig, use_container_width=True, key="cs1_layer_bar")

    # --- Table relationships ---
    st.markdown(
        "**How the tables connect:** `COMPANIES` is the central entity. every other table references it. "
        "Documents are split into chunks (1:N). Signals are aggregated into summaries (N:1). "
        "Scoring results join back to companies and reference dimension scores."
    )
    st_mermaid("""
erDiagram
    COMPANIES ||--o{ DOCUMENTS : "has filings"
    COMPANIES ||--o{ EXTERNAL_SIGNALS : "has signals"
    COMPANIES ||--o{ ASSESSMENTS : "has assessments"
    COMPANIES ||--|| COMPANY_SIGNAL_SUMMARIES : "summarized as"
    COMPANIES ||--o{ SCORING : "scored as"
    DOCUMENTS ||--o{ DOCUMENT_CHUNKS : "split into"
    ASSESSMENTS ||--o{ DIMENSION_SCORES : "has 7 dimensions"
    SCORING ||--o{ EVIDENCE_DIMENSION_SCORES : "has 7 dimensions"
    SIGNAL_DIMENSION_MAPPING }o--|| EVIDENCE_DIMENSION_SCORES : "weights feed"
""")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4. API Layer
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 4. API Layer Design")
    st.markdown(
        "The FastAPI application exposes a structured REST API organized by case study scope. "
        "All endpoints are versioned under `/api/v1/`. Scoring endpoints accept a ticker and "
        "return a complete JSON result that is simultaneously written to Snowflake (MERGE upsert) "
        "and S3 (timestamped key. preserving full history)."
    )

    _endpoints = pd.DataFrame([
        {"CS": "1", "Method": "GET",   "Path": "/health",                            "Description": "System health with all dependency statuses"},
        {"CS": "1", "Method": "POST",  "Path": "/api/v1/companies",                  "Description": "Register a new company for scoring"},
        {"CS": "1", "Method": "GET",   "Path": "/api/v1/companies",                  "Description": "List all companies (paginated)"},
        {"CS": "1", "Method": "GET",   "Path": "/api/v1/companies/{id}",             "Description": "Retrieve company by ID"},
        {"CS": "1", "Method": "PUT",   "Path": "/api/v1/companies/{id}",             "Description": "Update company metadata"},
        {"CS": "1", "Method": "DELETE", "Path": "/api/v1/companies/{id}",            "Description": "Soft-delete company record"},
        {"CS": "2", "Method": "POST",  "Path": "/api/v1/documents/collect",          "Description": "Trigger SEC filing collection for a ticker"},
        {"CS": "2", "Method": "POST",  "Path": "/api/v1/signals/collect",            "Description": "Collect hiring, patent, and web signals"},
        {"CS": "3", "Method": "POST",  "Path": "/api/v1/scoring/tc-vr/{ticker}",     "Description": "Compute TC + V^R for one ticker"},
        {"CS": "3", "Method": "POST",  "Path": "/api/v1/scoring/hr/{ticker}",        "Description": "Compute H^R (position-adjusted readiness)"},
        {"CS": "3", "Method": "POST",  "Path": "/api/v1/scoring/orgair/portfolio",   "Description": "Score all 5 companies end-to-end"},
        {"CS": "3", "Method": "POST",  "Path": "/api/v1/scoring/orgair/results",     "Description": "Persist results JSON files to disk"},
    ])

    _ep_cs1  = int((_endpoints["CS"] == "1").sum())
    _ep_cs2  = int((_endpoints["CS"] == "2").sum())
    _ep_cs3  = int((_endpoints["CS"] == "3").sum())
    _ep_total = len(_endpoints)

    _e1, _e2, _e3, _e4 = st.columns(4)
    _e1.metric("Total Endpoints", _ep_total)
    _e2.metric("CS1 Endpoints", _ep_cs1)
    _e3.metric("CS2 Endpoints", _ep_cs2)
    _e4.metric("CS3 Endpoints", _ep_cs3)

    st.dataframe(_endpoints, use_container_width=True, hide_index=True)

    st.markdown(
        "**Design principle:** Each endpoint is idempotent. re-running the same scoring request "
        "produces the same result and safely upserts (MERGE) rather than duplicating rows. "
        "This makes the pipeline safe to retry on failure without data corruption."
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5. Tech Stack Summary
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 5. Technology Stack")
    st.markdown(
        "A summary of every technology used across the platform, grouped by role. "
        "This is the foundation that CS2 (evidence collection) and CS3 (scoring engine) build on."
    )

    _tech = pd.DataFrame([
        {"Role": "API Framework",     "Technology": "FastAPI + Uvicorn",      "Why": "Async-first, auto-generated OpenAPI docs, Pydantic validation"},
        {"Role": "Data Warehouse",    "Technology": "Snowflake",              "Why": "Columnar analytics, VARIANT type for JSON metadata, MERGE upsert"},
        {"Role": "Object Storage",    "Technology": "AWS S3",                 "Why": "Raw document storage, timestamped result history, cheap at scale"},
        {"Role": "Cache",             "Technology": "Redis",                  "Why": "Sub-ms latency for repeated SEC parsing, TTL-based expiry"},
        {"Role": "SEC Filing Access", "Technology": "sec-edgar-downloader",   "Why": "Handles SEC rate limits, downloads 10-K / DEF 14A filings"},
        {"Role": "Web Scraping",      "Technology": "Playwright + httpx",     "Why": "JS-rendered pages (Indeed, Glassdoor), async HTTP for APIs"},
        {"Role": "PDF / HTML Parsing", "Technology": "pdfplumber + BeautifulSoup", "Why": "Extract text from SEC PDFs and HTML filings"},
        {"Role": "Data Modeling",     "Technology": "Pydantic v2",            "Why": "Request/response validation, ORM-free Snowflake mapping"},
        {"Role": "Dashboard",         "Technology": "Streamlit + Plotly",     "Why": "Rapid prototyping, interactive charts, direct Snowflake queries"},
        {"Role": "Testing",           "Technology": "pytest + Hypothesis",    "Why": "Property-based tests for scoring bounds, 500-example runs"},
    ])
    st.dataframe(_tech, use_container_width=True, hide_index=True)