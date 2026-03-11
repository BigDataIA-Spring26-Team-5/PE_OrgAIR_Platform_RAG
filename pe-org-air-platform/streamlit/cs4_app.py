"""
PE Org-AI-R Platform — CS4 Streamlit App
streamlit/cs4_app.py

Main entry point for the CS4 RAG & Search interface.
Run with: streamlit run streamlit/cs4_app.py

Two main sections:
  1. Pipeline — run AI-readiness assessment for any company
  2. Chatbot  — ask questions about processed companies
"""
from __future__ import annotations

import sys
import os
import streamlit as st

# Add project root to path so utils/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Clean professional sidebar */
    .css-1d391kg { padding-top: 1rem; }

    /* Metric cards */
    [data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 12px;
    }

    /* Chat messages */
    [data-testid="stChatMessage"] {
        border-radius: 8px;
        margin-bottom: 8px;
    }

    /* Primary buttons */
    .stButton > button[kind="primary"] {
        background-color: #1f3a5f;
        color: white;
        border-radius: 6px;
        font-weight: 600;
    }

    /* Success/info boxes */
    .stSuccess, .stInfo {
        border-radius: 8px;
    }

    /* Dividers */
    hr { margin: 1rem 0; }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Session State Init ────────────────────────────────────────────────────────
if "active_page" not in st.session_state:
    st.session_state["active_page"] = "pipeline"
if "chatbot_ticker" not in st.session_state:
    st.session_state["chatbot_ticker"] = ""
if "chatbot_company" not in st.session_state:
    st.session_state["chatbot_company"] = ""
if "resolved_company" not in st.session_state:
    st.session_state["resolved_company"] = None
if "auto_resolved" not in st.session_state:
    st.session_state["auto_resolved"] = False
if "run_pipeline" not in st.session_state:
    st.session_state["run_pipeline"] = False

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("# 🏦 PE Org-AI-R")
    st.markdown("*AI-Readiness Assessment Platform*")
    st.divider()

    # Navigation
    st.markdown("### Navigation")

    pipeline_btn = st.button(
        "▶ Pipeline",
        use_container_width=True,
        type="primary" if st.session_state["active_page"] == "pipeline" else "secondary",
        help="Run AI-readiness assessment for a company",
    )

    chatbot_btn = st.button(
        "💬 Company Q&A",
        use_container_width=True,
        type="primary" if st.session_state["active_page"] == "chatbot" else "secondary",
        help="Ask questions about assessed companies",
    )

    if pipeline_btn:
        st.session_state["active_page"] = "pipeline"
        st.rerun()

    if chatbot_btn:
        st.session_state["active_page"] = "chatbot"
        st.rerun()

    st.divider()

    # Current company context
    if st.session_state.get("chatbot_ticker"):
        st.markdown("### Active Company")
        ticker = st.session_state["chatbot_ticker"]
        company = st.session_state["chatbot_company"]
        st.markdown(f"**{company}**")
        st.caption(f"Ticker: {ticker}")

        # Quick score check
        try:
            import requests
            resp = requests.get(
                f"http://localhost:8000/api/v1/scoring/{ticker}/dimensions",
                timeout=3,
            )
            if resp.status_code == 200:
                scores = resp.json().get("scores", [])
                if scores:
                    avg = sum(d["score"] for d in scores) / len(scores)
                    st.metric("Avg Score", f"{avg:.1f}/100")
        except Exception:
            pass

        st.divider()

    # Platform status
    st.markdown("### Platform Status")
    try:
        import requests
        health = requests.get("http://localhost:8000/health", timeout=3).json()
        rag_status = requests.get("http://localhost:8000/rag/status", timeout=3).json()

        deps = health.get("dependencies", {})
        snowflake_ok = "healthy" in str(deps.get("snowflake", ""))
        s3_ok = "healthy" in str(deps.get("s3", ""))
        redis_ok = "healthy" in str(deps.get("redis", ""))

        st.caption(f"{'✅' if snowflake_ok else '❌'} Snowflake")
        st.caption(f"{'✅' if s3_ok else '❌'} S3")
        st.caption(f"{'✅' if redis_ok else '⚠️'} Redis")
        st.caption(f"✅ ChromaDB ({rag_status.get('indexed_documents', 0)} docs)")
    except Exception:
        st.caption("⚠️ Could not reach API")

    st.divider()
    st.caption("PE Org-AI-R Platform v1.0")
    st.caption("CS4 — RAG & Search")


# ── Main Content ──────────────────────────────────────────────────────────────
from views.pipeline_cs4 import render_pipeline_page
from views.chatbot_cs4 import render_chatbot_page

if st.session_state["active_page"] == "pipeline":
    render_pipeline_page()
elif st.session_state["active_page"] == "chatbot":
    render_chatbot_page()