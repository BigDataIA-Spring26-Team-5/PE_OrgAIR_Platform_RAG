"""
PE Org-AI-R Platform — CS4 Streamlit App
streamlit/cs4_app.py

Run with: streamlit run streamlit/cs4_app.py
"""
from __future__ import annotations

import sys
import os
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme-aware CSS ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Metric cards */
    [data-testid="metric-container"] {
        border: 1px solid rgba(128,128,128,0.2);
        border-radius: 8px;
        padding: 12px 16px;
    }

    /* Strongest dimension highlight */
    [data-testid="metric-container"].strongest-dim {
        border: 2px solid #7F77DD !important;
        background: rgba(127,119,221,0.08) !important;
    }

    /* Chat messages */
    [data-testid="stChatMessage"] {
        border-radius: 8px;
        margin-bottom: 8px;
    }

    /* Primary buttons */
    .stButton > button[kind="primary"] {
        border-radius: 6px;
        font-weight: 600;
    }

    /* Source badge row */
    .source-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: 500;
        margin-right: 4px;
    }

    /* Dimension score pill */
    .dim-pill {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 500;
        margin: 2px;
    }

    /* Step row */
    .step-running {
        border-left: 3px solid #7F77DD !important;
    }
    .step-done {
        border-left: 3px solid #1D9E75 !important;
    }
    .step-error {
        border-left: 3px solid #E24B4A !important;
    }

    /* Hide streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Session State Init ────────────────────────────────────────────────────────
defaults = {
    "active_page": "pipeline",
    "chatbot_ticker": "",
    "chatbot_company": "",
    "resolved_company": None,
    "auto_resolved": False,
    "run_pipeline": False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 PE Org-AI-R")
    st.caption("AI-Readiness Assessment Platform")
    st.divider()

    # Navigation — Pipeline first
    pipe_active = st.session_state["active_page"] == "pipeline"
    chat_active = st.session_state["active_page"] == "chatbot"

    if st.button(
        "⚡ Pipeline",
        use_container_width=True,
        type="primary" if pipe_active else "secondary",
        help="Run AI-readiness assessment for a company",
    ):
        st.session_state["active_page"] = "pipeline"
        st.rerun()

    if st.button(
        "💬 Company Q&A",
        use_container_width=True,
        type="primary" if chat_active else "secondary",
        help="Ask questions about assessed companies",
    ):
        st.session_state["active_page"] = "chatbot"
        st.rerun()

    st.divider()

    # Companies list from API
    st.markdown("**Companies**")
    try:
        resp = requests.get("http://localhost:8000/api/v1/companies/all", timeout=5)
        if resp.status_code == 200:
            companies = resp.json().get("items", [])
            if companies:
                for c in companies[:8]:
                    t = c.get("ticker", "")
                    n = c.get("name", t)
                    is_active = (t == st.session_state.get("chatbot_ticker"))
                    label = f"{'▶ ' if is_active else ''}{t}"
                    if st.button(label, key=f"sb_co_{t}", use_container_width=True):
                        st.session_state["active_page"] = "chatbot"
                        st.session_state["chatbot_ticker"] = t
                        st.session_state["chatbot_company"] = n
                        st.rerun()
            else:
                st.caption("No companies yet")
    except Exception:
        st.caption("API offline")

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