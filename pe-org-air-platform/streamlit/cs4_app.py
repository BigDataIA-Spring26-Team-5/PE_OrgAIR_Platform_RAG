"""
PE Org-AI-R Platform — CS4 Streamlit App
streamlit/cs4_app.py
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

st.markdown("""
<style>
/* ===== FORCE BLUE on ALL Streamlit primary buttons ===== */
.stButton > button[kind="primary"],
.stButton > button[data-testid="stBaseButton-primary"],
button[kind="primary"] {
  background-color: #4F46E5 !important;
  border-color: #4F46E5 !important;
  color: #fff !important;
}
.stButton > button[kind="primary"]:hover, button[kind="primary"]:hover {
  background-color: #4338CA !important; border-color: #4338CA !important;
}
.stButton > button[kind="secondary"] {
  border-color: rgba(79,70,229,0.3) !important; color: #4F46E5 !important;
}
.stButton > button[kind="secondary"]:hover {
  border-color: #4F46E5 !important; background-color: rgba(79,70,229,0.06) !important;
}
.stSpinner > div > div { border-top-color: #4F46E5 !important; }
a { color: #4F46E5 !important; }
.stProgress > div > div > div { background-color: #4F46E5 !important; }

footer { visibility: hidden; }
[data-testid="stSidebar"] > div:first-child { padding: 16px 12px; }

/* ===== Sidebar logo — BIGGER TEXT ===== */
.logo-block {
  display: flex; align-items: center; gap: 10px;
  padding-bottom: 14px; margin-bottom: 14px;
  border-bottom: 1px solid rgba(128,128,128,0.2);
}
.logo-icon {
  width: 38px; height: 38px; background: #4F46E5; border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  font-size: 14px; font-weight: 700; color: #fff; flex-shrink: 0;
}
.logo-text { font-size: 16px; font-weight: 700; line-height: 1.3; }
.logo-sub  { font-size: 12px; opacity: 0.55; margin-top: 1px; }

.nav-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.06em; padding: 0 4px; margin-bottom: 6px; display: block;
}
.sec-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.06em; padding: 0 4px; margin: 14px 0 6px; display: block;
}

/* ===== Score header ===== */
.score-header {
  border-bottom: 1px solid rgba(128,128,128,0.15);
  padding: 10px 0 12px 0; margin-bottom: 12px;
}
.sh-top { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
.sh-company { font-size: 15px; font-weight: 600; }
.sh-ticker {
  font-size: 11px; padding: 2px 8px; border-radius: 5px; font-weight: 600;
  background: rgba(79,70,229,0.12); color: #4F46E5;
}
.sh-rec {
  margin-left: auto; font-size: 11px; font-weight: 600;
  padding: 4px 12px; border-radius: 5px; border: 1px solid;
}
.rec-proceed { background: #f0fdf4; color: #15803d; border-color: #86efac; }
.rec-caution { background: #fffbeb; color: #92400e; border-color: #fcd34d; }
.rec-pending { background: rgba(128,128,128,0.08); color: #6b7280; border-color: rgba(128,128,128,0.2); }

.sh-scores {
  display: flex; border: 1px solid rgba(128,128,128,0.2);
  border-radius: 8px; overflow: hidden;
}
.sc-block {
  flex: 1; padding: 7px 10px;
  border-right: 1px solid rgba(128,128,128,0.15); min-width: 0;
}
.sc-block:last-child { border-right: none; }
.sc-label {
  font-size: 9px; opacity: 0.55; margin-bottom: 3px;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  text-transform: uppercase; letter-spacing: 0.04em;
}
.sc-val  { font-size: 16px; font-weight: 600; }
.sc-tag  {
  display: inline-flex; font-size: 9px; padding: 2px 5px;
  border-radius: 3px; margin-top: 2px; font-weight: 600;
}
.tag-high { background: #f0fdf4; color: #15803d; }
.tag-low  { background: #fffbeb; color: #92400e; }
.tag-med  { background: #eff6ff; color: #1e40af; }

.sc-block.sc-main { background: rgba(79,70,229,0.08); flex: 0 0 130px; }
.sc-main .sc-label { color: #4F46E5; opacity: 1; }
.sc-main .sc-val   { font-size: 20px; color: #4F46E5; }
.sc-main .sc-tag   { background: #4F46E5; color: #fff; }

.sc-block.sc-strongest { background: #fffbeb !important; border-left: 3px solid #d97706 !important; }
.sc-strongest .sc-val   { color: #92400e !important; }
.sc-strongest .sc-label { color: #92400e !important; opacity: 1 !important; }

/* ===== Signal strip — BIGGER labels ===== */
.signal-row {
  display: flex; margin-top: 8px;
  border: 1px solid rgba(128,128,128,0.15);
  border-radius: 6px; overflow: hidden;
}
.sig-block { flex: 1; padding: 8px 12px; border-right: 1px solid rgba(128,128,128,0.15); }
.sig-block:last-child { border-right: none; }
.sig-label  { font-size: 11px; opacity: 0.65; margin-bottom: 3px; font-weight: 500; }
.sig-val    { font-size: 15px; font-weight: 600; }
.sig-source { font-size: 10px; opacity: 0.45; margin-top: 2px; font-style: italic; }

/* ===== Pipeline step rows ===== */
.step-row {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 14px; border-radius: 7px;
  border: 1px solid rgba(128,128,128,0.2); margin-bottom: 5px;
}
.step-row-running { border-color: #4F46E5 !important; background: rgba(79,70,229,0.06) !important; }
.step-row-done    { border-color: #86efac !important; background: #f0fdf4 !important; }
.step-row-error   { border-color: #93c5fd !important; background: #eff6ff !important; }

.step-num {
  width: 22px; height: 22px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 10px; font-weight: 700; flex-shrink: 0;
}
.sn-idle    { background: rgba(128,128,128,0.1); color: #6b7280; }
.sn-running { background: #4F46E5; color: #fff; }
.sn-done    { background: #16a34a; color: #fff; }
.sn-err     { background: #4F46E5; color: #fff; }

.step-icon   { font-size: 14px; flex-shrink: 0; }
.step-name   { font-size: 12px; font-weight: 600; margin-bottom: 1px; }
.step-detail { font-size: 10px; opacity: 0.5; font-family: monospace; }
.step-msg    { font-size: 10px; color: #4F46E5; margin-top: 2px; }
.step-status-lbl { font-size: 11px; font-weight: 600; white-space: nowrap; }
.st-idle    { opacity: 0.45; }
.st-running { color: #4F46E5; }
.st-done    { color: #16a34a; }
.st-err     { color: #4F46E5; }
.step-time  { font-size: 10px; opacity: 0.45; min-width: 52px; text-align: right; }

.prog-label { font-size: 11px; opacity: 0.6; margin: 8px 0 5px; }
.prog-bar   { width: 100%; height: 4px; background: rgba(128,128,128,0.15); border-radius: 2px; overflow: hidden; }
.prog-fill  { height: 100%; background: #4F46E5; border-radius: 2px; transition: width 0.4s; }

/* ===== Company confirm card ===== */
.co-confirm-card {
  border: 1px solid rgba(128,128,128,0.2); border-radius: 8px;
  padding: 12px 16px; margin: 10px 0 12px 0;
  display: flex; align-items: center; gap: 12px;
}
.co-confirm-card.confirmed { border-color: #86efac; background: #f0fdf4; }
.co-logo {
  width: 36px; height: 36px; border-radius: 7px;
  background: rgba(79,70,229,0.1);
  display: flex; align-items: center; justify-content: center;
  font-size: 13px; font-weight: 700; color: #4F46E5; flex-shrink: 0;
}

/* ===== Chat bubbles ===== */
.msg-user-wrap { text-align: right; margin-bottom: 12px; }
.bubble-user {
  display: inline-block; padding: 10px 14px; border-radius: 10px;
  font-size: 13px; line-height: 1.6; max-width: 85%;
  background: #4F46E5; color: #fff;
}
.bubble-ai {
  display: inline-block; padding: 10px 14px; border-radius: 10px;
  font-size: 13px; line-height: 1.6; max-width: 85%;
  border: 1px solid rgba(128,128,128,0.2);
}
.bubble-thinking { font-style: italic; opacity: 0.5; font-size: 12px; }
.cite-tag {
  display: inline; padding: 1px 5px; border-radius: 3px; font-size: 10px;
  background: rgba(79,70,229,0.1); color: #4F46E5;
  margin-left: 2px; border: 1px solid rgba(79,70,229,0.2);
}

/* ===== Suggested questions — SMALLER pills ===== */
.sq-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.04em; margin-bottom: 7px;
}

/* ===== Evidence panel ===== */
.ev-header {
  padding: 12px 14px; border-bottom: 1px solid rgba(128,128,128,0.15);
  font-size: 11px; font-weight: 600; opacity: 0.7;
}
.ev-card {
  border: 1px solid rgba(128,128,128,0.2); border-radius: 7px;
  padding: 9px 11px; margin-bottom: 7px;
}
.ev-top    { display: flex; align-items: center; gap: 5px; margin-bottom: 5px; }
.src-badge { font-size: 9px; padding: 2px 6px; border-radius: 20px; font-weight: 600; }
.src-sec   { background: #f0fdf4; color: #15803d; border: 1px solid #86efac; }
.src-job   { background: #fffbeb; color: #92400e; border: 1px solid #fcd34d; }
.src-gd    { background: rgba(128,128,128,0.08); color: #6b7280; border: 1px solid rgba(128,128,128,0.2); }
.src-proxy { background: #eff6ff; color: #1e40af; border: 1px solid #93c5fd; }
.ev-score   { margin-left: auto; font-size: 9px; opacity: 0.45; }
.ev-section { font-size: 10px; opacity: 0.65; margin-bottom: 3px; font-weight: 500; }
.ev-snippet { font-size: 11px; line-height: 1.5; opacity: 0.8; }

[data-testid="column"] { padding-left: 4px !important; padding-right: 4px !important; }

/* ===== Dimension scores grid — ROW layout ===== */
.dim-scores-grid {
  display: flex; flex-wrap: wrap; gap: 8px;
  margin: 12px 0; padding: 10px;
  border: 1px solid rgba(128,128,128,0.2);
  border-radius: 8px; background: rgba(79,70,229,0.03);
}
.dim-score-item {
  flex: 1 1 120px; min-width: 100px;
  padding: 8px 10px; border-radius: 6px; background: white;
  border: 1px solid rgba(128,128,128,0.15);
}
.dim-score-label {
  font-size: 9px; opacity: 0.6; text-transform: uppercase;
  letter-spacing: 0.04em; margin-bottom: 3px;
}
.dim-score-value {
  font-size: 18px; font-weight: 700; color: #4F46E5;
}
</style>
""", unsafe_allow_html=True)

# ── Session State ─────────────────────────────────────────────────────────────
for k, v in {
    "active_page":      "pipeline",
    "chatbot_ticker":   "",
    "chatbot_company":  "",
    "resolved_company": None,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v


def _fetch_companies() -> list[dict]:
    """Fetch all companies from Snowflake via API."""
    try:
        r = requests.get("http://localhost:8000/api/v1/companies/all")
        if r.status_code == 200:
            return r.json().get("items", [])
    except Exception:
        pass
    return []


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:

    st.markdown("""
    <div class="logo-block">
      <div class="logo-icon">PE</div>
      <div>
        <div class="logo-text">OrgAIR Platform</div>
        <div class="logo-sub">AI-Readiness Assessment</div>
      </div>
    </div>
    <span class="nav-label-txt">Navigation</span>
    """, unsafe_allow_html=True)

    pipe_active = st.session_state["active_page"] == "pipeline"
    chat_active = st.session_state["active_page"] == "chatbot"

    if st.button("⚡  Pipeline", use_container_width=True,
                 type="primary" if pipe_active else "secondary", key="nav_pipe"):
        st.session_state["active_page"] = "pipeline"
        st.rerun()

    if st.button("💬  Chatbot", use_container_width=True,
                 type="primary" if chat_active else "secondary", key="nav_chat"):
        st.session_state["active_page"] = "chatbot"
        st.rerun()

    st.markdown('<span class="sec-label-txt">Companies</span>', unsafe_allow_html=True)

    companies = _fetch_companies()

    if companies:
        options = ["Select a company..."] + [f"{c['ticker']} — {c['name']}" for c in companies]
        current_ticker = st.session_state.get("chatbot_ticker", "")

        default_idx = 0
        if current_ticker:
            match = [i + 1 for i, c in enumerate(companies) if c["ticker"] == current_ticker]
            if match:
                default_idx = match[0]

        selected = st.selectbox(
            "Company", options=options, index=default_idx,
            key="sidebar_company_select", label_visibility="collapsed"
        )

        if selected != "Select a company..." and " — " in selected:
            sel_ticker = selected.split(" — ")[0].strip()
            sel_name = selected.split(" — ")[1].strip()

            if sel_ticker != current_ticker:
                st.session_state["chatbot_ticker"] = sel_ticker
                st.session_state["chatbot_company"] = sel_name
                if st.session_state["active_page"] != "chatbot":
                    st.session_state["active_page"] = "chatbot"
                    st.rerun()
    else:
        st.caption("No companies yet — run the Pipeline to add one")

    st.divider()
    st.caption("PE Org-AI-R Platform · CS4")


# ── Route ─────────────────────────────────────────────────────────────────────
from views.pipeline_cs4 import render_pipeline_page
from views.chatbot_cs4  import render_chatbot_page

if st.session_state["active_page"] == "pipeline":
    render_pipeline_page()
else:
    render_chatbot_page()