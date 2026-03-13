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

# ── Structural CSS only — NO color overrides, Streamlit theme handles colors ──
st.markdown("""
<style>
footer { visibility: hidden; }

/* Sidebar padding */
[data-testid="stSidebar"] > div:first-child { padding: 16px 12px; }

/* Logo block */
.logo-block {
  display: flex; align-items: center; gap: 10px;
  padding-bottom: 14px; margin-bottom: 14px;
  border-bottom: 1px solid rgba(128,128,128,0.2);
}
.logo-icon {
  width: 30px; height: 30px; background: #4F46E5; border-radius: 6px;
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 700; color: #fff; flex-shrink: 0;
}
.logo-text { font-size: 12px; font-weight: 600; line-height: 1.3; }
.logo-sub  { font-size: 10px; opacity: 0.6; }

/* Section labels */
.nav-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.06em; padding: 0 4px; margin-bottom: 6px; display: block;
}
.sec-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.06em; padding: 0 4px; margin: 14px 0 6px; display: block;
}

/* Company list items */
.co-item {
  display: flex; align-items: center; gap: 8px;
  padding: 6px 8px; border-radius: 6px;
  margin-bottom: 2px;
}
.co-dot       { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.co-dot-green { background: #16a34a; }
.co-dot-gray  { background: #9ca3af; }
.co-ticker    { font-size: 12px; font-weight: 600; }
.co-vectors   { font-size: 10px; opacity: 0.5; margin-left: auto; }

/* Score header layout */
.score-header {
  border-bottom: 1px solid rgba(128,128,128,0.15);
  padding: 10px 0 12px 0;
  margin-bottom: 12px;
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

/* Score blocks row */
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

/* Composite score block */
.sc-block.sc-main { background: rgba(79,70,229,0.08); flex: 0 0 130px; }
.sc-main .sc-label { color: #4F46E5; opacity: 1; }
.sc-main .sc-val   { font-size: 20px; color: #4F46E5; }
.sc-main .sc-tag   { background: #4F46E5; color: #fff; }

/* Strongest dimension */
.sc-block.sc-strongest { background: #fffbeb !important; border-left: 3px solid #d97706 !important; }
.sc-strongest .sc-val   { color: #92400e !important; }
.sc-strongest .sc-label { color: #92400e !important; opacity: 1 !important; }

/* Signal strip */
.signal-row {
  display: flex; margin-top: 8px;
  border: 1px solid rgba(128,128,128,0.15);
  border-radius: 6px; overflow: hidden;
}
.sig-block { flex: 1; padding: 6px 12px; border-right: 1px solid rgba(128,128,128,0.15); }
.sig-block:last-child { border-right: none; }
.sig-label  { font-size: 9px; opacity: 0.5; margin-bottom: 2px; text-transform: uppercase; letter-spacing: 0.04em; }
.sig-val    { font-size: 13px; font-weight: 600; }
.sig-source { font-size: 9px; opacity: 0.45; margin-top: 1px; font-style: italic; }

/* Step rows */
.step-row {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 14px; border-radius: 7px;
  border: 1px solid rgba(128,128,128,0.2);
  margin-bottom: 5px;
}
.step-row-running { border-color: #4F46E5 !important; background: rgba(79,70,229,0.06) !important; }
.step-row-done    { border-color: #86efac !important; background: #f0fdf4 !important; }
.step-row-error   { border-color: #fca5a5 !important; background: #fef2f2 !important; }

.step-num {
  width: 22px; height: 22px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 10px; font-weight: 700; flex-shrink: 0;
}
.sn-idle    { background: rgba(128,128,128,0.1); color: #6b7280; }
.sn-running { background: #4F46E5; color: #fff; }
.sn-done    { background: #16a34a; color: #fff; }
.sn-err     { background: #dc2626; color: #fff; }

.step-icon   { font-size: 14px; flex-shrink: 0; }
.step-name   { font-size: 12px; font-weight: 600; margin-bottom: 1px; }
.step-detail { font-size: 10px; opacity: 0.5; font-family: monospace; }
.step-msg    { font-size: 10px; color: #4F46E5; margin-top: 2px; }
.step-status-lbl { font-size: 11px; font-weight: 600; white-space: nowrap; }
.st-idle    { opacity: 0.45; }
.st-running { color: #4F46E5; }
.st-done    { color: #16a34a; }
.st-err     { color: #dc2626; }
.step-time  { font-size: 10px; opacity: 0.45; min-width: 52px; text-align: right; }

/* Progress bar */
.prog-label { font-size: 11px; opacity: 0.6; margin: 8px 0 5px; }
.prog-bar   { width: 100%; height: 4px; background: rgba(128,128,128,0.15); border-radius: 2px; overflow: hidden; }
.prog-fill  { height: 100%; background: #4F46E5; border-radius: 2px; transition: width 0.4s; }

/* Company confirm card */
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

/* Chat bubbles */
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
  background: rgba(91,33,182,0.1); color: #5b21b6;
  margin-left: 2px; border: 1px solid rgba(91,33,182,0.2);
}

/* Suggested questions */
.sq-label-txt {
  font-size: 10px; opacity: 0.5; text-transform: uppercase;
  letter-spacing: 0.04em; margin-bottom: 7px;
}
.sq-tabs-row  { display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 9px; }
.sq-pills-row { display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 10px; }

/* Evidence panel */
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

/* Columns spacing */
[data-testid="column"] { padding-left: 4px !important; padding-right: 4px !important; }
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
    try:
        r = requests.get("http://localhost:8000/api/v1/companies/all", timeout=5)
        if r.status_code == 200:
            return r.json().get("items", [])
    except Exception:
        pass
    return []


def _check_indexed(ticker: str) -> tuple[bool, int]:
    try:
        r = requests.get(f"http://localhost:8000/api/v1/companies/{ticker}", timeout=3)
        if r.status_code == 200:
            d = r.json()
            cnt = d.get("indexed_count", 0) or d.get("vector_count", 0)
            return cnt > 0, cnt
    except Exception:
        pass
    return False, 0


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

    companies    = _fetch_companies()
    active_ticker = st.session_state.get("chatbot_ticker", "")

    if companies:
        for c in companies[:10]:
            ticker  = c.get("ticker", "")
            name    = c.get("name", ticker)
            indexed, vec_count = _check_indexed(ticker)
            dot_cls = "co-dot-green" if indexed else "co-dot-gray"
            vec_lbl = f"{vec_count}v" if indexed and vec_count else "–"
            is_act  = ticker == active_ticker and chat_active

            cl, cr = st.columns([5, 1])
            with cl:
                st.markdown(
                    f'<div class="co-item">'
                    f'<span class="co-dot {dot_cls}"></span>'
                    f'<span class="co-ticker">{ticker}</span>'
                    f'<span class="co-vectors">{vec_lbl}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with cr:
                if st.button("→", key=f"sb_{ticker}", help=f"Chat about {ticker}"):
                    st.session_state["active_page"]     = "chatbot"
                    st.session_state["chatbot_ticker"]  = ticker
                    st.session_state["chatbot_company"] = name
                    st.rerun()
    else:
        st.caption("No companies yet")

    if st.button("＋  Add company", use_container_width=True, key="add_co"):
        st.session_state["active_page"] = "pipeline"
        st.rerun()

    st.divider()
    st.caption("PE Org-AI-R Platform · CS4")


# ── Route ─────────────────────────────────────────────────────────────────────
from views.pipeline_cs4 import render_pipeline_page
from views.chatbot_cs4  import render_chatbot_page

if st.session_state["active_page"] == "pipeline":
    render_pipeline_page()
else:
    render_chatbot_page()