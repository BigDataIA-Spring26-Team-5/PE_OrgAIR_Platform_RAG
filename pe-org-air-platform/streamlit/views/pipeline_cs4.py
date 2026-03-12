"""
PE Org-AI-R Platform — CS4 Pipeline Trigger View
streamlit/views/pipeline_cs4.py

9-step pipeline — PIPELINE_STEPS is the single source of truth.
step_placeholders keyed by name (not index) so adding/removing steps never breaks indexing.

CACHE FIX: _load_companies() no longer uses @st.cache_data.
  - Cache was preventing newly-processed companies from appearing after pipeline runs.
  - _load_companies.clear() at the bottom of _run_pipeline was unreachable in Streamlit's
    top-down execution model — the page re-renders from the top, hitting the cache again.
  - Fix: always hit Snowflake/API directly. The /api/v1/companies/all endpoint is fast
    enough (< 1s) that a TTL cache provides no meaningful benefit at this page's traffic.
"""
from __future__ import annotations

import sys
import os
import time
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from utils.company_resolver import resolve_company
from utils.pipeline_client import PipelineClient, PipelineStepResult, SignalFlag

BASE_URL = "http://localhost:8000"

DIMENSIONS = [
    "data_infrastructure", "ai_governance", "technology_stack",
    "talent", "leadership", "use_case_portfolio", "culture",
]

DIMENSION_LABELS = {
    "data_infrastructure": "Data Infrastructure",
    "ai_governance": "AI Governance",
    "technology_stack": "Technology Stack",
    "talent": "Talent & Skills",
    "leadership": "Leadership & Vision",
    "use_case_portfolio": "Use Case Portfolio",
    "culture": "Culture & Change",
}

TICKER_SECTOR_MAP = {
    "NVDA": "Technology", "NFLX": "Technology", "MSFT": "Technology",
    "GOOGL": "Technology", "GOOG": "Technology", "AAPL": "Technology",
    "META": "Technology", "AMZN": "Technology", "CRM": "Technology",
    "ORCL": "Technology", "IBM": "Technology", "INTC": "Technology",
    "AMD": "Technology", "AVGO": "Technology", "NOW": "Technology",
    "SNOW": "Technology", "PLTR": "Technology",
    "ADP": "Business Services", "PAYX": "Business Services",
    "JPM": "Financial Services", "BAC": "Financial Services",
    "GS": "Financial Services", "MS": "Financial Services",
    "WFC": "Financial Services", "BLK": "Financial Services",
    "HCA": "Healthcare", "UNH": "Healthcare", "JNJ": "Healthcare",
    "PFE": "Healthcare", "ABT": "Healthcare",
    "DG": "Retail", "WMT": "Retail", "TGT": "Retail", "COST": "Retail",
    "CAT": "Industrials", "DE": "Industrials", "GE": "Industrials",
    "HON": "Industrials", "MMM": "Industrials", "BA": "Industrials",
    "XOM": "Energy", "CVX": "Energy", "NEE": "Energy",
}

# ── Step definitions — single source of truth ────────────────────────────────
PIPELINE_STEPS = [
    ("Company Setup",     "🏢"),
    ("SEC Filings",       "📄"),
    ("Parse Documents",   "🔍"),
    ("Chunk Documents",   "✂️"),
    ("Signal Scoring",    "📡"),
    ("Glassdoor Culture", "💬"),
    ("Board Governance",  "🏛️"),
    ("Scoring",           "🧮"),
    ("Index Evidence",    "🗂️"),
]
STEP_NAMES  = [s[0] for s in PIPELINE_STEPS]
STEP_ICONS  = {s[0]: s[1] for s in PIPELINE_STEPS}
TOTAL_STEPS = len(PIPELINE_STEPS)

_ICON_WAITING = "⬜"
_ICON_RUNNING = "🔄"
_ICON_SUCCESS = "✅"
_ICON_SKIPPED = "⏭️"
_ICON_ERROR   = "❌"


def _step_icon(status: str) -> str:
    return {"success": _ICON_SUCCESS, "skipped": _ICON_SKIPPED,
            "error": _ICON_ERROR, "running": _ICON_RUNNING}.get(status, _ICON_WAITING)


def _get_sector(company: dict) -> str:
    sector = company.get("sector")
    if sector:
        return sector.title()
    return TICKER_SECTOR_MAP.get(company.get("ticker", "").upper(), "Unknown")


def _load_companies() -> list:
    """
    Load all companies from Snowflake via API — NO CACHE.

    Cache removed intentionally: @st.cache_data(ttl=120) caused newly-processed
    companies to be invisible for up to 2 minutes after pipeline completion.
    The /api/v1/companies/all endpoint is fast enough to call on every render.
    """
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=60)
        if resp.status_code == 200:
            return resp.json().get("items", [])
    except Exception:
        pass
    return []


def render_pipeline_page():
    st.markdown("## Company Pipeline")
    st.markdown(
        "Enter a company ticker, name, or CIK to run the full "
        "AI-readiness assessment pipeline."
    )
    st.divider()

    col1, col2 = st.columns([3, 1])
    with col1:
        company_input = st.text_input(
            "Company",
            placeholder="e.g. GOOGL, Microsoft, Apple Inc, 0001652044",
            help="Enter a ticker symbol, company name, or SEC CIK number",
            key="pipeline_company_input",
        )
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        resolve_btn = st.button("Search", use_container_width=True)

    resolved = None

    if company_input and (resolve_btn or st.session_state.get("auto_resolved")):
        with st.spinner("Resolving company..."):
            try:
                resolved = resolve_company(company_input)
                st.session_state["resolved_company"] = resolved
                st.session_state["auto_resolved"] = True
            except Exception as e:
                st.error(f"Could not resolve company: {e}")

    if not resolved and st.session_state.get("resolved_company"):
        prev = st.session_state["resolved_company"]
        if prev.ticker and company_input and (
            company_input.upper() == prev.ticker
            or company_input.lower() in prev.name.lower()
        ):
            resolved = prev

    if resolved:
        with st.container():
            st.markdown("#### Resolved Company")
            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("Company", resolved.name)
            with c2: st.metric("Ticker", resolved.ticker)
            with c3:
                sector = getattr(resolved, "sector", None) or TICKER_SECTOR_MAP.get(resolved.ticker, "Unknown")
                st.metric("Sector", sector.title())
            with c4:
                st.metric("Revenue", f"${resolved.revenue_millions:,.0f}M" if resolved.revenue_millions else "N/A")

            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Employees", f"{resolved.employee_count:,}" if resolved.employee_count else "N/A")
            with c2: st.metric("Market Cap Percentile", f"{resolved.market_cap_percentile:.0%}" if resolved.market_cap_percentile else "N/A")
            with c3: st.metric("CIK", resolved.cik or "N/A")

            if resolved.warnings:
                for w in resolved.warnings:
                    st.warning(w)

        st.divider()

        client = PipelineClient()
        status = client.get_company_status(resolved.ticker)

        if status["chatbot_ready"]:
            score_str = f"Org-AI-R Score: **{status['org_air_score']}/100** | " if status["org_air_score"] else ""
            st.success(
                f"**{resolved.name}** is already processed and ready for Q&A. "
                f"{score_str}Evidence: **{status['indexed_documents']} documents**"
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Re-run Pipeline", use_container_width=True):
                    st.session_state["run_pipeline"] = True
            with c2:
                if st.button("Go to Chatbot", use_container_width=True, type="primary"):
                    st.session_state["active_page"] = "chatbot"
                    st.session_state["chatbot_ticker"] = resolved.ticker
                    st.session_state["chatbot_company"] = resolved.name
                    st.rerun()
        else:
            st.info(
                f"**{resolved.name}** has not been processed yet. "
                "Run the pipeline to enable the AI-readiness assessment and chatbot."
            )

        if not status["chatbot_ready"] or st.session_state.get("run_pipeline"):
            if st.button(
                f"Run Full Pipeline for {resolved.ticker}",
                type="primary", use_container_width=True,
            ):
                st.session_state["run_pipeline"] = False
                _run_pipeline(resolved, client)

    st.divider()
    _render_processed_companies()


def _run_pipeline(resolved, client: PipelineClient):
    st.markdown("### Pipeline Progress")

    step_placeholders: dict = {}
    for i, (name, icon) in enumerate(PIPELINE_STEPS):
        ph = st.empty()
        ph.markdown(f"{_ICON_WAITING} **Step {i+1}: {icon} {name}** — waiting...")
        step_placeholders[name] = ph

    substep_ph   = st.empty()
    progress_bar = st.progress(0)
    status_text  = st.empty()
    pipeline_start = time.time()

    def on_step_start(step_name: str):
        if step_name not in step_placeholders:
            return
        idx  = STEP_NAMES.index(step_name)
        icon = STEP_ICONS.get(step_name, "")
        step_placeholders[step_name].markdown(
            f"{_ICON_RUNNING} **Step {idx+1}: {icon} {step_name}** — running..."
        )
        status_text.markdown(f"*⏳ Running: {step_name}...*")
        substep_ph.empty()

    def on_step_complete(step: PipelineStepResult):
        name = step.name
        if name not in step_placeholders:
            return
        idx  = STEP_NAMES.index(name)
        icon = STEP_ICONS.get(name, "")
        dur  = f"*({step.duration_seconds:.1f}s)*"
        step_placeholders[name].markdown(
            f"{_step_icon(step.status)} **Step {idx+1}: {icon} {name}** {dur}  \n"
            f"&nbsp;&nbsp;&nbsp;&nbsp;{step.message}"
        )
        progress_bar.progress((idx + 1) / TOTAL_STEPS)
        substep_ph.empty()
        if step.error:
            st.error(f"**{name} error:** {step.error}")

    def on_substep(step_name: str, msg: str):
        substep_ph.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ *{msg}*")

    with st.spinner("Pipeline running — Signal Scoring alone can take 10+ minutes (jobs + patents + leadership)..."):
        result = client.run_pipeline(
            resolved,
            on_step_start=on_step_start,
            on_step_complete=on_step_complete,
            on_substep=on_substep,
        )

    total_elapsed = time.time() - pipeline_start
    progress_bar.progress(1.0)
    status_text.empty()
    substep_ph.empty()

    mins, secs = divmod(int(total_elapsed), 60)
    time_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    st.divider()
    if result.overall_status == "success":
        st.balloons()
        st.success(f"✅ Pipeline completed in **{time_str}**")
    elif result.overall_status == "partial":
        st.warning(f"⚠️ Pipeline completed with some issues in **{time_str}**")
    else:
        st.error(f"❌ Pipeline failed after **{time_str}**")

    if result.steps:
        with st.expander("Step timing breakdown", expanded=False):
            c0, c1, c2 = st.columns([3, 1, 1])
            c0.markdown("**Step**"); c1.markdown("**Status**"); c2.markdown("**Duration**")
            for s in result.steps:
                a, b, c = st.columns([3, 1, 1])
                a.write(s.name); b.write(_step_icon(s.status)); c.write(f"{s.duration_seconds:.1f}s")

    # ── Signal sanity flags ───────────────────────────────────────────────────
    # Populated by the Groq LLM sanity check in _step_signal_scoring().
    # Never blocks the pipeline — analyst decides whether to force a re-score.
    if result.signal_flags:
        st.divider()
        st.markdown("### ⚠️ Signal Score Review")
        st.caption(
            "The following signal scores were flagged as potentially suspicious by an LLM sanity check. "
            "Scores are **not** automatically corrected — review and use **Force Re-score** if needed."
        )

        SEVERITY_COLOR = {"high": "🔴", "medium": "🟡", "low": "🟠"}
        CATEGORY_LABELS = {
            "technology_hiring":   "Technology Hiring",
            "digital_presence":    "Digital Presence",
            "innovation_activity": "Innovation Activity",
            "leadership_signals":  "Leadership Signals",
        }

        for flag in result.signal_flags:
            severity_icon = SEVERITY_COLOR.get(flag.severity, "⚠️")
            cat_label     = CATEGORY_LABELS.get(flag.category, flag.category.replace("_", " ").title())
            with st.container(border=True):
                col_a, col_b = st.columns([1, 4])
                with col_a:
                    st.metric(cat_label, f"{flag.score:.1f}/100")
                    st.caption(f"{severity_icon} Severity: **{flag.severity.upper()}**")
                with col_b:
                    st.markdown(f"**Why flagged:** {flag.reason}")
                    if flag.raw_value:
                        st.caption(f"Raw evidence: *{flag.raw_value}*")
                    st.caption(
                        "ℹ️ To re-score this signal, run the pipeline with **Force Re-score** enabled."
                    )

        # Offer force re-score button directly
        if st.button(
            f"🔄 Force Re-score All Signals for {resolved.ticker}",
            key="force_rescore_btn",
            help="Re-runs all 4 signal scrapers regardless of today's cached scores",
        ):
            with st.spinner("Re-scoring all signals..."):
                from utils.pipeline_client import PipelineClient as _PC
                _client = _PC()
                rescore_result = _client.run_pipeline(
                    resolved,
                    force_rescore=True,
                )
            if rescore_result.signal_flags:
                st.warning(f"Still flagged after re-score: {len(rescore_result.signal_flags)} signal(s)")
            else:
                st.success("✅ All scores look plausible after re-score")

    scoring_step = next(
        (s for s in result.steps if s.name == "Scoring" and s.status == "success"), None
    )
    if scoring_step:
        _render_score_summary(scoring_step.data, resolved)

    index_step = next(
        (s for s in result.steps if s.name == "Index Evidence" and s.status == "success"), None
    )
    if index_step:
        indexed = index_step.data.get("indexed_count", 0)
        st.success(f"**{indexed} evidence pieces indexed** — Chatbot is now ready!")
        if st.button("Start Chatbot for " + resolved.ticker, type="primary", use_container_width=True):
            st.session_state["active_page"] = "chatbot"
            st.session_state["chatbot_ticker"] = resolved.ticker
            st.session_state["chatbot_company"] = resolved.name
            st.rerun()


def _render_score_summary(data: dict, resolved):
    dim_scores = data.get("dimension_scores", data.get("scores", []))
    if not dim_scores:
        return

    st.markdown("### AI-Readiness Scores")
    st.markdown(f"**{resolved.name} ({resolved.ticker})**")

    scores_dict = (
        {d["dimension"]: d["score"] for d in dim_scores}
        if isinstance(dim_scores, list) else dim_scores
    )
    avg_score = sum(scores_dict.values()) / len(scores_dict) if scores_dict else 0

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Avg AI-Readiness", f"{avg_score:.1f}/100")
    with c2:
        level = "Excellent" if avg_score >= 80 else "Good" if avg_score >= 60 else "Adequate" if avg_score >= 40 else "Developing"
        st.metric("Overall Level", level)
    with c3:
        rec = "PROCEED" if avg_score >= 65 else "PROCEED WITH CAUTION" if avg_score >= 45 else "FURTHER DILIGENCE"
        st.metric("Recommendation", rec)

    st.markdown("#### Dimension Breakdown")
    sorted_scores = sorted(scores_dict.items(), key=lambda x: x[1], reverse=True)
    cols = st.columns(len(sorted_scores))
    for i, (dim, score) in enumerate(sorted_scores):
        with cols[i]:
            label = DIMENSION_LABELS.get(dim, dim.replace("_", " ").title())
            delta = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
            st.metric(label[:12], f"{score:.0f}", delta=delta)


def _render_processed_companies():
    st.markdown("### Companies in Platform")
    companies = _load_companies()   # always fresh — no cache
    if not companies:
        st.info("No companies yet — enter a company above to get started.")
        return

    c0, c1, c2, c3 = st.columns([2, 1, 1, 1])
    c0.markdown("**Company**"); c1.markdown("**Ticker**"); c2.markdown("**Sector**"); c3.markdown("**Action**")
    st.divider()

    for company in companies[:10]:
        ticker = company.get("ticker", "")
        name   = company.get("name", ticker)
        sector = _get_sector(company)
        r0, r1, r2, r3 = st.columns([2, 1, 1, 1])
        r0.write(name[:30]); r1.write(ticker); r2.write(sector)
        with r3:
            if st.button("Chat", key=f"chat_{ticker}", use_container_width=True):
                st.session_state["active_page"] = "chatbot"
                st.session_state["chatbot_ticker"] = ticker
                st.session_state["chatbot_company"] = name
                st.rerun()