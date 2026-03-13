"""
PE Org-AI-R Platform — CS4 Pipeline Trigger View
streamlit/views/pipeline_cs4.py

Changes vs original:
  - Individual step run buttons with prerequisite enforcement
  - Signal scores row with data source labels (Wappalyzer, USPTO, etc.)
  - Score header matches mockup style
  - Strongest dimension highlighted
  - No cache on _load_companies()
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
    "ai_governance":       "AI Governance",
    "technology_stack":    "Technology Stack",
    "talent":              "Talent & Skills",
    "leadership":          "Leadership & Vision",
    "use_case_portfolio":  "Use Case Portfolio",
    "culture":             "Culture & Change",
}

# Signal score metadata — what each signal measures and where data comes from
SIGNAL_META = {
    "technology_hiring": {
        "label":  "Technology Hiring",
        "source": "LinkedIn · Indeed job postings",
        "icon":   "👥",
        "desc":   "AI/ML role density in active job postings",
    },
    "digital_presence": {
        "label":  "Digital Presence",
        "source": "Wappalyzer · BuiltWith",
        "icon":   "🌐",
        "desc":   "Tech stack sophistication of company website",
    },
    "innovation_activity": {
        "label":  "Innovation Activity",
        "source": "USPTO patent database",
        "icon":   "💡",
        "desc":   "AI/ML patent portfolio volume and recency",
    },
    "leadership_signals": {
        "label":  "Leadership Signals",
        "source": "SEC DEF 14A proxy filings",
        "icon":   "🏛",
        "desc":   "Board and executive AI/tech background",
    },
}

PIPELINE_STEPS = [
    ("Company Setup",     "🏢", 1),
    ("SEC Filings",       "📄", 2),
    ("Parse Documents",   "🔍", 3),
    ("Chunk Documents",   "✂️",  4),
    ("Signal Scoring",    "📡", 5),
    ("Glassdoor Culture", "💬", 6),
    ("Board Governance",  "🏛️", 7),
    ("Scoring",           "🧮", 8),
    ("Index Evidence",    "🗂️", 9),
]

# Steps that require prior steps to have completed
STEP_PREREQUISITES = {
    1: [],
    2: [1],
    3: [1, 2],
    4: [1, 2, 3],
    5: [1, 2],
    6: [1],
    7: [1],
    8: [1, 5, 6, 7],
    9: [1, 8],
}

# Which steps are fatal (pipeline aborts on error)
FATAL_STEPS = {1, 2, 5, 8}

STEP_NAMES  = [s[0] for s in PIPELINE_STEPS]
STEP_ICONS  = {s[0]: s[1] for s in PIPELINE_STEPS}
STEP_NUMS   = {s[0]: s[2] for s in PIPELINE_STEPS}
TOTAL_STEPS = len(PIPELINE_STEPS)

_ICON_WAITING = "⬜"
_ICON_RUNNING = "🔄"
_ICON_SUCCESS = "✅"
_ICON_SKIPPED = "⏭️"
_ICON_ERROR   = "❌"

TICKER_SECTOR_MAP = {
    "NVDA": "Technology", "NFLX": "Technology", "MSFT": "Technology",
    "GOOGL": "Technology", "GOOG": "Technology", "AAPL": "Technology",
    "META": "Technology", "AMZN": "Technology", "CRM": "Technology",
    "JPM": "Financial Services", "BAC": "Financial Services",
    "GS": "Financial Services", "UNH": "Healthcare", "JNJ": "Healthcare",
}


def _step_icon(status: str) -> str:
    return {
        "success": _ICON_SUCCESS, "skipped": _ICON_SKIPPED,
        "error": _ICON_ERROR,     "running": _ICON_RUNNING,
    }.get(status, _ICON_WAITING)


def _get_sector(company: dict) -> str:
    sector = company.get("sector")
    if sector:
        return sector.title()
    return TICKER_SECTOR_MAP.get(company.get("ticker", "").upper(), "Unknown")


def _load_companies() -> list:
    """Always fresh — no cache (cache prevented newly-processed companies from appearing)."""
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=60)
        if resp.status_code == 200:
            return resp.json().get("items", [])
    except Exception:
        pass
    return []


def _get_completed_steps(ticker: str) -> set[int]:
    """
    Determine which step numbers have already completed for this ticker.
    Used to enforce prerequisites for individual step buttons.
    """
    completed = set()
    client = PipelineClient()
    status = client.get_company_status(ticker)

    # Step 1 done if company exists
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/{ticker}", timeout=5)
        if r.status_code == 200:
            completed.add(1)
    except Exception:
        pass

    # Steps 2-4 done if documents exist
    if status.get("has_documents"):
        completed.update({2, 3, 4})

    # Step 5 done if signals exist (non-zero scores)
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/{ticker}/evidence", timeout=5)
        if r.status_code == 200:
            sig = r.json().get("signal_summary", {})
            if any(sig.get(k, 0) for k in ["technology_hiring_score", "digital_presence_score",
                                            "innovation_activity_score", "leadership_signals_score"]):
                completed.add(5)
    except Exception:
        pass

    # Steps 6-7 — always allow (non-fatal, hard to check)
    completed.update({6, 7})

    # Steps 8-9 done if scores / indexed exist
    if status.get("has_scores"):
        completed.add(8)
    if status.get("chatbot_ready"):
        completed.add(9)

    return completed


def render_pipeline_page():
    st.markdown("## ⚡ Pipeline Builder")
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
        resolve_btn = st.button("🔍 Search", use_container_width=True)

    resolved = None

    if company_input and (resolve_btn or st.session_state.get("auto_resolved")):
        with st.spinner("Resolving company via Yahoo Finance + SEC EDGAR..."):
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
        _render_resolved_company(resolved)
        st.divider()
        _render_pipeline_controls(resolved)

    st.divider()
    _render_processed_companies()


def _render_resolved_company(resolved):
    """Company confirmation card with all metadata from yfinance."""
    st.markdown("#### Resolved Company")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Company", resolved.name[:25] if len(resolved.name) > 25 else resolved.name)
    with col2:
        st.metric("Ticker", resolved.ticker)
    with col3:
        sector = getattr(resolved, "sector", None) or TICKER_SECTOR_MAP.get(resolved.ticker, "Unknown")
        st.metric("Sector", sector.title())
    with col4:
        rev = resolved.revenue_millions
        st.metric("Revenue", f"${rev:,.0f}M" if rev else "N/A")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        emp = resolved.employee_count
        st.metric("Employees", f"{emp:,}" if emp else "N/A")
    with col2:
        mcp = resolved.market_cap_percentile
        st.metric("Mkt Cap Percentile", f"{mcp:.0%}" if mcp else "N/A")
    with col3:
        st.metric("CIK", resolved.cik or "N/A")
    with col4:
        st.metric("Sub-sector", (resolved.sub_sector or "N/A")[:18])

    if resolved.warnings:
        for w in resolved.warnings:
            st.warning(w)


def _render_pipeline_controls(resolved):
    """Full pipeline run + individual step buttons."""
    ticker = resolved.ticker
    client = PipelineClient()
    status = client.get_company_status(ticker)

    # ── Already processed banner ──────────────────────────────────
    if status["chatbot_ready"]:
        score_str = f"Org-AI-R Score: **{status['org_air_score']}/100** | " if status["org_air_score"] else ""
        st.success(
            f"**{resolved.name}** is already processed and chatbot-ready. "
            f"{score_str}Evidence: **{status['indexed_documents']} vectors**"
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔄 Re-run Full Pipeline", use_container_width=True):
                st.session_state["run_pipeline"] = True
        with c2:
            if st.button("💬 Go to Chatbot", use_container_width=True, type="primary"):
                st.session_state["active_page"] = "chatbot"
                st.session_state["chatbot_ticker"] = ticker
                st.session_state["chatbot_company"] = resolved.name
                st.rerun()
    else:
        st.info(f"**{resolved.name}** has not been fully processed yet.")

    # ── Full pipeline button ──────────────────────────────────────
    if not status["chatbot_ready"] or st.session_state.get("run_pipeline"):
        if st.button(
            f"▶ Run Full Pipeline for {ticker}",
            type="primary", use_container_width=True,
            key="run_full_pipeline_btn",
        ):
            st.session_state["run_pipeline"] = False
            _run_pipeline(resolved, client)
            return

    # ── Individual step buttons ───────────────────────────────────
    with st.expander("🔧 Run Individual Steps", expanded=False):
        st.caption(
            "Run a specific step independently. Prerequisites must be completed first. "
            "Steps 1, 2, 5, 8 are **fatal** — pipeline aborts if they fail."
        )

        completed = _get_completed_steps(ticker)

        for step_name, step_icon, step_num in PIPELINE_STEPS:
            prereqs = STEP_PREREQUISITES[step_num]
            missing = [p for p in prereqs if p not in completed]
            is_fatal = step_num in FATAL_STEPS
            already_done = step_num in completed

            col_a, col_b, col_c = st.columns([3, 1, 1])
            with col_a:
                done_icon = "✅" if already_done else ("🔴" if is_fatal else "⬜")
                st.markdown(
                    f"{done_icon} **Step {step_num}: {step_icon} {step_name}**"
                    + (" *(fatal)*" if is_fatal else "")
                )
                if missing:
                    st.caption(f"⚠️ Requires steps {missing} first")

            with col_b:
                btn_disabled = len(missing) > 0
                btn_label = "Re-run" if already_done else "Run"
                if st.button(
                    btn_label,
                    key=f"step_btn_{step_num}",
                    disabled=btn_disabled,
                    use_container_width=True,
                ):
                    _run_single_step(resolved, client, step_num, step_name)
                    st.rerun()

            with col_c:
                if missing:
                    st.caption("🔒 Locked")
                elif already_done:
                    st.caption("✅ Done")
                else:
                    st.caption("Ready")


def _run_single_step(resolved, client: PipelineClient, step_num: int, step_name: str):
    """Run a single pipeline step and show result inline."""
    ticker = resolved.ticker
    ph = st.empty()
    ph.markdown(f"🔄 Running **Step {step_num}: {step_name}**...")

    start = time.time()
    result = None

    try:
        if step_num == 1:
            result = client._step_create_company(resolved)
        elif step_num == 2:
            result = client._step_collect_sec(ticker, resolved.cik)
        elif step_num == 3:
            doc_status = client._get_doc_status(ticker)
            result = client._step_parse(ticker, doc_status=doc_status)
        elif step_num == 4:
            doc_status = client._get_doc_status(ticker)
            result = client._step_chunk(ticker, doc_status=doc_status)
        elif step_num == 5:
            website = getattr(resolved, "website", None)
            result = client._step_signal_scoring(
                ticker,
                company_name=resolved.name,
                website=website,
            )
        elif step_num == 6:
            result = client._step_glassdoor(ticker)
        elif step_num == 7:
            result = client._step_board_governance(ticker)
        elif step_num == 8:
            result = client._step_score(ticker)
        elif step_num == 9:
            result = client._step_index(ticker, force=True)

        elapsed = time.time() - start

        if result:
            icon = _step_icon(result.status)
            ph.markdown(
                f"{icon} **Step {step_num}: {step_name}** "
                f"*({elapsed:.1f}s)*  \n&nbsp;&nbsp;&nbsp;&nbsp;{result.message}"
            )
            if result.error:
                st.error(f"Error: {result.error}")
        else:
            ph.markdown(f"⚠️ **Step {step_num}: {step_name}** — no result returned")

    except Exception as e:
        elapsed = time.time() - start
        ph.markdown(f"❌ **Step {step_num}: {step_name}** *({elapsed:.1f}s)* — exception")
        st.error(str(e))


def _run_pipeline(resolved, client: PipelineClient):
    st.markdown("### Pipeline Progress")

    step_placeholders: dict = {}
    for name, icon, num in PIPELINE_STEPS:
        ph = st.empty()
        ph.markdown(f"{_ICON_WAITING} **Step {num}: {icon} {name}** — waiting...")
        step_placeholders[name] = ph

    substep_ph   = st.empty()
    progress_bar = st.progress(0)
    status_text  = st.empty()
    pipeline_start = time.time()

    def on_step_start(step_name: str):
        if step_name not in step_placeholders:
            return
        num  = STEP_NUMS.get(step_name, "?")
        icon = STEP_ICONS.get(step_name, "")
        step_placeholders[step_name].markdown(
            f"{_ICON_RUNNING} **Step {num}: {icon} {step_name}** — running..."
        )
        status_text.markdown(f"*⏳ Running: {step_name}...*")
        substep_ph.empty()

    def on_step_complete(step: PipelineStepResult):
        name = step.name
        if name not in step_placeholders:
            return
        num  = STEP_NUMS.get(name, "?")
        icon = STEP_ICONS.get(name, "")
        dur  = f"*({step.duration_seconds:.1f}s)*"
        step_placeholders[name].markdown(
            f"{_step_icon(step.status)} **Step {num}: {icon} {name}** {dur}  \n"
            f"&nbsp;&nbsp;&nbsp;&nbsp;{step.message}"
        )
        done_count = sum(
            1 for n, _, _ in PIPELINE_STEPS
            if step_placeholders.get(n) and "✅" in str(step_placeholders[n])
        )
        progress_bar.progress((STEP_NUMS.get(name, 1)) / TOTAL_STEPS)
        substep_ph.empty()
        if step.error:
            st.error(f"**{name} error:** {step.error}")

    def on_substep(step_name: str, msg: str):
        substep_ph.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↳ *{msg}*")

    with st.spinner("Pipeline running — Signal Scoring can take 10+ minutes (jobs + patents + leadership)..."):
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

    # ── Signal score flags ────────────────────────────────────────
    if result.signal_flags:
        st.divider()
        st.markdown("### ⚠️ Signal Score Review")
        st.caption(
            "Flagged by LLM sanity check. Scores are **not** auto-corrected — "
            "review and force re-score if needed."
        )
        SEVERITY_COLOR = {"high": "🔴", "medium": "🟡", "low": "🟠"}
        for flag in result.signal_flags:
            meta = SIGNAL_META.get(flag.category, {})
            sev_icon = SEVERITY_COLOR.get(flag.severity, "⚠️")
            with st.container(border=True):
                col_a, col_b = st.columns([1, 4])
                with col_a:
                    st.metric(meta.get("label", flag.category), f"{flag.score:.1f}/100")
                    st.caption(f"{sev_icon} Severity: **{flag.severity.upper()}**")
                    st.caption(f"Source: {meta.get('source', 'N/A')}")
                with col_b:
                    st.markdown(f"**Why flagged:** {flag.reason}")
                    if flag.raw_value:
                        st.caption(f"Raw evidence: *{flag.raw_value}*")

        if st.button(
            f"🔄 Force Re-score All Signals for {resolved.ticker}",
            key="force_rescore_btn",
        ):
            with st.spinner("Re-scoring all signals..."):
                rescore_result = client.run_pipeline(resolved, force_rescore=True)
            if rescore_result.signal_flags:
                st.warning(f"Still flagged: {len(rescore_result.signal_flags)} signal(s)")
            else:
                st.success("✅ All scores look plausible after re-score")

    # ── Step timing ───────────────────────────────────────────────
    if result.steps:
        with st.expander("Step timing breakdown", expanded=False):
            c0, c1, c2 = st.columns([3, 1, 1])
            c0.markdown("**Step**")
            c1.markdown("**Status**")
            c2.markdown("**Duration**")
            for s in result.steps:
                a, b, c = st.columns([3, 1, 1])
                a.write(s.name)
                b.write(_step_icon(s.status))
                c.write(f"{s.duration_seconds:.1f}s")

    # ── Score summary ─────────────────────────────────────────────
    scoring_step = next(
        (s for s in result.steps if s.name == "Scoring" and s.status == "success"), None
    )
    if scoring_step:
        _render_score_summary(scoring_step.data, resolved)

    # ── Signal scores with source labels ─────────────────────────
    signal_step = next(
        (s for s in result.steps if s.name == "Signal Scoring" and s.status == "success"), None
    )
    if signal_step:
        _render_signal_scores(signal_step.data.get("signal_results", {}))

    # ── Chatbot CTA ───────────────────────────────────────────────
    index_step = next(
        (s for s in result.steps if s.name == "Index Evidence" and s.status == "success"), None
    )
    if index_step:
        indexed = index_step.data.get("indexed_count", 0)
        st.success(f"**{indexed} evidence vectors indexed** — Chatbot is now ready!")
        if st.button(
            f"💬 Start Chatbot for {resolved.ticker}",
            type="primary", use_container_width=True,
        ):
            st.session_state["active_page"] = "chatbot"
            st.session_state["chatbot_ticker"] = resolved.ticker
            st.session_state["chatbot_company"] = resolved.name
            st.rerun()


def _render_score_summary(data: dict, resolved):
    """Score header matching mockup: composite + all 7 dimensions + strongest highlighted."""
    dim_scores = data.get("dimension_scores", data.get("scores", []))
    if not dim_scores:
        return

    scores_dict = (
        {d["dimension"]: d["score"] for d in dim_scores}
        if isinstance(dim_scores, list) else dim_scores
    )
    avg_score = sum(scores_dict.values()) / len(scores_dict) if scores_dict else 0
    best_dim  = max(scores_dict, key=scores_dict.get) if scores_dict else None
    rec       = (
        "PROCEED" if avg_score >= 65
        else "PROCEED WITH CAUTION" if avg_score >= 45
        else "FURTHER DILIGENCE"
    )

    st.markdown("### AI-Readiness Scores")
    st.markdown(f"**{resolved.name} ({resolved.ticker})**")

    # Top 3 summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg AI-Readiness", f"{avg_score:.1f}/100")
    with col2:
        st.metric("Strongest Dimension", DIMENSION_LABELS.get(best_dim, best_dim or "—"))
    with col3:
        st.metric("Recommendation", rec)

    # All dimension scores — strongest highlighted
    st.markdown("#### Dimension Breakdown")
    sorted_dims = sorted(scores_dict.items(), key=lambda x: x[1], reverse=True)
    cols = st.columns(len(sorted_dims))
    for i, (dim, score) in enumerate(sorted_dims):
        with cols[i]:
            label = DIMENSION_LABELS.get(dim, dim.replace("_", " ").title())
            delta = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
            st.metric(label[:14], f"{score:.0f}", delta=delta)
            # Highlight strongest dimension
            if dim == best_dim:
                st.markdown(
                    "<div style='text-align:center;font-size:10px;"
                    "color:#7F77DD;font-weight:600;margin-top:-8px'>★ Strongest</div>",
                    unsafe_allow_html=True,
                )


def _render_signal_scores(signal_results: dict):
    """Signal scores section with data source labels."""
    if not signal_results:
        return

    st.markdown("#### Signal Scores")
    st.caption("Raw signals that feed into dimension scoring")

    cols = st.columns(4)
    for i, (cat, meta) in enumerate(SIGNAL_META.items()):
        info  = signal_results.get(cat, {})
        score = info.get("score")
        src   = info.get("source", "fresh")

        with cols[i]:
            score_str = f"{score:.1f}" if score is not None else "—"
            skipped   = src == "skipped"
            tag       = "↑ HIGH" if (score or 0) >= 70 else "↑ MID" if (score or 0) >= 40 else "↑ LOW"
            st.metric(
                f"{meta['icon']} {meta['label']}",
                score_str,
                delta=f"{tag} {'(cached)' if skipped else ''}",
            )
            st.caption(f"📡 {meta['source']}")
            st.caption(f"_{meta['desc']}_")


def _render_processed_companies():
    st.markdown("### Companies in Platform")
    companies = _load_companies()
    if not companies:
        st.info("No companies yet — enter a company above to get started.")
        return

    c0, c1, c2, c3 = st.columns([2, 1, 1, 1])
    c0.markdown("**Company**")
    c1.markdown("**Ticker**")
    c2.markdown("**Sector**")
    c3.markdown("**Action**")
    st.divider()

    for company in companies[:10]:
        ticker = company.get("ticker", "")
        name   = company.get("name", ticker)
        sector = _get_sector(company)
        r0, r1, r2, r3 = st.columns([2, 1, 1, 1])
        r0.write(name[:30])
        r1.write(ticker)
        r2.write(sector)
        with r3:
            if st.button("💬 Chat", key=f"chat_{ticker}", use_container_width=True):
                st.session_state["active_page"] = "chatbot"
                st.session_state["chatbot_ticker"] = ticker
                st.session_state["chatbot_company"] = name
                st.rerun()