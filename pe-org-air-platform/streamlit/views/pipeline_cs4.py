"""
PE Org-AI-R Platform — CS4 Pipeline View
streamlit/views/pipeline_cs4.py

BUG FIXES (v2):
  - BUG #2 FIXED: _run_single_step now passes force=True to _step_parse and
    _step_chunk when the step was already done (status=="done"). This ensures
    the Re-run button actually re-runs instead of silently skipping.
  - BUG #4 FIXED: _get_completed_steps now fetches /evidence only ONCE and
    reuses the response for steps 4, 5, and 6. Previously hit Snowflake 3×
    redundantly, causing 5-10 second page lag per render.
"""
from __future__ import annotations

import sys
import os
import time
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from utils.company_resolver import resolve_company
from utils.pipeline_client  import PipelineClient, PipelineStepResult

BASE_URL = "http://localhost:8000"

PIPELINE_STEPS = [
    ("Company Setup",     "🏢", "POST /api/v1/companies"),
    ("SEC Filings",       "📄", "POST /api/v1/documents/collect — EDGAR + S3"),
    ("Parse Documents",   "🔍", "POST /api/v1/documents/parse/{ticker}"),
    ("Chunk Documents",   "✂️",  "POST /api/v1/documents/chunk/{ticker}"),
    ("Signal Scoring",    "📡", "POST /api/v1/signals/score/{ticker}/all — sync"),
    ("Glassdoor Culture", "💬", "POST /api/v1/glassdoor-signals/{ticker}"),
    ("Board Governance",  "🏛️", "POST /api/v1/board-governance/analyze/{ticker}"),
    ("Scoring",           "🧮", "POST /api/v1/scoring/{ticker}"),
    ("Index Evidence",    "🗂️", "POST /rag/index/{ticker}?force=true"),
]

STEP_RUNNING_MSGS = [
    "Writing company to Snowflake companies table...",
    "Fetching 10-K, DEF 14A, 8-K from SEC EDGAR...",
    "Extracting text from PDF filings via pdfplumber...",
    "Splitting into overlapping chunks with metadata...",
    "Running technology_hiring · digital_presence · innovation_activity signals...",
    "Scraping Glassdoor reviews and computing sentiment...",
    "Analyzing board composition and proxy governance signals...",
    "Computing 7 dimension scores and composite...",
    "Embedding chunks and uploading to Chroma pe_evidence collection...",
]

STEP_PREREQUISITES = {
    0: [], 1: [0], 2: [0, 1], 3: [0, 1, 2], 4: [0, 1],
    5: [0], 6: [0], 7: [0, 4, 5, 6], 8: [0, 7],
}

TICKER_SECTOR_MAP = {
    "NVDA": "Technology", "NFLX": "Technology", "MSFT": "Technology",
    "GOOGL": "Technology", "GOOG": "Technology", "AAPL": "Technology",
    "META": "Technology", "AMZN": "Technology", "CRM": "Technology",
    "JPM": "Financial Services", "BAC": "Financial Services",
    "GS": "Financial Services", "UNH": "Healthcare", "JNJ": "Healthcare",
}


def _get_sector(company) -> str:
    sector = getattr(company, "sector", None) or (
        company.get("sector") if isinstance(company, dict) else None
    )
    if sector:
        return sector.title()
    ticker = getattr(company, "ticker", "") or (
        company.get("ticker", "") if isinstance(company, dict) else ""
    )
    return TICKER_SECTOR_MAP.get(ticker.upper(), "Unknown")


def _load_companies() -> list:
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=10)
        if r.status_code == 200:
            return r.json().get("items", [])
    except Exception:
        pass
    return []


def _get_completed_steps(ticker: str) -> set[int]:
    """
    Determine which pipeline steps are already complete for a ticker.

    BUG #4 FIX: Previously fetched /api/v1/companies/{ticker}/evidence three
    separate times — once each for steps 4 (signals), 5 (glassdoor), and 6
    (board). Each call opens a new Snowflake connection (~1-2s each), causing
    5-10 seconds of lag every time the pipeline page renders or reruns.

    Fix: fetch evidence ONCE, reuse the response for all three signal checks.

    Steps:
    - Step 0 (company):   GET /api/v1/companies/{ticker}
    - Step 1 (collect):   doc raw_count OR evidence doc_count > 0
    - Step 2 (parse):     parsed_count > 0  via /documents/{ticker}/status
    - Step 3 (chunk):     chunk_count > 0
    - Step 4 (signals):   technology_hiring/digital_presence/etc score present
    - Step 5 (glassdoor): culture_score present
    - Step 6 (board):     board_governance_score present
    - Step 7 (scoring):   dimension scores exist
    - Step 8 (index):     chatbot_ready
    """
    completed = set()

    # ── Step 0 — company exists ───────────────────────────────────────────────
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/{ticker}", timeout=5)
        if r.status_code == 200:
            completed.add(0)
    except Exception:
        pass

    # ── Steps 1 / 2 / 3 — document pipeline status ───────────────────────────
    try:
        r = requests.get(f"{BASE_URL}/api/v1/documents/{ticker}/status", timeout=5)
        if r.status_code == 200:
            ds = r.json()
            if ds.get("raw_count", 0) > 0:
                completed.add(1)        # collect done
            if ds.get("parsed_count", 0) > 0:
                completed.add(2)        # parse done
            if ds.get("chunk_count", 0) > 0:
                completed.add(3)        # chunk done
    except Exception:
        pass

    # ── Steps 1 (fallback) + 4 / 5 / 6 — ONE evidence fetch covers all ────────
    # BUG #4 FIX: fetch /evidence ONCE, reuse for step 1 fallback AND all signal
    # checks (steps 4, 5, 6). Previously called 3× separately = 3 Snowflake
    # connections. Now a single request covers every signal-related check.
    evidence_data: dict = {}
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/{ticker}/evidence", timeout=5)
        if r.status_code == 200:
            evidence_data = r.json()
    except Exception:
        pass

    # Fallback for step 1 if /documents/status endpoint didn't return it
    if 1 not in completed:
        doc_count = (
            evidence_data.get("document_count", 0)
            or evidence_data.get("total_documents", 0)
        )
        if doc_count > 0:
            completed.add(1)

    sig = evidence_data.get("signal_summary", {}) or {}

    # Step 4 — technology signals
    if sig and any(sig.get(k) for k in [
        "technology_hiring_score", "digital_presence_score",
        "innovation_activity_score", "leadership_signals_score",
    ]):
        completed.add(4)

    # Step 5 — glassdoor/culture signal
    if sig and sig.get("culture_score") is not None:
        completed.add(5)

    # Step 6 — board governance signal
    if sig and sig.get("board_governance_score") is not None:
        completed.add(6)

    # ── Step 7 — dimension scores ─────────────────────────────────────────────
    try:
        r = requests.get(f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions", timeout=5)
        if r.status_code == 200 and r.json().get("scores"):
            completed.add(7)
    except Exception:
        pass

    # ── Step 8 — evidence indexed (chatbot ready) ─────────────────────────────
    try:
        client = PipelineClient()
        status = client.get_company_status(ticker)
        if status.get("chatbot_ready"):
            completed.add(8)
    except Exception:
        pass

    return completed


def _step_html(i: int, name: str, icon: str, detail: str,
               status: str, message: str, elapsed: str) -> str:
    circle_cls = {"idle": "sn-idle", "running": "sn-running", "done": "sn-done", "error": "sn-err"}.get(status, "sn-idle")
    circle_lbl = {"idle": str(i + 1), "running": "…", "done": "✓", "error": "!"}.get(status, str(i + 1))
    row_cls    = {"running": "step-row-running", "done": "step-row-done", "error": "step-row-error"}.get(status, "")
    status_lbl = {"idle": "Waiting", "running": "Running...", "done": "Done", "error": "Error"}.get(status, "Waiting")
    status_cls = {"idle": "st-idle", "running": "st-running", "done": "st-done", "error": "st-err"}.get(status, "st-idle")
    msg_html   = f'<div class="step-msg">{message}</div>' if message else ""
    return (
        f'<div class="step-row {row_cls}">'
        f'<div class="step-num {circle_cls}">{circle_lbl}</div>'
        f'<span class="step-icon">{icon}</span>'
        f'<div style="flex:1;min-width:0">'
        f'<div class="step-name">{i + 1}. {name}</div>'
        f'<div class="step-detail">{detail}</div>'
        f'{msg_html}'
        f'</div>'
        f'<span class="step-status-lbl {status_cls}">{status_lbl}</span>'
        f'<span class="step-time">{elapsed}</span>'
        f'</div>'
    )


def _run_single_step(resolved, client: PipelineClient, idx: int, name: str, ticker: str, already_done: bool = False):
    """
    Run a single pipeline step from the individual step Re-run buttons.

    BUG #2 FIX: Pass force=True to _step_parse and _step_chunk when the step
    was already done (already_done=True). Without force=True, the skip conditions
    inside those methods would fire immediately and silently do nothing, making
    the Re-run button appear broken.

    already_done is True when step_states[idx]["status"] == "done", meaning the
    step completed successfully in a prior run and the user is explicitly
    requesting a re-run.
    """
    ph = st.empty()
    ph.info(f"🔄 Running **Step {idx + 1}: {name}**...")
    start = time.time()
    try:
        if idx == 0:
            result = client._step_create_company(resolved)
        elif idx == 1:
            result = client._step_collect_sec(ticker, resolved.cik)
        elif idx == 2:
            ds = client._get_doc_status(ticker)
            # BUG #2 FIX: force=True when re-running an already-completed parse step.
            result = client._step_parse(ticker, doc_status=ds, force=already_done)
        elif idx == 3:
            ds = client._get_doc_status(ticker)
            # BUG #2 FIX: force=True when re-running an already-completed chunk step.
            result = client._step_chunk(ticker, doc_status=ds, force=already_done)
        elif idx == 4:
            website = getattr(resolved, "website", None)
            result = client._step_signal_scoring(ticker, company_name=resolved.name, website=website)
        elif idx == 5:
            result = client._step_glassdoor(ticker)
        elif idx == 6:
            result = client._step_board_governance(ticker)
        elif idx == 7:
            result = client._step_score(ticker)
        elif idx == 8:
            result = client._step_index(ticker, force=True)
        else:
            result = None

        elapsed = time.time() - start
        if result and result.status == "success":
            ph.success(f"✅ **Step {idx + 1}: {name}** ({elapsed:.1f}s) — {result.message}")
        elif result and result.status == "skipped":
            ph.info(f"⏭️ **Step {idx + 1}: {name}** ({elapsed:.1f}s) — {result.message}")
        elif result and result.status == "error":
            ph.error(f"❌ **Step {idx + 1}: {name}** — {result.error}")
        else:
            ph.warning(f"⚠️ **Step {idx + 1}: {name}** — no result returned")
    except Exception as e:
        ph.error(f"❌ **Step {idx + 1}: {name}** — {e}")


# ── Main render ───────────────────────────────────────────────────────────────

def render_pipeline_page():

    st.markdown(
        '<div class="step-name" style="font-size:15px;margin-bottom:3px">Pipeline builder</div>'
        '<div class="step-detail" style="font-size:12px;opacity:0.6">Collect, parse, chunk, score evidence and index evidence for a company</div>',
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Input row ─────────────────────────────────────────────────
    inp_col, btn_col = st.columns([5, 1])
    with inp_col:
        company_input = st.text_input(
            label="Company ticker, name, or CIK",
            placeholder="Enter a company ticker, name, or CIK...",
            key="pipe_input",
            label_visibility="collapsed",
        )
    with btn_col:
        fetch_clicked = st.button("Fetch company details", use_container_width=True, key="fetch_btn")

    # ── Resolve ───────────────────────────────────────────────────
    resolved = st.session_state.get("resolved_company")

    if company_input and fetch_clicked:
        with st.spinner("Resolving via Yahoo Finance + SEC EDGAR..."):
            try:
                resolved = resolve_company(company_input)
                st.session_state["resolved_company"]  = resolved
                st.session_state["company_confirmed"] = False
            except Exception as e:
                st.error(f"Could not resolve: {e}")
                resolved = None

    # ── Company confirm card ──────────────────────────────────────
    if resolved:
        ticker    = resolved.ticker
        name      = resolved.name
        sector    = _get_sector(resolved)
        cik       = resolved.cik or "N/A"
        rev       = resolved.revenue_millions
        emp       = resolved.employee_count
        initials  = ticker[:2].upper()
        confirmed = st.session_state.get("company_confirmed", False)

        rev_str  = f"${rev:,.0f}M" if rev else "N/A"
        emp_str  = f"{emp:,}" if emp else "N/A"
        meta_str = f"{ticker} · {sector} · Revenue {rev_str} · Employees {emp_str} · CIK {cik}"
        card_cls = "co-confirm-card confirmed" if confirmed else "co-confirm-card"

        st.markdown(
            f'<div class="{card_cls}">'
            f'<div class="co-logo">{initials}</div>'
            f'<div style="flex:1">'
            f'<div style="font-size:18px;font-weight:700;margin-bottom:5px">{name}</div>'
            f'<div style="font-size:14px;opacity:0.7;line-height:1.8">{meta_str}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if not confirmed:
            c1, c2, _ = st.columns([1, 1, 4])
            with c1:
                if st.button("Looks good ✓", key="confirm_yes", type="primary"):
                    st.session_state["company_confirmed"] = True
                    st.rerun()
            with c2:
                if st.button("Not right", key="confirm_no"):
                    st.session_state["resolved_company"]  = None
                    st.session_state["company_confirmed"] = False
                    st.rerun()

        # ── Run full pipeline button ──────────────────────────────
        if st.button(
            "Run full pipeline",
            type="primary",
            disabled=not confirmed,
            use_container_width=True,
            key="run_full_btn",
        ):
            _run_full_pipeline(resolved)
            return

        # ── Step rows ─────────────────────────────────────────────
        client    = PipelineClient()
        completed = _get_completed_steps(ticker) if confirmed else set()

        step_states_key = f"step_states_{ticker}"
        if step_states_key not in st.session_state:
            st.session_state[step_states_key] = {
                i: {"status": "idle", "msg": "", "elapsed": ""}
                for i in range(len(PIPELINE_STEPS))
            }
        step_states = st.session_state[step_states_key]
        for i in completed:
            if step_states[i]["status"] == "idle":
                step_states[i]["status"] = "done"

        st.markdown("<br>", unsafe_allow_html=True)

        for i, (step_name, icon, detail) in enumerate(PIPELINE_STEPS):
            prereqs  = STEP_PREREQUISITES[i]
            missing  = [p for p in prereqs if p not in completed]
            disabled = (not confirmed) or bool(missing)
            state    = step_states[i]
            btn_lbl  = "Re-run" if state["status"] == "done" else "▶ Run"
            # BUG #2 FIX: track whether this step is already done so _run_single_step
            # can pass force=True to parse/chunk, bypassing the skip logic.
            already_done = (state["status"] == "done")

            row_col, btn_col = st.columns([10, 1])
            with row_col:
                st.markdown(
                    _step_html(i, step_name, icon, detail,
                               state["status"], state["msg"], state["elapsed"]),
                    unsafe_allow_html=True,
                )
            with btn_col:
                st.markdown("<div style='padding-top:12px'>", unsafe_allow_html=True)
                if st.button(btn_lbl, key=f"run_step_{i}_{ticker}",
                             disabled=disabled, use_container_width=True):
                    # BUG #2 FIX: pass already_done so force=True is set for re-runs
                    _run_single_step(resolved, client, i, step_name, ticker, already_done=already_done)
                    st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)

        # ── Progress bar ──────────────────────────────────────────
        done_count = sum(1 for s in step_states.values() if s["status"] == "done")
        pct        = done_count / len(PIPELINE_STEPS)
        prog_lbl   = f"{done_count} of {len(PIPELINE_STEPS)} steps complete" if done_count else "Ready to run"
        st.markdown(
            f'<div class="prog-label">{prog_lbl}</div>'
            f'<div class="prog-bar"><div class="prog-fill" style="width:{pct*100:.0f}%"></div></div>',
            unsafe_allow_html=True,
        )

    # ── Companies table ───────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.divider()
    _render_companies_table()


def _render_companies_table():
    st.markdown("**Companies in Platform**")
    companies = _load_companies()
    if not companies:
        st.info("No companies yet — enter a company above to get started.")
        return

    h0, h1, h2, h3 = st.columns([2, 1, 1, 1])
    h0.caption("Company"); h1.caption("Ticker"); h2.caption("Sector"); h3.caption("Action")

    for co in companies[:10]:
        ticker = co.get("ticker", "")
        name   = co.get("name", ticker)
        sector = _get_sector(co)
        r0, r1, r2, r3 = st.columns([2, 1, 1, 1])
        r0.write(name[:28])
        r1.write(ticker)
        r2.write(sector)
        with r3:
            if st.button("💬 Chat", key=f"co_chat_{ticker}", use_container_width=True):
                st.session_state["active_page"]     = "chatbot"
                st.session_state["chatbot_ticker"]  = ticker
                st.session_state["chatbot_company"] = name
                st.rerun()


def _run_full_pipeline(resolved):
    ticker = resolved.ticker
    client = PipelineClient()

    st.markdown("### Pipeline Progress")

    step_phs = []
    for i, (name, icon, detail) in enumerate(PIPELINE_STEPS):
        ph = st.empty()
        ph.markdown(_step_html(i, name, icon, detail, "idle", "", ""), unsafe_allow_html=True)
        step_phs.append(ph)

    prog_ph   = st.empty()
    status_ph = st.empty()
    prog_ph.markdown(
        '<div class="prog-label">Ready to run</div>'
        '<div class="prog-bar"><div class="prog-fill" style="width:0%"></div></div>',
        unsafe_allow_html=True,
    )

    def on_step_start(step_name: str):
        idx = next((i for i, (n, _, _) in enumerate(PIPELINE_STEPS) if n == step_name), None)
        if idx is not None:
            step_phs[idx].markdown(
                _step_html(idx, step_name, PIPELINE_STEPS[idx][1], PIPELINE_STEPS[idx][2],
                           "running", STEP_RUNNING_MSGS[idx], ""),
                unsafe_allow_html=True,
            )
            status_ph.caption(f"⏳ Running: {step_name}...")

    def on_step_complete(step: PipelineStepResult):
        idx = next((i for i, (n, _, _) in enumerate(PIPELINE_STEPS) if n == step.name), None)
        if idx is not None:
            dur = f"{step.duration_seconds:.1f}s"
            step_phs[idx].markdown(
                _step_html(idx, step.name, PIPELINE_STEPS[idx][1], PIPELINE_STEPS[idx][2],
                           step.status, step.message, dur),
                unsafe_allow_html=True,
            )
            pct = (idx + 1) / len(PIPELINE_STEPS)
            prog_ph.markdown(
                f'<div class="prog-label">{idx+1} of {len(PIPELINE_STEPS)} steps complete</div>'
                f'<div class="prog-bar"><div class="prog-fill" style="width:{pct*100:.0f}%"></div></div>',
                unsafe_allow_html=True,
            )

    def on_substep(step_name: str, msg: str):
        status_ph.caption(f"↳ {msg}")

    # Track step results to warn on parse/chunk failure
    _step_results: dict = {}
    _orig_complete = on_step_complete

    def on_step_complete_with_guard(step: PipelineStepResult):
        _step_results[step.name] = step
        _orig_complete(step)
        # If chunk fails, warn loudly — signals will run but have no SEC evidence
        if step.name == "Chunk Documents" and step.status == "error":
            status_ph.warning(
                "⚠️ Chunk failed — no parsed docs found. "
                "Re-run **Parse Documents** (Step 3) then **Chunk Documents** (Step 4) "
                "before re-running Signal Scoring and Index."
            )

    pipeline_start = time.time()
    with st.spinner("Pipeline running — Signal Scoring can take 10+ minutes..."):
        result = client.run_pipeline(
            resolved,
            on_step_start=on_step_start,
            on_step_complete=on_step_complete_with_guard,
            on_substep=on_substep,
        )

    total_elapsed = time.time() - pipeline_start
    mins, secs = divmod(int(total_elapsed), 60)
    time_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    status_ph.empty()
    prog_ph.markdown(
        f'<div class="prog-label">{len(PIPELINE_STEPS)} of {len(PIPELINE_STEPS)} steps complete</div>'
        f'<div class="prog-bar"><div class="prog-fill" style="width:100%"></div></div>',
        unsafe_allow_html=True,
    )

    st.divider()
    if result.overall_status == "success":
        st.balloons()
        st.success(f"✅ Pipeline completed in **{time_str}**")
    elif result.overall_status == "partial":
        st.warning(f"⚠️ Pipeline completed with some issues in **{time_str}**")
    else:
        st.error(f"❌ Pipeline failed after **{time_str}**")

    index_step = next(
        (s for s in (result.steps or []) if s.name == "Index Evidence" and s.status == "success"),
        None,
    )
    if index_step:
        indexed = index_step.data.get("indexed_count", 0)
        st.success(f"**{indexed} evidence vectors indexed** — Chatbot is now ready!")
        if st.button(f"💬 Start Chatbot for {ticker}", type="primary",
                     use_container_width=True, key="go_chat_post_pipe"):
            st.session_state["active_page"]     = "chatbot"
            st.session_state["chatbot_ticker"]  = ticker
            st.session_state["chatbot_company"] = resolved.name
            st.rerun()

    if result.steps:
        with st.expander("Step timing breakdown", expanded=False):
            for s in result.steps:
                icon = {"success": "✅", "error": "❌", "skipped": "⏭️"}.get(s.status, "⬜")
                st.markdown(f"{icon} **{s.name}** — {s.duration_seconds:.1f}s"
                            + (f"  \n`{s.error}`" if s.error else ""))