"""
PE Org-AI-R Platform — CS4 Pipeline Trigger View
streamlit/views/pipeline_cs4.py

Allows users to enter a ticker, company name, or CIK,
auto-resolves company metadata, and runs the full pipeline
with real-time step-by-step progress updates.
"""
from __future__ import annotations

import sys
import os
import time
import streamlit as st

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from utils.company_resolver import resolve_company, format_resolution_preview
from utils.pipeline_client import PipelineClient, PipelineStepResult


# ── Constants ────────────────────────────────────────────────────────────────

DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent",
    "leadership",
    "use_case_portfolio",
    "culture",
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

STEP_ICONS = {
    "running": "⏳",
    "success": "✅",
    "skipped": "⏭️",
    "error": "❌",
    "pending": "⬜",
}

RECOMMENDATION_COLORS = {
    "PROCEED": "🟢",
    "PROCEED WITH CAUTION": "🟡",
    "FURTHER DILIGENCE": "🔴",
}


def render_pipeline_page():
    """Main pipeline trigger page."""

    # ── Header ────────────────────────────────────────────────────
    st.markdown("## 🏢 Company Pipeline")
    st.markdown(
        "Enter a company ticker, name, or CIK to run the full "
        "AI-readiness assessment pipeline."
    )
    st.divider()

    # ── Input Section ─────────────────────────────────────────────
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
        resolve_btn = st.button("🔍 Resolve", use_container_width=True)

    # ── Resolution Preview ────────────────────────────────────────
    resolved = None

    if company_input and (resolve_btn or st.session_state.get("auto_resolved")):
        with st.spinner("Resolving company..."):
            try:
                resolved = resolve_company(company_input)
                st.session_state["resolved_company"] = resolved
                st.session_state["auto_resolved"] = True
            except Exception as e:
                st.error(f"Could not resolve company: {e}")

    # Use previously resolved company
    if not resolved and st.session_state.get("resolved_company"):
        prev = st.session_state["resolved_company"]
        if prev.ticker and company_input and (
            company_input.upper() == prev.ticker or
            company_input.lower() in prev.name.lower()
        ):
            resolved = prev

    if resolved:
        # Show resolution card
        with st.container():
            st.markdown("#### Resolved Company")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Company", resolved.name)
            with col2:
                st.metric("Ticker", resolved.ticker)
            with col3:
                st.metric("Sector", (resolved.sector or "Unknown").title())
            with col4:
                st.metric(
                    "Revenue",
                    f"${resolved.revenue_millions:,.0f}M" if resolved.revenue_millions else "N/A"
                )

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Employees",
                    f"{resolved.employee_count:,}" if resolved.employee_count else "N/A"
                )
            with col2:
                st.metric(
                    "Market Cap Percentile",
                    f"{resolved.market_cap_percentile:.0%}" if resolved.market_cap_percentile else "N/A"
                )
            with col3:
                st.metric("CIK", resolved.cik or "N/A")

            if resolved.warnings:
                for w in resolved.warnings:
                    st.warning(w)

        st.divider()

        # ── Check existing status ─────────────────────────────────
        client = PipelineClient()
        status = client.get_company_status(resolved.ticker)

        if status["chatbot_ready"]:
            st.success(
                f"✅ **{resolved.name}** is already processed and ready for Q&A. "
                f"Org-AI-R Score: **{status['org_air_score']}/100** | "
                f"Evidence: **{status['indexed_documents']} documents**"
            )
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Re-run Pipeline", use_container_width=True):
                    st.session_state["run_pipeline"] = True
            with col2:
                if st.button("💬 Go to Chatbot", use_container_width=True, type="primary"):
                    st.session_state["active_page"] = "chatbot"
                    st.session_state["chatbot_ticker"] = resolved.ticker
                    st.session_state["chatbot_company"] = resolved.name
                    st.rerun()
        else:
            st.info(
                f"**{resolved.name}** has not been processed yet. "
                "Run the pipeline to enable the AI-readiness assessment and chatbot."
            )

        # ── Pipeline Run Button ───────────────────────────────────
        if not status["chatbot_ready"] or st.session_state.get("run_pipeline"):
            if st.button(
                f"▶ Run Full Pipeline for {resolved.ticker}",
                type="primary",
                use_container_width=True,
            ):
                st.session_state["run_pipeline"] = False
                _run_pipeline(resolved, client)

    # ── Previously processed companies ───────────────────────────
    st.divider()
    _render_processed_companies()


def _run_pipeline(resolved, client: PipelineClient):
    """Execute pipeline with real-time progress UI."""

    st.markdown("### 🔄 Pipeline Progress")

    # Step status placeholders
    step_names = [
        "Company Setup",
        "SEC Filings",
        "External Signals",
        "Scoring",
        "Index Evidence",
    ]

    step_placeholders = []
    for i, name in enumerate(step_names):
        placeholder = st.empty()
        placeholder.markdown(
            f"{STEP_ICONS['pending']} **Step {i+1}: {name}** — waiting..."
        )
        step_placeholders.append(placeholder)

    progress_bar = st.progress(0)
    status_text = st.empty()
    results_placeholder = st.empty()

    completed_steps = []

    def on_step_start(step_name: str):
        idx = step_names.index(step_name) if step_name in step_names else 0
        step_placeholders[idx].markdown(
            f"{STEP_ICONS['running']} **Step {idx+1}: {step_name}** — running..."
        )
        status_text.markdown(f"*Running: {step_name}...*")

    def on_step_complete(step: PipelineStepResult):
        idx = step.step - 1
        icon = STEP_ICONS.get(step.status, "⬜")
        duration = f"({step.duration_seconds:.1f}s)"
        step_placeholders[idx].markdown(
            f"{icon} **Step {step.step}: {step.name}** {duration}\n\n"
            f"&nbsp;&nbsp;&nbsp;&nbsp;{step.message}"
        )
        progress_bar.progress((step.step) / len(step_names))
        completed_steps.append(step)

        if step.error:
            st.error(f"**{step.name} failed:** {step.error}")

    # Run pipeline
    start_time = time.time()
    with st.spinner("Pipeline running..."):
        result = client.run_pipeline(
            resolved,
            on_step_start=on_step_start,
            on_step_complete=on_step_complete,
        )

    total_time = time.time() - start_time
    progress_bar.progress(1.0)
    status_text.empty()

    # ── Results Summary ───────────────────────────────────────────
    st.divider()

    if result.overall_status == "success":
        st.balloons()
        st.success(f"✅ Pipeline completed in {total_time:.0f}s")
    elif result.overall_status == "partial":
        st.warning(f"⚠️ Pipeline completed with some issues in {total_time:.0f}s")
    else:
        st.error(f"❌ Pipeline failed after {total_time:.0f}s")

    # Show score summary if available
    scoring_step = next((s for s in result.steps if s.name == "Scoring" and s.status == "success"), None)
    if scoring_step:
        _render_score_summary(scoring_step.data, resolved)

    # Chatbot CTA
    index_step = next((s for s in result.steps if s.name == "Index Evidence" and s.status == "success"), None)
    if index_step:
        indexed = index_step.data.get("indexed_count", 0)
        st.success(f"💬 **{indexed} evidence pieces indexed** — Chatbot is now ready!")

        if st.button("💬 Start Chatbot for " + resolved.ticker, type="primary", use_container_width=True):
            st.session_state["active_page"] = "chatbot"
            st.session_state["chatbot_ticker"] = resolved.ticker
            st.session_state["chatbot_company"] = resolved.name
            st.rerun()

    # Store result for chatbot
    st.session_state["last_pipeline_ticker"] = resolved.ticker
    st.session_state["last_pipeline_company"] = resolved.name
    st.session_state["last_pipeline_score"] = result.org_air_score


def _render_score_summary(data: dict, resolved):
    """Render dimension scores after pipeline completes."""
    dim_scores = data.get("dimension_scores", data.get("scores", []))
    if not dim_scores:
        return

    st.markdown("### 📊 AI-Readiness Scores")
    st.markdown(f"**{resolved.name} ({resolved.ticker})**")

    # Calculate average
    if isinstance(dim_scores, list):
        scores_dict = {d["dimension"]: d["score"] for d in dim_scores}
    else:
        scores_dict = dim_scores

    avg_score = sum(scores_dict.values()) / len(scores_dict) if scores_dict else 0

    # Overall score metric
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg AI-Readiness", f"{avg_score:.1f}/100")
    with col2:
        level = "Excellent" if avg_score >= 80 else "Good" if avg_score >= 60 else "Adequate" if avg_score >= 40 else "Developing"
        st.metric("Overall Level", level)
    with col3:
        rec = "PROCEED" if avg_score >= 65 else "PROCEED WITH CAUTION" if avg_score >= 45 else "FURTHER DILIGENCE"
        icon = RECOMMENDATION_COLORS.get(rec, "⚪")
        st.metric("Recommendation", f"{icon} {rec}")

    # Dimension scores bar chart
    st.markdown("#### Dimension Breakdown")
    cols = st.columns(len(scores_dict))
    for i, (dim, score) in enumerate(sorted(scores_dict.items(), key=lambda x: x[1], reverse=True)):
        with cols[i]:
            label = DIMENSION_LABELS.get(dim, dim.replace("_", " ").title())
            color = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
            st.metric(
                label[:12],
                f"{color} {score:.0f}",
            )


def _render_processed_companies():
    """Show companies already in the platform."""
    st.markdown("### 🏦 Companies in Platform")

    client = PipelineClient()
    try:
        import requests
        resp = requests.get("http://localhost:8000/api/v1/companies/all", timeout=5)
        if resp.status_code != 200:
            st.info("Could not load companies list.")
            return

        companies = resp.json().get("items", [])
        if not companies:
            st.info("No companies in platform yet. Enter a company above to get started.")
            return

        # Show as a clean table
        cols = st.columns([2, 1, 1, 1, 1])
        cols[0].markdown("**Company**")
        cols[1].markdown("**Ticker**")
        cols[2].markdown("**Sector**")
        cols[3].markdown("**Score**")
        cols[4].markdown("**Action**")

        st.divider()

        for company in companies[:10]:
            ticker = company.get("ticker", "")
            name = company.get("name", "")
            sector = (company.get("sector") or "Unknown").title()

            # Get score
            score_str = "—"
            try:
                score_resp = requests.get(
                    f"http://localhost:8000/api/v1/scoring/{ticker}/dimensions",
                    timeout=3,
                )
                if score_resp.status_code == 200:
                    dim_scores = score_resp.json().get("scores", [])
                    if dim_scores:
                        avg = sum(d["score"] for d in dim_scores) / len(dim_scores)
                        score_str = f"{avg:.0f}/100"
            except Exception:
                pass

            cols = st.columns([2, 1, 1, 1, 1])
            cols[0].write(name[:30])
            cols[1].write(ticker)
            cols[2].write(sector[:15])
            cols[3].write(score_str)
            with cols[4]:
                if st.button("Chat", key=f"chat_{ticker}", use_container_width=True):
                    st.session_state["active_page"] = "chatbot"
                    st.session_state["chatbot_ticker"] = ticker
                    st.session_state["chatbot_company"] = name
                    st.rerun()

    except Exception as e:
        st.info(f"Could not load companies: {e}")