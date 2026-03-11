"""
PE Org-AI-R Platform — CS4 Company Q&A Chatbot View
streamlit/views/chatbot_cs4.py

IC-quality chatbot that answers questions about company AI-readiness
using the CS4 RAG pipeline with cited evidence.
"""
from __future__ import annotations

import sys
import os
import requests
import streamlit as st
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

BASE_URL = "http://localhost:8000"

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

SUGGESTED_QUESTIONS = [
    "What is {ticker}'s overall AI readiness?",
    "Why did {ticker} score on Data Infrastructure?",
    "What are {ticker}'s AI governance practices?",
    "What AI use cases does {ticker} have in production?",
    "What are {ticker}'s key AI talent capabilities?",
    "What are the main gaps in {ticker}'s AI strategy?",
    "How does {ticker}'s technology stack support AI?",
    "What is the investment recommendation for {ticker}?",
]

SOURCE_TYPE_LABELS = {
    "sec_10k_item_1": "📄 10-K Business Description",
    "sec_10k_item_1a": "⚠️ 10-K Risk Factors",
    "sec_10k_item_7": "📈 10-K MD&A",
    "job_posting_linkedin": "👔 LinkedIn Job Posting",
    "job_posting_indeed": "👔 Indeed Job Posting",
    "patent_uspto": "🔬 USPTO Patent",
    "glassdoor_review": "⭐ Glassdoor Review",
    "board_proxy_def14a": "🏛️ Board Proxy (DEF 14A)",
    "digital_presence": "🌐 Digital Presence",
    "technology_hiring": "💻 Technology Hiring",
    "innovation_activity": "💡 Innovation Activity",
    "culture_signals": "🎭 Culture Signals",
    "leadership_signals": "👑 Leadership Signals",
    "governance_signals": "⚖️ Governance Signals",
}


def render_chatbot_page():
    """Main chatbot page."""

    # ── Header ────────────────────────────────────────────────────
    st.markdown("## 💬 Company Q&A")
    st.markdown(
        "Ask questions about a company's AI-readiness. "
        "Answers are grounded in real evidence from SEC filings and signals."
    )
    st.divider()

    # ── Company Selection ─────────────────────────────────────────
    ticker = st.session_state.get("chatbot_ticker", "")
    company_name = st.session_state.get("chatbot_company", "")

    # Get available companies
    available_companies = _get_available_companies()

    if available_companies:
        options = [f"{c['ticker']} — {c['name']}" for c in available_companies]
        default_idx = 0
        if ticker:
            matching = [i for i, c in enumerate(available_companies) if c["ticker"] == ticker]
            if matching:
                default_idx = matching[0]

        selected = st.selectbox(
            "Select Company",
            options=options,
            index=default_idx,
            key="chatbot_company_select",
        )
        selected_ticker = selected.split(" — ")[0].strip()
        selected_name = selected.split(" — ")[1].strip() if " — " in selected else selected_ticker

        if selected_ticker != ticker:
            ticker = selected_ticker
            company_name = selected_name
            st.session_state["chatbot_ticker"] = ticker
            st.session_state["chatbot_company"] = company_name
            # Clear chat history when company changes
            st.session_state[f"chat_history_{ticker}"] = []
    else:
        st.warning(
            "No companies have been processed yet. "
            "Go to the **Pipeline** page to run the assessment first."
        )
        if st.button("▶ Go to Pipeline", type="primary"):
            st.session_state["active_page"] = "pipeline"
            st.rerun()
        return

    if not ticker:
        return

    # ── Company Score Summary ─────────────────────────────────────
    _render_company_summary(ticker, company_name)
    st.divider()

    # ── Two column layout: Chat + IC Justification ────────────────
    chat_col, justify_col = st.columns([3, 2])

    with chat_col:
        _render_chat_interface(ticker, company_name)

    with justify_col:
        _render_justification_panel(ticker)


def _get_available_companies() -> list:
    """Get companies that are ready for chatbot (indexed in ChromaDB)."""
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=5)
        if resp.status_code != 200:
            return []
        companies = resp.json().get("items", [])

        # Check which have scores
        ready = []
        for c in companies:
            ticker = c.get("ticker", "")
            if not ticker:
                continue
            try:
                score_resp = requests.get(
                    f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions",
                    timeout=2,
                )
                if score_resp.status_code == 200:
                    scores = score_resp.json().get("scores", [])
                    if scores:
                        ready.append(c)
            except Exception:
                pass
        return ready
    except Exception:
        return []


def _render_company_summary(ticker: str, company_name: str):
    """Render compact score summary for selected company."""
    try:
        resp = requests.get(
            f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions",
            timeout=5,
        )
        if resp.status_code != 200:
            return

        data = resp.json()
        dim_scores = data.get("scores", [])
        if not dim_scores:
            return

        scores_dict = {d["dimension"]: d["score"] for d in dim_scores}
        avg_score = sum(scores_dict.values()) / len(scores_dict)
        rec = "PROCEED" if avg_score >= 65 else "PROCEED WITH CAUTION" if avg_score >= 45 else "FURTHER DILIGENCE"
        rec_icons = {"PROCEED": "🟢", "PROCEED WITH CAUTION": "🟡", "FURTHER DILIGENCE": "🔴"}

        st.markdown(f"### {company_name} ({ticker})")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg AI-Readiness Score", f"{avg_score:.1f}/100")
        with col2:
            best_dim = max(scores_dict, key=scores_dict.get)
            st.metric("Strongest Dimension", DIMENSION_LABELS.get(best_dim, best_dim))
        with col3:
            st.metric("Recommendation", f"{rec_icons.get(rec, '⚪')} {rec}")

        # Compact dimension scores
        cols = st.columns(7)
        for i, dim in enumerate(DIMENSIONS):
            score = scores_dict.get(dim, 0)
            color = "🟢" if score >= 70 else "🟡" if score >= 50 else "🔴"
            cols[i].metric(
                DIMENSION_LABELS.get(dim, dim)[:8],
                f"{color} {score:.0f}",
            )

    except Exception:
        st.markdown(f"### {company_name} ({ticker})")


def _render_chat_interface(ticker: str, company_name: str):
    """Render the chat Q&A interface."""
    st.markdown("#### 💬 Ask a Question")

    # Initialize chat history
    history_key = f"chat_history_{ticker}"
    if history_key not in st.session_state:
        st.session_state[history_key] = []

    chat_history = st.session_state[history_key]

    # Suggested questions
    with st.expander("💡 Suggested Questions", expanded=len(chat_history) == 0):
        for q_template in SUGGESTED_QUESTIONS[:4]:
            q = q_template.format(ticker=ticker)
            if st.button(q, key=f"suggested_{q[:30]}", use_container_width=True):
                st.session_state["pending_question"] = q

    # Chat history display
    chat_container = st.container()
    with chat_container:
        for msg in chat_history:
            if msg["role"] == "user":
                with st.chat_message("user"):
                    st.write(msg["content"])
            else:
                with st.chat_message("assistant", avatar="🏦"):
                    st.markdown(msg["content"])
                    # Show evidence if available
                    if msg.get("evidence"):
                        _render_evidence_citations(msg["evidence"])

    # Question input
    pending = st.session_state.pop("pending_question", None)
    question = st.chat_input(
        f"Ask about {company_name}...",
        key=f"chat_input_{ticker}",
    )

    # Use pending question from suggested questions
    if pending and not question:
        question = pending

    if question:
        # Add user message
        chat_history.append({"role": "user", "content": question})

        with st.chat_message("user"):
            st.write(question)

        # Get answer
        with st.chat_message("assistant", avatar="🏦"):
            with st.spinner("Analyzing evidence..."):
                answer, evidence = _get_chatbot_answer(ticker, question)

            st.markdown(answer)
            if evidence:
                _render_evidence_citations(evidence)

        # Add assistant message
        chat_history.append({
            "role": "assistant",
            "content": answer,
            "evidence": evidence,
        })

        st.session_state[history_key] = chat_history

    # Clear chat button
    if chat_history:
        if st.button("🗑️ Clear Chat", key=f"clear_{ticker}"):
            st.session_state[history_key] = []
            st.rerun()


def _get_chatbot_answer(ticker: str, question: str) -> tuple[str, list]:
    """Get answer from RAG chatbot endpoint."""
    try:
        resp = requests.get(
            f"{BASE_URL}/rag/chatbot/{ticker}",
            params={"question": question, "use_hyde": False},
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("answer", "No answer generated."), data.get("evidence", [])
        else:
            return f"Error getting answer (HTTP {resp.status_code}). Please try again.", []
    except Exception as e:
        return f"Could not connect to the API: {e}", []


def _render_evidence_citations(evidence: list):
    """Render cited evidence in a collapsible section."""
    if not evidence:
        return

    with st.expander(f"📎 {len(evidence)} Evidence Sources", expanded=False):
        for i, ev in enumerate(evidence, 1):
            source_type = ev.get("source_type", "unknown")
            source_label = SOURCE_TYPE_LABELS.get(source_type, f"📄 {source_type}")
            score = ev.get("score", 0)
            dimension = ev.get("dimension", "")
            dim_label = DIMENSION_LABELS.get(dimension, dimension.replace("_", " ").title())

            st.markdown(
                f"**[{i}] {source_label}** | "
                f"Relevance: {score:.2f} | "
                f"Dimension: {dim_label}"
            )
            st.caption(ev.get("content", "")[:300] + "...")
            if i < len(evidence):
                st.divider()


def _render_justification_panel(ticker: str):
    """Render IC-style justification for a selected dimension."""
    st.markdown("#### 📋 Score Justification")
    st.caption("Select a dimension to see IC-ready evidence justification")

    selected_dim = st.selectbox(
        "Dimension",
        options=DIMENSIONS,
        format_func=lambda x: DIMENSION_LABELS.get(x, x),
        key=f"justify_dim_{ticker}",
    )

    if st.button(
        f"Generate Justification",
        key=f"justify_btn_{ticker}",
        type="primary",
        use_container_width=True,
    ):
        _fetch_and_render_justification(ticker, selected_dim)

    # Show cached justification
    cache_key = f"justification_{ticker}_{selected_dim}"
    if cache_key in st.session_state:
        _display_justification(st.session_state[cache_key])


def _fetch_and_render_justification(ticker: str, dimension: str):
    """Fetch and display justification from API."""
    cache_key = f"justification_{ticker}_{dimension}"

    with st.spinner(f"Generating justification for {DIMENSION_LABELS.get(dimension, dimension)}..."):
        try:
            resp = requests.get(
                f"{BASE_URL}/rag/justify/{ticker}/{dimension}",
                timeout=120,
            )
            if resp.status_code == 200:
                data = resp.json()
                st.session_state[cache_key] = data
                _display_justification(data)
            else:
                st.error(f"Could not generate justification: HTTP {resp.status_code}")
        except Exception as e:
            st.error(f"Error: {e}")


def _display_justification(data: dict):
    """Display a justification result in IC memo style."""
    dim = data.get("dimension", "")
    score = data.get("score", 0)
    level = data.get("level", 0)
    level_name = data.get("level_name", "")
    strength = data.get("evidence_strength", "weak")
    summary = data.get("generated_summary", "")
    evidence = data.get("supporting_evidence", [])
    gaps = data.get("gaps_identified", [])

    # Score header
    strength_colors = {"strong": "🟢", "moderate": "🟡", "weak": "🔴"}
    strength_icon = strength_colors.get(strength, "⚪")

    st.markdown(
        f"**{DIMENSION_LABELS.get(dim, dim)}**: "
        f"**{score:.0f}/100** (Level {level} — {level_name})"
    )
    st.caption(f"Evidence Strength: {strength_icon} {strength.title()}")

    # Generated summary
    if summary:
        st.info(summary)

    # Supporting evidence
    if evidence:
        st.markdown(f"**Supporting Evidence** ({len(evidence)} pieces)")
        for i, ev in enumerate(evidence[:3], 1):
            source_type = ev.get("source_type", "")
            source_label = SOURCE_TYPE_LABELS.get(source_type, f"📄 {source_type}")
            confidence = ev.get("confidence", 0)
            keywords = ev.get("matched_keywords", [])

            st.markdown(f"*{i}. {source_label}* (conf: {confidence:.2f})")
            if keywords:
                st.caption(f"Keywords matched: {', '.join(keywords[:5])}")
            st.caption(ev.get("content", "")[:200] + "...")

    # Gaps
    if gaps:
        st.markdown("**Gaps to Next Level**")
        for gap in gaps[:3]:
            st.caption(f"• {gap}")