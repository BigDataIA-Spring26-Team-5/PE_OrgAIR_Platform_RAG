"""
PE Org-AI-R Platform -- CS4 Company Q&A Chatbot View
streamlit/views/chatbot_cs4.py
"""
from __future__ import annotations

import sys
import os
import requests
import streamlit as st

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
    "What are {ticker}'s key AI strengths?",
    "What are the main gaps in {ticker}'s AI strategy?",
    "What AI use cases does {ticker} have in production?",
    "What are {ticker}'s AI governance practices?",
    "How does {ticker}'s technology stack support AI?",
    "What AI talent is {ticker} hiring for?",
    "What is the investment recommendation for {ticker}?",
]

SOURCE_TYPE_LABELS = {
    "sec_10k_item_1":      "10-K Business Description",
    "sec_10k_item_1a":     "10-K Risk Factors",
    "sec_10k_item_7":      "10-K MD&A",
    "job_posting_linkedin":"LinkedIn Job Posting",
    "job_posting_indeed":  "Indeed Job Posting",
    "patent_uspto":        "USPTO Patent",
    "glassdoor_review":    "Glassdoor Review",
    "board_proxy_def14a":  "Board Proxy (DEF 14A)",
    "digital_presence":    "Digital Presence",
    "technology_hiring":   "Technology Hiring",
    "innovation_activity": "Innovation Activity",
    "culture_signals":     "Culture Signals",
    "leadership_signals":  "Leadership Signals",
    "governance_signals":  "Governance Signals",
}


def render_chatbot_page():
    st.markdown("## Company Q&A")
    st.markdown(
        "Ask questions about a company's AI-readiness. "
        "Answers are grounded in real evidence from SEC filings and signals."
    )
    st.divider()

    ticker = st.session_state.get("chatbot_ticker", "")
    company_name = st.session_state.get("chatbot_company", "")

    available_companies = _get_available_companies()

    if not available_companies:
        st.warning(
            "No companies have been processed yet. "
            "Go to the Pipeline page to run the assessment first."
        )
        if st.button("Go to Pipeline", type="primary"):
            st.session_state["active_page"] = "pipeline"
            st.rerun()
        return

    options = [f"{c['ticker']} -- {c['name']}" for c in available_companies]
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
    selected_ticker = selected.split(" -- ")[0].strip()
    selected_name = selected.split(" -- ")[1].strip() if " -- " in selected else selected_ticker

    if selected_ticker != ticker:
        ticker = selected_ticker
        company_name = selected_name
        st.session_state["chatbot_ticker"] = ticker
        st.session_state["chatbot_company"] = company_name
        st.session_state[f"chat_history_{ticker}"] = []

    if not ticker:
        return

    _render_company_summary(ticker, company_name)
    st.divider()

    chat_col, justify_col = st.columns([3, 2])
    with chat_col:
        _render_chat_interface(ticker, company_name)
    with justify_col:
        _render_justification_panel(ticker)


@st.cache_data(ttl=300, show_spinner=False)
@st.cache_data(ttl=120, show_spinner=False)
def _get_available_companies() -> list:
    """Non-blocking company loader. Cached 2 min. Never raises -- returns [] on any error."""
    try:
        import requests as _r
        resp = _r.get(f"{BASE_URL}/api/v1/companies/all", timeout=60)
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            return [c for c in items if c.get("ticker")]
    except Exception:
        pass
    return []

def _render_company_summary(ticker: str, company_name: str):
    try:
        resp = requests.get(
            f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions",
            timeout=5,
        )
        if resp.status_code != 200:
            st.markdown(f"### {company_name} ({ticker})")
            st.caption("Scores not yet available -- run the pipeline first.")
            return

        data = resp.json()
        dim_scores = data.get("scores", [])
        if not dim_scores:
            st.markdown(f"### {company_name} ({ticker})")
            st.caption("Scores not yet available -- run the pipeline first.")
            return

        scores_dict = {d["dimension"]: d["score"] for d in dim_scores}
        avg_score = sum(scores_dict.values()) / len(scores_dict)

        if avg_score >= 65:
            rec = "PROCEED"
        elif avg_score >= 45:
            rec = "PROCEED WITH CAUTION"
        else:
            rec = "FURTHER DILIGENCE"

        st.markdown(f"### {company_name} ({ticker})")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Avg AI-Readiness", f"{avg_score:.1f}/100")
        with col2:
            best_dim = max(scores_dict, key=scores_dict.get)
            st.metric("Strongest Dimension", DIMENSION_LABELS.get(best_dim, best_dim))
        with col3:
            st.metric("Recommendation", rec)

        cols = st.columns(7)
        for i, dim in enumerate(DIMENSIONS):
            score = scores_dict.get(dim, 0)
            delta = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
            cols[i].metric(
                DIMENSION_LABELS.get(dim, dim)[:8],
                f"{score:.0f}",
                delta=delta,
            )

    except Exception:
        st.markdown(f"### {company_name} ({ticker})")


def _render_chat_interface(ticker: str, company_name: str):
    st.markdown("#### Ask a Question")

    history_key = f"chat_history_{ticker}"
    if history_key not in st.session_state:
        st.session_state[history_key] = []

    chat_history = st.session_state[history_key]

    with st.expander("Suggested Questions", expanded=len(chat_history) == 0):
        for q_template in SUGGESTED_QUESTIONS[:4]:
            q = q_template.format(ticker=ticker)
            if st.button(q, key=f"suggested_{ticker}_{q[:20]}", use_container_width=True):
                st.session_state["pending_question"] = q

    for msg in chat_history:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.write(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
                if msg.get("evidence"):
                    _render_evidence_citations(msg["evidence"])

    pending = st.session_state.pop("pending_question", None)
    question = st.chat_input(f"Ask about {company_name}...")

    if pending and not question:
        question = pending

    if question:
        chat_history.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing evidence..."):
                answer, evidence, dim_detected, confidence = _get_chatbot_answer(ticker, question)

            st.markdown(answer)

            if dim_detected:
                dim_label = DIMENSION_LABELS.get(dim_detected, dim_detected)
                conf_pct = f"{confidence*100:.0f}%" if confidence else "?"
                st.caption(f"Dimension detected: {dim_label} (confidence: {conf_pct})")

            if evidence:
                _render_evidence_citations(evidence)

        chat_history.append({
            "role": "assistant",
            "content": answer,
            "evidence": evidence,
        })
        st.session_state[history_key] = chat_history

    if chat_history:
        if st.button("Clear Chat", key=f"clear_{ticker}"):
            st.session_state[history_key] = []
            st.rerun()


def _get_chatbot_answer(ticker: str, question: str) -> tuple[str, list, str, float]:
    try:
        resp = requests.get(
            f"{BASE_URL}/rag/chatbot/{ticker}",
            params={"question": question, "use_hyde": False},
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            return (
                data.get("answer", "No answer generated."),
                data.get("evidence", []),
                data.get("dimension_detected", ""),
                data.get("dim_confidence", 0.0),
            )
        else:
            return f"Error getting answer (HTTP {resp.status_code}).", [], "", 0.0
    except Exception as e:
        return f"Could not connect to the API: {e}", [], "", 0.0


def _render_evidence_citations(evidence: list):
    if not evidence:
        return

    with st.expander(f"{len(evidence)} Evidence Sources", expanded=False):
        for i, ev in enumerate(evidence, 1):
            source_type = ev.get("source_type", "unknown")
            source_label = SOURCE_TYPE_LABELS.get(source_type, source_type)
            score = ev.get("score", 0)
            dimension = ev.get("dimension", "")
            dim_label = DIMENSION_LABELS.get(dimension, dimension.replace("_", " ").title())
            fy = ev.get("fiscal_year", "")

            st.markdown(
                f"**[{i}] {source_label}**"
                + (f" ({fy})" if fy else "")
                + f" | Relevance: {score:.3f} | {dim_label}"
            )
            st.caption(ev.get("content", "")[:300] + "...")
            if i < len(evidence):
                st.divider()


def _render_justification_panel(ticker: str):
    st.markdown("#### Score Justification")
    st.caption("Select a dimension for IC-ready evidence justification")

    selected_dim = st.selectbox(
        "Dimension",
        options=DIMENSIONS,
        format_func=lambda x: DIMENSION_LABELS.get(x, x),
        key=f"justify_dim_{ticker}",
    )

    if st.button("Generate Justification", key=f"justify_btn_{ticker}",
                 type="primary", use_container_width=True):
        _fetch_and_render_justification(ticker, selected_dim)

    cache_key = f"justification_{ticker}_{selected_dim}"
    if cache_key in st.session_state:
        _display_justification(st.session_state[cache_key])


def _fetch_and_render_justification(ticker: str, dimension: str):
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
    dim = data.get("dimension", "")
    score = data.get("score", 0)
    level = data.get("level", 0)
    level_name = data.get("level_name", "")
    strength = data.get("evidence_strength", "weak")
    summary = data.get("generated_summary", "")
    evidence = data.get("supporting_evidence", [])
    gaps = data.get("gaps_identified", [])

    st.markdown(
        f"**{DIMENSION_LABELS.get(dim, dim)}**: "
        f"**{score:.0f}/100** (Level {level} -- {level_name})"
    )
    st.caption(f"Evidence Strength: {strength.upper()}")

    if summary:
        st.info(summary)

    if evidence:
        st.markdown(f"**Supporting Evidence** ({len(evidence)} pieces)")
        for i, ev in enumerate(evidence[:3], 1):
            source_type = ev.get("source_type", "")
            source_label = SOURCE_TYPE_LABELS.get(source_type, source_type)
            confidence = ev.get("confidence", 0)
            keywords = ev.get("matched_keywords", [])
            st.markdown(f"*{i}. {source_label}* (conf: {confidence:.2f})")
            if keywords:
                st.caption(f"Keywords: {', '.join(keywords[:5])}")
            st.caption(ev.get("content", "")[:200] + "...")

    if gaps:
        st.markdown("**Gaps to Next Level**")
        for gap in gaps[:3]:
            st.caption(f"- {gap}")