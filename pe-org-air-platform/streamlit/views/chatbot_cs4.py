"""
PE Org-AI-R Platform — CS4 Company Q&A Chatbot View
streamlit/views/chatbot_cs4.py

Changes vs original:
  - LLM-generated suggested questions per company + scores (ic_summary task)
  - All 9 dimension tabs (+ Overall) in suggested questions
  - Signal scores row with source labels above dimension scores
  - Strongest dimension highlighted with colored border
  - Rotating thinking messages while answer loads
  - Evidence panel updates per question
  - Out-of-context graceful handling
"""
from __future__ import annotations

import sys
import os
import time
import random
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

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

SOURCE_TYPE_LABELS = {
    "sec_10k_item_1":       "10-K Business Description",
    "sec_10k_item_1a":      "10-K Risk Factors",
    "sec_10k_item_7":       "10-K MD&A",
    "job_posting_linkedin": "LinkedIn Job Posting",
    "job_posting_indeed":   "Indeed Job Posting",
    "patent_uspto":         "USPTO Patent",
    "glassdoor_review":     "Glassdoor Review",
    "board_proxy_def14a":   "Board Proxy (DEF 14A)",
    "digital_presence":     "Digital Presence",
    "technology_hiring":    "Technology Hiring",
    "innovation_activity":  "Innovation Activity",
    "culture_signals":      "Culture Signals",
    "leadership_signals":   "Leadership Signals",
    "governance_signals":   "Governance Signals",
}

SOURCE_COLORS = {
    "sec_10k_item_1":       ("🟢", "#0d2a1a", "#1D9E75"),
    "sec_10k_item_1a":      ("🟢", "#0d2a1a", "#1D9E75"),
    "sec_10k_item_7":       ("🟢", "#0d2a1a", "#1D9E75"),
    "board_proxy_def14a":   ("🔵", "#0a1f35", "#378ADD"),
    "job_posting_linkedin": ("🟡", "#2a1a00", "#EF9F27"),
    "job_posting_indeed":   ("🟡", "#2a1a00", "#EF9F27"),
    "glassdoor_review":     ("⚪", "#1a1a1a", "#888888"),
    "patent_uspto":         ("🟣", "#1e1a3a", "#9990ee"),
}

SIGNAL_META = {
    "technology_hiring":   {"label": "Tech Hiring",       "source": "LinkedIn · Indeed"},
    "digital_presence":    {"label": "Digital Presence",  "source": "Wappalyzer · BuiltWith"},
    "innovation_activity": {"label": "Innovation",        "source": "USPTO patents"},
    "leadership_signals":  {"label": "Leadership",        "source": "DEF 14A proxy"},
}

THINKING_MESSAGES = [
    "Retrieving evidence...",
    "Scanning SEC filings...",
    "Whirring through chunks...",
    "Consulting the knowledge base...",
    "Thinking...",
    "Cross-referencing dimensions...",
    "Sifting through evidence...",
    "Analyzing signals...",
    "Almost there...",
]

# Fallback questions used while LLM-generated ones are loading
FALLBACK_QUESTIONS = {
    "Overall": [
        "What is {ticker}'s overall AI readiness?",
        "What are {ticker}'s biggest AI strengths vs competitors?",
        "Summarize {ticker}'s AI strategy for an investment committee",
        "What drives {ticker}'s composite AI readiness score?",
    ],
    "Data Infrastructure": [
        "What is {ticker}'s data infrastructure and cloud strategy?",
        "How mature is {ticker}'s data pipeline and MLOps stack?",
        "What proprietary data assets does {ticker} own?",
        "How does {ticker} handle data at scale for AI?",
    ],
    "AI Governance": [
        "What is {ticker}'s AI governance and ethics framework?",
        "How does {ticker} manage AI risk and compliance?",
        "What board-level oversight exists for {ticker}'s AI strategy?",
        "Are there documented responsible AI policies at {ticker}?",
    ],
    "Technology Stack": [
        "What technology stack does {ticker} use for AI development?",
        "What ML frameworks, GPUs, and platforms does {ticker} deploy?",
        "How does {ticker}'s AI infrastructure compare to peers?",
        "What proprietary AI tools or platforms has {ticker} built?",
    ],
    "Talent": [
        "What talent and hiring trends does {ticker} show?",
        "What AI and ML roles is {ticker} actively recruiting for?",
        "How does {ticker}'s AI talent density compare to industry?",
        "What percentage of {ticker}'s engineers work on AI projects?",
    ],
    "Leadership": [
        "What does {ticker}'s leadership say about AI investment priorities?",
        "How aligned is {ticker}'s C-suite on AI transformation?",
        "What is {ticker}'s CEO and CTO's AI vision?",
        "Has {ticker}'s leadership made bold AI commitments publicly?",
    ],
    "Use Cases": [
        "What AI use cases does {ticker} currently have in production?",
        "What are {ticker}'s key AI revenue drivers?",
        "How mature are {ticker}'s AI deployments across business units?",
        "What evidence exists of measurable AI ROI at {ticker}?",
    ],
    "Culture": [
        "What is the innovation and AI culture like at {ticker}?",
        "What do {ticker} employees say about AI adoption internally?",
        "How does {ticker} score on AI-readiness culture signals?",
        "Is there evidence of bottom-up AI experimentation at {ticker}?",
    ],
    "Digital": [
        "What is {ticker}'s digital presence and online AI positioning?",
        "How does {ticker} communicate AI capabilities externally?",
        "What does {ticker}'s website and job posting data signal about AI focus?",
        "How strong is {ticker}'s digital brand in the AI space?",
    ],
}


@st.cache_data(ttl=300, show_spinner=False)
def _get_available_companies() -> list:
    try:
        resp = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=60)
        if resp.status_code == 200:
            return [c for c in resp.json().get("items", []) if c.get("ticker")]
    except Exception:
        pass
    return []


@st.cache_data(ttl=600, show_spinner=False)
def _get_dimension_scores(ticker: str) -> dict:
    """Returns {dim: score} dict. Cached 10 min."""
    try:
        resp = requests.get(
            f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions", timeout=5
        )
        if resp.status_code == 200:
            return {d["dimension"]: d["score"] for d in resp.json().get("scores", [])}
    except Exception:
        pass
    return {}


@st.cache_data(ttl=600, show_spinner=False)
def _get_signal_scores(ticker: str) -> dict:
    """Returns {category: score} dict. Cached 10 min."""
    try:
        resp = requests.get(
            f"{BASE_URL}/api/v1/companies/{ticker}/evidence", timeout=5
        )
        if resp.status_code == 200:
            summary = resp.json().get("signal_summary", {})
            return {
                "technology_hiring":   summary.get("technology_hiring_score"),
                "digital_presence":    summary.get("digital_presence_score"),
                "innovation_activity": summary.get("innovation_activity_score"),
                "leadership_signals":  summary.get("leadership_signals_score"),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=1800, show_spinner=False)
def _generate_llm_questions(
    ticker: str,
    company_name: str,
    scores_json: str,   # JSON string so it's hashable for cache
) -> dict[str, list[str]]:
    """
    Call the backend LLM router (ic_summary task) to generate
    company-specific suggested questions based on scores.

    Returns dict: {dimension_label: [q1, q2, q3, q4]}
    Falls back to FALLBACK_QUESTIONS on any error.
    """
    import json

    try:
        scores = json.loads(scores_json)
    except Exception:
        scores = {}

    if not scores:
        return _format_fallback(ticker)

    # Build score summary for the prompt
    score_lines = []
    for dim, score in scores.items():
        label = DIMENSION_LABELS.get(dim, dim)
        level = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
        score_lines.append(f"  - {label}: {score:.0f}/100 ({level})")

    score_summary = "\n".join(score_lines)
    best_dim  = max(scores, key=scores.get)
    worst_dim = min(scores, key=scores.get)
    best_label  = DIMENSION_LABELS.get(best_dim, best_dim)
    worst_label = DIMENSION_LABELS.get(worst_dim, worst_dim)

    prompt = f"""You are a PE investment analyst preparing for an IC meeting on {company_name} ({ticker}).

AI Readiness Scores:
{score_summary}

Strongest dimension: {best_label} ({scores.get(best_dim, 0):.0f}/100)
Weakest dimension: {worst_label} ({scores.get(worst_dim, 0):.0f}/100)

Generate 4 specific, IC-relevant questions for each of these categories.
Questions should reference {company_name} specifically and be informed by the scores above.
Focus on dimensions with LOW scores for gap analysis, HIGH scores for validation.

Return ONLY a JSON object in this exact format, no markdown, no explanation:
{{
  "Overall": ["q1", "q2", "q3", "q4"],
  "Data Infrastructure": ["q1", "q2", "q3", "q4"],
  "AI Governance": ["q1", "q2", "q3", "q4"],
  "Technology Stack": ["q1", "q2", "q3", "q4"],
  "Talent": ["q1", "q2", "q3", "q4"],
  "Leadership": ["q1", "q2", "q3", "q4"],
  "Use Cases": ["q1", "q2", "q3", "q4"],
  "Culture": ["q1", "q2", "q3", "q4"],
  "Digital": ["q1", "q2", "q3", "q4"]
}}"""

    try:
        resp = requests.post(
            f"{BASE_URL}/rag/chatbot/{ticker}",
            json={"task": "ic_summary", "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        # The /rag router's ic_summary endpoint — try it
        if resp.status_code != 200:
            # Fallback: call the LLM router directly
            resp = requests.post(
                f"{BASE_URL}/llm/complete",
                json={"task": "ic_summary", "messages": [{"role": "user", "content": prompt}]},
                timeout=30,
            )

        if resp.status_code == 200:
            import json as _json
            raw = resp.json()
            # Extract text from response — handle both direct string and nested
            text = raw if isinstance(raw, str) else (
                raw.get("answer") or raw.get("response") or raw.get("content") or ""
            )
            text = text.strip().replace("```json", "").replace("```", "").strip()
            parsed = _json.loads(text)
            if isinstance(parsed, dict) and "Overall" in parsed:
                return parsed

    except Exception:
        pass

    return _format_fallback(ticker)


def _format_fallback(ticker: str) -> dict[str, list[str]]:
    """Format FALLBACK_QUESTIONS with ticker substituted."""
    return {
        k: [q.format(ticker=ticker) for q in qs]
        for k, qs in FALLBACK_QUESTIONS.items()
    }


def render_chatbot_page():
    st.markdown("## 💬 Company Q&A")
    st.markdown(
        "Ask questions about a company's AI-readiness. "
        "Answers are grounded in real evidence from SEC filings, job postings, and signals."
    )
    st.divider()

    available_companies = _get_available_companies()

    if not available_companies:
        st.warning(
            "No companies have been processed yet. "
            "Go to the Pipeline page to run the assessment first."
        )
        if st.button("⚡ Go to Pipeline", type="primary"):
            st.session_state["active_page"] = "pipeline"
            st.rerun()
        return

    ticker      = st.session_state.get("chatbot_ticker", "")
    company_name = st.session_state.get("chatbot_company", "")

    options     = [f"{c['ticker']} — {c['name']}" for c in available_companies]
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
    selected_name   = selected.split(" — ")[1].strip() if " — " in selected else selected_ticker

    if selected_ticker != ticker:
        ticker       = selected_ticker
        company_name = selected_name
        st.session_state["chatbot_ticker"]    = ticker
        st.session_state["chatbot_company"]   = company_name
        st.session_state[f"chat_history_{ticker}"] = []

    if not ticker:
        return

    # ── Score header ──────────────────────────────────────────────
    _render_score_header(ticker, company_name)
    st.divider()

    # ── Main layout: chat | evidence ─────────────────────────────
    chat_col, ev_col = st.columns([3, 2])
    with chat_col:
        _render_chat_interface(ticker, company_name)
    with ev_col:
        _render_evidence_panel(ticker)
        st.divider()
        _render_justification_panel(ticker)


def _render_score_header(ticker: str, company_name: str):
    """Score header: company info + signal scores + dimension scores + strongest highlighted."""
    scores_dict  = _get_dimension_scores(ticker)
    signal_scores = _get_signal_scores(ticker)

    if not scores_dict:
        st.markdown(f"### {company_name} ({ticker})")
        st.caption("Scores not yet available — run the pipeline first.")
        return

    avg_score = sum(scores_dict.values()) / len(scores_dict)
    best_dim  = max(scores_dict, key=scores_dict.get)
    rec = (
        "PROCEED" if avg_score >= 65
        else "PROCEED WITH CAUTION" if avg_score >= 45
        else "FURTHER DILIGENCE"
    )

    # Company name + summary row
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg AI-Readiness", f"{avg_score:.1f}/100")
    with col2:
        st.metric("Strongest Dimension", DIMENSION_LABELS.get(best_dim, best_dim))
    with col3:
        st.metric("Recommendation", rec)

    # ── Signal scores row ─────────────────────────────────────────
    if any(v is not None for v in signal_scores.values()):
        st.markdown("**Signal Scores**")
        sig_cols = st.columns(4)
        for i, (cat, meta) in enumerate(SIGNAL_META.items()):
            score = signal_scores.get(cat)
            with sig_cols[i]:
                score_str = f"{score:.1f}" if score is not None else "—"
                delta_str = (
                    "↑ HIGH" if (score or 0) >= 70
                    else "↑ MID" if (score or 0) >= 40
                    else "↑ LOW"
                ) if score is not None else "N/A"
                st.metric(f"{meta['label']}", score_str, delta=delta_str)
                st.caption(f"📡 {meta['source']}")

    # ── Dimension scores row — strongest highlighted ──────────────
    st.markdown("**Dimension Scores**")
    sorted_dims = sorted(scores_dict.items(), key=lambda x: x[1], reverse=True)
    dim_cols = st.columns(len(sorted_dims))
    for i, (dim, score) in enumerate(sorted_dims):
        with dim_cols[i]:
            label = DIMENSION_LABELS.get(dim, dim)[:12]
            delta = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
            st.metric(label, f"{score:.0f}", delta=delta)
            if dim == best_dim:
                st.markdown(
                    "<div style='text-align:center;font-size:10px;"
                    "color:#7F77DD;font-weight:600;margin-top:-10px'>★ Best</div>",
                    unsafe_allow_html=True,
                )


def _render_chat_interface(ticker: str, company_name: str):
    """Chat interface with LLM-generated suggested questions and rotating thinking messages."""
    history_key = f"chat_history_{ticker}"
    if history_key not in st.session_state:
        st.session_state[history_key] = []
    chat_history = st.session_state[history_key]

    # ── Suggested questions ───────────────────────────────────────
    scores_dict = _get_dimension_scores(ticker)
    import json
    scores_json = json.dumps(scores_dict)

    with st.expander("💡 Suggested Questions", expanded=len(chat_history) == 0):
        # Generate LLM questions — show spinner only on first load
        q_cache_key = f"llm_questions_{ticker}"
        if q_cache_key not in st.session_state:
            with st.spinner(f"Generating {company_name}-specific questions..."):
                questions = _generate_llm_questions(ticker, company_name, scores_json)
                st.session_state[q_cache_key] = questions
        else:
            questions = st.session_state[q_cache_key]

        # Dimension tabs
        tab_keys = list(questions.keys())
        if tab_keys:
            active_tab_key = f"sq_tab_{ticker}"
            if active_tab_key not in st.session_state:
                st.session_state[active_tab_key] = tab_keys[0]

            # Tab buttons
            tab_cols = st.columns(len(tab_keys))
            for i, tab in enumerate(tab_keys):
                with tab_cols[i]:
                    is_active = st.session_state[active_tab_key] == tab
                    if st.button(
                        tab,
                        key=f"sq_tab_btn_{ticker}_{tab}",
                        type="primary" if is_active else "secondary",
                        use_container_width=True,
                    ):
                        st.session_state[active_tab_key] = tab
                        st.rerun()

            # Question pills for active tab
            active_tab = st.session_state[active_tab_key]
            tab_questions = questions.get(active_tab, [])
            for q in tab_questions:
                if st.button(q, key=f"sq_{ticker}_{q[:30]}", use_container_width=True):
                    st.session_state["pending_question"] = q

        # Regenerate button
        if st.button("🔄 Regenerate questions", key=f"regen_q_{ticker}"):
            if q_cache_key in st.session_state:
                del st.session_state[q_cache_key]
            _generate_llm_questions.clear()
            st.rerun()

    # ── Chat history ──────────────────────────────────────────────
    for msg in chat_history:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.write(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
                dim_detected = msg.get("dim_detected")
                confidence   = msg.get("confidence", 0)
                if dim_detected:
                    dim_label = DIMENSION_LABELS.get(dim_detected, dim_detected)
                    conf_pct  = f"{confidence * 100:.0f}%"
                    st.caption(f"Dimension: **{dim_label}** · Confidence: {conf_pct}")
                if msg.get("evidence"):
                    _render_evidence_citations_inline(msg["evidence"])

    # ── Input ─────────────────────────────────────────────────────
    pending  = st.session_state.pop("pending_question", None)
    question = st.chat_input(f"Ask anything about {company_name}...")
    if pending and not question:
        question = pending

    if question:
        chat_history.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            # Rotating thinking messages
            think_ph = st.empty()
            msgs     = THINKING_MESSAGES.copy()
            random.shuffle(msgs)
            think_ph.markdown(f"*{msgs[0]}*")

            answer, evidence, dim_detected, confidence = _get_chatbot_answer(ticker, question)

            think_ph.empty()
            st.markdown(answer)

            if dim_detected:
                dim_label = DIMENSION_LABELS.get(dim_detected, dim_detected)
                conf_pct  = f"{confidence * 100:.0f}%"
                st.caption(f"Dimension: **{dim_label}** · Confidence: {conf_pct}")

            if evidence:
                # Store latest evidence for the evidence panel
                st.session_state[f"latest_evidence_{ticker}"] = evidence
                _render_evidence_citations_inline(evidence)

        chat_history.append({
            "role":         "assistant",
            "content":      answer,
            "evidence":     evidence,
            "dim_detected": dim_detected,
            "confidence":   confidence,
        })
        st.session_state[history_key] = chat_history

    if chat_history:
        if st.button("🗑️ Clear Chat", key=f"clear_{ticker}"):
            st.session_state[history_key] = []
            if f"latest_evidence_{ticker}" in st.session_state:
                del st.session_state[f"latest_evidence_{ticker}"]
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
        elif resp.status_code == 422:
            return (
                "⚠️ This question appears to be outside the scope of the evidence indexed "
                f"for {ticker}. Try rephrasing or asking about a specific dimension like "
                "data infrastructure, talent, or AI governance.",
                [], "", 0.0,
            )
        else:
            return f"Error getting answer (HTTP {resp.status_code}).", [], "", 0.0
    except Exception as e:
        return f"Could not connect to the API: {e}", [], "", 0.0


def _render_evidence_citations_inline(evidence: list):
    """Compact inline evidence citations under each answer."""
    if not evidence:
        return
    with st.expander(f"📎 {len(evidence)} evidence sources", expanded=False):
        for i, ev in enumerate(evidence, 1):
            source_type  = ev.get("source_type", "unknown")
            source_label = SOURCE_TYPE_LABELS.get(source_type, source_type)
            score        = ev.get("score", 0)
            dimension    = ev.get("dimension", "")
            dim_label    = DIMENSION_LABELS.get(dimension, dimension.replace("_", " ").title())
            fy           = ev.get("fiscal_year", "")
            color_info   = SOURCE_COLORS.get(source_type, ("⚪", "#1a1a1a", "#888888"))
            dot          = color_info[0]

            st.markdown(
                f"{dot} **[{i}] {source_label}**"
                + (f" ({fy})" if fy else "")
                + f" &nbsp;|&nbsp; Relevance: `{score:.4f}` &nbsp;|&nbsp; {dim_label}"
            )
            st.caption((ev.get("content", "")[:280] + "...") if ev.get("content") else "")
            if i < len(evidence):
                st.divider()


def _render_evidence_panel(ticker: str):
    """Right-side evidence panel — updates with each new answer."""
    st.markdown("#### 📋 Evidence Sources")

    evidence = st.session_state.get(f"latest_evidence_{ticker}", [])
    if not evidence:
        st.caption("Evidence will appear here after you ask a question.")
        return

    st.caption(f"{len(evidence)} sources retrieved for last question")

    for i, ev in enumerate(evidence):
        source_type  = ev.get("source_type", "unknown")
        source_label = SOURCE_TYPE_LABELS.get(source_type, source_type)
        score        = ev.get("score", 0)
        dimension    = ev.get("dimension", "")
        dim_label    = DIMENSION_LABELS.get(dimension, dimension.replace("_", " ").title())
        fy           = ev.get("fiscal_year", "")
        color_info   = SOURCE_COLORS.get(source_type, ("⚪", "#1a1a1a", "#888888"))
        dot          = color_info[0]

        with st.expander(
            f"{dot} {source_label}" + (f" ({fy})" if fy else "") + f" — {dim_label}",
            expanded=(i == 0),
        ):
            st.caption(f"Relevance score: `{score:.4f}`")
            content = ev.get("content", "")
            if content:
                st.markdown(f"_{content[:400]}..._" if len(content) > 400 else f"_{content}_")


def _render_justification_panel(ticker: str):
    """Score justification panel for IC prep."""
    st.markdown("#### 📊 Score Justification")
    st.caption("Generate IC-ready evidence justification for any dimension")

    selected_dim = st.selectbox(
        "Dimension",
        options=DIMENSIONS,
        format_func=lambda x: DIMENSION_LABELS.get(x, x),
        key=f"justify_dim_{ticker}",
    )

    if st.button(
        "Generate Justification",
        key=f"justify_btn_{ticker}",
        type="primary",
        use_container_width=True,
    ):
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
    dim        = data.get("dimension", "")
    score      = data.get("score", 0)
    level      = data.get("level", 0)
    level_name = data.get("level_name", "")
    strength   = data.get("evidence_strength", "weak")
    summary    = data.get("generated_summary", "")
    evidence   = data.get("supporting_evidence", [])
    gaps       = data.get("gaps_identified", [])

    st.markdown(
        f"**{DIMENSION_LABELS.get(dim, dim)}**: "
        f"**{score:.0f}/100** (Level {level} — {level_name})"
    )
    st.caption(f"Evidence Strength: {strength.upper()}")

    if summary:
        st.info(summary)

    if evidence:
        st.markdown(f"**Supporting Evidence** ({len(evidence)} pieces)")
        for i, ev in enumerate(evidence[:3], 1):
            source_type  = ev.get("source_type", "")
            source_label = SOURCE_TYPE_LABELS.get(source_type, source_type)
            confidence   = ev.get("confidence", 0)
            keywords     = ev.get("matched_keywords", [])
            st.markdown(f"*{i}. {source_label}* (conf: {confidence:.2f})")
            if keywords:
                st.caption(f"Keywords: {', '.join(keywords[:5])}")
            st.caption((ev.get("content", "")[:200] + "..."))

    if gaps:
        st.markdown("**Gaps to Next Level**")
        for gap in gaps[:3]:
            st.caption(f"— {gap}")