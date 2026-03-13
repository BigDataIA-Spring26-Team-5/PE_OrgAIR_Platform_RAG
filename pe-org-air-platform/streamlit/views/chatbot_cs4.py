"""
PE Org-AI-R Platform — CS4 Company Q&A Chatbot View
streamlit/views/chatbot_cs4.py
"""
from __future__ import annotations

import sys
import os
import random
import json
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

BASE_URL = "http://localhost:8000"

SCORE_LABELS = {
    "composite":           "AI Readiness",
    "data_infrastructure": "Data Infra",
    "ai_governance":       "AI Govern",
    "technology_stack":    "Tech Stack",
    "talent":              "Talent",
    "leadership":          "Leadership",
    "use_case_portfolio":  "Use Case",
    "culture":             "Culture",
    "digital":             "Digital",
}

SCORE_BLOCK_ORDER = [
    "composite", "data_infrastructure", "ai_governance", "technology_stack",
    "talent", "leadership", "use_case_portfolio", "culture", "digital",
]

DIMENSION_LABELS = {
    "data_infrastructure":  "Data Infrastructure",
    "ai_governance":        "AI Governance",
    "technology_stack":     "Technology Stack",
    "talent":               "Talent",
    "leadership":           "Leadership",
    "use_case_portfolio":   "Use Case Portfolio",
    "culture":              "Culture",
}

SIGNAL_DEFS = [
    {"key": "technology_hiring",   "label": "Tech Hiring Score",        "src": "Indeed · LinkedIn"},
    {"key": "digital_presence",    "label": "Digital Presence Score",   "src": "Wappalyzer · BuiltWith"},
    {"key": "innovation_activity", "label": "Innovation Activity Score","src": "USPTO Patents"},
    {"key": "leadership_signals",  "label": "Leadership Signal Score",  "src": "SEC Filings · News"},
]

SOURCE_TYPE_LABELS = {
    "sec_10k_item_1":       "10-K Item 1",
    "sec_10k_item_1a":      "10-K Item 1A",
    "sec_10k_item_7":       "10-K Item 7",
    "board_proxy_def14a":   "DEF 14A",
    "job_posting_linkedin": "Job Posting",
    "job_posting_indeed":   "Job Posting",
    "glassdoor_review":     "Glassdoor",
    "patent_uspto":         "USPTO Patent",
}

SOURCE_SECTION_LABELS = {
    "sec_10k_item_1":       "Item 1 — Business",
    "sec_10k_item_1a":      "Item 1A — Risk Factors",
    "sec_10k_item_7":       "Item 7 — MD&A",
    "board_proxy_def14a":   "DEF 14A — Proxy",
    "job_posting_linkedin": "LinkedIn Job Posting",
    "job_posting_indeed":   "Indeed Job Posting",
    "glassdoor_review":     "Glassdoor Review",
    "patent_uspto":         "USPTO Patent",
}

SOURCE_BADGE_CLASS = {
    "sec_10k_item_1":       "src-sec",
    "sec_10k_item_1a":      "src-sec",
    "sec_10k_item_7":       "src-sec",
    "board_proxy_def14a":   "src-proxy",
    "job_posting_linkedin": "src-job",
    "job_posting_indeed":   "src-job",
    "glassdoor_review":     "src-gd",
    "patent_uspto":         "src-gd",
}

THINKING_MESSAGES = [
    "Retrieving evidence...",
    "Scanning SEC filings...",
    "Whirring through chunks...",
    "Consulting the knowledge base...",
    "Thinking...",
    "Cross-referencing dimensions...",
    "Sifting through evidence...",
    "Almost there...",
    "Triangulating signals...",
    "Reasoning over evidence...",
]

FALLBACK_QUESTIONS = {
    "Overall": [
        "What is {ticker}'s overall AI readiness?",
        "What are {ticker}'s biggest AI strengths vs competitors?",
        "Summarize {ticker}'s AI strategy for an investment committee",
        "What drives {ticker}'s composite AI readiness score?",
    ],
    "Data Infra": [
        "What is {ticker}'s data infrastructure and cloud strategy?",
        "How mature is {ticker}'s data pipeline and MLOps stack?",
        "What proprietary data assets does {ticker} own?",
        "How does {ticker} handle data at scale for AI?",
    ],
    "AI Gov": [
        "What is {ticker}'s AI governance and ethics framework?",
        "How does {ticker} manage AI risk and compliance?",
        "What board-level oversight exists for {ticker}'s AI strategy?",
        "Are there documented responsible AI policies at {ticker}?",
    ],
    "Tech Stack": [
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


# ── Data fetchers ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _get_available_companies() -> list:
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/all", timeout=10)
        if r.status_code == 200:
            return [c for c in r.json().get("items", []) if c.get("ticker")]
    except Exception:
        pass
    return []


@st.cache_data(ttl=600, show_spinner=False)
def _get_dimension_scores(ticker: str) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/api/v1/scoring/{ticker}/dimensions", timeout=5)
        if r.status_code == 200:
            return {d["dimension"]: d["score"] for d in r.json().get("scores", [])}
    except Exception:
        pass
    return {}


@st.cache_data(ttl=600, show_spinner=False)
def _get_composite_score(ticker: str) -> float | None:
    try:
        r = requests.get(f"{BASE_URL}/api/v1/scoring/{ticker}", timeout=5)
        if r.status_code == 200:
            d = r.json()
            return d.get("composite_score") or d.get("org_air_score")
    except Exception:
        pass
    return None


@st.cache_data(ttl=600, show_spinner=False)
def _get_signal_scores(ticker: str) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/api/v1/companies/{ticker}/evidence", timeout=5)
        if r.status_code == 200:
            sig = r.json().get("signal_summary", {})
            return {
                "technology_hiring":   sig.get("technology_hiring_score"),
                "digital_presence":    sig.get("digital_presence_score"),
                "innovation_activity": sig.get("innovation_activity_score"),
                "leadership_signals":  sig.get("leadership_signals_score"),
            }
    except Exception:
        pass
    return {}


@st.cache_data(ttl=1800, show_spinner=False)
def _generate_llm_questions(ticker: str, company_name: str, scores_json: str) -> dict:
    try:
        scores = json.loads(scores_json)
    except Exception:
        scores = {}

    if not scores:
        return {k: [q.format(ticker=ticker) for q in qs] for k, qs in FALLBACK_QUESTIONS.items()}

    score_lines = []
    for dim, score in scores.items():
        label = DIMENSION_LABELS.get(dim, dim)
        level = "HIGH" if score >= 70 else "MID" if score >= 50 else "LOW"
        score_lines.append(f"  - {label}: {score:.0f}/100 ({level})")

    best_dim  = max(scores, key=scores.get)
    worst_dim = min(scores, key=scores.get)

    prompt = f"""You are a PE investment analyst preparing for an IC meeting on {company_name} ({ticker}).

AI Readiness Scores:
{chr(10).join(score_lines)}

Strongest: {DIMENSION_LABELS.get(best_dim, best_dim)} ({scores.get(best_dim, 0):.0f}/100)
Weakest:   {DIMENSION_LABELS.get(worst_dim, worst_dim)} ({scores.get(worst_dim, 0):.0f}/100)

Generate 4 specific IC-relevant questions for each category, referencing {company_name} specifically.
Return ONLY a JSON object — no markdown, no explanation:
{{
  "Overall": ["q1","q2","q3","q4"],
  "Data Infra": ["q1","q2","q3","q4"],
  "AI Gov": ["q1","q2","q3","q4"],
  "Tech Stack": ["q1","q2","q3","q4"],
  "Talent": ["q1","q2","q3","q4"],
  "Leadership": ["q1","q2","q3","q4"],
  "Use Cases": ["q1","q2","q3","q4"],
  "Culture": ["q1","q2","q3","q4"],
  "Digital": ["q1","q2","q3","q4"]
}}"""

    try:
        r = requests.post(
            f"{BASE_URL}/llm/complete",
            json={"task": "ic_summary", "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        if r.status_code == 200:
            raw  = r.json()
            text = raw if isinstance(raw, str) else (
                raw.get("answer") or raw.get("response") or raw.get("content") or ""
            )
            text = text.strip().replace("```json", "").replace("```", "").strip()
            parsed = json.loads(text)
            if isinstance(parsed, dict) and "Overall" in parsed:
                return parsed
    except Exception:
        pass

    return {k: [q.format(ticker=ticker) for q in qs] for k, qs in FALLBACK_QUESTIONS.items()}


def _get_chatbot_answer(ticker: str, question: str) -> tuple[str, list, str, float]:
    try:
        r = requests.get(
            f"{BASE_URL}/rag/chatbot/{ticker}",
            params={"question": question, "use_hyde": False},
            timeout=60,
        )
        if r.status_code == 200:
            d = r.json()
            return (
                d.get("answer", "No answer generated."),
                d.get("evidence", []),
                d.get("dimension_detected", ""),
                d.get("dim_confidence", 0.0),
            )
        return (
            "⚠️ This question is outside the scope of the indexed evidence. "
            "Try asking about data infrastructure, AI governance, talent, or technology stack.",
            [], "", 0.0,
        )
    except Exception as e:
        return f"Could not connect to the API: {e}", [], "", 0.0


# ── Score header ──────────────────────────────────────────────────────────────

def _render_score_header(ticker: str, company_name: str):
    dim_scores    = _get_dimension_scores(ticker)
    composite     = _get_composite_score(ticker)
    signal_scores = _get_signal_scores(ticker)

    if not dim_scores and not composite:
        st.markdown(
            f'<div class="score-header">'
            f'<div class="sh-top">'
            f'<span class="sh-company">{company_name}</span>'
            f'<span class="sh-ticker">{ticker}</span>'
            f'<span class="sh-rec rec-pending">⏳ PENDING</span>'
            f'</div>'
            f'<div style="font-size:11px;opacity:0.5;margin-top:6px">Scores not available — run the pipeline first.</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return

    if composite is None and dim_scores:
        composite = sum(dim_scores.values()) / len(dim_scores)

    if composite >= 65:
        rec_cls, rec_txt = "rec-proceed", "✓ PROCEED"
    elif composite >= 45:
        rec_cls, rec_txt = "rec-caution", "⏳ PROCEED WITH CAUTION"
    else:
        rec_cls, rec_txt = "rec-pending", "⚠️ FURTHER DILIGENCE"

    strongest = max(dim_scores, key=dim_scores.get) if dim_scores else None

    # Top row
    header_html = (
        f'<div class="score-header">'
        f'<div class="sh-top">'
        f'<span class="sh-company">{company_name}</span>'
        f'<span class="sh-ticker">{ticker}</span>'
        f'<span class="sh-rec {rec_cls}">{rec_txt}</span>'
        f'</div>'
    )

    # Score blocks
    blocks_html = '<div class="sh-scores">'
    for key in SCORE_BLOCK_ORDER:
        if key == "composite":
            val = composite or 0
            tag_cls = "tag-high" if val >= 70 else "tag-med" if val >= 40 else "tag-low"
            tag_lbl = "HIGH" if val >= 70 else "MED" if val >= 40 else "LOW"
            blocks_html += (
                f'<div class="sc-block sc-main">'
                f'<div class="sc-label">{SCORE_LABELS["composite"]}</div>'
                f'<div class="sc-val">{val:.1f}</div>'
                f'<div class="sc-tag">↑ {tag_lbl}</div>'
                f'</div>'
            )
        else:
            val       = dim_scores.get(key, 0) or 0
            label     = SCORE_LABELS.get(key, key)
            is_strong = key == strongest
            block_cls = "sc-block sc-strongest" if is_strong else "sc-block"
            star      = " ★" if is_strong else ""
            tag_cls   = "tag-high" if val >= 70 else "tag-med" if val >= 40 else "tag-low"
            tag_lbl   = "HIGH" if val >= 70 else "MED" if val >= 40 else "LOW"
            blocks_html += (
                f'<div class="{block_cls}">'
                f'<div class="sc-label">{label}{star}</div>'
                f'<div class="sc-val">{val:.0f}</div>'
                f'<div class="sc-tag {tag_cls}">↑ {tag_lbl}</div>'
                f'</div>'
            )
    blocks_html += '</div>'

    # Signal strip
    signal_html = '<div class="signal-row">'
    for sd in SIGNAL_DEFS:
        val     = signal_scores.get(sd["key"])
        val_str = f"{val:.1f}" if val is not None else "—"
        signal_html += (
            f'<div class="sig-block">'
            f'<div class="sig-label">{sd["label"]}</div>'
            f'<div class="sig-val">{val_str}</div>'
            f'<div class="sig-source">{sd["src"]}</div>'
            f'</div>'
        )
    signal_html += '</div>'

    st.markdown(header_html + blocks_html + signal_html + '</div>', unsafe_allow_html=True)


# ── Evidence panel ────────────────────────────────────────────────────────────

def _render_evidence_panel(ticker: str):
    evidence  = st.session_state.get(f"latest_evidence_{ticker}", [])
    count_txt = f"Evidence sources · {len(evidence)} retrieved" if evidence else "Evidence sources"
    st.markdown(f'<div class="ev-header">{count_txt}</div>', unsafe_allow_html=True)

    if not evidence:
        st.caption("Evidence will appear here after you send a question.")
        return

    expanded_key = f"ev_expanded_{ticker}"
    if expanded_key not in st.session_state:
        st.session_state[expanded_key] = 0

    for i, ev in enumerate(evidence):
        src_type  = ev.get("source_type", "unknown")
        badge_lbl = SOURCE_TYPE_LABELS.get(src_type, src_type)
        badge_cls = SOURCE_BADGE_CLASS.get(src_type, "src-gd")
        section   = SOURCE_SECTION_LABELS.get(src_type, src_type.replace("_", " ").title())
        score     = ev.get("score", 0)
        content   = ev.get("content", "")
        is_exp    = st.session_state.get(expanded_key) == i
        body      = content[:600] + ("..." if len(content) > 600 else "") if is_exp else (content[:200] + "..." if len(content) > 200 else content)
        card_cls  = "ev-card"

        if st.button(
            f"{'▼' if is_exp else '▶'} {badge_lbl} — {section[:28]}",
            key=f"ev_{ticker}_{i}",
            use_container_width=True,
        ):
            st.session_state[expanded_key] = i if not is_exp else -1
            st.rerun()

        st.markdown(
            f'<div class="{card_cls}">'
            f'<div class="ev-top">'
            f'<span class="src-badge {badge_cls}">{badge_lbl}</span>'
            f'<span class="ev-score">{score:.4f}</span>'
            f'</div>'
            f'<div class="ev-section">{section}</div>'
            f'<div class="ev-snippet">{body}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── Chat interface ────────────────────────────────────────────────────────────

def _render_chat_interface(ticker: str, company_name: str):
    history_key = f"chat_history_{ticker}"
    if history_key not in st.session_state:
        st.session_state[history_key] = []
    chat_history = st.session_state[history_key]

    # ── Suggested questions ───────────────────────────────────────
    dim_scores  = _get_dimension_scores(ticker)
    q_cache_key = f"llm_q_{ticker}"

    if q_cache_key not in st.session_state:
        with st.spinner(f"Generating {company_name}-specific questions..."):
            questions = _generate_llm_questions(ticker, company_name, json.dumps(dim_scores))
            st.session_state[q_cache_key] = questions
    else:
        questions = st.session_state[q_cache_key]

    tab_keys      = list(questions.keys())
    active_tab_key = f"sq_tab_{ticker}"
    if active_tab_key not in st.session_state:
        st.session_state[active_tab_key] = tab_keys[0] if tab_keys else "Overall"

    st.markdown('<div class="sq-label-txt">Suggested questions</div>', unsafe_allow_html=True)

    # Tab pills
    tab_cols = st.columns(len(tab_keys))
    for i, tab in enumerate(tab_keys):
        with tab_cols[i]:
            is_active = st.session_state[active_tab_key] == tab
            if st.button(tab, key=f"sqtab_{ticker}_{tab}",
                         type="primary" if is_active else "secondary",
                         use_container_width=True):
                st.session_state[active_tab_key] = tab
                st.rerun()

    # Question pills for active tab
    active_tab = st.session_state[active_tab_key]
    tab_qs     = questions.get(active_tab, [])
    for i, q in enumerate(tab_qs):
        if st.button(q, key=f"sqpill_{ticker}_{active_tab}_{i}", use_container_width=True):
            st.session_state["pending_question"] = q

    st.divider()

    # ── Chat history ──────────────────────────────────────────────
    for msg in chat_history:
        if msg["role"] == "user":
            st.markdown(
                f'<div class="msg-user-wrap"><div class="bubble-user">{msg["content"]}</div></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div style="margin-bottom:12px"><div class="bubble-ai">{msg["content"]}</div></div>',
                unsafe_allow_html=True,
            )
            ev = msg.get("evidence", [])
            if ev:
                cite_parts = [
                    f'<span class="cite-tag">{SOURCE_TYPE_LABELS.get(e.get("source_type",""), e.get("source_type",""))}</span>'
                    for e in ev[:3]
                ]
                st.markdown(
                    '<div style="margin-bottom:8px">' + " ".join(cite_parts) + "</div>",
                    unsafe_allow_html=True,
                )

    # ── Input ─────────────────────────────────────────────────────
    pending  = st.session_state.pop("pending_question", None)
    question = st.chat_input(f"Ask anything about {ticker}...")
    if pending and not question:
        question = pending

    if question:
        st.markdown(
            f'<div class="msg-user-wrap"><div class="bubble-user">{question}</div></div>',
            unsafe_allow_html=True,
        )
        chat_history.append({"role": "user", "content": question})

        think_ph = st.empty()
        msgs = THINKING_MESSAGES.copy()
        random.shuffle(msgs)
        think_ph.markdown(
            f'<div style="margin-bottom:12px">'
            f'<div class="bubble-ai bubble-thinking">{msgs[0]}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        answer, evidence, dim_detected, confidence = _get_chatbot_answer(ticker, question)
        think_ph.empty()

        st.markdown(
            f'<div style="margin-bottom:12px"><div class="bubble-ai">{answer}</div></div>',
            unsafe_allow_html=True,
        )

        if evidence:
            st.session_state[f"latest_evidence_{ticker}"] = evidence
            cite_parts = [
                f'<span class="cite-tag">{SOURCE_TYPE_LABELS.get(e.get("source_type",""), e.get("source_type",""))}</span>'
                for e in evidence[:3]
            ]
            st.markdown(
                '<div style="margin-bottom:8px">' + " ".join(cite_parts) + "</div>",
                unsafe_allow_html=True,
            )

        chat_history.append({
            "role": "assistant", "content": answer,
            "evidence": evidence, "dim_detected": dim_detected, "confidence": confidence,
        })
        st.session_state[history_key] = chat_history
        st.rerun()

    if chat_history:
        if st.button("🗑️ Clear chat", key=f"clear_{ticker}"):
            st.session_state[history_key] = []
            st.session_state.pop(f"latest_evidence_{ticker}", None)
            st.rerun()


# ── Main render ───────────────────────────────────────────────────────────────

def render_chatbot_page():
    available = _get_available_companies()

    if not available:
        st.markdown("## 💬 Company Q&A")
        st.warning("No companies have been assessed yet. Run the pipeline first.")
        if st.button("⚡ Go to Pipeline", type="primary"):
            st.session_state["active_page"] = "pipeline"
            st.rerun()
        return

    ticker       = st.session_state.get("chatbot_ticker", "")
    company_name = st.session_state.get("chatbot_company", "")

    options     = [f"{c['ticker']} — {c['name']}" for c in available]
    default_idx = 0
    if ticker:
        match = [i for i, c in enumerate(available) if c["ticker"] == ticker]
        if match:
            default_idx = match[0]

    selected = st.selectbox(
        "Select Company", options=options, index=default_idx,
        key="chatbot_co_select", label_visibility="collapsed",
    )
    sel_ticker = selected.split(" — ")[0].strip()
    sel_name   = selected.split(" — ")[1].strip() if " — " in selected else sel_ticker

    if sel_ticker != ticker:
        ticker       = sel_ticker
        company_name = sel_name
        st.session_state["chatbot_ticker"]             = ticker
        st.session_state["chatbot_company"]            = company_name
        st.session_state[f"chat_history_{ticker}"]    = []

    if not ticker:
        return

    # Score header (full width)
    _render_score_header(ticker, company_name)

    # Two-panel layout
    chat_col, ev_col = st.columns([3, 2])
    with chat_col:
        _render_chat_interface(ticker, company_name)
    with ev_col:
        _render_evidence_panel(ticker)