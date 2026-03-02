"""Page: Evidence Collection (CS2) — SEC filings, rubric scoring, external signals."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

from data_loader import get_document_stats, get_signal_summaries, COMPANY_NAMES
from components.charts import signal_comparison_chart, signal_heatmap


def render():
    st.title("📄 Evidence Collection (CS2)")
    st.caption("Bridging what companies SAY in filings with what they actually DO — using SEC filings and 4 external signals")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1 — The Say-Do Gap (Why CS2 Exists)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 1. The Say-Do Gap")
    st.markdown(
        "CS2 exists because of a fundamental problem in corporate AI assessment: "
        "**what companies say in SEC filings often doesn't match what they actually do.** "
        "73% of companies now mention AI in their 10-K filings (up from 12% in 2018), "
        "but only 23% have deployed AI in production. CS2 collects both types of evidence "
        "so CS3 can measure the gap."
    )

    col_say, col_do = st.columns(2)
    with col_say:
        st.markdown("#### 📝 What Companies SAY")
        st.markdown(
            "- **10-K** — Annual report: strategy, risk factors, MD&A\n"
            "- **DEF 14A** — Proxy statement: board composition, executive AI oversight\n\n"
            "Source: **SEC EDGAR** via `sec-edgar-downloader`"
        )
    with col_do:
        st.markdown("#### 🔍 What Companies DO")
        st.markdown(
            "- **Hiring** — Are they posting AI/ML engineering roles?\n"
            "- **Patents** — Are they filing AI-related IP?\n"
            "- **Tech Stack** — What tools are they actually running?\n"
            "- **Leadership** — Do executives mention AI with specificity?\n\n"
            "Sources: **Indeed, USPTO, BuiltWith, SEC proxy**"
        )

    _g1, _g2, _g3 = st.columns(3)
    _g1.metric("AI mentions in 10-K filings", "73%", help="Up from 12% in 2018")
    _g2.metric("Companies with AI in production", "23%", help="Source: McKinsey State of AI 2024")
    # _g3.metric("The Say-Do Gap", "50 pp", help="The gap this platform is designed to quantify")
    _g3.metric("The Say-Do Gap", "50%", help="73% mention AI in filings, but only 23% have it in production")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2 — CS2 Pipeline Architecture
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 2. Pipeline Architecture")
    st.markdown(
        "CS2 runs two parallel pipelines. The **Document Pipeline** pulls SEC filings from EDGAR, "
        "parses them, extracts key sections, and chunks them for storage. "
        "The **Signal Pipeline** scrapes external sources to measure actual AI execution. "
        "Both pipelines write to Snowflake (metadata) and S3 (raw content)."
    )

    _arch_img = Path(__file__).parent.parent / "screenshots" / "cs2_architecture.png"
    if _arch_img.exists():
        # st.image(str(_arch_img), caption="CS2 Evidence Collection Architecture — Document + Signal Pipelines")
        st.image(str(_arch_img), caption="CS2 Evidence Collection Architecture — Document + Signal Pipelines", width=400)
    else:
        try:
            from streamlit_mermaid import st_mermaid
            st_mermaid("""
graph TB
    subgraph External Sources
        EDGAR([SEC EDGAR])
        INDEED([Indeed Jobs])
        BUILT([BuiltWith])
        USPTO([USPTO Patents])
    end
    subgraph Document Pipeline
        EDGAR --> Download[Download 10-K / DEF 14A]
        Download --> Parse[Parse PDF / HTML]
        Parse --> Extract[Extract Sections<br/>Item 1, 1A, 7]
        Extract --> Chunk[Semantic Chunking<br/>~500 tokens]
    end
    subgraph Signal Pipeline
        INDEED --> Jobs[Job Signal Collector]
        BUILT --> Tech[Tech Stack Collector]
        USPTO --> Patents[Patent Collector]
        Jobs --> Normalize[Normalize 0-100]
        Tech --> Normalize
        Patents --> Normalize
    end
    Chunk --> Snowflake[(Snowflake)]
    Chunk --> S3[(S3)]
    Normalize --> Snowflake
""")
        except ImportError:
            st.info("Install `streamlit-mermaid` for architecture diagram, or add `screenshots/cs2_architecture.png`")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3 — SEC Filing Collection
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 3. SEC Filing Collection")
    st.markdown(
        "For each portfolio company, CS2 downloads two filing types from SEC EDGAR. "
        "Each filing type contains different AI-relevant sections that feed into different "
        "CS3 scoring dimensions."
    )

    # Filing types reference table
    st.markdown("**Filing Types and Their AI Evidence Value:**")
    _filing_types = pd.DataFrame([
        {"Filing": "10-K", "Frequency": "Annual", "Key Sections": "Item 1 (Business), Item 1A (Risk), Item 7 (MD&A)", "Feeds CS3 Dimensions": "Use Cases, AI Governance, Leadership"},
        {"Filing": "DEF 14A", "Frequency": "Annual", "Key Sections": "Board bios, Committee charters, Exec compensation", "Feeds CS3 Dimensions": "AI Governance, Leadership Vision"},
    ])
    st.dataframe(_filing_types, use_container_width=True, hide_index=True)

    # Processing pipeline
    st.markdown(
        "**Processing flow:** Each filing goes through 4 stages before it's usable by CS3:"
    )
    _p1, _p2, _p3, _p4 = st.columns(4)
    _p1.markdown("**① Download**\n\nSEC EDGAR API → raw HTML/PDF")
    _p2.markdown("**② Parse**\n\npdfplumber (PDF) or BeautifulSoup (HTML) → plain text")
    _p3.markdown("**③ Section Extract**\n\nRegex patterns → Item 1, 1A, 7 sections")
    _p4.markdown("**④ Chunk & Store**\n\n~500 token chunks → Snowflake metadata + S3 raw")

    # Live document stats
    doc_df = get_document_stats()
    if not doc_df.empty:
        pivot = doc_df.pivot_table(
            index="TICKER", columns="FILING_TYPE",
            values="DOC_COUNT", aggfunc="sum", fill_value=0
        ).reset_index()

        total_docs = int(doc_df["DOC_COUNT"].sum())
        total_words = int(doc_df["TOTAL_WORDS"].sum())
        total_chunks = int(doc_df["TOTAL_CHUNKS"].sum())

        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Total Documents", f"{total_docs}")
        mc2.metric("Total Words Processed", f"{total_words:,}")
        mc3.metric("Total Semantic Chunks", f"{total_chunks:,}")
        mc4.metric("Avg Chunk Size (words)", f"{total_words // max(total_chunks, 1):,}")

        st.markdown("**Documents per Company by Filing Type:**")
        st.dataframe(pivot, use_container_width=True, hide_index=True)

        # Document bar chart
        if "TICKER" in doc_df.columns:
            _doc_by_co = doc_df.groupby("TICKER")["DOC_COUNT"].sum().reset_index()
            _doc_fig = px.bar(
                _doc_by_co, x="TICKER", y="DOC_COUNT",
                title="Documents Collected per Company",
                labels={"DOC_COUNT": "Documents", "TICKER": ""},
                color_discrete_sequence=["#6366f1"],
            )
            _doc_fig.update_layout(height=280, margin=dict(t=50, b=40), showlegend=False, plot_bgcolor="white")
            st.plotly_chart(_doc_fig, use_container_width=True, key="cs2_doc_bar")
    else:
        st.info("No document data — run `POST /api/v1/documents/collect` first")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4 — Section Parsing & Rubric Scoring
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 4. Section Parsing & Rubric Scoring")
    st.markdown(
        "Once extracted, each SEC section is scored on a **0–100 rubric** using keyword matching, "
        "depth analysis, and specificity checks. These rubric scores become evidence inputs for "
        "the CS3 dimension mapping matrix. Each section maps to specific dimensions:"
    )

    _section_map = pd.DataFrame([
        {"SEC Section": "Item 1 (Business)", "What It Reveals": "AI use cases, production deployments, revenue from AI products", "Primary CS3 Dimension": "Use Case Portfolio (70%)", "Secondary": "Technology Stack (30%)"},
        {"SEC Section": "Item 1A (Risk Factors)", "What It Reveals": "AI risk awareness, governance disclosures, mitigation strategies", "Primary CS3 Dimension": "AI Governance (80%)", "Secondary": "Data Infrastructure (20%)"},
        {"SEC Section": "Item 7 (MD&A)", "What It Reveals": "AI investment plans, strategic roadmaps, metric disclosures", "Primary CS3 Dimension": "Leadership Vision (50%)", "Secondary": "Use Cases (30%), Data Infra (20%)"},
        {"SEC Section": "DEF 14A (Proxy)", "What It Reveals": "Board AI expertise, tech committees, executive AI oversight", "Primary CS3 Dimension": "AI Governance (70%)", "Secondary": "Leadership Vision (30%)"},
    ])
    st.dataframe(_section_map, use_container_width=True, hide_index=True)

    with st.expander("📐 View Rubric Scoring Formulae"):
        st.markdown("**SEC Item 1 (Business Description) → Use Case Portfolio score:**")
        st.latex(
            r"\text{Item1\_score} = "
            r"\min\!\left(\text{AI keywords} \times 5,\; 50\right) + "
            r"\min\!\left(\text{deployment mentions} \times 10,\; 30\right) + "
            r"\min\!\left(\text{ROI references} \times 5,\; 20\right)"
        )

        st.markdown("**SEC Item 1A (Risk Factors) → AI Governance score:**")
        st.latex(
            r"\text{Item1A\_score} = "
            r"\min\!\left(\text{risk disclosures} \times 8,\; 40\right) + "
            r"\min\!\left(\text{mitigation actions} \times 6,\; 30\right) + "
            r"\min\!\left(\text{governance mentions} \times 10,\; 30\right)"
        )

        st.markdown("**SEC Item 7 (MD&A) → Leadership Vision score:**")
        st.latex(
            r"\text{Item7\_score} = "
            r"\min\!\left(\text{investment mentions} \times 5,\; 40\right) + "
            r"\min\!\left(\text{roadmap detail} \times 5,\; 30\right) + "
            r"\min\!\left(\text{metric disclosures} \times 10,\; 30\right)"
        )

        st.markdown("**DEF 14A (Proxy Statement) → AI Governance + Leadership score:**")
        st.latex(
            r"\text{Proxy\_score} = "
            r"\min\!\left(\text{AI committee mentions} \times 10,\; 40\right) + "
            r"\min\!\left(\text{oversight language} \times 8,\; 30\right) + "
            r"\min\!\left(\text{director AI expertise} \times 10,\; 30\right)"
        )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5 — External Signal Collection
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 5. External Signal Collection")
    st.markdown(
        "While SEC filings capture what companies *say*, external signals measure what they *do*. "
        "Four signal categories are collected, each from a different source, and scored 0–100. "
        "A weighted composite score summarizes overall external execution."
    )

    # Data sources table
    st.markdown("**Signal Categories, Sources & Weights:**")
    _signal_src = pd.DataFrame([
        {"Signal": "Technology Hiring", "Source": "Indeed (via Playwright scrape)", "What It Measures": "AI/ML job postings, senior ratio, skill breadth", "Weight": "30%"},
        {"Signal": "Innovation Activity", "Source": "USPTO Patent Database", "What It Measures": "AI patent filings, recency, category diversity", "Weight": "25%"},
        {"Signal": "Digital Presence", "Source": "BuiltWith / Wappalyzer", "What It Measures": "AI tools detected, cloud maturity, data platforms", "Weight": "25%"},
        {"Signal": "Leadership Signals", "Source": "SEC DEF 14A + 10-K", "What It Measures": "Executive AI mentions, vision specificity, commitment", "Weight": "20%"},
    ])
    st.dataframe(_signal_src, use_container_width=True, hide_index=True)

    st.markdown("**Composite Score Formula:**")
    st.latex(
        r"\text{Composite} = 0.30 \times \text{Hiring} + 0.25 \times \text{Innovation} "
        r"+ 0.25 \times \text{Digital} + 0.20 \times \text{Leadership}"
    )

    with st.expander("📐 View Individual Signal Score Formulae"):
        st.markdown("**Technology Hiring Score:**")
        st.latex(
            r"\text{Hiring} = "
            r"\min\!\left(\frac{\text{AI jobs}}{10},\; 50\right) + "
            r"\min\!\left(\text{senior\_ratio} \times 30,\; 30\right) + "
            r"\min\!\left(\text{skill\_breadth} \times 20,\; 20\right)"
        )

        st.markdown("**Innovation Activity Score:**")
        st.latex(
            r"\text{Innovation} = "
            r"\min\!\left(\text{ai\_patents} \times 5,\; 50\right) + "
            r"\min\!\left(\text{recent\_filings} \times 2,\; 20\right) + "
            r"\min\!\left(\text{categories} \times 10,\; 30\right)"
        )

        st.markdown("**Digital Presence Score:**")
        st.latex(
            r"\text{Digital} = "
            r"\min\!\left(\text{ai\_tools} \times 15,\; 60\right) + "
            r"\min\!\left(\text{cloud\_maturity} \times 20,\; 20\right) + "
            r"\min\!\left(\text{data\_platform} \times 20,\; 20\right)"
        )

        st.markdown("**Leadership Signals Score:**")
        st.latex(
            r"\text{Leadership} = "
            r"\min\!\left(\text{ai\_mentions\_per\_1k} \times 10,\; 50\right) + "
            r"\min\!\left(\text{vision\_specificity} \times 30,\; 30\right) + "
            r"\min\!\left(\text{commitment} \times 20,\; 20\right)"
        )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6 — Signal Results & Comparison
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 6. Signal Results & Comparison")
    st.markdown(
        "The heatmap shows each company's score across all 4 signal categories — "
        "green cells indicate strong external evidence of AI execution, red cells indicate gaps. "
        "The grouped bar chart lets you compare specific signals side-by-side."
    )

    sig_df = get_signal_summaries()
    if not sig_df.empty:
        _sc1, _sc2 = st.columns(2)
        with _sc1:
            st.plotly_chart(signal_heatmap(sig_df), use_container_width=True, key="cs2_signal_heat")
        with _sc2:
            st.plotly_chart(signal_comparison_chart(sig_df), use_container_width=True, key="cs2_signals_chart")

        # Per-company composite ranking
        st.markdown("**Composite Signal Rankings:**")
        st.markdown(
            "The composite score weights the 4 signals to produce a single external-execution measure per company. "
            "This feeds into CS3 as the baseline for dimension mapping."
        )

        _comp_col = "COMPOSITE_SCORE"
        if _comp_col in sig_df.columns:
            _ranked = sig_df.sort_values(_comp_col, ascending=False).copy()
            _rank_rows = []
            for i, (_, row) in enumerate(_ranked.iterrows()):
                ticker = row["TICKER"]
                comp = float(row[_comp_col])
                _rank_rows.append({
                    "Rank": i + 1,
                    "Ticker": ticker,
                    "Company": COMPANY_NAMES.get(ticker, ticker),
                    "Tech Hiring": float(row.get("TECHNOLOGY_HIRING_SCORE", 0)),
                    "Innovation": float(row.get("INNOVATION_ACTIVITY_SCORE", 0)),
                    "Digital": float(row.get("DIGITAL_PRESENCE_SCORE", 0)),
                    "Leadership": float(row.get("LEADERSHIP_SIGNALS_SCORE", 0)),
                    "Composite": round(comp, 1),
                })
            st.dataframe(pd.DataFrame(_rank_rows), use_container_width=True, hide_index=True)

            # Composite bar chart
            _comp_df = pd.DataFrame(_rank_rows)
            _comp_fig = px.bar(
                _comp_df, x="Ticker", y="Composite",
                title="Composite External Signal Score (Weighted)",
                color_discrete_sequence=["#0ea5e9"],
                text="Composite",
            )
            _comp_fig.update_traces(textposition="outside")
            _comp_fig.update_layout(
                height=300, margin=dict(t=50, b=40),
                yaxis=dict(range=[0, 105]), showlegend=False, plot_bgcolor="white",
            )
            st.plotly_chart(_comp_fig, use_container_width=True, key="cs2_composite_bar")

        with st.expander("View raw signal data"):
            st.dataframe(sig_df, use_container_width=True, hide_index=True)
    else:
        st.info("No signal data — run `POST /api/v1/signals/collect` first")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 7 — How CS2 Feeds CS3
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 7. How CS2 Feeds CS3")
    st.markdown(
        "CS2 produces **9 evidence sources** that CS3 consumes through the Evidence-to-Dimension mapper. "
        "The table below shows every CS2 output and which CS3 scoring component it feeds into."
    )

    _feed = pd.DataFrame([
        {"CS2 Output": "Technology Hiring signal", "Type": "External Signal", "CS3 Consumer": "EvidenceMapper → Talent (70%), Tech Stack (20%), Culture (10%)"},
        {"CS2 Output": "Innovation Activity signal", "Type": "External Signal", "CS3 Consumer": "EvidenceMapper → Tech Stack (50%), Use Cases (30%), Data Infra (20%)"},
        {"CS2 Output": "Digital Presence signal", "Type": "External Signal", "CS3 Consumer": "EvidenceMapper → Data Infra (60%), Tech Stack (40%)"},
        {"CS2 Output": "Leadership signal", "Type": "External Signal", "CS3 Consumer": "EvidenceMapper → Leadership (60%), AI Gov (25%), Culture (15%)"},
        {"CS2 Output": "SEC Item 1 (Business) rubric", "Type": "SEC Rubric", "CS3 Consumer": "EvidenceMapper → Use Cases (70%), Tech Stack (30%)"},
        {"CS2 Output": "SEC Item 1A (Risk) rubric", "Type": "SEC Rubric", "CS3 Consumer": "EvidenceMapper → AI Governance (80%), Data Infra (20%)"},
        {"CS2 Output": "SEC Item 7 (MD&A) rubric", "Type": "SEC Rubric", "CS3 Consumer": "EvidenceMapper → Leadership (50%), Use Cases (30%), Data Infra (20%)"},
        {"CS2 Output": "Glassdoor reviews [CS3-new]", "Type": "Culture Signal", "CS3 Consumer": "EvidenceMapper → Culture (80%), Talent (10%), Leadership (10%)"},
        {"CS2 Output": "Board composition [CS3-new]", "Type": "Governance Signal", "CS3 Consumer": "EvidenceMapper → AI Governance (70%), Leadership (30%)"},
    ])
    st.dataframe(_feed, use_container_width=True, hide_index=True)

    st.markdown(
        "The last two sources (Glassdoor and Board composition) are **new in CS3** — "
        "they fill the Culture and AI Governance gaps that CS2's 4 signal categories couldn't fully cover. "
        "Together, these 9 sources give complete coverage across all 7 CS3 dimensions."
    )