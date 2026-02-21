# """
# PE Org-AI-R Platform — Comprehensive Dashboard
# Covers CS1 (Platform), CS2 (Evidence), CS3 (Scoring Engine)

# Run: .\.venv\Scripts\python.exe -m streamlit run .\streamlit\app.py
# """

# import streamlit as st

# st.set_page_config(
#     page_title="PE Org-AI-R Platform",
#     page_icon="🏢",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# # ---------------------------------------------------------------------------
# # Sidebar
# # ---------------------------------------------------------------------------
# st.sidebar.markdown("## 🏢 PE Org-AI-R Platform")
# st.sidebar.caption("AI Readiness Scoring Engine")
# st.sidebar.divider()

# page = st.sidebar.radio(
#     "Navigate",
#     [
#         "📊 Executive Summary",
#         "🏗️ Platform Foundation (CS1)",
#         "📄 Evidence Collection (CS2)",
#         "⚙️ Scoring Engine (CS3)",
#         "🔍 Company Deep Dive",
#     ],
# )

# st.sidebar.divider()

# from data_loader import check_health, CS3_TICKERS, COMPANY_NAMES

# # st.sidebar.divider()
# st.sidebar.caption("Big Data & Intelligent Analytics — Spring 2026")

# # ---------------------------------------------------------------------------
# # Page routing
# # ---------------------------------------------------------------------------
# from views import executive_summary, platform_cs1, evidence_cs2, scoring_cs3, company_deep_dive

# if page == "📊 Executive Summary":
#     executive_summary.render()
# elif page == "🏗️ Platform Foundation (CS1)":
#     platform_cs1.render()
# elif page == "📄 Evidence Collection (CS2)":
#     evidence_cs2.render()
# elif page == "⚙️ Scoring Engine (CS3)":
#     scoring_cs3.render()
# elif page == "🔍 Company Deep Dive":
#     company_deep_dive.render()


"""
PE Org-AI-R Platform — Comprehensive Dashboard
Covers CS1 (Platform), CS2 (Evidence), CS3 (Scoring Engine)

Run: .\.venv\Scripts\python.exe -m streamlit run .\streamlit\app.py
"""

import streamlit as st

st.set_page_config(
    page_title="PE Org-AI-R Platform",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.markdown("## 🏢 PE Org-AI-R Platform")
st.sidebar.caption("AI Readiness Scoring Engine")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    [
        "📊 Executive Summary",
        "🏗️ Platform Foundation (CS1)",
        "📄 Evidence Collection (CS2)",
        "⚙️ Scoring Engine (CS3)",
        "🔍 Company Deep Dive",
        "🧪 Testing & Coverage",
    ],
)

st.sidebar.divider()

from data_loader import check_health, CS3_TICKERS, COMPANY_NAMES

# st.sidebar.divider()
st.sidebar.caption("Big Data & Intelligent Analytics — Spring 2026")

# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------
from views import executive_summary, platform_cs1, evidence_cs2, scoring_cs3, company_deep_dive
from views import testing_coverage

if page == "📊 Executive Summary":
    executive_summary.render()
elif page == "🏗️ Platform Foundation (CS1)":
    platform_cs1.render()
elif page == "📄 Evidence Collection (CS2)":
    evidence_cs2.render()
elif page == "⚙️ Scoring Engine (CS3)":
    scoring_cs3.render()
elif page == "🔍 Company Deep Dive":
    company_deep_dive.render()
elif page == "🧪 Testing & Coverage":
    testing_coverage.render()