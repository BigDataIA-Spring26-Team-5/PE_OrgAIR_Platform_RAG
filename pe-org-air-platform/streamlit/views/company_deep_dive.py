"""Page: Company Deep Dive — single-company analysis with full context."""

import streamlit as st
import pandas as pd
import json as _json
import plotly.graph_objects as go

from data_loader import (
    load_result, load_all_results, build_dimensions_df,
    DIMENSION_LABELS, SECTORS, EXPECTED_RANGES,
    CS3_TICKERS, COMPANY_NAMES,
)
from components.charts import (
    dimension_bar_chart, waterfall_chart,
    single_company_radar, peer_rank_bar, score_flow_chart,
    DIMENSION_ORDER,
)

COMPANY_CONTEXT = {
    "NVDA": "The dominant AI chip maker — designs GPUs that power most AI training and inference worldwide. "
            "Massive R&D investment, rapidly growing AI revenue, and deep talent pool.",
    "JPM": "The largest U.S. bank by assets with $15B+ annual technology spend. "
           "Extensive AI deployment in fraud detection, trading, and risk management.",
    "WMT": "The world's largest retailer using AI for supply chain optimization, demand forecasting, "
           "and store automation. Significant scale but retail-sector AI expectations are lower.",
    "GE":  "An industrial conglomerate (now focused on aerospace) investing in digital twins, "
           "predictive maintenance, and Industrial IoT. Manufacturing sector has moderate AI expectations.",
    "DG":  "A discount retailer focused on low-cost operations with minimal technology investment. "
           "Limited AI deployment — the lowest AI maturity in the portfolio.",
}


def _parse_kv_string(s: str) -> dict:
    """Parse 'key=value key2=value2 ...' string, handling unique_skills=[...] list."""
    result = {}
    if not s or not isinstance(s, str):
        return result
    # Extract unique_skills list separately
    skills_list = []
    skills_match = s.find("unique_skills=[")
    if skills_match >= 0:
        skills_str = s[skills_match + len("unique_skills=["):]
        end_bracket = skills_str.find("]")
        if end_bracket >= 0:
            skills_raw = skills_str[:end_bracket]
            skills_list = [x.strip().strip("'\"") for x in skills_raw.split(",") if x.strip()]
        clean = s[:skills_match].strip()
    else:
        clean = s.strip()
    for part in clean.split():
        if "=" in part:
            k, _, v = part.partition("=")
            try:
                result[k.strip()] = float(v.strip())
            except ValueError:
                pass
    if skills_list:
        result["unique_skills"] = skills_list
    return result


def render():
    st.title("🔍 Company Deep Dive")

    ticker = st.selectbox(
        "Select Company", CS3_TICKERS,
        format_func=lambda t: f"{t} — {COMPANY_NAMES[t]}",
    )

    result = load_result(ticker)
    if not result:
        st.warning(f"No results found for {ticker}. Generate results first.")
        st.stop()

    all_results = load_all_results()

    # ══════════════════════════════════════════════════════════════════════
    # 1. Company Context Card + Hero Metrics
    # ══════════════════════════════════════════════════════════════════════
    score = float(result.get("org_air_score", 0) or 0)
    exp = EXPECTED_RANGES.get(ticker, (0, 100))
    in_range = exp[0] <= score <= exp[1]
    sector = SECTORS.get(ticker, "")

    st.markdown(f"### {COMPANY_NAMES.get(ticker, ticker)} — {sector}")
    st.markdown(COMPANY_CONTEXT.get(ticker, ""))

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Org-AI-R", f"{score:.1f}", delta="✅ In Range" if in_range else "⚠️ Out")
    c2.metric("Expected", f"{exp[0]}–{exp[1]}")
    c3.metric("V^R", f"{float(result.get('vr_score', 0) or 0):.1f}")
    c4.metric("H^R", f"{float(result.get('hr_score', 0) or 0):.1f}")
    c5.metric("TC", f"{float(result.get('talent_concentration', 0) or 0):.4f}")
    c6.metric("PF", f"{float(result.get('position_factor', 0) or 0):+.4f}")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 2. Org-AI-R Formula Breakdown (Waterfall)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## Org-AI-R Formula Breakdown")
    st.markdown(
        "How V^R, H^R, and Synergy combine into the final score. "
        "V^R contributes 60% of the weight, H^R contributes 40%, "
        "and Synergy adds a bonus (12% blend) when both are high."
    )
    st.plotly_chart(waterfall_chart(result, ticker), use_container_width=True, key=f"wf_{ticker}")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 3. Score Pipeline Flow
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## Score Pipeline")
    st.markdown(
        "How each stage of the scoring engine transforms data into the final score. "
        "Start from the average dimension score (raw AI readiness), then see how V^R, H^R, "
        "Synergy, and the final blend produce the Org-AI-R."
    )
    st.plotly_chart(score_flow_chart(result, ticker), use_container_width=True, key=f"flow_{ticker}")
    st.caption(
        "Gray = average of 7 dimension scores. Purple = V^R (after TC + balance penalties). "
        "Blue = H^R (sector-adjusted). Green = synergy bonus. Black = final Org-AI-R."
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 4. Dimension Analysis — Radar + Bar + Strengths/Weaknesses
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## Dimension Analysis")

    dims = result.get("dimension_scores", {}) or {}
    if dims:
        labeled = {DIMENSION_LABELS.get(k, k): v for k, v in dims.items()}

        dims_df = build_dimensions_df()
        avg_dims = {}
        if not dims_df.empty:
            for d in DIMENSION_ORDER:
                if d in dims_df.columns:
                    avg_dims[d] = float(dims_df[d].mean())

        col_radar, col_bar = st.columns([1, 1])
        with col_radar:
            if avg_dims:
                st.plotly_chart(
                    single_company_radar(labeled, avg_dims, ticker),
                    use_container_width=True, key=f"radar_{ticker}",
                )
                st.caption(
                    "Colored area = this company's scores. Dashed line = portfolio average. "
                    "Where the color extends beyond the dashed line, this company is above average."
                )
        with col_bar:
            st.plotly_chart(
                dimension_bar_chart(labeled, ticker),
                use_container_width=True, key=f"dim_{ticker}",
            )
            st.caption(
                "Each bar = one dimension score (0–100). Background bands show rubric levels: "
                "red = Nascent, orange = Developing, yellow = Adequate, green = Good, blue = Excellent."
            )

        # Strengths & Weaknesses
        best_dim = max(labeled, key=labeled.get)
        worst_dim = min(labeled, key=labeled.get)
        gap = labeled[best_dim] - labeled[worst_dim]

        st.markdown("### Strengths & Weaknesses")
        _sw1, _sw2, _sw3 = st.columns(3)
        _sw1.metric("Strongest", f"{best_dim}", delta=f"{labeled[best_dim]:.0f}")
        _sw2.metric("Weakest", f"{worst_dim}", delta=f"{labeled[worst_dim]:.0f}")
        _sw3.metric("Balance Gap", f"{gap:.0f} pts",
                     delta="🟢 Balanced" if gap < 25 else ("🟡 Moderate" if gap < 40 else "🔴 Uneven"))

        if avg_dims:
            _dim_rows = []
            for d in DIMENSION_ORDER:
                co_score = labeled.get(d, 0)
                avg_score = avg_dims.get(d, 0)
                diff = co_score - avg_score
                _dim_rows.append({
                    "Dimension": d,
                    "Score": round(co_score, 0),
                    "Portfolio Avg": round(avg_score, 0),
                    "vs Avg": f"{diff:+.0f}" + (" ✅" if diff > 5 else (" ⚠️" if diff < -5 else "")),
                })
            st.dataframe(pd.DataFrame(_dim_rows), use_container_width=True, hide_index=True)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 5. Peer Comparison
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## Peer Comparison")
    st.markdown(
        f"Where **{COMPANY_NAMES.get(ticker, ticker)}** ranks among the 5 portfolio companies. "
        f"The selected company is highlighted in purple."
    )

    if all_results:
        st.plotly_chart(peer_rank_bar(all_results, ticker), use_container_width=True, key=f"peer_{ticker}")

        _rank_data = []
        for t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            r = all_results.get(t, {})
            _rank_data.append({
                "Ticker": t,
                "Company": COMPANY_NAMES.get(t, t),
                "Org-AI-R": round(float(r.get("org_air_score", 0) or 0), 1),
            })
        _rank_df = pd.DataFrame(_rank_data).sort_values("Org-AI-R", ascending=False).reset_index(drop=True)
        _rank_df.index = _rank_df.index + 1
        _rank_df.index.name = "Rank"

        current_rank = int(_rank_df[_rank_df["Ticker"] == ticker].index[0]) if ticker in _rank_df["Ticker"].values else 0
        st.dataframe(_rank_df, use_container_width=True)
        st.caption(f"{COMPANY_NAMES.get(ticker, ticker)} ranks **#{current_rank}** out of 5 companies.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 6. Evidence Trail — TC Breakdown + Job Analysis
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## Evidence Trail")

    # ── TC Breakdown ──
    st.markdown("### Talent Concentration Breakdown")
    tc_bd = result.get("tc_breakdown")
    tc_components = {}
    if tc_bd:
        if isinstance(tc_bd, dict):
            tc_components = {k: float(v) for k, v in tc_bd.items() if isinstance(v, (int, float))}
        elif isinstance(tc_bd, str):
            tc_components = {k: v for k, v in _parse_kv_string(tc_bd).items() if isinstance(v, float)}

    if tc_components:
        _tc_keys = ["leadership_ratio", "team_size_factor", "skill_concentration", "individual_factor"]
        _tc_labels = ["Leadership\nRatio", "Team Size\nFactor", "Skill\nConcentration", "Individual\nFactor"]
        _tc_colors = ["#6366f1", "#f59e0b", "#ef4444", "#10b981"]
        _tc_vals = [tc_components.get(k, 0) for k in _tc_keys]

        _tc_fig = go.Figure()
        _tc_fig.add_trace(go.Bar(
            x=_tc_labels, y=_tc_vals,
            marker_color=_tc_colors,
            text=[f"{v:.3f}" for v in _tc_vals],
            textposition="outside", textfont=dict(size=13),
        ))
        _tc_fig.update_layout(
            title=f"{ticker} — TC Sub-Components",
            yaxis=dict(title="Value (0 = low risk, 1 = high risk)", range=[0, 1.15]),
            height=320, margin=dict(t=50, b=60),
            showlegend=False, plot_bgcolor="white",
        )
        st.plotly_chart(_tc_fig, use_container_width=True, key=f"tc_detail_{ticker}")
        st.caption(
            "Each bar = one component of talent concentration. "
            "Taller = more risk in that area. "
            "Leadership ratio near 1.0 means almost all AI roles are senior — few junior hires building distributed capability."
        )

        _tc_meanings = {
            "leadership_ratio": "Fraction of AI roles that are senior/exec level",
            "team_size_factor": "Inverse of team size — small teams score high",
            "skill_concentration": "How narrow the required skill set is",
            "individual_factor": "How often specific individuals are named as AI capability",
        }
        _tc_items = []
        for k in _tc_keys:
            v = tc_components.get(k, 0)
            _tc_items.append({
                "Component": k.replace("_", " ").title(),
                "Value": f"{v:.4f}",
                "Risk": "🟢 Low" if v < 0.3 else ("🟡 Medium" if v < 0.6 else "🔴 High"),
                "Meaning": _tc_meanings.get(k, ""),
            })
        st.dataframe(pd.DataFrame(_tc_items), use_container_width=True, hide_index=True)
    else:
        st.info("No TC breakdown data available")

    st.divider()

    # ── Job Analysis ──
    st.markdown("### Job Analysis")
    ja = result.get("job_analysis")
    if ja:
        if isinstance(ja, str):
            try:
                ja = _json.loads(ja)
            except Exception:
                ja = _parse_kv_string(ja)
                ja = ja if ja else None

        if isinstance(ja, dict):
            total = int(ja.get("total_ai_jobs", 0) or 0)
            senior = int(ja.get("senior_ai_jobs", 0) or 0)
            mid = int(ja.get("mid_ai_jobs", 0) or 0)
            entry = int(ja.get("entry_ai_jobs", 0) or 0)

            jc1, jc2, jc3, jc4 = st.columns(4)
            jc1.metric("Total AI Jobs", total)
            jc2.metric("Senior", senior)
            jc3.metric("Mid-Level", mid)
            jc4.metric("Entry", entry)

            if total > 0:
                _job_bar = go.Figure()
                _job_bar.add_trace(go.Bar(
                    x=["Senior", "Mid-Level", "Entry"],
                    y=[senior, mid, entry],
                    marker_color=["#6366f1", "#0ea5e9", "#10b981"],
                    text=[str(senior), str(mid), str(entry)],
                    textposition="outside", textfont=dict(size=14),
                ))
                _job_bar.update_layout(
                    title=f"{ticker} — AI Jobs by Seniority Level (Total: {total})",
                    yaxis=dict(title="Number of Jobs"),
                    height=320, margin=dict(t=50, b=40),
                    showlegend=False, plot_bgcolor="white",
                )
                st.plotly_chart(_job_bar, use_container_width=True, key=f"job_bar_{ticker}")
                st.caption(
                    "A healthy AI team has a pyramid shape — many mid/entry roles building distributed capability. "
                    "Top-heavy (mostly senior) = concentrated talent risk."
                )

            skills = ja.get("unique_skills", [])
            if isinstance(skills, list) and skills:
                st.markdown(f"**Unique Skills Detected ({len(skills)}):** {', '.join(str(s) for s in skills[:30])}")
                st.caption("More diverse skills = lower skill concentration risk in the TC calculation.")
    else:
        st.info("No job analysis data available")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # 7. Validation + Raw JSON (expanders)
    # ══════════════════════════════════════════════════════════════════════
    with st.expander("Validation Checks"):
        val = result.get("validation")
        if val:
            if isinstance(val, str):
                try:
                    val = _json.loads(val)
                except Exception:
                    st.code(val)
                    val = None
            if isinstance(val, dict):
                val_rows = []
                for k, v in val.items():
                    val_rows.append({"Check": k.replace("_", " ").title(), "Result": str(v)})
                st.dataframe(pd.DataFrame(val_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No validation data available")

    with st.expander("Raw JSON"):
        st.json(result)