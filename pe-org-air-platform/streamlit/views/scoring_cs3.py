"""Page: Scoring Engine (CS3) — Visual showcase of the scoring pipeline."""

import streamlit as st
import pandas as pd
import plotly.express as px

from data_loader import (
    DIMENSION_LABELS, load_all_results, build_tc_breakdown_df,
    COMPANY_NAMES, EXPECTED_RANGES, build_dimensions_df, build_portfolio_df,
)
from components.charts import (
    tc_breakdown_bar, dimension_heatmap, dimension_grouped_bar,
    pf_horizontal_bar, hr_waterfall_multi, synergy_bubble,
    orgair_ci_bar, score_composition_bar,
)


def render():
    st.title("⚙️ Scoring Engine (CS3)")
    st.caption("From 9 evidence sources to a validated Org-AI-R score for each company")

    _all = load_all_results()

    # st.markdown("# Part I — Evidence to V^R")
    _render_evidence_mapper()
    st.divider()
    _render_rubric_scorer(_all)
    st.divider()
    _render_talent_concentration(_all)
    st.divider()
    _render_vr(_all)
    st.divider()

    # st.markdown("# Part II — H^R, Synergy & Final Score")
    _render_position_factor(_all)
    st.divider()
    _render_hr(_all)
    st.divider()
    _render_confidence(_all)
    st.divider()
    _render_synergy(_all)
    st.divider()
    _render_orgair(_all)
    st.divider()
    _render_portfolio(_all)


# ═══════════════════════════════════════════════════════════════════════════

def _render_evidence_mapper():
    st.markdown("## Evidence-to-Dimension Mapper")
    st.markdown(
        "The scoring engine collects 9 types of evidence about each company — job postings, patents, "
        "website tech stack, leadership signals, three SEC filing sections, employee reviews, and board data. "
        "Each evidence type contributes to one or more of the 7 AI readiness dimensions shown below. "
        "The darker the cell, the stronger that evidence source influences that dimension."
    )

    _sources = [
        "tech_hiring", "innovation", "digital", "leadership",
        "sec_item_1", "sec_item_1a", "sec_item_7", "glassdoor", "board",
    ]
    _dims = ["Data Infra", "AI Gov", "Tech Stack", "Talent", "Leadership", "Use Cases", "Culture"]
    _weights = [
        [0.10, 0.00, 0.20, 0.70, 0.00, 0.00, 0.10],
        [0.20, 0.00, 0.50, 0.00, 0.00, 0.30, 0.00],
        [0.60, 0.00, 0.40, 0.00, 0.00, 0.00, 0.00],
        [0.00, 0.25, 0.00, 0.00, 0.60, 0.00, 0.15],
        [0.00, 0.00, 0.30, 0.00, 0.00, 0.70, 0.00],
        [0.20, 0.80, 0.00, 0.00, 0.00, 0.00, 0.00],
        [0.20, 0.00, 0.00, 0.00, 0.50, 0.30, 0.00],
        [0.00, 0.00, 0.00, 0.10, 0.10, 0.00, 0.80],
        [0.00, 0.70, 0.00, 0.00, 0.30, 0.00, 0.00],
    ]
    _fig = px.imshow(
        _weights, x=_dims, y=_sources,
        color_continuous_scale="Blues", zmin=0, zmax=0.8,
        text_auto=".2f", aspect="auto",
        title="Evidence Source → Dimension Weight Matrix",
    )
    _fig.update_traces(textfont_size=12)
    _fig.update_layout(height=360, margin=dict(t=50, b=40, l=110, r=40), coloraxis_showscale=False)
    st.plotly_chart(_fig, use_container_width=True, key="cs3_mapper_heat")
    st.caption(
        "How to read: tech_hiring contributes 70% of its score to Talent, 20% to Tech Stack, and 10% to Culture. "
        "If a dimension has no evidence at all, it defaults to 50 (neutral)."
    )


def _render_rubric_scorer(_all):
    st.markdown("## Rubric-Based Scorer — Dimension Scores")
    st.markdown(
        "After mapping evidence to dimensions, each dimension gets a score from 0 to 100. "
        "The score is determined by matching keywords found in SEC filings, job postings, and employee reviews "
        "against a 5-level rubric (Nascent 0–19, Developing 20–39, Adequate 40–59, Good 60–79, Excellent 80–100). "
        "The charts below show the **actual scores produced** for all 5 companies."
    )

    dims_df = build_dimensions_df()
    if not dims_df.empty:
        st.plotly_chart(dimension_grouped_bar(dims_df), use_container_width=True, key="cs3_rubric_grouped")
        st.caption(
            "Each group of bars = one dimension. Each colored bar = one company's score in that dimension. "
            "Taller bars mean stronger evidence was found. Compare across companies to see who leads where."
        )

        st.plotly_chart(dimension_heatmap(dims_df), use_container_width=True, key="cs3_rubric_heat")
        st.caption(
            "Same data as above, but as a color grid. Green cells = strong scores (80+). "
            "Red cells = weak scores (<30). Read a column top-to-bottom to see if all companies are weak in one area."
        )

        if _all:
            _dim_rows = []
            for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
                _r = _all.get(_t, {})
                _dims = _r.get("dimension_scores", {}) or {}
                if _dims:
                    row = {"Ticker": _t}
                    for dk, label in DIMENSION_LABELS.items():
                        row[label] = round(float(_dims.get(dk, 0)), 0)
                    _dim_rows.append(row)
            if _dim_rows:
                st.dataframe(pd.DataFrame(_dim_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No dimension data available — run the scoring pipeline first.")


def _render_talent_concentration(_all):
    st.markdown("## Talent Concentration (TC)")
    st.markdown(
        "TC answers: **\"If one key person or team leaves, how much AI capability does the company lose?\"** "
        "It's measured from 0 (capability spread across many people) to 1 (everything depends on one person). "
        "The score is built from 4 components:"
    )
    st.markdown(
        "- **Leadership ratio** — What fraction of AI roles are senior/executive level? High ratio = few people hold the knowledge\n"
        "- **Team size factor** — How small is the AI team? Fewer people = higher risk if anyone leaves\n"
        "- **Skill concentration** — Does every AI role require the same narrow skill? Or diverse specializations?\n"
        "- **Individual factor** — Are specific individuals named in filings as the AI capability? Named = concentrated"
    )

    _tc_df = build_tc_breakdown_df()
    if not _tc_df.empty:
        st.plotly_chart(tc_breakdown_bar(_tc_df), use_container_width=True, key="cs3_tc_bar")
        st.caption(
            "Each company has 4 bars — one per TC component. "
            "Taller bars = higher concentration risk in that area. "
            "NVDA's bars are short (distributed talent), DG's are tall (concentrated)."
        )

    if _all:
        _tc_rows = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _tc_val = float(_r.get("talent_concentration", 0) or 0)
            if _tc_val < 0.15:
                _risk = "🟢 Low — talent is well-distributed"
            elif _tc_val < 0.25:
                _risk = "🟡 Medium — some concentration, manageable"
            else:
                _risk = "🔴 High — significant key-person dependency"
            _tc_rows.append({
                "Ticker": _t,
                "Company": COMPANY_NAMES.get(_t, _t),
                "TC Score": round(_tc_val, 4),
                "What This Means": _risk,
            })
        st.dataframe(pd.DataFrame(_tc_rows), use_container_width=True, hide_index=True)


def _render_vr(_all):
    st.markdown("## V^R — Valuation Readiness (Internal AI Capability)")
    st.markdown(
        "V^R answers: **\"How strong is this company's internal AI capability right now?\"** "
        "It looks at 7 dimensions of AI readiness — data infrastructure, governance, technology stack, "
        "talent, leadership vision, deployed use cases, and organizational culture."
    )
    st.markdown(
        "The score starts with a **weighted average of the 7 dimension scores** (higher = better). "
        "Then two penalties are applied:"
    )
    st.markdown(
        "- **Balance penalty** — If a company is excellent at tech stack but terrible at governance, "
        "it gets penalized. AI readiness requires strength *across* dimensions, not just in one area. "
        "The more uneven the scores, the bigger the penalty.\n"
        "- **Talent concentration penalty** — If the company's AI capability depends on a few key people "
        "(high TC from above), V^R is reduced. Concentrated talent = fragile readiness."
    )

    if _all:
        _vr_data = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _tc = float(_r.get("talent_concentration", 0) or 0)
            _vr = float(_r.get("vr_score", 0) or 0)
            _dims = _r.get("dimension_scores", {}) or {}
            _d_avg = sum(_dims.values()) / len(_dims) if _dims else 0
            _tc_adj = 1 - 0.15 * max(0, _tc - 0.25)
            _penalty = _d_avg - _vr
            _vr_data.append({
                "Ticker": _t,
                "Company": COMPANY_NAMES.get(_t, _t),
                "Avg Dimension Score": round(_d_avg, 1),
                "Penalty Applied": round(_penalty, 1),
                "TC Penalty": f"{_tc_adj:.4f}" + (" (no penalty)" if _tc_adj >= 1.0 else " (reduced)"),
                "V^R Score": round(_vr, 2),
            })

        # Chart: Avg Dimension (gray) vs Final V^R (colored) per company
        import plotly.graph_objects as go
        from components.charts import BRIGHT_COLORS

        _fig = go.Figure()
        _tickers = [r["Ticker"] for r in _vr_data]
        _avgs = [r["Avg Dimension Score"] for r in _vr_data]
        _vrs = [r["V^R Score"] for r in _vr_data]
        _penalties = [r["Penalty Applied"] for r in _vr_data]

        _fig.add_trace(go.Bar(
            name="Avg Dimension Score (before penalties)",
            x=_tickers, y=_avgs,
            marker_color="#d1d5db",
            text=[f"{v:.1f}" for v in _avgs],
            textposition="outside", textfont=dict(size=12, color="#6b7280"),
        ))
        _fig.add_trace(go.Bar(
            name="V^R Score (after penalties)",
            x=_tickers, y=_vrs,
            marker_color=[BRIGHT_COLORS.get(t, "#6366f1") for t in _tickers],
            text=[f"{v:.1f}" for v in _vrs],
            textposition="outside", textfont=dict(size=13, color="#1e293b"),
        ))

        # Annotations showing penalty size
        for i, t in enumerate(_tickers):
            if _penalties[i] > 0.5:
                _fig.add_annotation(
                    x=t, y=max(_avgs[i], _vrs[i]) + 5,
                    text=f"−{_penalties[i]:.1f} penalty",
                    showarrow=False, font=dict(size=10, color="#ef4444"),
                )

        _fig.update_layout(
            barmode="group",
            title="V^R — Dimension Average vs Final Score (after TC + Balance Penalties)",
            yaxis=dict(title="Score", range=[0, 110]),
            height=400, margin=dict(t=50, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="white",
        )
        st.plotly_chart(_fig, use_container_width=True, key="cs3_vr_comparison")
        st.caption(
            "Gray bar = raw average of 7 dimension scores. Colored bar = V^R after penalties. "
            "The gap between them shows how much the balance and TC penalties reduced the score. "
            "Companies with even dimensions and distributed talent have smaller gaps."
        )

        st.dataframe(pd.DataFrame(_vr_data), use_container_width=True, hide_index=True)
        st.caption(
            "Penalty Applied = how many points were lost from the raw dimension average. "
            "TC Penalty < 1.0 means talent concentration reduced V^R further."
        )


def _render_position_factor(_all):
    st.markdown("## Position Factor (PF)")
    st.markdown(
        "PF answers: **\"Is this company ahead of or behind its sector peers in AI?\"** "
        "It compares each company to the average AI readiness in its sector and adjusts for market size."
    )
    st.markdown(
        "- **Positive PF** (bar extends right) → The company is **ahead** of its sector. "
        "It's an AI leader relative to competitors. This *boosts* the final score.\n"
        "- **Negative PF** (bar extends left) → The company is **behind** its sector average. "
        "It's trailing competitors in AI adoption. This *reduces* the final score.\n"
        "- **Zero** → The company is at the sector average — neither leading nor trailing."
    )

    if _all:
        _sector_map = {"NVDA": "Technology", "JPM": "Financial Services", "WMT": "Retail", "GE": "Manufacturing", "DG": "Retail"}
        _pf_data = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _pf = round(float(_r.get("position_factor", 0) or 0), 4)
            if _pf > 0.5:
                _summary = "Strong AI leader in sector"
            elif _pf > 0:
                _summary = "Above sector average"
            elif _pf == 0:
                _summary = "At sector average"
            elif _pf > -0.3:
                _summary = "Slightly behind sector peers"
            else:
                _summary = "Trailing sector in AI adoption"
            _pf_data.append({
                "Ticker": _t,
                "Sector": _sector_map.get(_t, ""),
                "PF": _pf,
                "Interpretation": _summary,
            })

        st.plotly_chart(pf_horizontal_bar(_pf_data), use_container_width=True, key="cs3_pf_bar")
        st.caption(
            "The zero line represents the sector average. "
            "NVDA extends far right — it dominates its technology sector. "
            "DG extends left — it trails other retailers in AI readiness."
        )
        st.dataframe(pd.DataFrame(_pf_data), use_container_width=True, hide_index=True)


def _render_hr(_all):
    st.markdown("## H^R — Holistic Readiness (Sector-Adjusted Score)")
    st.markdown(
        "H^R answers: **\"What AI readiness score should this company have, given its industry and market position?\"** "
        "It's not based on the company's own capabilities (that's V^R) — instead it reflects "
        "the *external context* the company operates in."
    )
    st.markdown(
        "H^R is built from three layers, shown in the chart below:"
    )
    st.markdown(
        "- **Gray bar (H^R Base)** — The starting score set by sector. Technology = 84, "
        "Financial Services = 68, Retail = 55, Manufacturing = 52. Higher base = market expects more AI.\n"
        "- **Timing adjustment** — How urgent is AI adoption *right now* in this sector? "
        "Tech gets +20% (AI is critical today). Retail/Manufacturing get no boost.\n"
        "- **Position Factor adjustment** — Leaders get a positive boost, trailing companies get a drag.\n\n"
        "The **colored bar** shows the final H^R after all adjustments. "
        "The annotation above each pair shows exactly how much timing and PF changed the score."
    )

    if _all:
        _sector_map = {"NVDA": "Technology", "JPM": "Financial Services", "WMT": "Retail", "GE": "Manufacturing", "DG": "Retail"}
        _hr_base_map = {"Technology": 84.0, "Financial Services": 68.0, "Retail": 55.0, "Manufacturing": 52.0}
        _timing_map = {"Technology": 1.20, "Financial Services": 1.05, "Retail": 1.00, "Manufacturing": 1.00}

        _hr_data = []
        _hr_rows = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _sector = _sector_map.get(_t, "")
            _hr_base = _hr_base_map.get(_sector, 55.0)
            _timing = _timing_map.get(_sector, 1.0)
            _pf = float(_r.get("position_factor", 0) or 0)
            _hr = float(_r.get("hr_score", 0) or 0)

            _hr_data.append({
                "Ticker": _t, "HR_base": _hr_base,
                "Timing_effect": round(_hr_base * (_timing - 1.0), 1),
                "PF_effect": round(_hr_base * _timing * 0.15 * _pf, 1),
                "HR": round(_hr, 1),
            })
            _hr_rows.append({
                "Ticker": _t, "Sector": _sector,
                "H^R Base": _hr_base, "Timing Multiplier": _timing,
                "PF": round(_pf, 4), "H^R Score": round(_hr, 2),
            })

        st.plotly_chart(hr_waterfall_multi(_hr_data), use_container_width=True, key="cs3_hr_waterfall")
        st.caption(
            "Gray bar = sector baseline. Colored bar = final H^R after timing and PF adjustments. "
            "The text above each pair shows how much each adjustment added or subtracted. "
            "NVDA's colored bar is much taller than its base — it benefits from both timing (+16.8) and PF boost."
        )
        st.dataframe(pd.DataFrame(_hr_rows), use_container_width=True, hide_index=True)


def _render_confidence(_all):
    st.markdown("## Confidence Intervals")
    st.markdown(
        "No score is perfectly precise — it depends on how much evidence we have and how consistent "
        "that evidence is. The **confidence interval** shows the range where the true score likely falls."
    )
    st.markdown(
        "In the chart below:"
        "\n- The **colored bar** is the Org-AI-R score (our best estimate)"
        "\n- The **thin error bars** above and below show the 95% confidence range — "
        "we're 95% confident the true AI readiness falls somewhere in that range"
        "\n- The **light shaded band** behind each bar is the expected range from CS3 Table 5"
        "\n\n**Wider error bars** = less certainty (fewer evidence sources or inconsistent signals). "
        "**Narrow error bars** = high confidence (many consistent evidence sources)."
    )

    if _all:
        _df = build_portfolio_df()
        if not _df.empty:
            st.plotly_chart(orgair_ci_bar(_df, _all), use_container_width=True, key="cs3_ci_bar")

        _ci_rows = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _score = float(_r.get("org_air_score", 0) or 0)
            _ci = _r.get("org_air_ci", {})
            if isinstance(_ci, dict) and _ci.get("lower") is not None:
                _width = float(_ci["upper"]) - float(_ci["lower"])
                _ci_rows.append({
                    "Ticker": _t,
                    "Org-AI-R": round(_score, 2),
                    "Low End": round(float(_ci["lower"]), 1),
                    "High End": round(float(_ci["upper"]), 1),
                    "CI Width": round(_width, 1),
                    "Confidence": "High" if _width < 10 else ("Medium" if _width < 20 else "Low"),
                })
        if _ci_rows:
            st.dataframe(pd.DataFrame(_ci_rows), use_container_width=True, hide_index=True)
            st.caption(
                "CI Width = High End − Low End. Narrower = more confident. "
                "A width of 5 means we're very sure; a width of 20+ means the evidence was sparse or inconsistent."
            )


def _render_synergy(_all):
    st.markdown("## Synergy")
    st.markdown(
        "Synergy answers: **\"Does being strong internally AND well-positioned externally create extra value?\"** "
        "Yes — companies that score high on *both* V^R and H^R get a bonus, because real AI readiness "
        "requires capability (V^R) *and* a market environment that rewards it (H^R). "
        "The bonus is proportional to V^R × H^R — so it's biggest when both are high."
    )

    if _all:
        _syn_data = []
        _syn_rows = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _vr = float(_r.get("vr_score", 0) or 0)
            _hr = float(_r.get("hr_score", 0) or 0)
            _syn = float(_r.get("synergy_score", 0) or 0)
            _syn_data.append({"Ticker": _t, "VR": _vr, "HR": _hr, "Synergy": _syn})
            _syn_rows.append({
                "Ticker": _t, "V^R": round(_vr, 1), "H^R": round(_hr, 1),
                "Synergy Bonus": round(_syn, 2),
            })

        st.plotly_chart(synergy_bubble(_syn_data), use_container_width=True, key="cs3_syn_bubble")
        st.caption(
            "Each bubble is one company. Position = V^R (x) and H^R (y). Bubble size = synergy bonus. "
            "NVDA is top-right with the biggest bubble — strong internally AND in a favorable sector. "
            "DG is bottom-left with a small bubble — weak on both, minimal synergy."
        )
        st.dataframe(pd.DataFrame(_syn_rows), use_container_width=True, hide_index=True)


def _render_orgair(_all):
    st.markdown("## Org-AI-R — Final Score")
    st.markdown(
        "The final Org-AI-R score combines everything: 60% from internal readiness (V^R), "
        "40% from sector-adjusted readiness (H^R), and a 12% synergy bonus for companies "
        "that are strong on both. The stacked bar below shows exactly how many points "
        "each component contributes to each company's final score."
    )

    if _all:
        _df = build_portfolio_df()
        if not _df.empty:
            st.plotly_chart(score_composition_bar(_df), use_container_width=True, key="cs3_orgair_comp")
            st.caption(
                "Purple = points from V^R (internal capability). "
                "Blue = points from H^R (sector context). "
                "Green = synergy bonus. The total length = final Org-AI-R score."
            )

        _rows = []
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t)
            if not _r:
                continue
            _rows.append({
                "Ticker": _t,
                "V^R": round(float(_r.get("vr_score", 0) or 0), 1),
                "H^R": round(float(_r.get("hr_score", 0) or 0), 1),
                "Synergy": round(float(_r.get("synergy_score", 0) or 0), 1),
                "Org-AI-R": round(float(_r.get("org_air_score", 0) or 0), 2),
            })
        if _rows:
            _score_df = pd.DataFrame(_rows)
            st.dataframe(_score_df, use_container_width=True, hide_index=True)

            _fig = px.bar(
                _score_df, x="Ticker", y="Org-AI-R",
                title="Org-AI-R Score by Company",
                color_discrete_sequence=["#6366f1"], text="Org-AI-R",
            )
            _fig.update_traces(textposition="outside")
            _fig.update_layout(
                height=320, margin=dict(t=50, b=40),
                yaxis=dict(range=[0, 105]), showlegend=False, plot_bgcolor="white",
            )
            st.plotly_chart(_fig, use_container_width=True, key="cs3_orgair_bar")


def _render_portfolio(_all):
    st.markdown("## Portfolio Validation")
    st.markdown(
        "The ultimate test: do the scores make sense? Each company has a pre-defined expected range "
        "based on its known AI maturity. NVIDIA (the AI chip leader) should score 85–95. "
        "Dollar General (limited tech investment) should score 35–45. "
        "If all 5 companies land within their expected ranges, the scoring engine is validated."
    )

    _target = pd.DataFrame([
        {"Company": "NVIDIA", "Sector": "Technology", "Expected": "85–95", "Why": "AI chip leader — dominates GPU/AI hardware"},
        {"Company": "JPMorgan", "Sector": "Financial Svc", "Expected": "65–75", "Why": "$15B+ annual tech spend, large AI team"},
        {"Company": "Walmart", "Sector": "Retail", "Expected": "55–65", "Why": "Supply chain AI, significant scale"},
        {"Company": "GE Aerospace", "Sector": "Manufacturing", "Expected": "45–55", "Why": "Industrial IoT, digital twin initiatives"},
        {"Company": "Dollar General", "Sector": "Retail", "Expected": "35–45", "Why": "Minimal tech investment, cost-focused"},
    ])
    st.dataframe(_target, use_container_width=True, hide_index=True)

    if _all:
        _port_rows = []
        _all_pass = True
        for _t in ["NVDA", "JPM", "WMT", "GE", "DG"]:
            _r = _all.get(_t, {})
            _score = float(_r.get("org_air_score", 0) or 0)
            _exp = EXPECTED_RANGES.get(_t, (0, 100))
            _in_range = _exp[0] <= _score <= _exp[1]
            if not _in_range:
                _all_pass = False
            _port_rows.append({
                "Ticker": _t,
                "Company": COMPANY_NAMES.get(_t, _t),
                "V^R": round(float(_r.get("vr_score", 0) or 0), 2),
                "H^R": round(float(_r.get("hr_score", 0) or 0), 2),
                "Synergy": round(float(_r.get("synergy_score", 0) or 0), 2),
                "TC": round(float(_r.get("talent_concentration", 0) or 0), 4),
                "PF": round(float(_r.get("position_factor", 0) or 0), 4),
                "Org-AI-R": round(_score, 2),
                "Expected": f"{_exp[0]}–{_exp[1]}",
                "Status": "✅ Pass" if _in_range else "❌ Fail",
            })

        st.dataframe(pd.DataFrame(_port_rows), use_container_width=True, hide_index=True)

        if _all_pass:
            st.success("✅ All 5 companies score within their expected ranges — scoring engine validated.")
        else:
            st.warning("⚠️ Some companies are outside expected ranges — check calibration.")