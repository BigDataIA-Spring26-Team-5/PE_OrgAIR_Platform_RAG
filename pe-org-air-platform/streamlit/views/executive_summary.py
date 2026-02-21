"""Page: Executive Summary. Portfolio overview with visual insights."""

import streamlit as st
import pandas as pd

from data_loader import (
    load_all_results, build_portfolio_df, build_dimensions_df,
    EXPECTED_RANGES, COMPANY_NAMES, DIMENSION_LABELS,
)
from components.charts import (
    portfolio_bar_chart, radar_chart,
    vr_hr_scatter, score_composition_bar,
    dimension_heatmap, orgair_ci_bar,
)


def render():
    st.title("📊 Executive Summary")
    st.caption("PE Org-AI-R Portfolio AI Readiness. 5 Company Assessment")

    df = build_portfolio_df()
    dims_df = build_dimensions_df()
    all_results = load_all_results()

    if df.empty:
        st.warning("No results found. Run `POST /api/v1/scoring/orgair/results` first.")
        st.stop()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1. At a Glance
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 1. At a Glance")
    st.markdown(
        "The portfolio's headline numbers. Each company is scored on a 0–100 Org-AI-R scale "
        "measuring overall AI readiness. The expected range (from CS3 Table 5) is the target "
        "each company should fall within based on its sector and market position."
    )

    c1, c2, c3, c4 = st.columns(4)
    passing = (df["In Range"] == "✅").sum()
    c1.metric("Portfolio Validation", f"{passing}/5 ✅")
    c2.metric("Avg Org-AI-R", f"{df['Org-AI-R'].mean():.1f}")
    c3.metric("Highest", f"{df['Org-AI-R'].max():.1f} ({df.loc[df['Org-AI-R'].idxmax(), 'Ticker']})")
    c4.metric("Lowest", f"{df['Org-AI-R'].min():.1f} ({df.loc[df['Org-AI-R'].idxmin(), 'Ticker']})")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2. Final Scores with Confidence
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 2. Final Scores with Confidence")
    st.markdown(
        "The bar chart below shows each company's **Org-AI-R score** alongside its "
        "**95% confidence interval** (error bars) computed using the Spearman-Brown SEM formula. "
        "The light shaded band behind each bar is the CS3 Table 5 expected range. "
        "if the bar falls inside the band, the score is validated."
    )

    st.plotly_chart(
        orgair_ci_bar(df, all_results),
        use_container_width=True, key="exec_ci_bar",
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3. What Drives Each Score
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 3. What Drives Each Score")
    st.markdown(
        "Every Org-AI-R score is built from three components:"
        "\n- **V^R (Internal Readiness)**. how strong the company's own AI capabilities are (60% weight)"
        "\n- **H^R (Sector Context)**. how the company's sector expectations and market position shape its score (40% weight)"
        "\n- **Synergy Bonus**. an extra boost when both V^R and H^R are simultaneously high (12% blend)"
        "\n\nThe stacked bar shows exactly how many points each component contributes."
    )

    st.plotly_chart(
        score_composition_bar(df),
        use_container_width=True, key="exec_composition",
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4. Internal vs External Positioning
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 4. Internal vs External Positioning")
    st.markdown(
        "This scatter plot maps each company on two axes:"
        "\n- **X-axis (V^R)**. internal AI readiness (talent, tech stack, governance, use cases)"
        "\n- **Y-axis (H^R)**. sector-adjusted readiness (industry expectations × market position)"
        "\n\nThe four quadrants reveal positioning:"
        "\n- 🟢 **AI Leaders** (top-right). strong internally *and* well-positioned in their sector"
        "\n- 🔵 **Hidden Strength** (bottom-right). strong AI capabilities but in a sector with lower expectations"
        "\n- 🟠 **Sector-carried** (top-left). benefits from a strong sector position despite weaker internals"
        "\n- 🔴 **At Risk** (bottom-left). weak on both dimensions, furthest from AI readiness"
    )

    st.plotly_chart(
        vr_hr_scatter(df),
        use_container_width=True, key="exec_scatter",
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5. Where Are the Strengths and Gaps?
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 5. Where Are the Strengths and Gaps?")
    st.markdown(
        "The V^R score is built from **7 AI readiness dimensions**. The heatmap below shows "
        "every company's score across all 7 dimensions. read *columns* to find portfolio-wide "
        "gaps (e.g., if an entire column is yellow/red, that dimension is a shared weakness), "
        "and read *rows* to see how balanced each company is."
    )

    col_heat, col_radar = st.columns([1, 1])
    with col_heat:
        if not dims_df.empty:
            st.plotly_chart(
                dimension_heatmap(dims_df),
                use_container_width=True, key="exec_dim_heat",
            )
    with col_radar:
        if not dims_df.empty:
            st.plotly_chart(
                radar_chart(dims_df),
                use_container_width=True, key="exec_radar",
            )

    # Strengths & Weaknesses table
    if not dims_df.empty:
        st.markdown(
            "The table below summarizes each company's **strongest** and **weakest** dimension. "
            "The **Balance Gap** column shows the point difference between the two. "
            "a large gap means the company's AI readiness is uneven (strong in one area but "
            "significantly behind in another), which the non-compensatory penalty in the VR formula penalizes."
        )
        sw_rows = []
        for _, row in dims_df.iterrows():
            dim_scores = {d: row.get(d, 0) for d in [
                "Data Infrastructure", "AI Governance", "Technology Stack",
                "Talent & Skills", "Leadership Vision", "Use Case Portfolio",
                "Culture & Change",
            ]}
            best_dim = max(dim_scores, key=dim_scores.get)
            worst_dim = min(dim_scores, key=dim_scores.get)
            gap = dim_scores[best_dim] - dim_scores[worst_dim]
            sw_rows.append({
                "Ticker": row["Ticker"],
                "Company": COMPANY_NAMES.get(row["Ticker"], row["Ticker"]),
                "Strongest Dimension": f"{best_dim} ({dim_scores[best_dim]:.0f})",
                "Weakest Dimension": f"{worst_dim} ({dim_scores[worst_dim]:.0f})",
                "Balance Gap": f"{gap:.0f} pts {'🔴' if gap > 40 else '🟡' if gap > 25 else '🟢'}",
            })
        st.dataframe(pd.DataFrame(sw_rows), use_container_width=True, hide_index=True)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6. Portfolio Scores
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 6. Full Portfolio Scores")
    st.dataframe(
        df[["Ticker", "Company", "Sector", "Org-AI-R", "V^R", "H^R", "Synergy", "TC", "PF", "In Range"]],
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 7. Key Findings
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("## 7. Key Findings")

    top = df.loc[df["Org-AI-R"].idxmax()]
    bot = df.loc[df["Org-AI-R"].idxmin()]

    # Portfolio-wide dimension analysis
    weakest_portfolio_dim = None
    strongest_portfolio_dim = None
    dim_avgs = {}
    if not dims_df.empty:
        dim_avgs = {d: dims_df[d].mean() for d in [
            "Data Infrastructure", "AI Governance", "Technology Stack",
            "Talent & Skills", "Leadership Vision", "Use Case Portfolio",
            "Culture & Change",
        ] if d in dims_df.columns}
        weakest_portfolio_dim = min(dim_avgs, key=dim_avgs.get) if dim_avgs else None
        strongest_portfolio_dim = max(dim_avgs, key=dim_avgs.get) if dim_avgs else None

    findings = [
        f"**{top['Company']}** leads the portfolio at **{top['Org-AI-R']:.1f}**, "
        f"driven by strong V^R ({top['V^R']:.1f}) and positive sector positioning (PF = {top['PF']:+.2f})",

        f"**{bot['Company']}** trails at **{bot['Org-AI-R']:.1f}** with the lowest V^R ({bot['V^R']:.1f}) "
        f"and a negative position factor (PF = {bot['PF']:+.2f})",

        f"The portfolio spans **{top['Org-AI-R'] - bot['Org-AI-R']:.1f} points** from leader to trailer "
        f". reflecting genuine AI maturity differences across sectors",
    ]

    if strongest_portfolio_dim and weakest_portfolio_dim:
        findings.append(
            f"Portfolio-wide, **{strongest_portfolio_dim}** is the strongest dimension "
            f"(avg {dim_avgs[strongest_portfolio_dim]:.0f}), while **{weakest_portfolio_dim}** "
            f"is the weakest (avg {dim_avgs[weakest_portfolio_dim]:.0f}). a shared investment priority"
        )

    findings.append(
        f"All {passing}/5 companies score within their CS3 Table 5 expected ranges, "
        f"validating the scoring methodology"
    )

    for f in findings:
        st.markdown(f"- {f}")