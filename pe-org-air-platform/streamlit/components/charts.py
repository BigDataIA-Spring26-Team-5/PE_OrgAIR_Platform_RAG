"""
components/charts.py — Reusable Plotly chart builders for the dashboard.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List, Optional


SECTOR_COLORS = {
    "Technology": "#6366f1",
    "Financial Services": "#0ea5e9",
    "Retail": "#10b981",
    "Manufacturing": "#f59e0b",
}

DIMENSION_ORDER = [
    "Data Infrastructure", "AI Governance", "Technology Stack",
    "Talent & Skills", "Leadership Vision", "Use Case Portfolio",
    "Culture & Change",
]

# Bright, highly distinguishable colors for radar
BRIGHT_COLORS = {
    "NVDA": "#22c55e",   # bright green
    "JPM":  "#3b82f6",   # bright blue
    "WMT":  "#f97316",   # bright orange
    "GE":   "#a855f7",   # bright purple
    "DG":   "#ef4444",   # bright red
}


def portfolio_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart: Org-AI-R scores with expected range bands."""
    fig = go.Figure()

    for _, row in df.iterrows():
        fig.add_shape(
            type="rect", x0=row["Expected Low"], x1=row["Expected High"],
            y0=row["Ticker"], y1=row["Ticker"],
            fillcolor="rgba(59,130,246,0.12)", line=dict(width=0),
            layer="below",
        )

    colors = [SECTOR_COLORS.get(row["Sector"], "#6b7280") for _, row in df.iterrows()]
    fig.add_trace(go.Bar(
        x=df["Org-AI-R"], y=df["Ticker"], orientation="h",
        marker_color=colors, text=df["Org-AI-R"].round(1),
        textposition="outside", textfont=dict(size=14, color="#1e293b"),
    ))

    fig.update_layout(
        title="Portfolio Org-AI-R Scores",
        xaxis=dict(title="Org-AI-R Score", range=[0, 105]),
        yaxis=dict(autorange="reversed"),
        height=350, margin=dict(l=80, r=40, t=50, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def radar_chart(dims_df: pd.DataFrame) -> go.Figure:
    """Spider/radar chart comparing all companies across 7 dimensions."""
    fig = go.Figure()

    for _, row in dims_df.iterrows():
        ticker = row["Ticker"]
        values = [row.get(d, 0) for d in DIMENSION_ORDER]
        values.append(values[0])
        categories = DIMENSION_ORDER + [DIMENSION_ORDER[0]]
        color = BRIGHT_COLORS.get(ticker, "#6b7280")

        fig.add_trace(go.Scatterpolar(
            r=values, theta=categories, name=ticker,
            fill="toself", opacity=0.2,
            line=dict(color=color, width=3),
            fillcolor=color,
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title="7-Dimension Comparison (All Companies)",
        height=500, margin=dict(t=60, b=40),
        legend=dict(font=dict(size=14)),
    )
    return fig


def dimension_bar_chart(dims: Dict[str, float], ticker: str) -> go.Figure:
    """Vertical bar chart for one company's 7 dimension scores with rubric bands."""
    labels = list(dims.keys())
    scores = list(dims.values())

    fig = go.Figure()
    band_colors = [
        (0, 20, "rgba(239,68,68,0.08)"),
        (20, 40, "rgba(249,115,22,0.08)"),
        (40, 60, "rgba(234,179,8,0.08)"),
        (60, 80, "rgba(34,197,94,0.08)"),
        (80, 100, "rgba(59,130,246,0.08)"),
    ]
    for y0, y1, color in band_colors:
        fig.add_hrect(y0=y0, y1=y1, fillcolor=color, line_width=0, layer="below")

    fig.add_trace(go.Bar(
        x=labels, y=scores, marker_color="#6366f1",
        text=[f"{s:.0f}" for s in scores], textposition="outside",
    ))

    fig.update_layout(
        title=f"{ticker} — 7 Dimension Scores",
        yaxis=dict(title="Score", range=[0, 105]),
        xaxis=dict(tickangle=-30),
        height=400, margin=dict(t=50, b=80),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def waterfall_chart(result: Dict, ticker: str) -> go.Figure:
    """Waterfall showing VR → HR blend → Synergy → Org-AI-R."""
    formula = result.get("formula", {})
    if not isinstance(formula, dict):
        formula = {}
    orgair = result.get("org_air_score", 0)
    wb = formula.get("weighted_base", 0)
    sc = formula.get("synergy_contribution", 0)
    vr_w = formula.get("vr_weighted", 0)
    hr_w = formula.get("hr_weighted", 0)

    fig = go.Figure(go.Waterfall(
        name="", orientation="v",
        x=["V^R (×0.6)", "H^R (×0.4)", "× (1−β)", "Synergy (×β)", "Org-AI-R"],
        y=[vr_w, hr_w, wb - vr_w - hr_w, sc, 0],
        measure=["relative", "relative", "relative", "relative", "total"],
        text=[f"{vr_w:.1f}", f"{hr_w:.1f}", f"×0.88", f"+{sc:.1f}", f"{orgair:.1f}"],
        textposition="outside",
        connector=dict(line=dict(color="rgb(63, 63, 63)")),
    ))

    fig.update_layout(
        title=f"{ticker} — Org-AI-R Formula Waterfall",
        yaxis=dict(title="Score Contribution"),
        height=400, margin=dict(t=50, b=40),
        showlegend=False,
    )
    return fig


def signal_comparison_chart(signals_df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart: 4 CS2 signals × 5 companies."""
    if signals_df.empty:
        return go.Figure()

    signal_cols = {
        "TECHNOLOGY_HIRING_SCORE": "Tech Hiring",
        "INNOVATION_ACTIVITY_SCORE": "Innovation",
        "DIGITAL_PRESENCE_SCORE": "Digital Presence",
        "LEADERSHIP_SIGNALS_SCORE": "Leadership",
    }

    fig = go.Figure()
    for col, label in signal_cols.items():
        if col in signals_df.columns:
            fig.add_trace(go.Bar(
                name=label, x=signals_df["TICKER"], y=signals_df[col],
            ))

    fig.update_layout(
        barmode="group", title="CS2 External Signals by Company",
        yaxis=dict(title="Score (0-100)", range=[0, 105]),
        height=400, margin=dict(t=50, b=40),
    )
    return fig


def signal_heatmap(signals_df: pd.DataFrame) -> go.Figure:
    """Heatmap: 5 companies × 4 external signal scores (green = high, red = low)."""
    if signals_df.empty:
        return go.Figure()

    cols = {
        "TECHNOLOGY_HIRING_SCORE": "Tech Hiring",
        "INNOVATION_ACTIVITY_SCORE": "Innovation",
        "DIGITAL_PRESENCE_SCORE": "Digital Presence",
        "LEADERSHIP_SIGNALS_SCORE": "Leadership",
    }
    available = {c: l for c, l in cols.items() if c in signals_df.columns}
    if not available or "TICKER" not in signals_df.columns:
        return go.Figure()

    z = [[float(signals_df.loc[i, c]) for c in available] for i in signals_df.index]
    x_labels = list(available.values())
    y_labels = signals_df["TICKER"].tolist()

    fig = go.Figure(go.Heatmap(
        z=z, x=x_labels, y=y_labels,
        colorscale="RdYlGn", zmin=0, zmax=100,
        text=[[f"{v:.0f}" for v in row] for row in z],
        texttemplate="%{text}",
        textfont=dict(size=13),
        showscale=True,
        colorbar=dict(title="Score", thickness=12),
    ))
    fig.update_layout(
        title="Signal Score Heatmap (0–100)",
        height=280,
        margin=dict(t=50, b=30, l=60, r=60),
        yaxis=dict(autorange="reversed"),
    )
    return fig


def tc_breakdown_bar(tc_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: TC sub-components per company (all values 0–1)."""
    if tc_df.empty:
        return go.Figure()

    components = [
        ("leadership_ratio",    "Leadership Ratio",    "#6366f1"),
        ("team_size_factor",    "Team Size Factor",    "#f59e0b"),
        ("skill_concentration", "Skill Concentration", "#ef4444"),
        ("individual_factor",   "Individual Factor",   "#10b981"),
    ]

    fig = go.Figure()
    for col, label, color in components:
        if col in tc_df.columns:
            fig.add_trace(go.Bar(
                name=label,
                x=tc_df["Ticker"],
                y=tc_df[col],
                marker_color=color,
                text=[f"{v:.3f}" for v in tc_df[col]],
                textposition="outside",
                textfont=dict(size=11),
            ))

    fig.update_layout(
        barmode="group",
        title="Talent Concentration — Sub-Component Values per Company",
        yaxis=dict(title="Component Value [0–1]", range=[0, 1.25]),
        height=420,
        margin=dict(t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# NEW — Executive Summary charts
# ═══════════════════════════════════════════════════════════════════════════

def vr_hr_scatter(df: pd.DataFrame) -> go.Figure:
    """
    V^R vs H^R scatter with quadrant lines at portfolio medians.
    Each dot = one company, colored by sector, sized by Org-AI-R.
    Quadrants reveal: high-internal/low-external vs low-internal/high-external.
    """
    if df.empty:
        return go.Figure()

    vr_med = df["V^R"].median()
    hr_med = df["H^R"].median()

    fig = go.Figure()

    # Quadrant background shading
    fig.add_shape(type="rect", x0=0, x1=vr_med, y0=hr_med, y1=110,
                  fillcolor="rgba(249,115,22,0.06)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=vr_med, x1=110, y0=hr_med, y1=110,
                  fillcolor="rgba(34,197,94,0.08)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=0, x1=vr_med, y0=0, y1=hr_med,
                  fillcolor="rgba(239,68,68,0.06)", line_width=0, layer="below")
    fig.add_shape(type="rect", x0=vr_med, x1=110, y0=0, y1=hr_med,
                  fillcolor="rgba(59,130,246,0.06)", line_width=0, layer="below")

    # Quadrant labels
    _ann = dict(showarrow=False, font=dict(size=10, color="#94a3b8"))
    fig.add_annotation(x=vr_med / 2, y=hr_med + (110 - hr_med) / 2,
                       text="Sector-carried<br>(weak internal, strong context)", **_ann)
    fig.add_annotation(x=vr_med + (110 - vr_med) / 2, y=hr_med + (110 - hr_med) / 2,
                       text="AI Leaders<br>(strong on both)", **_ann)
    fig.add_annotation(x=vr_med / 2, y=hr_med / 2,
                       text="At Risk<br>(weak on both)", **_ann)
    fig.add_annotation(x=vr_med + (110 - vr_med) / 2, y=hr_med / 2,
                       text="Hidden Strength<br>(strong internal, weak context)", **_ann)

    # Median cross-hairs
    fig.add_hline(y=hr_med, line_dash="dot", line_color="#cbd5e1", line_width=1)
    fig.add_vline(x=vr_med, line_dash="dot", line_color="#cbd5e1", line_width=1)

    for _, row in df.iterrows():
        color = SECTOR_COLORS.get(row["Sector"], "#6b7280")
        fig.add_trace(go.Scatter(
            x=[row["V^R"]], y=[row["H^R"]],
            mode="markers+text",
            marker=dict(
                size=max(12, row["Org-AI-R"] / 4),
                color=color, line=dict(width=1.5, color="white"),
            ),
            text=[row["Ticker"]], textposition="top center",
            textfont=dict(size=12, color="#1e293b"),
            name=f"{row['Ticker']} ({row['Sector']})",
            hovertemplate=(
                f"<b>{row['Ticker']}</b><br>"
                f"V^R: {row['V^R']:.1f}<br>"
                f"H^R: {row['H^R']:.1f}<br>"
                f"Org-AI-R: {row['Org-AI-R']:.1f}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title="V^R vs H^R — Internal Readiness vs Sector Context",
        xaxis=dict(title="V^R (Internal Readiness)", range=[0, 105]),
        yaxis=dict(title="H^R (Holistic / Sector Readiness)", range=[0, 105]),
        height=420, margin=dict(t=50, b=50, l=60, r=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def score_composition_bar(df: pd.DataFrame) -> go.Figure:
    """
    Stacked horizontal bar: how V^R (×α), H^R (×(1-α)), and Synergy (×β)
    contribute to the final Org-AI-R for each company.
    """
    if df.empty:
        return go.Figure()

    alpha = 0.60
    beta = 0.12

    rows = []
    for _, row in df.iterrows():
        vr_contrib = alpha * (1 - beta) * row["V^R"]
        hr_contrib = (1 - alpha) * (1 - beta) * row["H^R"]
        syn_contrib = beta * row["Synergy"]
        rows.append({
            "Ticker": row["Ticker"],
            "V^R Contribution": round(vr_contrib, 1),
            "H^R Contribution": round(hr_contrib, 1),
            "Synergy Contribution": round(syn_contrib, 1),
        })
    comp_df = pd.DataFrame(rows)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=comp_df["Ticker"], x=comp_df["V^R Contribution"],
        name="V^R — Internal Readiness (60%)", orientation="h",
        marker_color="#6366f1",
        text=[f"{v:.1f}" for v in comp_df["V^R Contribution"]],
        textposition="inside", textfont=dict(color="white", size=11),
    ))
    fig.add_trace(go.Bar(
        y=comp_df["Ticker"], x=comp_df["H^R Contribution"],
        name="H^R — Sector Context (40%)", orientation="h",
        marker_color="#0ea5e9",
        text=[f"{v:.1f}" for v in comp_df["H^R Contribution"]],
        textposition="inside", textfont=dict(color="white", size=11),
    ))
    fig.add_trace(go.Bar(
        y=comp_df["Ticker"], x=comp_df["Synergy Contribution"],
        name="Synergy Bonus (12%)", orientation="h",
        marker_color="#10b981",
        text=[f"{v:.1f}" for v in comp_df["Synergy Contribution"]],
        textposition="inside", textfont=dict(color="white", size=11),
    ))

    fig.update_layout(
        barmode="stack",
        title="Score Composition — What Drives Each Company's Org-AI-R",
        xaxis=dict(title="Score Points", range=[0, 105]),
        yaxis=dict(autorange="reversed"),
        height=340, margin=dict(l=60, r=40, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
    )
    return fig


def dimension_heatmap(dims_df: pd.DataFrame) -> go.Figure:
    """
    Heatmap: 5 companies (rows) × 7 dimensions (cols).
    Green = high score, Red = low. Instant portfolio strength/weakness view.
    """
    if dims_df.empty:
        return go.Figure()

    tickers = dims_df["Ticker"].tolist()
    z = []
    for _, row in dims_df.iterrows():
        z.append([float(row.get(d, 0)) for d in DIMENSION_ORDER])

    fig = go.Figure(go.Heatmap(
        z=z, x=DIMENSION_ORDER, y=tickers,
        colorscale="RdYlGn", zmin=0, zmax=100,
        text=[[f"{v:.0f}" for v in row] for row in z],
        texttemplate="%{text}",
        textfont=dict(size=13),
        showscale=True,
        colorbar=dict(title="Score", thickness=12),
    ))

    fig.update_layout(
        title="Portfolio Dimension Heatmap — 7 AI Readiness Dimensions",
        height=300, margin=dict(t=50, b=60, l=60, r=60),
        yaxis=dict(autorange="reversed"),
        xaxis=dict(tickangle=-25),
    )
    return fig


def orgair_ci_bar(df: pd.DataFrame, results: Dict) -> go.Figure:
    """
    Org-AI-R bar chart with SEM-based 95% confidence interval error bars
    and expected-range shading behind each bar.
    """
    if df.empty:
        return go.Figure()

    from data_loader import EXPECTED_RANGES

    tickers = df["Ticker"].tolist()
    scores = df["Org-AI-R"].tolist()
    colors = [SECTOR_COLORS.get(row["Sector"], "#6b7280") for _, row in df.iterrows()]

    # Extract CI bounds from result JSONs
    ci_low = []
    ci_high = []
    for t in tickers:
        r = results.get(t, {})
        ci = r.get("org_air_ci", {})
        if isinstance(ci, dict) and ci.get("lower") is not None:
            ci_low.append(float(ci["lower"]))
            ci_high.append(float(ci["upper"]))
        else:
            ci_low.append(None)
            ci_high.append(None)

    has_ci = any(v is not None for v in ci_low)

    fig = go.Figure()

    # Expected range bands
    for t in tickers:
        exp = EXPECTED_RANGES.get(t, (0, 100))
        fig.add_shape(
            type="rect", y0=exp[0], y1=exp[1],
            x0=tickers.index(t) - 0.4, x1=tickers.index(t) + 0.4,
            fillcolor="rgba(59,130,246,0.08)", line=dict(width=1, color="rgba(59,130,246,0.25)", dash="dot"),
            layer="below",
        )

    # Build error bars if CI data exists
    error_y_config = None
    if has_ci:
        err_minus = []
        err_plus = []
        for i, s in enumerate(scores):
            if ci_low[i] is not None:
                err_minus.append(s - ci_low[i])
                err_plus.append(ci_high[i] - s)
            else:
                err_minus.append(0)
                err_plus.append(0)
        error_y_config = dict(
            type="data", symmetric=False,
            array=err_plus, arrayminus=err_minus,
            color="#475569", thickness=2, width=6,
        )

    fig.add_trace(go.Bar(
        x=tickers, y=scores,
        marker_color=colors,
        text=[f"{s:.1f}" for s in scores],
        textposition="outside", textfont=dict(size=14, color="#1e293b"),
        error_y=error_y_config,
    ))

    fig.update_layout(
        title="Org-AI-R Scores with 95% Confidence Intervals",
        yaxis=dict(title="Org-AI-R Score", range=[0, 110]),
        xaxis=dict(title=""),
        height=400, margin=dict(t=50, b=40, l=60, r=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# CS3 Task-level charts
# ═══════════════════════════════════════════════════════════════════════════

def dimension_grouped_bar(dims_df: pd.DataFrame) -> go.Figure:
    """Grouped bar: 7 dimensions × 5 companies — shows what builds each V^R."""
    if dims_df.empty:
        return go.Figure()

    fig = go.Figure()
    for _, row in dims_df.iterrows():
        ticker = row["Ticker"]
        values = [row.get(d, 0) for d in DIMENSION_ORDER]
        fig.add_trace(go.Bar(
            name=ticker, x=DIMENSION_ORDER, y=values,
            marker_color=BRIGHT_COLORS.get(ticker, "#6b7280"),
            text=[f"{v:.0f}" for v in values],
            textposition="outside", textfont=dict(size=9),
        ))

    fig.update_layout(
        barmode="group",
        title="7 Dimension Scores by Company",
        yaxis=dict(title="Score (0–100)", range=[0, 110]),
        xaxis=dict(tickangle=-25),
        height=420, margin=dict(t=50, b=80),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
    )
    return fig


def pf_horizontal_bar(pf_data: list) -> go.Figure:
    """Horizontal bar chart of Position Factor values from -1 to +1 with zero line."""
    if not pf_data:
        return go.Figure()

    tickers = [r["Ticker"] for r in pf_data]
    pf_vals = [r["PF"] for r in pf_data]
    sectors = [r.get("Sector", "") for r in pf_data]
    colors = [SECTOR_COLORS.get(s, "#6b7280") for s in sectors]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=tickers, x=pf_vals, orientation="h",
        marker_color=colors,
        text=[f"{v:+.3f}" for v in pf_vals],
        textposition="outside", textfont=dict(size=12),
    ))

    fig.add_vline(x=0, line_dash="solid", line_color="#94a3b8", line_width=2)

    fig.update_layout(
        title="Position Factor — Sector-Relative Standing",
        xaxis=dict(title="Position Factor", range=[-1.1, 1.1]),
        yaxis=dict(autorange="reversed"),
        height=300, margin=dict(l=80, r=60, t=50, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def hr_waterfall_multi(hr_data: list) -> go.Figure:
    """Side-by-side bar: H^R Base (gray) vs Final H^R (colored), with annotation showing the delta."""
    if not hr_data:
        return go.Figure()

    tickers = [r["Ticker"] for r in hr_data]
    hr_bases = [r["HR_base"] for r in hr_data]
    hr_finals = [r["HR"] for r in hr_data]
    timing_effects = [r["Timing_effect"] for r in hr_data]
    pf_effects = [r["PF_effect"] for r in hr_data]

    fig = go.Figure()

    # Base bars (gray)
    fig.add_trace(go.Bar(
        name="H^R Base (sector starting point)",
        x=tickers, y=hr_bases,
        marker_color="#d1d5db",
        text=[f"{v:.0f}" for v in hr_bases],
        textposition="inside", textfont=dict(size=13, color="#374151"),
    ))

    # Final H^R bars (colored by sector)
    _colors = []
    for r in hr_data:
        _t = r["Ticker"]
        _colors.append(SECTOR_COLORS.get(
            {"NVDA": "Technology", "JPM": "Financial Services", "WMT": "Retail",
             "GE": "Manufacturing", "DG": "Retail"}.get(_t, ""), "#6b7280"
        ))

    fig.add_trace(go.Bar(
        name="Final H^R (after timing + PF)",
        x=tickers, y=hr_finals,
        marker_color=_colors,
        text=[f"{v:.1f}" for v in hr_finals],
        textposition="outside", textfont=dict(size=13, color="#1e293b"),
    ))

    # Annotations showing what changed
    for i, t in enumerate(tickers):
        _delta = hr_finals[i] - hr_bases[i]
        _timing_txt = f"Timing {timing_effects[i]:+.1f}" if timing_effects[i] != 0 else ""
        _pf_txt = f"PF {pf_effects[i]:+.1f}" if pf_effects[i] != 0 else ""
        _parts = [p for p in [_timing_txt, _pf_txt] if p]
        if _parts:
            fig.add_annotation(
                x=t, y=max(hr_finals[i], hr_bases[i]) + 6,
                text=f"{'  +  '.join(_parts)}<br>= {_delta:+.1f} total",
                showarrow=False,
                font=dict(size=10, color="#6b7280"),
                align="center",
            )

    fig.update_layout(
        barmode="group",
        title="H^R — Sector Base vs Final Score (after Timing & Position Factor adjustments)",
        yaxis=dict(title="Score", range=[0, 130]),
        height=420, margin=dict(t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
    )
    return fig


def synergy_bubble(syn_data: list) -> go.Figure:
    """Bubble chart: x=V^R, y=H^R, size=Synergy score. Shows multiplicative relationship."""
    if not syn_data:
        return go.Figure()

    fig = go.Figure()
    for r in syn_data:
        color = BRIGHT_COLORS.get(r["Ticker"], "#6b7280")
        fig.add_trace(go.Scatter(
            x=[r["VR"]], y=[r["HR"]],
            mode="markers+text",
            marker=dict(
                size=max(15, r["Synergy"] / 1.5),
                color=color, line=dict(width=1.5, color="white"),
                opacity=0.8,
            ),
            text=[r["Ticker"]], textposition="top center",
            textfont=dict(size=12, color="#1e293b"),
            name=r["Ticker"],
            hovertemplate=(
                f"<b>{r['Ticker']}</b><br>"
                f"V^R: {r['VR']:.1f}<br>"
                f"H^R: {r['HR']:.1f}<br>"
                f"Synergy: {r['Synergy']:.1f}<extra></extra>"
            ),
        ))

    fig.update_layout(
        title="Synergy — Bubble Size = V^R × H^R / 100",
        xaxis=dict(title="V^R (Internal Readiness)", range=[0, 105]),
        yaxis=dict(title="H^R (Sector Readiness)", range=[0, 115]),
        height=400, margin=dict(t=50, b=50, l=60, r=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# Company Deep Dive charts
# ═══════════════════════════════════════════════════════════════════════════

def single_company_radar(company_dims: Dict[str, float], avg_dims: Dict[str, float], ticker: str) -> go.Figure:
    """Radar: one company (filled) vs portfolio average (dashed outline)."""
    dims = list(DIMENSION_ORDER)
    co_vals = [company_dims.get(d, 0) for d in dims] + [company_dims.get(dims[0], 0)]
    avg_vals = [avg_dims.get(d, 0) for d in dims] + [avg_dims.get(dims[0], 0)]
    cats = dims + [dims[0]]

    color = BRIGHT_COLORS.get(ticker, "#6366f1")

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=avg_vals, theta=cats, name="Portfolio Avg",
        line=dict(color="#94a3b8", width=2, dash="dot"),
        fill="none", opacity=0.6,
    ))
    fig.add_trace(go.Scatterpolar(
        r=co_vals, theta=cats, name=ticker,
        line=dict(color=color, width=3),
        fill="toself", fillcolor=color, opacity=0.2,
    ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title=f"{ticker} vs Portfolio Average",
        height=420, margin=dict(t=60, b=40),
        legend=dict(font=dict(size=12)),
    )
    return fig


def ci_number_line(score: float, ci_lower: float, ci_upper: float, exp_low: float, exp_high: float, ticker: str) -> go.Figure:
    """Number line showing score point, CI range, and expected range band."""
    fig = go.Figure()

    # Expected range band
    fig.add_shape(
        type="rect", x0=exp_low, x1=exp_high, y0=-0.4, y1=0.4,
        fillcolor="rgba(59,130,246,0.10)", line=dict(width=1, color="rgba(59,130,246,0.3)", dash="dot"),
    )
    fig.add_annotation(x=(exp_low + exp_high) / 2, y=0.55, text=f"Expected: {exp_low}–{exp_high}",
                       showarrow=False, font=dict(size=10, color="#64748b"))

    # CI range bar
    fig.add_shape(
        type="line", x0=ci_lower, x1=ci_upper, y0=0, y1=0,
        line=dict(color="#475569", width=3),
    )
    # CI endpoints
    for x in [ci_lower, ci_upper]:
        fig.add_shape(type="line", x0=x, x1=x, y0=-0.15, y1=0.15, line=dict(color="#475569", width=2))

    # Score point
    fig.add_trace(go.Scatter(
        x=[score], y=[0], mode="markers+text",
        marker=dict(size=18, color=BRIGHT_COLORS.get(ticker, "#6366f1"), line=dict(width=2, color="white")),
        text=[f"{score:.1f}"], textposition="top center", textfont=dict(size=14, color="#1e293b"),
        showlegend=False,
        hovertemplate=f"<b>{ticker}</b><br>Score: {score:.1f}<br>95% CI: [{ci_lower:.1f}, {ci_upper:.1f}]<extra></extra>",
    ))

    fig.update_layout(
        title=f"{ticker} — Score with 95% Confidence Interval",
        xaxis=dict(title="Org-AI-R Score", range=[0, 105]),
        yaxis=dict(visible=False, range=[-0.8, 0.8]),
        height=180, margin=dict(t=50, b=40, l=50, r=50),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def peer_rank_bar(all_results: Dict, current_ticker: str) -> go.Figure:
    """Horizontal bar of all 5 companies, highlighting the selected one."""
    tickers = ["NVDA", "JPM", "WMT", "GE", "DG"]
    scores = []
    for t in tickers:
        r = all_results.get(t, {})
        scores.append(float(r.get("org_air_score", 0) or 0))

    # Sort by score descending
    paired = sorted(zip(tickers, scores), key=lambda x: -x[1])
    sorted_tickers = [p[0] for p in paired]
    sorted_scores = [p[1] for p in paired]

    colors = ["#6366f1" if t == current_ticker else "#d1d5db" for t in sorted_tickers]
    text_colors = ["#1e293b" if t == current_ticker else "#94a3b8" for t in sorted_tickers]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=sorted_tickers, x=sorted_scores, orientation="h",
        marker_color=colors,
        text=[f"{s:.1f}" for s in sorted_scores],
        textposition="outside",
        textfont=dict(size=13, color=text_colors),
    ))

    fig.update_layout(
        title=f"Portfolio Ranking — {current_ticker} highlighted",
        xaxis=dict(title="Org-AI-R", range=[0, 105]),
        yaxis=dict(autorange="reversed"),
        height=260, margin=dict(l=70, r=40, t=50, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig


def score_flow_chart(result: Dict, ticker: str) -> go.Figure:
    """Horizontal funnel/flow: Avg Dim → V^R → H^R → Synergy → Org-AI-R."""
    dims = result.get("dimension_scores", {}) or {}
    d_avg = sum(dims.values()) / len(dims) if dims else 0
    vr = float(result.get("vr_score", 0) or 0)
    hr = float(result.get("hr_score", 0) or 0)
    syn = float(result.get("synergy_score", 0) or 0)
    orgair = float(result.get("org_air_score", 0) or 0)

    stages = ["Avg Dimension", "V^R", "H^R", "Synergy", "Org-AI-R"]
    values = [round(d_avg, 1), round(vr, 1), round(hr, 1), round(syn, 1), round(orgair, 1)]
    colors = ["#94a3b8", "#6366f1", "#0ea5e9", "#10b981", "#1e293b"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=values, y=stages, orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}" for v in values],
        textposition="outside", textfont=dict(size=13),
    ))

    fig.update_layout(
        title=f"{ticker} — Score Pipeline Flow",
        xaxis=dict(title="Score", range=[0, 110]),
        yaxis=dict(autorange="reversed"),
        height=280, margin=dict(l=110, r=50, t=50, b=40),
        showlegend=False, plot_bgcolor="white",
    )
    return fig