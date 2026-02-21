"""Page: Testing & Coverage — Property-based tests + code coverage results."""

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests

from data_loader import API_BASE

# Paths to test result files (relative to streamlit/ parent = pe-org-air-platform/)
# _PROJECT_ROOT = Path(__file__).parent.parent
_STREAMLIT_DIR = Path(__file__).parent.parent  # streamlit/views/../ = streamlit/
_PROJECT_ROOT = _STREAMLIT_DIR.parent           # streamlit/../ = pe-org-air-platform/
_COVERAGE_RAW = _PROJECT_ROOT / "test_results" / "coverage_raw.json"
_TEST_XML = _PROJECT_ROOT / "test_results" / "test_results.xml"
_COVERAGE_MD = _PROJECT_ROOT / "test_results" / "test_coverage_report.md"
_PROPERTY_TXT = _PROJECT_ROOT / "test_results" / "test_cases_property_based.txt"
if not (_PROJECT_ROOT / "test_results").exists():
    _PROJECT_ROOT = Path.cwd()
if not (_PROJECT_ROOT / "test_results").exists():
    _PROJECT_ROOT = Path.cwd().parent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_coverage_json() -> dict | None:
    if _COVERAGE_RAW.exists():
        return json.loads(_COVERAGE_RAW.read_text())
    return None


def _load_test_xml() -> dict | None:
    if not _TEST_XML.exists():
        return None
    tree = ET.parse(str(_TEST_XML))
    root = tree.getroot()
    ts = root.find("testsuite") or root
    tests_run = int(ts.get("tests", 0))
    failures = int(ts.get("failures", 0))
    errors = int(ts.get("errors", 0))
    skipped = int(ts.get("skipped", 0))
    passed = tests_run - failures - errors - skipped
    time_taken = float(ts.get("time", 0))

    # Parse individual test cases
    test_cases = []
    for tc in root.iter("testcase"):
        status = "✅ Passed"
        if tc.find("failure") is not None:
            status = "❌ Failed"
        elif tc.find("error") is not None:
            status = "⚠️ Error"
        elif tc.find("skipped") is not None:
            status = "⏭️ Skipped"

        test_cases.append({
            "Class": tc.get("classname", "").split(".")[-1],
            "Test": tc.get("name", ""),
            "Time (s)": round(float(tc.get("time", 0)), 3),
            "Status": status,
        })

    return {
        "tests_run": tests_run,
        "passed": passed,
        "failed": failures,
        "errors": errors,
        "skipped": skipped,
        "time": round(time_taken, 2),
        "test_cases": test_cases,
    }


def _load_property_txt() -> str | None:
    if _PROPERTY_TXT.exists():
        return _PROPERTY_TXT.read_text(encoding="utf-8")
    return None


def _call_property_test_api() -> dict | None:
    """Call the FastAPI property test runner endpoint."""
    try:
        r = requests.get(f"{API_BASE}/api/v1/property-tests/run", timeout=120)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render():
    st.title("🧪 Testing & Coverage")
    st.caption("Property-based tests (Hypothesis, 500 examples) and scoring engine code coverage")

    # ══════════════════════════════════════════════════════════════════════
    # Mode selector
    # ══════════════════════════════════════════════════════════════════════
    mode = st.radio(
        "Data Source",
        ["📂 Load from saved results", "🚀 Run tests live (via API)"],
        horizontal=True,
    )

    st.divider()

    if mode == "🚀 Run tests live (via API)":
        _render_live_mode()
    else:
        _render_saved_mode()


# ---------------------------------------------------------------------------
# Saved mode — read from test_results/ files
# ---------------------------------------------------------------------------

def _render_saved_mode():
    # ── Section 1: Test Summary ──
    st.markdown("## 1. Test Results Summary")

    xml_data = _load_test_xml()
    if xml_data:
        st.markdown(
            f"Loaded from `test_results/test_results.xml` — "
            f"**{xml_data['tests_run']} tests** in **{xml_data['time']}s**"
        )

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total", xml_data["tests_run"])
        c2.metric("Passed", xml_data["passed"], delta="✅")
        c3.metric("Failed", xml_data["failed"],
                  delta="🔴" if xml_data["failed"] > 0 else "✅")
        c4.metric("Errors", xml_data["errors"],
                  delta="🔴" if xml_data["errors"] > 0 else "✅")
        c5.metric("Skipped", xml_data["skipped"])

        # Pass rate gauge
        pass_rate = (xml_data["passed"] / max(xml_data["tests_run"], 1)) * 100
        _fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pass_rate,
            title={"text": "Pass Rate"},
            number={"suffix": "%"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#10b981" if pass_rate >= 90 else "#f59e0b"},
                "steps": [
                    {"range": [0, 50], "color": "#fee2e2"},
                    {"range": [50, 80], "color": "#fef3c7"},
                    {"range": [80, 100], "color": "#d1fae5"},
                ],
                "threshold": {"line": {"color": "#059669", "width": 3}, "value": 90},
            },
        ))
        _fig_gauge.update_layout(height=250, margin=dict(t=50, b=20))
        st.plotly_chart(_fig_gauge, use_container_width=True, key="test_gauge")

        # Test cases table
        with st.expander(f"View all {xml_data['tests_run']} test cases"):
            tc_df = pd.DataFrame(xml_data["test_cases"])
            st.dataframe(tc_df, use_container_width=True, hide_index=True)
    else:
        st.warning(
            "No test results found. Run tests first:\n\n"
            "```\n"
            ".\\.venv\\Scripts\\python.exe -m pytest tests/test_property_based.py "
            "tests/test_evidence_mapper.py tests/test_models.py tests/test_signals.py "
            "--junitxml=test_results/test_results.xml -v\n"
            "```"
        )

    st.divider()

    # ── Section 2: Code Coverage ──
    st.markdown("## 2. Code Coverage (Scoring Engine)")

    cov_data = _load_coverage_json()
    if cov_data:
        totals = cov_data["totals"]
        cov_pct = totals["percent_covered"]
        target = 80

        st.markdown(
            f"Loaded from `test_results/coverage_raw.json` — "
            f"**{cov_pct:.0f}%** coverage ({totals['covered_lines']}/{totals['num_statements']} statements)"
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Coverage", f"{cov_pct:.0f}%",
                  delta="✅ Above 80%" if cov_pct >= target else "🔴 Below 80%")
        c2.metric("Statements", totals["num_statements"])
        c3.metric("Covered", totals["covered_lines"])
        c4.metric("Missing", totals["missing_lines"])

        # Coverage bar chart per file
        rows = []
        for name, info in sorted(cov_data["files"].items()):
            s = info["summary"]
            short = name.replace("app\\scoring\\", "").replace("app/scoring/", "")
            rows.append({
                "File": short,
                "Coverage": s["percent_covered"],
                "Statements": s["num_statements"],
                "Missing": s["missing_lines"],
            })

        cov_df = pd.DataFrame(rows)

        _fig_cov = go.Figure()
        colors = ["#10b981" if c >= 80 else ("#f59e0b" if c >= 60 else "#ef4444")
                  for c in cov_df["Coverage"]]
        _fig_cov.add_trace(go.Bar(
            x=cov_df["File"], y=cov_df["Coverage"],
            marker_color=colors,
            text=[f"{c:.0f}%" for c in cov_df["Coverage"]],
            textposition="outside", textfont=dict(size=12),
        ))
        _fig_cov.add_hline(y=80, line_dash="dash", line_color="#6366f1",
                           annotation_text="80% target", annotation_position="top left")
        _fig_cov.update_layout(
            title="Coverage by Scoring Module",
            yaxis=dict(title="Coverage %", range=[0, 110]),
            height=350, margin=dict(t=50, b=80),
            showlegend=False, plot_bgcolor="white",
        )
        st.plotly_chart(_fig_cov, use_container_width=True, key="cov_bar")

        # Coverage table
        st.dataframe(cov_df, use_container_width=True, hide_index=True)
    else:
        st.warning(
            "No coverage data found. Run with coverage:\n\n"
            "```\n"
            ".\\.venv\\Scripts\\python.exe -m pytest tests/test_property_based.py "
            "--cov=app/scoring --cov-report=json:test_results/coverage_raw.json -v\n"
            "```"
        )

    st.divider()

    # ── Section 3: Property-Based Test Details ──
    st.markdown("## 3. Property-Based Tests (Hypothesis)")
    st.markdown(
        "CS3 requires **17 property-based tests** running **500 examples each** using Hypothesis. "
        "These tests verify mathematical properties of the scoring engine — bounds, monotonicity, "
        "determinism — across randomly generated inputs."
    )

    # Show property test groups from XML data
    if xml_data:
        prop_tests = [t for t in xml_data["test_cases"]
                     if "property" in t["Class"].lower() or "PropertyBased" in t["Class"]]
        if prop_tests:
            prop_df = pd.DataFrame(prop_tests)

            # Group summary
            groups = {}
            for t in prop_tests:
                cls = t["Class"]
                if cls not in groups:
                    groups[cls] = {"total": 0, "passed": 0}
                groups[cls]["total"] += 1
                if "Passed" in t["Status"]:
                    groups[cls]["passed"] += 1

            group_rows = []
            for cls, counts in groups.items():
                clean_name = cls.replace("TestPropertyBased", "").replace("PropertyBased", "")
                group_rows.append({
                    "Test Group": clean_name or cls,
                    "Tests": counts["total"],
                    "Passed": counts["passed"],
                    "Examples (each)": 500,
                    "Total Examples": counts["total"] * 500,
                    "Status": "✅" if counts["passed"] == counts["total"] else "❌",
                })
            group_df = pd.DataFrame(group_rows)

            total_examples = sum(r["Total Examples"] for r in group_rows)
            total_tests = sum(r["Tests"] for r in group_rows)

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Property Tests", total_tests)
            mc2.metric("Examples per Test", 500)
            mc3.metric("Total Examples Run", f"{total_examples:,}")

            st.dataframe(group_df, use_container_width=True, hide_index=True)

            with st.expander("View individual property test results"):
                st.dataframe(prop_df, use_container_width=True, hide_index=True)

    # Show raw property test report if available
    prop_txt = _load_property_txt()
    if prop_txt:
        with st.expander("View raw property test report"):
            st.code(prop_txt, language="text")

    st.divider()

    # ── Section 4: Coverage Report (Markdown) ──
    if _COVERAGE_MD.exists():
        with st.expander("View full coverage report (Markdown)"):
            st.markdown(_COVERAGE_MD.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Live mode — run via API
# ---------------------------------------------------------------------------

def _render_live_mode():
    st.markdown("## Run Tests via API")
    st.markdown(
        f"This will call `{API_BASE}/api/v1/property-tests/run` to execute "
        f"17 Hypothesis property tests (500 examples each). **This takes ~15-60 seconds.**"
    )

    col_btn1, col_btn2 = st.columns(2)

    with col_btn1:
        if st.button("🧪 Run Property Tests", type="primary"):
            with st.spinner("Running 17 property tests × 500 examples..."):
                result = _call_property_test_api()

            if result:
                st.success(
                    f"✅ Completed in {result.get('run_duration_seconds', '?')}s — "
                    f"{result.get('tests_passed', 0)}/{result.get('total_tests', 0)} passed"
                )

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Tests", result.get("total_tests", 0))
                c2.metric("Passed", result.get("tests_passed", 0))
                c3.metric("Exit Code", result.get("pytest_exit_code", "?"))
                c4.metric("Duration", f"{result.get('run_duration_seconds', 0)}s")

                # Show groups
                for group in result.get("groups", []):
                    st.markdown(f"### {group['name']}")
                    rows = []
                    for t in group.get("tests", []):
                        rows.append({
                            "Test": t["name"],
                            "Status": "✅ PASS" if t["status"] == "PASS" else "⚠️ WARN",
                            "Examples": f"{t['examples_ran']}/{t['target']}",
                        })
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    # Show example inputs
                    for t in group.get("tests", []):
                        if t.get("top_examples"):
                            with st.expander(f"Example inputs for {t['name']}"):
                                for ex in t["top_examples"]:
                                    st.code(ex, language="text")
            else:
                st.error(
                    f"❌ Could not reach the API at `{API_BASE}/api/v1/property-tests/run`. "
                    f"Make sure the FastAPI server is running."
                )

    with col_btn2:
        spacer = st.empty()
        spacer.write("")  # empty row

        st.info(
            "💡 **Tip:** If the API is not running, use **Load from saved results** mode instead. "
            "The saved results were generated during the last local test run."
        )