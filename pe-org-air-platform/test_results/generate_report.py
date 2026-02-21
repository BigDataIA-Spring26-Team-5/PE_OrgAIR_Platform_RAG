"""Generate test_results/test_coverage_report.md from pytest outputs."""
import json
import xml.etree.ElementTree as ET
from datetime import datetime

# run by .\.venv\Scripts\python.exe test_results/generate_report.py
# running test coverage 
# .\.venv\Scripts\python.exe -m pytest tests/test_property_based.py tests/test_evidence_mapper.py tests/test_models.py tests/test_signals.py -k "not (TestRedisCache or test_api or test_talent_level_5 or test_missing_evidence_defaults or test_coverage_report or TestSignalsCollect or TestSignalsTask or TestListSignals or TestCompanySignals)" --cov=app/scoring --cov-report=term-missing --cov-report=json:test_results/coverage_raw.json --junitxml=test_results/test_results.xml -v

# Parse coverage
cov = json.load(open("test_results/coverage_raw.json"))
totals = cov["totals"]

# Parse test results
tree = ET.parse("test_results/test_results.xml")
root = tree.getroot()
ts = root.find("testsuite") or root
tests_run = int(ts.get("tests", 0))
failures = int(ts.get("failures", 0))
errors = int(ts.get("errors", 0))
passed = tests_run - failures - errors

# Build markdown
lines = [
    "# Test & Coverage Report",
    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    "",
    "## Test Results",
    "| Metric | Value |",
    "|--------|-------|",
    f"| Total Tests | {tests_run} |",
    f"| Passed | {passed} |",
    f"| Failed | {failures} |",
    f"| Errors | {errors} |",
    "",
    "## Coverage Summary",
    f"**Total Coverage: {totals['percent_covered']:.0f}%** ({totals['covered_lines']}/{totals['num_statements']} statements)",
    "",
    "| File | Stmts | Miss | Cover | Missing |",
    "|------|-------|------|-------|---------|",
]

for name, info in sorted(cov["files"].items()):
    s = info["summary"]
    short = name.replace("app\\scoring\\", "").replace("app/scoring/", "")
    missing = ", ".join(str(x) for x in info.get("missing_lines", []))
    lines.append(
        f"| {short} | {s['num_statements']} | {s['missing_lines']} | {s['percent_covered']:.0f}% | {missing} |"
    )

lines.append(
    f"| **TOTAL** | **{totals['num_statements']}** | **{totals['missing_lines']}** | **{totals['percent_covered']:.0f}%** | |"
)

with open("test_results/test_coverage_report.md", "w") as f:
    f.write("\n".join(lines))

print("\n".join(lines))
print("\nSaved to test_results/test_coverage_report.md")