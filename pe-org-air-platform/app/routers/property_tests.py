"""
Property-Based Test Runner — Task 5.3
app/routers/property_tests.py

GET /api/v1/property-tests/run
  Triggers `pytest tests/test_property_based.py` (17 Hypothesis tests, 500 examples each),
  then reads test-results/test_cases_property_based.txt and returns structured JSON.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(
    prefix="/api/v1/property-tests",
    tags=["Property-Based Tests"],
)

# Resolve project root (pe-org-air-platform/) from this file's location
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_REPORT_FILE = _PROJECT_ROOT / "test_results" / "test_cases_property_based.txt"


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class TestResult(BaseModel):
    name: str
    status: str          # "PASS" or "WARN"
    examples_ran: int
    target: int
    top_examples: List[str]


class TestGroup(BaseModel):
    name: str
    tests: List[TestResult]


class PropertyTestReport(BaseModel):
    generated: Optional[str]
    target_examples: int
    total_tests: int
    tests_passed: int
    pytest_exit_code: int
    run_duration_seconds: float
    groups: List[TestGroup]


# ---------------------------------------------------------------------------
# Report parser
# ---------------------------------------------------------------------------

def _parse_report(text: str) -> dict:
    """Parse test_cases_property_based.txt into a structured dict."""
    lines = text.split("\n")

    generated = None
    for line in lines[:5]:
        if "Generated" in line and ":" in line:
            generated = line.split(":", 1)[1].strip()
            break

    groups: list = []
    current_group: Optional[dict] = None
    current_test: Optional[dict] = None
    total_tests = 0
    tests_passed = 0

    for line in lines:
        # Test result line: "  [PASS] test_name   500/500 examples ran"
        m = re.match(r"\s+\[(PASS|WARN)\]\s+(\S+)\s+(\d+)/(\d+)\s+examples\s+ran", line)
        if m:
            current_test = {
                "name": m.group(2),
                "status": m.group(1),
                "examples_ran": int(m.group(3)),
                "target": int(m.group(4)),
                "top_examples": [],
            }
            if current_group is not None:
                current_group["tests"].append(current_test)
            total_tests += 1
            if m.group(1) == "PASS":
                tests_passed += 1
            continue

        # Example line: "         Ex.1: tc=0  scores=[ ... ]"
        m2 = re.match(r"\s+Ex\.(\d+):\s+(.*)", line)
        if m2 and current_test is not None:
            current_test["top_examples"].append(m2.group(2).strip())
            continue

        # Group header: "  VRCalculator", "  EvidenceMapper", etc.
        m3 = re.match(r"^  ([A-Z][A-Za-z]+)$", line)
        if m3:
            if current_group is not None:
                groups.append(current_group)
            current_group = {"name": m3.group(1), "tests": []}
            current_test = None

    if current_group is not None:
        groups.append(current_group)

    return {
        "generated": generated,
        "target_examples": 500,
        "total_tests": total_tests,
        "tests_passed": tests_passed,
        "groups": groups,
    }


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/run",
    response_model=PropertyTestReport,
    summary="Run property-based tests and return JSON results",
    description=(
        "Triggers `pytest tests/test_property_based.py` — 17 Hypothesis property tests, "
        "500 examples each across VRCalculator, EvidenceMapper, SynergyCalculator, "
        "ConfidenceCalculator, and OrgAIRCalculator. "
        "After the suite completes, reads `test-results/test_cases_property_based.txt` "
        "and returns structured JSON with pass/fail status, example counts, and top 5 "
        "input examples per test. **Allow ~15 seconds for the suite to run.**"
    ),
)
def run_property_tests() -> PropertyTestReport:
    """Run the 17 Hypothesis property-based tests and return parsed results."""
    t0 = time.time()

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_property_based.py", "-q", "--tb=no"],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(_PROJECT_ROOT),
    )

    duration = round(time.time() - t0, 2)

    if not _REPORT_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail=(
                f"Report file was not generated. "
                f"pytest exit code: {proc.returncode}. "
                f"stderr: {proc.stderr[:500]}"
            ),
        )

    report_text = _REPORT_FILE.read_text(encoding="utf-8")
    parsed = _parse_report(report_text)

    return PropertyTestReport(
        generated=parsed["generated"],
        target_examples=parsed["target_examples"],
        total_tests=parsed["total_tests"],
        tests_passed=parsed["tests_passed"],
        pytest_exit_code=proc.returncode,
        run_duration_seconds=duration,
        groups=[
            TestGroup(
                name=g["name"],
                tests=[TestResult(**t) for t in g["tests"]],
            )
            for g in parsed["groups"]
        ],
    )
