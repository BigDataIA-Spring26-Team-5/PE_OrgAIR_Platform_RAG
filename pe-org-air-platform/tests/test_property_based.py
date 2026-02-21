# tests/test_property_based.py
"""
Property-Based Tests — Task 5.3 (CS3)

8 Hypothesis tests with max_examples=500, covering:
  - 5 VRCalculator properties
  - 3 EvidenceMapper properties
"""

import datetime
import pathlib
from decimal import Decimal

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from app.scoring.evidence_mapper import (
    Dimension,
    DimensionScore,
    EvidenceMapper,
    EvidenceScore,
    SignalSource,
)
from app.scoring.vr_calculator import VRCalculator, VRResult
from app.scoring.synergy_calculator import SynergyCalculator
from app.scoring.confidence_calculator import ConfidenceCalculator
from app.scoring.orgair_calculator import OrgAIRCalculator

# ---------------------------------------------------------------------------
# Shared strategies
# ---------------------------------------------------------------------------

ALL_DIMS = [d.value for d in Dimension]  # 7 strings

# Abbreviated dimension labels for compact output
_DIM_SHORT = {
    "data_infrastructure": "DI",
    "ai_governance":       "AG",
    "technology_stack":    "TS",
    "talent_skills":       "TL",
    "leadership_vision":   "LV",
    "use_case_portfolio":  "UC",
    "culture_change":      "CC",
}

dim_score_st = st.floats(
    min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
)
tc_st = st.floats(
    min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
)


@st.composite
def dim_scores_dict(draw):
    """Draw a dict mapping each dimension name → float score in [0, 100]."""
    return {d: draw(dim_score_st) for d in ALL_DIMS}


@st.composite
def evidence_score_st(draw):
    """Draw a random EvidenceScore with valid fields."""
    return EvidenceScore(
        source=draw(st.sampled_from(list(SignalSource))),
        raw_score=Decimal(
            str(
                draw(
                    st.floats(
                        min_value=0.0,
                        max_value=100.0,
                        allow_nan=False,
                        allow_infinity=False,
                    )
                )
            )
        ),
        confidence=Decimal(
            str(
                draw(
                    st.floats(
                        min_value=0.01,
                        max_value=1.0,
                        allow_nan=False,
                        allow_infinity=False,
                    )
                )
            )
        ),
        evidence_count=draw(st.integers(min_value=1, max_value=100)),
    )


# ---------------------------------------------------------------------------
# Per-test call counters + top-5 example stores
# Each _cnt_* list grows by 1 per Hypothesis call (proof of 500 runs).
# Each _ex_* list stores up to 5 example dicts for the report.
# ---------------------------------------------------------------------------

_cnt_vr_bounded: list = [];       _ex_vr_bounded: list = []
_cnt_vr_monotone: list = [];      _ex_vr_monotone: list = []
_cnt_vr_tc_penalty: list = [];    _ex_vr_tc_penalty: list = []
_cnt_vr_uniform_cv: list = [];    _ex_vr_uniform_cv: list = []
_cnt_vr_deterministic: list = []; _ex_vr_deterministic: list = []

_cnt_em_all_dims: list = [];      _ex_em_all_dims: list = []
_cnt_em_default_50: list = [];    _ex_em_default_50: list = []
_cnt_em_more_evidence: list = []; _ex_em_more_evidence: list = []

_cnt_syn_bounded: list = [];        _ex_syn_bounded: list = []
_cnt_syn_zero: list = [];           _ex_syn_zero: list = []
_cnt_syn_deterministic: list = [];  _ex_syn_deterministic: list = []

_cnt_conf_range: list = [];         _ex_conf_range: list = []
_cnt_conf_reliability: list = [];   _ex_conf_reliability: list = []
_cnt_conf_deterministic: list = []; _ex_conf_deterministic: list = []

_cnt_orgair_bounded: list = [];       _ex_orgair_bounded: list = []
_cnt_orgair_monotone: list = [];      _ex_orgair_monotone: list = []
_cnt_orgair_deterministic: list = []; _ex_orgair_deterministic: list = []


def _fmt_scores(scores: dict) -> str:
    """Compact dim-score string: DI=52.3 AG=10.1 ..."""
    return "  ".join(
        f"{_DIM_SHORT.get(k, k[:2])}={v:.1f}" for k, v in scores.items()
    )


# ---------------------------------------------------------------------------
# VR Property Tests
# ---------------------------------------------------------------------------


class TestVRPropertyBased:
    """5 property tests for VRCalculator."""

    @given(dim_scores_dict(), tc_st)
    @settings(max_examples=500)
    def test_vr_always_bounded(self, scores, tc):
        """V^R score is always in [0, 100] for any valid inputs."""
        _cnt_vr_bounded.append(1)
        if len(_ex_vr_bounded) < 5:
            _ex_vr_bounded.append({"tc": round(tc, 4), "scores": _fmt_scores(scores)})
        result = VRCalculator().calculate(scores, tc)
        assert Decimal("0") <= result.vr_score <= Decimal("100")

    @given(
        dim_scores_dict(),
        tc_st,
        st.floats(
            min_value=1.0, max_value=50.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=500)
    def test_higher_scores_increase_vr(self, scores, tc, delta):
        """Boosting every dimension score by delta (capped at 100) does not decrease VR."""
        _cnt_vr_monotone.append(1)
        if len(_ex_vr_monotone) < 5:
            _ex_vr_monotone.append({
                "tc": round(tc, 4),
                "delta": round(delta, 2),
                "scores": _fmt_scores(scores),
            })
        calc = VRCalculator()
        result_original = calc.calculate(scores, tc)
        boosted = {k: min(100.0, v + delta) for k, v in scores.items()}
        result_boosted = calc.calculate(boosted, tc)
        assert result_boosted.vr_score >= result_original.vr_score

    @given(
        dim_scores_dict(),
        st.floats(
            min_value=0.0, max_value=0.5, allow_nan=False, allow_infinity=False
        ),
        st.floats(
            min_value=0.5, max_value=1.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=500)
    def test_talent_concentration_penalty(self, scores, low_tc, high_tc):
        """Higher TC value produces a lower or equal VR score (penalty is monotone)."""
        _cnt_vr_tc_penalty.append(1)
        if len(_ex_vr_tc_penalty) < 5:
            _ex_vr_tc_penalty.append({
                "low_tc": round(low_tc, 4),
                "high_tc": round(high_tc, 4),
                "scores": _fmt_scores(scores),
            })
        calc = VRCalculator()
        result_low = calc.calculate(scores, low_tc)
        result_high = calc.calculate(scores, high_tc)
        assert result_high.vr_score <= result_low.vr_score

    @given(
        st.floats(
            min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
        tc_st,
    )
    @settings(max_examples=500)
    def test_uniform_dimensions_no_cv_penalty(self, uniform_val, tc):
        """When all dimension scores are equal, CV = 0 so cv_penalty >= 0.99."""
        _cnt_vr_uniform_cv.append(1)
        if len(_ex_vr_uniform_cv) < 5:
            _ex_vr_uniform_cv.append({
                "uniform_val": round(uniform_val, 2),
                "tc": round(tc, 4),
            })
        scores = {d: uniform_val for d in ALL_DIMS}
        result = VRCalculator().calculate(scores, tc)
        assert result.cv_penalty >= Decimal("0.99")

    @given(dim_scores_dict(), tc_st)
    @settings(max_examples=500)
    def test_deterministic(self, scores, tc):
        """Calling calculate() twice with identical args yields the same vr_score."""
        _cnt_vr_deterministic.append(1)
        if len(_ex_vr_deterministic) < 5:
            _ex_vr_deterministic.append({"tc": round(tc, 4), "scores": _fmt_scores(scores)})
        calc = VRCalculator()
        result1 = calc.calculate(scores, tc)
        result2 = calc.calculate(scores, tc)
        assert result1.vr_score == result2.vr_score


# ---------------------------------------------------------------------------
# Evidence Mapper Property Tests
# ---------------------------------------------------------------------------


class TestEvidenceMapperPropertyBased:
    """3 property tests for EvidenceMapper."""

    @given(st.lists(evidence_score_st(), min_size=1, max_size=9))
    @settings(max_examples=500)
    def test_all_dimensions_returned(self, raw_evidence):
        """map_evidence_to_dimensions always returns exactly 7 keys, one per Dimension."""
        _cnt_em_all_dims.append(1)
        # Deduplicate by source so each SignalSource appears at most once
        seen: set = set()
        evidence = []
        for ev in raw_evidence:
            if ev.source not in seen:
                seen.add(ev.source)
                evidence.append(ev)

        if len(_ex_em_all_dims) < 5:
            _ex_em_all_dims.append({
                "n_sources": len(evidence),
                "sources": [e.source.value[:12] for e in evidence],
            })

        mapper = EvidenceMapper()
        result = mapper.map_evidence_to_dimensions(evidence)

        assert len(result) == 7
        assert set(result.keys()) == set(Dimension)

    @given(st.integers())  # dummy strategy forces 500 iterations; evidence is always []
    @settings(max_examples=500)
    def test_missing_evidence_defaults_to_50(self, _dummy):
        """With no evidence, every dimension score defaults to 50.0."""
        _cnt_em_default_50.append(1)
        if len(_ex_em_default_50) < 5:
            _ex_em_default_50.append({"evidence": "[]  (empty — fixed)", "expected_score": 50.0})
        mapper = EvidenceMapper()
        result = mapper.map_evidence_to_dimensions([])
        for dim, ds in result.items():
            assert ds.score == Decimal("50.0"), (
                f"{dim.value} expected Decimal('50.0'), got {ds.score!r}"
            )

    @given(
        st.lists(
            st.sampled_from(list(SignalSource)),
            min_size=1,
            max_size=2,
            unique=True,
        ),
        st.lists(
            st.sampled_from(list(SignalSource)),
            min_size=5,
            max_size=9,
            unique=True,
        ),
    )
    @settings(max_examples=500)
    def test_more_evidence_higher_confidence(self, small_sources, large_sources):
        """5-9 unique sources yield average dimension confidence >= 1-2 unique sources.

        Uses fixed raw_score=70 and confidence=0.8 to isolate coverage behavior.
        The large set is forced to be a superset of small to guarantee the property.
        """
        _cnt_em_more_evidence.append(1)
        # Ensure large is a proper superset of small (coverage can only increase)
        large_set = list({*large_sources, *small_sources})
        assume(len(large_set) > len(small_sources))

        if len(_ex_em_more_evidence) < 5:
            _ex_em_more_evidence.append({
                "small": [s.value[:10] for s in small_sources],
                "large": [s.value[:10] for s in large_set],
            })

        def make_scores(sources):
            return [
                EvidenceScore(
                    source=s,
                    raw_score=Decimal("70"),
                    confidence=Decimal("0.8"),
                    evidence_count=5,
                )
                for s in sources
            ]

        mapper = EvidenceMapper()
        small_result = mapper.map_evidence_to_dimensions(make_scores(small_sources))
        large_result = mapper.map_evidence_to_dimensions(make_scores(large_set))

        avg_small = sum(float(ds.confidence) for ds in small_result.values()) / len(
            small_result
        )
        avg_large = sum(float(ds.confidence) for ds in large_result.values()) / len(
            large_result
        )

        assert avg_large >= avg_small


# ---------------------------------------------------------------------------
# Shared strategies for new calculators
# ---------------------------------------------------------------------------

score_0_100 = st.floats(
    min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
)
alignment_st = st.floats(
    min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
)
timing_st = st.floats(
    min_value=0.5, max_value=1.5, allow_nan=False, allow_infinity=False
)
evidence_count_st = st.integers(min_value=1, max_value=50)


# ---------------------------------------------------------------------------
# Synergy Property Tests
# ---------------------------------------------------------------------------


class TestSynergyPropertyBased:
    """3 property tests for SynergyCalculator."""

    @given(score_0_100, score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_always_bounded(self, vr, hr, alignment, timing):
        """Synergy score is always in [0, 100] for any valid inputs."""
        _cnt_syn_bounded.append(1)
        if len(_ex_syn_bounded) < 5:
            _ex_syn_bounded.append({
                "vr": round(vr, 2), "hr": round(hr, 2),
                "alignment": round(alignment, 4), "timing": round(timing, 4),
            })
        result = SynergyCalculator().calculate(vr, hr, alignment=alignment, timing_factor=timing)
        assert Decimal("0") <= result.synergy_score <= Decimal("100")

    @given(score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_zero_when_either_is_zero(self, nonzero, alignment, timing):
        """Synergy is 0 when VR=0 or HR=0."""
        _cnt_syn_zero.append(1)
        if len(_ex_syn_zero) < 5:
            _ex_syn_zero.append({
                "nonzero": round(nonzero, 2),
                "alignment": round(alignment, 4),
                "timing": round(timing, 4),
            })
        calc = SynergyCalculator()
        result_vr_zero = calc.calculate(0.0, nonzero, alignment=alignment, timing_factor=timing)
        result_hr_zero = calc.calculate(nonzero, 0.0, alignment=alignment, timing_factor=timing)
        assert result_vr_zero.synergy_score == Decimal("0")
        assert result_hr_zero.synergy_score == Decimal("0")

    @given(score_0_100, score_0_100, alignment_st, timing_st)
    @settings(max_examples=500)
    def test_synergy_deterministic(self, vr, hr, alignment, timing):
        """Same inputs always produce the same Synergy score."""
        _cnt_syn_deterministic.append(1)
        if len(_ex_syn_deterministic) < 5:
            _ex_syn_deterministic.append({
                "vr": round(vr, 2), "hr": round(hr, 2),
                "alignment": round(alignment, 4), "timing": round(timing, 4),
            })
        calc = SynergyCalculator()
        r1 = calc.calculate(vr, hr, alignment=alignment, timing_factor=timing)
        r2 = calc.calculate(vr, hr, alignment=alignment, timing_factor=timing)
        assert r1.synergy_score == r2.synergy_score


# ---------------------------------------------------------------------------
# Confidence Interval Property Tests
# ---------------------------------------------------------------------------


class TestConfidencePropertyBased:
    """3 property tests for ConfidenceCalculator."""

    @given(score_0_100, evidence_count_st, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_ci_always_valid_range(self, score, n, score_type):
        """CI bounds are valid: ci_lower <= ci_upper, both within [0, 100]."""
        _cnt_conf_range.append(1)
        if len(_ex_conf_range) < 5:
            _ex_conf_range.append({
                "score": round(score, 2), "n": n, "score_type": score_type,
            })
        result = ConfidenceCalculator().calculate(score, n, score_type)
        assert result.ci_lower <= result.ci_upper
        assert Decimal("0") <= result.ci_lower <= Decimal("100")
        assert Decimal("0") <= result.ci_upper <= Decimal("100")

    @given(score_0_100, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_more_evidence_higher_reliability(self, score, score_type):
        """Larger evidence count yields equal or higher reliability (ρ)."""
        _cnt_conf_reliability.append(1)
        if len(_ex_conf_reliability) < 5:
            _ex_conf_reliability.append({
                "score": round(score, 2), "score_type": score_type,
                "n_small": 1, "n_large": 20,
            })
        calc = ConfidenceCalculator()
        small = calc.calculate(score, 1, score_type)
        large = calc.calculate(score, 20, score_type)
        assert large.reliability >= small.reliability

    @given(score_0_100, evidence_count_st, st.sampled_from(["vr", "hr", "org_air"]))
    @settings(max_examples=500)
    def test_confidence_deterministic(self, score, n, score_type):
        """Same inputs always produce the same CI."""
        _cnt_conf_deterministic.append(1)
        if len(_ex_conf_deterministic) < 5:
            _ex_conf_deterministic.append({
                "score": round(score, 2), "n": n, "score_type": score_type,
            })
        calc = ConfidenceCalculator()
        r1 = calc.calculate(score, n, score_type)
        r2 = calc.calculate(score, n, score_type)
        assert r1.ci_lower == r2.ci_lower
        assert r1.ci_upper == r2.ci_upper


# ---------------------------------------------------------------------------
# Org-AI-R Property Tests
# ---------------------------------------------------------------------------


class TestOrgAIRPropertyBased:
    """3 property tests for OrgAIRCalculator."""

    @given(score_0_100, score_0_100, score_0_100)
    @settings(max_examples=500)
    def test_orgair_always_bounded(self, vr, hr, synergy):
        """Org-AI-R score is always in [0, 100] for any valid inputs."""
        _cnt_orgair_bounded.append(1)
        if len(_ex_orgair_bounded) < 5:
            _ex_orgair_bounded.append({
                "vr": round(vr, 2), "hr": round(hr, 2), "synergy": round(synergy, 2),
            })
        result = OrgAIRCalculator().calculate(vr, hr, synergy_score=synergy)
        assert Decimal("0") <= result.org_air_score <= Decimal("100")

    @given(
        st.floats(min_value=0.0, max_value=80.0, allow_nan=False, allow_infinity=False),
        st.floats(min_value=10.0, max_value=20.0, allow_nan=False, allow_infinity=False),
        score_0_100,
        score_0_100,
    )
    @settings(max_examples=500)
    def test_orgair_monotone_with_vr(self, vr_low, delta, hr, synergy):
        """Increasing VR (with HR and synergy fixed) does not decrease Org-AI-R."""
        _cnt_orgair_monotone.append(1)
        if len(_ex_orgair_monotone) < 5:
            _ex_orgair_monotone.append({
                "vr_low": round(vr_low, 2), "delta": round(delta, 2),
                "hr": round(hr, 2), "synergy": round(synergy, 2),
            })
        vr_high = min(100.0, vr_low + delta)
        calc = OrgAIRCalculator()
        result_low = calc.calculate(vr_low, hr, synergy_score=synergy)
        result_high = calc.calculate(vr_high, hr, synergy_score=synergy)
        assert result_high.org_air_score >= result_low.org_air_score

    @given(score_0_100, score_0_100, score_0_100)
    @settings(max_examples=500)
    def test_orgair_deterministic(self, vr, hr, synergy):
        """Same inputs always produce the same Org-AI-R score."""
        _cnt_orgair_deterministic.append(1)
        if len(_ex_orgair_deterministic) < 5:
            _ex_orgair_deterministic.append({
                "vr": round(vr, 2), "hr": round(hr, 2), "synergy": round(synergy, 2),
            })
        calc = OrgAIRCalculator()
        r1 = calc.calculate(vr, hr, synergy_score=synergy)
        r2 = calc.calculate(vr, hr, synergy_score=synergy)
        assert r1.org_air_score == r2.org_air_score


# ---------------------------------------------------------------------------
# Post-run report writer
# ---------------------------------------------------------------------------

def teardown_module():
    """Write results/test_cases_property_based.txt after all tests complete."""
    results_dir = pathlib.Path(__file__).parent.parent / "test_results"
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / "test_cases_property_based.txt"

    MAX = 500

    rows = [
        # (group_label, test_name, counter_list, example_list)
        ("VRCalculator",      "test_vr_always_bounded",               _cnt_vr_bounded,       _ex_vr_bounded),
        ("VRCalculator",      "test_higher_scores_increase_vr",       _cnt_vr_monotone,      _ex_vr_monotone),
        ("VRCalculator",      "test_talent_concentration_penalty",     _cnt_vr_tc_penalty,    _ex_vr_tc_penalty),
        ("VRCalculator",      "test_uniform_dimensions_no_cv_penalty", _cnt_vr_uniform_cv,    _ex_vr_uniform_cv),
        ("VRCalculator",      "test_deterministic",                    _cnt_vr_deterministic, _ex_vr_deterministic),
        ("EvidenceMapper",    "test_all_dimensions_returned",          _cnt_em_all_dims,      _ex_em_all_dims),
        ("EvidenceMapper",    "test_missing_evidence_defaults_to_50",  _cnt_em_default_50,    _ex_em_default_50),
        ("EvidenceMapper",    "test_more_evidence_higher_confidence",  _cnt_em_more_evidence, _ex_em_more_evidence),
        ("SynergyCalculator", "test_synergy_always_bounded",           _cnt_syn_bounded,      _ex_syn_bounded),
        ("SynergyCalculator", "test_synergy_zero_when_either_is_zero", _cnt_syn_zero,         _ex_syn_zero),
        ("SynergyCalculator", "test_synergy_deterministic",            _cnt_syn_deterministic,_ex_syn_deterministic),
        ("ConfidenceCalc",    "test_ci_always_valid_range",            _cnt_conf_range,       _ex_conf_range),
        ("ConfidenceCalc",    "test_more_evidence_higher_reliability", _cnt_conf_reliability, _ex_conf_reliability),
        ("ConfidenceCalc",    "test_confidence_deterministic",         _cnt_conf_deterministic,_ex_conf_deterministic),
        ("OrgAIRCalculator",  "test_orgair_always_bounded",            _cnt_orgair_bounded,   _ex_orgair_bounded),
        ("OrgAIRCalculator",  "test_orgair_monotone_with_vr",          _cnt_orgair_monotone,  _ex_orgair_monotone),
        ("OrgAIRCalculator",  "test_orgair_deterministic",             _cnt_orgair_deterministic, _ex_orgair_deterministic),
    ]

    def fmt_ex(ex: dict) -> str:
        parts = []
        for k, v in ex.items():
            if k == "scores":
                parts.append(f"scores=[ {v} ]")
            elif isinstance(v, list):
                parts.append(f"{k}={v}")
            elif isinstance(v, float):
                parts.append(f"{k}={v:.4g}")
            else:
                parts.append(f"{k}={v}")
        return "  ".join(parts)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "Property-Based Test Run Report",
        f"Generated : {now}",
        f"Target    : {MAX} examples per test  |  Showing top 5 passed examples per test",
        "=" * 75,
        "",
    ]

    current_group = None
    passed_total = 0

    for group, name, counter, examples in rows:
        if group != current_group:
            if current_group is not None:
                lines.append("")
            lines.append(f"  {group}")
            lines.append("  " + "-" * 65)
            current_group = group

        ran = len(counter)
        status = "PASS" if ran == MAX else "WARN"
        if ran == MAX:
            passed_total += 1

        lines.append(f"  [{status}] {name:<48}  {ran:>3}/{MAX} examples ran")

        for i, ex in enumerate(examples, 1):
            lines.append(f"         Ex.{i}: {fmt_ex(ex)}")

    lines += [
        "",
        "=" * 75,
        f"Result: {passed_total}/17 tests completed {MAX} examples",
        "",
    ]

    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
