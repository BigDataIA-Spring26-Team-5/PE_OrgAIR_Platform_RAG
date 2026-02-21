# tests/test_rubric_scorer.py
import unittest
from app.scoring.rubric_scorer import RubricScorer
class TestRubricScorer:
    def setUp(self):
        from app.scoring.rubric_scorer import RubricScorer
        self.rubric_scorer = RubricScorer()

    def test_score_positive_sentiment(self):
        from app.scoring.rubric_scorer import RubricScorer
        scorer = RubricScorer()
        result = scorer.score_dimension(
            "culture",
            "innovative data-driven forward-thinking experimental creative freedom",
            {"culture_metric": 0.5}
        )
        assert result.score >= 0
        assert result.score <= 100

    def test_score_neutral_sentiment(self):
        from app.scoring.rubric_scorer import RubricScorer
        scorer = RubricScorer()
        result = scorer.score_dimension(
            "culture",
            "standard workplace with normal processes",
            {"culture_metric": 0.3}
        )
        assert result.score >= 0
        assert result.score <= 100

    def test_score_no_reviews(self):
        from app.scoring.rubric_scorer import RubricScorer
        scorer = RubricScorer()
        result = scorer.score_dimension(
            "culture",
            "",
            {"culture_metric": 0.0}
        )
        assert result.score >= 0
        assert result.score <= 100