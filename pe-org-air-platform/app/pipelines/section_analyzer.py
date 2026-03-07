import re
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class SectionStats:
    """Statistics for a single section"""
    section_name: str
    word_count: int
    keywords: Dict[str, int]


@dataclass
class DocumentAnalysis:
    """Analysis results for a single document"""
    document_id: str
    ticker: str
    filing_type: str
    filing_date: str
    total_word_count: int
    sections: List[SectionStats]
    total_keywords: Dict[str, int]


class SectionAnalyzer:
    """Analyze SEC filing sections for word counts and keyword mentions"""
    
    # AI-related keywords to search for
    AI_KEYWORDS = [
        "artificial intelligence",
        "machine learning", 
        "deep learning",
        "neural network",
        "natural language processing",
        "nlp",
        "computer vision",
        "robotics",
        "predictive analytics",
        "generative ai",
        "large language model",
        "llm",
        "chatgpt",
        "ai-powered",
        "ai-driven",
    ]
    
    TECH_KEYWORDS = [
        "automation",
        "digital",
        "cloud",
        "data analytics",
        "cybersecurity",
        "blockchain",
        "iot",
        "internet of things",
        "software",
        "algorithm",
        "api",
        "saas",
        "platform",
        "digital transformation",
    ]
    
    # Friendly names for sections
    SECTION_DISPLAY_NAMES = {
        "item_1_business": "Business",
        "item_1a_risk_factors": "Risk Factors",
        "item_7_mda": "MD&A",
        "item_7a_quantitative": "Quantitative",
        "item_8_financial": "Financial Statements",
        "item_9a_controls": "Controls",
        "item_2_mda": "MD&A (10-Q)",
        "item_8_01_other": "Other Events",
        "executive_compensation": "Exec Compensation",
        "director_compensation": "Director Compensation",
    }
    
    def __init__(self):
        # Combine all keywords for searching
        self.all_keywords = {
            "ai": self.AI_KEYWORDS,
            "tech": self.TECH_KEYWORDS
        }
        logger.info("📊 Section Analyzer initialized")
    
    def count_keywords(self, text: str) -> Dict[str, int]:
        """Count occurrences of each keyword in text"""
        return self._count_keywords_with_list(text, self.AI_KEYWORDS)

    def _count_keywords_with_list(self, text: str, ai_keywords: List[str]) -> Dict[str, int]:
        """Count occurrences of the given ai_keywords list plus TECH_KEYWORDS in text."""
        text_lower = text.lower()
        counts = {}

        for keyword in ai_keywords:
            count = len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
            if count > 0:
                counts[keyword] = count

        for keyword in self.TECH_KEYWORDS:
            count = len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
            if count > 0:
                counts[keyword] = count

        return counts
    
    def get_keyword_summary(self, keyword_counts: Dict[str, int]) -> Dict[str, int]:
        """Summarize keywords into categories"""
        ai_total = sum(keyword_counts.get(kw, 0) for kw in self.AI_KEYWORDS)
        tech_total = sum(keyword_counts.get(kw, 0) for kw in self.TECH_KEYWORDS)
        
        return {
            "ai_total": ai_total,
            "tech_total": tech_total,
            "artificial_intelligence": keyword_counts.get("artificial intelligence", 0),
            "machine_learning": keyword_counts.get("machine learning", 0),
            "automation": keyword_counts.get("automation", 0),
            "digital": keyword_counts.get("digital", 0),
            "cloud": keyword_counts.get("cloud", 0),
            "data_analytics": keyword_counts.get("data analytics", 0),
        }
    
    def _get_expanded_ai_keywords(self, ticker: str) -> List[str]:
        """
        Return AI_KEYWORDS expanded with company-specific terms from Groq.
        Falls back to the base list if Groq is unavailable.
        Results are cached inside groq_enrichment so Groq is called at most once
        per (ticker, dimension) per process lifetime.
        """
        try:
            from app.services.groq_enrichment import get_dimension_keywords
            return get_dimension_keywords(
                ticker, ticker, "sec_filing_ai_keywords", self.AI_KEYWORDS
            )
        except Exception as exc:
            logger.warning("Groq SEC keyword expansion failed for %s: %s", ticker, exc)
            return self.AI_KEYWORDS

    def analyze_sections(self, sections: Dict[str, str], document_id: str,
                         ticker: str, filing_type: str, filing_date: str,
                         total_word_count: int) -> DocumentAnalysis:
        """Analyze all sections in a document"""
        # Expand AI keywords with company-specific Groq terms once per call
        ai_keywords = self._get_expanded_ai_keywords(ticker) if ticker else self.AI_KEYWORDS

        section_stats = []
        all_keywords = {}

        for section_name, section_content in sections.items():
            if not section_content:
                continue

            # Word count
            word_count = len(section_content.split())

            # Keyword counts (use expanded keywords)
            keywords = self._count_keywords_with_list(section_content, ai_keywords)
            
            # Merge into total
            for kw, count in keywords.items():
                all_keywords[kw] = all_keywords.get(kw, 0) + count
            
            section_stats.append(SectionStats(
                section_name=section_name,
                word_count=word_count,
                keywords=keywords
            ))
        
        return DocumentAnalysis(
            document_id=document_id,
            ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            total_word_count=total_word_count,
            sections=section_stats,
            total_keywords=all_keywords
        )
    
    def get_section_display_name(self, section_key: str) -> str:
        """Get friendly display name for a section"""
        return self.SECTION_DISPLAY_NAMES.get(section_key, section_key)


# Singleton
_analyzer: Optional[SectionAnalyzer] = None

def get_section_analyzer() -> SectionAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = SectionAnalyzer()
    return _analyzer