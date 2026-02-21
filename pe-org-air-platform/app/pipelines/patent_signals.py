# """
# Patent Signals Pipeline - PatentsView API Integration
# app/pipelines/patent_signals.py

# Fetches patents from PatentsView PatentSearch API and classifies AI-related patents.
# Outputs results to JSON files.

# API Docs: https://search.patentsview.org/docs/
# """

# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# from collections import defaultdict
# from datetime import datetime, timezone, timedelta
# from pathlib import Path
# from typing import List, Optional, Set
# from dataclasses import dataclass, asdict
# from uuid import uuid4

# import httpx
# from dotenv import load_dotenv

# # from app.pipelines.pipeline2_state import Pipeline2State
# from app.pipelines.signal_pipeline_state import SignalPipelineState as Pipeline2State
# from app.pipelines.utils import clean_nan, safe_filename
# from app.models.signal import SignalCategory, SignalSource, ExternalSignal
# from app.services.s3_storage import get_s3_service

# # Load environment variables from .env file
# load_dotenv()

# logger = logging.getLogger(__name__)

# # PatentsView PatentSearch API configuration
# PATENTSVIEW_API_URL = os.getenv("PATENTSVIEW_API_URL", "https://search.patentsview.org/api/v1/patent/")
# PATENTSVIEW_REQUEST_DELAY = 1.5  # Rate limiting (45 req/min = 1.33s minimum)
# PATENTSVIEW_API_KEY = os.getenv("PATENTSVIEW_API_KEY")


# @dataclass
# class Patent:
#     """A patent record."""
#     patent_number: str
#     title: str
#     abstract: str
#     filing_date: datetime
#     grant_date: datetime | None
#     inventors: list[str]
#     assignee: str
#     is_ai_related: bool = False
#     ai_categories: list[str] = None
    
#     def __post_init__(self):
#         if self.ai_categories is None:
#             self.ai_categories = []


# class PatentSignalCollector:
#     """Collect patent signals for AI innovation."""
    
#     # AI Patent Keywords
#     AI_PATENT_KEYWORDS = [
#         "machine learning", "neural network", "deep learning",
#         "artificial intelligence", "natural language processing",
#         "computer vision", "reinforcement learning",
#         "predictive model", "classification algorithm",
#         "convolutional neural", "recurrent neural",
#         "transformer", "attention mechanism",
#         "generative adversarial", "large language model",
#         "llm", "gpt", "bert", "transformer",
#         "object detection", "semantic segmentation",
#         "speech recognition", "sentiment analysis",
#         "recommendation system", "anomaly detection"
#     ]
    
#     # AI Patent Categories
#     AI_PATENT_CATEGORIES = {
#         "deep_learning": ["neural network", "deep learning", "convolutional", "recurrent", "transformer"],
#         "nlp": ["natural language", "language model", "llm", "gpt", "bert", "sentiment"],
#         "computer_vision": ["computer vision", "image", "object detection", "segmentation"],
#         "predictive_analytics": ["predictive", "forecast", "anomaly detection"],
#         "reinforcement": ["reinforcement learning"],
#         "generative": ["generative adversarial", "generative ai"]
#     }
    
#     def __init__(self, api_key: Optional[str] = None):
#         self.api_key = api_key or PATENTSVIEW_API_KEY
#         self.headers = {"Content-Type": "application/json"}
#         if self.api_key:
#             self.headers["X-Api-Key"] = self.api_key
        
#         # Pre-compile regex patterns for more accurate matching
#         self.keyword_patterns = {
#             keyword: re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
#             for keyword in self.AI_PATENT_KEYWORDS
#         }
        
#         # Pre-compile category patterns
#         self.category_patterns = {}
#         for category, keywords in self.AI_PATENT_CATEGORIES.items():
#             patterns = [re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE) for kw in keywords]
#             self.category_patterns[category] = patterns
    
#     async def fetch_patents(
#         self,
#         company_name: str,
#         years_back: int = 5,
#         max_results: int = 100
#     ) -> List[Patent]:
#         """
#         Fetch patents for a company from PatentsView API.
        
#         Args:
#             company_name: Company name to search for
#             years_back: How many years back to search
#             max_results: Maximum number of patents to fetch
            
#         Returns:
#             List of Patent objects
#         """
#         # Calculate date range
#         end_date = datetime.now(timezone.utc)
#         start_date = end_date - timedelta(days=years_back * 365)
#         start_date_str = start_date.strftime("%Y-%m-%d")
        
#         # Build query for PatentsView API - use _and to combine conditions
#         query_obj = {
#             "_and": [
#                 {"_text_phrase": {"assignees.assignee_organization": company_name}},
#                 {"_gte": {"patent_date": start_date_str}}
#             ]
#         }
        
#         # Fields to return - simplified to essential fields
#         fields = [
#             "patent_id",
#             "patent_title",
#             "patent_abstract",
#             "patent_date",
#             "patent_type",
#             "assignees.assignee_organization",
#             "inventors.inventor_first_name",
#             "inventors.inventor_last_name"
#         ]
        
#         # Build URL with query params
#         params = {
#             "q": json.dumps(query_obj),
#             "f": json.dumps(fields),
#             "s": json.dumps([{"patent_date": "desc"}]),
#             "o": json.dumps({"size": min(max_results, 1000)})
#         }
        
#         async with httpx.AsyncClient(timeout=30.0) as client:
#             await asyncio.sleep(PATENTSVIEW_REQUEST_DELAY)
            
#             try:
#                 # Make the request
#                 response = await client.get(
#                     PATENTSVIEW_API_URL,
#                     params=params,
#                     headers=self.headers
#                 )
                
#                 # Log the response for debugging
#                 logger.debug(f"API Response Status: {response.status_code}")
                
#                 # Check for errors
#                 if response.status_code != 200:
#                     logger.error(f"API Error {response.status_code}: {response.text}")
#                     return []
                
#                 data = response.json()
                
#                 # Check for API errors in response (error: true indicates failure)
#                 if data.get("error") is True:
#                     logger.error(f"API Error: {data}")
#                     return []
                
#                 patents_data = data.get("patents", []) or []
#                 patents = []
                
#                 for patent_data in patents_data:
#                     # Extract assignee names
#                     assignees = patent_data.get("assignees") or []
#                     assignee_names = [
#                         a.get("assignee_organization", "")
#                         for a in assignees if a.get("assignee_organization")
#                     ]
                    
#                     # Extract inventor names
#                     inventors = patent_data.get("inventors") or []
#                     inventor_names = [
#                         f"{inv.get('inventor_first_name', '')} {inv.get('inventor_last_name', '')}".strip()
#                         for inv in inventors
#                     ]
                    
#                     # Parse patent date
#                     patent_date_str = clean_nan(patent_data.get("patent_date"))
#                     filing_date = None
                    
#                     try:
#                         if patent_date_str:
#                             # Parse the date and ensure it's timezone-aware
#                             parsed = datetime.fromisoformat(patent_date_str.replace("Z", "+00:00"))
#                             # If naive (no timezone), assume UTC
#                             if parsed.tzinfo is None:
#                                 filing_date = parsed.replace(tzinfo=timezone.utc)
#                             else:
#                                 filing_date = parsed
#                         else:
#                             # If no date, use current date as fallback
#                             filing_date = datetime.now(timezone.utc)
#                     except (ValueError, AttributeError) as e:
#                         logger.warning(f"Could not parse date '{patent_date_str}': {e}")
#                         filing_date = datetime.now(timezone.utc)
                    
#                     # Use first assignee as primary assignee
#                     primary_assignee = assignee_names[0] if assignee_names else company_name
                    
#                     patent = Patent(
#                         patent_number=str(patent_data.get("patent_id", "")),
#                         title=str(patent_data.get("patent_title", "")),
#                         abstract=str(patent_data.get("patent_abstract", "") or ""),
#                         filing_date=filing_date,
#                         grant_date=None,  # Grant date not available in basic API response
#                         inventors=inventor_names,
#                         assignee=primary_assignee,
#                         is_ai_related=False,
#                         ai_categories=[]
#                     )
                    
#                     patents.append(patent)
                
#                 logger.info(f"Fetched {len(patents)} patents for {company_name}")
#                 return patents
                
#             except httpx.HTTPStatusError as e:
#                 logger.error(f"HTTP error fetching patents for {company_name}: {e.response.status_code} - {e.response.text[:200]}")
#                 return []
#             except Exception as e:
#                 logger.error(f"Error fetching patents for {company_name}: {str(e)}")
#                 return []

#     def classify_patent(self, patent: Patent) -> Patent:
#         """Classify a patent as AI-related."""
#         text = f"{patent.title} {patent.abstract}".lower()

#         # Check for AI keywords
#         ai_keywords_found = []
#         for keyword, pattern in self.keyword_patterns.items():
#             if pattern.search(text):
#                 ai_keywords_found.append(keyword)

#         # Determine AI categories
#         ai_categories = []
#         for category, patterns in self.category_patterns.items():
#             for pattern in patterns:
#                 if pattern.search(text):
#                     ai_categories.append(category)
#                     break  # Found one keyword from this category

#         patent.is_ai_related = len(ai_keywords_found) > 0 or len(ai_categories) > 0
#         patent.ai_categories = list(set(ai_categories))  # Remove duplicates

#         return patent

#     def analyze_patents(
#         self,
#         company_id: str,
#         company_name: str,
#         patents: List[Patent],
#         years: int = 5
#     ) -> ExternalSignal:
#         """Analyze patent portfolio for AI innovation."""

#         cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
#         recent_patents = [p for p in patents if p.filing_date > cutoff]
#         ai_patents = [p for p in recent_patents if p.is_ai_related]

#         # Last year cutoff for recency bonus
#         last_year = datetime.now(timezone.utc) - timedelta(days=365)
#         recent_ai = [p for p in ai_patents if p.filing_date > last_year]

#         # Collect all AI categories
#         categories = set()
#         for p in ai_patents:
#             categories.update(p.ai_categories)

#         # Calculate AI patent ratio (what % of patents are AI-related)
#         ai_ratio = len(ai_patents) / len(recent_patents) if recent_patents else 0

#         # UPDATED Scoring Algorithm (harder to max out):
#         # ================================================
#         # Component 1: AI Patent Volume (max 40 points)
#         #   - 2 points per AI patent (need 20 AI patents to max)
#         #   - Previously: 5 points per patent, max at 10 patents
#         #
#         # Component 2: AI Patent Ratio (max 25 points)
#         #   - Based on % of patents that are AI-related
#         #   - 25 points if 50%+ are AI patents
#         #   - Scales linearly: ratio * 50 (capped at 25)
#         #
#         # Component 3: Recency Bonus (max 20 points)
#         #   - 1 point per AI patent filed in last year (need 20 to max)
#         #   - Previously: 2 points per patent, max at 10 patents
#         #
#         # Component 4: Category Diversity (max 15 points)
#         #   - 3 points per unique AI category (need 5 categories to max)
#         #   - Previously: 10 points per category, max at 3 categories
#         #
#         # Total possible: 100 points

#         volume_score = min(len(ai_patents) * 2, 40)
#         ratio_score = min(ai_ratio * 50, 25)
#         recency_score = min(len(recent_ai) * 1, 20)
#         diversity_score = min(len(categories) * 3, 15)

#         score = volume_score + ratio_score + recency_score + diversity_score

#         # Ensure score is between 0 and 100
#         normalized_score = min(100.0, max(0.0, float(score)))

#         # Calculate confidence based on sample size
#         # More patents = higher confidence in the score
#         if len(recent_patents) >= 50:
#             confidence = 0.95
#         elif len(recent_patents) >= 20:
#             confidence = 0.85
#         elif len(recent_patents) >= 10:
#             confidence = 0.75
#         else:
#             confidence = 0.60

#         # Create ExternalSignal
#         signal = ExternalSignal(
#             company_id=company_id,
#             company_name=company_name,
#             category=SignalCategory.INNOVATION_ACTIVITY,
#             source=SignalSource.USPTO,
#             signal_date=datetime.now(timezone.utc),
#             raw_value=f"{len(ai_patents)} AI patents in {years} years",
#             normalized_score=round(normalized_score, 1),
#             confidence=confidence,
#             metadata={
#                 "total_patents": len(patents),
#                 "recent_patents": len(recent_patents),
#                 "ai_patents": len(ai_patents),
#                 "recent_ai_patents": len(recent_ai),
#                 "ai_ratio": round(ai_ratio, 3),
#                 "ai_categories": list(categories),
#                 "years_analyzed": years,
#                 "score_breakdown": {
#                     "volume_score": round(volume_score, 1),
#                     "ratio_score": round(ratio_score, 1),
#                     "recency_score": round(recency_score, 1),
#                     "diversity_score": round(diversity_score, 1)
#                 },
#                 "analysis_date": datetime.now(timezone.utc).isoformat()
#             }
#         )

#         return signal


# async def run_patent_signals(
#     state: Pipeline2State,
#     years_back: int = 5,
#     results_per_company: int = 100,
#     api_key: Optional[str] = None,
#     skip_storage: bool = False,
# ) -> Pipeline2State:
#     """
#     Run the patent signals collection pipeline.
    
#     Args:
#         state: Pipeline state with companies loaded
#         years_back: How many years back to search (default: 5)
#         results_per_company: Max patents per company
#         api_key: PatentsView API key (optional, or set PATENTSVIEW_API_KEY env var)
#         skip_storage: If True, skip all storage steps
    
#     Returns:
#         Updated pipeline state with patents, classifications, and scores
#     """
#     logger.info("-" * 60)
#     logger.info("📊 PATENT SIGNALS PIPELINE")
#     logger.info("-" * 60)

#     collector = PatentSignalCollector(api_key=api_key)
#     s3_service = get_s3_service()

#     all_patents = []
#     patent_signals = {}

#     for company in state.companies:
#         company_id = company.get("id", "")
#         company_name = company.get("name", "")
#         ticker = company.get("ticker", "").upper()

#         if not company_name:
#             continue

#         logger.info(f"Processing {company_name}...")

#         # Fetch patents
#         patents = await collector.fetch_patents(
#             company_name=company_name,
#             years_back=years_back,
#             max_results=results_per_company
#         )

#         if not patents:
#             logger.warning(f"No patents found for {company_name}")
#             continue

#         # Store raw patents to S3 BEFORE classification
#         if not skip_storage and ticker:
#             raw_patents_data = {
#                 "company_id": company_id,
#                 "company_name": company_name,
#                 "ticker": ticker,
#                 "years_back": years_back,
#                 "total_patents": len(patents),
#                 "fetched_at": datetime.now(timezone.utc).isoformat(),
#                 "patents": [asdict(p) for p in patents]
#             }
#             try:
#                 s3_service.store_signal_data(
#                     signal_type="patents",
#                     ticker=ticker,
#                     data=raw_patents_data
#                 )
#                 logger.info(f"  📤 Stored {len(patents)} raw patents to S3 for {ticker}")
#             except Exception as e:
#                 logger.warning(f"  ⚠️ Failed to store raw patents to S3: {e}")

#         # Classify each patent (AFTER storing raw data)
#         classified_patents = []
#         for patent in patents:
#             classified_patent = collector.classify_patent(patent)
#             classified_patents.append(classified_patent)
        
#         # Analyze patent portfolio
#         signal = collector.analyze_patents(
#             company_id=company_id,
#             company_name=company_name,
#             patents=classified_patents,
#             years=years_back
#         )
        
#         # Store in state
#         all_patents.extend([asdict(p) for p in classified_patents])
#         patent_signals[company_id] = signal
        
#         # Update state
#         state.patents.extend([asdict(p) for p in classified_patents])
#         state.patent_scores[company_id] = signal.normalized_score
        
#         logger.info(f"  ✓ {company_name}: {signal.normalized_score}/100 "
#                    f"({len([p for p in classified_patents if p.is_ai_related])} AI patents)")
    
#     # Store aggregated results in state
#     state.summary["patents_collected"] = len(all_patents)
#     state.summary["ai_patents"] = sum(1 for p in all_patents if p.get("is_ai_related", False))
#     state.summary["patent_signals"] = {
#         company_id: {
#             "score": signal.normalized_score,
#             "ai_patents": signal.metadata.get("ai_patents", 0),
#             "total_patents": signal.metadata.get("total_patents", 0)
#         }
#         for company_id, signal in patent_signals.items()
#     }
    
#     logger.info(f"\n✅ Patent pipeline complete:")
#     logger.info(f"   • Total patents collected: {len(all_patents)}")
#     logger.info(f"   • AI-related patents: {sum(1 for p in all_patents if p.get('is_ai_related', False))}")
#     logger.info(f"   • Companies processed: {len(patent_signals)}")
    
#     return state


# # Legacy function for backward compatibility
# async def run_patent_signals_legacy(
#     state: Pipeline2State,
#     years_back: int = 5,
#     results_per_company: int = 100,
#     api_key: Optional[str] = None,
#     skip_storage: bool = False,
# ) -> Pipeline2State:
#     """Legacy wrapper for backward compatibility."""
#     return await run_patent_signals(
#         state=state,
#         years_back=years_back,
#         results_per_company=results_per_company,
#         api_key=api_key,
#         skip_storage=skip_storage
#     )

# """
# Patent Signals Pipeline - PatentsView API Integration
# app/pipelines/patent_signals.py

# ALIGNED WITH CASE STUDY 2 PDF SPEC (pages 17-19).

# Key changes from previous version:
#   - Scoring: min(ai*5,50) + min(recent*2,20) + min(cat*10,30) per PDF
#   - Confidence: fixed 0.90 per PDF page 19 line 71
#   - Keywords: match PDF page 18 lines 21-26 (9 multi-word phrases)
#   - Categories: 4 per PDF (deep_learning, nlp, computer_vision, predictive_analytics)
#   - classify_patent checks title + abstract per PDF page 19 line 82
#   - Multi-name patent search: queries all known assignee names per company
#     and deduplicates by patent_id (PatentsView treats each exact spelling
#     as a separate entity)
# """
# from __future__ import annotations

# import asyncio
# import json
# import logging
# import os
# import re
# from collections import defaultdict
# from datetime import datetime, timezone, timedelta
# from typing import List, Optional
# from dataclasses import dataclass, asdict

# import httpx
# from dotenv import load_dotenv

# from app.pipelines.signal_pipeline_state import SignalPipelineState as Pipeline2State
# from app.pipelines.utils import clean_nan, safe_filename
# from app.models.signal import SignalCategory, SignalSource, ExternalSignal
# from app.services.s3_storage import get_s3_service

# load_dotenv()
# logger = logging.getLogger(__name__)

# PATENTSVIEW_API_URL = os.getenv("PATENTSVIEW_API_URL", "https://search.patentsview.org/api/v1/patent/")
# PATENTSVIEW_REQUEST_DELAY = 1.5
# PATENTSVIEW_API_KEY = os.getenv("PATENTSVIEW_API_KEY")


# @dataclass
# class Patent:
#     """A patent record (PDF page 18 lines 6-16)."""
#     patent_number: str
#     title: str
#     abstract: str
#     filing_date: datetime
#     grant_date: datetime | None
#     inventors: list[str]
#     assignee: str
#     is_ai_related: bool = False
#     ai_categories: list[str] = None

#     def __post_init__(self):
#         if self.ai_categories is None:
#             self.ai_categories = []


# class PatentSignalCollector:
#     """Collect patent signals for AI innovation (PDF pages 17-19)."""

#     # ---------------------------------------------------------------
#     # AI Patent Keywords (PDF page 18, lines 21-26)
#     # ---------------------------------------------------------------
#     AI_PATENT_KEYWORDS = [
#         "machine learning",
#         "neural network",
#         "deep learning",
#         "artificial intelligence",
#         "natural language processing",
#         "computer vision",
#         "reinforcement learning",
#         "predictive model",
#         "classification algorithm",
#     ]

#     # ---------------------------------------------------------------
#     # AI Patent Classes (PDF page 18, lines 28-32)
#     # ---------------------------------------------------------------
#     AI_PATENT_CLASSES = [
#         "706",  # Data processing: AI
#         "382",  # Image analysis
#         "704",  # Speech processing
#     ]

#     # ---------------------------------------------------------------
#     # AI Patent Categories (PDF page 19, lines 86-94)
#     # 4 categories only: deep_learning, nlp, computer_vision, predictive_analytics
#     # ---------------------------------------------------------------
#     AI_PATENT_CATEGORIES = {
#         "deep_learning": ["neural network", "deep learning"],
#         "nlp": ["natural language"],
#         "computer_vision": [
#             "computer vision",
#             "image recognition",
#             "image classification",
#             "image segmentation",
#             "image processing",
#             "image analysis",
#             "object detection",
#             "visual recognition",
#             "convolutional neural",
#         ],
#         "predictive_analytics": ["predictive"],
#     }

#     def __init__(self, api_key: Optional[str] = None):
#         self.api_key = api_key or PATENTSVIEW_API_KEY
#         self.headers = {"Content-Type": "application/json"}
#         if self.api_key:
#             self.headers["X-Api-Key"] = self.api_key

#         self.keyword_patterns = {
#             kw: re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
#             for kw in self.AI_PATENT_KEYWORDS
#         }
#         self.category_patterns = {}
#         for cat, keywords in self.AI_PATENT_CATEGORIES.items():
#             self.category_patterns[cat] = [
#                 re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
#                 for kw in keywords
#             ]

#     # ------------------------------------------------------------------
#     # Fetch patents from PatentsView API (single assignee name)
#     # ------------------------------------------------------------------
#     async def _fetch_patents_single(
#         self,
#         company_name: str,
#         years_back: int = 5,
#         max_results: int = 100,
#     ) -> List[Patent]:
#         """Fetch patents for a single assignee organization name."""
#         end_date = datetime.now(timezone.utc)
#         start_date = end_date - timedelta(days=years_back * 365)

#         query_obj = {
#             "_and": [
#                 {"_text_phrase": {"assignees.assignee_organization": company_name}},
#                 {"_gte": {"patent_date": start_date.strftime("%Y-%m-%d")}},
#             ]
#         }
#         fields = [
#             "patent_id", "patent_title", "patent_abstract", "patent_date",
#             "patent_type",
#             "assignees.assignee_organization",
#             "inventors.inventor_first_name", "inventors.inventor_last_name",
#         ]
#         params = {
#             "q": json.dumps(query_obj),
#             "f": json.dumps(fields),
#             "s": json.dumps([{"patent_date": "desc"}]),
#             "o": json.dumps({"size": min(max_results, 1000)}),
#         }

#         async with httpx.AsyncClient(timeout=30.0) as client:
#             await asyncio.sleep(PATENTSVIEW_REQUEST_DELAY)
#             try:
#                 resp = await client.get(PATENTSVIEW_API_URL, params=params, headers=self.headers)
#                 if resp.status_code != 200:
#                     logger.error(f"API Error {resp.status_code}: {resp.text[:300]}")
#                     return []

#                 data = resp.json()
#                 if data.get("error") is True:
#                     logger.error(f"API Error: {data}")
#                     return []

#                 patents = []
#                 for pd in data.get("patents", []) or []:
#                     assignees = pd.get("assignees") or []
#                     assignee_names = [a.get("assignee_organization", "") for a in assignees if a.get("assignee_organization")]
#                     inventors = pd.get("inventors") or []
#                     inv_names = [f"{i.get('inventor_first_name','')} {i.get('inventor_last_name','')}".strip() for i in inventors]

#                     date_str = clean_nan(pd.get("patent_date"))
#                     try:
#                         if date_str:
#                             parsed = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
#                             filing_date = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
#                         else:
#                             filing_date = datetime.now(timezone.utc)
#                     except (ValueError, AttributeError):
#                         filing_date = datetime.now(timezone.utc)

#                     patents.append(Patent(
#                         patent_number=str(pd.get("patent_id", "")),
#                         title=str(pd.get("patent_title", "")),
#                         abstract=str(pd.get("patent_abstract", "") or ""),
#                         filing_date=filing_date,
#                         grant_date=None,
#                         inventors=inv_names,
#                         assignee=assignee_names[0] if assignee_names else company_name,
#                     ))

#                 logger.info(f"Fetched {len(patents)} patents for {company_name}")
#                 return patents
#             except Exception as e:
#                 logger.error(f"Error fetching patents for {company_name}: {e}")
#                 return []

#     # ------------------------------------------------------------------
#     # Fetch patents across ALL known assignee names (with dedup)
#     # ------------------------------------------------------------------
#     async def fetch_patents(
#         self,
#         company_names: str | List[str],
#         years_back: int = 5,
#         max_results: int = 100,
#     ) -> List[Patent]:
#         """
#         Fetch patents for one or more assignee organization names.

#         PatentsView treats each exact spelling as a separate entity, so
#         companies like Walmart need multiple queries:
#           - "Walmart Apollo, LLC" (current)
#           - "Wal-Mart Stores, Inc." (legacy)

#         Results are deduplicated by patent_id.

#         Args:
#             company_names: Single name (str) or list of assignee names
#             years_back: How many years to look back
#             max_results: Max results per assignee name query

#         Returns:
#             Deduplicated list of Patent objects
#         """
#         # Normalize to list
#         if isinstance(company_names, str):
#             names = [company_names]
#         else:
#             names = list(company_names)

#         if not names:
#             return []

#         # Single name — no dedup needed
#         if len(names) == 1:
#             return await self._fetch_patents_single(names[0], years_back, max_results)

#         # Multiple names — fetch all and deduplicate by patent_id
#         all_patents: List[Patent] = []
#         seen_ids: set = set()
#         total_before_dedup = 0

#         for name in names:
#             patents = await self._fetch_patents_single(name, years_back, max_results)
#             total_before_dedup += len(patents)
#             for p in patents:
#                 if p.patent_number not in seen_ids:
#                     seen_ids.add(p.patent_number)
#                     all_patents.append(p)

#         dupes = total_before_dedup - len(all_patents)
#         logger.info(
#             f"  Combined {len(names)} assignee names → {len(all_patents)} unique patents"
#             + (f" ({dupes} duplicates removed)" if dupes > 0 else "")
#         )

#         return all_patents

#     # ------------------------------------------------------------------
#     # Classify patent (PDF page 19, lines 80-98)
#     # ------------------------------------------------------------------
#     def classify_patent(self, patent: Patent) -> Patent:
#         """Classify a patent as AI-related. Checks title + abstract."""
#         text = f"{patent.title} {patent.abstract}".lower()

#         is_ai = any(p.search(text) for p in self.keyword_patterns.values())

#         categories = []
#         if "neural network" in text or "deep learning" in text:
#             categories.append("deep_learning")
#         if "natural language" in text:
#             categories.append("nlp")

#         # computer_vision: require specific AI-image phrases, not bare "image"
#         cv_phrases = [
#             "computer vision", "image recognition", "image classification",
#             "image segmentation", "image processing", "image analysis",
#             "object detection", "visual recognition", "convolutional neural",
#         ]
#         if any(phrase in text for phrase in cv_phrases):
#             categories.append("computer_vision")

#         if "predictive" in text or "classification algorithm" in text:
#             categories.append("predictive_analytics")

#         # Catch-all for AI keywords that didn't map to a specific category
#         if is_ai and not categories:
#             if "reinforcement learning" in text:
#                 categories.append("deep_learning")
#             if "machine learning" in text or "artificial intelligence" in text:
#                 categories.append("predictive_analytics")

#         patent.is_ai_related = is_ai or len(categories) > 0
#         patent.ai_categories = list(set(categories))
#         return patent
#     # ------------------------------------------------------------------
#     # Scoring (PDF pages 18-19, lines 46-62)
#     # ------------------------------------------------------------------
#     def analyze_patents(
#         self,
#         company_id: str,
#         company_name: str,
#         patents: List[Patent],
#         years: int = 5,
#     ) -> ExternalSignal:
#         """
#         Analyze patent portfolio for AI innovation.

#         Scoring per CS2 PDF (pages 18-19):
#           - AI patent count:     5 pts each   (max 50)
#           - Recency bonus:       2 pts each   (max 20)  — filed in last year
#           - Category diversity:  10 pts each  (max 30)
#           Total possible: 100

#         Confidence: 0.90 (PDF page 19 line 71)
#         """
#         cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
#         recent_patents = [p for p in patents if p.filing_date > cutoff]
#         ai_patents = [p for p in recent_patents if p.is_ai_related]

#         last_year = datetime.now(timezone.utc) - timedelta(days=365)
#         recent_ai = [p for p in ai_patents if p.filing_date > last_year]

#         categories = set()
#         for p in ai_patents:
#             categories.update(p.ai_categories)

#         volume_score = min(len(ai_patents) * 5, 50)
#         recency_score = min(len(recent_ai) * 2, 20)
#         diversity_score = min(len(categories) * 10, 30)

#         score = volume_score + recency_score + diversity_score
#         normalized_score = min(100.0, max(0.0, float(score)))

#         confidence = 0.90

#         return ExternalSignal(
#             company_id=company_id,
#             company_name=company_name,
#             category=SignalCategory.INNOVATION_ACTIVITY,
#             source=SignalSource.USPTO,
#             signal_date=datetime.now(timezone.utc),
#             raw_value=f"{len(ai_patents)} AI patents in {years} years",
#             normalized_score=round(normalized_score, 1),
#             confidence=confidence,
#             metadata={
#                 "total_patents": len(patents),
#                 "recent_patents": len(recent_patents),
#                 "ai_patents": len(ai_patents),
#                 "recent_ai_patents": len(recent_ai),
#                 "ai_categories": sorted(categories),
#                 "years_analyzed": years,
#                 "score_breakdown": {
#                     "volume_score": round(volume_score, 1),
#                     "recency_score": round(recency_score, 1),
#                     "diversity_score": round(diversity_score, 1),
#                 },
#             },
#         )


# # ------------------------------------------------------------------
# # Pipeline runner
# # ------------------------------------------------------------------
# async def run_patent_signals(
#     state: Pipeline2State,
#     years_back: int = 5,
#     results_per_company: int = 100,
#     api_key: Optional[str] = None,
#     skip_storage: bool = False,
# ) -> Pipeline2State:
#     logger.info("-" * 60)
#     logger.info("📊 PATENT SIGNALS PIPELINE")
#     logger.info("-" * 60)

#     collector = PatentSignalCollector(api_key=api_key)
#     s3 = get_s3_service()
#     all_patents = []
#     patent_signals = {}

#     for company in state.companies:
#         cid = company.get("id", "")
#         name = company.get("name", "")
#         ticker = company.get("ticker", "").upper()
#         if not name:
#             continue

#         logger.info(f"Processing {name}...")

#         # --- Multi-name patent lookup ---
#         from app.config import get_patent_search_names
#         patent_names = get_patent_search_names(ticker)
#         if not patent_names:
#             # Fallback: use company name directly
#             patent_names = [name]

#         patents = await collector.fetch_patents(patent_names, years_back, results_per_company)
#         if not patents:
#             logger.warning(f"No patents found for {name}")

#         # Classify FIRST, then store
#         classified = [collector.classify_patent(p) for p in patents]
#         ai_count = len([p for p in classified if p.is_ai_related])

#         # Store classified patents to S3
#         if not skip_storage and ticker:
#             try:
#                 s3.store_signal_data(
#                     signal_type="patents", ticker=ticker,
#                     data={
#                         "company_id": cid, "company_name": name, "ticker": ticker,
#                         "years_back": years_back, "total_patents": len(classified),
#                         "ai_patents": ai_count,
#                         "assignee_names_searched": patent_names,
#                         "fetched_at": datetime.now(timezone.utc).isoformat(),
#                         "patents": [asdict(p) for p in classified],
#                     },
#                 )
#                 logger.info(f"  📤 Stored {len(classified)} patents ({ai_count} AI) to S3 for {ticker}")
#             except Exception as e:
#                 logger.warning(f"  ⚠️ Failed to store patents to S3: {e}")

#         signal = collector.analyze_patents(cid, name, classified, years_back)

#         all_patents.extend([asdict(p) for p in classified])
#         patent_signals[cid] = signal
#         state.patents.extend([asdict(p) for p in classified])
#         state.patent_scores[cid] = signal.normalized_score

#         logger.info(f"  ✓ {name}: {signal.normalized_score}/100 ({ai_count} AI patents)")

#     state.summary["patents_collected"] = len(all_patents)
#     state.summary["ai_patents"] = sum(1 for p in all_patents if p.get("is_ai_related"))

#     logger.info(f"\n✅ Patent pipeline complete:")
#     logger.info(f"   • Total patents: {len(all_patents)}")
#     logger.info(f"   • AI patents: {state.summary['ai_patents']}")
#     logger.info(f"   • Companies: {len(patent_signals)}")
#     return state

"""
Patent Signals Pipeline - PatentsView API Integration
app/pipelines/patent_signals.py

ALIGNED WITH CASE STUDY 2 PDF SPEC (pages 17-19).

Key changes from previous version:
  - Scoring: min(ai*5,50) + min(recent*2,20) + min(cat*10,30) per PDF
  - Confidence: fixed 0.90 per PDF page 19 line 71
  - Keywords: match PDF page 18 lines 21-26 (9 multi-word phrases)
  - Categories: 4 per PDF (deep_learning, nlp, computer_vision, predictive_analytics)
  - classify_patent checks title + abstract per PDF page 19 line 82
  - Multi-name patent search: queries all known assignee names per company
    and deduplicates by patent_id (PatentsView treats each exact spelling
    as a separate entity)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from dataclasses import dataclass, asdict

import httpx
from dotenv import load_dotenv

from app.pipelines.signal_pipeline_state import SignalPipelineState as Pipeline2State
from app.pipelines.utils import clean_nan, safe_filename
from app.models.signal import SignalCategory, SignalSource, ExternalSignal
from app.services.s3_storage import get_s3_service

load_dotenv()
logger = logging.getLogger(__name__)

PATENTSVIEW_API_URL = os.getenv("PATENTSVIEW_API_URL", "https://search.patentsview.org/api/v1/patent/")
PATENTSVIEW_REQUEST_DELAY = 1.5
PATENTSVIEW_API_KEY = os.getenv("PATENTSVIEW_API_KEY")


@dataclass
class Patent:
    """A patent record (PDF page 18 lines 6-16)."""
    patent_number: str
    title: str
    abstract: str
    filing_date: datetime
    grant_date: datetime | None
    inventors: list[str]
    assignee: str
    is_ai_related: bool = False
    ai_categories: list[str] = None

    def __post_init__(self):
        if self.ai_categories is None:
            self.ai_categories = []


class PatentSignalCollector:
    """Collect patent signals for AI innovation (PDF pages 17-19)."""

    # ---------------------------------------------------------------
    # AI Patent Keywords (PDF page 18, lines 21-26)
    # ---------------------------------------------------------------
    AI_PATENT_KEYWORDS = [
        "machine learning",
        "neural network",
        "deep learning",
        "artificial intelligence",
        "natural language processing",
        "computer vision",
        "reinforcement learning",
        "predictive model",
        "classification algorithm",
    ]

    # ---------------------------------------------------------------
    # AI Patent Classes (PDF page 18, lines 28-32)
    # ---------------------------------------------------------------
    AI_PATENT_CLASSES = [
        "706",  # Data processing: AI
        "382",  # Image analysis
        "704",  # Speech processing
    ]

    # ---------------------------------------------------------------
    # AI Patent Categories (PDF page 19, lines 86-94)
    # 4 categories only: deep_learning, nlp, computer_vision, predictive_analytics
    # ---------------------------------------------------------------
    AI_PATENT_CATEGORIES = {
        "deep_learning": ["neural network", "deep learning"],
        "nlp": ["natural language"],
        "computer_vision": [
            "computer vision",
            "image recognition", "image classification",
            "image segmentation", "image processing",
            "image analysis", "object detection",
            "visual recognition", "convolutional neural",
        ],
        "predictive_analytics": ["predictive"],
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or PATENTSVIEW_API_KEY
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers["X-Api-Key"] = self.api_key

        self.keyword_patterns = {
            kw: re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
            for kw in self.AI_PATENT_KEYWORDS
        }
        self.category_patterns = {}
        for cat, keywords in self.AI_PATENT_CATEGORIES.items():
            self.category_patterns[cat] = [
                re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
                for kw in keywords
            ]

    # ------------------------------------------------------------------
    # Fetch patents from PatentsView API (single assignee name)
    # ------------------------------------------------------------------
    async def _fetch_patents_single(
        self,
        company_name: str,
        years_back: int = 5,
        max_results: int = 100,
    ) -> List[Patent]:
        """Fetch patents for a single assignee organization name."""
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=years_back * 365)

        query_obj = {
            "_and": [
                {"_text_phrase": {"assignees.assignee_organization": company_name}},
                {"_gte": {"patent_date": start_date.strftime("%Y-%m-%d")}},
            ]
        }
        fields = [
            "patent_id", "patent_title", "patent_abstract", "patent_date",
            "patent_type",
            "assignees.assignee_organization",
            "inventors.inventor_first_name", "inventors.inventor_last_name",
        ]
        params = {
            "q": json.dumps(query_obj),
            "f": json.dumps(fields),
            "s": json.dumps([{"patent_date": "desc"}]),
            "o": json.dumps({"size": min(max_results, 1000)}),
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            await asyncio.sleep(PATENTSVIEW_REQUEST_DELAY)
            try:
                resp = await client.get(PATENTSVIEW_API_URL, params=params, headers=self.headers)
                if resp.status_code != 200:
                    logger.error(f"API Error {resp.status_code}: {resp.text[:300]}")
                    return []

                data = resp.json()
                if data.get("error") is True:
                    logger.error(f"API Error: {data}")
                    return []

                patents = []
                for pd in data.get("patents", []) or []:
                    assignees = pd.get("assignees") or []
                    assignee_names = [a.get("assignee_organization", "") for a in assignees if a.get("assignee_organization")]
                    inventors = pd.get("inventors") or []
                    inv_names = [f"{i.get('inventor_first_name','')} {i.get('inventor_last_name','')}".strip() for i in inventors]

                    date_str = clean_nan(pd.get("patent_date"))
                    try:
                        if date_str:
                            parsed = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                            filing_date = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                        else:
                            filing_date = datetime.now(timezone.utc)
                    except (ValueError, AttributeError):
                        filing_date = datetime.now(timezone.utc)

                    patents.append(Patent(
                        patent_number=str(pd.get("patent_id", "")),
                        title=str(pd.get("patent_title", "")),
                        abstract=str(pd.get("patent_abstract", "") or ""),
                        filing_date=filing_date,
                        grant_date=None,
                        inventors=inv_names,
                        assignee=assignee_names[0] if assignee_names else company_name,
                    ))

                logger.info(f"Fetched {len(patents)} patents for {company_name}")
                return patents
            except Exception as e:
                logger.error(f"Error fetching patents for {company_name}: {e}")
                return []

    # ------------------------------------------------------------------
    # Fetch patents across ALL known assignee names (with dedup)
    # ------------------------------------------------------------------
    async def fetch_patents(
        self,
        company_names: str | List[str],
        years_back: int = 5,
        max_results: int = 100,
    ) -> List[Patent]:
        """
        Fetch patents for one or more assignee organization names.

        PatentsView treats each exact spelling as a separate entity, so
        companies like Walmart need multiple queries:
          - "Walmart Apollo, LLC" (current)
          - "Wal-Mart Stores, Inc." (legacy)

        Results are deduplicated by patent_id.

        Args:
            company_names: Single name (str) or list of assignee names
            years_back: How many years to look back
            max_results: Max results per assignee name query

        Returns:
            Deduplicated list of Patent objects
        """
        # Normalize to list
        if isinstance(company_names, str):
            names = [company_names]
        else:
            names = list(company_names)

        if not names:
            return []

        # Single name — no dedup needed
        if len(names) == 1:
            return await self._fetch_patents_single(names[0], years_back, max_results)

        # Multiple names — fetch all and deduplicate by patent_id
        all_patents: List[Patent] = []
        seen_ids: set = set()
        total_before_dedup = 0

        for name in names:
            patents = await self._fetch_patents_single(name, years_back, max_results)
            total_before_dedup += len(patents)
            for p in patents:
                if p.patent_number not in seen_ids:
                    seen_ids.add(p.patent_number)
                    all_patents.append(p)

        dupes = total_before_dedup - len(all_patents)
        logger.info(
            f"  Combined {len(names)} assignee names → {len(all_patents)} unique patents"
            + (f" ({dupes} duplicates removed)" if dupes > 0 else "")
        )

        return all_patents

    # ------------------------------------------------------------------
    # Classify patent (PDF page 19, lines 80-98)
    # ------------------------------------------------------------------
    def classify_patent(self, patent: Patent) -> Patent:
        """Classify a patent as AI-related. Checks title + abstract."""
        text = f"{patent.title} {patent.abstract}".lower()

        is_ai = any(p.search(text) for p in self.keyword_patterns.values())

        categories = []
        if "neural network" in text or "deep learning" in text:
            categories.append("deep_learning")
        if "natural language" in text:
            categories.append("nlp")
        # computer_vision: require specific AI-image phrases, not bare "image"
        cv_phrases = [
            "computer vision", "image recognition", "image classification",
            "image segmentation", "image processing", "image analysis",
            "object detection", "visual recognition", "convolutional neural",
        ]
        if any(phrase in text for phrase in cv_phrases):
            categories.append("computer_vision")
        if "predictive" in text or "classification algorithm" in text:
            categories.append("predictive_analytics")

        if is_ai and not categories:
            if "reinforcement learning" in text:
                categories.append("deep_learning")
            if "machine learning" in text or "artificial intelligence" in text:
                categories.append("predictive_analytics")

        patent.is_ai_related = is_ai or len(categories) > 0
        patent.ai_categories = list(set(categories))
        return patent

    # ------------------------------------------------------------------
    # Scoring (PDF pages 18-19, lines 46-62)
    # ------------------------------------------------------------------
    def analyze_patents(
        self,
        company_id: str,
        company_name: str,
        patents: List[Patent],
        years: int = 5,
    ) -> ExternalSignal:
        """
        Analyze patent portfolio for AI innovation.

        Scoring per CS2 PDF (pages 18-19):
          - AI patent count:     5 pts each   (max 50)
          - Recency bonus:       2 pts each   (max 20)  — filed in last year
          - Category diversity:  10 pts each  (max 30)
          Total possible: 100

        Confidence: 0.90 (PDF page 19 line 71)
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
        recent_patents = [p for p in patents if p.filing_date > cutoff]
        ai_patents = [p for p in recent_patents if p.is_ai_related]

        last_year = datetime.now(timezone.utc) - timedelta(days=365)
        recent_ai = [p for p in ai_patents if p.filing_date > last_year]

        categories = set()
        for p in ai_patents:
            categories.update(p.ai_categories)

        volume_score = min(len(ai_patents) * 5, 50)
        recency_score = min(len(recent_ai) * 2, 20)
        diversity_score = min(len(categories) * 10, 30)

        score = volume_score + recency_score + diversity_score
        normalized_score = min(100.0, max(0.0, float(score)))

        confidence = 0.90

        return ExternalSignal(
            company_id=company_id,
            company_name=company_name,
            category=SignalCategory.INNOVATION_ACTIVITY,
            source=SignalSource.USPTO,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{len(ai_patents)} AI patents in {years} years",
            normalized_score=round(normalized_score, 1),
            confidence=confidence,
            metadata={
                "total_patents": len(patents),
                "recent_patents": len(recent_patents),
                "ai_patents": len(ai_patents),
                "recent_ai_patents": len(recent_ai),
                "ai_categories": sorted(categories),
                "years_analyzed": years,
                "score_breakdown": {
                    "volume_score": round(volume_score, 1),
                    "recency_score": round(recency_score, 1),
                    "diversity_score": round(diversity_score, 1),
                },
            },
        )


# ------------------------------------------------------------------
# Pipeline runner
# ------------------------------------------------------------------
async def run_patent_signals(
    state: Pipeline2State,
    years_back: int = 5,
    results_per_company: int = 100,
    api_key: Optional[str] = None,
    skip_storage: bool = False,
) -> Pipeline2State:
    logger.info("-" * 60)
    logger.info("📊 PATENT SIGNALS PIPELINE")
    logger.info("-" * 60)

    collector = PatentSignalCollector(api_key=api_key)
    s3 = get_s3_service()
    all_patents = []
    patent_signals = {}

    for company in state.companies:
        cid = company.get("id", "")
        name = company.get("name", "")
        ticker = company.get("ticker", "").upper()
        if not name:
            continue

        logger.info(f"Processing {name}...")

        # --- Multi-name patent lookup ---
        from app.config import get_patent_search_names
        patent_names = get_patent_search_names(ticker)
        if not patent_names:
            # Fallback: use company name directly
            patent_names = [name]

        patents = await collector.fetch_patents(patent_names, years_back, results_per_company)
        if not patents:
            logger.warning(f"No patents found for {name}")

        # Classify FIRST, then store
        classified = [collector.classify_patent(p) for p in patents]
        ai_count = len([p for p in classified if p.is_ai_related])

        # Store classified patents to S3
        if not skip_storage and ticker:
            try:
                s3.store_signal_data(
                    signal_type="patents", ticker=ticker,
                    data={
                        "company_id": cid, "company_name": name, "ticker": ticker,
                        "years_back": years_back, "total_patents": len(classified),
                        "ai_patents": ai_count,
                        "assignee_names_searched": patent_names,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "patents": [asdict(p) for p in classified],
                    },
                )
                logger.info(f"  📤 Stored {len(classified)} patents ({ai_count} AI) to S3 for {ticker}")
            except Exception as e:
                logger.warning(f"  ⚠️ Failed to store patents to S3: {e}")

        signal = collector.analyze_patents(cid, name, classified, years_back)

        all_patents.extend([asdict(p) for p in classified])
        patent_signals[cid] = signal
        state.patents.extend([asdict(p) for p in classified])
        state.patent_scores[cid] = signal.normalized_score

        logger.info(f"  ✓ {name}: {signal.normalized_score}/100 ({ai_count} AI patents)")

    state.summary["patents_collected"] = len(all_patents)
    state.summary["ai_patents"] = sum(1 for p in all_patents if p.get("is_ai_related"))

    logger.info(f"\n✅ Patent pipeline complete:")
    logger.info(f"   • Total patents: {len(all_patents)}")
    logger.info(f"   • AI patents: {state.summary['ai_patents']}")
    logger.info(f"   • Companies: {len(patent_signals)}")
    return state