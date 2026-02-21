"""
Diagnose GE section extraction.

Usage: python -m app.scripts.diagnose_ge_sections
"""
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


def diagnose():
    from app.pipelines.sec_edgar import SECEdgarCollector
    from app.pipelines.document_parser import DocumentParser

    collector = SECEdgarCollector()
    parser = DocumentParser()

    # Download latest GE 10-K
    filings = list(collector.get_company_filings("GE", ["10-K"], years_back=1))
    if not filings:
        logger.error("No filings found")
        return

    filing = filings[0]
    logger.info(f"Downloading {filing.filing_type} from {filing.filing_date}")
    content = collector.download_filing(filing)

    # Parse to get clean text
    html_text = content.decode('utf-8', errors='ignore')
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')
    for element in soup(['script', 'style', 'meta', 'link']):
        element.decompose()
    text = soup.get_text(separator='\n')
    lines = (line.strip() for line in text.splitlines())
    text = '\n'.join(line for line in lines if line)
    text = parser._clean_text(text)

    logger.info(f"Total text length: {len(text)} chars, {len(text.split())} words")

    # Find ALL occurrences of "ITEM 1" pattern
    content_upper = text.upper()
    
    patterns = {
        "ITEM_1_BUSINESS": r"ITEM\s*1\.?\s*BUSINESS",
        "ITEM_1A_RISK": r"ITEM\s*1A\.?\s*RISK\s*FACTORS",
        "ITEM_7_MDA": r"ITEM\s*7\.?\s*MANAGEMENT",
    }

    for name, pattern in patterns.items():
        matches = list(re.finditer(pattern, content_upper))
        logger.info(f"\n{'='*60}")
        logger.info(f"Pattern: {name} → {len(matches)} matches")
        
        for i, match in enumerate(matches):
            pos = match.start()
            # Show 100 chars before and 300 chars after
            before = text[max(0, pos-100):pos].replace('\n', '↵')
            after = text[pos:pos+400].replace('\n', '↵')
            
            # Check if it looks like TOC (page refs nearby)
            nearby = text[pos:pos+200]
            has_page_refs = bool(re.search(r'\d+\s*[-–]\s*\d+', nearby))
            
            logger.info(f"\n  Match {i+1} at char {pos} (page_refs={has_page_refs}):")
            logger.info(f"    BEFORE: ...{before}")
            logger.info(f"    AFTER:  {after}...")
            
            # Show word count to next ITEM header
            remaining = content_upper[pos+100:]
            next_item = re.search(r"ITEM\s*\d", remaining)
            if next_item:
                between = text[pos:pos+100+next_item.start()]
                wc = len(between.split())
                logger.info(f"    Words to next ITEM: {wc}")
            
    # Also show what a bigger capture would look like for the LAST match of business
    logger.info(f"\n{'='*60}")
    logger.info(f"FULL CAPTURE TEST: Last 'ITEM 1 BUSINESS' match")
    biz_matches = list(re.finditer(r"ITEM\s*1\.?\s*BUSINESS", content_upper))
    if len(biz_matches) >= 2:
        last_match = biz_matches[-1]
        pos = last_match.start()
        # Find next ITEM 1A after this
        remaining_upper = content_upper[pos+200:]
        end_match = re.search(r"ITEM\s*1A", remaining_upper)
        if end_match:
            end_pos = pos + 200 + end_match.start()
            captured = text[pos:end_pos]
            logger.info(f"  Capture: {len(captured.split())} words")
            logger.info(f"  First 500 chars: {captured[:500].replace(chr(10), ' ')}")
            logger.info(f"  Last 200 chars: {captured[-200:].replace(chr(10), ' ')}")
        else:
            logger.info(f"  No ITEM 1A found after last match")


if __name__ == "__main__":
    diagnose()