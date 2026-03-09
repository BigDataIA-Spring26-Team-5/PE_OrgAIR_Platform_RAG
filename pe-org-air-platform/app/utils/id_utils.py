import hashlib


def stable_evidence_id(ticker: str, source_type: str, content: str) -> str:
    """
    Generate a stable, deterministic evidence ID from content.
    Same content + same ticker + same source_type always produces the same ID.
    Prevents duplicate documents on re-index.
    """
    fingerprint = f"{ticker}::{source_type}::{content.strip()}"
    return hashlib.sha256(fingerprint.encode()).hexdigest()[:32]
