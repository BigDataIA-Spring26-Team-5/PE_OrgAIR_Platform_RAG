"""
analyst_notes.py — CS4 RAG Search
src/services/collection/analyst_notes.py

Stub for ingesting analyst interview notes and data room documents into ChromaDB.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

from ..retrieval.hybrid import HybridRetriever

logger = structlog.get_logger(__name__)


@dataclass
class AnalystNote:
    """A single analyst note or data room document."""
    content: str
    ticker: str
    source_type: str = "analyst_interview"  # or "dd_data_room"
    author: Optional[str] = None
    note_date: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    note_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnalystNoteIngester:
    """
    Ingests analyst notes into the hybrid retriever's index.
    Stub implementation — extend with file parsing (PDF, DOCX) as needed.
    """

    def __init__(self, retriever: HybridRetriever) -> None:
        self._retriever = retriever

    def ingest(self, notes: List[AnalystNote]) -> int:
        """
        Add notes to the retrieval index.
        Returns number of notes indexed.
        """
        if not notes:
            return 0

        texts = [n.content for n in notes]
        ids = [n.note_id for n in notes]
        metadatas = [
            {
                "ticker": n.ticker,
                "source": n.source_type,
                "author": n.author or "",
                "note_date": n.note_date.isoformat() if n.note_date else "",
                "tags": ",".join(n.tags),
                **n.metadata,
            }
            for n in notes
        ]

        self._retriever.add_documents(texts, ids, metadatas)
        logger.info("analyst notes indexed", count=len(notes))
        return len(notes)

    def ingest_text(
        self,
        content: str,
        ticker: str,
        source_type: str = "analyst_interview",
        **kwargs: Any,
    ) -> AnalystNote:
        """Convenience method: create and ingest a single note."""
        note = AnalystNote(content=content, ticker=ticker, source_type=source_type, **kwargs)
        self.ingest([note])
        return note
