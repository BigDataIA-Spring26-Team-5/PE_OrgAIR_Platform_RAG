"""
streamlit/utils/company_resolver.py
Re-exports from the canonical app-level implementation.
"""
from app.utils.company_resolver import (  # noqa: F401
    ResolvedCompany,
    INDUSTRY_MAP,
    SECTOR_TO_INDUSTRY,
    resolve_company,
    format_resolution_preview,
)
