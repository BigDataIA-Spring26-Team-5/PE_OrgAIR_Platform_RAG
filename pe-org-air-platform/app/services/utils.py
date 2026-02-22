"""
Service Utilities - PE Org-AI-R Platform
app/services/utils.py

Shared helpers for service layer.
"""
from typing import Callable, TypeVar

T = TypeVar("T")


def make_singleton_factory(cls: type) -> Callable[[], object]:
    """Return a zero-arg factory that creates and caches exactly one instance of cls."""
    _instance = [None]

    def _factory():
        if _instance[0] is None:
            _instance[0] = cls()
        return _instance[0]

    return _factory
