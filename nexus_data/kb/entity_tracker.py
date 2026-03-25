"""
nexus_data/kb/entity_tracker.py — Multi-Turn Entity Tracker (#3)
Maintains a session-level map of named entities (concrete values) extracted
from query results and user messages.

Purpose:
  "show orders for that user" → resolves "that user" to the user_id/name
  seen in the previous result row.

Entity types tracked:
  - Result entities: values from the last result that look like IDs/names
  - Filter entities: values used in WHERE clauses (extracted from goal filters)
  - Mention entities: named values the user typed inline
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_PRONOUN_PATTERN = re.compile(
    r"\b(that|this|the\s+same|those|these|it|them|him|her|the\s+previous)\b",
    re.IGNORECASE,
)

# Column name segments that strongly suggest an entity (vs a metric)
_ID_LIKE_PARTS = frozenset([
    "id", "name", "title", "code", "key", "slug", "email",
    "username", "user", "product", "item", "sku",
])


def _is_entity_column(col: str) -> bool:
    """Return True if any underscore/space-separated segment of col is an entity keyword."""
    return any(part in _ID_LIKE_PARTS for part in re.split(r"[_\W]+", col.lower()) if part)


class EntityTracker:
    """
    Session-level entity map.  Serialisable to a plain dict for session_cache.
    """

    def __init__(self) -> None:
        # {entity_type: {name: value}}
        self._entities: Dict[str, Any] = {}
        self._last_result_entities: List[Dict[str, Any]] = []

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def ingest_result(self, columns: List[str], rows: List[Any]) -> None:
        """Extract entity values from the first few rows of a result set."""
        self._last_result_entities = []
        if not columns or not rows:
            return

        # Find ID/name-like columns
        entity_cols = [
            (i, col) for i, col in enumerate(columns)
            if _is_entity_column(col)
        ]
        if not entity_cols:
            return  # No entity columns detected

        for row in rows[:3]:  # Only inspect first 3 rows
            row_list = list(row) if not isinstance(row, (list, tuple)) else list(row)
            for idx, col in entity_cols:
                if idx < len(row_list) and row_list[idx] is not None:
                    entity = {
                        "column": col,
                        "value": row_list[idx],
                        "type": "result_entity",
                    }
                    self._last_result_entities.append(entity)
                    self._entities[col.lower()] = row_list[idx]
                    logger.debug("EntityTracker: stored %s=%s", col, row_list[idx])

    def ingest_filters(self, filters: List[str]) -> None:
        """Extract entity values from SQL-style filter strings."""
        for f in filters:
            # Match: col = 'value' or col = 123
            m = re.match(r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?", f.strip())
            if m:
                col, val = m.group(1).lower(), m.group(2).strip()
                self._entities[col] = val

    # ── Resolution ────────────────────────────────────────────────────────────

    def has_pronoun(self, text: str) -> bool:
        """True if the query contains pronoun references."""
        return bool(_PRONOUN_PATTERN.search(text))

    def resolve_context(self) -> str:
        """
        Return a context string summarising known entities for injection
        into the planner/resolver prompt.
        """
        if not self._entities:
            return ""
        lines = [f"  - {k} = {v}" for k, v in list(self._entities.items())[-10:]]
        return "Known entities from conversation:\n" + "\n".join(lines)

    def get_last_entities(self) -> List[Dict[str, Any]]:
        return self._last_result_entities

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._entities)

    def clear(self) -> None:
        self._entities.clear()
        self._last_result_entities.clear()
