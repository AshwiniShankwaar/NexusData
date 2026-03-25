"""
nexus_data/pipeline/normalizer.py — Pipeline Stage 1
Normalizer: lightweight, LLM-free.
- Extracts structured info (entities, intent hint, temporal) via keyword matching.
- Checks semantic vector cache for a hit.
Produces a structured NormalizedQuery used by all downstream agents.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from nexus_data.kb.manager import KBManager
from nexus_data.kb.vector_repo import VectorQueryRepo

logger = logging.getLogger(__name__)

# ── Keyword patterns (all LLM-free) ──────────────────────────────────────────

_SCHEMA_KWS = re.compile(
    r"\b(columns?|fields?|schema|structure|describe|attributes?|"
    r"what is in|what are in|pragma|show columns?|list columns?|"
    r"what columns?|which columns?|what fields?|table info|table structure)\b", re.I
)
_COUNT_KWS = re.compile(
    r"\b(count|total(?: number)?|how many|number of|tally|frequency)\b", re.I
)
_AGG_KWS = re.compile(
    r"\b(sum|average|avg|mean|min|max|minimum|maximum|aggregate|total|"
    r"revenue|profit|margin|percentage|ratio|proportion|running total|"
    r"cumulative|trend|breakdown|distribution)\b", re.I
)
_TEMPORAL = re.compile(
    r"\b("
    r"last \d+ (?:days?|weeks?|months?|years?)"
    r"|last (?:week|month|year|quarter)"
    r"|past \d+ (?:days?|weeks?|months?|years?)"
    r"|past (?:week|month|year|quarter)"
    r"|this (?:week|month|year|quarter)"
    r"|next (?:week|month|year|quarter)"
    r"|yesterday|today|now"
    r"|in \d{4}|\d{4}"
    r"|Q[1-4]\s*\d{4}"
    r"|(?:january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+\d{4}"
    r"|\d{1,2}[/-]\d{4}"
    r")\b", re.I
)
# Only strong follow-up pronouns — avoid false positives on "last month" etc.
_REFERENCE_KWS = re.compile(
    r"\b(them|those|these|aforementioned|"
    r"drill.*down|break.*down|filter.*those|"
    r"the(?:se)? results?|of those|among them|of them)\b", re.I
)
_NUMERICS = re.compile(r"\b\d+(?:\.\d+)?\b")


@dataclass
class NormalizedQuery:
    """Structured extraction from raw user query — produced without LLM."""
    raw_query: str
    normalized_text: str            # lowercase, stripped
    intent_hint: str                # 'schema' | 'count' | 'aggregate' | 'select'
    mentioned_tables: List[str]     # tables found in query text
    mentioned_columns: List[str]    # columns found in query text
    temporal_expression: Optional[str]
    numeric_values: List[str]
    is_follow_up_hint: bool         # pronouns detected


@dataclass
class NormalizerResult:
    original_input: str
    normalized_input: str           # kept for backward compat
    normalized: NormalizedQuery     # rich structured form
    cached_sql: Optional[str] = None
    is_cached: bool = False


class QueryNormalizer:
    def __init__(self, kb_manager: KBManager, vector_repo: VectorQueryRepo):
        self.kb = kb_manager
        self.vector_repo = vector_repo

    def normalize(self, query: str) -> NormalizerResult:
        logger.info("Stage 1: Normalizer initiated")

        norm_text = query.lower().strip()
        self.kb.append_shortterm_memory(f"User: '{query}'")
        self.kb.add_to_session_cache(f"Active Query: '{query}'")

        # Extract structured info
        nq = self._extract(query, norm_text)

        # Semantic cache lookup (only for non-schema intents — schema queries
        # change every run and shouldn't be cached)
        cached_sql: Optional[str] = None
        if nq.intent_hint != "schema":
            cached_sql = self.vector_repo.search_canonical_sql(norm_text)

        if cached_sql:
            logger.info("Semantic cache HIT — bypassing LLM stages.")
            return NormalizerResult(
                original_input=query,
                normalized_input=norm_text,
                normalized=nq,
                cached_sql=cached_sql,
                is_cached=True,
            )

        return NormalizerResult(
            original_input=query,
            normalized_input=norm_text,
            normalized=nq,
        )

    # ── Extraction ────────────────────────────────────────────────────────────

    def _extract(self, raw: str, norm: str) -> NormalizedQuery:
        known_tables, known_columns = self.kb.get_schema_names()

        # Match table names mentioned
        mentioned_tables = [t for t in known_tables if t.lower() in norm]

        # Match column names mentioned
        all_cols = {c for cols in known_columns.values() for c in cols}
        mentioned_columns = [c for c in all_cols if c.lower() in norm]

        # Intent hint
        if _SCHEMA_KWS.search(norm):
            intent_hint = "schema"
        elif _COUNT_KWS.search(norm):
            intent_hint = "count"
        elif _AGG_KWS.search(norm):
            intent_hint = "aggregate"
        else:
            intent_hint = "select"

        temporal = None
        t_match = _TEMPORAL.search(raw)
        if t_match:
            temporal = t_match.group()

        numerics = _NUMERICS.findall(norm)
        is_follow_up = bool(_REFERENCE_KWS.search(norm))

        return NormalizedQuery(
            raw_query=raw,
            normalized_text=norm,
            intent_hint=intent_hint,
            mentioned_tables=mentioned_tables,
            mentioned_columns=mentioned_columns,
            temporal_expression=temporal,
            numeric_values=numerics,
            is_follow_up_hint=is_follow_up,
        )
