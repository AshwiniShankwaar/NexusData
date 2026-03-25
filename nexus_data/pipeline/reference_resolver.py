"""
nexus_data/pipeline/reference_resolver.py — Pipeline Stage 3
Reference Resolver: detects follow-up questions (pronouns, references to previous
results) and augments the goal JSON with context from the preceding execution.
Optionally uses the LLM to produce a self-contained expanded goal.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from nexus_data.kb.manager import KBManager
from nexus_data.kb.conversation_graph import ConversationGraph, ConversationTurn
from nexus_data.pipeline.goal_identifier import GoalIdentifierResult

logger = logging.getLogger(__name__)

# Pronoun / reference patterns that signal a follow-up question.
# Only strong pronoun references — avoid false positives ("also", "how about").
_REFERENCE_PATTERNS = re.compile(
    r"\b(them|those|these|it|that|same|previous|prior|last|above|"
    r"aforementioned|such|the result|the results|those results|"
    r"of them|of those|among them|break.*down|drill.*down|filter.*those)\b",
    re.IGNORECASE,
)

# Matches entity-ID filters like "users.id = 3", "orders.id = 42", "id = 7"
_ENTITY_ID_FILTER_RE = re.compile(
    r"(?:\w+\.)?\bid\b\s*=\s*\d+", re.IGNORECASE
)


@dataclass
class ResolvedGoalResult:
    goal_result: GoalIdentifierResult
    resolved_goal_json: Dict[str, Any]
    is_follow_up: bool


class ReferenceResolverAgent:
    """Detects follow-up intent and enriches the goal with previous-execution context."""

    def __init__(self, kb_manager: KBManager, conv_graph: Optional[ConversationGraph] = None):
        self.kb = kb_manager
        self._conv_graph = conv_graph if conv_graph is not None else ConversationGraph()

    # ── Public API ─────────────────────────────────────────────────────────────

    def resolve(self, goal_res: GoalIdentifierResult) -> ResolvedGoalResult:
        logger.info("Stage 3: Reference Resolver evaluating goal…")

        goal_json: Dict[str, Any] = dict(goal_res.goal_dict)  # shallow copy
        original_query = goal_res.normalized_result.original_input
        curr_tables: List[str] = goal_res.goal_dict.get("relevant_tables", []) or goal_res.relevant_tables

        # Graph-aware parent lookup instead of flat last-entry scan
        best_parent: Optional[ConversationTurn] = self._conv_graph.find_best_parent(curr_tables)
        last_meta: Optional[Dict[str, Any]] = self._turn_to_meta(best_parent) if best_parent else None

        is_explicit_followup = goal_json.get("operation") == "follow_up"
        # Use normalizer's pronoun hint OR re-check with our own pattern
        has_reference = (
            goal_res.normalized_result.normalized.is_follow_up_hint
            or bool(_REFERENCE_PATTERNS.search(original_query))
        )
        is_follow_up = (is_explicit_followup or has_reference) and last_meta is not None

        if is_follow_up and last_meta:
            logger.info(
                "Follow-up detected (explicit=%s, pronoun=%s). Parent turn: '%s'",
                is_explicit_followup, has_reference,
                best_parent.intent_summary if best_parent else "none",
            )
            goal_json = self._merge_context(goal_json, last_meta, original_query)

            # If no tables were identified by the Goal Identifier, carry forward parent's
            if not goal_res.relevant_tables:
                prev_tables: List[str] = last_meta.get("tables_used", [])
                goal_res.relevant_tables = prev_tables
                goal_json["relevant_tables"] = prev_tables

            # Upgrade operation from follow_up to the actual operation type
            if goal_json.get("operation") == "follow_up":
                goal_json["operation"] = "general"

            # Annotate for orchestrator so it can link this turn to its parent
            if best_parent:
                goal_json["_parent_turn_id"] = best_parent.turn_id

        return ResolvedGoalResult(
            goal_result=goal_res,
            resolved_goal_json=goal_json,
            is_follow_up=is_follow_up,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _turn_to_meta(self, turn: ConversationTurn) -> Dict[str, Any]:
        """Convert a ConversationTurn to the dict shape expected by _merge_context."""
        return {
            "type": "execution_meta",
            "sql_used": turn.sql_used,
            "filters_used": turn.filters_used,
            "tables_used": turn.tables_used,
            "metrics_used": turn.metrics_used,
            "grouping_used": turn.grouping_used,
        }

    def _merge_context(
        self,
        goal_json: Dict[str, Any],
        last_meta: Dict[str, Any],
        original_query: str,
    ) -> Dict[str, Any]:
        """
        Merge previous execution context into the current goal.
        Filter carry-forward rules:
        - Only carry old filters when current and previous tables overlap >= 50%
        - Never carry entity-ID filters (e.g. "users.id = 3") unless the query
          has a strong entity pronoun ("them", "those", etc.)
        - Always add the new filters from the current goal
        """
        prev_tables: List[str] = last_meta.get("tables_used", [])
        curr_tables: List[str] = goal_json.get("relevant_tables", [])

        # Table overlap ratio — carry filters only when queries share context
        overlap = (
            len(set(prev_tables) & set(curr_tables)) / max(len(set(prev_tables)), 1)
            if prev_tables
            else 0.0
        )
        should_carry_filters = overlap >= 0.50 or not curr_tables  # no tables yet = early resolution

        old_filters: List[str] = last_meta.get("filters_used", [])
        new_filters: List[str] = goal_json.get("filters", [])

        # Strong entity pronouns signal the user wants entity-specific context
        has_entity_pronoun = bool(re.search(r"\b(them|those|these|it)\b", original_query, re.IGNORECASE))

        if should_carry_filters and old_filters:
            carried: List[str] = []
            for f in old_filters:
                # Skip entity-ID filters unless user explicitly referenced an entity
                if _ENTITY_ID_FILTER_RE.search(f) and not has_entity_pronoun:
                    logger.debug("Skipping entity-ID filter carry: %s", f)
                    continue
                carried.append(f)

            seen: set = set()
            merged_filters: List[str] = []
            for f in carried + new_filters:
                if f not in seen:
                    seen.add(f)
                    merged_filters.append(f)
            goal_json["filters"] = merged_filters
        else:
            # Different table context — only keep current filters
            if not should_carry_filters:
                logger.debug(
                    "Filter carry skipped (table overlap=%.0f%%). Keeping only new filters.",
                    overlap * 100,
                )
            goal_json["filters"] = new_filters

        # If grouping is empty but the previous query had grouping, inherit it
        if not goal_json.get("grouping") and last_meta.get("grouping_used"):
            goal_json["grouping"] = last_meta["grouping_used"]

        # If metrics are empty but previous query had them, inherit
        if not goal_json.get("metrics") and last_meta.get("metrics_used"):
            goal_json["metrics"] = last_meta["metrics_used"]

        # Annotate for the planner — inject previous SQL + concise context note
        prev_sql = last_meta.get("sql_used", "")
        if prev_sql:
            goal_json["_previous_sql"] = prev_sql
            goal_json["_context_note"] = (
                "This is a follow-up question. Build on the previous query below. "
                "Apply the user's new filter/change on top of it.\n"
                f"Previous SQL:\n{prev_sql}"
            )

        return goal_json
