"""
nexus_data/pipeline/decomposer.py — Query Decomposer
Breaks complex NL queries into ordered sub-goals before SQL planning.
Activated only when a complexity score threshold is exceeded.
The output is a single enriched goal dict with a CTE plan the planner can follow.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List

from nexus_data.engine.llm_controller import LLMController
from nexus_data.kb.manager import KBManager

logger = logging.getLogger(__name__)

_COMPLEXITY_THRESHOLD = 2   # score at or above this triggers decomposition

_DECOMPOSE_PROMPT = """\
You are a SQL planning expert. The user has a complex data question that requires \
multiple steps. Break it into ordered sub-goals, then describe the exact CTE plan \
the SQL generator should follow.

## Database Schema
{db_info}

## User Question
{query}

## Current Goal JSON
{goal_json}

## Instructions
- Each sub-goal maps to exactly one CTE (e.g. Base, Filtered, Ranked, Totals, Final).
- Name each CTE step clearly so the planner can chain them.
- If the query needs a ratio or percentage: one CTE for the numerator/group, one for the grand total, final SELECT joins them.
- If the query needs top-N-per-group: one CTE to rank with ROW_NUMBER/RANK OVER (PARTITION BY ...), final SELECT filters WHERE rn <= N.
- If the query needs running totals: one CTE to order the data, final SELECT uses SUM(...) OVER (...).
- If the query needs a pivot: one CTE aggregates, final SELECT uses CASE WHEN for each pivot column.

Output a single JSON object (no markdown):
{{
  "is_complex": true,
  "sub_goals": [
    {{"step": 1, "description": "...", "tables": ["..."], "operation": "...", "cte_name": "Step1"}},
    ...
  ],
  "cte_plan": "Numbered plain-English CTE chain (e.g. 'Base → Ranked → Final SELECT WHERE rn <= 5')",
  "combined_intent": "One-sentence description of the full query"
}}
"""


@dataclass
class DecompositionResult:
    original_query: str
    is_complex: bool
    sub_goals: List[Dict[str, Any]]
    cte_plan: str
    combined_intent: str
    enriched_goal: Dict[str, Any]   # original goal + decomposition added


class QueryDecomposer:
    def __init__(self, llm: LLMController, kb_manager: KBManager):
        self.llm = llm
        self.kb = kb_manager

    # ── Complexity scoring ────────────────────────────────────────────────────

    def _score(self, query: str, goal: Dict[str, Any]) -> int:
        score = 0
        if len(goal.get("relevant_tables", [])) > 1:         score += 2
        if len(goal.get("filters", [])) > 2:                 score += 1
        if len(goal.get("metrics", [])) > 1:                 score += 1
        if goal.get("grouping"):                             score += 1
        if goal.get("time_frame", "none") != "none" and goal.get("grouping"):
            score += 1
        if goal.get("operation") in (
            "compare", "trend", "rank", "top_n", "aggregate",
            "multi_step", "top_n_per_group", "running_total", "percentage", "pivot",
        ):
            score += 2
        if goal.get("needs_window_function"):                score += 2
        if goal.get("is_percentage_or_ratio"):               score += 1
        if goal.get("needs_subquery"):                       score += 1
        if len(query.split()) > 20:                          score += 1
        # Keywords that hint at multi-step reasoning or complex analytics
        multi_kws = re.compile(
            r"\b(for each|per|by|breakdown|compare|versus|vs|between|difference|"
            r"who have|that have|which have|except|excluding|not in|among those|"
            r"ratio|percentage|proportion|relative to|as a percent|cumulative|"
            r"running total|rank within|top \d+ per|profit|margin|calculate|"
            r"how much of|share of|contribution|year.over.year|month.over.month|"
            r"pivot|crosstab|lag|lead|moving average)\b",
            re.I,
        )
        if multi_kws.search(query):                          score += 1
        return score

    # ── Public API ────────────────────────────────────────────────────────────

    def decompose(self, query: str, goal: Dict[str, Any]) -> DecompositionResult:
        score = self._score(query, goal)
        logger.debug("Decomposer complexity score: %d", score)

        if score < _COMPLEXITY_THRESHOLD:
            return DecompositionResult(
                original_query=query, is_complex=False,
                sub_goals=[], cte_plan="", combined_intent=goal.get("intent_summary", query),
                enriched_goal=goal,
            )

        logger.info("Complex query detected (score=%d) — decomposing…", score)
        db_info = self.kb.read_db_info()

        try:
            raw = self.llm.generate(
                _DECOMPOSE_PROMPT.format(
                    db_info=db_info,
                    query=query,
                    goal_json=json.dumps(goal, indent=2),
                ),
                query,
                max_retries=1,
            )
        except Exception as exc:
            logger.warning("Decomposer LLM call failed: %s — proceeding without decomposition", exc)
            return DecompositionResult(
                original_query=query, is_complex=False,
                sub_goals=[], cte_plan="", combined_intent=goal.get("intent_summary", query),
                enriched_goal=goal,
            )

        # Extract JSON
        clean = re.sub(r"```(?:json)?", "", raw, flags=re.I).replace("```", "").strip()
        match = re.search(r"\{[\s\S]*\}", clean)
        data: Dict[str, Any] = {}
        if match:
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                pass

        sub_goals = data.get("sub_goals", [])
        cte_plan = data.get("cte_plan", "")
        combined_intent = data.get("combined_intent", goal.get("intent_summary", query))

        # Inject decomposition into goal so the planner sees it
        enriched = dict(goal)
        if sub_goals:
            enriched["_decomposition"] = {
                "sub_goals": sub_goals,
                "cte_plan": cte_plan,
                "combined_intent": combined_intent,
            }
            logger.info("Decomposed into %d sub-goals.", len(sub_goals))

        return DecompositionResult(
            original_query=query, is_complex=bool(sub_goals),
            sub_goals=sub_goals, cte_plan=cte_plan,
            combined_intent=combined_intent, enriched_goal=enriched,
        )
