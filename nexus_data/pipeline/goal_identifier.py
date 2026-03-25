"""
nexus_data/pipeline/goal_identifier.py — Pipeline Stage 2
LLM agent: classifies intent, selects tables, extracts filters/metrics.
Produces a rich GoalIdentifierResult including:
- Structured goal dict
- Ambiguity flag + clarification question
- skip_cache flag (schema/metadata queries)
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from nexus_data.engine.llm_controller import LLMController
from nexus_data.kb.manager import KBManager
from nexus_data.pipeline.normalizer import NormalizerResult

logger = logging.getLogger(__name__)


_DESTRUCTIVE_PATTERN = re.compile(
    r"^\s*(insert\s+into|update\s+\w|delete\s+from|drop\s+table|drop\s+database|"
    r"truncate\s+|alter\s+table|create\s+table|create\s+database|exec\s*\(|"
    r"execute\s+|grant\s+|revoke\s+)",
    re.IGNORECASE,
)

# Narrower intent keywords — only unambiguously destructive NL phrases.
# Keep this list tight to avoid false positives on analytical queries like
# "how did revenue change", "add up the totals", "remove outliers from the chart".
_DESTRUCTIVE_INTENT_KEYWORDS = re.compile(
    r"\b("
    r"insert\s+(a\s+)?(new\s+)?(row|record|entry)\b"        # "insert a new entry in …"
    r"|add\s+(a\s+)?(new\s+)?(row|record|entry)\b"          # "add a new record for …"
    r"|create\s+(a\s+)?(new\s+)?(row|record|entry)\b"       # "create a new row in …"
    r"|delete\s+(this\s+|that\s+|the\s+|all\s+)?(row|record|entry|records)\b"  # "delete this record"
    r"|remove\s+(this\s+|that\s+|the\s+|all\s+)?(row|record|entry|records)\b"  # "remove the row where"
    r"|update\s+(this\s+|that\s+|the\s+)?(row|record|entry|value)\b"           # "update the value of …"
    r"|set\s+\w+\s*=\s*.+\s+where\s+\w"
    r")\b",
    re.IGNORECASE,
)


@dataclass
class GoalIdentifierResult:
    normalized_result: NormalizerResult
    goal_dict: Dict[str, Any]
    relevant_tables: List[str]
    intent_summary: str
    is_ambiguous: bool = False
    clarification_question: Optional[str] = None
    skip_cache: bool = False            # True for schema/metadata queries
    is_restricted: bool = False         # True when query intends destructive DML/DDL


_SYSTEM_PROMPT = """\
You are the NexusData Goal Identifier — an expert data analyst AI.

## Database Schema
{db_info}

## Extraction context (keyword analysis — treat as hints, not ground truth)
- Intent hint       : {intent_hint}
- Tables mentioned  : {mentioned_tables}
- Columns mentioned : {mentioned_columns}
- Temporal          : {temporal}

## Conversation Memory
{persona_context}

## Task
Analyse the user's question and output ONLY a single JSON object (no markdown, no commentary):

{{
  "operation": "<see operation guide below>",
  "time_frame": "<e.g. 'last 30 days' | '2024' | 'none'>",
  "filters": ["<SQL-style condition string>"],
  "grouping": ["<column to GROUP BY>"],
  "metrics": ["<column or expression to measure or compute>"],
  "ordering": "<ASC|DESC|none>",
  "limit": <integer or null>,
  "relevant_tables": ["<exact table names from schema>"],
  "intent_summary": "<one-sentence plain-English restatement of what the user wants>",
  "ambiguous": <true|false>,
  "clarification_question": "<concise question if ambiguous, else null>",
  "skip_cache": <true if schema/metadata query else false>,
  "needs_window_function": <true if query requires ROW_NUMBER/RANK/LAG/LEAD/running total/moving average>,
  "needs_subquery": <true if query requires a derived table, EXISTS, or correlated subquery>,
  "is_percentage_or_ratio": <true if result should be a ratio, %, or proportion of total>
}}

## Operation guide — pick the SINGLE best match
- select            → retrieve rows (no aggregation)
- count             → COUNT(*) or COUNT(DISTINCT col)
- sum               → SUM of a column
- average           → AVG of a column
- min / max         → MIN / MAX of a column
- list              → SELECT DISTINCT col
- aggregate         → mixed aggregation (multiple functions in one query)
- multi_step        → query requires 2+ independent computations combined (use CTEs)
- compare           → side-by-side comparison of two groups or time periods
- trend             → time-bucketed aggregation (monthly totals, daily counts, etc.)
- rank              → ORDER BY metric; simple top-N or bottom-N
- top_n_per_group   → top/bottom N rows WITHIN each group (requires window function)
- running_total     → cumulative sum or count over time (window function)
- percentage        → ratio/percentage of a column relative to total or group total
- pivot             → reshape data (rows to columns or vice versa)
- search            → LIKE / full-text search
- list_columns      → user wants schema / column names for a table
- describe_table    → user wants full schema description of a table
- follow_up         → question refers to the previous result using pronouns
- correction        → user is pointing out an error or asking to redo/fix the previous answer
- general           → any other broad SELECT that doesn't fit above

## Ambiguity rules — ONLY set "ambiguous": true when:
- The question cannot be answered without knowing which specific table/column to use AND
  there is no reasonable default
- The question is completely unrelated to the database (e.g. "what is the weather")
- DO NOT mark as ambiguous: follow-ups, corrections, vague phrasings of valid data questions,
  or questions that simply don't mention a table by name (infer from schema context).

## Correction rules
Set operation = "correction" when the user says the previous answer was wrong, asks to redo,
points out a mistake, or asks to "look again" / "try again" / "recalculate". Never mark these
as ambiguous — treat them as follow-up corrections.

## Cache rules
Set "skip_cache": true for list_columns, describe_table, and any schema inspection.

Output ONLY the JSON object, nothing else.
"""


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    clean = re.sub(r"```(?:json)?", "", text, flags=re.IGNORECASE).replace("```", "").strip()
    for candidate in [clean, re.search(r"\{[\s\S]*\}", clean).group() if re.search(r"\{[\s\S]*\}", clean) else ""]:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            # Progressive trim
            for end in range(len(candidate), 0, -1):
                try:
                    return json.loads(candidate[:end])
                except json.JSONDecodeError:
                    continue
    return None


class GoalIdentifierAgent:
    def __init__(self, llm: LLMController, kb_manager: KBManager, config=None):
        self.llm = llm
        self.kb = kb_manager
        self._config = config

    def identify(self, prev_result: NormalizerResult) -> GoalIdentifierResult:
        logger.info("Stage 2: Goal Identifier Agent triggered")

        # ── Destructive intent check (pre-LLM, zero cost) ─────────────────────
        raw = prev_result.original_input.strip()
        if _DESTRUCTIVE_PATTERN.match(raw) or _DESTRUCTIVE_INTENT_KEYWORDS.search(raw):
            logger.warning("Destructive intent detected — blocking: %s", raw[:80])
            return GoalIdentifierResult(
                normalized_result=prev_result,
                goal_dict={"operation": "restricted"},
                relevant_tables=[],
                intent_summary=raw,
                is_restricted=True,
            )

        if prev_result.is_cached:
            return GoalIdentifierResult(
                normalized_result=prev_result,
                goal_dict={"operation": "cached"},
                relevant_tables=[],
                intent_summary=prev_result.normalized_input,
            )

        nq = prev_result.normalized
        db_info = self.kb.read_db_info()
        persona_context = self.kb.get_combined_persona_context()

        sys_prompt = _SYSTEM_PROMPT.format(
            db_info=db_info,
            intent_hint=nq.intent_hint,
            mentioned_tables=nq.mentioned_tables or "none detected",
            mentioned_columns=nq.mentioned_columns or "none detected",
            temporal=nq.temporal_expression or "none",
            persona_context=persona_context,
        )

        response_text = self.llm.generate(sys_prompt, prev_result.original_input)
        data = _extract_json(response_text)

        if data is None:
            logger.warning("GoalIdentifier: JSON extraction failed. Raw: %s", response_text[:200])
            data = {
                "operation": "general",
                "time_frame": "none", "filters": [], "grouping": [],
                "metrics": [], "ordering": "none", "limit": None,
                "relevant_tables": nq.mentioned_tables,
                "intent_summary": prev_result.original_input,
                "ambiguous": False, "clarification_question": None, "skip_cache": False,
            }

        # Defaults
        for k, v in [
            ("operation", "general"), ("time_frame", "none"), ("filters", []),
            ("grouping", []), ("metrics", []), ("ordering", "none"), ("limit", None),
            ("relevant_tables", nq.mentioned_tables), ("intent_summary", prev_result.original_input),
            ("ambiguous", False), ("clarification_question", None), ("skip_cache", False),
        ]:
            data.setdefault(k, v)

        if data["operation"] == "unknown":
            data["operation"] = "general"

        # Schema operations always skip cache
        if data["operation"] in ("list_columns", "describe_table"):
            data["skip_cache"] = True

        tables = data.get("relevant_tables", []) or nq.mentioned_tables
        summary = data.get("intent_summary", prev_result.original_input)

        logger.info("Goal: op=%s tables=%s ambiguous=%s", data["operation"], tables, data["ambiguous"])

        return GoalIdentifierResult(
            normalized_result=prev_result,
            goal_dict=data,
            relevant_tables=tables,
            intent_summary=summary,
            is_ambiguous=bool(data.get("ambiguous")),
            clarification_question=data.get("clarification_question"),
            skip_cache=bool(data.get("skip_cache")),
        )
