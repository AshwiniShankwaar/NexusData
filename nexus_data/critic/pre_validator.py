"""
nexus_data/critic/pre_validator.py — SQL Pre-Validator
Validates LLM-generated SQL against the original question BEFORE execution.
Two-pass approach:
  1. Fast structural check (free — checks tables/columns mentioned in goal exist in SQL)
  2. LLM semantic check (cheap — only triggered when structural check raises doubt)
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from nexus_data.engine.llm_controller import LLMController

logger = logging.getLogger(__name__)

_VALIDATE_PROMPT = """\
You are an expert SQL reviewer. Decide whether the generated SQL correctly answers \
the user's question given the database schema.

## User Question
{query}

## Goal
{goal_summary}

## Database Schema (relevant excerpt)
{db_info}

## Generated SQL
{sql}

Respond with a JSON object (no markdown):
{{
  "is_correct": <true|false>,
  "issues": ["<issue1>", "..."],
  "corrected_sql": "<fixed SQL if is_correct is false, else same SQL>"
}}

Rules:
- is_correct = true if the SQL would return the right data for the question.
- If incorrect, provide a corrected_sql that fixes every issue listed.
- corrected_sql MUST be a SELECT statement.
- Output ONLY the JSON.
"""


class SQLPreValidator:
    def __init__(self, llm: LLMController, db_info: str):
        self.llm = llm
        self.db_info = db_info

    def validate_and_fix(
        self,
        query: str,
        sql: str,
        goal_dict: Dict[str, Any],
    ) -> Tuple[str, bool]:
        """
        Returns (final_sql, was_modified).
        Runs structural check first; only calls LLM if structural issues found
        or if the query is non-trivial.
        """
        if not sql or not sql.strip():
            return sql, False

        # Pass 1 — fast structural check
        structural_issues = self._structural_check(sql, goal_dict)

        # Determine structural complexity without hardcoding specific operation keywords
        # A query is complex if it references 2+ tables, uses window/subquery flags, or has 3+ filters
        num_tables = len(goal_dict.get("relevant_tables", []))
        is_complex = (
            num_tables > 1
            or goal_dict.get("needs_window_function")
            or goal_dict.get("needs_subquery")
            or goal_dict.get("is_percentage_or_ratio")
            or len(goal_dict.get("filters", [])) > 2
        )

        if not structural_issues and not is_complex:
            return sql, False  # looks fine, skip LLM call

        # Pass 2 — LLM semantic validation
        logger.info("Pre-validator: running LLM check (structural_issues=%s, complex=%s)",
                    structural_issues, is_complex)
        try:
            goal_summary = (
                f"operation={goal_dict.get('operation')}, "
                f"tables={goal_dict.get('relevant_tables')}, "
                f"filters={goal_dict.get('filters')}, "
                f"metrics={goal_dict.get('metrics')}"
            )
            raw = self.llm.generate(
                _VALIDATE_PROMPT.format(
                    query=query,
                    goal_summary=goal_summary,
                    db_info=self.db_info[:5000],  # schema context
                    sql=sql,
                ),
                query,
                max_retries=1,
            )
        except Exception as exc:
            logger.warning("Pre-validator LLM call failed: %s — using original SQL", exc)
            return sql, False

        import json
        clean = re.sub(r"```(?:json)?", "", raw, flags=re.I).replace("```", "").strip()
        match = re.search(r"\{[\s\S]*\}", clean)
        if not match:
            return sql, False

        try:
            data = json.loads(match.group())
        except json.JSONDecodeError:
            return sql, False

        is_correct = bool(data.get("is_correct", True))
        corrected = data.get("corrected_sql", sql) or sql

        # Strip any markdown the model might add
        corrected = re.sub(r"```(?:sql)?", "", corrected, flags=re.I).replace("```", "").strip()

        if not is_correct and corrected and corrected.strip().upper().startswith("SELECT"):
            issues = data.get("issues", [])
            logger.info("Pre-validator fixed SQL. Issues: %s", issues)
            return corrected, True

        return sql, False

    # ── Structural check ──────────────────────────────────────────────────────

    def _structural_check(self, sql: str, goal_dict: Dict[str, Any]) -> List[str]:
        """
        Fast checks that don't require an LLM call.
        Returns list of issue strings (empty = no issues).
        """
        issues: List[str] = []
        sql_upper = sql.upper()

        # Check relevant tables are referenced
        for tbl in goal_dict.get("relevant_tables", []):
            if tbl.upper() not in sql_upper:
                issues.append(f"Table '{tbl}' from goal not found in SQL")

        # Check it's a SELECT
        stripped = sql.strip().upper()
        if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
            issues.append("SQL does not start with SELECT or WITH")

        # Check operation alignment (only for unambiguous simple aggregations)
        op = goal_dict.get("operation", "")
        if op == "count" and "COUNT" not in sql_upper:
            issues.append("Operation is 'count' but COUNT() not found in SQL")
        if op == "sum" and "SUM" not in sql_upper:
            issues.append("Operation is 'sum' but SUM() not found in SQL")
        if op == "average" and "AVG" not in sql_upper:
            issues.append("Operation is 'average' but AVG() not found in SQL")
        if op in ("running_total",) and "OVER" not in sql_upper:
            issues.append("Operation is 'running_total' but window OVER clause not found in SQL")
        if op == "top_n_per_group" and ("ROW_NUMBER" not in sql_upper and "RANK" not in sql_upper):
            issues.append("Operation is 'top_n_per_group' but ROW_NUMBER/RANK not found in SQL")

        # Check SELECT list does not consist solely of id-like columns
        # (only meaningful for non-aggregation queries where the user wants entity data)
        if "COUNT(" not in sql_upper and "SUM(" not in sql_upper and "AVG(" not in sql_upper:
            id_only_issue = self._check_id_only_select(sql_upper)
            if id_only_issue:
                issues.append(id_only_issue)

        if issues:
            logger.debug("Structural issues: %s", issues)

        return issues

    @staticmethod
    def _check_id_only_select(sql_upper: str) -> Optional[str]:
        """
        Returns an issue string if the SELECT list contains ONLY id-like columns
        (e.g. `id`, `user_id`, `product_id`), which is almost never the right answer
        when a user asks for "unique users" / "list products" etc.

        Safe guards:
        - Ignores queries with aggregation functions (handled by caller).
        - Ignores subqueries in FROM — only checks the outermost SELECT.
        - Does not flag COUNT(DISTINCT id) patterns.
        """
        # Extract the outermost SELECT … FROM … portion
        # Strip CTE prefix (WITH … AS (…)) to reach the final SELECT
        sel_match = re.search(
            r"\bSELECT\s+(?:DISTINCT\s+)?(.+?)\s+FROM\b",
            sql_upper,
            re.DOTALL,
        )
        if not sel_match:
            return None

        raw_cols = sel_match.group(1).strip()

        # Skip if it looks like a wildcard or contains a function call
        if raw_cols == "*" or re.search(r"\w+\s*\(", raw_cols):
            return None

        # Split on commas (naively — good enough for simple SELECT lists)
        cols = [c.strip() for c in raw_cols.split(",")]

        # Pattern: bare `id`, `t.id`, `user_id`, `t.user_id`, `product_id`, etc.
        _id_col = re.compile(
            r"^(?:\w+\.)?(?:id|\w+_id|id_\w+)(?:\s+AS\s+\w+)?$",
            re.IGNORECASE,
        )

        if cols and all(_id_col.match(c) for c in cols):
            return (
                "SELECT list contains only ID column(s) — include descriptive columns "
                "(name, email, category, status, etc.) instead of bare primary keys"
            )
