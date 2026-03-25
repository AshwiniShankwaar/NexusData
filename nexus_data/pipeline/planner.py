"""
nexus_data/pipeline/planner.py — Pipeline Stage 4
Planner: translates a resolved Goal JSON into dialect-correct SQL.
- list_columns / describe_table → generated directly (no LLM, no tokens)
- ambiguous goals → skipped (executor returns clarification)
- everything else → LLM with a comprehensive prompt
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import List, Optional

from nexus_data.engine.llm_controller import LLMController
from nexus_data.kb.graph_store import SQLGraphStore
from nexus_data.kb.manager import KBManager
from nexus_data.pipeline.reference_resolver import ResolvedGoalResult

logger = logging.getLogger(__name__)


@dataclass
class PlannerResult:
    goal_result: ResolvedGoalResult
    sql: str


# ── Dialect-specific schema inspection SQL ────────────────────────────────────

def _schema_sql(dialect: str, tables: List[str]) -> str:
    """Generate SQL to inspect columns for the given tables, no LLM needed."""
    if not tables:
        # No table specified — list all tables
        if dialect == "sqlite":
            return "SELECT name AS table_name, type FROM sqlite_master WHERE type='table' ORDER BY name;"
        elif dialect in ("postgresql", "postgres"):
            return (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name;"
            )
        else:
            return "SHOW TABLES;"

    parts = []
    for table in tables:
        if dialect == "sqlite":
            parts.append(
                f"SELECT '{table}' AS table_name, name AS column_name, type AS data_type, "
                f"pk AS is_primary_key FROM pragma_table_info('{table}');"
            )
        elif dialect in ("postgresql", "postgres"):
            parts.append(
                f"SELECT '{table}' AS table_name, column_name, data_type, is_nullable "
                f"FROM information_schema.columns WHERE table_name = '{table}' "
                f"ORDER BY ordinal_position;"
            )
        elif dialect == "mysql":
            parts.append(
                f"SELECT '{table}' AS table_name, COLUMN_NAME AS column_name, "
                f"DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable "
                f"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' "
                f"ORDER BY ORDINAL_POSITION;"
            )
        else:  # mssql / generic
            parts.append(
                f"SELECT '{table}' AS table_name, COLUMN_NAME AS column_name, DATA_TYPE "
                f"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}';"
            )
    # Return first one (for multi-table queries we'd need UNION but keep simple)
    return parts[0] if len(parts) == 1 else "\n-- Run each separately:\n" + "\n".join(parts)


# ── LLM prompt ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are the NexusData Planner Agent — an expert SQL engineer. You write SQL for any level
of complexity: simple lookups, multi-table JOINs, CTEs, window functions, self-joins,
correlated subqueries, pivots, running totals, percentages, and recursive queries.

## Dialect
{dialect}

## Resolved Goal
{goal_json}

## Relevant Tables
{focus_tables}

## Full Database Schema (with sample values and relationships)
{db_info}

{decomposition_section}\
{entity_section}\
{graph_section}\
{feedback_section}\
## Memory
{persona_context}

## STRICT RULES
1. Output ONLY the raw SQL — no markdown fences, no explanation, no comments.
2. Must be a SELECT statement (no INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE).
3. Use EXACT table and column names from the schema above.
4. Apply every filter, grouping, ordering, and limit from the Goal JSON.
5. COALESCE/NULLIF for nulls where appropriate.
6. When `_previous_sql` is in the Goal, build on it rather than starting from scratch.
7. For corrections: examine what was wrong in `_previous_sql` and fix that specific issue.
8. NEVER select only `id` or `*_id` columns in the SELECT list unless the user explicitly
   asks for IDs or a count. `id` columns are for JOIN ON, WHERE, and ORDER BY only.
   When asked for "unique users", "list products", "show orders", etc., SELECT the entity's
   descriptive columns from the schema (e.g. name, email, category, status, price) —
   NOT the primary key alone.

## OPERATION PATTERNS — follow these for the operation type in the Goal

### Simple operations
- count     → SELECT COUNT(*) or COUNT(DISTINCT col) FROM …
- sum/avg   → SELECT SUM(col), AVG(col) … GROUP BY …
- top_n     → … ORDER BY metric DESC LIMIT n
- list/unique → SELECT DISTINCT name, email, … FROM … (descriptive columns — never bare id)
- search    → … WHERE col LIKE '%term%'
- trend     → GROUP BY time_bucket (strftime for SQLite, DATE_TRUNC for PG, YEAR/MONTH for MySQL)

### Complex operations — always use CTEs (WITH … AS (…))
- multi_step:
  Break into numbered CTEs: Step1 computes one thing, Step2 uses Step1, final SELECT joins them.
  Example:
    WITH Base AS (SELECT …), Summary AS (SELECT … FROM Base GROUP BY …)
    SELECT … FROM Summary;

- top_n_per_group (ranking within groups):
  WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY metric DESC) AS rn
    FROM table
  )
  SELECT * FROM Ranked WHERE rn <= N;

- running_total / cumulative sum:
  SELECT col, SUM(amount) OVER (PARTITION BY group_col ORDER BY date_col
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
  FROM table;

- percentage / ratio (X as % of total or group total):
  WITH Totals AS (SELECT SUM(col) AS grand_total FROM table)
  SELECT t.group_col,
         SUM(t.col) AS subtotal,
         ROUND(SUM(t.col) * 100.0 / tt.grand_total, 2) AS pct
  FROM table t CROSS JOIN Totals tt
  GROUP BY t.group_col, tt.grand_total;

- compare (two groups side by side):
  SELECT
    SUM(CASE WHEN condition_a THEN metric END) AS group_a,
    SUM(CASE WHEN condition_b THEN metric END) AS group_b
  FROM table;

- pivot (rows to columns — SQLite/generic):
  SELECT
    group_col,
    SUM(CASE WHEN pivot_col='A' THEN value END) AS "A",
    SUM(CASE WHEN pivot_col='B' THEN value END) AS "B"
  FROM table GROUP BY group_col;

- LAG/LEAD (period-over-period change):
  SELECT date_col, metric,
         LAG(metric) OVER (ORDER BY date_col) AS prev_period,
         metric - LAG(metric) OVER (ORDER BY date_col) AS change
  FROM table;

- Self-referential / derived calculations (profit, margin, etc.):
  Compute derived columns inline:
  SELECT name, price, price * 0.02 AS profit_margin, price * 1.02 AS selling_price
  FROM products;
  For per-product totals:
  WITH Sales AS (
    SELECT p.name, SUM(oi.quantity * p.price) AS cost_total,
           SUM(oi.quantity * p.price * 1.02) AS revenue_total
    FROM products p JOIN order_items oi ON p.id = oi.product_id
    GROUP BY p.name
  )
  SELECT name, cost_total, revenue_total, revenue_total - cost_total AS profit
  FROM Sales ORDER BY profit DESC;

- Correlated subquery:
  SELECT col, (SELECT COUNT(*) FROM other WHERE other.fk = main.pk) AS sub_count
  FROM main;

## DIALECT NOTES
- SQLite : strftime('%Y-%m', col), no ILIKE (use LIKE), no FULL OUTER JOIN, no ARRAY.
- PostgreSQL : DATE_TRUNC('month', col), ILIKE, ::type casting, GENERATE_SERIES.
- MySQL  : YEAR(col), DATE_FORMAT(col,'%Y-%m'), backtick identifiers.
- MSSQL  : TOP n, DATEPART(month, col), square-bracket identifiers, CONVERT.

## QUALITY CHECKS (apply before outputting)
- Every JOIN has an ON clause — never implicit cross-joins.
- GROUP BY includes every non-aggregated SELECT column.
- If no LIMIT is specified and the result set could be huge, add LIMIT 1000.
- Avoid SELECT * when specific columns are known; name every column needed.
- Use table aliases to keep the SQL readable.
- SELECT list must not consist solely of `id` / `*_id` columns — always include at least
  one descriptive column (name, title, email, status, category, etc.).
"""


def _clean_sql(raw: str) -> str:
    # Strip any opening code fence with an optional language tag (```sql, ```sqlite, ```postgresql, etc.)
    sql = re.sub(r"```[a-zA-Z0-9_]*", "", raw, flags=re.IGNORECASE).replace("```", "").strip()
    # Handle rare case where the LLM writes "sql\n" as the first line without a fence
    if sql.lower().startswith("sql\n"):
        sql = sql[4:].strip()
    return sql


class PlannerAgent:
    def __init__(self, llm: LLMController, kb_manager: KBManager, dialect: str = "sqlite",
                 graph_store: Optional[SQLGraphStore] = None):
        self.llm = llm
        self.kb = kb_manager
        self.dialect = dialect
        self._graph_store = graph_store

    def plan(self, prev: ResolvedGoalResult) -> PlannerResult:
        logger.info("Stage 4: Planner Agent triggered")

        base_goal = prev.goal_result

        # If ambiguous — skip, executor will handle clarification
        if base_goal.is_ambiguous:
            logger.info("Planner: goal is ambiguous — skipping SQL generation.")
            return PlannerResult(prev, "")

        # Cache hit — bypass LLM
        if base_goal.normalized_result.is_cached:
            logger.info("Planner bypassed — cache hit.")
            return PlannerResult(prev, base_goal.normalized_result.cached_sql)

        operation = prev.resolved_goal_json.get("operation", "general")

        # Schema inspection — generate directly, no LLM needed
        if operation in ("list_columns", "describe_table"):
            sql = _schema_sql(self.dialect, base_goal.relevant_tables)
            logger.info("Planner: schema SQL generated directly for op=%s", operation)
            return PlannerResult(prev, sql)

        # LLM-powered SQL generation
        db_info = self.kb.read_db_info()
        persona_context = self.kb.get_combined_persona_context()

        goal_for_prompt = {k: v for k, v in prev.resolved_goal_json.items() if not k.startswith("_")}
        focus_tables = (
            ", ".join(f"`{t}`" for t in base_goal.relevant_tables)
            if base_goal.relevant_tables else "All tables in schema"
        )

        # Decomposition section (populated by QueryDecomposer when query is complex)
        decomp = prev.resolved_goal_json.get("_decomposition")
        if decomp:
            sub_goals_text = "\n".join(
                f"  {sg['step']}. [{sg.get('operation','')}] {sg['description']} (tables: {sg.get('tables',[])})"
                for sg in decomp.get("sub_goals", [])
            )
            decomposition_section = (
                f"## Query Decomposition Plan\n"
                f"This is a complex multi-step query. Break it into CTEs as follows:\n"
                f"{sub_goals_text}\n\n"
                f"CTE Strategy: {decomp.get('cte_plan','')}\n"
                f"Combined Intent: {decomp.get('combined_intent','')}\n\n"
            )
        else:
            decomposition_section = ""

        # Few-shot feedback examples
        feedback_section = self.kb.get_feedback_examples(max_examples=3)
        if feedback_section:
            feedback_section += "\n"

        # Entity context (for pronoun resolution in follow-up queries)
        entity_ctx = prev.resolved_goal_json.get("_entity_context", "")
        entity_section = f"## Known Entities (from conversation context)\n{entity_ctx}\n\n" if entity_ctx else ""

        # Graph template — inject closest stored SQL as a structural example
        intent_summary = base_goal.intent_summary
        graph_section = ""
        if self._graph_store:
            template_sql = self._graph_store.find_template(
                intent_summary, base_goal.relevant_tables
            )
            if template_sql:
                graph_section = (
                    f"## Structural Template (similar past query — adapt, don't copy)\n"
                    f"```sql\n{template_sql}\n```\n\n"
                )

        sys_prompt = _SYSTEM_PROMPT.format(
            dialect=self.dialect.upper(),
            goal_json=json.dumps(goal_for_prompt, indent=2),
            focus_tables=focus_tables,
            db_info=db_info,
            decomposition_section=decomposition_section,
            entity_section=entity_section,
            graph_section=graph_section,
            feedback_section=feedback_section,
            persona_context=persona_context,
        )

        user_prompt = base_goal.normalized_result.original_input
        context_note = prev.resolved_goal_json.get("_context_note", "")
        if context_note:
            user_prompt = f"{context_note}\n\nUser question: {user_prompt}"

        response_text = self.llm.generate(sys_prompt, user_prompt)
        sql = _clean_sql(response_text)

        logger.debug("Drafted SQL: %s", sql)
        return PlannerResult(prev, sql)
