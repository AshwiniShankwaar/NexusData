"""
nexus_data/critic/performance_advisor.py — SQL Performance Advisor (#4)
Analyses generated SQL BEFORE execution for common performance anti-patterns.
Returns a list of hint strings to display to the user.
Zero LLM cost — pure regex/string analysis.
"""
from __future__ import annotations

import re
from typing import List


def advise(sql: str, indexed_columns: List[str] | None = None) -> List[str]:
    """
    Returns a list of performance hints. Empty = no issues found.

    Args:
        sql: The SQL string to analyse.
        indexed_columns: Optional list of column names known to be indexed
                         (from KB/introspector). Used to suggest better WHERE columns.
    """
    hints: List[str] = []
    sql_upper = sql.upper()
    indexed = set(c.upper() for c in (indexed_columns or []))

    # ── SELECT * ──────────────────────────────────────────────────────────────
    if re.search(r"\bSELECT\s+\*", sql_upper):
        hints.append(
            "SELECT * fetches all columns — consider selecting only the columns you need "
            "to reduce I/O and speed up the query."
        )

    # ── No LIMIT on large SELECT ───────────────────────────────────────────────
    has_agg = (
        any(f in sql_upper for f in ("COUNT(", "COUNT (*)", "SUM(", "AVG(", "MAX(", "MIN("))
        or "GROUP BY" in sql_upper
        or "OVER (" in sql_upper  # window functions
    )
    if not has_agg and "LIMIT" not in sql_upper and "TOP " not in sql_upper:
        hints.append(
            "No LIMIT clause — for large tables this may return all rows. "
            "Consider adding LIMIT unless you need the full result set."
        )

    # ── WHERE on non-indexed column ───────────────────────────────────────────
    if indexed:
        where_match = re.search(
            r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)", sql_upper, re.DOTALL
        )
        if where_match:
            where_clause = where_match.group(1)
            # Extract column names in WHERE clause (simple word extraction)
            cols_in_where = set(re.findall(r"\b([A-Z_][A-Z0-9_]*)\b", where_clause))
            cols_in_where -= {"AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE",
                              "BETWEEN", "TRUE", "FALSE", "EXISTS"}
            non_indexed = cols_in_where - indexed
            if non_indexed and cols_in_where:
                hints.append(
                    f"WHERE clause uses non-indexed column(s): {', '.join(sorted(non_indexed))}. "
                    "This may cause a full table scan. Consider using indexed columns in filters."
                )

    # ── LIKE with leading wildcard ─────────────────────────────────────────────
    if re.search(r"LIKE\s+'%\w", sql_upper):
        hints.append(
            "LIKE '%...' with a leading wildcard cannot use indexes and will scan the full column."
        )

    # ── OR in WHERE (index skipping) ──────────────────────────────────────────
    if " OR " in sql_upper and "WHERE" in sql_upper:
        hints.append(
            "OR conditions in WHERE clauses can prevent index usage in some databases. "
            "Consider UNION ALL if performance is critical."
        )

    # ── No JOIN condition (implicit cross-join) ────────────────────────────────
    join_count = sql_upper.count(" JOIN ")
    if join_count > 0 and "ON " not in sql_upper and "USING(" not in sql_upper:
        hints.append(
            "JOIN detected without an ON or USING clause — possible implicit cross-join."
        )

    # ── ORDER BY on non-indexed column ────────────────────────────────────────
    if indexed:
        order_match = re.search(r"ORDER BY\s+([^LIMIT\n]+)", sql_upper)
        if order_match:
            order_cols = set(re.findall(r"\b([A-Z_][A-Z0-9_]*)\b", order_match.group(1)))
            order_cols -= {"ASC", "DESC", "NULLS", "LAST", "FIRST"}
            non_idx_order = order_cols - indexed
            if non_idx_order:
                hints.append(
                    f"ORDER BY uses non-indexed column(s): {', '.join(sorted(non_idx_order))}. "
                    "This requires a full sort — add an index if this query runs frequently."
                )

    return hints
