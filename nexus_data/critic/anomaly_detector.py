"""
nexus_data/critic/anomaly_detector.py — Result Anomaly Detector (#1)
Scans a QueryResult for suspicious patterns AFTER execution and returns
a list of human-readable warning strings.

Checks:
- Zero rows on non-aggregate, non-count query (possible bad filter)
- Cartesian explosion: row count >> sum of individual table sizes (not easy to detect here,
  so we use a simple threshold on total rows for JOINed queries)
- Single-row aggregate that is NULL (aggregation on wrong column)
- Extremely large row count with no LIMIT
"""
from __future__ import annotations

import re
from typing import List


_CARTESIAN_THRESHOLD = 100_000   # warn if row_count exceeds this without LIMIT


def detect_anomalies(
    sql: str,
    rows: List,
    columns: List[str],
    operation: str = "general",
) -> List[str]:
    """
    Returns a list of warning strings. Empty list = no anomalies.
    """
    warnings: List[str] = []
    sql_upper = sql.upper()
    row_count = len(rows)

    # ── 1. Zero rows on non-aggregate query ───────────────────────────────────
    # Ops that legitimately return 0 rows or a single-row result without it being an error
    aggregate_ops = {
        "count", "sum", "average", "min", "max", "aggregate",
        "trend", "percentage", "running_total", "pivot",
        "multi_step", "top_n_per_group", "compare",
        "list_columns", "describe_table",
    }
    if (
        row_count == 0
        and operation not in aggregate_ops
    ):
        warnings.append(
            "Query returned 0 rows. This may indicate an overly restrictive filter, "
            "a misspelled value, or a date range with no matching data."
        )

    # ── 2. Large result without LIMIT ─────────────────────────────────────────
    if row_count > _CARTESIAN_THRESHOLD and "LIMIT" not in sql_upper:
        warnings.append(
            f"Very large result set ({row_count:,} rows) with no LIMIT. "
            "This may indicate a cartesian JOIN or missing filter."
        )
    elif row_count > 1000 and "LIMIT" not in sql_upper and operation in ("select", "general", "list"):
        warnings.append(
            f"Large result set ({row_count:,} rows) returned without a LIMIT. "
            "Consider adding LIMIT to the query for better performance."
        )

    # ── 3. JOIN present but suspiciously high row count ───────────────────────
    join_count = sql_upper.count("JOIN")
    if join_count >= 2 and row_count > 10_000 and "WHERE" not in sql_upper:
        warnings.append(
            f"Query has {join_count} JOINs and no WHERE clause — "
            "possible cartesian product."
        )

    # ── 4. Single NULL aggregate result ───────────────────────────────────────
    aggregate_fns = ("COUNT", "SUM", "AVG", "MAX", "MIN")
    has_agg = any(fn in sql_upper for fn in aggregate_fns)
    if has_agg and row_count == 1 and rows:
        row = rows[0]
        all_null = all(v is None for v in (row if isinstance(row, (list, tuple)) else [row]))
        if all_null:
            warnings.append(
                "Aggregate query returned a single NULL. "
                "The aggregated column may be empty or mismatched."
            )

    # ── 5. SELECT * without LIMIT on non-trivial table ────────────────────────
    if "SELECT *" in sql_upper and "LIMIT" not in sql_upper and row_count > 500:
        warnings.append(
            f"SELECT * with no LIMIT returned {row_count} rows. "
            "Consider adding a LIMIT or selecting specific columns."
        )

    return warnings
