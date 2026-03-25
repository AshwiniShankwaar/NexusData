"""
nexus_data/librarian/profiler.py  — Task 1.2
Deep Profiler: CardinalityScanner + TypeInferrer + RelationshipMapper
Uses sqlalchemy.inspect as the foundational unit.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from nexus_data.models import ColumnMeta, DatabaseProfile, TableMeta

logger = logging.getLogger(__name__)

# ── Heuristics ────────────────────────────────────────────────────────────────
DATE_PATTERNS: list[re.Pattern] = [
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),                    # 2024-01-01
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}"),         # ISO-8601
    re.compile(r"^\d{2}/\d{2}/\d{4}$"),                     # 01/23/2024
    re.compile(r"^\d{2}-\d{2}-\d{4}$"),                     # 01-23-2024
]

CATEGORY_RATIO_THRESHOLD = 0.05   # < 5 % distinct → category
SAMPLE_SIZE = 20


def _infer_string_type(value: str) -> str:
    """Best-effort heuristic type for a string sample value."""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, (dict, list)):
            return "json"
    except (ValueError, TypeError):
        pass
    for pat in DATE_PATTERNS:
        if pat.match(value.strip()):
            return "datetime"
    return ""


# ── CardinalityScanner ────────────────────────────────────────────────────────

def _cardinality_ratio(engine: Engine, table: str, column: str) -> float:
    """Return distinct/total ratio for *column* in *table*.  1.0 on failure."""
    sql = text(
        f"SELECT CAST(COUNT(DISTINCT {column}) AS FLOAT) / NULLIF(COUNT({column}), 0) "
        f"FROM {table} WHERE {column} IS NOT NULL"
    )
    try:
        with engine.connect() as conn:
            ratio = conn.execute(sql).scalar()
        return float(ratio) if ratio is not None else 1.0
    except SQLAlchemyError as exc:
        logger.debug("Cardinality query failed for %s.%s: %s", table, column, exc)
        return 1.0


# ── TypeInferrer ──────────────────────────────────────────────────────────────

def _sample_and_infer(
    engine: Engine, table: str, column: str
) -> Tuple[List[Any], str]:
    """Return (sample_values, inferred_type) by sampling up to SAMPLE_SIZE rows."""
    sql = text(
        f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT {SAMPLE_SIZE}"
    )
    samples: List[Any] = []
    inferred = ""
    try:
        with engine.connect() as conn:
            for row in conn.execute(sql):
                val = row[0]
                samples.append(val)
                if not inferred and isinstance(val, str):
                    inferred = _infer_string_type(val)
    except SQLAlchemyError as exc:
        logger.debug("Sampling failed for %s.%s: %s", table, column, exc)
    return samples, inferred


# ── RelationshipMapper ────────────────────────────────────────────────────────

def _find_implicit_joins(tables: List[TableMeta]) -> Dict[str, List[str]]:
    """
    Heuristic: columns with *identical names* across tables that end with
    common FK suffixes (_id, _fk, _key) are marked as implicit join candidates.

    Returns
    -------
    dict mapping "table.column" → list of "other_table.column" matches
    """
    suffix_re = re.compile(r"_id$|_fk$|_key$", re.IGNORECASE)
    col_index: Dict[str, List[str]] = {}

    for tbl in tables:
        for col in tbl.columns:
            if suffix_re.search(col.name):
                col_index.setdefault(col.name, []).append(f"{tbl.name}.{col.name}")

    return {
        col_name: refs
        for col_name, refs in col_index.items()
        if len(refs) >= 2   # only interesting if seen in ≥ 2 tables
    }


# ── Main Profiler ─────────────────────────────────────────────────────────────

def build_database_profile(engine: Engine) -> DatabaseProfile:
    """
    Full introspection of a SQLAlchemy-connected database.

    Steps
    -----
    1. `inspect()` → table names, columns, PKs, FKs
    2. TypeInferrer → sample 20 rows, detect JSON / datetime
    3. CardinalityScanner → detect low-cardinality category columns
    4. RelationshipMapper → find implicit joins across tables

    Returns
    -------
    DatabaseProfile — a structured, Pydantic-validated description of the DB.
    """
    inspector = inspect(engine)
    dialect = engine.dialect.name
    tables: List[TableMeta] = []

    for table_name in inspector.get_table_names():
        pk_cols: List[str] = inspector.get_pk_constraint(table_name).get(
            "constrained_columns", []
        )
        fk_cols: List[str] = [
            c
            for fk in inspector.get_foreign_keys(table_name)
            for c in fk.get("constrained_columns", [])
        ]

        columns: List[ColumnMeta] = []
        for col_info in inspector.get_columns(table_name):
            col_name = col_info["name"]
            col_type = str(col_info["type"])

            # TypeInferrer
            samples, inferred = _sample_and_infer(engine, table_name, col_name)

            # CardinalityScanner (only for textual columns)
            cardinality = 1.0
            is_textual = any(
                kw in col_type.upper()
                for kw in ("VARCHAR", "TEXT", "CHAR", "STRING", "NVARCHAR", "CLOB")
            )
            if is_textual:
                cardinality = _cardinality_ratio(engine, table_name, col_name)
                if inferred == "" and cardinality < CATEGORY_RATIO_THRESHOLD:
                    inferred = "category"

            columns.append(
                ColumnMeta(
                    name=col_name,
                    type=col_type,
                    is_primary_key=col_name in pk_cols,
                    is_foreign_key=col_name in fk_cols,
                    sample_values=samples[:5],  # keep profile compact
                    cardinality_ratio=round(cardinality, 4),
                    inferred_type=inferred,
                )
            )

        tables.append(TableMeta(name=table_name, columns=columns))

    # RelationshipMapper — annotate implicit join columns
    implicit = _find_implicit_joins(tables)
    if implicit:
        logger.info("Implicit join candidates: %s", list(implicit.keys()))

    return DatabaseProfile(tables=tables, dialect=dialect)
