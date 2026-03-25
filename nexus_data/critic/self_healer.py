"""
nexus_data/critic/self_healer.py  — Task 3.2
Self-Healer: captures DB errors and runs a correction loop (max 3 retries).
"""
from __future__ import annotations

import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from nexus_data.models import QueryResult
from nexus_data.critic.guardian import SafetyViolation

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class ErrorCapture:
    """Converts raw SQLAlchemy exceptions into structured LLM-readable dicts."""

    @staticmethod
    def capture(exc: Exception) -> Dict[str, str]:
        return {
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(limit=5),
        }


def execute_with_healing(
    engine: Engine,
    sql: str,
    llm_fix_fn: Callable[[str, Dict[str, str]], str],
    guardian_validate_fn: Optional[Callable[[str], str]] = None,
) -> QueryResult:
    """
    Execute *sql* against *engine*, automatically retrying up to MAX_RETRIES
    times using an LLM correction function on failure.

    Parameters
    ----------
    engine            : live SQLAlchemy engine
    sql               : initial SQL string to execute
    llm_fix_fn        : callable(original_sql, error_dict) → corrected_sql
    guardian_validate_fn : optional, called before each attempt to safety-check
    """
    current_sql = sql

    for attempt in range(1, MAX_RETRIES + 1):
        # Safety gate — outside try/except so SafetyViolation is never swallowed
        if guardian_validate_fn:
            current_sql = guardian_validate_fn(current_sql)  # raises SafetyViolation → caller handles it

        try:
            with engine.connect() as conn:
                result = conn.execute(text(current_sql))
                cols = list(result.keys())
                rows = [list(row) for row in result.fetchall()]

            logger.info("Query succeeded on attempt %d.", attempt)
            return QueryResult(sql=current_sql, rows=rows, columns=cols)

        except SafetyViolation:
            raise  # should not happen here but be explicit
        except Exception as exc:  # noqa: BLE001
            err_info = ErrorCapture.capture(exc)
            logger.warning(
                "Attempt %d/%d failed (%s): %s",
                attempt, MAX_RETRIES, err_info["error_type"], err_info["message"],
            )

            if attempt == MAX_RETRIES:
                return QueryResult(
                    sql=current_sql,
                    error=f"Failed after {MAX_RETRIES} attempts: {err_info['message']}",
                )

            # Ask the LLM to produce a corrected query
            current_sql = llm_fix_fn(current_sql, err_info)
            logger.info("Self-healer generated corrected SQL for attempt %d.", attempt + 1)

    # Should never reach here
    return QueryResult(sql=current_sql, error="Unexpected termination of healing loop.")
