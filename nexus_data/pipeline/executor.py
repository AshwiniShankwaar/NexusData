"""
nexus_data/pipeline/executor.py — Pipeline Stage 5
Executor: validates → executes → heals → caches → saves turn record.
Handles ambiguous goals by returning a clarification request.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List

from sqlalchemy import Engine

from nexus_data.critic.anomaly_detector import detect_anomalies
from nexus_data.critic.guardian import Guardian, SafetyViolation
from nexus_data.critic.performance_advisor import advise
from nexus_data.critic.pre_validator import SQLPreValidator
from nexus_data.critic.self_healer import execute_with_healing
from nexus_data.engine.llm_controller import LLMController
from nexus_data.kb.manager import KBManager
from nexus_data.kb.vector_repo import VectorQueryRepo
from nexus_data.models import QueryResult
from nexus_data.pipeline.planner import PlannerResult

logger = logging.getLogger(__name__)


class ExecutorAgent:
    def __init__(self, engine: Engine, llm: LLMController, kb_manager: KBManager, vector_repo: VectorQueryRepo):
        self.engine = engine
        self.llm = llm
        self.kb = kb_manager
        self.vector_repo = vector_repo
        self.guardian = Guardian(dialect=engine.dialect.name)
        self._turn_counter = 0

    def execute(self, plan: PlannerResult) -> QueryResult:
        logger.info("Stage 5: Executor logic triggered")

        base_goal = plan.goal_result.goal_result
        goal_json = plan.goal_result.resolved_goal_json
        is_cached_run = base_goal.normalized_result.is_cached
        intent_summary = base_goal.intent_summary
        skip_cache = base_goal.skip_cache

        # ── Restricted (destructive DML/DDL) ─────────────────────────────────
        if base_goal.is_restricted:
            msg = (
                "This operation is restricted. NexusData is a read-only analytics assistant "
                "and cannot perform INSERT, UPDATE, DELETE, DROP, or other data-modification commands. "
                "If you need to modify data, please use your database client directly."
            )
            logger.warning("Executor: blocked restricted operation.")
            self._save_turn_record(plan, sql="", result=None, clarification_question=msg)
            return QueryResult(sql="", error=msg)

        # ── Clarification ────────────────────────────────────────────────────
        if base_goal.is_ambiguous:
            q = base_goal.clarification_question or "Could you clarify your question?"
            logger.info("Executor: returning clarification request.")
            self._save_turn_record(
                plan, sql="", result=None,
                clarification_question=q
            )
            return QueryResult(sql="", is_clarification=True, clarification_question=q)

        # ── Execution ────────────────────────────────────────────────────────
        db_info = self.kb.read_db_info()
        sql_to_run = plan.sql

        # Pre-validate SQL before execution (structural + optional LLM check)
        if sql_to_run and not skip_cache:  # skip for schema/cached queries
            try:
                validator = SQLPreValidator(self.llm, db_info)
                sql_to_run, was_fixed = validator.validate_and_fix(
                    base_goal.normalized_result.original_input,
                    sql_to_run,
                    goal_json,
                )
                if was_fixed:
                    logger.info("Executor: pre-validator corrected SQL before execution.")
            except Exception as exc:
                logger.warning("Pre-validator error (ignored): %s", exc)

        _original_query = base_goal.normalized_result.original_input

        def fix_fn(bad_sql: str, db_error: Any) -> str:
            return self.llm.generate_sql_fix(
                bad_sql, db_error, db_info=db_info, original_query=_original_query
            )

        t_start = time.monotonic()
        try:
            res = execute_with_healing(
                engine=self.engine,
                sql=sql_to_run,
                llm_fix_fn=fix_fn,
                guardian_validate_fn=self.guardian.validate,
            )
            exec_ms = (time.monotonic() - t_start) * 1000

            col_names: List[str] = res.columns if hasattr(res, "columns") else []
            row_count: int = len(res.rows) if hasattr(res, "rows") else 0

            # Anomaly detection (post-execution)
            anomaly_warns: List[str] = []
            if not res.error:
                operation = goal_json.get("operation", "general")
                anomaly_warns = detect_anomalies(
                    sql=res.sql, rows=res.rows,
                    columns=res.columns, operation=operation,
                )
                if anomaly_warns:
                    logger.warning("Anomalies detected: %s", anomaly_warns)

            # Save rich execution metadata for follow-up detection
            exec_meta: Dict[str, Any] = {
                "type": "execution_meta",
                "sql_used": res.sql,
                "filters_used": goal_json.get("filters", []),
                "tables_used": base_goal.relevant_tables,
                "metrics_used": goal_json.get("metrics", []),
                "grouping_used": goal_json.get("grouping", []),
                "result_columns": col_names,
                "row_count": row_count,
            }
            self.kb.add_to_session_cache(exec_meta)

            # Cache successful LLM-generated non-schema queries
            if not res.error and not is_cached_run and not skip_cache:
                self.vector_repo.save_canonical_sql(intent=intent_summary, sql=res.sql)

            # Performance hints on the final executed SQL (after any self-healing)
            perf_hints = advise(res.sql)

            res.from_cache = is_cached_run
            res.anomaly_warnings = anomaly_warns
            res.performance_hints = perf_hints
            res.execution_ms = round(exec_ms, 1)

            self._save_turn_record(plan, sql=res.sql, result=res)
            return res

        except SafetyViolation as exc:
            logger.error("Safety violation: %s", exc)
            return QueryResult(sql=plan.sql, error=f"Safety Check Failed: {exc}")

    # ── Per-turn structured metadata ─────────────────────────────────────────

    def _save_turn_record(
        self,
        plan: PlannerResult,
        sql: str,
        result: Any,
        clarification_question: str = "",
    ) -> None:
        """Save a full structured turn record to session cache for follow-up context."""
        self._turn_counter += 1
        base_goal = plan.goal_result.goal_result
        nq = base_goal.normalized_result.normalized

        record: Dict[str, Any] = {
            "type": "turn_record",
            "turn_id": self._turn_counter,
            "user_query": base_goal.normalized_result.original_input,
            "normalized": {
                "intent_hint": nq.intent_hint,
                "mentioned_tables": nq.mentioned_tables,
                "mentioned_columns": nq.mentioned_columns,
                "temporal": nq.temporal_expression,
                "is_follow_up_hint": nq.is_follow_up_hint,
            },
            "goal": {k: v for k, v in plan.goal_result.resolved_goal_json.items()
                     if not k.startswith("_")},
            "sql": sql,
            "result_summary": (
                f"{len(result.rows)} rows, columns: {result.columns}"
                if result and not result.error else
                (result.error if result else clarification_question)
            ),
        }
        self.kb.add_to_session_cache(record)
        # Also log to short-term memory
        self.kb.append_shortterm_memory(
            f"Turn {self._turn_counter}: '{base_goal.normalized_result.original_input}' → "
            f"op={base_goal.goal_dict.get('operation')} | rows={len(result.rows) if result else 0}"
        )
