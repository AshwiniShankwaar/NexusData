"""
nexus_data/orchestrator.py — Top-level façade over the 5-Stage Pipeline.
"""
from __future__ import annotations
import logging
import threading
import time as _time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Optional

from nexus_data.engine.llm_controller import LLMController
from nexus_data.kb.manager import KBManager
from nexus_data.kb.vector_repo import VectorQueryRepo
from nexus_data.librarian.connector import ConnectionValidator
from nexus_data.librarian.introspector import DatabaseIntrospector
from nexus_data.kb.audit_log import AuditLogger
from nexus_data.kb.bookmarks import BookmarkStore
from nexus_data.kb.conversation_graph import ConversationGraph, ConversationTurn
from nexus_data.kb.entity_tracker import EntityTracker
from nexus_data.kb.graph_store import SQLGraphStore
from nexus_data.kb.kb_updater import ConversationKBUpdater
from nexus_data.pipeline.decomposer import QueryDecomposer
from nexus_data.pipeline.executor import ExecutorAgent
from nexus_data.pipeline.goal_identifier import GoalIdentifierAgent
from nexus_data.pipeline.normalizer import QueryNormalizer
from nexus_data.pipeline.planner import PlannerAgent
from nexus_data.pipeline.reference_resolver import ReferenceResolverAgent
from nexus_data.models import QueryResult
from nexus_data.core.config_manager import ConfigManager
from nexus_data.core.setup_wizard import run_cli_setup

logger = logging.getLogger(__name__)


class NexusData:
    def __init__(self, kb_dir: Optional[Path] = None, timeout_seconds: int = 5,
                 interactive_setup: bool = True,
                 config_manager: Optional[ConfigManager] = None):
        self._config = config_manager or ConfigManager()
        if not self._config.is_configured() and interactive_setup:
            logger.info("First-time boot detected. Running Setup Wizard...")
            run_cli_setup(self._config)

        self._connector = ConnectionValidator(timeout_seconds=timeout_seconds)
        self._kb = KBManager(kb_dir=kb_dir)
        self._vector_repo = VectorQueryRepo(db_dir=kb_dir / "vector_store" if kb_dir else None)
        self._llm = LLMController(self._config)
        self._graph_store = SQLGraphStore(kb_dir=kb_dir)
        self._kb_updater = ConversationKBUpdater(llm=self._llm)
        self._entity_tracker = EntityTracker()
        self._bookmarks = BookmarkStore(kb_dir=kb_dir)
        self._conv_graph = ConversationGraph(kb_dir=kb_dir)
        self._audit = AuditLogger(
            log_dir=kb_dir,
            enabled=self._config.config.audit_log_enabled,
        )
        # Result diff: intent_summary → last QueryResult (bounded OrderedDict, max 100)
        self._result_history: OrderedDict[str, QueryResult] = OrderedDict()
        self._refresh_timer: Optional[threading.Timer] = None

        self._normalizer = QueryNormalizer(self._kb, self._vector_repo)
        self._identifier = GoalIdentifierAgent(self._llm, self._kb, self._config)
        self._resolver = ReferenceResolverAgent(self._kb, self._conv_graph)
        self._decomposer: Optional[QueryDecomposer] = None
        self._planner: Optional[PlannerAgent] = None
        self._executor: Optional[ExecutorAgent] = None
        self._engine = None

    def connect_and_initialize(self, db_uri: Optional[str] = None, interactive: bool = True) -> None:
        uri = db_uri or self._config.config.db.uri
        if not uri:
            raise ValueError("No Database URI configured.")
        self._engine = self._connector.get_engine(uri)
        # Pass LLM to introspector for AI auto-describe
        introspector = DatabaseIntrospector(self._engine, self._kb, self._config, llm=self._llm)
        introspector.initialize(interactive=interactive)
        self._decomposer = QueryDecomposer(self._llm, self._kb)
        self._planner = PlannerAgent(
            self._llm, self._kb,
            dialect=self._engine.dialect.name,
            graph_store=self._graph_store,
        )
        self._executor = ExecutorAgent(self._engine, self._llm, self._kb, self._vector_repo)
        logger.info("System initialized. Knowledge Base & Vector Repo ready.")

        # Start background KB refresh if configured
        hours = self._config.config.kb_refresh_interval_hours
        if hours > 0:
            self.start_background_refresh(interval_seconds=hours * 3600)

    def set_user_context(self, note: str) -> None:
        self._kb.append_longterm_memory(note)

    _INTENT_SYSTEM = """\
You are an intent classifier for a natural-language SQL assistant.
The assistant can ONLY read data from a database (SELECT queries, aggregations, joins, filters, analysis, charts).

Classify the user message into exactly ONE of these categories:

data          - The user wants to query, retrieve, count, filter, group, sort, analyse, or visualise data from the database.
                ALSO classify as data: questions asking WHICH database is currently connected/active, WHAT model or LLM is currently being used, or any other question about the current system state — these are informational lookups, not actions.
clear_memory  - The user explicitly wants to clear, reset, wipe, or delete the chat history, memory, cache, or session.
destructive   - The user wants to delete rows, drop tables, truncate data, modify/update records, or alter the database schema.
logout        - The user explicitly wants to log out, sign out, exit the app, or end the session.
change_model  - The user explicitly wants to SWITCH TO, CHANGE TO, or UPDATE which AI model or LLM provider is used (an action, not a question about the current model).
change_db     - The user explicitly wants to SWITCH TO, CONNECT TO, or CHANGE which database is active (an action, not a question about the current database).
unsupported   - The user wants something this assistant cannot do (e.g. send emails, browse the web, write files, create tables, insert data, run code unrelated to data analysis).

Key distinction: "which db are you connected to?" or "what model are you using?" = data. "switch to postgres" or "use GPT-4" = change_db/change_model.

Reply with ONLY the single category word. No punctuation, no explanation."""

    _INTENT_GUIDANCE = {
        "clear_memory": (
            "To clear memory or history:\n"
            "  • CLI → /clear-cache  (wipes the semantic vector cache)\n"
            "  • UI  → Settings › Profile › Clear All Conversations & Data"
        ),
        "destructive": (
            "This assistant is read-only and cannot delete, update, or modify your database.\n"
            "To make changes, connect directly to your database with a client (e.g. DBeaver, psql, mysql CLI)."
        ),
        "logout": (
            "To log out:\n"
            "  • UI  → click Logout in the sidebar\n"
            "  • CLI → type  exit  or press Ctrl+C"
        ),
        "change_model": (
            "To change the AI model:\n"
            "  • CLI → /change-model  (interactive wizard)\n"
            "  • UI  → Settings › Model Config  (view only — full change via CLI)"
        ),
        "change_db": (
            "To switch the active database:\n"
            "  • CLI → /change-db\n"
            "  • UI  → click the database name in the sidebar › DB Picker"
        ),
        "unsupported": (
            "This assistant only answers questions about your database data.\n"
            "It cannot perform actions outside of reading and analysing data.\n"
            "If you meant to query your data, try rephrasing your question."
        ),
    }

    def _classify_intent(self, query: str) -> "Optional[QueryResult]":
        """
        Ask the LLM to classify the query intent.
        Returns a QueryResult with a helpful message if the intent is non-data,
        or None if it is a data query that should proceed through the pipeline.
        Fails open (returns None) on any error so the pipeline always runs as fallback.
        """
        try:
            category = self._llm.generate(
                self._INTENT_SYSTEM,
                f"User message: {query}",
            ).strip().lower().split()[0]  # take first word only in case model adds extras
        except Exception:
            return None  # fail open — let the pipeline handle it

        if category == "data":
            return None  # proceed normally

        msg = self._INTENT_GUIDANCE.get(
            category,
            self._INTENT_GUIDANCE["unsupported"],
        )
        return QueryResult(sql="", error=msg)

    _MAX_QUERY_LEN = 4000  # chars — guard against accidental paste of huge blobs

    def ask(self, nl_query: str, phase_callback: Optional[Callable[[str, float], None]] = None) -> QueryResult:
        if not self._engine or not self._planner or not self._executor:
            raise RuntimeError("Not connected. Call connect_and_initialize() first.")

        # Input length guard
        if len(nl_query) > self._MAX_QUERY_LEN:
            return QueryResult(
                sql="",
                error=(
                    f"Query too long ({len(nl_query)} chars, max {self._MAX_QUERY_LEN}). "
                    "Please shorten your question."
                ),
            )

        # Prompt injection guard
        from nexus_data.critic.guardian import Guardian, PromptInjectionError
        _guard = Guardian()
        try:
            nl_query = _guard.check_user_input(nl_query)
        except PromptInjectionError as e:
            return QueryResult(sql="", error=str(e))

        # Intent classifier — LLM decides whether this is a data query or a system action
        _intent_result = self._classify_intent(nl_query)
        if _intent_result is not None:
            return _intent_result

        # Phase timing helper
        t0 = _time.monotonic()

        def _emit(phase: str) -> None:
            if phase_callback:
                phase_callback(phase, round((_time.monotonic() - t0) * 1000, 1))

        # Self-improving KB: scan the message for domain knowledge assertions
        self._kb_updater.scan_and_update(nl_query, self._kb)

        _emit("normalizing")
        norm = self._normalizer.normalize(nl_query)
        _emit("identifying_goal")
        goal = self._identifier.identify(norm)
        _emit("resolving_references")
        resolved = self._resolver.resolve(goal)

        # Entity context injection for pronoun resolution (#3)
        if self._entity_tracker.has_pronoun(nl_query):
            entity_ctx = self._entity_tracker.resolve_context()
            if entity_ctx:
                resolved.resolved_goal_json["_entity_context"] = entity_ctx

        # Decompose complex queries — injects _decomposition key into resolved_goal_json
        if self._decomposer and not goal.is_ambiguous:
            decomp = self._decomposer.decompose(nl_query, resolved.resolved_goal_json)
            if decomp.is_complex and decomp.enriched_goal.get("_decomposition"):
                resolved.resolved_goal_json["_decomposition"] = decomp.enriched_goal["_decomposition"]

        # Correction shortcut — route to ask_with_feedback using last turn's SQL
        if goal.goal_dict.get("operation") == "correction":
            last = self._kb.get_last_turn_record()
            if last and last.get("sql"):
                logger.info("Correction detected — routing to ask_with_feedback.")
                return self.ask_with_feedback(
                    original_query=last.get("user_query", nl_query),
                    bad_sql=last["sql"],
                    feedback=nl_query,
                )

        _emit("planning")
        plan = self._planner.plan(resolved)
        _emit("executing")
        result = self._executor.execute(plan)

        # Track cache hit for adaptive threshold
        if result.from_cache and not result.error:
            self._vector_repo.record_hit()

        # Save successful SQL as a graph pattern for future template reuse
        if not result.error and not result.is_clarification and result.sql:
            self._graph_store.save_pattern(intent=goal.intent_summary, sql=result.sql)
            # Record this turn in the conversation lineage graph
            self._conv_graph.add_turn(ConversationTurn(
                turn_id=str(uuid.uuid4()),
                query=nl_query,
                intent_summary=goal.intent_summary,
                sql_used=result.sql,
                tables_used=goal.relevant_tables or [],
                filters_used=goal.goal_dict.get("filters", []),
                grouping_used=goal.goal_dict.get("grouping", []),
                metrics_used=goal.goal_dict.get("metrics", []),
                parent_turn_id=resolved.resolved_goal_json.get("_parent_turn_id"),
                timestamp=datetime.now(timezone.utc).isoformat(),
            ))

        # Entity tracker — extract entities from result for pronoun resolution
        if not result.error and not result.is_clarification and result.rows:
            self._entity_tracker.ingest_result(result.columns, result.rows)
        if not goal.is_ambiguous:
            self._entity_tracker.ingest_filters(goal.goal_dict.get("filters", []))

        # Result diffing — compare to previous result for same intent
        if not result.error and not result.is_clarification:
            result.diff_summary = self._diff_result(goal.intent_summary, result)
            # Bounded history: evict oldest when at capacity
            if len(self._result_history) >= 100:
                self._result_history.popitem(last=False)
            self._result_history[goal.intent_summary] = result

        # Natural language summary (only for real data results)
        if not result.error and not result.is_clarification and result.rows:
            try:
                result.natural_language_summary = self._llm.summarise_result(
                    nl_query, result.sql, result.columns, result.rows
                )
            except Exception:
                pass

        # Confidence heuristic
        result.confidence = self._estimate_confidence(result, goal)

        # Audit log
        self._audit.log(
            query=nl_query,
            sql=result.sql,
            operation=goal.goal_dict.get("operation", "general"),
            rows=len(result.rows),
            columns=result.columns,
            error=result.error,
            from_cache=result.from_cache,
            confidence=result.confidence,
            anomaly_warnings=result.anomaly_warnings,
            execution_ms=result.execution_ms,
        )

        # Cap short-term memory log
        self._cap_history()

        _emit("done")
        return result

    def ask_with_feedback(self, original_query: str, bad_sql: str, feedback: str) -> QueryResult:
        """Re-run query incorporating user feedback.
        Generates corrected SQL directly via LLM (bypasses the normaliser/goal/planner
        chain so the full DB schema never inflates the query length limit).
        """
        if not self._engine or not self._executor:
            raise RuntimeError("Not connected. Call connect_and_initialize() first.")

        self._vector_repo.record_correction()

        db_info = self._kb.read_db_info()
        error_ctx = f"User feedback: {feedback}"
        corrected_sql = self._llm.generate_sql_fix(
            bad_sql=bad_sql,
            db_error=error_ctx,
            db_info=db_info,
            original_query=original_query,
        )

        from nexus_data.critic.guardian import Guardian, SafetyViolation
        from nexus_data.critic.self_healer import execute_with_healing
        guardian = Guardian(dialect=self._engine.dialect.name)
        try:
            result = execute_with_healing(
                engine=self._engine,
                sql=corrected_sql,
                llm_fix_fn=lambda bad, err: self._llm.generate_sql_fix(
                    bad, err, db_info=db_info, original_query=original_query
                ),
                guardian_validate_fn=guardian.validate,
            )
        except SafetyViolation as exc:
            return QueryResult(sql=corrected_sql, error=f"Safety Check Failed: {exc}")

        result.confidence = 0.85  # feedback-corrected SQL gets a baseline confidence

        # Save as few-shot example for future queries
        if not result.error and result.sql:
            self._kb.append_feedback_example(
                query=original_query,
                bad_sql=bad_sql,
                good_sql=result.sql,
                feedback=feedback,
            )

        return result

    # ── Explain mode (#2) ────────────────────────────────────────────────────

    def explain_last(self, query: str) -> str:
        """Return a plain-English explanation of the last executed SQL."""
        last = self._kb.get_last_turn_record()
        if not last or not last.get("sql"):
            return "No previous query to explain."
        return self._llm.explain_sql(last["sql"], query)

    # ── Bookmarks (#7) ───────────────────────────────────────────────────────

    def save_bookmark(self, name: str) -> str:
        """Save the last query+SQL as a named bookmark."""
        last = self._kb.get_last_turn_record()
        if not last:
            return "Nothing to bookmark — ask a question first."
        self._bookmarks.save(name, last["user_query"], last["sql"])
        return f"Bookmarked as '{name}'."

    def run_bookmark(self, name: str) -> QueryResult:
        """Run a named bookmark directly (bypasses LLM, uses stored SQL)."""
        entry = self._bookmarks.get(name)
        if not entry:
            return QueryResult(sql="", error=f"Bookmark '{name}' not found. Use /bookmarks to list.")
        query, sql = entry
        # Execute stored SQL directly via self-healer (safety validated)
        from nexus_data.critic.guardian import Guardian, SafetyViolation
        from nexus_data.critic.self_healer import execute_with_healing
        db_info = self._kb.read_db_info()
        guardian = Guardian(dialect=self._engine.dialect.name)
        try:
            res = execute_with_healing(
                engine=self._engine,
                sql=sql,
                llm_fix_fn=lambda bad, err: self._llm.generate_sql_fix(bad, err, db_info=db_info),
                guardian_validate_fn=guardian.validate,
            )
            res.from_cache = True  # bookmark = cached intent
            return res
        except SafetyViolation as exc:
            return QueryResult(sql=sql, error=f"Safety Check Failed: {exc}")

    # ── Scheduled KB refresh (#5) ────────────────────────────────────────────

    def start_background_refresh(self, interval_seconds: int = 86400) -> None:
        """Start a background timer that checks for schema drift periodically."""
        def _refresh():
            logger.info("Background KB refresh triggered.")
            try:
                if self._engine:
                    introspector = DatabaseIntrospector(
                        self._engine, self._kb, self._config, llm=self._llm
                    )
                    introspector.initialize(interactive=False)
            except Exception as exc:
                logger.warning("Background KB refresh failed: %s", exc)
            finally:
                # Reschedule
                self._refresh_timer = threading.Timer(interval_seconds, _refresh)
                self._refresh_timer.daemon = True
                self._refresh_timer.start()

        self._refresh_timer = threading.Timer(interval_seconds, _refresh)
        self._refresh_timer.daemon = True
        self._refresh_timer.start()
        logger.info("Background KB refresh scheduled every %dh.", interval_seconds // 3600)

    def stop_background_refresh(self) -> None:
        if self._refresh_timer:
            self._refresh_timer.cancel()
            self._refresh_timer = None

    # ── Result diffing (#6) ──────────────────────────────────────────────────

    def _diff_result(self, intent: str, result: QueryResult) -> Optional[str]:
        """Compare current result to previous result for the same intent."""
        prev = self._result_history.get(intent)
        if not prev or not prev.rows or not result.rows:
            return None
        try:
            prev_set = set(tuple(r) if isinstance(r, list) else r for r in prev.rows)
            curr_set = set(tuple(r) if isinstance(r, list) else r for r in result.rows)
        except TypeError:
            return None  # unhashable types (e.g. JSON columns) — skip diff
        added = len(curr_set - prev_set)
        removed = len(prev_set - curr_set)
        if added == 0 and removed == 0:
            return "Result unchanged since last run."
        parts = []
        if added:
            parts.append(f"+{added} new row{'s' if added != 1 else ''}")
        if removed:
            parts.append(f"-{removed} removed row{'s' if removed != 1 else ''}")
        return f"Δ vs last run: {', '.join(parts)}."

    def _estimate_confidence(self, result: QueryResult, goal) -> float:
        """
        Heuristic confidence score (0.0–1.0).
        Starts at 0.90 and applies deductions for known quality signals.
        """
        if result.is_clarification:
            return 0.0
        if result.error:
            return 0.2
        if goal.is_ambiguous:
            return 0.3
        if result.from_cache:
            return 0.97   # cached SQL was previously validated
        if goal.is_restricted:
            return 0.0

        score = 0.90

        # Deductions for uncertainty signals
        if result.anomaly_warnings:
            score -= 0.10 * min(len(result.anomaly_warnings), 2)

        # Complex operations have inherently higher chance of SQL error
        op = goal.goal_dict.get("operation", "general")
        complex_ops = {
            "compare", "trend", "rank", "top_n", "aggregate", "general",
            "multi_step", "top_n_per_group", "running_total", "percentage", "pivot",
        }
        if op in complex_ops:
            score -= 0.05

        # Decomposed queries are higher risk
        if goal.goal_dict.get("_decomposition"):
            score -= 0.05

        # Window function or subquery adds uncertainty
        if goal.goal_dict.get("needs_window_function") or goal.goal_dict.get("needs_subquery"):
            score -= 0.03

        return max(0.10, round(score, 2))

    def _cap_history(self) -> None:
        """Keep shortterm_memory.md within configured line limit."""
        self._kb.cap_shortterm_memory(self._config.config.history_max_lines)
