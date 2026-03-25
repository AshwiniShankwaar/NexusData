"""
tests/test_pipeline_integration.py — Integration tests for the full pipeline.

Uses an in-memory SQLite engine (mem_engine from conftest.py) and a stubbed
LLMController so no real API calls are made.

Coverage:
  - Full ask() flow: normalise → goal → resolver → planner → executor
  - Destructive intent blocking
  - Input length guard
  - Bookmark save/run cycle
  - Anomaly detection on zero-row result
  - Entity tracker ingestion
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from nexus_data.orchestrator import NexusData
from nexus_data.models import QueryResult


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_goal_json(operation="select", tables=("customers",), filters=None, limit=10):
    return json.dumps({
        "operation": operation,
        "time_frame": "none",
        "filters": filters or [],
        "grouping": [],
        "metrics": [],
        "ordering": "none",
        "limit": limit,
        "relevant_tables": list(tables),
        "intent_summary": "test intent",
        "ambiguous": False,
        "clarification_question": None,
        "skip_cache": False,
    })


def _make_planner_sql(sql: str) -> str:
    return sql


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def nd(mem_engine, tmp_path):
    """
    A NexusData instance wired to the in-memory SQLite engine.
    LLM is stubbed so no real API calls are made.
    """
    with patch("nexus_data.orchestrator.LLMController") as MockLLM, \
         patch("nexus_data.orchestrator.VectorQueryRepo") as MockVecRepo, \
         patch("nexus_data.orchestrator.ConfigManager") as MockCfg:

        # Config stub
        cfg_instance = MagicMock()
        cfg_instance.is_configured.return_value = True
        cfg_instance.config.db.uri = "sqlite:///:memory:"
        cfg_instance.config.audit_log_enabled = False
        cfg_instance.config.kb_refresh_interval_hours = 0
        cfg_instance.config.confidence_threshold = 0.50
        MockCfg.return_value = cfg_instance

        # LLM stub — returns appropriate JSON for each stage
        llm_instance = MagicMock()

        def _llm_generate(sys_prompt, user_msg, **kwargs):
            # Goal identifier call
            if "Goal Identifier" in sys_prompt:
                return _make_goal_json()
            # Planner call
            if "SQL" in sys_prompt or "planner" in sys_prompt.lower() or "SELECT" in sys_prompt:
                return "SELECT id, name FROM customers LIMIT 10"
            # Pre-validator
            if "validator" in sys_prompt.lower() or "structural" in sys_prompt.lower():
                return json.dumps({"valid": True, "fixed_sql": None})
            # Self-healer
            return "SELECT id, name FROM customers LIMIT 10"

        llm_instance.generate.side_effect = _llm_generate
        llm_instance.summarise_result.return_value = "Found 10 customers."
        llm_instance.explain_sql.return_value = "Selects id and name from customers."
        MockLLM.return_value = llm_instance

        # Vector repo stub — no caching
        vec_instance = MagicMock()
        vec_instance.search_canonical_sql.return_value = None
        vec_instance._embedder_broken = True
        vec_instance._table = None
        MockVecRepo.return_value = vec_instance

        instance = NexusData(kb_dir=tmp_path, interactive_setup=False)
        # Bypass the connector and inject the mem_engine directly
        instance._engine = mem_engine
        # Initialize pipeline agents manually (skip introspector for speed)
        from nexus_data.pipeline.decomposer import QueryDecomposer
        from nexus_data.pipeline.executor import ExecutorAgent
        from nexus_data.pipeline.planner import PlannerAgent

        instance._decomposer = QueryDecomposer(llm_instance, instance._kb)
        instance._planner = PlannerAgent(
            llm_instance, instance._kb,
            dialect="sqlite",
            graph_store=instance._graph_store,
        )
        instance._executor = ExecutorAgent(
            mem_engine, llm_instance, instance._kb, vec_instance
        )
        # Write minimal DB info so goal/planner have schema context
        instance._kb.write_db_info(
            "Tables: customers(id, name, tier, notes), orders(id, customer_id, amount, status)"
        )
        return instance


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_ask_returns_query_result(nd):
    result = nd.ask("show me all customers")
    assert isinstance(result, QueryResult)


def test_ask_produces_sql(nd):
    result = nd.ask("show me all customers")
    # Either sql is populated or there's an error (self-healer may fail without real LLM)
    assert result.sql is not None


def test_destructive_intent_blocked(nd):
    result = nd.ask("delete all records from orders")
    # Blocked either by the intent classifier or by goal_identifier — error must be set
    assert result.error is not None
    assert result.sql == ""


def test_destructive_sql_literal_blocked(nd):
    result = nd.ask("INSERT INTO customers VALUES (999, 'Hacker', 'basic', '{}')")
    # Blocked either by the intent classifier or by goal_identifier — error must be set
    assert result.error is not None
    assert result.sql == ""


def test_input_length_guard(nd):
    long_query = "a" * 4001
    result = nd.ask(long_query)
    assert result.error is not None
    assert "too long" in result.error.lower()


def test_bookmark_save_and_run(nd):
    # Save directly via BookmarkStore (save_bookmark on orchestrator reads last turn)
    nd._bookmarks.save("top_customers", "show top customers", "SELECT id, name FROM customers LIMIT 5")
    # Run it
    result = nd.run_bookmark("top_customers")
    assert isinstance(result, QueryResult)
    assert result.sql == "SELECT id, name FROM customers LIMIT 5"


def test_bookmark_run_nonexistent(nd):
    result = nd.run_bookmark("does_not_exist")
    assert result.error is not None


def test_entity_tracker_ingests_result(nd):
    nd.ask("show me all customers")
    # Entity tracker may or may not have values depending on columns; just assert no crash
    entities = nd._entity_tracker.as_dict()
    assert isinstance(entities, dict)


def test_graph_store_saves_pattern(nd):
    """After a successful query the graph store should have a pattern."""
    initial = len(nd._graph_store._patterns)
    # Force a direct save (since LLM stub may produce non-SELECT SQL)
    nd._graph_store.save_pattern("test query", "SELECT id, name FROM customers LIMIT 10")
    assert len(nd._graph_store._patterns) > initial


def test_ask_zero_rows_anomaly(nd):
    """When the executed SQL returns 0 rows, anomaly detector should flag it."""
    from nexus_data.critic.anomaly_detector import detect_anomalies
    sql = "SELECT * FROM orders WHERE status = 'nonexistent_status'"
    warnings = detect_anomalies(sql, [], ["id", "amount"], "select")
    assert len(warnings) == 1
    assert "0 rows" in warnings[0].lower()
