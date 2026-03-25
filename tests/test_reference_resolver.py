"""
tests/test_reference_resolver.py
Integration tests for the Reference Resolver handling follow-up queries.
Tests the conversation lineage graph: branching follow-ups, parent lookup,
and context carry-forward rules.
"""
from __future__ import annotations

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from nexus_data.kb.manager import KBManager
from nexus_data.kb.conversation_graph import ConversationGraph, ConversationTurn
from nexus_data.pipeline.goal_identifier import GoalIdentifierResult
from nexus_data.pipeline.normalizer import NormalizerResult, NormalizedQuery
from nexus_data.pipeline.reference_resolver import ReferenceResolverAgent


def _make_norm(text: str, is_follow_up: bool = False) -> NormalizerResult:
    nq = NormalizedQuery(
        raw_query=text,
        normalized_text=text.lower(),
        intent_hint="select",
        mentioned_tables=[],
        mentioned_columns=[],
        temporal_expression=None,
        numeric_values=[],
        is_follow_up_hint=is_follow_up,
    )
    return NormalizerResult(
        original_input=text,
        normalized_input=text.lower(),
        normalized=nq,
    )


def _make_turn(turn_id: str, tables: list, filters: list, sql: str,
               parent_turn_id=None, intent="test") -> ConversationTurn:
    return ConversationTurn(
        turn_id=turn_id,
        query="test query",
        intent_summary=intent,
        sql_used=sql,
        tables_used=tables,
        filters_used=filters,
        grouping_used=[],
        metrics_used=[],
        parent_turn_id=parent_turn_id,
        timestamp="2026-01-01T00:00:00+00:00",
    )


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def kb(tmp_dir):
    return KBManager(kb_dir=tmp_dir)


@pytest.fixture
def graph(tmp_dir):
    return ConversationGraph(kb_dir=tmp_dir)


class TestReferenceResolver:

    def test_follow_up_augmentation(self, kb, graph):
        """Follow-up query inherits filters from previous turn via graph."""
        graph.add_turn(_make_turn(
            "t1", tables=["users"], filters=["region = 'US'"],
            sql="SELECT * FROM users WHERE region = 'US'",
        ))

        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        follow_up_goal = {
            "operation": "follow_up",
            "time_frame": "none",
            "filters": ["status = 'active'"],
            "grouping": [],
            "metrics": [],
        }
        norm = _make_norm("What about active ones?", is_follow_up=True)
        goal_res = GoalIdentifierResult(
            normalized_result=norm,
            goal_dict=follow_up_goal,
            relevant_tables=[],
            intent_summary="Filter previous by active status",
        )

        resolved = resolver.resolve(goal_res)

        assert resolved.is_follow_up is True
        final_filters = resolved.resolved_goal_json["filters"]
        assert "region = 'US'" in final_filters
        assert "status = 'active'" in final_filters
        assert len(final_filters) == 2
        assert resolved.goal_result.relevant_tables == ["users"]

    def test_non_follow_up_keeps_isolated(self, kb, graph):
        """Non-follow-up query does not inherit any previous context."""
        graph.add_turn(_make_turn(
            "t1", tables=["users"], filters=["region = 'US'"],
            sql="SELECT * FROM users WHERE region = 'US'",
        ))

        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        fresh_goal = {
            "operation": "select",
            "time_frame": "none",
            "filters": [],
            "grouping": [],
            "metrics": [],
        }
        norm = _make_norm("Show me products")
        goal_res = GoalIdentifierResult(norm, fresh_goal, ["products"], "fetch products")

        resolved = resolver.resolve(goal_res)

        assert resolved.is_follow_up is False
        assert resolved.resolved_goal_json["filters"] == []
        assert resolved.goal_result.relevant_tables == ["products"]

    def test_branch_detection_bypasses_wrong_lineage(self, kb, graph):
        """
        Q1(orders) → FQ2(orders) → FQ3(orders)
        FQ4 references 'users' table — should still find a parent via fallback
        but NOT carry order-specific filters since table overlap is 0%.
        """
        graph.add_turn(_make_turn("q1", ["orders"], ["status='pending'"],
                                  "SELECT * FROM orders WHERE status='pending'", intent="Q1"))
        graph.add_turn(_make_turn("fq2", ["orders"], ["status='pending'", "region='US'"],
                                  "SELECT * FROM orders WHERE ...", parent_turn_id="q1", intent="FQ2"))
        graph.add_turn(_make_turn("fq3", ["orders"], ["status='pending'", "region='US'"],
                                  "SELECT COUNT(*) FROM orders WHERE ...", parent_turn_id="fq2", intent="FQ3"))

        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        # FQ4: asking about users (different table — no overlap with orders)
        fq4_goal = {
            "operation": "select",
            "filters": [],
            "grouping": [],
            "metrics": [],
            "relevant_tables": ["users"],
        }
        norm = _make_norm("Show me all users")
        goal_res = GoalIdentifierResult(norm, fq4_goal, ["users"], "show users")

        resolved = resolver.resolve(goal_res)

        # No pronoun / reference pattern → not a follow-up despite graph having entries
        assert resolved.is_follow_up is False
        # No order filters should bleed into this query
        assert resolved.resolved_goal_json["filters"] == []

    def test_same_table_follow_up_gets_correct_parent(self, kb, graph):
        """
        Q1(orders) → FQ2(orders, users) — the next follow-up about orders
        should attach to FQ2 (most recent with overlap >= 0.5).
        """
        graph.add_turn(_make_turn("q1", ["orders"], ["year=2024"],
                                  "SELECT * FROM orders WHERE year=2024", intent="Q1"))
        graph.add_turn(_make_turn("fq2", ["orders", "users"], ["year=2024"],
                                  "SELECT o.*, u.name FROM orders o JOIN users u ...",
                                  parent_turn_id="q1", intent="FQ2"))

        best = graph.find_best_parent(["orders"])
        # fq2 is most recent with overlap; both q1 and fq2 have "orders"
        # fq2 is newer so it should win
        assert best is not None
        assert best.turn_id == "fq2"

    def test_entity_id_filter_not_carried_without_pronoun(self, kb, graph):
        """Entity-ID filters (users.id = 3) must not bleed into unrelated follow-ups."""
        graph.add_turn(_make_turn(
            "t1", tables=["users"], filters=["users.id = 3"],
            sql="SELECT * FROM users WHERE users.id = 3",
        ))

        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        goal = {
            "operation": "follow_up",
            "filters": [],
            "grouping": [],
            "metrics": [],
        }
        # "them" would be a pronoun — using a non-pronoun query instead
        norm = _make_norm("What is their total order count?", is_follow_up=True)
        goal_res = GoalIdentifierResult(norm, goal, [], "order count")

        resolved = resolver.resolve(goal_res)

        # "their" is not in _REFERENCE_PATTERNS — so entity ID filter should not be carried
        final_filters = resolved.resolved_goal_json["filters"]
        assert "users.id = 3" not in final_filters

    def test_parent_turn_id_annotated_in_goal(self, kb, graph):
        """Resolver annotates _parent_turn_id so the orchestrator can link turns."""
        graph.add_turn(_make_turn("t1", ["orders"], [], "SELECT * FROM orders", intent="Q1"))

        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        goal = {"operation": "follow_up", "filters": [], "grouping": [], "metrics": []}
        norm = _make_norm("Show me those orders", is_follow_up=True)
        goal_res = GoalIdentifierResult(norm, goal, [], "follow up orders")

        resolved = resolver.resolve(goal_res)

        assert resolved.is_follow_up is True
        assert resolved.resolved_goal_json.get("_parent_turn_id") == "t1"

    def test_empty_graph_no_follow_up(self, kb, graph):
        """With an empty graph, nothing is detected as a follow-up."""
        resolver = ReferenceResolverAgent(kb_manager=kb, conv_graph=graph)
        goal = {"operation": "follow_up", "filters": [], "grouping": [], "metrics": []}
        norm = _make_norm("Show those results", is_follow_up=True)
        goal_res = GoalIdentifierResult(norm, goal, [], "test")

        resolved = resolver.resolve(goal_res)

        # No parent in graph → is_follow_up must be False
        assert resolved.is_follow_up is False


class TestConversationGraph:

    def test_add_and_get(self, graph):
        turn = _make_turn("t1", ["orders"], [], "SELECT 1")
        graph.add_turn(turn)
        assert graph.get_turn("t1") is turn

    def test_eviction_at_max(self, tmp_dir):
        from nexus_data.kb.conversation_graph import _MAX_TURNS
        g = ConversationGraph(kb_dir=tmp_dir)
        for i in range(_MAX_TURNS + 5):
            g.add_turn(_make_turn(f"t{i}", ["orders"], [], "SELECT 1"))
        assert len(g) == _MAX_TURNS
        # The first 5 should have been evicted
        for i in range(5):
            assert g.get_turn(f"t{i}") is None

    def test_find_best_parent_empty(self, graph):
        assert graph.find_best_parent(["orders"]) is None

    def test_find_best_parent_overlap(self, graph):
        graph.add_turn(_make_turn("t1", ["orders"], [], "SELECT 1", intent="Q1"))
        graph.add_turn(_make_turn("t2", ["users"], [], "SELECT 2", intent="Q2"))
        # Query about orders → should return t1 (50%+ overlap)
        best = graph.find_best_parent(["orders"])
        assert best is not None
        assert best.turn_id == "t1"

    def test_find_best_parent_prefers_most_recent(self, graph):
        graph.add_turn(_make_turn("t1", ["orders"], [], "SELECT 1"))
        graph.add_turn(_make_turn("t2", ["orders"], [], "SELECT 2"))
        best = graph.find_best_parent(["orders"])
        assert best.turn_id == "t2"  # most recent with overlap

    def test_get_ancestors(self, graph):
        graph.add_turn(_make_turn("t1", ["orders"], [], "SELECT 1"))
        graph.add_turn(_make_turn("t2", ["orders"], [], "SELECT 2", parent_turn_id="t1"))
        graph.add_turn(_make_turn("t3", ["orders"], [], "SELECT 3", parent_turn_id="t2"))

        ancestors = graph.get_ancestors("t3")
        # Should return [t1, t2] (oldest first)
        assert [t.turn_id for t in ancestors] == ["t1", "t2"]

    def test_persistence(self, tmp_dir):
        g1 = ConversationGraph(kb_dir=tmp_dir)
        g1.add_turn(_make_turn("t1", ["orders"], ["year=2024"], "SELECT 1"))

        # Load fresh instance from same dir
        g2 = ConversationGraph(kb_dir=tmp_dir)
        t = g2.get_turn("t1")
        assert t is not None
        assert t.filters_used == ["year=2024"]
        assert t.tables_used == ["orders"]
