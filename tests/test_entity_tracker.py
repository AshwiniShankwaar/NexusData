"""tests/test_entity_tracker.py — unit tests for EntityTracker."""
import pytest
from nexus_data.kb.entity_tracker import EntityTracker


@pytest.fixture
def tracker():
    return EntityTracker()


def test_ingest_result_extracts_id_column(tracker):
    tracker.ingest_result(["user_id", "name", "amount"], [[1, "Alice", 100]])
    entities = tracker.as_dict()
    assert "user_id" in entities
    assert entities["user_id"] == 1


def test_ingest_result_extracts_name_column(tracker):
    tracker.ingest_result(["id", "product_name", "price"], [[5, "Laptop", 999]])
    entities = tracker.as_dict()
    assert "product_name" in entities
    assert entities["product_name"] == "Laptop"


def test_no_id_like_column_no_entities(tracker):
    tracker.ingest_result(["amount", "revenue", "cost"], [[100, 200, 50]])
    assert tracker.as_dict() == {}


def test_ingest_filters(tracker):
    tracker.ingest_filters(["status = 'active'", "tier = 'pro'"])
    entities = tracker.as_dict()
    assert entities.get("status") == "active"
    assert entities.get("tier") == "pro"


def test_has_pronoun_detected(tracker):
    assert tracker.has_pronoun("show me those users") is True
    assert tracker.has_pronoun("what about the same product") is True


def test_has_pronoun_not_detected(tracker):
    assert tracker.has_pronoun("show me all users") is False
    assert tracker.has_pronoun("count orders by status") is False


def test_resolve_context_empty(tracker):
    assert tracker.resolve_context() == ""


def test_resolve_context_nonempty(tracker):
    tracker.ingest_result(["user_id"], [[42]])
    ctx = tracker.resolve_context()
    assert "user_id" in ctx
    assert "42" in ctx


def test_clear(tracker):
    tracker.ingest_result(["id"], [[1]])
    tracker.clear()
    assert tracker.as_dict() == {}
    assert tracker.get_last_entities() == []
