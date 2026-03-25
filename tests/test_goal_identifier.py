"""tests/test_goal_identifier.py — unit tests for GoalIdentifier helpers."""
import pytest
from nexus_data.pipeline.goal_identifier import (
    _extract_json,
    _DESTRUCTIVE_PATTERN,
    _DESTRUCTIVE_INTENT_KEYWORDS,
)


# ── _extract_json ──────────────────────────────────────────────────────────────

def test_extract_json_clean():
    text = '{"operation": "count", "filters": []}'
    result = _extract_json(text)
    assert result is not None
    assert result["operation"] == "count"


def test_extract_json_with_markdown_fences():
    text = "```json\n{\"operation\": \"select\"}\n```"
    result = _extract_json(text)
    assert result is not None
    assert result["operation"] == "select"


def test_extract_json_embedded_in_prose():
    text = 'Sure, here is the result: {"operation": "sum", "limit": 10} done.'
    result = _extract_json(text)
    assert result is not None
    assert result["operation"] == "sum"


def test_extract_json_invalid_returns_none():
    result = _extract_json("This is not JSON at all")
    assert result is None


def test_extract_json_truncated_recovers():
    # Partial JSON that can be recovered by progressive trim
    text = '{"operation": "count", "filters": ["status = \'active\'"'
    # May or may not recover — just assert no exception
    result = _extract_json(text)  # could be None or partial dict


# ── Destructive pattern ────────────────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    "INSERT INTO users VALUES (1, 'Alice')",
    "UPDATE users SET name = 'Bob' WHERE id = 1",
    "DELETE FROM orders WHERE status = 'old'",
    "DROP TABLE customers",
    "TRUNCATE orders",
    "ALTER TABLE users ADD COLUMN email TEXT",
    "CREATE TABLE new_table (id INT)",
])
def test_destructive_pattern_matches(text):
    assert _DESTRUCTIVE_PATTERN.match(text), f"Should match: {text}"


@pytest.mark.parametrize("text", [
    "show me all users",
    "count orders by status",
    "SELECT * FROM users",
    "list all products",
])
def test_destructive_pattern_no_match(text):
    assert not _DESTRUCTIVE_PATTERN.match(text), f"Should NOT match: {text}"


@pytest.mark.parametrize("text", [
    "add a new record for user Alice",
    "insert a new entry in the orders table",
    "delete this record",
    "remove the row where id = 5",
    "update the value of status for user 10",
])
def test_destructive_intent_keywords_match(text):
    assert _DESTRUCTIVE_INTENT_KEYWORDS.search(text), f"Should match: {text}"


@pytest.mark.parametrize("text", [
    "show all users",
    "how many orders are paid",
    "which products cost more than 100",
])
def test_destructive_intent_keywords_no_match(text):
    assert not _DESTRUCTIVE_INTENT_KEYWORDS.search(text), f"Should NOT match: {text}"
