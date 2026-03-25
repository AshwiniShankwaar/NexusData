"""tests/test_performance_advisor.py — unit tests for PerformanceAdvisor."""
import pytest
from nexus_data.critic.performance_advisor import advise


def test_clean_query_no_hints():
    sql = "SELECT id, name FROM users WHERE id = 1 LIMIT 10"
    assert advise(sql) == []


def test_select_star_hint():
    sql = "SELECT * FROM orders WHERE status = 'paid'"
    hints = advise(sql)
    assert any("select *" in h.lower() for h in hints)


def test_no_limit_hint():
    sql = "SELECT id, name FROM users WHERE active = 1"
    hints = advise(sql)
    assert any("limit" in h.lower() for h in hints)


def test_aggregate_no_limit_ok():
    # Aggregates don't need LIMIT
    sql = "SELECT COUNT(*) FROM orders"
    hints = advise(sql)
    assert not any("limit" in h.lower() for h in hints)


def test_leading_wildcard_like():
    sql = "SELECT * FROM products WHERE name LIKE '%phone%'"
    hints = advise(sql)
    assert any("leading wildcard" in h.lower() or "like" in h.lower() for h in hints)


def test_or_in_where_hint():
    sql = "SELECT id FROM users WHERE status = 'active' OR type = 'admin' LIMIT 100"
    hints = advise(sql)
    assert any("or" in h.lower() for h in hints)


def test_indexed_column_no_warning():
    sql = "SELECT id, name FROM users WHERE id = 5 ORDER BY id LIMIT 10"
    hints = advise(sql, indexed_columns=["id", "name"])
    # id is indexed — no index warning expected
    assert not any("non-indexed" in h.lower() for h in hints)


def test_non_indexed_where_warning():
    sql = "SELECT id, notes FROM users WHERE notes = 'something' LIMIT 10"
    hints = advise(sql, indexed_columns=["id"])
    assert any("non-indexed" in h.lower() for h in hints)
