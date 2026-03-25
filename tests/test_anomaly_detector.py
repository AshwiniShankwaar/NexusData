"""tests/test_anomaly_detector.py — unit tests for AnomalyDetector."""
import pytest
from nexus_data.critic.anomaly_detector import detect_anomalies


def test_no_anomaly_normal_result():
    sql = "SELECT id, name FROM users WHERE id = 1"
    rows = [["1", "Alice"]]
    assert detect_anomalies(sql, rows, ["id", "name"], "select") == []


def test_zero_rows_select_triggers_warning():
    sql = "SELECT * FROM orders WHERE status = 'nonexistent'"
    warnings = detect_anomalies(sql, [], ["id", "amount"], "select")
    assert len(warnings) == 1
    assert "0 rows" in warnings[0].lower()


def test_zero_rows_count_no_warning():
    # COUNT queries legitimately return 0
    sql = "SELECT COUNT(*) FROM orders WHERE amount > 1000000"
    warnings = detect_anomalies(sql, [[0]], ["count(*)"], "count")
    assert warnings == []


def test_large_result_no_limit_triggers_warning():
    sql = "SELECT * FROM big_table"
    rows = [[i] for i in range(200_001)]
    warnings = detect_anomalies(sql, rows, ["id"], "select")
    assert any("limit" in w.lower() or "large" in w.lower() for w in warnings)


def test_join_without_where_many_rows():
    sql = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id"
    rows = [[i] for i in range(15_000)]
    warnings = detect_anomalies(sql, rows, ["id"], "select")
    assert any("join" in w.lower() or "cartesian" in w.lower() for w in warnings)


def test_null_aggregate_triggers_warning():
    sql = "SELECT AVG(amount) FROM orders"
    warnings = detect_anomalies(sql, [[None]], ["avg(amount)"], "average")
    assert any("null" in w.lower() for w in warnings)


def test_select_star_no_limit_many_rows():
    sql = "SELECT * FROM products"
    rows = [[i] for i in range(600)]
    warnings = detect_anomalies(sql, rows, ["id"], "select")
    assert any("select *" in w.lower() or "limit" in w.lower() for w in warnings)
