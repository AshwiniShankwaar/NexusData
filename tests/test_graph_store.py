"""tests/test_graph_store.py — unit tests for SQLGraphStore."""
import pytest
from nexus_data.kb.graph_store import SQLGraphStore, _parse_with_regex


# ── _parse_with_regex ──────────────────────────────────────────────────────────

def test_regex_parser_extracts_tables():
    sql = "SELECT id, name FROM users WHERE id = 1"
    g = _parse_with_regex(sql)
    assert "users" in g["tables"]


def test_regex_parser_join_tables():
    sql = "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.customer_id"
    g = _parse_with_regex(sql)
    assert "users" in g["tables"]
    assert "orders" in g["tables"]


def test_regex_parser_aggregation_detected():
    sql = "SELECT COUNT(*) FROM orders WHERE status = 'paid'"
    g = _parse_with_regex(sql)
    assert "COUNT" in g["aggregations"]


def test_regex_parser_where_condition():
    sql = "SELECT * FROM users WHERE tier = 'pro' LIMIT 10"
    g = _parse_with_regex(sql)
    assert len(g["conditions"]) > 0


# ── SQLGraphStore ──────────────────────────────────────────────────────────────

@pytest.fixture
def store(tmp_path):
    return SQLGraphStore(kb_dir=tmp_path)


def test_save_pattern_stores_select(store):
    store.save_pattern("count paid orders", "SELECT COUNT(*) FROM orders WHERE status = 'paid'")
    assert len(store._patterns) == 1


def test_save_pattern_ignores_non_select(store):
    store.save_pattern("bad intent", "INSERT INTO users VALUES (1)")
    assert len(store._patterns) == 0


def test_save_pattern_deduplicates_intent(store):
    store.save_pattern("same intent", "SELECT 1 FROM users")
    store.save_pattern("same intent", "SELECT 2 FROM users")
    assert len(store._patterns) == 1


def test_find_template_returns_sql(store):
    store.save_pattern("users query", "SELECT id, name FROM users WHERE tier = 'pro' LIMIT 10")
    result = store.find_template("show pro users", ["users"])
    assert result is not None
    assert "users" in result.lower()


def test_find_template_no_overlap_returns_none(store):
    store.save_pattern("orders query", "SELECT id FROM orders LIMIT 10")
    result = store.find_template("show products", ["products"])
    assert result is None


def test_find_template_empty_store_returns_none(store):
    result = store.find_template("anything", ["users"])
    assert result is None


def test_get_full_graph_nodes(store):
    store.save_pattern("q1", "SELECT id FROM users LIMIT 10")
    graph = store.get_full_graph()
    assert "nodes" in graph
    assert "edges" in graph
    assert graph["pattern_count"] == 1
    assert any(n["id"] == "users" for n in graph["nodes"])


def test_persistence_across_instances(tmp_path):
    s1 = SQLGraphStore(kb_dir=tmp_path)
    s1.save_pattern("find users", "SELECT id, name FROM users LIMIT 5")
    s2 = SQLGraphStore(kb_dir=tmp_path)
    assert len(s2._patterns) == 1


def test_export_graph_json(store, tmp_path):
    store.save_pattern("q", "SELECT id FROM users LIMIT 10")
    out = store.export_graph_json(tmp_path / "export.json")
    import json
    data = json.loads(open(out).read())
    assert "nodes" in data
    assert "pattern_count" in data
