"""
tests/test_id_only_select.py
Tests that the structural validator catches SELECT lists consisting solely of
id-like columns — covering the "get unique users → SELECT DISTINCT id" bug.
"""
from __future__ import annotations

import pytest
from nexus_data.critic.pre_validator import SQLPreValidator


def _check(sql: str) -> str | None:
    """Convenience wrapper — returns the issue string or None."""
    return SQLPreValidator._check_id_only_select(sql.upper())


# ── Should flag (only id columns) ─────────────────────────────────────────────

class TestIdOnlyFlagged:
    def test_bare_id(self):
        assert _check("SELECT id FROM users") is not None

    def test_distinct_id(self):
        assert _check("SELECT DISTINCT id FROM users") is not None

    def test_table_aliased_id(self):
        assert _check("SELECT u.id FROM users u") is not None

    def test_user_id_foreign_key(self):
        assert _check("SELECT user_id FROM orders") is not None

    def test_product_id_foreign_key(self):
        assert _check("SELECT product_id FROM order_items") is not None

    def test_multiple_id_columns_only(self):
        assert _check("SELECT user_id, product_id FROM order_items") is not None

    def test_id_with_alias(self):
        assert _check("SELECT id AS user_id FROM users") is not None

    def test_distinct_multiple_ids(self):
        assert _check("SELECT DISTINCT user_id, order_id FROM order_items") is not None


# ── Should NOT flag (has descriptive columns or aggregation) ──────────────────

class TestIdOnlyNotFlagged:
    def test_id_plus_name(self):
        assert _check("SELECT id, name FROM users") is None

    def test_name_only(self):
        assert _check("SELECT name, email FROM users") is None

    def test_star(self):
        assert _check("SELECT * FROM users") is None

    def test_count_distinct_id(self):
        # Aggregation — caller guards this but regex should still pass
        assert _check("SELECT COUNT(DISTINCT id) FROM users") is None

    def test_count_star(self):
        assert _check("SELECT COUNT(*) FROM users") is None

    def test_sum_column(self):
        assert _check("SELECT SUM(price) FROM products") is None

    def test_name_email_status(self):
        assert _check("SELECT name, email, status FROM users") is None

    def test_id_plus_status(self):
        assert _check("SELECT id, status FROM orders") is None

    def test_no_from(self):
        # Malformed SQL — should not raise, just return None
        assert _check("SELECT 1") is None

    def test_cte_final_select_with_name(self):
        sql = (
            "WITH u AS (SELECT id, name FROM users) "
            "SELECT DISTINCT name FROM u"
        )
        # The outermost SELECT has 'name' — should NOT flag
        assert _check(sql) is None


# ── Integration: structural_check triggers LLM rewrite pass ──────────────────

class TestStructuralCheckIntegration:
    """Verify _structural_check returns an issue for id-only SELECT."""

    def _make_validator(self):
        from unittest.mock import MagicMock
        llm = MagicMock()
        db_info = "## Table: users\n- id (INTEGER)\n- name (TEXT)\n- email (TEXT)\n"
        return SQLPreValidator(llm, db_info)

    def test_id_only_raises_structural_issue(self):
        v = self._make_validator()
        goal = {"relevant_tables": ["users"], "operation": "select", "filters": []}
        issues = v._structural_check("SELECT DISTINCT id FROM users", goal)
        assert any("ID column" in i for i in issues), f"Expected ID issue, got: {issues}"

    def test_name_email_no_structural_issue_for_id(self):
        v = self._make_validator()
        goal = {"relevant_tables": ["users"], "operation": "select", "filters": []}
        issues = v._structural_check("SELECT name, email FROM users", goal)
        assert not any("ID column" in i for i in issues)

    def test_count_id_not_flagged(self):
        v = self._make_validator()
        goal = {"relevant_tables": ["users"], "operation": "count", "filters": []}
        # COUNT queries skip the id-only check entirely (guarded by COUNT( in sql_upper)
        issues = v._structural_check("SELECT COUNT(id) FROM users", goal)
        assert not any("ID column" in i for i in issues)
