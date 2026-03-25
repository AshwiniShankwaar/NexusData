"""
tests/test_guardian.py  ─  AST Safety Test (Task 3.1)
Must block 100 % of mutation statements.
"""
from __future__ import annotations

import pytest

from nexus_data.critic.guardian import Guardian, SafetyViolation


@pytest.fixture
def guardian():
    return Guardian(dialect="sqlite")


SAFE_QUERIES = [
    "SELECT * FROM customers",
    "SELECT id, name FROM orders WHERE amount > 100",
    "SELECT c.name, COUNT(o.id) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name",
    "SELECT tier, AVG(amount) FROM customers JOIN orders ON customers.id = orders.customer_id GROUP BY tier",
    "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte LIMIT 10",
]

FORBIDDEN_QUERIES = [
    "DROP TABLE customers",
    "DELETE FROM orders WHERE id = 1",
    "INSERT INTO customers(name) VALUES('hacker')",
    "UPDATE customers SET tier='enterprise' WHERE id=1",
    "ALTER TABLE customers ADD COLUMN secret TEXT",
    "GRANT ALL PRIVILEGES ON customers TO hacker",
    "TRUNCATE TABLE orders",
    # Disguised attacks
    "SELECT * FROM customers; DROP TABLE customers",
]


class TestGuardian:

    @pytest.mark.parametrize("sql", SAFE_QUERIES)
    def test_safe_queries_pass(self, guardian, sql):
        result = guardian.validate(sql)
        assert result == sql

    @pytest.mark.parametrize("sql", FORBIDDEN_QUERIES)
    def test_forbidden_queries_blocked(self, guardian, sql):
        with pytest.raises((SafetyViolation, ValueError)):
            guardian.validate(sql)

    def test_empty_sql_raises(self, guardian):
        with pytest.raises((ValueError, Exception)):
            guardian.validate("")

    def test_returns_original_sql_unchanged(self, guardian):
        sql = "SELECT 1 AS test"
        assert guardian.validate(sql) == sql
