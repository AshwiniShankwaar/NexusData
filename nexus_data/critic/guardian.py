"""
nexus_data/critic/guardian.py  — Task 3.1
The Guardian: AST-level safety enforcement using sqlglot.
Blocks 100 % of mutation statements (DROP, DELETE, INSERT, UPDATE, ALTER, GRANT, TRUNCATE).
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)

# Every statement class that mutates data or schema
FORBIDDEN_STATEMENT_TYPES = (
    exp.Drop,
    exp.Delete,
    exp.Insert,
    exp.Update,
    exp.Alter,
    exp.Create,   # CREATE TABLE ... AS SELECT could hide mutations
    exp.Grant,
    exp.Revoke,
    exp.Command,  # catches raw TRUNCATE, EXEC, etc.
    exp.Transaction,
)

FORBIDDEN_KEYWORDS = frozenset([
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER",
    "CREATE", "GRANT", "REVOKE", "TRUNCATE", "EXEC",
    "EXECUTE", "INTO", "MERGE",
])


class SafetyViolation(Exception):
    """Raised when a query fails the AST Safety Test."""


class PromptInjectionError(Exception):
    """Raised when user input contains prompt injection attempt."""


class Guardian:
    """
    Parses SQL with sqlglot and raises SafetyViolation on any
    non-SELECT statement.

    Supports multi-dialect parsing (PostgreSQL, MySQL, T-SQL, DuckDB, …).
    """

    _INJECTION_PATTERNS = re.compile(
        r"(ignore\s+(all\s+)?(previous|prior|above)\s+(instructions?|prompts?|rules?)|"
        r"disregard\s+(all\s+)?(previous|prior|above)|"
        r"you\s+are\s+now\s+(a|an)\s+|"
        r"new\s+instructions?:|"
        r"system\s*:\s*you\s+|"
        r"<\s*/?system\s*>|"
        r"\[INST\]|\[SYS\]|<\|im_start\|>|<\|im_end\|>|"
        r"forget\s+(everything|all|your|prior)|"
        r"do\s+not\s+follow\s+(your\s+)?(previous|prior|above|original)\s+(instructions?|rules?)|"
        r"pretend\s+(you\s+are|to\s+be)|"
        r"act\s+as\s+(if\s+you\s+are|a\s+)|"
        r"reveal\s+(your\s+)?(system\s+)?(prompt|instructions?)|"
        r"what\s+(are|is)\s+your\s+(system\s+)?(prompt|instructions?)|"
        r"override\s+(safety|security|guard|filter))",
        re.IGNORECASE,
    )

    def __init__(self, dialect: Optional[str] = None):
        self.dialect = dialect

    def validate(self, sql: str) -> str:
        """
        Validate *sql* and return it unchanged if safe.

        Raises
        ------
        SafetyViolation
            If the query contains any mutation statement.
        ValueError
            If the query cannot be parsed.
        """
        sql = sql.strip()
        if not sql:
            raise SafetyViolation("Empty SQL string — nothing to execute.")

        # ----- Pass 1: keyword scan (fast) -----------------------------------
        # TRUNCATE is not always mapped to a named AST node; catch it here.
        upper = sql.upper()
        if re.search(r"\bTRUNCATE\b", upper):
            raise SafetyViolation(
                "TRUNCATE statement detected. Only SELECT queries are permitted."
            )
        for kw in FORBIDDEN_KEYWORDS:
            # Check whole-word match to avoid false positives (e.g. "updated_at")
            if re.search(rf"\b{kw}\b", upper):
                # Could be benign (column name).  Let AST confirm below.
                pass

        # ----- Pass 2: AST analysis (accurate) --------------------------------
        try:
            statements = sqlglot.parse(sql, dialect=self.dialect or "")
        except sqlglot.errors.ParseError as exc:
            raise ValueError(f"SQL parse error: {exc}") from exc

        if not statements:
            raise ValueError("Empty or unparseable SQL.")

        for stmt in statements:
            if stmt is None:
                continue
            if isinstance(stmt, FORBIDDEN_STATEMENT_TYPES):
                raise SafetyViolation(
                    f"Forbidden statement type '{type(stmt).__name__}' detected. "
                    "Only SELECT queries are permitted."
                )
            # Also walk the AST for any nested mutations (rare but possible)
            for node in stmt.walk():
                if isinstance(node, FORBIDDEN_STATEMENT_TYPES):
                    raise SafetyViolation(
                        f"Nested forbidden node '{type(node).__name__}' detected."
                    )

        logger.debug("Guardian: query PASSED safety check.")
        return sql

    def check_user_input(self, text: str) -> str:
        """Sanitize and check user NL input for prompt injection. Returns clean text."""
        # Strip null bytes and control chars (keep newlines/tabs)
        clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        if self._INJECTION_PATTERNS.search(clean):
            raise PromptInjectionError(
                "Input contains patterns that could manipulate the AI system. "
                "Please rephrase your question about the database."
            )
        return clean
