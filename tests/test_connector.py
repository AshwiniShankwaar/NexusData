"""
tests/test_connector.py  ─  URI Integrity Test (Task 1.1.2)
Covers 15+ URI schemes and connection behaviour.
"""
from __future__ import annotations

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import ArgumentError

from nexus_data.librarian.connector import (
    ConnectionValidator,
    URIValidator,
    SUPPORTED_SCHEMES,
)


# ── URIValidator ──────────────────────────────────────────────────────────────

class TestURIValidator:
    """URI Integrity Test: must handle 15+ schemes."""

    VALID_URIS = [
        "sqlite:///test.db",
        "sqlite:///:memory:",
        "postgresql://user:pass@host/db",
        "postgresql+psycopg2://user:pass@host/db",
        "postgresql+asyncpg://user:pass@host/db",
        "postgresql+pg8000://user:pass@host/db",
        "mysql://user:pass@host/db",
        "mysql+pymysql://user:pass@host/db",
        "mysql+mysqlconnector://user:pass@host/db",
        "mssql+pyodbc://user:pass@host/db",
        "mssql+pymssql://user:pass@host/db",
        "oracle://user:pass@host/db",
        "oracle+cx_oracle://user:pass@host/db",
        "duckdb:///data.duckdb",
        "bigquery://project/dataset",
        "hive://user@host:10000/db",
    ]

    def test_supported_scheme_count(self):
        """Verify at least 15 schemes in the registry."""
        assert len(SUPPORTED_SCHEMES) >= 15

    @pytest.mark.parametrize("uri", VALID_URIS)
    def test_valid_uris_accepted(self, uri):
        scheme = URIValidator.validate(uri)
        assert isinstance(scheme, str)
        assert len(scheme) > 0

    def test_invalid_scheme_rejected(self):
        with pytest.raises(ValueError, match="Unsupported scheme"):
            URIValidator.validate("mongodbsrv://user:pass@host/db")

    def test_malformed_uri_rejected(self):
        with pytest.raises(ValueError, match="Malformed URI"):
            URIValidator.validate("not_a_uri_at_all")

    def test_empty_uri_rejected(self):
        with pytest.raises((ValueError, AttributeError)):
            URIValidator.validate("")


# ── ConnectionValidator ────────────────────────────────────────────────────────

class TestConnectionValidator:

    def test_sqlite_memory_returns_engine(self):
        v = ConnectionValidator(timeout_seconds=2, max_retries=1)
        engine = v.get_engine("sqlite:///:memory:")
        assert isinstance(engine, Engine)
        assert engine.dialect.name == "sqlite"

    def test_sqlite_file_uri(self, tmp_path):
        db_file = tmp_path / "test.db"
        v = ConnectionValidator(timeout_seconds=2, max_retries=1)
        engine = v.get_engine(f"sqlite:///{db_file}")
        assert isinstance(engine, Engine)

    def test_invalid_scheme_raises_value_error(self):
        v = ConnectionValidator(timeout_seconds=1, max_retries=1)
        with pytest.raises(ValueError):
            v.get_engine("notreal://user:pass@host/db")

    def test_unreachable_host_exhausts_retries(self, monkeypatch):
        pytest.importorskip("psycopg2", reason="psycopg2 not installed — skipping live connection test")
        monkeypatch.setattr("time.sleep", lambda _: None)
        v = ConnectionValidator(timeout_seconds=1, max_retries=2)
        with pytest.raises(ValueError, match="Failed URI Integrity Check after 2 attempts"):
            v.get_engine("postgresql://user:pass@127.0.0.1:19999/nonexistent")
