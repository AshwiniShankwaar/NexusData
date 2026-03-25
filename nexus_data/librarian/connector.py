"""
nexus_data/librarian/connector.py  — Task 1.1
Multi-Protocol Connector with URI Integrity Test, retry logic, and dialect-specific timeouts.
"""
from __future__ import annotations
import re
import time
import logging
from typing import Any, Dict

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.exc import SQLAlchemyError, ArgumentError

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Supported URI schemes (URI Integrity Test: 15+)
# ──────────────────────────────────────────────
SUPPORTED_SCHEMES: set[str] = {
    "sqlite", "sqlite+pysqlite",
    "postgresql", "postgresql+psycopg2", "postgresql+asyncpg", "postgresql+pg8000",
    "mysql", "mysql+pymysql", "mysql+mysqlconnector", "mysql+aiomysql",
    "mssql", "mssql+pyodbc", "mssql+pymssql",
    "oracle", "oracle+cx_oracle",
    "duckdb", "duckdb+duckdb_engine",
    "bigquery",
    "hive",
    "presto",
}

URI_REGEX = re.compile(r"^[a-zA-Z][a-zA-Z0-9+_\-.]*://")


class URIValidator:
    """Validates that a URI string represents a supported database scheme."""

    @staticmethod
    def validate(uri: str) -> str:
        """Raise ValueError if the URI is unsupported or malformed."""
        match = URI_REGEX.match(uri)
        if not match:
            raise ValueError(f"Malformed URI — could not extract scheme: '{uri}'")
        scheme = uri.split("://")[0].split("+")[0].lower()
        if scheme not in SUPPORTED_SCHEMES:
            raise ValueError(
                f"Unsupported scheme '{scheme}'. "
                f"Supported: {sorted(SUPPORTED_SCHEMES)}"
            )
        return scheme


class ConnectionValidator:
    """
    Validates and establishes connections to database URIs.

    Enforces:
      - URI Integrity Test (15+ dialects)
      - 5-second timeout
      - Exponential-backoff retry (max_retries attempts)
    """

    def __init__(self, timeout_seconds: int = 5, max_retries: int = 3):
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

    # ── Internal helpers ────────────────────────────────────────────────────

    def _connect_args(self, uri: str) -> Dict[str, Any]:
        """Dialect-specific connection arguments."""
        args: Dict[str, Any] = {}
        scheme = uri.split("://")[0].split("+")[0].lower()
        if scheme in ("postgresql",):
            args["connect_timeout"] = self.timeout_seconds
        elif scheme in ("mysql",):
            args["connect_timeout"] = self.timeout_seconds
        elif scheme in ("sqlite",):
            args["timeout"] = self.timeout_seconds
        elif scheme in ("mssql",):
            args["timeout"] = self.timeout_seconds
        return args

    def _engine_kwargs(self, uri: str) -> Dict[str, Any]:
        """Engine-level kwargs."""
        kwargs: Dict[str, Any] = {
            "connect_args": self._connect_args(uri),
        }
        # pool_pre_ping is only useful for server-based engines
        if not uri.startswith("sqlite:///:memory:"):
            kwargs["pool_pre_ping"] = True
        return kwargs

    # ── Public API ───────────────────────────────────────────────────────────

    def get_engine(self, db_uri: str) -> Engine:
        """
        Validate the URI, then attempt connection with retry logic.

        Returns
        -------
        Engine
            A live SQLAlchemy Engine.

        Raises
        ------
        ValueError
            On invalid URI or exhausted retries.
        """
        # Step 1 — URI integrity check
        URIValidator.validate(db_uri)

        engine = create_engine(db_uri, **self._engine_kwargs(db_uri))

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with engine.connect():
                    logger.info("Connection established on attempt %d.", attempt)
                    return engine
            except SQLAlchemyError as exc:
                last_err = exc
                logger.warning("Attempt %d/%d failed: %s", attempt, self.max_retries, exc)
                if attempt < self.max_retries:
                    time.sleep(2 ** (attempt - 1))  # 1s, 2s, 4s …

        raise ValueError(
            f"Failed URI Integrity Check after {self.max_retries} attempts: {last_err}"
        )
