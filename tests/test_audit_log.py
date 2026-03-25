"""tests/test_audit_log.py — unit tests for AuditLogger."""
import json
import pytest
from nexus_data.kb.audit_log import AuditLogger


@pytest.fixture
def logger(tmp_path):
    return AuditLogger(log_dir=tmp_path, enabled=True)


def _read_lines(logger):
    lines = logger._path.read_text(encoding="utf-8").splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def test_log_writes_entry(logger):
    logger.log("show users", "SELECT * FROM users", rows=5)
    records = _read_lines(logger)
    assert len(records) == 1
    assert records[0]["query"] == "show users"
    assert records[0]["sql"] == "SELECT * FROM users"
    assert records[0]["rows"] == 5


def test_log_fields_present(logger):
    logger.log("count orders", "SELECT COUNT(*) FROM orders",
               operation="count", rows=1, columns=["count(*)"],
               from_cache=True, confidence=0.97, execution_ms=12.3)
    rec = _read_lines(logger)[0]
    assert rec["operation"] == "count"
    assert rec["from_cache"] is True
    assert rec["confidence"] == 0.97
    assert rec["execution_ms"] == pytest.approx(12.3)
    assert "count(*)" in rec["columns"]


def test_log_error_field(logger):
    logger.log("bad query", "", error="syntax error")
    rec = _read_lines(logger)[0]
    assert rec["error"] == "syntax error"


def test_log_disabled_writes_nothing(tmp_path):
    al = AuditLogger(log_dir=tmp_path, enabled=False)
    al.log("ignored", "SELECT 1")
    assert not al._path.exists()


def test_multiple_entries_appended(logger):
    logger.log("q1", "SELECT 1")
    logger.log("q2", "SELECT 2")
    logger.log("q3", "SELECT 3")
    records = _read_lines(logger)
    assert len(records) == 3
    assert records[1]["query"] == "q2"


def test_session_id_consistent(logger):
    logger.log("q1", "SELECT 1")
    logger.log("q2", "SELECT 2")
    records = _read_lines(logger)
    assert records[0]["session_id"] == records[1]["session_id"]
    assert records[0]["session_id"] == logger.session_id


def test_anomaly_warnings_stored(logger):
    logger.log("big query", "SELECT * FROM t",
               anomaly_warnings=["0 rows returned", "no LIMIT"])
    rec = _read_lines(logger)[0]
    assert len(rec["anomaly_warnings"]) == 2


def test_timestamp_present(logger):
    logger.log("q", "SELECT 1")
    rec = _read_lines(logger)[0]
    assert "ts" in rec
    assert "T" in rec["ts"]  # ISO format
