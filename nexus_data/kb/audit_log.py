"""
nexus_data/kb/audit_log.py — Structured Audit Logger (#10)
Writes every query turn to a JSONL file for compliance, debugging, and replay.

Each line is a JSON object:
{
  "ts": "2026-03-24T10:15:30.123",
  "session_id": "abc123",
  "query": "...",
  "sql": "...",
  "operation": "count",
  "rows": 42,
  "columns": ["col1", "col2"],
  "error": null,
  "from_cache": false,
  "confidence": 0.85,
  "anomaly_warnings": [],
  "execution_ms": 123.4
}
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class AuditLogger:
    def __init__(self, log_dir: Optional[Path] = None, enabled: bool = True):
        self._enabled = enabled
        self._dir = log_dir or Path("./nexus_kb")
        self._dir.mkdir(parents=True, exist_ok=True)
        self.session_id = uuid.uuid4().hex[:8]
        self._path = self._dir / f"audit_{datetime.now().strftime('%Y%m%d')}.jsonl"
        if enabled:
            logger.info("Audit log: %s (session=%s)", self._path, self.session_id)

    def log(
        self,
        query: str,
        sql: str,
        operation: str = "general",
        rows: int = 0,
        columns: list | None = None,
        error: Optional[str] = None,
        from_cache: bool = False,
        confidence: float = 1.0,
        anomaly_warnings: list | None = None,
        execution_ms: Optional[float] = None,
        phase_outputs: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._enabled:
            return
        record: Dict[str, Any] = {
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "session_id": self.session_id,
            "query": query,
            "sql": sql,
            "operation": operation,
            "rows": rows,
            "columns": columns or [],
            "error": error,
            "from_cache": from_cache,
            "confidence": confidence,
            "anomaly_warnings": anomaly_warnings or [],
            "execution_ms": execution_ms,
            "phase_outputs": phase_outputs or {},
        }
        try:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as exc:
            logger.warning("Audit log write failed: %s", exc)
