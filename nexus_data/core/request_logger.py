"""
nexus_data/core/request_logger.py
Structured per-request logger for CLI and UI sessions.

Writes JSON-line records to  logs/nexus_YYYY-MM-DD.log  (at the project root).
Each record captures: source tag, query, SQL generated, rows returned,
phase timings, and any error — giving a full audit trail of every interaction.

Daily rotation is automatic (new file per UTC date).
Weekly archiving (files older than 7 days) is triggered automatically on write.
Archives land in  logs/archive/week_YYYY-WNN.zip
"""
from __future__ import annotations

import json
import logging
import os
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

# Default: logs/ at the same level as nexus_cli.py (project root).
# Override with NEXUS_LOG_DIR env var if needed.
_LOG_DIR = Path(os.getenv("NEXUS_LOG_DIR", "./logs"))
_ARCHIVE_DIR = _LOG_DIR / "archive"
_logger = logging.getLogger("nexus.requests")

# How many days to keep raw log files before archiving
_ARCHIVE_AFTER_DAYS = 7


def _archive_old_logs() -> None:
    """
    Move log files older than _ARCHIVE_AFTER_DAYS into weekly zip archives.
    Called automatically on every write — cheap because Path.glob is fast
    and we only act when old files exist.
    """
    try:
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=_ARCHIVE_AFTER_DAYS)
        old_files = [
            p for p in _LOG_DIR.glob("nexus_*.log")
            if _parse_log_date(p.stem) and _parse_log_date(p.stem) < cutoff
        ]
        if not old_files:
            return

        _ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        # Group files by ISO calendar week
        by_week: Dict[str, list] = {}
        for f in old_files:
            d = _parse_log_date(f.stem)
            if d:
                week_key = f"week_{d.strftime('%Y-W%V')}"
                by_week.setdefault(week_key, []).append(f)

        for week_key, files in by_week.items():
            zip_path = _ARCHIVE_DIR / f"{week_key}.zip"
            mode = "a" if zip_path.exists() else "w"
            with zipfile.ZipFile(zip_path, mode, compression=zipfile.ZIP_DEFLATED) as zf:
                for f in files:
                    zf.write(f, arcname=f.name)
            # Remove originals only after successful zip
            for f in files:
                f.unlink(missing_ok=True)

        _logger.debug("Archived %d log file(s) to %s", sum(len(v) for v in by_week.values()), _ARCHIVE_DIR)
    except Exception:
        pass  # never let archiving break a request


def _parse_log_date(stem: str):
    """Parse date from 'nexus_YYYY-MM-DD' stem.  Returns date or None."""
    try:
        return datetime.strptime(stem, "nexus_%Y-%m-%d").date()
    except ValueError:
        return None


def log_pipeline_request(
    *,
    source: str,                              # "CLI" or "UI"
    query: str,
    result_sql: Optional[str] = None,
    result_rows: int = 0,
    result_error: Optional[str] = None,
    is_clarification: bool = False,
    confidence: float = 1.0,
    phase_timings: Optional[Dict[str, float]] = None,
    user_id: Optional[str] = None,
    conv_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Write one JSON-line record to the daily log file.
    Triggers weekly archiving of old log files.
    Never raises — logging failures must not crash the pipeline.
    """
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = _LOG_DIR / f"nexus_{date_str}.log"

        record: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "user_id": user_id,
            "conv_id": conv_id,
            "query": query,
            "sql": result_sql,
            "rows_returned": result_rows,
            "error": result_error,
            "is_clarification": is_clarification,
            "confidence": round(confidence, 3),
            "phase_ms": {k: round(v, 1) for k, v in (phase_timings or {}).items()},
        }
        if extra:
            record.update(extra)

        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

        # Also emit a compact human-readable INFO line
        timings_str = " | ".join(
            f"{k}={v:.0f}ms" for k, v in (phase_timings or {}).items()
        )
        _logger.info(
            "[%s] query=%r  rows=%d  error=%s  %s",
            source,
            query[:100],
            result_rows,
            result_error or "-",
            timings_str,
        )

        # Opportunistically archive old files (no-op if nothing to archive)
        _archive_old_logs()

    except Exception:
        pass  # never let logging break a request
