"""
nexus_data/core/reset.py — Factory Reset Utility

Three levels:
  soft    — clears conversation history and semantic cache (current DB only)
  full    — soft + all KB knowledge files for the current DB
  factory — full + auth DB history + all KB dirs + logs (complete wipe)

None of the levels delete user accounts, config.json, or .env.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Report helpers ─────────────────────────────────────────────────────────────

def _rm_file(path: Path, report: List[str]) -> None:
    """Delete a single file; record outcome."""
    try:
        if path.exists():
            path.unlink()
            report.append(f"  ✓ Deleted  {path}")
        else:
            report.append(f"  - Skipped  {path}  (not found)")
    except OSError as exc:
        report.append(f"  ✗ Failed   {path}  ({exc})")


def _rm_dir(path: Path, report: List[str]) -> None:
    """Recursively delete a directory; record outcome."""
    try:
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            report.append(f"  ✓ Deleted  {path}/")
        else:
            report.append(f"  - Skipped  {path}/  (not found)")
    except OSError as exc:
        report.append(f"  ✗ Failed   {path}/  ({exc})")


def _rm_glob(directory: Path, pattern: str, report: List[str]) -> None:
    """Delete all files matching a glob pattern inside a directory."""
    if not directory.exists():
        return
    matches = list(directory.glob(pattern))
    if not matches:
        report.append(f"  - Skipped  {directory}/{pattern}  (no matches)")
        return
    for p in matches:
        _rm_file(p, report)


# ── Reset levels ───────────────────────────────────────────────────────────────

def soft_reset(kb_dir: Path) -> Tuple[bool, str]:
    """
    Clears conversation history and semantic vector cache for one database.
    Preserves: schema (db_info.md), long-term memory, query patterns, bookmarks.

    Returns (success, report_text).
    """
    report: List[str] = ["[Soft Reset — clearing conversation & cache]"]

    _rm_file(kb_dir / "conversation_lineage.json", report)
    _rm_file(kb_dir / "shortterm_memory.md", report)
    _rm_dir(kb_dir / "vector_store", report)
    _rm_file(kb_dir / "cache_stats.json", report)

    # Recreate empty shortterm_memory.md so KBManager doesn't error on next read
    try:
        short_mem = kb_dir / "shortterm_memory.md"
        if not short_mem.exists():
            short_mem.write_text("", encoding="utf-8")
    except OSError:
        pass

    report.append("\nConversation history and semantic cache cleared.")
    return True, "\n".join(report)


def full_reset(kb_dir: Path) -> Tuple[bool, str]:
    """
    Clears the entire knowledge base for one database.
    Preserves: all other DB knowledge bases, auth accounts, config, logs.

    Returns (success, report_text).
    """
    report: List[str] = ["[Full Reset — clearing complete KB for this database]"]

    # Session state
    _rm_file(kb_dir / "conversation_lineage.json", report)
    _rm_file(kb_dir / "shortterm_memory.md", report)
    _rm_dir(kb_dir / "vector_store", report)
    _rm_file(kb_dir / "cache_stats.json", report)

    # Learned knowledge
    _rm_file(kb_dir / "longterm_memory.md", report)
    _rm_file(kb_dir / "db_info.md", report)
    _rm_file(kb_dir / "query_graph.json", report)
    _rm_file(kb_dir / "bookmarks.json", report)

    # Audit logs for this database
    _rm_glob(kb_dir, "audit_*.jsonl", report)

    report.append(
        "\nAll KB files cleared. The schema will be re-introspected on the next connection."
    )
    return True, "\n".join(report)


def factory_reset(kb_root: Path, auth_db_path: Optional[Path] = None,
                  logs_dir: Optional[Path] = None) -> Tuple[bool, str]:
    """
    Complete application wipe:
      • Deletes the entire nexus_kb/ directory tree (all databases)
      • Clears auth DB conversation/session/feedback history (keeps user accounts)
      • Clears log files

    Does NOT delete: user accounts, config.json, .env, sample databases.

    Returns (success, report_text).
    """
    report: List[str] = ["[Factory Reset — wiping all application state]"]

    # ── 1. KB directory ───────────────────────────────────────────────────────
    report.append("\n-- Knowledge Base --")
    _rm_dir(kb_root, report)

    # ── 2. Auth DB history ────────────────────────────────────────────────────
    report.append("\n-- Auth Database (history only, accounts preserved) --")
    if auth_db_path and auth_db_path.exists():
        _clear_auth_history(auth_db_path, report)
    else:
        report.append("  - Skipped  auth DB  (not found or not specified)")

    # ── 3. Logs ───────────────────────────────────────────────────────────────
    report.append("\n-- Logs --")
    if logs_dir and logs_dir.exists():
        _rm_glob(logs_dir, "nexus_*.log", report)
    else:
        report.append("  - Skipped  logs/  (not found or not specified)")

    report.append(
        "\nFactory reset complete. Restart the application to begin fresh setup."
    )
    return True, "\n".join(report)


def _clear_auth_history(auth_db_path: Path, report: List[str]) -> None:
    """
    Truncate conversation/message/session/feedback tables in nexus_auth.db.
    User accounts and DB connections are preserved.
    """
    try:
        import sqlite3
        conn = sqlite3.connect(str(auth_db_path))
        cur = conn.cursor()

        # Tables that hold ephemeral history (safe to wipe)
        history_tables = ["messages", "conversations", "feedback", "sessions"]
        cleared: List[str] = []
        skipped: List[str] = []

        for table in history_tables:
            try:
                cur.execute(f"DELETE FROM {table}")  # noqa: S608
                cleared.append(table)
            except sqlite3.OperationalError:
                skipped.append(table)  # table doesn't exist yet — fine

        conn.commit()
        conn.close()

        if cleared:
            report.append(f"  ✓ Cleared  auth tables: {', '.join(cleared)}")
        if skipped:
            report.append(f"  - Skipped  auth tables: {', '.join(skipped)}  (not found)")

    except Exception as exc:
        report.append(f"  ✗ Failed   auth DB  ({exc})")
