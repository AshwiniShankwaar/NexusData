"""
tests/test_reset.py
Unit tests for nexus_data/core/reset.py — factory reset utility.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from nexus_data.core.reset import soft_reset, full_reset, factory_reset


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def kb_dir(tmp_path: Path) -> Path:
    """A fake KB directory with all the files a real KB would contain."""
    d = tmp_path / "nexus_kb" / "mydb"
    d.mkdir(parents=True)

    (d / "conversation_lineage.json").write_text('{"turns": []}', encoding="utf-8")
    (d / "shortterm_memory.md").write_text("# session history", encoding="utf-8")
    (d / "longterm_memory.md").write_text("# learned facts", encoding="utf-8")
    (d / "db_info.md").write_text("# schema", encoding="utf-8")
    (d / "query_graph.json").write_text("[]", encoding="utf-8")
    (d / "bookmarks.json").write_text("{}", encoding="utf-8")
    (d / "cache_stats.json").write_text("{}", encoding="utf-8")
    (d / "audit_20260324.jsonl").write_text('{"q":"test"}\n', encoding="utf-8")

    vs = d / "vector_store"
    vs.mkdir()
    (vs / "data.bin").write_bytes(b"\x00\x01\x02")

    return d


@pytest.fixture()
def auth_db(tmp_path: Path) -> Path:
    """A minimal nexus_auth.db with all history tables populated."""
    db_path = tmp_path / "nexus_auth.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT);
        CREATE TABLE conversations (id TEXT PRIMARY KEY, user_id TEXT);
        CREATE TABLE messages (id TEXT PRIMARY KEY, conversation_id TEXT);
        CREATE TABLE sessions (id TEXT PRIMARY KEY, user_id TEXT);
        CREATE TABLE feedback (id TEXT PRIMARY KEY, message_id TEXT);

        INSERT INTO users VALUES ('u1', 'Alice');
        INSERT INTO conversations VALUES ('c1', 'u1');
        INSERT INTO messages VALUES ('m1', 'c1');
        INSERT INTO sessions VALUES ('s1', 'u1');
        INSERT INTO feedback VALUES ('f1', 'm1');
    """)
    conn.commit()
    conn.close()
    return db_path


# ── soft_reset ────────────────────────────────────────────────────────────────

class TestSoftReset:
    def test_removes_conversation_lineage(self, kb_dir: Path):
        ok, _ = soft_reset(kb_dir)
        assert ok
        assert not (kb_dir / "conversation_lineage.json").exists()

    def test_removes_shortterm_memory(self, kb_dir: Path):
        soft_reset(kb_dir)
        # File is deleted then recreated empty
        short = kb_dir / "shortterm_memory.md"
        # Either deleted or recreated empty — must not contain session content
        if short.exists():
            assert short.read_text(encoding="utf-8") == ""

    def test_removes_vector_store(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert not (kb_dir / "vector_store").exists()

    def test_removes_cache_stats(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert not (kb_dir / "cache_stats.json").exists()

    def test_preserves_longterm_memory(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert (kb_dir / "longterm_memory.md").exists()

    def test_preserves_db_info(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert (kb_dir / "db_info.md").exists()

    def test_preserves_query_graph(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert (kb_dir / "query_graph.json").exists()

    def test_preserves_bookmarks(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert (kb_dir / "bookmarks.json").exists()

    def test_preserves_audit_log(self, kb_dir: Path):
        soft_reset(kb_dir)
        assert (kb_dir / "audit_20260324.jsonl").exists()

    def test_returns_success_flag(self, kb_dir: Path):
        ok, _ = soft_reset(kb_dir)
        assert ok is True

    def test_report_mentions_conversation(self, kb_dir: Path):
        _, report = soft_reset(kb_dir)
        assert "conversation_lineage" in report

    def test_idempotent_second_call(self, kb_dir: Path):
        soft_reset(kb_dir)
        ok, report = soft_reset(kb_dir)  # second call — files already gone
        assert ok is True
        assert "Skipped" in report or "not found" in report.lower()

    def test_recreates_empty_shortterm_memory(self, kb_dir: Path):
        soft_reset(kb_dir)
        short = kb_dir / "shortterm_memory.md"
        # Must exist and be empty so KBManager doesn't throw on first read
        assert short.exists()
        assert short.read_text(encoding="utf-8") == ""


# ── full_reset ────────────────────────────────────────────────────────────────

class TestFullReset:
    def test_removes_conversation_lineage(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "conversation_lineage.json").exists()

    def test_removes_longterm_memory(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "longterm_memory.md").exists()

    def test_removes_db_info(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "db_info.md").exists()

    def test_removes_query_graph(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "query_graph.json").exists()

    def test_removes_bookmarks(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "bookmarks.json").exists()

    def test_removes_audit_logs(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "audit_20260324.jsonl").exists()

    def test_removes_vector_store(self, kb_dir: Path):
        full_reset(kb_dir)
        assert not (kb_dir / "vector_store").exists()

    def test_returns_success_flag(self, kb_dir: Path):
        ok, _ = full_reset(kb_dir)
        assert ok is True

    def test_report_mentions_longterm_memory(self, kb_dir: Path):
        _, report = full_reset(kb_dir)
        assert "longterm_memory" in report

    def test_multiple_audit_logs_removed(self, kb_dir: Path):
        (kb_dir / "audit_20260323.jsonl").write_text("", encoding="utf-8")
        (kb_dir / "audit_20260325.jsonl").write_text("", encoding="utf-8")
        full_reset(kb_dir)
        assert not list(kb_dir.glob("audit_*.jsonl"))

    def test_directory_still_exists_after_full(self, kb_dir: Path):
        """full_reset removes files, not the directory itself."""
        full_reset(kb_dir)
        assert kb_dir.exists()


# ── factory_reset ─────────────────────────────────────────────────────────────

class TestFactoryReset:
    def test_removes_entire_kb_root(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        ok, _ = factory_reset(kb_root)
        assert ok
        assert not kb_root.exists()

    def test_clears_auth_history_tables(self, kb_dir: Path, tmp_path: Path, auth_db: Path):
        kb_root = tmp_path / "nexus_kb"
        factory_reset(kb_root, auth_db_path=auth_db)

        conn = sqlite3.connect(str(auth_db))
        assert conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == 0
        conn.close()

    def test_preserves_user_accounts(self, kb_dir: Path, tmp_path: Path, auth_db: Path):
        kb_root = tmp_path / "nexus_kb"
        factory_reset(kb_root, auth_db_path=auth_db)

        conn = sqlite3.connect(str(auth_db))
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()
        assert count == 1  # Alice still exists

    def test_clears_log_files(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        logs = tmp_path / "logs"
        logs.mkdir()
        (logs / "nexus_2026-03-24.log").write_text("line1\n", encoding="utf-8")
        (logs / "nexus_2026-03-23.log").write_text("line2\n", encoding="utf-8")
        (logs / ".gitkeep").write_text("", encoding="utf-8")  # must survive

        factory_reset(kb_root, logs_dir=logs)

        assert not (logs / "nexus_2026-03-24.log").exists()
        assert not (logs / "nexus_2026-03-23.log").exists()
        assert (logs / ".gitkeep").exists()  # gitkeep preserved

    def test_returns_success_flag(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        ok, _ = factory_reset(kb_root)
        assert ok is True

    def test_report_mentions_kb(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        _, report = factory_reset(kb_root)
        assert "nexus_kb" in report or "Knowledge Base" in report

    def test_tolerates_missing_auth_db(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        missing = tmp_path / "does_not_exist.db"
        ok, report = factory_reset(kb_root, auth_db_path=missing)
        assert ok is True
        assert "Skipped" in report

    def test_tolerates_missing_logs_dir(self, kb_dir: Path, tmp_path: Path):
        kb_root = tmp_path / "nexus_kb"
        missing = tmp_path / "no_logs"
        ok, report = factory_reset(kb_root, logs_dir=missing)
        assert ok is True

    def test_tolerates_missing_kb_root(self, tmp_path: Path):
        """factory_reset on a non-existent KB root should succeed silently."""
        kb_root = tmp_path / "nonexistent_kb"
        ok, report = factory_reset(kb_root)
        assert ok is True
        assert "not found" in report.lower() or "Skipped" in report
