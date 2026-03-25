"""
nexus_data/kb/manager.py
3-Tier Memory: long-term (persona), short-term (log), session cache (in-memory).
Also manages db_info.md and provides schema name lookup for normalizer.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

KB_DIR = Path(os.getenv("NEXUS_KB_DIR", "./nexus_kb"))


_SESSION_CACHE_MAX = 200   # max entries — older items evicted to prevent memory leak


class KBManager:
    def __init__(self, kb_dir: Optional[Path] = None):
        self._dir = kb_dir or KB_DIR
        self._dir.mkdir(parents=True, exist_ok=True)

        self._init_file("db_info.md", "# Database Topology\n\n(Auto-generated)")
        self._init_file("longterm_memory.md", "# Long-Term Memory\n\n")
        self._init_file("shortterm_memory.md", "# Short-Term Memory\n\n")

        self._session_cache: List[Any] = []
        # Parsed schema names — populated lazily by get_schema_names()
        self._schema_tables: List[str] = []
        self._schema_columns: Dict[str, List[str]] = {}  # table → columns
        self._db_info_cache: Optional[str] = None

    def _init_file(self, filename: str, default: str) -> None:
        p = self._dir / filename
        if not p.exists():
            p.write_text(default, encoding="utf-8")

    # ── DB Info ───────────────────────────────────────────────────────────────

    def read_db_info(self) -> str:
        if self._db_info_cache is None:
            self._db_info_cache = (self._dir / "db_info.md").read_text(encoding="utf-8")
        return self._db_info_cache

    def write_db_info(self, content: str) -> None:
        (self._dir / "db_info.md").write_text(content, encoding="utf-8")
        self._db_info_cache = None  # invalidate
        # Invalidate schema cache
        self._schema_tables = []
        self._schema_columns = {}

    def get_schema_names(self) -> Tuple[List[str], Dict[str, List[str]]]:
        """Parse db_info.md and return (table_names, {table: [col_names]}).
        Cached after first parse."""
        if self._schema_tables:
            return self._schema_tables, self._schema_columns

        md = self.read_db_info()
        tables: List[str] = []
        columns: Dict[str, List[str]] = {}
        current_table: Optional[str] = None

        for line in md.splitlines():
            # ## Table: `users`
            t_match = re.match(r"^## Table: `([^`]+)`", line)
            if t_match:
                current_table = t_match.group(1)
                tables.append(current_table)
                columns[current_table] = []
                continue
            # - `col_name` (TYPE)
            if current_table:
                c_match = re.match(r"^- `([^`]+)`", line)
                if c_match:
                    columns[current_table].append(c_match.group(1))

        self._schema_tables = tables
        self._schema_columns = columns
        return tables, columns

    # ── Memory ────────────────────────────────────────────────────────────────

    def read_longterm_memory(self) -> str:
        return (self._dir / "longterm_memory.md").read_text(encoding="utf-8")

    def append_longterm_memory(self, content: str) -> None:
        path = self._dir / "longterm_memory.md"
        current = path.read_text(encoding="utf-8")
        if content not in current:
            path.write_text(current.rstrip() + f"\n\n- {content}\n", encoding="utf-8")

    def read_shortterm_memory(self) -> str:
        return (self._dir / "shortterm_memory.md").read_text(encoding="utf-8")

    def append_shortterm_memory(self, log_entry: str) -> None:
        path = self._dir / "shortterm_memory.md"
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n- {log_entry}\n")

    def get_session_cache(self) -> List[Any]:
        return self._session_cache

    def add_to_session_cache(self, item: Any) -> None:
        self._session_cache.append(item)
        # Evict oldest entries to prevent unbounded memory growth
        if len(self._session_cache) > _SESSION_CACHE_MAX:
            self._session_cache = self._session_cache[-_SESSION_CACHE_MAX:]

    def get_last_turn_record(self) -> Optional[Dict[str, Any]]:
        """Return the most recent turn_record dict from session cache."""
        for entry in reversed(self._session_cache):
            if isinstance(entry, dict) and entry.get("type") == "turn_record":
                return entry
        return None

    # ── Feedback examples (few-shot) ──────────────────────────────────────────

    def append_feedback_example(
        self, query: str, bad_sql: str, good_sql: str, feedback: str
    ) -> None:
        """Persist a user-correction as a few-shot example in longterm_memory.md."""
        block = (
            f"\n### Feedback Example\n"
            f"- **Question**: {query}\n"
            f"- **Wrong SQL**: `{bad_sql}`\n"
            f"- **Feedback**: {feedback}\n"
            f"- **Correct SQL**: `{good_sql}`\n"
        )
        path = self._dir / "longterm_memory.md"
        current = path.read_text(encoding="utf-8")
        path.write_text(current.rstrip() + "\n" + block, encoding="utf-8")
        logger.info("Feedback example saved to longterm_memory.")

    def get_feedback_examples(self, max_examples: int = 3) -> str:
        """Return the last N feedback examples as a formatted string for prompts."""
        ltm = self.read_longterm_memory()
        blocks = re.split(r"(?=### Feedback Example)", ltm)
        examples = [b.strip() for b in blocks if "### Feedback Example" in b]
        if not examples:
            return ""
        recent = examples[-max_examples:]
        return "## Past Correction Examples (few-shot)\n" + "\n\n".join(recent) + "\n"

    def cap_shortterm_memory(self, max_lines: int) -> None:
        """Trim shortterm_memory.md to at most max_lines lines."""
        path = self._dir / "shortterm_memory.md"
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            if len(lines) > max_lines:
                header = lines[:2]
                tail = lines[-(max_lines - 2):]
                path.write_text("\n".join(header + ["…(older history trimmed)…"] + tail), encoding="utf-8")
        except Exception:
            pass

    def get_combined_persona_context(self) -> str:
        ltm = self.read_longterm_memory()
        stm = self.read_shortterm_memory()[-2000:]
        temp = "\n".join(str(item) for item in self._session_cache[-10:])
        return (
            f"<LONG_TERM_CONTEXT>\n{ltm}\n</LONG_TERM_CONTEXT>\n\n"
            f"<SHORT_TERM_CONTEXT>\n{stm}\n</SHORT_TERM_CONTEXT>\n\n"
            f"<CURRENT_SESSION_CONTEXT>\n{temp}\n</CURRENT_SESSION_CONTEXT>"
        )
