"""
tests/test_kb.py
Verifies the new 3-Tier Memory Architecture in the KBManager.
"""
from __future__ import annotations

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from nexus_data.kb.manager import KBManager

@pytest.fixture
def temp_kb():
    with TemporaryDirectory() as tmp:
        yield KBManager(kb_dir=Path(tmp))

class Test3TierMemory:
    def test_longterm_memory(self, temp_kb):
        # Starts empty (with header)
        initial = temp_kb.read_longterm_memory()
        assert "Long-Term" in initial

        # Append
        temp_kb.append_longterm_memory("User likes aggregate views.")
        updated = temp_kb.read_longterm_memory()
        assert "aggregate views" in updated
        
        # Deduplication check
        temp_kb.append_longterm_memory("User likes aggregate views.")
        lines = updated.splitlines()
        assert sum(1 for line in lines if "aggregate views" in line) == 1

    def test_shortterm_memory(self, temp_kb):
        temp_kb.append_shortterm_memory("User asked about sales.")
        temp_kb.append_shortterm_memory("System retrieved 10 rows.")
        log = temp_kb.read_shortterm_memory()
        assert "sales" in log
        assert "10 rows" in log

    def test_temp_session_cache(self, temp_kb):
        temp_kb.add_to_session_cache("Task 1 completed")
        cache = temp_kb.get_session_cache()
        assert len(cache) == 1
        assert cache[0] == "Task 1 completed"

    def test_combined_context(self, temp_kb):
        temp_kb.append_longterm_memory("Persona: CFO")
        temp_kb.append_shortterm_memory("History: checked churn")
        temp_kb.add_to_session_cache("Current: looking at Q3")
        
        ctx = temp_kb.get_combined_persona_context()
        assert "Persona: CFO" in ctx
        assert "History: checked churn" in ctx
        assert "Current: looking at Q3" in ctx
