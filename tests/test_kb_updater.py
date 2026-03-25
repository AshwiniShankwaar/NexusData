"""tests/test_kb_updater.py — unit tests for ConversationKBUpdater."""
import pytest
from nexus_data.kb.kb_updater import ConversationKBUpdater, _extract_regex_facts


# ── Regex fact extraction ──────────────────────────────────────────────────────

def test_meaning_pattern_extracted():
    facts = _extract_regex_facts("status 'A' means Active")
    assert any("means" in f.lower() or "active" in f.lower() for f in facts)


def test_column_info_pattern_extracted():
    facts = _extract_regex_facts("the tier column contains the subscription level")
    assert any("tier" in f.lower() for f in facts)


def test_table_info_pattern_extracted():
    facts = _extract_regex_facts("the orders table stores purchase records")
    assert any("orders" in f.lower() for f in facts)


def test_filter_hint_pattern_extracted():
    facts = _extract_regex_facts("always use status = 'active' when filtering")
    assert any("filter" in f.lower() or "hint" in f.lower() for f in facts)


def test_correction_pattern_use_instead_of():
    facts = _extract_regex_facts("use customer_id instead of user_id")
    assert any("customer_id" in f for f in facts)


def test_no_facts_from_plain_query():
    facts = _extract_regex_facts("show me all users with status active")
    assert facts == []


# ── scan_and_update ────────────────────────────────────────────────────────────

class _FakeKBManager:
    def __init__(self):
        self.saved = []

    def append_longterm_memory(self, text):
        self.saved.append(text)


def test_scan_and_update_saves_facts():
    updater = ConversationKBUpdater(llm=None)
    kb = _FakeKBManager()
    facts = updater.scan_and_update("the tier column contains subscription tier info", kb)
    assert len(facts) > 0
    assert len(kb.saved) > 0


def test_scan_and_update_skips_query_like():
    updater = ConversationKBUpdater(llm=None)
    kb = _FakeKBManager()
    facts = updater.scan_and_update("show all users where status is active", kb)
    assert facts == []
    assert kb.saved == []


def test_scan_and_update_skips_short_message():
    updater = ConversationKBUpdater(llm=None)
    kb = _FakeKBManager()
    facts = updater.scan_and_update("ok", kb)
    assert facts == []


def test_scan_and_update_prefixes_with_auto_learned():
    updater = ConversationKBUpdater(llm=None)
    kb = _FakeKBManager()
    updater.scan_and_update("the orders table stores purchase records", kb)
    assert all("[Auto-learned]" in s for s in kb.saved)


def test_scan_and_update_no_llm_no_crash():
    """Should not crash or call LLM when llm=None."""
    updater = ConversationKBUpdater(llm=None)
    kb = _FakeKBManager()
    # No knowledge verbs, no regex match → returns []
    updater.scan_and_update("the sky is blue today at noon", kb)
    # No assert needed — just confirm no exception
