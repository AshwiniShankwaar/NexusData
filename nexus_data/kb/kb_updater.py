"""
nexus_data/kb/kb_updater.py — Conversational Self-Improving Knowledge Base
Scans user messages for domain-knowledge assertions and persists them to
longterm_memory.md so future queries benefit from them automatically.

Detection strategy:
  Pass 1 — fast regex: catches common "X means Y", "column X contains...", etc.
  Pass 2 — LLM extraction: only when the message is non-trivial and regex found nothing
            but message looks knowledge-bearing (has assignment/definition verbs).
"""
from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Pattern definitions ────────────────────────────────────────────────────────

# "status 'A' means active" / "status = 1 means active" / "1 means active"
_MEANING_PATTERN = re.compile(
    r"(?:(?:column\s+)?(?P<field>[\w.]+)\s+(?:=|of|'[^']+'|\"[^\"]+\"|\d+)\s+)?"
    r"(?:means?|indicates?|represents?|stands?\s+for|is\s+used\s+for)\s+(?P<value>.{3,80})",
    re.IGNORECASE,
)

# "the X column contains Y" / "X column stores Y" / "X field holds Y"
_COLUMN_INFO_PATTERN = re.compile(
    r"(?:the\s+)?(?P<col>[\w.]+)\s+(?:column|field|attribute)\s+"
    r"(?:contains?|stores?|holds?|has|represents?|is)\s+(?P<desc>.{3,120})",
    re.IGNORECASE,
)

# "table X is used for Y" / "X table stores Y"
_TABLE_INFO_PATTERN = re.compile(
    r"(?:the\s+)?(?P<tbl>[\w.]+)\s+table\s+"
    r"(?:is\s+used\s+for|stores?|contains?|holds?|represents?)\s+(?P<desc>.{3,120})",
    re.IGNORECASE,
)

# "always use X = Y when filtering" / "filter by X = Y"
_FILTER_HINT_PATTERN = re.compile(
    r"(?:always\s+(?:use|apply|filter\s+by)|when\s+filtering\s+use)\s+"
    r"(?P<hint>[^.!?]{5,100})",
    re.IGNORECASE,
)

# "X is not Y" / "X should be Y" / "use X instead of Y"
_CORRECTION_PATTERN = re.compile(
    r"(?:use\s+(?P<correct>[^,]+?)\s+instead\s+of\s+(?P<wrong>[^.!?]+)|"
    r"(?P<field2>[\w.]+)\s+should\s+be\s+(?P<val>[^.!?]+))",
    re.IGNORECASE,
)

# Verbs that suggest the message may carry domain knowledge
_KNOWLEDGE_VERBS = re.compile(
    r"\b(means?|refers?\s+to|stands?\s+for|indicates?|represents?|stores?|holds?|"
    r"contains?|should\s+be|instead\s+of|is\s+used\s+for|always\s+use|filter\s+by)\b",
    re.IGNORECASE,
)

_MIN_KNOWLEDGE_LEN = 10  # shorter messages usually aren't domain knowledge


def _extract_regex_facts(text: str) -> List[str]:
    facts: List[str] = []

    for m in _MEANING_PATTERN.finditer(text):
        field = m.group("field") or "value"
        val = m.group("value").rstrip(". ")
        facts.append(f"Domain: '{field}' means '{val}'")

    for m in _COLUMN_INFO_PATTERN.finditer(text):
        col = m.group("col")
        desc = m.group("desc").rstrip(". ")
        facts.append(f"Column info: '{col}' — {desc}")

    for m in _TABLE_INFO_PATTERN.finditer(text):
        tbl = m.group("tbl")
        desc = m.group("desc").rstrip(". ")
        facts.append(f"Table info: '{tbl}' — {desc}")

    for m in _FILTER_HINT_PATTERN.finditer(text):
        hint = m.group("hint").rstrip(". ")
        facts.append(f"Filter hint: {hint}")

    for m in _CORRECTION_PATTERN.finditer(text):
        if m.group("correct"):
            facts.append(
                f"Correction: use '{m.group('correct').strip()}' "
                f"instead of '{m.group('wrong').strip()}'"
            )
        elif m.group("field2"):
            facts.append(
                f"Correction: '{m.group('field2')}' should be '{m.group('val').strip()}'"
            )

    return facts


_LLM_EXTRACT_PROMPT = """\
Extract factual domain knowledge from the user's message that would help a SQL assistant
understand the database better (e.g. what column values mean, how tables are used, filter hints).

Message: {message}

Output a JSON array of short fact strings. If there is no useful domain knowledge, output [].
Example: ["status='A' means Active", "orders table stores purchase records"]
Output ONLY the JSON array."""


class ConversationKBUpdater:
    """
    Scans each user message for domain knowledge and persists findings to KB.
    """

    def __init__(self, llm=None):
        self._llm = llm   # optional — enables LLM fallback extraction

    def scan_and_update(self, message: str, kb_manager) -> List[str]:
        """
        Scan a user message for domain knowledge.
        Saves any found facts to longterm_memory.md.
        Returns list of saved fact strings (empty if nothing found).
        """
        if len(message.strip()) < _MIN_KNOWLEDGE_LEN:
            return []

        # Skip if message looks like a data query (starts with question words)
        if re.match(
            r"^(show|list|how many|what|which|who|where|when|count|get|find|"
            r"select|give me|tell me|fetch)",
            message.strip(), re.IGNORECASE
        ):
            return []

        facts = _extract_regex_facts(message)

        # LLM fallback: if regex found nothing but message has knowledge verbs
        if not facts and self._llm and _KNOWLEDGE_VERBS.search(message):
            facts = self._llm_extract(message)

        if not facts:
            return []

        saved = []
        for fact in facts:
            normalized = fact.strip()
            if normalized:
                kb_manager.append_longterm_memory(f"[Auto-learned] {normalized}")
                saved.append(normalized)
                logger.info("KB auto-learned: %s", normalized)

        return saved

    def _llm_extract(self, message: str) -> List[str]:
        """LLM-based extraction fallback — returns list of fact strings."""
        if not self._llm:
            return []
        try:
            import json, re as _re
            raw = self._llm.generate(
                _LLM_EXTRACT_PROMPT.format(message=message[:500]),
                message,
                max_retries=1,
            )
            clean = _re.sub(r"```(?:json)?", "", raw, flags=_re.IGNORECASE).replace("```", "").strip()
            data = json.loads(clean)
            if isinstance(data, list):
                return [str(f) for f in data if f]
        except Exception as exc:
            logger.debug("KB updater LLM extraction failed: %s", exc)
        return []
