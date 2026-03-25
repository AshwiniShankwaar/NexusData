"""
nexus_data/kb/conversation_graph.py
Conversation lineage graph stored as metadata.

Each executed turn is a node with a parent_turn_id pointer, enabling:
  - Branch detection: FQ4 can bypass FQ3 and attach to Q1 when tables differ
  - Ancestry traversal: FQ3 → FQ2 → Q1 for full chain context
  - Persistent across session restarts (JSON file)

Q1 ─→ FQ2 ─→ FQ3
 └──→ FQ4
"""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_MAX_TURNS = 50


@dataclass
class ConversationTurn:
    turn_id: str                     # uuid4 string
    query: str                       # original natural-language input
    intent_summary: str              # from GoalIdentifierResult.intent_summary
    sql_used: str                    # executed SQL
    tables_used: List[str] = field(default_factory=list)
    filters_used: List[str] = field(default_factory=list)
    grouping_used: List[str] = field(default_factory=list)
    metrics_used: List[str] = field(default_factory=list)
    parent_turn_id: Optional[str] = None   # None for root turns (not a follow-up)
    timestamp: str = ""                    # ISO-8601 UTC, set by add_turn if empty


class ConversationGraph:
    """
    In-memory graph of conversation turns, persisted to JSON.
    Provides graph-aware parent lookup instead of flat last-entry scan.
    """

    def __init__(self, kb_dir: Optional[Path] = None) -> None:
        self._dir = kb_dir or Path("./nexus_kb")
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "conversation_lineage.json"
        self._turns: Dict[str, ConversationTurn] = {}  # turn_id → turn
        self._order: List[str] = []                    # insertion order
        self._load()

    # ── Public API ─────────────────────────────────────────────────────────────

    def add_turn(self, turn: ConversationTurn) -> None:
        """Add a turn; evict oldest if over _MAX_TURNS. Persists to JSON."""
        if not turn.timestamp:
            turn.timestamp = datetime.now(timezone.utc).isoformat()
        self._turns[turn.turn_id] = turn
        self._order.append(turn.turn_id)
        # Sliding window eviction
        while len(self._order) > _MAX_TURNS:
            evicted = self._order.pop(0)
            self._turns.pop(evicted, None)
        self._save()

    def get_turn(self, turn_id: str) -> Optional[ConversationTurn]:
        """Return turn by ID, or None."""
        return self._turns.get(turn_id)

    def get_ancestors(self, turn_id: str, max_depth: int = 5) -> List[ConversationTurn]:
        """
        Walk parent_turn_id chain up to max_depth.
        Returns list ordered oldest-first (root at index 0).
        Stops silently if a parent was evicted from the sliding window.
        """
        chain: List[ConversationTurn] = []
        current = self._turns.get(turn_id)
        for _ in range(max_depth):
            if not current or not current.parent_turn_id:
                break
            parent = self._turns.get(current.parent_turn_id)
            if not parent:
                break
            chain.append(parent)
            current = parent
        chain.reverse()  # oldest first
        return chain

    def find_best_parent(self, tables: List[str]) -> Optional[ConversationTurn]:
        """
        Search all stored turns (newest-first) for the best context match.

        Returns the most recent turn where table_overlap >= 0.5.
        Falls back to the most recent turn overall if no overlap match found.
        Returns None if the graph is empty.

        This enables branching follow-up detection:
          Q1(orders) → FQ2(orders) → FQ3(orders)
          FQ4 asks about orders → correctly attaches to FQ3 (most recent overlap)
          FQ4 asks about users  → fallback to most recent (FQ3), resolver then
                                   skips filter carry due to low table overlap
        """
        if not self._order:
            return None

        query_set = set(tables)
        fallback = self._turns[self._order[-1]]  # most recent

        if not query_set:
            return fallback

        for tid in reversed(self._order):
            turn = self._turns[tid]
            prev_set = set(turn.tables_used)
            if not prev_set:
                continue
            overlap = len(query_set & prev_set) / max(len(prev_set), 1)
            if overlap >= 0.50:
                logger.debug(
                    "ConversationGraph: best parent = '%s' (overlap=%.0f%%) for tables=%s",
                    turn.intent_summary, overlap * 100, tables,
                )
                return turn

        logger.debug("ConversationGraph: no overlap match; returning most recent turn as fallback.")
        return fallback

    def __len__(self) -> int:
        return len(self._order)

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Deserialize from JSON. Silently resets on parse failure."""
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            for item in raw.get("turns", []):
                turn = ConversationTurn(**item)
                self._turns[turn.turn_id] = turn
                self._order.append(turn.turn_id)
        except Exception as exc:
            logger.warning("ConversationGraph: failed to load lineage (%s). Starting fresh.", exc)
            self._turns.clear()
            self._order.clear()

    def _save(self) -> None:
        """Serialize to JSON. Logs warning on failure, never raises."""
        try:
            payload = {
                "turns": [dataclasses.asdict(self._turns[tid]) for tid in self._order]
            }
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("ConversationGraph: save failed: %s", exc)
