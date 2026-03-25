"""
nexus_data/diplomat/clarification.py  — Task 5.2
Clarification UI Bridge: detects ambiguous queries against available tables,
generates structured A/B options, and records the user's choice back to
Shadow Metadata.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from nexus_data.models import DatabaseProfile, TableMeta

logger = logging.getLogger(__name__)


@dataclass
class ClarificationRequest:
    """Structured question sent back to the user when confidence is low."""
    question: str
    options: List[Dict[str, str]] = field(default_factory=list)
    original_nl: str = ""
    ambiguous_token: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "question": self.question,
            "options": self.options,
            "original_nl": self.original_nl,
            "ambiguous_token": self.ambiguous_token,
        }


class ClarificationBridge:
    """
    Generates human-readable disambiguation options and routes user answers
    back to the ShadowMetadataManager.

    Ambiguity detection
    -------------------
    - Multiple tables whose names contain the same keyword as the NL query
    - Confidence score below threshold
    """

    def __init__(self, kb_manager=None):
        self.kb = kb_manager
        
    def detect_ambiguous_tables(
        self, nl_query: str, profile: DatabaseProfile
    ) -> List[TableMeta]:
        tokens = set(nl_query.lower().split())
        candidates: List[TableMeta] = []
        for tbl in profile.tables:
            tbl_words = set(tbl.name.lower().replace("_", " ").split())
            if tokens & tbl_words or any(tok in tbl.name.lower() for tok in tokens):
                candidates.append(tbl)
        return candidates

    def build_clarification(
        self,
        nl_query: str,
        candidates: List[TableMeta],
        ambiguous_token: str = "",
    ) -> ClarificationRequest:
        options = []
        for i, tbl in enumerate(candidates):
            label = chr(ord("A") + i)
            col_preview = ", ".join(c.name for c in tbl.columns[:4])
            options.append({
                "label": label,
                "table": tbl.name,
                "description": tbl.description or tbl.name.replace("_", " ").title(),
                "columns_preview": col_preview,
            })

        question = f"I found {len(candidates)} possible tables for '{nl_query}'. Which did you mean?"
        return ClarificationRequest(question=question, options=options, original_nl=nl_query, ambiguous_token=ambiguous_token or nl_query)

    def resolve(
        self,
        clarification: ClarificationRequest,
        user_choice: str,
    ) -> Optional[TableMeta]:
        chosen_table: Optional[TableMeta] = None
        for opt in clarification.options:
            if user_choice.upper() == opt["label"] or user_choice.lower() == opt["table"].lower():
                chosen_table = next((o for o in clarification.options if o["label"] == opt["label"]), None)
                break

        if chosen_table and self.kb:
            # Inject clarification directly into persona memory
            self.kb.append_user_persona(f"When user says '{clarification.original_nl}', they mean table '{chosen_table['table']}'.")
            
        return chosen_table
