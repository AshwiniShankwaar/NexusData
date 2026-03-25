"""
nexus_data/kb/bookmarks.py — Named Query Bookmarks (#7)
Lets users save a query + SQL under a name and replay it instantly.
Persists to bookmarks.json in the KB directory.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_MAX_BOOKMARKS = 100


class BookmarkStore:
    def __init__(self, kb_dir: Optional[Path] = None):
        self._dir = kb_dir or Path("./nexus_kb")
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "bookmarks.json"
        self._data: Dict[str, Dict] = self._load()

    def _load(self) -> Dict[str, Dict]:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {}

    def _save(self) -> None:
        try:
            self._path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Bookmark save failed: %s", exc)

    def save(self, name: str, query: str, sql: str) -> None:
        """Save or overwrite a named bookmark."""
        if len(self._data) >= _MAX_BOOKMARKS and name not in self._data:
            # Evict oldest if at capacity
            oldest = next(iter(self._data))
            del self._data[oldest]
        self._data[name.lower()] = {"query": query, "sql": sql}
        self._save()
        logger.info("Bookmark saved: '%s'", name)

    def get(self, name: str) -> Optional[Tuple[str, str]]:
        """Return (query, sql) tuple or None."""
        entry = self._data.get(name.lower())
        if entry:
            return entry["query"], entry["sql"]
        return None

    def delete(self, name: str) -> bool:
        """Delete a bookmark. Returns True if it existed."""
        if name.lower() in self._data:
            del self._data[name.lower()]
            self._save()
            return True
        return False

    def list_all(self) -> List[Tuple[str, str]]:
        """Return [(name, query)] sorted by name."""
        return [(name, v["query"]) for name, v in sorted(self._data.items())]

    def __len__(self) -> int:
        return len(self._data)
