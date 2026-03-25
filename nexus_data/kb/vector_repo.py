"""
nexus_data/kb/vector_repo.py
LanceDB-backed semantic cache for Intent → SQL mapping.
Gracefully degrades if sentence_transformers / TensorFlow cannot load.
Adaptive threshold: raises similarity bar when correction rate exceeds 30%.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import lancedb
from lancedb.pydantic import LanceModel, Vector

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 384
_THRESHOLD_MIN = 0.80
_THRESHOLD_MAX = 0.98
_CORRECTION_RATE_LIMIT = 0.30   # raise threshold when corrections exceed this
_THRESHOLD_STEP = 0.02


class CachedQuerySchema(LanceModel):
    intent: str
    sql: str
    vector: Vector(EMBEDDING_DIM)
    frequency: int = 1


class VectorQueryRepo:
    """Semantic cache. Falls back to a no-op if the embedder cannot be loaded."""

    def __init__(self, db_dir: Optional[Path] = None, similarity_threshold: float = 0.90):
        self._db_path = db_dir or Path("./nexus_kb/vector_store")
        self._db_path.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(self._db_path))
        self._stats_path = self._db_path / "cache_stats.json"
        self._stats = self._load_stats(similarity_threshold)
        self.similarity_threshold = self._stats["threshold"]
        self._embedder = None
        self._embedder_broken = False   # set True if import fails — disables cache
        self._table = self._ensure_table()

    # ── Adaptive threshold stats ───────────────────────────────────────────────

    def _load_stats(self, default_threshold: float) -> Dict[str, Any]:
        if self._stats_path.exists():
            try:
                return json.loads(self._stats_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"total_hits": 0, "total_corrections": 0, "threshold": default_threshold}

    def _save_stats(self) -> None:
        try:
            self._stats_path.write_text(json.dumps(self._stats, indent=2), encoding="utf-8")
        except Exception as exc:
            logger.warning("Could not save cache stats: %s", exc)

    def record_hit(self) -> None:
        """Call when a cached SQL was used successfully (no correction needed)."""
        self._stats["total_hits"] = self._stats.get("total_hits", 0) + 1
        self._save_stats()

    def record_correction(self) -> None:
        """Call when a cached SQL was served but was wrong (user gave feedback).
        If correction rate exceeds 30% raise the similarity threshold."""
        self._stats["total_corrections"] = self._stats.get("total_corrections", 0) + 1
        total = self._stats["total_hits"] + self._stats["total_corrections"]
        if total >= 5:  # need minimum samples
            rate = self._stats["total_corrections"] / total
            if rate > _CORRECTION_RATE_LIMIT:
                old = self._stats["threshold"]
                self._stats["threshold"] = min(_THRESHOLD_MAX, old + _THRESHOLD_STEP)
                self.similarity_threshold = self._stats["threshold"]
                logger.info(
                    "Adaptive cache: correction rate=%.0f%% → threshold %.2f → %.2f",
                    rate * 100, old, self._stats["threshold"],
                )
        self._save_stats()

    # ── Table setup ───────────────────────────────────────────────────────────

    def _ensure_table(self):
        try:
            # Try to open first — more reliable than list_tables() across LanceDB versions
            try:
                return self._db.open_table("query_repo")
            except Exception:
                return self._db.create_table("query_repo", schema=CachedQuerySchema)
        except Exception as exc:
            # Windows: WinError 32 (file locked by another process)
            err_str = str(exc)
            if "32" in err_str or "lock" in err_str.lower() or "being used" in err_str.lower():
                self._embedder_broken = True
                logger.warning(
                    "Vector store locked by another process (Windows file lock). "
                    "Semantic cache disabled for this session. "
                    "Close other NexusData instances to re-enable caching."
                )
            else:
                self._embedder_broken = True
                logger.warning("Vector store init failed (%s) — cache disabled.", exc)
            return None

    # ── Embedder (lazy, fault-tolerant) ───────────────────────────────────────

    def _get_embedder(self):
        if self._embedder_broken:
            return None
        if self._embedder is not None:
            return self._embedder
        try:
            import os, warnings
            # Tell transformers NOT to load TensorFlow (use PyTorch only)
            os.environ["USE_TF"] = "0"
            os.environ["TRANSFORMERS_NO_TF"] = "1"
            os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
            os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
            warnings.filterwarnings("ignore")
            from sentence_transformers import SentenceTransformer
            self._embedder = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Sentence-transformer embedder loaded.")
            return self._embedder
        except Exception as exc:
            self._embedder_broken = True
            logger.warning(
                "Vector cache DISABLED — could not load sentence_transformers: %s. "
                "The pipeline will still work; semantic caching is simply off.",
                type(exc).__name__,
            )
            return None

    def _embed(self, text: str) -> Optional[list]:
        embedder = self._get_embedder()
        if embedder is None:
            return None
        try:
            return embedder.encode(text).tolist()
        except Exception as exc:
            logger.warning("Embedding failed: %s", exc)
            self._embedder_broken = True
            return None

    # ── Public API ────────────────────────────────────────────────────────────

    def search_canonical_sql(self, intent: str) -> Optional[str]:
        """Return cached SQL if a close semantic match exists, else None."""
        if self._embedder_broken or self._table is None:
            return None
        try:
            if self._table.count_rows() == 0:
                return None
            vector = self._embed(intent)
            if vector is None:
                return None
            results = self._table.search(vector).limit(1).to_list()
            if not results:
                return None
            top = results[0]
            distance = top.get("_distance", float("inf"))
            if distance < (1.0 - self.similarity_threshold):
                logger.info("Vector cache HIT (dist=%.3f) for: '%s'", distance, intent)
                return top["sql"]
            logger.debug("Vector cache MISS (dist=%.3f).", distance)
            return None
        except Exception as exc:
            logger.warning("Vector search error (cache disabled): %s", exc)
            self._embedder_broken = True
            return None

    def save_canonical_sql(self, intent: str, sql: str) -> None:
        """Embed and store intent → SQL. No-op if embedder is unavailable."""
        if self._embedder_broken or self._table is None:
            return
        try:
            if self.search_canonical_sql(intent) is not None:
                return  # already cached
            vector = self._embed(intent)
            if vector is None:
                return
            self._table.add([{"intent": intent, "sql": sql, "vector": vector, "frequency": 1}])
            logger.info("Vector cache SAVED: '%s'", intent)
        except Exception as exc:
            logger.warning("Vector save error: %s", exc)
