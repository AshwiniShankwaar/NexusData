"""
nexus_data/kb/vector_repo.py
Numpy-backed semantic cache for Intent → SQL mapping.
Works on all platforms (replaces lancedb which has no Windows wheels).
Gracefully degrades if sentence_transformers cannot load.
Adaptive threshold: raises similarity bar when correction rate exceeds 30%.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 384


class _TFIDFEmbedder:
    """Lightweight TF-IDF embedder used when sentence_transformers is unavailable.
    Produces L2-normalised dense vectors via sklearn's TfidfVectorizer.
    Automatically refits when new intents are added."""

    def __init__(self) -> None:
        from sklearn.feature_extraction.text import TfidfVectorizer
        self._vec = TfidfVectorizer(
            analyzer="char_wb", ngram_range=(2, 4), max_features=EMBEDDING_DIM,
            sublinear_tf=True,
        )
        self._fitted = False

    def fit(self, texts: List[str]) -> None:
        if texts:
            self._vec.fit(texts)
            self._fitted = True

    def encode(self, text: str) -> List[float]:
        import numpy as np
        if not self._fitted:
            # Return a zero vector — will produce a miss (sim=0 < threshold)
            return [0.0] * EMBEDDING_DIM
        vec = self._vec.transform([text]).toarray()[0]
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        # Pad or truncate to EMBEDDING_DIM
        if len(vec) < EMBEDDING_DIM:
            vec = list(vec) + [0.0] * (EMBEDDING_DIM - len(vec))
        return list(vec[:EMBEDDING_DIM])
_THRESHOLD_MIN = 0.80
_THRESHOLD_MAX = 0.98
_CORRECTION_RATE_LIMIT = 0.30   # raise threshold when corrections exceed this
_THRESHOLD_STEP = 0.02


class VectorQueryRepo:
    """Semantic cache. Falls back to a no-op if the embedder cannot be loaded."""

    def __init__(self, db_dir: Optional[Path] = None, similarity_threshold: float = 0.90):
        self._db_path = db_dir or Path("./nexus_kb/vector_store")
        self._db_path.mkdir(parents=True, exist_ok=True)
        self._cache_file = self._db_path / "query_cache.json"
        self._stats_path = self._db_path / "cache_stats.json"
        self._stats = self._load_stats(similarity_threshold)
        self.similarity_threshold = self._stats["threshold"]
        self._embedder = None
        self._embedder_broken = False
        self._records: List[Dict[str, Any]] = self._load_records()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load_records(self) -> List[Dict[str, Any]]:
        if self._cache_file.exists():
            try:
                return json.loads(self._cache_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return []

    def _save_records(self) -> None:
        try:
            self._cache_file.write_text(
                json.dumps(self._records, indent=None), encoding="utf-8"
            )
        except Exception as exc:
            logger.warning("Could not save vector cache: %s", exc)

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

    # ── Embedder (lazy, fault-tolerant) ───────────────────────────────────────

    def _get_embedder(self):
        if self._embedder_broken:
            return None
        if self._embedder is not None:
            return self._embedder

        # 1. Try sentence-transformers (transformer-quality embeddings)
        try:
            import os, warnings
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
            logger.debug("sentence_transformers unavailable (%s), trying TF-IDF fallback.", type(exc).__name__)

        # 2. Fallback: sklearn TF-IDF (works on all platforms, no DLL dependencies)
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            self._embedder = _TFIDFEmbedder()
            # Load existing intents if available (so the vectorizer can fit)
            if self._records:
                intents = [r["intent"] for r in self._records]
                self._embedder.fit(intents)
            logger.info("TF-IDF fallback embedder loaded (sentence_transformers unavailable).")
            return self._embedder
        except Exception as exc2:
            self._embedder_broken = True
            logger.warning(
                "Vector cache DISABLED — no embedder available (sentence_transformers: DLL error, "
                "sklearn: %s). The pipeline will still work; semantic caching is simply off.",
                type(exc2).__name__,
            )
            return None

    def _embed(self, text: str) -> Optional[list]:
        embedder = self._get_embedder()
        if embedder is None:
            return None
        try:
            result = embedder.encode(text)
            if hasattr(result, "tolist"):
                return result.tolist()
            return list(result)
        except Exception as exc:
            logger.warning("Embedding failed: %s", exc)
            self._embedder_broken = True
            return None

    # ── Public API ────────────────────────────────────────────────────────────

    def search_canonical_sql(self, intent: str) -> Optional[str]:
        """Return cached SQL if a close semantic match exists, else None."""
        if self._embedder_broken or not self._records:
            return None
        vector = self._embed(intent)
        if vector is None:
            return None
        try:
            import numpy as np
            q = np.array(vector, dtype="float32")
            q_norm = np.linalg.norm(q)
            if q_norm == 0:
                return None
            best_sim = -1.0
            best_sql: Optional[str] = None
            for rec in self._records:
                v = np.array(rec["vector"], dtype="float32")
                v_norm = np.linalg.norm(v)
                if v_norm == 0:
                    continue
                sim = float(np.dot(q, v) / (q_norm * v_norm))
                if sim > best_sim:
                    best_sim = sim
                    best_sql = rec["sql"]
            if best_sim >= self.similarity_threshold:
                logger.info("Vector cache HIT (sim=%.3f) for: '%s'", best_sim, intent)
                return best_sql
            logger.debug("Vector cache MISS (sim=%.3f).", best_sim)
            return None
        except Exception as exc:
            logger.warning("Vector search error (cache disabled): %s", exc)
            self._embedder_broken = True
            return None

    def save_canonical_sql(self, intent: str, sql: str) -> None:
        """Embed and store intent → SQL. No-op if embedder is unavailable."""
        if self._embedder_broken:
            return
        if self.search_canonical_sql(intent) is not None:
            return  # already cached

        # If using TF-IDF, refit on all known intents before encoding the new one
        embedder = self._get_embedder()
        if isinstance(embedder, _TFIDFEmbedder) and self._records:
            embedder.fit([r["intent"] for r in self._records] + [intent])

        vector = self._embed(intent)
        if vector is None:
            return
        self._records.append({"intent": intent, "sql": sql, "vector": vector})
        self._save_records()
        logger.info("Vector cache SAVED: '%s'", intent)
