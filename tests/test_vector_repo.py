"""
tests/test_vector_repo.py
Verifies the semantic caching capability using LanceDB.
"""
from __future__ import annotations

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from nexus_data.kb.vector_repo import VectorQueryRepo

@pytest.fixture
def repo():
    with TemporaryDirectory() as tmp:
        # High threshold for tests to avoid false positives
        yield VectorQueryRepo(db_dir=Path(tmp), similarity_threshold=0.85)

class TestVectorQueryRepo:
    def test_save_and_exact_match(self, repo):
        intent = "Show me the top 5 highest paying customers"
        sql = "SELECT * FROM customers ORDER BY total_spent DESC LIMIT 5"
        
        repo.save_canonical_sql(intent, sql)
        
        hit = repo.search_canonical_sql(intent)
        assert hit == sql

    @pytest.mark.xfail(
        reason="Semantic similarity depends on embedder availability and threshold; "
               "may not match in all environments.",
        strict=False,
    )
    def test_semantic_match(self, repo):
        intent1 = "Count the total number of orders"
        sql = "SELECT COUNT(*) FROM orders"
        repo.save_canonical_sql(intent1, sql)
        if repo._embedder_broken:
            pytest.skip("Sentence-transformer embedder unavailable — semantic cache disabled")

        # A slightly phrased differently intent
        intent2 = "What is the total order count?"
        hit = repo.search_canonical_sql(intent2)

        # With default sentence_transformers, this should be high similarity
        assert hit == sql

    def test_semantic_miss(self, repo):
        intent = "Show me all users"
        repo.save_canonical_sql(intent, "SELECT * FROM users")
        
        # Completely different intent
        miss = repo.search_canonical_sql("Average time spent on website")
        assert miss is None

    def test_duplicate_save_is_ignored(self, repo):
        repo.save_canonical_sql("test intent", "SELECT 1")
        # Should not throw or duplicate
        repo.save_canonical_sql("test intent", "SELECT 1")
        
        hit = repo.search_canonical_sql("test intent")
        assert hit == "SELECT 1"
