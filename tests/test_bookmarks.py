"""tests/test_bookmarks.py — unit tests for BookmarkStore."""
import pytest
from pathlib import Path
from nexus_data.kb.bookmarks import BookmarkStore


@pytest.fixture
def store(tmp_path):
    return BookmarkStore(kb_dir=tmp_path)


def test_save_and_get(store):
    store.save("myquery", "show all users", "SELECT * FROM users LIMIT 100")
    result = store.get("myquery")
    assert result is not None
    query, sql = result
    assert query == "show all users"
    assert "SELECT" in sql


def test_get_nonexistent_returns_none(store):
    assert store.get("does_not_exist") is None


def test_case_insensitive_lookup(store):
    store.save("MyQuery", "test query", "SELECT 1")
    assert store.get("myquery") is not None
    assert store.get("MYQUERY") is not None


def test_delete_existing(store):
    store.save("todelete", "q", "SELECT 1")
    assert store.delete("todelete") is True
    assert store.get("todelete") is None


def test_delete_nonexistent_returns_false(store):
    assert store.delete("nonexistent") is False


def test_list_all_sorted(store):
    store.save("zzz", "q3", "SELECT 3")
    store.save("aaa", "q1", "SELECT 1")
    store.save("mmm", "q2", "SELECT 2")
    items = store.list_all()
    names = [n for n, _ in items]
    assert names == sorted(names)


def test_overwrite_existing(store):
    store.save("dup", "old query", "SELECT 1")
    store.save("dup", "new query", "SELECT 2")
    _, sql = store.get("dup")
    assert sql == "SELECT 2"


def test_persistence(tmp_path):
    """Data survives across store instances (same dir)."""
    s1 = BookmarkStore(kb_dir=tmp_path)
    s1.save("persistent", "my query", "SELECT 99")
    s2 = BookmarkStore(kb_dir=tmp_path)
    assert s2.get("persistent") is not None
