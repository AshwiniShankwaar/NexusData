"""
nexus_data/auth/models.py
Local SQLite auth + conversation storage (no external services needed).
"""
from __future__ import annotations
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)

_AUTH_DB = Path(os.getenv("NEXUS_AUTH_DB", "./nexus_auth.db"))


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    db_path = Path(os.getenv("NEXUS_AUTH_DB", str(_AUTH_DB)))
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")  # safe for concurrent reads
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    """Create all tables if they don't exist."""
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                email       TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                settings    TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS db_connections (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL REFERENCES users(id),
                name        TEXT NOT NULL,
                uri_encrypted TEXT NOT NULL,
                dialect     TEXT DEFAULT '',
                db_metadata TEXT DEFAULT '{}',
                created_at  TEXT NOT NULL,
                UNIQUE(user_id, name)
            );

            CREATE TABLE IF NOT EXISTS conversations (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL REFERENCES users(id),
                db_conn_id  TEXT REFERENCES db_connections(id),
                title       TEXT NOT NULL DEFAULT 'New Chat',
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id          TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL REFERENCES conversations(id),
                role        TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
                content     TEXT NOT NULL,
                sql_used    TEXT DEFAULT '',
                phase_timings TEXT DEFAULT '{}',
                result_json TEXT DEFAULT 'null',
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS feedback (
                id          TEXT PRIMARY KEY,
                message_id  TEXT NOT NULL REFERENCES messages(id),
                user_id     TEXT NOT NULL REFERENCES users(id),
                rating      TEXT NOT NULL CHECK(rating IN ('up', 'down')),
                created_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL REFERENCES users(id),
                token_hash  TEXT NOT NULL UNIQUE,
                created_at  TEXT NOT NULL,
                expires_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_keys (
                id          TEXT PRIMARY KEY,
                user_id     TEXT NOT NULL REFERENCES users(id),
                name        TEXT NOT NULL,
                key_hash    TEXT NOT NULL UNIQUE,
                key_preview TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_conversations_user ON conversations(user_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
        """)
        # Safe migrations for columns added after initial schema
        for migration in [
            "ALTER TABLE db_connections ADD COLUMN db_metadata TEXT DEFAULT '{}'",
            "ALTER TABLE messages ADD COLUMN result_json TEXT DEFAULT 'null'",
        ]:
            try:
                con.execute(migration)
                con.commit()
            except Exception:
                pass  # Column already exists
    logger.info("Auth DB initialised at %s", _AUTH_DB)


# ── User CRUD ─────────────────────────────────────────────────────────────────

def create_user(name: str, email: str, password_hash: str) -> Dict[str, Any]:
    uid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO users (id,name,email,password_hash,created_at) VALUES (?,?,?,?,?)",
            (uid, name, email.lower().strip(), password_hash, now),
        )
    return {"id": uid, "name": name, "email": email, "created_at": now}


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            "SELECT id,name,email,password_hash,created_at,settings FROM users WHERE email=?",
            (email.lower().strip(),),
        ).fetchone()
    return dict(row) if row else None


def list_all_users() -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute("SELECT id,name,email FROM users").fetchall()
    return [dict(r) for r in rows]


def get_user_by_id(uid: str) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            "SELECT id,name,email,created_at,settings FROM users WHERE id=?", (uid,)
        ).fetchone()
    return dict(row) if row else None


def update_user_settings(uid: str, settings: Dict[str, Any]) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE users SET settings=? WHERE id=?",
            (json.dumps(settings), uid),
        )


def update_password(uid: str, new_hash: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE users SET password_hash=? WHERE id=?",
            (new_hash, uid),
        )


# ── DB Connections ─────────────────────────────────────────────────────────────

def save_db_connection(user_id: str, name: str, uri: str, dialect: str = "") -> str:
    cid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            """INSERT INTO db_connections (id,user_id,name,uri_encrypted,dialect,created_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(user_id,name) DO UPDATE SET uri_encrypted=excluded.uri_encrypted,
               dialect=excluded.dialect""",
            (cid, user_id, name, uri, dialect, now),
        )
    return cid


def list_db_connections(user_id: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id,name,uri_encrypted,dialect,created_at FROM db_connections WHERE user_id=? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_db_connection(user_id: str, conn_id: str) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            "SELECT id,name,uri_encrypted,dialect,db_metadata,created_at FROM db_connections WHERE id=? AND user_id=?",
            (conn_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def update_db_metadata(user_id: str, conn_id: str, metadata: Dict[str, Any]) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE db_connections SET db_metadata=? WHERE id=? AND user_id=?",
            (json.dumps(metadata), conn_id, user_id),
        )


def delete_db_connection(user_id: str, conn_id: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM db_connections WHERE id=? AND user_id=?",
            (conn_id, user_id),
        )


def update_db_connection(user_id: str, conn_id: str, name: Optional[str] = None, uri: Optional[str] = None) -> None:
    if not name and not uri:
        return
    with _conn() as con:
        if name and uri:
            con.execute(
                "UPDATE db_connections SET name=?, uri_encrypted=? WHERE id=? AND user_id=?",
                (name, uri, conn_id, user_id),
            )
        elif name:
            con.execute(
                "UPDATE db_connections SET name=? WHERE id=? AND user_id=?",
                (name, conn_id, user_id),
            )
        else:
            con.execute(
                "UPDATE db_connections SET uri_encrypted=? WHERE id=? AND user_id=?",
                (uri, conn_id, user_id),
            )


# ── API Keys ───────────────────────────────────────────────────────────────────

def create_api_key(user_id: str, name: str) -> Tuple[str, str, str]:
    """Generate a new API key. Returns (full_key, preview, id)."""
    full_key = f"sk-nexus-{secrets.token_urlsafe(24)}"
    key_hash = hashlib.sha256(full_key.encode()).hexdigest()
    preview = full_key[:12] + "..."
    kid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO api_keys (id,user_id,name,key_hash,key_preview,created_at) VALUES (?,?,?,?,?,?)",
            (kid, user_id, name, key_hash, preview, now),
        )
    return full_key, preview, kid


def list_api_keys(user_id: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id,name,key_preview,created_at FROM api_keys WHERE user_id=? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_api_key(user_id: str, key_id: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM api_keys WHERE id=? AND user_id=?",
            (key_id, user_id),
        )


# ── Conversations ──────────────────────────────────────────────────────────────

def create_conversation(user_id: str, db_conn_id: Optional[str] = None, title: str = "New Chat") -> Dict[str, Any]:
    cid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO conversations (id,user_id,db_conn_id,title,created_at,updated_at) VALUES (?,?,?,?,?,?)",
            (cid, user_id, db_conn_id, title, now, now),
        )
    return {"id": cid, "user_id": user_id, "db_conn_id": db_conn_id, "title": title, "created_at": now}


def list_conversations(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id,title,db_conn_id,created_at,updated_at FROM conversations WHERE user_id=? ORDER BY updated_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_conversation(user_id: str, conv_id: str) -> Optional[Dict[str, Any]]:
    with _conn() as con:
        row = con.execute(
            "SELECT id,title,db_conn_id,created_at,updated_at FROM conversations WHERE id=? AND user_id=?",
            (conv_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def update_conversation_title(user_id: str, conv_id: str, title: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "UPDATE conversations SET title=?,updated_at=? WHERE id=? AND user_id=?",
            (title, now, conv_id, user_id),
        )


def touch_conversation(conv_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute("UPDATE conversations SET updated_at=? WHERE id=?", (now, conv_id))


def delete_conversation(user_id: str, conv_id: str) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM messages WHERE conversation_id=?", (conv_id,)
        )
        con.execute(
            "DELETE FROM conversations WHERE id=? AND user_id=?", (conv_id, user_id)
        )


# ── Messages ───────────────────────────────────────────────────────────────────

def save_message(
    conv_id: str,
    role: str,
    content: str,
    sql_used: str = "",
    phase_timings: Optional[Dict[str, float]] = None,
    result_json: Optional[str] = None,
) -> str:
    mid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO messages (id,conversation_id,role,content,sql_used,phase_timings,result_json,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (mid, conv_id, role, content, sql_used, json.dumps(phase_timings or {}), result_json or "null", now),
        )
    touch_conversation(conv_id)
    return mid


def list_messages(conv_id: str) -> List[Dict[str, Any]]:
    with _conn() as con:
        rows = con.execute(
            "SELECT id,role,content,sql_used,phase_timings,result_json,created_at FROM messages WHERE conversation_id=? ORDER BY created_at ASC",
            (conv_id,),
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["phase_timings"] = json.loads(d["phase_timings"] or "{}")
        except Exception:
            d["phase_timings"] = {}
        try:
            d["result_json"] = json.loads(d["result_json"] or "null")
        except Exception:
            d["result_json"] = None
        result.append(d)
    return result


# ── Feedback ───────────────────────────────────────────────────────────────────

def save_feedback(message_id: str, user_id: str, rating: str) -> None:
    fid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            """INSERT INTO feedback (id,message_id,user_id,rating,created_at) VALUES (?,?,?,?,?)
               ON CONFLICT DO NOTHING""",
            (fid, message_id, user_id, rating, now),
        )


# ── Sessions ───────────────────────────────────────────────────────────────────

def create_session(user_id: str, token_hash: str, expires_at: str) -> None:
    """Store a new session record. Also purges expired sessions for this user."""
    sid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        # Clean up expired sessions for this user first
        con.execute(
            "DELETE FROM sessions WHERE user_id=? AND expires_at < ?",
            (user_id, now),
        )
        con.execute(
            "INSERT INTO sessions (id,user_id,token_hash,created_at,expires_at) VALUES (?,?,?,?,?)",
            (sid, user_id, token_hash, now, expires_at),
        )


def get_session(token_hash: str) -> Optional[Dict[str, Any]]:
    """Return session row if it exists and has not expired, else None."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        row = con.execute(
            "SELECT id,user_id,created_at,expires_at FROM sessions WHERE token_hash=? AND expires_at > ?",
            (token_hash, now),
        ).fetchone()
    return dict(row) if row else None


def delete_session(token_hash: str) -> None:
    """Revoke a session (logout)."""
    with _conn() as con:
        con.execute("DELETE FROM sessions WHERE token_hash=?", (token_hash,))
