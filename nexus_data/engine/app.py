"""
nexus_data/engine/app.py — NexusData FastAPI Service
Local-first: auth via SQLite+JWT, per-user conversations, SSE phase streaming.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()  # load .env before anything else

import shutil
from fastapi import Depends, FastAPI, File, HTTPException, Request, Security, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from nexus_data.auth import models as auth_db
from nexus_data.auth.manager import create_access_token, decode_token, hash_password, verify_password, revoke_token
from nexus_data.core.env_writer import ensure_secret_key
from nexus_data.core.config_manager import ConfigManager, _safe_name
from nexus_data.core.request_logger import log_pipeline_request
from nexus_data.models import QueryResult
from nexus_data.orchestrator import NexusData

logger = logging.getLogger(__name__)

# ── .env loaded above ─────────────────────────────────────────────────────────

_SECRET_KEY = os.getenv("NEXUS_SECRET_KEY", "dev-secret-change-me")
_CORS_ORIGINS = [o.strip() for o in os.getenv("NEXUS_CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",")]
_RATE_LIMIT = int(os.getenv("NEXUS_RATE_LIMIT", "60"))
_DATA_DIR = Path(os.getenv("NEXUS_DATA_DIR", "./data"))

# ── Rate limiter ──────────────────────────────────────────────────────────────
_rate_window: Dict[str, List[float]] = defaultdict(list)
_MAX_RATE_WINDOW_IPS = 10_000


async def _rate_limit(request: Request) -> None:
    ip = request.client.host if request.client else "unknown"
    now = time.monotonic()
    _rate_window[ip] = [t for t in _rate_window[ip] if now - t < 60.0]
    if len(_rate_window[ip]) >= _RATE_LIMIT:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded ({_RATE_LIMIT} req/min).")
    _rate_window[ip].append(now)
    # Evict old IPs to prevent memory leak
    if len(_rate_window) > _MAX_RATE_WINDOW_IPS:
        oldest = sorted(_rate_window, key=lambda k: max(_rate_window[k]) if _rate_window[k] else 0)[:1000]
        for k in oldest:
            del _rate_window[k]


# ── JWT Auth ──────────────────────────────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)


async def _get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> Dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing Authorization header.")
    user_id = decode_token(credentials.credentials)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired token.")
    user = auth_db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found.")
    return user


# ── Per-user NexusData instances ───────────────────────────────────────────────
# key: (user_id, conv_id) → NexusData instance
_instances: Dict[str, NexusData] = {}
_instances_lock = asyncio.Lock()


def _kb_dir_for(user_id: str, conv_id: str) -> Path:
    base = Path(os.getenv("NEXUS_KB_DIR", "./nexus_kb"))
    return base / user_id / conv_id


async def _get_or_create_instance(user_id: str, conv_id: str, db_uri: Optional[str] = None) -> NexusData:
    key = f"{user_id}:{conv_id}"
    async with _instances_lock:
        if key not in _instances:
            cfg = ConfigManager()
            kb_dir = _kb_dir_for(user_id, conv_id)
            nd = NexusData(kb_dir=kb_dir, interactive_setup=False, config_manager=cfg)
            _instances[key] = nd
        nd = _instances[key]
        # Connect (or reconnect) if engine is missing and we have a URI
        if db_uri and nd._engine is None:
            await asyncio.to_thread(nd.connect_and_initialize, db_uri, False)
        return nd


def _sync_config_for_user(user_id: str, cfg: Optional[ConfigManager] = None) -> None:
    """
    Sync config.json databases → nexus_auth.db for a single user.
    Skips entries already present by either their raw name or safe-keyed name
    to avoid duplicates when the same DB was added via CLI (safe key) vs UI (raw name).
    """
    try:
        if cfg is None:
            cfg = ConfigManager()
        if not cfg.config.databases:
            return
        conns = auth_db.list_db_connections(user_id)
        # Accept both the stored name and its safe version as "already exists"
        existing = {c["name"] for c in conns} | {_safe_name(c["name"]) for c in conns}
        for db_name, db_cfg in cfg.config.databases.items():
            if db_name not in existing and db_cfg.uri:
                dialect = db_cfg.uri.split("://")[0] if "://" in db_cfg.uri else ""
                auth_db.save_db_connection(user_id, db_name, db_cfg.uri, dialect)
                logger.info("Synced CLI connection '%s' → auth DB for user %s", db_name, user_id)
    except Exception as exc:
        logger.warning("Per-user config sync failed (non-fatal): %s", exc)


def _sync_config_to_authdb() -> None:
    """
    Startup sync: run _sync_config_for_user for every registered user.
    Also called lazily from GET /v1/connections so CLI-added DBs appear
    in the UI without requiring a server restart.
    """
    try:
        cfg = ConfigManager()
        users = auth_db.list_all_users()
        for user in users:
            _sync_config_for_user(user["id"], cfg)
    except Exception as exc:
        logger.warning("Config→AuthDB sync failed (non-fatal): %s", exc)


# ── Lifespan ───────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Auto-generate secret key on first boot before anything reads it
    ensure_secret_key()
    auth_db.init_db()
    _sync_config_to_authdb()
    logger.info("NexusData API started.")
    yield
    for nd in _instances.values():
        nd.stop_background_refresh()
    logger.info("NexusData API shut down.")


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="NexusData",
    description="Local-first Natural Language Data Interface",
    version="0.4.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
    allow_credentials=True,
)


# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "version": app.version}


# ── System Status ──────────────────────────────────────────────────────────────
@app.get("/v1/system/status", tags=["System"])
async def system_status():
    """Check if the app has been configured via CLI (has LLM config)."""
    cfg = ConfigManager()
    return {
        "configured": cfg.is_configured(),
        "has_llm": bool(cfg.config.llm.api_key or cfg.config.llm.provider == "ollama"),
    }


# ── Auth Endpoints ─────────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., min_length=5, max_length=200)
    password: str = Field(..., min_length=8, max_length=200)


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/auth/register", tags=["Auth"])
async def register(req: RegisterRequest):
    existing = auth_db.get_user_by_email(req.email)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered.")
    pw_hash = hash_password(req.password)
    user = auth_db.create_user(req.name, req.email, pw_hash)
    token, expires_at = create_access_token(user["id"])
    return {"token": token, "expires_at": expires_at, "user": {"id": user["id"], "name": user["name"], "email": user["email"]}}


@app.post("/auth/login", tags=["Auth"])
async def login(req: LoginRequest):
    user = auth_db.get_user_by_email(req.email)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token, expires_at = create_access_token(user["id"])
    settings = {}
    try:
        settings = json.loads(user.get("settings") or "{}")
    except Exception:
        pass
    return {
        "token": token,
        "expires_at": expires_at,
        "user": {"id": user["id"], "name": user["name"], "email": user["email"], "settings": settings},
    }


@app.get("/auth/me", tags=["Auth"])
async def me(user: Dict[str, Any] = Depends(_get_current_user)):
    settings = {}
    try:
        settings = json.loads(user.get("settings") or "{}")
    except Exception:
        pass
    return {"id": user["id"], "name": user["name"], "email": user["email"], "settings": settings}


class UpdateProfileRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    settings: Optional[Dict[str, Any]] = None


@app.patch("/auth/me", tags=["Auth"])
async def update_profile(req: UpdateProfileRequest, user: Dict[str, Any] = Depends(_get_current_user)):
    if req.settings is not None:
        auth_db.update_user_settings(user["id"], req.settings)
    return {"status": "updated"}


@app.post("/auth/logout", tags=["Auth"])
async def logout(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
):
    """Revoke the current session token. The JWT becomes invalid immediately."""
    if credentials:
        revoke_token(credentials.credentials)
    return {"status": "logged_out"}


# ── DB Connections ─────────────────────────────────────────────────────────────

class DBConnectRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    uri: str = Field(..., min_length=5, max_length=500)


@app.get("/v1/connections", tags=["Connections"])
async def list_connections(user: Dict[str, Any] = Depends(_get_current_user)):
    # Sync CLI-configured DBs so they appear in the UI without a server restart
    _sync_config_for_user(user["id"])
    conns = auth_db.list_db_connections(user["id"])
    # Mask passwords in URIs
    masked = []
    for c in conns:
        uri = c["uri_encrypted"]
        masked_uri = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
        masked.append({**c, "uri_masked": masked_uri, "uri_encrypted": None})
    return {"connections": masked}


@app.post("/v1/connections", tags=["Connections"])
async def add_connection(req: DBConnectRequest, user: Dict[str, Any] = Depends(_get_current_user)):
    # Validate URI scheme
    allowed = ("sqlite", "postgresql", "postgres", "mysql", "mssql", "duckdb")
    scheme = req.uri.split("://")[0].lower() if "://" in req.uri else ""
    if scheme not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported DB scheme '{scheme}'. Allowed: {allowed}")
    conn_id = auth_db.save_db_connection(user["id"], req.name, req.uri)
    # Also persist to config.json so CLI sessions see it
    try:
        from nexus_data.core.config_manager import DBConfig
        cfg = ConfigManager()
        cfg.add_database(req.name, DBConfig(uri=req.uri))
    except Exception as exc:
        logger.warning("Could not sync new connection to config.json: %s", exc)
    return {"id": conn_id, "name": req.name, "status": "saved"}


@app.delete("/v1/connections/{conn_id}", tags=["Connections"])
async def delete_connection(conn_id: str, user: Dict[str, Any] = Depends(_get_current_user)):
    # Look up name before deleting so we can remove from config.json too
    conn = auth_db.get_db_connection(user["id"], conn_id)
    auth_db.delete_db_connection(user["id"], conn_id)
    if conn:
        try:
            cfg = ConfigManager()
            key = _safe_name(conn["name"])
            if key in cfg.config.databases:
                del cfg.config.databases[key]
                if cfg.config.active_db_name == key:
                    cfg.config.active_db_name = next(iter(cfg.config.databases), "")
                cfg.save()
        except Exception as exc:
            logger.warning("Could not remove connection from config.json: %s", exc)
    return {"status": "deleted"}


@app.get("/v1/connections/{conn_id}", tags=["Connections"])
async def get_connection(conn_id: str, user: Dict[str, Any] = Depends(_get_current_user)):
    conn = auth_db.get_db_connection(user["id"], conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found.")
    uri = conn["uri_encrypted"]
    masked_uri = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
    meta = {}
    try:
        meta = json.loads(conn.get("db_metadata") or "{}")
    except Exception:
        pass
    return {**conn, "uri_masked": masked_uri, "uri_encrypted": None, "db_metadata": meta}


class DBMetadataRequest(BaseModel):
    table_descriptions: Optional[Dict[str, str]] = None
    column_descriptions: Optional[Dict[str, Dict[str, str]]] = None
    notes: Optional[str] = None


@app.patch("/v1/connections/{conn_id}/metadata", tags=["Connections"])
async def update_connection_metadata(
    conn_id: str,
    req: DBMetadataRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Update table/column descriptions and notes for a DB connection."""
    conn = auth_db.get_db_connection(user["id"], conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found.")
    existing = {}
    try:
        existing = json.loads(conn.get("db_metadata") or "{}")
    except Exception:
        pass
    if req.table_descriptions is not None:
        existing["table_descriptions"] = req.table_descriptions
    if req.column_descriptions is not None:
        existing["column_descriptions"] = req.column_descriptions
    if req.notes is not None:
        existing["notes"] = req.notes
    auth_db.update_db_metadata(user["id"], conn_id, existing)
    return {"status": "updated"}


@app.post("/v1/connections/{conn_id}/upload", tags=["Connections"])
async def upload_connection_file(
    conn_id: str,
    file: UploadFile = File(...),
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Upload a schema description file (.md, .txt, .json) for a DB connection."""
    conn = auth_db.get_db_connection(user["id"], conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found.")
    # Only allow safe file types
    allowed_exts = {".md", ".txt", ".json", ".yaml", ".yml"}
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in allowed_exts:
        raise HTTPException(status_code=400, detail=f"File type '{suffix}' not allowed. Use: {allowed_exts}")
    upload_dir = _DATA_DIR / "uploads" / conn_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    dest = upload_dir / (Path(file.filename).name)
    content = await file.read()
    dest.write_bytes(content)
    return {"status": "uploaded", "filename": dest.name, "path": str(dest)}


# ── Conversations ──────────────────────────────────────────────────────────────

class NewConversationRequest(BaseModel):
    db_conn_id: str = Field(..., min_length=1)
    title: str = "New Chat"


@app.get("/v1/conversations", tags=["Conversations"])
async def list_conversations(user: Dict[str, Any] = Depends(_get_current_user)):
    return {"conversations": auth_db.list_conversations(user["id"])}


@app.post("/v1/conversations", tags=["Conversations"])
async def new_conversation(req: NewConversationRequest, user: Dict[str, Any] = Depends(_get_current_user)):
    # Verify the connection belongs to this user
    conn = auth_db.get_db_connection(user["id"], req.db_conn_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Database connection not found.")
    conv = auth_db.create_conversation(user["id"], req.db_conn_id, req.title)
    return conv


@app.get("/v1/conversations/{conv_id}/messages", tags=["Conversations"])
async def get_messages(conv_id: str, user: Dict[str, Any] = Depends(_get_current_user)):
    conv = auth_db.get_conversation(user["id"], conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return {"messages": auth_db.list_messages(conv_id)}


@app.patch("/v1/conversations/{conv_id}", tags=["Conversations"])
async def rename_conversation(conv_id: str, body: Dict[str, str], user: Dict[str, Any] = Depends(_get_current_user)):
    title = body.get("title", "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required.")
    auth_db.update_conversation_title(user["id"], conv_id, title)
    return {"status": "updated"}


@app.delete("/v1/conversations/{conv_id}", tags=["Conversations"])
async def delete_conversation(conv_id: str, user: Dict[str, Any] = Depends(_get_current_user)):
    auth_db.delete_conversation(user["id"], conv_id)
    return {"status": "deleted"}


@app.delete("/v1/memory", tags=["Conversations"])
async def clear_all_memory(user: Dict[str, Any] = Depends(_get_current_user)):
    """Delete ALL conversations and exported data files for this user. Does NOT remove the knowledge base."""
    convs = auth_db.list_conversations(user["id"])
    for conv in convs:
        cid = conv["id"]
        auth_db.delete_conversation(user["id"], cid)
        # Remove exported data files for this conversation
        conv_data_dir = _DATA_DIR / cid
        if conv_data_dir.exists():
            shutil.rmtree(conv_data_dir, ignore_errors=True)
        # Remove the in-memory instance
        key = f"{user['id']}:{cid}"
        _instances.pop(key, None)
    return {"status": "cleared", "conversations_deleted": len(convs)}


# ── SSE Ask (streaming phase updates) ─────────────────────────────────────────

class AskRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    db_uri: Optional[str] = None   # URI to connect if not yet connected


def _sse_event(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@app.post("/v1/conversations/{conv_id}/ask", tags=["Query"])
async def ask_stream(
    conv_id: str,
    req: AskRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """SSE endpoint — streams phase events then the final result."""
    conv = auth_db.get_conversation(user["id"], conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    # Resolve DB URI: explicit > connection record > error
    db_uri = req.db_uri
    if not db_uri and conv.get("db_conn_id"):
        conns = auth_db.list_db_connections(user["id"])
        for c in conns:
            if c["id"] == conv["db_conn_id"]:
                db_uri = c["uri_encrypted"]
                break

    # Save user message
    auth_db.save_message(conv_id, "user", req.query)

    async def event_generator():
        phase_timings: Dict[str, float] = {}
        t0 = time.monotonic()

        try:
            nd = await _get_or_create_instance(user["id"], conv_id, db_uri)

            # Yield phases as they happen via queue
            import queue as _queue
            phase_q: _queue.Queue = _queue.Queue()

            def cb(phase: str, elapsed_ms: float):
                phase_timings[phase] = elapsed_ms
                phase_q.put((phase, elapsed_ms))

            # Run pipeline in thread; emit phase events as they arrive
            result_holder: Dict[str, Any] = {}

            def run_pipeline():
                try:
                    result_holder["result"] = nd.ask(req.query, phase_callback=cb)
                except Exception as exc:
                    result_holder["error"] = str(exc)

            import threading
            t = threading.Thread(target=run_pipeline, daemon=True)
            t.start()

            # Stream phase events
            while t.is_alive() or not phase_q.empty():
                try:
                    phase, elapsed = phase_q.get(timeout=0.1)
                    yield _sse_event("phase", {"phase": phase, "elapsed_ms": elapsed})
                except _queue.Empty:
                    await asyncio.sleep(0.05)

            t.join()

            if "error" in result_holder:
                yield _sse_event("error", {"message": result_holder["error"]})
                return

            result: QueryResult = result_holder["result"]
            result_dict = result.model_dump()

            # Save assistant message (include rows+columns so history can re-render tables)
            result_json = json.dumps({
                "rows": result.rows,
                "columns": result.columns,
                "error": result.error,
                "anomaly_warnings": result.anomaly_warnings,
                "performance_hints": result.performance_hints,
                "is_clarification": result.is_clarification,
                "clarification_question": result.clarification_question,
            })
            msg_id = auth_db.save_message(
                conv_id, "assistant",
                result.natural_language_summary or result.error or "",
                sql_used=result.sql or "",
                phase_timings=phase_timings,
                result_json=result_json,
            )
            result_dict["message_id"] = msg_id

            # Structured request log (UI source)
            log_pipeline_request(
                source="UI",
                query=req.query,
                result_sql=result.sql or None,
                result_rows=len(result.rows),
                result_error=result.error,
                is_clarification=result.is_clarification,
                confidence=result.confidence,
                phase_timings=phase_timings,
                user_id=user["id"],
                conv_id=conv_id,
            )

            # Auto-title conversation from first query
            conv_data = auth_db.get_conversation(user["id"], conv_id)
            if conv_data and conv_data["title"] == "New Chat":
                title = req.query[:60].strip()
                auth_db.update_conversation_title(user["id"], conv_id, title)

            yield _sse_event("result", result_dict)
            yield _sse_event("done", {"phase_timings": phase_timings})

        except Exception as exc:
            logger.exception("SSE ask error")
            yield _sse_event("error", {"message": str(exc)})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── Feedback ───────────────────────────────────────────────────────────────────

class FeedbackRequest(BaseModel):
    rating: str = Field(..., pattern=r"^(up|down)$")


@app.post("/v1/messages/{message_id}/feedback", tags=["Feedback"])
async def submit_feedback(
    message_id: str,
    req: FeedbackRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    auth_db.save_feedback(message_id, user["id"], req.rating)
    return {"status": "recorded"}


# ── Re-run (edit user message) ─────────────────────────────────────────────────

class RerunRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    db_uri: Optional[str] = None


@app.post("/v1/conversations/{conv_id}/rerun", tags=["Query"])
async def rerun(
    conv_id: str,
    req: RerunRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Re-run with an edited query (same as ask but explicitly a re-run)."""
    return await ask_stream(conv_id, AskRequest(query=req.query, db_uri=req.db_uri), user)


# ── Slash commands (UI) ────────────────────────────────────────────────────────

class CommandRequest(BaseModel):
    command: str = Field(..., min_length=1, max_length=200)


@app.post("/v1/conversations/{conv_id}/command", tags=["Query"])
async def run_command(
    conv_id: str,
    req: CommandRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """
    Execute a slash command (e.g. /help, /schema, /clear-cache) within a conversation.
    Returns { "output": "<text response>" }.
    """
    conv = auth_db.get_conversation(user["id"], conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found.")

    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd:
        # Re-create instance if not in memory (e.g. server restart)
        db_uri = None
        if conv.get("db_conn_id"):
            conns = auth_db.list_db_connections(user["id"])
            for c in conns:
                if c["id"] == conv["db_conn_id"]:
                    db_uri = c["uri_encrypted"]
                    break
        nd = await _get_or_create_instance(user["id"], conv_id, db_uri)

    try:
        import nexus_data.core.slash_commands as cmds
        result = await asyncio.to_thread(cmds.handle, req.command, nd, None)
        # handle() returns either a string or a QueryResult
        if hasattr(result, "model_dump"):
            return {"output": None, "result": result.model_dump()}
        return {"output": str(result) if result else ""}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Feedback correction (bad SQL) ──────────────────────────────────────────────

class SQLFeedbackRequest(BaseModel):
    original_query: str = Field(..., max_length=2000)
    bad_sql: str = Field(..., max_length=5000)
    feedback: str = Field(..., max_length=1000)


@app.post("/v1/conversations/{conv_id}/sql-feedback", tags=["Query"])
async def sql_feedback(
    conv_id: str,
    req: SQLFeedbackRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    conv = auth_db.get_conversation(user["id"], conv_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd:
        raise HTTPException(status_code=400, detail="No active session. Send a query first.")
    try:
        result = await asyncio.to_thread(nd.ask_with_feedback, req.original_query, req.bad_sql, req.feedback)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Dataset export ─────────────────────────────────────────────────────────────

@app.get("/v1/conversations/{conv_id}/export", tags=["Export"])
async def export_last_result(
    conv_id: str,
    fmt: str = "csv",
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Export the last query result as CSV or JSON."""
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd:
        raise HTTPException(status_code=400, detail="No active session.")

    if not nd._result_history:
        raise HTTPException(status_code=404, detail="No result to export.")

    last_result = next(reversed(nd._result_history.values()))
    cols = last_result.columns or []
    rows = last_result.rows or []

    if fmt == "json":
        from fastapi.responses import JSONResponse
        return JSONResponse({"columns": cols, "rows": rows})

    # CSV
    import csv
    import io
    buf = io.StringIO()
    writer = csv.writer(buf)
    if cols:
        writer.writerow(cols)
    for row in rows:
        writer.writerow(row)

    from fastapi.responses import Response
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=nexusdata_export.csv"},
    )


# ── Save export to local data folder ──────────────────────────────────────────

@app.post("/v1/conversations/{conv_id}/save-export", tags=["Export"])
async def save_export(
    conv_id: str,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Save last result as CSV into data/{conv_id}/ and return a download URL."""
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd or not nd._result_history:
        raise HTTPException(status_code=404, detail="No result to export.")

    last_result = next(reversed(nd._result_history.values()))
    cols = last_result.columns or []
    rows = last_result.rows or []

    import csv, uuid as _uuid
    msg_id = _uuid.uuid4().hex[:8]
    out_dir = _DATA_DIR / conv_id / msg_id
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "export.csv"
    with dest.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if cols:
            writer.writerow(cols)
        for row in rows:
            writer.writerow(row)

    return {
        "status": "saved",
        "filename": "export.csv",
        "download_url": f"/v1/data/{conv_id}/{msg_id}/export.csv",
    }


@app.get("/v1/data/{conv_id}/{msg_id}/{filename}", tags=["Export"])
async def serve_data_file(
    conv_id: str,
    msg_id: str,
    filename: str,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Serve a previously saved export file."""
    # Sanitise path components
    safe = {".csv", ".json", ".png", ".txt"}
    suffix = Path(filename).suffix.lower()
    if suffix not in safe or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename.")
    dest = _DATA_DIR / conv_id / msg_id / filename
    if not dest.exists():
        raise HTTPException(status_code=404, detail="File not found.")
    from fastapi.responses import FileResponse
    return FileResponse(str(dest), filename=filename)


# ── Analytical agent ──────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    wants_chart: bool = False


@app.post("/v1/conversations/{conv_id}/analyze", tags=["Query"])
async def analyze(
    conv_id: str,
    req: AnalyzeRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Run pandas-based analysis on the last SQL result. No data sent to LLM."""
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd or not nd._result_history:
        raise HTTPException(status_code=400, detail="No result to analyse. Run a query first.")

    last_result = next(reversed(nd._result_history.values()))
    cols = last_result.columns or []
    rows = last_result.rows or []

    from nexus_data.analyst.agent import PandasAgent
    agent = PandasAgent(nd._llm)
    result = await asyncio.to_thread(agent.analyze, req.query, cols, rows, req.wants_chart)

    if result.error:
        raise HTTPException(status_code=500, detail=result.error)

    return {
        "summary": result.summary,
        "chart_b64": result.chart_b64,
        "chart_mime": result.chart_mime,
    }


# ── Schema / KB ────────────────────────────────────────────────────────────────

@app.get("/v1/conversations/{conv_id}/schema", tags=["Knowledge Base"])
async def schema(conv_id: str, user: Dict[str, Any] = Depends(_get_current_user)):
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd:
        raise HTTPException(status_code=400, detail="No active session.")
    return {"db_info": nd._kb.read_db_info()}


@app.post("/v1/conversations/{conv_id}/persona", tags=["Knowledge Base"])
async def update_persona(
    conv_id: str,
    body: Dict[str, str],
    user: Dict[str, Any] = Depends(_get_current_user),
):
    note = body.get("note", "").strip()
    if not note:
        raise HTTPException(status_code=400, detail="note required.")
    key = f"{user['id']}:{conv_id}"
    nd = _instances.get(key)
    if not nd:
        raise HTTPException(status_code=400, detail="No active session.")
    nd.set_user_context(note)
    return {"status": "updated"}


# ── Settings ───────────────────────────────────────────────────────────────────

@app.get("/v1/settings", tags=["Settings"])
async def get_settings(user: Dict[str, Any] = Depends(_get_current_user)):
    """Return full user settings (appearance, API keys config reference, etc.)."""
    raw = auth_db.get_user_by_id(user["id"])
    settings = {}
    try:
        settings = json.loads(raw.get("settings") or "{}")
    except Exception:
        pass
    return {
        "profile": {"name": raw["name"], "email": raw["email"]},
        "settings": settings,
    }


@app.put("/v1/settings", tags=["Settings"])
async def save_settings(
    body: Dict[str, Any],
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Save user settings (appearance, preferences, etc.)."""
    auth_db.update_user_settings(user["id"], body)
    return {"status": "saved"}


# ── Change Password ─────────────────────────────────────────────────────────────

class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8, max_length=200)


@app.post("/auth/change-password", tags=["Auth"])
async def change_password(
    req: ChangePasswordRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    """Verify current password, then update to new hash."""
    full_user = auth_db.get_user_by_email(user["email"])
    if not full_user or not verify_password(req.current_password, full_user["password_hash"]):
        raise HTTPException(status_code=400, detail="Current password is incorrect.")
    new_hash = hash_password(req.new_password)
    auth_db.update_password(user["id"], new_hash)
    return {"status": "password_updated"}


# ── Model Config ───────────────────────────────────────────────────────────────

class ModelConfigUpdate(BaseModel):
    provider: str
    model_name: str
    api_key: Optional[str] = None  # if provided, updates the stored API key


@app.get("/v1/model-config", tags=["Model"])
async def get_model_config(user: Dict[str, Any] = Depends(_get_current_user)):
    from nexus_data.core.setup_wizard import _MODELS_MAP
    cfg = ConfigManager()
    raw_key = cfg.config.llm.api_key or ""
    key_preview = (raw_key[:4] + "****") if len(raw_key) >= 4 else ("****" if raw_key else "")
    return {
        "provider": cfg.config.llm.provider,
        "model_name": cfg.config.llm.model_name,
        "api_key_preview": key_preview,
        "available_providers": _MODELS_MAP,
    }


@app.put("/v1/model-config", tags=["Model"])
async def update_model_config(
    req: ModelConfigUpdate,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    from nexus_data.core.setup_wizard import _MODELS_MAP
    from nexus_data.core.env_writer import write_env_key
    if req.provider not in _MODELS_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown provider '{req.provider}'.")
    if req.model_name not in _MODELS_MAP[req.provider]:
        raise HTTPException(status_code=400, detail=f"Model '{req.model_name}' not available for provider '{req.provider}'.")

    write_env_key("NEXUS_LLM_PROVIDER", req.provider)
    write_env_key("NEXUS_LLM_MODEL", req.model_name)
    if req.api_key:
        write_env_key("NEXUS_LLM_API_KEY", req.api_key)

    # Update all active NexusData instances
    async with _instances_lock:
        for nd in _instances.values():
            nd._config.config.llm.provider = req.provider
            nd._config.config.llm.model_name = req.model_name
            if req.api_key:
                nd._config.config.llm.api_key = req.api_key
                nd._llm.config = nd._config.config.llm
            else:
                nd._llm.config = nd._config.config.llm

    return {"status": "updated", "provider": req.provider, "model_name": req.model_name}


# ── API Keys (Personal Access Tokens) ─────────────────────────────────────────

class CreateAPIKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)


@app.get("/v1/api-keys", tags=["API Keys"])
async def list_api_keys(user: Dict[str, Any] = Depends(_get_current_user)):
    return {"api_keys": auth_db.list_api_keys(user["id"])}


@app.post("/v1/api-keys", tags=["API Keys"])
async def create_api_key(
    req: CreateAPIKeyRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    full_key, preview, kid = auth_db.create_api_key(user["id"], req.name)
    keys = auth_db.list_api_keys(user["id"])
    created_at = next((k["created_at"] for k in keys if k["id"] == kid), "")
    return {"id": kid, "key_preview": preview, "full_key": full_key, "created_at": created_at}


@app.delete("/v1/api-keys/{key_id}", tags=["API Keys"])
async def delete_api_key(
    key_id: str,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    auth_db.delete_api_key(user["id"], key_id)
    return {"status": "revoked"}


# ── Edit DB Connection ─────────────────────────────────────────────────────────

class EditConnectionRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    uri: Optional[str] = Field(None, min_length=5, max_length=500)


@app.patch("/v1/connections/{conn_id}", tags=["Connections"])
async def edit_connection(
    conn_id: str,
    req: EditConnectionRequest,
    user: Dict[str, Any] = Depends(_get_current_user),
):
    if not req.name and not req.uri:
        raise HTTPException(status_code=400, detail="Provide at least one of: name, uri.")
    if req.uri:
        allowed = ("sqlite", "postgresql", "postgres", "mysql", "mssql", "duckdb")
        scheme = req.uri.split("://")[0].lower() if "://" in req.uri else ""
        if scheme not in allowed:
            raise HTTPException(status_code=400, detail=f"Unsupported DB scheme '{scheme}'.")
    auth_db.update_db_connection(user["id"], conn_id, name=req.name, uri=req.uri)
    return {"status": "updated"}
