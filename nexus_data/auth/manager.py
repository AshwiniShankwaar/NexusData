"""
nexus_data/auth/manager.py
JWT creation/verification and password hashing.
- 7-day session expiry (enforced both in JWT exp claim and sessions table)
- Secret key is read lazily so auto-generated key from env_writer is picked up
- Sessions are tracked in SQLite; logout revokes the session immediately
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from jose import ExpiredSignatureError, JWTError, jwt
from passlib.context import CryptContext

_ALGO = "HS256"
_TOKEN_EXPIRE_DAYS = 7

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _secret() -> str:
    """Read secret lazily so auto-generated value in os.environ is always used."""
    return os.environ.get("NEXUS_SECRET_KEY", "dev-insecure-secret-change-me")


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ── Password ───────────────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return _pwd_ctx.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_ctx.verify(plain, hashed)


# ── Token lifecycle ────────────────────────────────────────────────────────────

def create_access_token(user_id: str) -> Tuple[str, str]:
    """
    Create a signed JWT and persist the session in SQLite.
    Returns (token, expires_at_iso).
    """
    from nexus_data.auth import models as auth_db  # local import to avoid circular

    expires_at = datetime.now(timezone.utc) + timedelta(days=_TOKEN_EXPIRE_DAYS)
    token = jwt.encode(
        {"sub": user_id, "exp": expires_at},
        _secret(),
        algorithm=_ALGO,
    )
    auth_db.create_session(user_id, _token_hash(token), expires_at.isoformat())
    return token, expires_at.isoformat()


def decode_token(token: str) -> Optional[str]:
    """
    Verify JWT signature + expiry, then confirm the session exists in the DB.
    Returns user_id on success, None on any failure (expired, revoked, invalid).
    """
    from nexus_data.auth import models as auth_db

    try:
        payload = jwt.decode(token, _secret(), algorithms=[_ALGO])
    except ExpiredSignatureError:
        # JWT itself is expired — session is also gone (we clean on login)
        return None
    except JWTError:
        return None

    user_id: Optional[str] = payload.get("sub")
    if not user_id:
        return None

    # Confirm session still exists in DB (catches logout / manual revocation)
    session = auth_db.get_session(_token_hash(token))
    if not session:
        return None

    return user_id


def revoke_token(token: str) -> None:
    """Invalidate a session (logout). Safe to call with an invalid token."""
    from nexus_data.auth import models as auth_db
    try:
        auth_db.delete_session(_token_hash(token))
    except Exception:
        pass
