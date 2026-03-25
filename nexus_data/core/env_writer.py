"""
nexus_data/core/env_writer.py
Utility to read/write individual keys in the .env file without clobbering comments.
Used by /change-model, /change-db, and first-boot secret-key generation.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ENV_PATH = Path(".env")


def _find_env() -> Path:
    """Locate the .env file: project root or cwd."""
    candidate = Path(__file__).resolve().parent.parent.parent / ".env"
    if candidate.exists():
        return candidate
    return _ENV_PATH


def write_env_key(key: str, value: str, env_path: Optional[Path] = None) -> None:
    """
    Set *key=value* in the .env file.
    - If the key already exists (with or without a value) it is updated in-place.
    - If the key is absent it is appended at the end.
    - Comments and blank lines are preserved.
    """
    path = env_path or _find_env()
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')

    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    else:
        lines = []

    pattern = re.compile(r"^(\s*#?\s*)" + re.escape(key) + r"\s*=.*$")
    replaced = False
    new_lines = []
    for line in lines:
        if pattern.match(line):
            # Preserve leading comment marker if line was commented out
            lead = re.match(r"^\s*#\s*", line)
            prefix = "" if not lead else ""   # always uncomment when writing
            new_lines.append(f'{key}="{escaped}"\n')
            replaced = True
        else:
            new_lines.append(line)

    if not replaced:
        # Ensure there's a trailing newline before appending
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] += "\n"
        new_lines.append(f'{key}="{escaped}"\n')

    path.write_text("".join(new_lines), encoding="utf-8")
    # Reload into os.environ so the running process sees it immediately
    os.environ[key] = value
    logger.debug("env_writer: %s updated in %s", key, path)


def read_env_key(key: str, env_path: Optional[Path] = None) -> str:
    """Read a key's value directly from the .env file (bypasses os.environ cache)."""
    path = env_path or _find_env()
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, v = stripped.partition("=")
        if k.strip() == key:
            return v.strip().strip('"').strip("'")
    return ""


def ensure_secret_key(env_path: Optional[Path] = None) -> str:
    """
    Auto-generate and persist NEXUS_SECRET_KEY on first boot.
    Returns the (possibly newly generated) secret.
    - Reads existing value from .env file directly (not os.environ, which may have
      the placeholder loaded at import time).
    - Only regenerates if the value is missing, empty, or the default placeholder.
    """
    _PLACEHOLDER = "change-me-to-a-random-secret"
    path = env_path or _find_env()

    current = read_env_key("NEXUS_SECRET_KEY", path)
    env_val = os.getenv("NEXUS_SECRET_KEY", "")

    if current and current != _PLACEHOLDER:
        # Already configured — make sure os.environ has it
        if env_val != current:
            os.environ["NEXUS_SECRET_KEY"] = current
        return current

    # Generate a new 32-byte hex secret
    new_secret = secrets.token_hex(32)
    write_env_key("NEXUS_SECRET_KEY", new_secret, path)
    logger.info("Generated new NEXUS_SECRET_KEY and saved to %s", path)
    print(f"[NexusData] First-boot: generated secret key saved to {path}")
    return new_secret


def parse_db_uri_to_components(uri: str) -> dict:
    """
    Decompose a SQLAlchemy URI into its component env vars.
    Returns a dict of key→value pairs to write to .env.
    """
    from urllib.parse import urlparse, unquote
    try:
        parsed = urlparse(uri)
        driver = parsed.scheme or ""          # e.g. postgresql+psycopg2
        host   = parsed.hostname or ""
        port   = str(parsed.port or "")
        dbname = (parsed.path or "").lstrip("/")
        user   = unquote(parsed.username or "")
        pwd    = unquote(parsed.password or "")
        return {
            "NEXUS_DB_DRIVER":   driver,
            "NEXUS_DB_HOST":     host,
            "NEXUS_DB_PORT":     port,
            "NEXUS_DB_NAME":     dbname,
            "NEXUS_DB_USER":     user,
            "NEXUS_DB_PASSWORD": pwd,
        }
    except Exception:
        return {"NEXUS_DB_URI": uri}


def build_db_uri_from_env() -> str:
    """
    Reconstruct a SQLAlchemy URI from the individual component env vars.
    Falls back to NEXUS_DB_URI if components are incomplete.
    """
    direct = os.getenv("NEXUS_DB_URI", "").strip()
    if direct:
        return direct

    driver = os.getenv("NEXUS_DB_DRIVER", "").strip()
    host   = os.getenv("NEXUS_DB_HOST",   "").strip()
    port   = os.getenv("NEXUS_DB_PORT",   "").strip()
    dbname = os.getenv("NEXUS_DB_NAME",   "").strip()
    user   = os.getenv("NEXUS_DB_USER",   "").strip()
    pwd    = os.getenv("NEXUS_DB_PASSWORD", "").strip()

    if not all([driver, host, dbname]):
        return ""

    auth = ""
    if user:
        from urllib.parse import quote
        auth = quote(user, safe="")
        if pwd:
            auth += f":{quote(pwd, safe='')}"
        auth += "@"

    port_part = f":{port}" if port else ""
    return f"{driver}://{auth}{host}{port_part}/{dbname}"
