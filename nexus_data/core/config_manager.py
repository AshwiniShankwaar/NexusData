"""
nexus_data/core/config_manager.py
"""
from __future__ import annotations
import json, logging, os, re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from pydantic import BaseModel, Field, ConfigDict

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


class LLMConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    provider: str = "openai"
    model_name: str = "gpt-4o"
    api_key: str = ""
    api_base: Optional[str] = None


class DBConfig(BaseModel):
    uri: str = "sqlite:///sample.db"
    do_not_touch_tables: List[str] = Field(default_factory=list)
    do_not_touch_columns: Dict[str, List[str]] = Field(default_factory=dict)
    table_descriptions: Dict[str, str] = Field(default_factory=dict)
    column_descriptions: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    schema_hash: str = ""   # MD5 of schema — used for drift detection


class AppConfig(BaseModel):
    llm: LLMConfig = Field(default_factory=LLMConfig)
    db: DBConfig = Field(default_factory=DBConfig)           # active DB (runtime)
    databases: Dict[str, DBConfig] = Field(default_factory=dict)  # name → saved config
    active_db_name: str = ""                                  # which entry in databases is active
    output_language: str = "English"
    history_max_lines: int = 200           # cap on shortterm_memory.md
    confidence_threshold: float = 0.50    # warn user when confidence < this
    audit_log_enabled: bool = True         # write audit.jsonl per session
    kb_refresh_interval_hours: int = 0    # 0 = disabled; >0 = background refresh


def _safe_name(name: str) -> str:
    """Filesystem-safe, lowercase version of a DB connection name."""
    return re.sub(r"[^\w\-]", "_", name.strip().lower())


class ConfigManager:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.config: AppConfig = self._load()

    def _load(self) -> AppConfig:
        if not self.config_path.exists():
            return AppConfig()
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            cfg = AppConfig(**data)
            # ── Migration: old single-db config → databases dict ──────────────
            if not cfg.databases and cfg.db.uri and cfg.db.uri != "sqlite:///sample.db":
                cfg.databases["default"] = cfg.db.model_copy()
                cfg.active_db_name = "default"
                logger.info("Migrated single DB config to databases['default'].")
            # Override LLM settings from env if set (written by /change-model)
            env_key = os.getenv("NEXUS_LLM_API_KEY", "")
            if env_key:
                cfg.llm.api_key = env_key
            env_provider = os.getenv("NEXUS_LLM_PROVIDER", "")
            if env_provider:
                cfg.llm.provider = env_provider
            env_model = os.getenv("NEXUS_LLM_MODEL", "")
            if env_model:
                cfg.llm.model_name = env_model
            # Override DB URI from env components if available (written by /change-db)
            from nexus_data.core.env_writer import build_db_uri_from_env
            env_uri = build_db_uri_from_env()
            if env_uri and env_uri != cfg.db.uri:
                cfg.db.uri = env_uri
            return cfg
        except Exception as e:
            logger.error("Failed to load config: %s", e)
            return AppConfig()

    def save(self) -> None:
        # Sync active DB config back to databases dict before writing
        if self.config.active_db_name and self.config.active_db_name in self.config.databases:
            self.config.databases[self.config.active_db_name] = self.config.db.model_copy()
        with open(self.config_path, "w", encoding="utf-8") as f:
            f.write(self.config.model_dump_json(indent=4))

    def is_configured(self) -> bool:
        # ollama runs locally — no API key required
        if self.config.llm.provider != "ollama" and not self.config.llm.api_key:
            return False
        return True

    # ── Multi-DB helpers ──────────────────────────────────────────────────────

    def add_database(self, name: str, db_config: DBConfig) -> str:
        """Save a named DB config. Returns the sanitized name used as key."""
        key = _safe_name(name)
        self.config.databases[key] = db_config.model_copy()
        self.save()
        logger.info("Database '%s' saved.", key)
        return key

    def get_database(self, name: str) -> Optional[DBConfig]:
        return self.config.databases.get(_safe_name(name))

    def list_databases(self) -> List[Tuple[str, str]]:
        """Return [(name, uri)] sorted by name."""
        return sorted(
            (name, cfg.uri) for name, cfg in self.config.databases.items()
        )

    def set_active_db(self, name: str) -> DBConfig:
        """
        Switch the active database. Updates config.db and active_db_name.
        Returns the selected DBConfig.
        Raises KeyError if name not found.
        """
        key = _safe_name(name)
        if key not in self.config.databases:
            raise KeyError(f"No database connection named '{key}'. Use /databases to list.")
        self.config.active_db_name = key
        self.config.db = self.config.databases[key].model_copy()
        return self.config.db

    def kb_dir_for(self, name: str) -> Path:
        """Return the per-DB KB directory path (not created yet)."""
        return Path("nexus_kb") / _safe_name(name)
