"""
nexus_data/core/setup_wizard.py
Interactive Setup Wizard — works in both rich terminals (questionary)
and plain terminals (VS Code, Git Bash, CI) via automatic fallback to input().
"""
from __future__ import annotations

import logging
import sys
from typing import List, Optional, Tuple

from sqlalchemy import create_engine, inspect

from nexus_data.core.config_manager import ConfigManager, LLMConfig, DBConfig

logger = logging.getLogger(__name__)

_MODELS_MAP = {
    "openai":      ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
    "google":      ["gemini-2.5-flash", "gemini-2.5-pro",
                    "gemini-2.0-flash", "gemini-2.0-flash-lite",
                    "gemini-1.5-flash", "gemini-1.5-pro"],
    "anthropic":   ["claude-sonnet-4-6", "claude-opus-4-6", "claude-haiku-4-5-20251001"],
    "openrouter":  [
        "meta-llama/llama-3.3-70b-instruct",
        "anthropic/claude-sonnet-4-5",
        "google/gemini-2.0-flash-001",
        "openai/gpt-4o",
        "deepseek/deepseek-chat-v3-0324",
    ],
}


# ── Terminal-agnostic helpers ──────────────────────────────────────────────────

def _questionary_available() -> bool:
    """Return True if questionary can open an interactive session in this terminal."""
    try:
        import questionary
        from prompt_toolkit.output import create_output
        create_output()  # raises NoConsoleScreenBufferError in non-native terminals
        return True
    except Exception:
        return False


_USE_QUESTIONARY = None   # computed once on first call


def _rich() -> bool:
    global _USE_QUESTIONARY
    if _USE_QUESTIONARY is None:
        _USE_QUESTIONARY = _questionary_available()
    return _USE_QUESTIONARY


def _select(prompt: str, choices: List[str], default: Optional[str] = None) -> str:
    """Select from a list — rich UI if available, numbered menu otherwise."""
    if _rich():
        import questionary
        result = questionary.select(prompt, choices=choices).ask()
        if result is None:
            sys.exit("Setup cancelled.")
        return result

    # Plain-text fallback
    print(f"\n{prompt}")
    for i, c in enumerate(choices, 1):
        marker = " (default)" if c == default else ""
        print(f"  {i}. {c}{marker}")
    while True:
        raw = input("Enter number: ").strip()
        if not raw and default:
            return default
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(choices)}.")


def _checkbox(prompt: str, choices: List[str]) -> List[str]:
    """Multi-select checkbox — rich UI if available, comma-separated input otherwise."""
    if _rich():
        import questionary
        result = questionary.checkbox(prompt, choices=choices).ask()
        return result or []

    print(f"\n{prompt}")
    for i, c in enumerate(choices, 1):
        print(f"  {i}. {c}")
    raw = input("Enter numbers separated by commas (or ENTER to skip): ").strip()
    if not raw:
        return []
    selected = []
    for part in raw.split(","):
        try:
            idx = int(part.strip()) - 1
            if 0 <= idx < len(choices):
                selected.append(choices[idx])
        except ValueError:
            pass
    return selected


def _text(prompt: str, default: str = "", password: bool = False) -> str:
    """Text/password input — rich UI if available, plain input() otherwise."""
    if _rich():
        import questionary
        if password:
            result = questionary.password(prompt).ask()
        else:
            result = questionary.text(prompt, default=default).ask()
        if result is None:
            sys.exit("Setup cancelled.")
        return result.strip()

    suffix = f" [{default}]" if default else ""
    if password:
        import getpass
        return getpass.getpass(f"{prompt}: ").strip()
    val = input(f"{prompt}{suffix}: ").strip()
    return val or default


# ── DB wizard (shared by first-time setup and /add-db) ────────────────────────

def _run_db_wizard() -> Tuple[str, DBConfig]:
    """
    Interactive DB connection wizard.
    Returns (connection_name, DBConfig).
    Exits on failure or cancellation.
    """
    # Connection name
    conn_name = _text("Connection name (e.g. production, analytics, local):", default="default")
    if not conn_name:
        conn_name = "default"

    db_type = _select(
        "Select Database Type:",
        ["SQLite", "PostgreSQL", "MySQL", "Microsoft SQL Server", "Custom URI"],
    )

    uri = ""

    if db_type == "SQLite":
        file_path = _text("Enter SQLite file path (e.g. sample.db):")
        if not file_path:
            sys.exit("Setup cancelled.")
        uri = file_path if file_path.startswith("sqlite:") else f"sqlite:///{file_path}"

    elif db_type == "Custom URI":
        uri = _text("Enter full SQLAlchemy connection URI:",
                    default="postgresql+psycopg2://user:pass@host:5432/dbname")
        if not uri:
            sys.exit("Setup cancelled.")

    else:
        _ports   = {"PostgreSQL": "5432", "MySQL": "3306", "Microsoft SQL Server": "1433"}
        _drivers = {"PostgreSQL": "postgresql+psycopg2",
                    "MySQL": "mysql+pymysql",
                    "Microsoft SQL Server": "mssql+pyodbc"}
        host     = _text("Host:", default="localhost")
        port     = _text("Port:", default=_ports[db_type])
        db_name  = _text("Database Name:")
        user     = _text("Username:")
        password = _text("Password (leave blank if none):", password=True)

        if not all([host, port, db_name, user]):
            sys.exit("Missing required fields. Setup cancelled.")

        auth = f"{user}:{password}@" if password else f"{user}@"
        uri  = f"{_drivers[db_type]}://{auth}{host}:{port}/{db_name}"
        if db_type == "Microsoft SQL Server":
            uri += "?driver=ODBC+Driver+17+for+SQL+Server"

    display = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
    print(f"\n[Testing Connection to: {display}]")
    try:
        engine = create_engine(uri)
        conn = engine.connect()
        conn.close()
        print("  Connected successfully.")
    except Exception as e:
        print(f"\n[FATAL] Failed to connect to DB: {e}")
        sys.exit(1)

    # Access Control
    print("\n--- Access Control (Do Not Touch) ---")
    print("Analyzing schema...")
    do_not_touch: List[str] = []

    try:
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        dialect = engine.dialect.name
        safe = []
        for t in all_tables:
            if dialect == "sqlite" and t == "sqlite_sequence":
                continue
            if dialect == "postgresql" and (t.startswith("pg_") or t.startswith("sql_")):
                continue
            if dialect == "mysql" and t.startswith("mysql"):
                continue
            safe.append(t)

        if safe:
            do_not_touch = _checkbox(
                "Select tables to mark as DO NOT TOUCH (AI will not query these):",
                safe,
            )
    except Exception as e:
        print(f"  Warning: Could not extract schema during setup. {e}")

    db_config = DBConfig(
        uri=uri,
        do_not_touch_tables=do_not_touch,
        do_not_touch_columns={},
    )
    return conn_name, db_config


# ── Wizards ────────────────────────────────────────────────────────────────────

def run_cli_setup(config_manager: ConfigManager) -> None:
    """
    First-time setup wizard: LLM + first DB connection.
    If LLM is already configured, skips straight to the DB section.
    """
    from nexus_data.core.env_writer import write_env_key

    print("\n" + "=" * 50)
    print("  NexusData Setup Wizard")
    print("=" * 50)

    # ── 1. LLM Provider — skip if already configured ──────────────────────────
    if not config_manager.is_configured():
        print("\n--- Step 1: LLM Configuration ---")
        provider = _select(
            "Select your preferred LLM Provider:",
            ["openai", "google", "anthropic", "openrouter"],
        )

        model_name = _select(
            f"Select a model for {provider.title()}:",
            _MODELS_MAP[provider],
        )

        api_key = _text(f"Enter API Key for {provider.title()}:", password=True)
        if not api_key:
            sys.exit("API Key is required. Setup cancelled.")

        api_base = "https://openrouter.ai/api/v1" if provider == "openrouter" else None

        config_manager.config.llm = LLMConfig(
            provider=provider,
            model_name=model_name,
            api_key=api_key,
            api_base=api_base,
        )

        # Write secrets to .env only — NOT to config.json
        write_env_key("NEXUS_LLM_PROVIDER", provider)
        write_env_key("NEXUS_LLM_MODEL", model_name)
        write_env_key("NEXUS_LLM_API_KEY", api_key)
    else:
        llm = config_manager.config.llm
        print(f"\n[LLM already configured: {llm.provider} / {llm.model_name}  — skipping LLM setup]")

    # ── 2. First DB connection ─────────────────────────────────────────────────
    print("\n--- Database Configuration ---")
    conn_name, db_config = _run_db_wizard()

    # Store under named databases dict AND set as active
    key = config_manager.add_database(conn_name, db_config)
    config_manager.config.active_db_name = key
    config_manager.config.db = db_config.model_copy()
    config_manager.save()

    print("\n" + "=" * 50)
    print(f"  Setup Complete! Connection '{key}' saved.")
    print("=" * 50 + "\n")


def run_add_db_wizard(config_manager: ConfigManager) -> Optional[str]:
    """
    Add a new database connection to the config.
    Returns the connection name if successful, None if cancelled.
    Called from /add-db slash command.
    """
    print("\n--- Add New Database Connection ---")
    conn_name, db_config = _run_db_wizard()
    key = config_manager.add_database(conn_name, db_config)
    print(f"\n  Connection '{key}' saved.\n")
    return key
