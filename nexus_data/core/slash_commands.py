"""
nexus_data/core/slash_commands.py
Handles all /slash-command processing for the CLI.
"""
from __future__ import annotations
import csv, json, os
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from nexus_data.orchestrator import NexusData
    from nexus_data.models import QueryResult

_HELP_TEXT = """
Available commands:
  /help                 — Show this help
  /databases            — List all saved database connections
  /add-db               — Add a new database connection
  /change-model         — Switch LLM provider or model
  /language <lang>      — Set output language (e.g. /language French)
  /change-db            — Switch to a different saved database connection
  /update-table-info    — Re-run the metadata wizard for tables/columns
  /schema               — Print current database schema
  /relations            — Show table relationship map
  /export csv           — Export last result to CSV
  /export json          — Export last result to JSON
  /clear-cache          — Clear the semantic vector cache
  /history              — Show current session query history
  /graph-export         — Export KB semantic query graph to JSON
  /explain              — Explain the last SQL query in plain English
  /bookmark <name>      — Save the last query as a named bookmark
  /bookmarks            — List all saved bookmarks
  /run <name>           — Run a saved bookmark
  /reset [soft|full|factory]
                        — Reset application state (see /reset --help)
  /exit / exit          — Quit
"""


def handle(command: str, nd: "NexusData", last_result: Optional["QueryResult"]) -> str:
    """
    Process a slash command. Returns a string message to print, or "" for silent.
    Modifies nd state in-place where needed.
    """
    parts = command.strip().split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/help":
        return _HELP_TEXT

    if cmd == "/databases":
        return _list_databases(nd)

    if cmd == "/add-db":
        return _add_db(nd)

    if cmd == "/change-model":
        return _change_model(nd)

    if cmd == "/language":
        return _set_language(nd, arg)

    if cmd == "/change-db":
        return _change_db(nd)

    if cmd == "/update-table-info":
        return _update_table_info(nd)

    if cmd == "/schema":
        return nd._kb.read_db_info()

    if cmd == "/relations":
        return _show_relations(nd)

    if cmd == "/export":
        return _export(last_result, arg or "csv")

    if cmd == "/clear-cache":
        return _clear_cache(nd)

    if cmd == "/history":
        return _show_history(nd)

    if cmd == "/graph-export":
        return _graph_export(nd)

    if cmd == "/explain":
        return nd.explain_last(arg or "Explain the SQL")

    if cmd == "/bookmark":
        if not arg:
            return "Usage: /bookmark <name>"
        return nd.save_bookmark(arg)

    if cmd == "/bookmarks":
        items = nd._bookmarks.list_all()
        if not items:
            return "No bookmarks saved yet. Use /bookmark <name> after a query."
        return "\n".join(f"  {name}: {q[:70]}" for name, q in items)

    if cmd == "/run":
        if not arg:
            return "Usage: /run <bookmark-name>"
        return nd.run_bookmark(arg)  # returns QueryResult — handled in CLI

    if cmd == "/reset":
        return _reset(nd, arg)

    return f"Unknown command: {cmd}. Type /help for available commands."


# ── Handlers ──────────────────────────────────────────────────────────────────

def _list_databases(nd: "NexusData") -> str:
    """Show all saved database connections."""
    dbs = nd._config.list_databases()
    if not dbs:
        return "No database connections saved. Use /add-db to add one."
    active = nd._config.config.active_db_name
    lines = []
    for name, uri in dbs:
        display = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
        marker = " [active]" if name == active else ""
        lines.append(f"  {name}{marker}  —  {display}")
    return "\n".join(lines)


def _add_db(nd: "NexusData") -> str:
    """Add a new DB connection via interactive wizard (does NOT switch active DB)."""
    from nexus_data.core.setup_wizard import run_add_db_wizard
    try:
        key = run_add_db_wizard(nd._config)
        if key:
            return f"Connection '{key}' saved. Use /change-db to switch to it."
        return "Cancelled."
    except SystemExit:
        return "Cancelled."
    except Exception as exc:
        return f"[ERROR] {exc}"


def _change_db(nd: "NexusData") -> str:
    """Switch to a different saved database connection (within this session)."""
    from nexus_data.core.setup_wizard import _select, _rich
    from nexus_data.core.env_writer import write_env_key, parse_db_uri_to_components

    dbs = nd._config.list_databases()
    if not dbs:
        return "No connections saved. Use /add-db first."

    active = nd._config.config.active_db_name
    choices = []
    for name, uri in dbs:
        display = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
        suffix = " (current)" if name == active else ""
        choices.append(f"{name}  —  {display}{suffix}")

    # Pick connection
    try:
        if _rich():
            import questionary
            selected = questionary.select("Switch to which database?", choices=choices).ask()
            if not selected:
                return "Cancelled."
            chosen_name = selected.split("  —  ")[0].strip()
        else:
            chosen_name = _select("Switch to which database?",
                                  [n for n, _ in dbs], default=active)
    except Exception:
        chosen_name = _select("Switch to which database?",
                              [n for n, _ in dbs], default=active)

    if chosen_name == active:
        return f"Already using '{chosen_name}'."

    try:
        nd._config.set_active_db(chosen_name)
        kb_dir = nd._config.kb_dir_for(chosen_name)
        new_uri = nd._config.config.db.uri
        print(f"\nSwitching to '{chosen_name}'…")
        # Replace the KB with a fresh instance for the new DB dir BEFORE
        # connect_and_initialize so all pipeline components receive the correct KB.
        # KBManager.__init__ handles mkdir + file creation automatically.
        from nexus_data.kb.manager import KBManager
        nd._kb = KBManager(kb_dir)
        nd.connect_and_initialize(db_uri=new_uri)

        # Persist new DB to .env
        components = parse_db_uri_to_components(new_uri)
        for k, v in components.items():
            write_env_key(k, v)
        # Also store the active connection name
        write_env_key("NEXUS_DB_ACTIVE", chosen_name)
        return f"Switched to '{chosen_name}' and saved to .env."
    except Exception as exc:
        return f"[ERROR] Failed to switch: {exc}"


def _change_model(nd: "NexusData") -> str:
    from nexus_data.core.setup_wizard import _MODELS_MAP, _rich, _select, _text
    from nexus_data.core.env_writer import write_env_key

    cfg = nd._config
    provider = _select(
        "Select LLM provider:",
        ["openai", "google", "anthropic", "openrouter"],
    )

    models = _MODELS_MAP.get(provider, []) + ["Custom…"]
    model = _select(f"Select model for {provider}:", models)
    if model == "Custom…":
        model = _text("Enter model name:")
        if not model:
            return "Cancelled."

    change_key = False
    if _rich():
        import questionary
        change_key = questionary.confirm("Change API key?", default=False).ask() or False
    else:
        change_key = input("Change API key? [y/N]: ").strip().lower() == "y"

    new_key = None
    if change_key:
        import getpass
        new_key = getpass.getpass(f"New API key for {provider}: ")
        cfg.config.llm.api_key = new_key

    cfg.config.llm.provider = provider
    cfg.config.llm.model_name = model
    cfg.save()

    # Persist to .env so future sessions pick up the change
    write_env_key("NEXUS_LLM_PROVIDER", provider)
    write_env_key("NEXUS_LLM_MODEL", model)
    if new_key:
        write_env_key("NEXUS_LLM_API_KEY", new_key)

    nd._llm.config = cfg.config.llm
    nd._llm._cfg_mgr = cfg
    return f"Model changed to {provider}/{model} and saved to .env."


def _set_language(nd: "NexusData", lang: str) -> str:
    from nexus_data.core.setup_wizard import _text
    if not lang:
        lang = _text("Output language (e.g. English, French, Hindi):") or "English"
    nd._config.config.output_language = lang
    nd._config.save()
    return f"Output language set to: {lang}"


def _update_table_info(nd: "NexusData") -> str:
    from nexus_data.librarian.introspector import DatabaseIntrospector
    if not nd._engine:
        return "[ERROR] Not connected to a database."
    intr = DatabaseIntrospector(nd._engine, nd._kb, nd._config, llm=nd._llm)
    intr.refresh()
    return "Table metadata updated."


def _show_relations(nd: "NexusData") -> str:
    kb_text = nd._kb.read_db_info()
    match = kb_text.find("## Table Relationships")
    if match == -1:
        return "No relationship map found. Run /update-table-info to regenerate."
    return kb_text[match:]


def _export(last_result: Optional["QueryResult"], fmt: str) -> str:
    if not last_result or not last_result.rows:
        return "[ERROR] No result to export. Run a query first."

    fmt = fmt.lower().strip()
    out_dir = Path("./nexus_exports")
    out_dir.mkdir(exist_ok=True)

    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if fmt == "json":
        path = out_dir / f"result_{ts}.json"
        rows_dicts = [dict(zip(last_result.columns, row)) for row in last_result.rows]
        path.write_text(json.dumps(rows_dicts, indent=2, default=str), encoding="utf-8")
    else:  # csv default
        path = out_dir / f"result_{ts}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(last_result.columns)
            w.writerows(last_result.rows)

    return f"Exported {len(last_result.rows)} rows → {path}"


def _clear_cache(nd: "NexusData") -> str:
    try:
        nd._vector_repo._table = nd._vector_repo._ensure_table()
        return "Semantic vector cache cleared."
    except Exception as e:
        return f"[ERROR] Could not clear cache: {e}"


def _show_history(nd: "NexusData") -> str:
    cache = nd._kb.get_session_cache()
    records = [e for e in cache if isinstance(e, dict) and e.get("type") == "turn_record"]
    if not records:
        return "No queries in this session yet."
    lines = [f"  Turn {r['turn_id']}: {r['user_query'][:80]}" for r in records]
    return "\n".join(lines)


def _graph_export(nd: "NexusData") -> str:
    try:
        path = nd._graph_store.export_graph_json()
        g = nd._graph_store.get_full_graph()
        return (
            f"KB semantic graph exported to: {path}\n"
            f"  Nodes: {len(g['nodes'])}  Edges: {len(g['edges'])}  "
            f"Patterns: {g['pattern_count']}"
        )
    except Exception as exc:
        return f"Graph export failed: {exc}"


_RESET_HELP = """
/reset — Application reset utility
  Levels:
    /reset soft     Clear conversation history and semantic cache (current DB only).
                    Preserves: schema, learned patterns, bookmarks, user accounts.

    /reset full     Clear the entire knowledge base for the current database.
                    Preserves: all other DBs, auth accounts, config, logs.

    /reset factory  Complete wipe: all KB directories, auth history (accounts kept),
                    and log files. The app will go through first-time setup on restart.
                    Preserves: user accounts, config.json, .env.

  You will be asked to confirm before anything is deleted.
"""


def _reset(nd: "NexusData", arg: str) -> str:
    from nexus_data.core.reset import soft_reset, full_reset, factory_reset
    from pathlib import Path
    import os

    level = arg.lower().strip() if arg else ""

    if level in ("--help", "-h", "help", ""):
        if level == "":
            # No arg — show interactive picker
            print(_RESET_HELP)
            try:
                level = input("Choose level [soft / full / factory / cancel]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return "Cancelled."
            if not level or level == "cancel":
                return "Cancelled."
        else:
            return _RESET_HELP

    if level not in ("soft", "full", "factory"):
        return f"Unknown reset level '{level}'. Use: soft, full, factory, or --help."

    kb_dir: Path = nd._kb._dir

    # ── Describe what will be deleted ────────────────────────────────────────
    if level == "soft":
        preview = (
            f"  • Conversation history ({kb_dir}/conversation_lineage.json)\n"
            f"  • Short-term memory ({kb_dir}/shortterm_memory.md)\n"
            f"  • Semantic vector cache ({kb_dir}/vector_store/)\n"
            f"  • Cache stats ({kb_dir}/cache_stats.json)"
        )
    elif level == "full":
        preview = (
            f"  • All KB files for '{nd._config.config.active_db_name}' ({kb_dir}/)\n"
            f"    — conversation history, schema cache, long-term memory,\n"
            f"      query patterns, bookmarks, audit logs"
        )
    else:  # factory
        kb_root = kb_dir.parent
        auth_db = Path(os.environ.get("NEXUS_AUTH_DB", "./nexus_auth.db"))
        logs_dir = Path("./logs")
        preview = (
            f"  • Entire KB directory ({kb_root}/)\n"
            f"  • Auth DB history — conversations, messages, sessions, feedback\n"
            f"    (user accounts are kept)\n"
            f"  • Log files ({logs_dir}/nexus_*.log)\n"
            f"\n  NOT deleted: user accounts, config.json, .env"
        )

    print(f"\nThis will delete:\n{preview}\n")

    # ── Confirmation ──────────────────────────────────────────────────────────
    prompt = 'Type "yes" to confirm, anything else to cancel: '
    try:
        confirm = input(prompt).strip().lower()
    except (EOFError, KeyboardInterrupt):
        return "\nCancelled."

    if confirm != "yes":
        return "Cancelled — nothing was deleted."

    # Factory reset requires a second confirmation
    if level == "factory":
        print("\nThis is a complete wipe and cannot be undone.")
        try:
            confirm2 = input('Type "RESET" (all caps) to proceed: ').strip()
        except (EOFError, KeyboardInterrupt):
            return "\nCancelled."
        if confirm2 != "RESET":
            return "Cancelled — nothing was deleted."

    # ── Execute ───────────────────────────────────────────────────────────────
    try:
        if level == "soft":
            ok, report = soft_reset(kb_dir)
        elif level == "full":
            ok, report = full_reset(kb_dir)
        else:
            kb_root = kb_dir.parent
            auth_db = Path(os.environ.get("NEXUS_AUTH_DB", "./nexus_auth.db"))
            logs_dir = Path("./logs")
            ok, report = factory_reset(kb_root, auth_db_path=auth_db, logs_dir=logs_dir)
    except Exception as exc:
        return f"[ERROR] Reset failed: {exc}"

    # Re-init in-memory state so the running session is consistent
    if ok and level in ("soft", "full"):
        try:
            nd._kb._session_cache.clear()
            if hasattr(nd, "_conv_graph"):
                nd._conv_graph._turns.clear()
                nd._conv_graph._order.clear()
        except Exception:
            pass  # non-fatal — state will be consistent after restart

    return report
