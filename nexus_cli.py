"""
nexus_cli.py — NexusData Interactive Console
Supports: natural language queries, /slash-commands, feedback correction,
          clarification flow, multi-language, export, multi-db selection.
"""
import sys, logging, warnings, traceback
from pathlib import Path
from typing import Optional

warnings.filterwarnings("ignore", message="Field .model_name. has conflict")

# Auto-generate secret key on first boot before anything else loads
from dotenv import load_dotenv
load_dotenv()
from nexus_data.core.env_writer import ensure_secret_key
ensure_secret_key()

from nexus_data.orchestrator import NexusData
from nexus_data.models import QueryResult
from nexus_data.core.request_logger import log_pipeline_request
from nexus_data.core.config_manager import ConfigManager, DBConfig
from nexus_data.core.setup_wizard import run_cli_setup
import nexus_data.core.slash_commands as cmds
import nexus_data.auth.models as auth_db
from nexus_data.auth.manager import hash_password, verify_password

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
for _noisy in ("httpx", "sentence_transformers", "lancedb", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

_COL_SEP = " | "
_ROW_LIMIT = 25


def _cli_login() -> Optional[dict]:
    """
    Authenticate the CLI user against nexus_auth.db.
    Creates a new account if no users exist yet.
    Returns the user dict, or None if the user skips (not recommended).
    """
    auth_db.init_db()
    users = auth_db.list_all_users()

    if not users:
        print("\n── First-time account setup ──────────────────────────")
        print("No accounts found. Create your NexusData account:\n")
        name  = input("  Full name  : ").strip() or "Admin"
        email = input("  Email      : ").strip().lower()
        if not email:
            print("  Email is required. Skipping account creation.")
            return None
        import getpass
        pw = getpass.getpass("  Password   : ").strip()
        if len(pw) < 8:
            print("  Password must be at least 8 characters. Skipping.")
            return None
        user = auth_db.create_user(name, email, hash_password(pw))
        print(f"\n  Account created for {email}. You can also log in via the web UI.\n")
        return user

    print("\n── Sign in ───────────────────────────────────────────")
    email = input("  Email      : ").strip().lower()
    if not email:
        return None
    import getpass
    pw = getpass.getpass("  Password   : ").strip()
    user_row = auth_db.get_user_by_email(email)
    if not user_row or not verify_password(pw, user_row["password_hash"]):
        print("\n  Invalid email or password.")
        sys.exit(1)
    print(f"\n  Welcome back, {user_row['name']}!\n")
    return user_row


def _sync_cli_connections(user_id: str, config: ConfigManager) -> None:
    """
    Push any config.json database connections that are missing from nexus_auth.db
    into nexus_auth.db for this user, and pull any nexus_auth.db connections
    missing from config.json back into config.json.
    This keeps CLI and UI in sync.
    """
    existing_names = {c["name"] for c in auth_db.list_db_connections(user_id)}
    # config.json → auth DB
    for db_name, db_cfg in config.config.databases.items():
        if db_name not in existing_names and db_cfg.uri:
            auth_db.save_db_connection(user_id, db_name, db_cfg.uri, db_cfg.uri.split("://")[0])
    # auth DB → config.json
    for conn in auth_db.list_db_connections(user_id):
        if conn["name"] not in config.config.databases:
            config.add_database(conn["name"], DBConfig(uri=conn["uri_encrypted"]))


def _print_table(columns: list, rows: list) -> None:
    if not columns:
        print("  (no columns)"); return
    str_rows = [[str(v) if v is not None else "NULL" for v in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in str_rows), 4) if str_rows else len(h)
              for i, h in enumerate(columns)]
    sep = "-+-".join("-" * w for w in widths)
    print(f"\n  {_COL_SEP.join(h.ljust(w) for h, w in zip(columns, widths))}")
    print(f"  {sep}")
    for row in str_rows[:_ROW_LIMIT]:
        print("  " + _COL_SEP.join(v.ljust(w) for v, w in zip(row, widths)))
    if len(rows) > _ROW_LIMIT:
        print(f"\n  … and {len(rows) - _ROW_LIMIT} more rows.")


def _show_result(result: QueryResult, query: str) -> None:
    if result.error:
        print(f"\n[ERROR] {result.error}")
        if "no such table" in result.error.lower():
            print("  Hint: Ask 'list all tables' or use /schema to see available tables.")
        elif "no such column" in result.error.lower():
            print("  Hint: Ask 'show columns in <table>' to see available columns.")
        print()
        return

    # SQL
    print("\n─── Generated SQL " + "─" * 42)
    for line in result.sql.splitlines():
        print(f"  {line}")

    # Result table
    print("\n─── Result " + "─" * 49)
    if result.rows:
        _print_table(result.columns, result.rows)
        print(f"\n  {len(result.rows)} row{'s' if len(result.rows) != 1 else ''} returned.")
    else:
        print("  No rows returned.")

    # Natural language summary
    if result.natural_language_summary:
        print(f"\n  Summary: {result.natural_language_summary}")

    # Result diff (change since last run)
    if result.diff_summary:
        print(f"  {result.diff_summary}")

    # Anomaly warnings
    for w in result.anomaly_warnings:
        print(f"\n  [ANOMALY] {w}")

    # Performance hints
    for h in result.performance_hints:
        print(f"  [PERF HINT] {h}")

    # Meta badges
    badges = []
    if result.from_cache:
        badges.append("Cache hit (0 LLM tokens)")
    conf_pct = int(result.confidence * 100)
    badges.append(f"Confidence: {conf_pct}%")
    if result.execution_ms is not None:
        badges.append(f"{result.execution_ms:.0f}ms")
    if badges:
        print(f"\n  {' | '.join(badges)}")

    print("\n" + "─" * 60 + "\n")


def _feedback_prompt(nd: NexusData, query: str, result: QueryResult) -> Optional[QueryResult]:
    """Ask user if result is correct; collect feedback and re-run (up to 2 rounds)."""
    current = result
    for _round in range(2):
        try:
            fb = input("Feedback (press ENTER if correct, or describe what's wrong): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return current if current is not result else None
        if not fb:
            return current if current is not result else None
        print(f"\nUnderstood: \"{fb}\"\nRe-running…\n")
        try:
            new_result = nd.ask_with_feedback(query, current.sql or "", fb)
            _show_result(new_result, query)
            current = new_result
            if not new_result.error:
                return current  # success — stop
        except Exception as exc:
            print(f"\n[FEEDBACK ERROR] {exc}\n")
            return None
    return current


def _select_database(config: ConfigManager) -> str:
    """
    Prompt the user to select a database connection from the saved list.
    Returns the selected connection name.
    Auto-selects if only one DB is configured.
    Exits if no DBs are configured.
    """
    dbs = config.list_databases()

    if not dbs:
        # No named DBs — fall back to the legacy db.uri if it looks real
        if config.config.db.uri and config.config.db.uri != "sqlite:///sample.db":
            key = config.add_database("default", config.config.db)
            print(f"[Auto-migrated existing connection as '{key}']")
            return key
        # Offer to run the DB wizard inline rather than hard-exit
        print("\n[No database connections configured yet.]")
        try:
            answer = input("Run the database setup wizard now? [Y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            answer = "n"
        if answer in ("", "y", "yes"):
            from nexus_data.core.setup_wizard import run_add_db_wizard
            key = run_add_db_wizard(config)
            if key:
                return key
        print("No database configured. Exiting.")
        sys.exit(1)

    if len(dbs) == 1:
        name, uri = dbs[0]
        display = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
        print(f"Using database: {name} ({display})")
        return name

    # Multiple DBs — show selection menu
    print("\n--- Select Database Connection ---")
    active = config.config.active_db_name
    choices = []
    for name, uri in dbs:
        display = uri if "@" not in uri else uri.split("://")[0] + "://***@" + uri.split("@")[-1]
        suffix = " (last used)" if name == active else ""
        choices.append(f"{name}  —  {display}{suffix}")

    # Try questionary first, fall back to numbered input
    try:
        from nexus_data.core.setup_wizard import _rich
        if _rich():
            import questionary
            selected = questionary.select("Choose a database:", choices=choices).ask()
            if not selected:
                sys.exit("Cancelled.")
            # Extract the name (before " — ")
            return selected.split("  —  ")[0].strip()
    except Exception:
        pass

    # Plain-text fallback
    for i, c in enumerate(choices, 1):
        print(f"  {i}. {c}")
    while True:
        raw = input("Enter number: ").strip()
        if active and not raw:
            return active
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(dbs):
                return dbs[idx][0]
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(dbs)}.")


def run_cli() -> None:
    print("\n" + "=" * 60)
    print("  Welcome to the NexusData Interactive Data Console")
    print("  Natural language queries | /help for commands | exit to quit")
    print("=" * 60 + "\n")

    try:
        # ── 0. Identity — login / register ───────────────────────────────────
        cli_user = _cli_login()

        # ── 1. Load config (before creating NexusData) ───────────────────────
        config = ConfigManager()
        if not config.is_configured() or not config.list_databases():
            run_cli_setup(config)

        # Sync connections between config.json ↔ nexus_auth.db
        if cli_user:
            _sync_cli_connections(cli_user["id"], config)

        # ── 2. Select which DB to use this session ────────────────────────────
        db_name = _select_database(config)
        config.set_active_db(db_name)
        kb_dir = config.kb_dir_for(db_name)

        # ── 3. Initialize NexusData with per-DB KB directory ─────────────────
        nd = NexusData(kb_dir=kb_dir, interactive_setup=False, config_manager=config)
        print(f"\nConnecting to '{db_name}' and loading knowledge base…")
        nd.connect_and_initialize(db_uri=config.config.db.uri)
        lang = config.config.output_language
        print(f"Ready! Output language: {lang}. Ask me anything about your data.\n")

    except SystemExit:
        raise
    except Exception as exc:
        print(f"\n[FATAL ERROR] {exc}")
        traceback.print_exc()
        sys.exit(1)

    last_result: Optional[QueryResult] = None

    while True:
        try:
            query = input("You > ").strip()
            if not query:
                continue
            if query.lower() in ("exit", "quit", "q"):
                print("Goodbye!"); break

            # ── Slash commands ──────────────────────────────────────────────
            if query.startswith("/"):
                result = cmds.handle(query, nd, last_result)
                from nexus_data.models import QueryResult as QR
                if isinstance(result, QR):
                    _show_result(result, query)
                    last_result = result
                elif result:
                    print(result)
                continue

            print("Thinking…\n")
            result = nd.ask(query)

            # ── Clarification flow ──────────────────────────────────────────
            if result.is_clarification:
                print(f"\n[Clarification needed]\n  {result.clarification_question}\n")
                answer = input("Your answer > ").strip()
                if answer:
                    print("\nThinking…\n")
                    result = nd.ask(f"{query} [{answer}]")
                    _show_result(result, query)
                    last_result = result
                else:
                    print("Skipped.\n")
                continue

            # ── Confidence threshold warning ────────────────────────────────
            conf_thresh = nd._config.config.confidence_threshold
            if (not result.error and not result.is_clarification
                    and result.confidence < conf_thresh and result.sql):
                print(
                    f"\n[LOW CONFIDENCE: {int(result.confidence*100)}%] "
                    "This result may not fully answer your question. "
                    "Verify the SQL above before relying on it.\n"
                )

            _show_result(result, query)
            last_result = result

            # ── Structured request log (CLI source) ──────────────────────────
            log_pipeline_request(
                source="CLI",
                query=query,
                result_sql=result.sql or None,
                result_rows=len(result.rows),
                result_error=result.error,
                is_clarification=result.is_clarification,
                confidence=result.confidence,
                user_id=cli_user["id"] if cli_user else None,
            )

            # ── Feedback — offered for both successful and errored results ──
            if result.sql or result.error:
                new = _feedback_prompt(nd, query, result)
                if new:
                    last_result = new

        except KeyboardInterrupt:
            print("\nGoodbye!"); break
        except Exception as exc:
            print(f"\n[PIPELINE ERROR] {exc}\n")
            traceback.print_exc()
            print()


if __name__ == "__main__":
    run_cli()
