# NexusData

**Talk to your database in plain English — locally, privately, and without sending your data anywhere.**

NexusData is a local-first, multi-user application that lets you query any SQL database using natural language. It runs entirely on your machine: no cloud uploads, no SaaS subscriptions, no Docker required. Your data never leaves your network.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Feature Overview](#feature-overview)
- [Prerequisites](#prerequisites)
- [Installation — From Scratch](#installation--from-scratch)
- [Step 1 · CLI Setup Wizard](#step-1--cli-setup-wizard)
- [Step 2 · Start the Web UI](#step-2--start-the-web-ui)
- [Step 3 · Create Your Account](#step-3--create-your-account)
- [Step 4 · Add a Database Connection](#step-4--add-a-database-connection)
- [Step 5 · Start Chatting](#step-5--start-chatting)
- [Using the CLI Directly](#using-the-cli-directly)
- [CLI Slash Commands](#cli-slash-commands)
- [Supported Databases](#supported-databases)
- [Supported LLM Providers](#supported-llm-providers)
- [Environment Variables Reference](#environment-variables-reference)
- [Project Structure](#project-structure)
- [Security Model](#security-model)
- [Data & File Storage](#data--file-storage)
- [Analytical Agent](#analytical-agent)
- [Troubleshooting](#troubleshooting)
- [Database Connectivity Matrix](#1-database-connectivity-matrix)
- [Local vs. Online Security Scenarios](#2-local-vs-online-security-scenarios)
- [Generating LLM API Keys](#3-generating-llm-api-keys)
- [Privacy Guardrails](#4-privacy-guardrails)

---

## How It Works

Every question you type passes through a 5-stage agentic pipeline before a single SQL query is executed.

```
Your Question
     │
     ▼
┌─────────────┐    ┌──────────────────┐    ┌────────────────────┐
│  Normalizer  │───▶│  Goal Identifier  │───▶│ Reference Resolver │
│  (sanitise)  │    │ (parse intent)   │    │ (map to schema)    │
└─────────────┘    └──────────────────┘    └────────────────────┘
                                                      │
                          ┌───────────────────────────┘
                          ▼
                 ┌────────────────┐    ┌──────────────────────────┐
                 │    Planner     │───▶│   Executor + Self-Healer  │
                 │ (build plan)   │    │ (run SQL, fix on error)   │
                 └────────────────┘    └──────────────────────────┘
                                                      │
                                                      ▼
                                              ┌──────────────┐
                                              │  Your Answer  │
                                              │ + Chart/CSV   │
                                              └──────────────┘
```

**Supporting systems** run alongside the pipeline:

| System                 | Role                                                                                            |
| ---------------------- | ----------------------------------------------------------------------------------------------- |
| **Knowledge Base**     | Per-connection long-term + short-term memory in markdown files                                  |
| **Vector Cache**       | LanceDB semantic cache — identical questions skip the LLM entirely                              |
| **Conversation Graph** | Tracks follow-up context ("those customers" → resolved to the previous query)                   |
| **Analytical Agent**   | Generates safe pandas/matplotlib code for charts and statistical analysis — no data sent to LLM |
| **Guardian**           | Blocks prompt-injection attempts and safety-policy violations before any LLM call               |
| **Self-Healer**        | If the generated SQL errors, feeds the traceback back to the LLM and retries (max 3 times)      |

---

## Feature Overview

- **Natural Language → SQL** across any SQLAlchemy-compatible database
- **5-stage reasoning pipeline** with real-time phase streaming in the UI
- **Pandas analytical agent** — ask for charts and statistical summaries; code runs locally, no data sent to the model
- **Semantic vector cache** — repeated or similar questions answered instantly from cache
- **Multi-user auth** with JWT sessions (7-day expiry), bcrypt passwords, and session revocation on logout
- **Per-user, per-conversation isolation** — each chat session has its own KB and memory
- **Multiple DB connections** per user, switchable from the sidebar
- **Conversation history** — every chat is saved; click any past conversation to reload it
- **Table & column descriptions** — annotate your schema from the UI to improve query accuracy
- **Schema file uploads** — upload `.md`/`.json` files with schema documentation per connection
- **Feedback loop** — 👍/👎 on any answer; wrong answers can be corrected and the pipeline re-runs
- **Export to CSV** — download results or save them locally under `data/`
- **Audit log** — optional per-session `audit.jsonl` for compliance
- **Prompt injection protection** — regex + AST guards on all user input
- **Appearance themes** — Onyx Dark (default), Frost Light, Midnight Blue
- **Fully local** — SQLite for auth/sessions, LanceDB for vectors, markdown for memory; zero external services

---

## Prerequisites

| Requirement    | Version        | Notes                                    |
| -------------- | -------------- | ---------------------------------------- |
| Python         | 3.11 or higher | Check with `python --version`            |
| pip            | Latest         | Bundled with Python                      |
| An LLM API key | —              | OpenAI, Anthropic, Google, or OpenRouter |

No Docker, no Redis, no Node.js, no cloud database required.

---

## Installation — From Scratch

### 1. Clone the repository

```bash
git clone https://github.com/your-org/nexusdata.git
cd nexusdata
```

### 2. Create and activate a virtual environment

**Windows:**

```bat
python -m venv .venv
.venv\Scripts\activate
```

**macOS / Linux:**

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

For database-specific drivers, install the matching extra:

| Database        | Extra install                            |
| --------------- | ---------------------------------------- |
| PostgreSQL      | `pip install psycopg2-binary`            |
| MySQL / MariaDB | `pip install pymysql`                    |
| MS SQL Server   | `pip install pyodbc`                     |
| Oracle          | `pip install cx_Oracle`                  |
| DuckDB          | already included in `requirements.txt`   |
| SQLite          | built into Python — nothing extra needed |

### 4. Copy the environment file

```bash
cp .env.example .env
```

The `.env` file is git-ignored. It holds your secrets and is never committed.
**You do not need to edit `.env` manually** — the setup wizard (next step) writes to it for you.

---

## Step 1 · CLI Setup Wizard

> **This step is required before the web UI can be used.** The wizard configures your LLM and runs once. After that, everything else can be managed from the browser.

```bash
python nexus_cli.py
```

The wizard will ask you to:

1. **Choose an LLM provider** — OpenAI, Anthropic, Google Gemini, or OpenRouter
2. **Paste your API key** — stored in `.env` as `NEXUS_LLM_API_KEY`; never written to `config.json`
3. **Select a model** — a menu of recommended models is shown per provider
4. **Add your first database connection** — paste a connection URI or enter credentials interactively

Once the wizard completes you will see:

```
Ready! Output language: English. Ask me anything about your data.
You >
```

You can type queries directly here (the CLI mode), or press `Ctrl+C` and move to the web UI. **The CLI is also a fully functional interface** — everything available in the UI can be done from the terminal.

### What the wizard configures

| Setting      | Stored in                                                        |
| ------------ | ---------------------------------------------------------------- |
| LLM provider | `.env` (`NEXUS_LLM_PROVIDER`) and `config.json`                  |
| LLM model    | `.env` (`NEXUS_LLM_MODEL`) and `config.json`                     |
| LLM API key  | `.env` (`NEXUS_LLM_API_KEY`) **only** — never in `config.json`   |
| DB URI       | `.env` (`NEXUS_DB_URI`, `NEXUS_DB_HOST`, etc.) and `config.json` |
| Secret key   | `.env` (`NEXUS_SECRET_KEY`) — auto-generated on first boot       |

---

## Step 2 · Start the Web UI

After CLI setup is complete, start the web UI server.

**Recommended — `uv` (fastest, no manual venv activation needed):**

```bash
uv run nexusdata-ui
```

Open your browser at:

```
http://localhost:7700
```

**Options:**

```bash
uv run nexusdata-ui --port 8080               # custom port
uv run nexusdata-ui --host 0.0.0.0            # expose on all interfaces (LAN access)
uv run nexusdata-ui --reload                  # auto-reload on code changes (dev mode)
```

**Alternative — activated virtualenv:**

```bash
# If you have the venv activated (pip install -e . was run):
nexusdata-ui
nexusdata-ui --port 8080 --reload
```

**Alternative — direct uvicorn:**

```bash
uv run uvicorn nexus_data.engine.ui_server:app --host 0.0.0.0 --port 7700 --reload
```

> **Default port is 7700.** The `--reload` flag restarts the server automatically when you edit source files — remove it in production.

For production (stable, multi-worker):

```bash
uv run uvicorn nexus_data.engine.ui_server:app --host 0.0.0.0 --port 7700 --workers 2
```

---

## Step 3 · Create Your Account

On first visit the app shows the login screen.

1. Click **Register**
2. Enter your name, email, and a password (minimum 8 characters)
3. Click **Create Account**

You are now logged in. Sessions last **7 days** and are revoked immediately on logout. Re-authentication is required after expiry.

> If the app shows **"Setup Required"** instead of the login screen, you have not completed Step 1. Run `python nexus_cli.py` first.

---

## Step 4 · Add a Database Connection

Every conversation is tied to a specific database. You need at least one connection before you can chat.

1. Click the **Settings** gear icon (bottom-left of the sidebar)
2. Go to the **Database** tab
3. Click **Add Connection**
4. Enter:
   - **Name** — a friendly label (e.g. `Sales DB`, `Production`)
   - **URI** — the SQLAlchemy connection string (see examples below)
5. Click **Save**

### Connection URI examples

| Database               | Example URI                                                                       |
| ---------------------- | --------------------------------------------------------------------------------- |
| SQLite (local file)    | `sqlite:///./mydata.db`                                                           |
| SQLite (absolute path) | `sqlite:////Users/me/data/sales.db`                                               |
| PostgreSQL             | `postgresql://user:password@localhost:5432/dbname`                                |
| MySQL                  | `mysql+pymysql://user:password@localhost:3306/dbname`                             |
| MS SQL Server          | `mssql+pyodbc://user:password@server/dbname?driver=ODBC+Driver+17+for+SQL+Server` |
| DuckDB                 | `duckdb:///./analytics.duckdb`                                                    |

> **Passwords in URIs are masked** in the UI — they are stored locally in `nexus_auth.db` and never displayed.

### Annotating your schema (optional but recommended)

Better descriptions = more accurate queries. In the **Database** tab, click **Edit** on any connection to add:

- **Table Descriptions** — JSON object mapping table names to plain-English descriptions
- **Column Descriptions** — nested JSON: `{ "table": { "column": "description" } }`
- **Notes** — free-text context about the data (date formats, business rules, etc.)
- **Schema file upload** — upload a `.md`, `.txt`, or `.json` file with full schema documentation

```json
// Table Descriptions example
{
  "orders": "One row per customer purchase. Includes status and totals.",
  "order_items": "Line items within each order. Links to products table."
}
```

```json
// Column Descriptions example
{
  "orders": {
    "status": "Values: pending, confirmed, shipped, delivered, cancelled",
    "total_amount": "Stored in USD cents (divide by 100 for dollars)"
  }
}
```

---

## Step 5 · Start Chatting

1. Click **New Chat** in the sidebar
2. Select the database you want to query from the picker
3. Type a question in the input box and press **Enter**

### What you will see

As your question is processed, the UI streams live phase updates:

```
⏱ Normalizing input...          0.1s
⏱ Identifying goal...           0.8s
⏱ Resolving schema references...1.2s
⏱ Planning query...             0.4s
⏱ Executing SQL...              0.3s
```

Then the full answer appears with:

- The **generated SQL** (with a copy button)
- A **results table** (up to 50 rows)
- A **natural language summary**
- **Confidence %** and execution time
- **Anomaly warnings** (e.g. unusually large values)
- **Performance hints** (e.g. missing index suggestions)
- **👍 / 👎 feedback** buttons — thumbs down opens a correction flow
- **📊 Analyse** button — opens the analytical agent modal (see below)
- **Export CSV** button — downloads the result as a CSV file

### Editing a question

Hover over any of your previous messages and click the **pencil icon** to load the text back into the input. Pressing Enter re-runs the query with the edited text.

### Following up

You can ask follow-up questions naturally:

```
You > Show me the top 10 customers by revenue last quarter
You > Now filter those to only ones in California
You > What was the average order value for those customers?
```

The conversation graph resolves "those customers" back to the correct context automatically.

---

## Using the CLI Directly

The terminal CLI is a complete alternative to the web UI. After setup it drops you into an interactive REPL:

```
You > show me total sales by product category this month
You > /export csv
You > /bookmark monthly_sales
You > /explain
```

Start it any time with:

```bash
python nexus_cli.py
```

If you have multiple database connections configured, a selection menu appears before the REPL starts.

---

## CLI Slash Commands

| Command               | Description                                            |
| --------------------- | ------------------------------------------------------ |
| `/help`               | Show all available commands                            |
| `/databases`          | List all saved database connections                    |
| `/add-db`             | Interactive wizard to add a new database connection    |
| `/change-db`          | Switch to a different database connection this session |
| `/change-model`       | Change the LLM provider and model (writes to `.env`)   |
| `/language <lang>`    | Set the response language (e.g. `/language Spanish`)   |
| `/schema`             | View the profiled schema for the active database       |
| `/relations`          | View the inferred table relationship map               |
| `/export csv`         | Export the last query result to a CSV file             |
| `/export json`        | Export the last query result to a JSON file            |
| `/bookmark <name>`    | Save the last query + SQL as a named bookmark          |
| `/bookmarks`          | List all saved bookmarks                               |
| `/run <name>`         | Execute a saved bookmark (bypasses the LLM entirely)   |
| `/explain`            | Plain-English explanation of the last generated SQL    |
| `/clear-cache`        | Wipe the semantic vector cache                         |
| `/history`            | Show recent queries this session                       |
| `/graph-export`       | Export the conversation knowledge graph to JSON        |
| `exit` / `quit` / `q` | Exit the CLI                                           |

---

## Supported Databases

NexusData works with any database that has a SQLAlchemy driver.

| Database            | URI prefix            | Extra install                        |
| ------------------- | --------------------- | ------------------------------------ |
| **SQLite**          | `sqlite:///`          | — (built-in)                         |
| **PostgreSQL**      | `postgresql://`       | `pip install psycopg2-binary`        |
| **MySQL / MariaDB** | `mysql+pymysql://`    | `pip install pymysql`                |
| **MS SQL Server**   | `mssql+pyodbc://`     | `pip install pyodbc`                 |
| **Oracle**          | `oracle+cx_oracle://` | `pip install cx_Oracle`              |
| **DuckDB**          | `duckdb:///`          | included                             |
| **Snowflake**       | `snowflake://`        | `pip install snowflake-sqlalchemy`   |
| **BigQuery**        | `bigquery://`         | `pip install sqlalchemy-bigquery`    |
| **CockroachDB**     | `cockroachdb://`      | `pip install sqlalchemy-cockroachdb` |

---

This comprehensive guide covers every database engine supported by **NexusData**, the networking configurations required for local versus cloud environments, and step-by-step instructions for acquiring API keys from supported LLM providers.

## 1. Database Connectivity Matrix

NexusData leverages SQLAlchemy to interface with your data. Below are the connection strings and required drivers for all supported databases.

| Database            | URI Prefix            | Extra Install Required                |
| :------------------ | :-------------------- | :------------------------------------ |
| **SQLite**          | `sqlite:///`          | None (built into Python)              |
| **DuckDB**          | `duckdb:///`          | None (included in `requirements.txt`) |
| **PostgreSQL**      | `postgresql://`       | `pip install psycopg2-binary`         |
| **MySQL / MariaDB** | `mysql+pymysql://`    | `pip install pymysql`                 |
| **MS SQL Server**   | `mssql+pyodbc://`     | `pip install pyodbc`                  |
| **Oracle**          | `oracle+cx_oracle://` | `pip install cx_Oracle`               |
| **Snowflake**       | `snowflake://`        | `pip install snowflake-sqlalchemy`    |
| **BigQuery**        | `bigquery://`         | `pip install sqlalchemy-bigquery`     |
| **CockroachDB**     | `cockroachdb://`      | `pip install sqlalchemy-cockroachdb`  |

---

## 2. Local vs. Online Security Scenarios

The "local-first" nature of NexusData means the software runs on your machine, but the data can live anywhere.

### Scenario A: Local Development (Workbench, pgAdmin, XAMPP)

- **Host:** Use `localhost` or `127.0.0.1`.
- **Access:** No extra steps are usually needed as the app and DB are on the same machine.

### Scenario B: Cloud/Online Servers (Azure, AWS RDS, DigitalOcean)

- **IP Whitelisting:** Cloud providers block all external traffic by default. You **must** find your local public IP and add it to the "Inbound Rules" or "Firewall Rules" of your database instance.
- **Port Forwarding:** Ensure the specific port (e.g., `3306` for MySQL, `5432` for Postgres, `1433` for Azure) is open to your IP.
- **SSL Encryption:** For online connections, it is recommended to use encrypted URIs to protect data in transit.

---

## Supported LLM Providers

Configure your provider once via the CLI wizard. The model can be viewed (but not changed) from the UI — use `/change-model` in the CLI to switch.

| Provider       | Recommended Models                                            | Environment variable |
| -------------- | ------------------------------------------------------------- | -------------------- |
| **OpenAI**     | `gpt-4o`, `gpt-4o-mini`, `gpt-4-turbo`                        | `NEXUS_LLM_API_KEY`  |
| **Anthropic**  | `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`        | `NEXUS_LLM_API_KEY`  |
| **Google**     | `gemini-2.0-flash`, `gemini-1.5-pro`                          | `NEXUS_LLM_API_KEY`  |
| **OpenRouter** | `meta-llama/llama-3.3-70b-instruct`, `deepseek/deepseek-chat` | `NEXUS_LLM_API_KEY`  |

> **API key security:** The API key is stored **only** in `.env` and never appears in `config.json`, the database, or the UI. The UI displays only the first 4 characters followed by `****`.

---

## 3. Generating LLM API Keys

NexusData requires an API key to power its 5-stage agentic reasoning pipeline. Keys are stored safely in your `.env` file and never exposed in the UI or code.

### **Google Gemini** (Recommended for speed/cost)

1.  Visit [Google AI Studio](https://aistudio.google.com/).
2.  Log in with your Google Account.
3.  Click **"Get API key"** in the sidebar.
4.  Click **"Create API key in new project."**

### **OpenAI** (GPT-4o, GPT-4-turbo)

1.  Go to the [OpenAI Platform](https://platform.openai.com/).
2.  Navigate to **API Keys** in the left sidebar.
3.  Click **"+ Create new secret key."**
4.  Ensure you have added credits to your billing account.

### **Anthropic** (Claude 3.5 Sonnet, Claude 3 Opus)

1.  Go to the [Anthropic Console](https://console.anthropic.com/).
2.  Navigate to **Settings > API Keys**.
3.  Click **"Create Key."**

### **OpenRouter** (Llama 3, DeepSeek)

1.  Visit [OpenRouter.ai](https://openrouter.ai/).
2.  Go to **Keys** in the dashboard.
3.  Click **"Create Key."** OpenRouter allows you to access many different open-source models through a single API key.

---

## Environment Variables Reference

All variables live in `.env`. Most are written automatically — you rarely need to edit this file by hand.

```bash
# ── Application secret (auto-generated on first boot) ─────────────────────────
NEXUS_SECRET_KEY=          # 64-char hex; JWT signing key

# ── LLM (written by CLI wizard or /change-model) ──────────────────────────────
NEXUS_LLM_API_KEY=         # Your API key — NEVER commit this
NEXUS_LLM_PROVIDER=        # openai | anthropic | google | openrouter
NEXUS_LLM_MODEL=           # e.g. gpt-4o

# ── Active database (written by CLI wizard or /change-db) ─────────────────────
NEXUS_DB_URI=              # Full SQLAlchemy URI (overrides component vars below)
NEXUS_DB_DRIVER=           # e.g. postgresql+psycopg2
NEXUS_DB_HOST=             # e.g. localhost
NEXUS_DB_PORT=             # e.g. 5432
NEXUS_DB_NAME=             # database name
NEXUS_DB_USER=             # username
NEXUS_DB_PASSWORD=         # password
NEXUS_DB_ACTIVE=           # name of the active connection profile

# ── Server ────────────────────────────────────────────────────────────────────
NEXUS_CORS_ORIGINS=        # comma-separated allowed origins (default: localhost:3000)
NEXUS_RATE_LIMIT=60        # max requests per minute per IP

# ── Storage paths ─────────────────────────────────────────────────────────────
NEXUS_KB_DIR=./nexus_kb    # knowledge base root directory
NEXUS_AUTH_DB=./nexus_auth.db  # SQLite auth database path
NEXUS_DATA_DIR=./data      # exported files and uploads root

# ── Optional REST API key (for programmatic access) ───────────────────────────
NEXUS_API_KEY=
```

---

## Project Structure

```
nexusdata/
├── nexus_cli.py                  # Entry point: CLI REPL + first-time setup wizard
├── config.json                   # Non-secret config (LLM provider/model, DB names)
├── .env                          # Secrets (API keys, passwords) — git-ignored
├── .env.example                  # Template — safe to commit
├── requirements.txt
├── pyproject.toml
│
├── nexus_data/
│   ├── orchestrator.py           # NexusData class — coordinates the 5-stage pipeline
│   ├── models.py                 # Pydantic models: QueryResult, TableMeta, etc.
│   │
│   ├── analyst/
│   │   └── agent.py              # PandasAgent — local pandas/matplotlib analysis
│   │
│   ├── auth/
│   │   ├── manager.py            # JWT create/decode/revoke; bcrypt hashing
│   │   └── models.py             # SQLite schema + CRUD for users/sessions/conversations
│   │
│   ├── core/
│   │   ├── config_manager.py     # ConfigManager — load/save config.json + .env overrides
│   │   ├── env_writer.py         # In-place .env writer; secret key bootstrapper
│   │   ├── setup_wizard.py       # Interactive CLI setup wizard (questionary/rich)
│   │   └── slash_commands.py     # CLI slash command router and handlers
│   │
│   ├── critic/
│   │   ├── guardian.py           # Prompt injection detection; safety rule enforcement
│   │   ├── self_healer.py        # SQL error → LLM fix loop (max 3 retries)
│   │   ├── anomaly_detector.py   # Statistical outlier detection on result sets
│   │   └── performance_advisor.py# Missing-index and query efficiency hints
│   │
│   ├── engine/
│   │   ├── app.py                # FastAPI application — all REST + SSE endpoints
│   │   ├── llm_controller.py     # Multi-provider LLM client (OpenAI / Anthropic / Google / OpenRouter)
│   │   └── ui_server.py          # Uvicorn launcher + static file serving
│   │
│   ├── kb/
│   │   ├── manager.py            # KBManager — 3-tier memory (long/short-term + session cache)
│   │   ├── vector_repo.py        # LanceDB semantic vector cache
│   │   ├── audit_log.py          # JSONL audit trail writer
│   │   ├── bookmarks.py          # Named bookmark store
│   │   ├── entity_tracker.py     # Cross-turn entity resolution
│   │   ├── graph_store.py        # Conversation graph persistence
│   │   └── kb_updater.py         # Background KB refresh from new conversations
│   │
│   ├── librarian/
│   │   ├── connector.py          # SQLAlchemy engine factory
│   │   ├── introspector.py       # Schema introspection + profiling
│   │   └── profiler.py           # Column statistics (min/max/avg/cardinality)
│   │
│   ├── pipeline/
│   │   ├── normalizer.py         # Stage 1: input sanitisation
│   │   ├── goal_identifier.py    # Stage 2: NL → structured JSON goal
│   │   ├── reference_resolver.py # Stage 3: goal terms → schema elements
│   │   ├── decomposer.py         # Stage 3b: complex query decomposition
│   │   ├── planner.py            # Stage 4: execution plan + SQL generation
│   │   └── executor.py           # Stage 5: SQL execution + result formatting
│   │
│   └── ui/
│       └── index.html            # Single-page chat UI (served by FastAPI)
│
├── nexus_kb/                     # Knowledge base files (auto-created; git-ignored)
│   └── {user_id}/{conv_id}/
│       ├── db_info.md            # Profiled schema documentation
│       ├── longterm_memory.md    # User preferences and business rules
│       └── shortterm_memory.md  # Recent conversation context
│
├── data/                         # Exported and uploaded files (git-ignored)
│   ├── uploads/{conn_id}/        # Schema files uploaded per connection
│   └── {conv_id}/{msg_id}/       # Per-message CSV exports
│
└── nexus_auth.db                 # SQLite auth database (auto-created; git-ignored)
```

---

## Security Model

| Area                 | Mechanism                                                                                                                                                                                     |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Passwords**        | bcrypt (cost factor 12) — never stored in plaintext                                                                                                                                           |
| **Sessions**         | JWT (HS256, 7-day expiry) + server-side session table — logout revokes immediately                                                                                                            |
| **API keys**         | SHA-256 hashed at rest; only the preview (first 12 chars + `...`) is stored readable                                                                                                          |
| **LLM API key**      | `.env` only — never written to `config.json`, the auth database, or served by any endpoint                                                                                                    |
| **Prompt injection** | `Guardian.check_user_input()` strips control characters and rejects 15+ injection patterns (jailbreak tokens, system-prompt overrides, `[INST]` wrappers, etc.) before any LLM call           |
| **SQL safety**       | Safety violations raised by the Guardian bypass the self-healer entirely — they are never "fixed" by the LLM                                                                                  |
| **Analytical code**  | `PandasAgent` AST-validates all generated pandas code before `exec()`. Forbidden: `os`, `sys`, `subprocess`, `open`, `eval`, `__import__`, network calls. Runs with restricted `__builtins__` |
| **File uploads**     | Only `.md`, `.txt`, `.json`, `.yaml`, `.yml` extensions accepted; stored under `data/uploads/{conn_id}/`                                                                                      |
| **Rate limiting**    | 60 requests/minute per IP (configurable via `NEXUS_RATE_LIMIT`); automatic eviction when > 10,000 IPs tracked                                                                                 |
| **CORS**             | Configurable via `NEXUS_CORS_ORIGINS`; defaults to localhost only                                                                                                                             |
| **Password in URIs** | Masked in all API responses (`postgresql://***@host/db`)                                                                                                                                      |

---

## Data & File Storage

All data is stored locally in your project directory.

```
nexus_auth.db          ← users, sessions, conversations, messages, feedback, API keys
nexus_kb/              ← per-conversation knowledge base (markdown files + LanceDB vectors)
data/
  uploads/             ← schema files you upload per DB connection
  {conv_id}/
    {msg_id}/
      export.csv       ← CSV exports saved from the chat UI
config.json            ← non-secret configuration (provider, model, DB names)
.env                   ← secrets (API keys, passwords, secret key)
```

### Clearing data

From the UI: **Settings → Profile → Danger Zone → Clear All Conversations & Data**

This deletes:

- All conversation records and messages
- All exported CSV files under `data/`
- All in-memory pipeline instances

This does **not** delete:

- The knowledge base (`nexus_kb/`) — your profiled schema and long-term memory are preserved
- Your database connections and their metadata
- Your user account

From the CLI: individual conversations can be deleted, or use `/clear-cache` to wipe the vector cache.

---

## Analytical Agent

When your question requires statistical analysis or a chart, click **📊 Analyse** on any result row.

The analytical agent:

1. Takes the SQL result data (rows + columns) already in memory
2. Sends only the **column names and a 5-row sample** to the LLM — never the full dataset
3. Receives Python/pandas code back from the LLM
4. **Validates the code with AST** — blocks all forbidden operations before execution
5. Executes the code locally with a restricted sandbox
6. Returns the analysis text and an optional chart image (base64 PNG)

### What you can ask

```
"Show total revenue by month as a bar chart"
"What is the correlation between order value and customer age?"
"Summarise the top 10 rows by spend"
"Show a pie chart of sales by region"
"Calculate month-over-month growth rate"
```

### Safety constraints

The generated code may only use: `pandas`, `numpy`, `matplotlib`, `io`, `base64`, `math`, `statistics`, `datetime`, `collections`, `itertools`, `functools`.

All of the following are blocked at the AST level and will raise an error if the LLM tries to use them: `os`, `sys`, `subprocess`, `socket`, `open()`, `exec()`, `eval()`, `__import__()`, `compile()`, `getattr()`, `input()`, any network call.

---

## 4. Privacy Guardrails

Regardless of the provider or database type, NexusData enforces a **Zero-Footprint** policy:

- **Metadata Only:** Only your table/column names and a 5-row sample are sent to the LLM to provide context.
- **Restricted Analytics:** The **Analytical Agent** (for charts and math) executes Python/Pandas code locally in a restricted sandbox—your full datasets are never uploaded to the cloud.
- **Prompt Protection:** The **Guardian** system blocks prompt-injection attempts before they ever reach the LLM.

---

## Troubleshooting

### "Setup Required" screen appears in the browser

The CLI setup wizard has not been completed. Run:

```bash
python nexus_cli.py
```

Follow the prompts to configure your LLM provider and API key, then refresh the browser.

---

### `ModuleNotFoundError` on startup

Your virtual environment is not activated, or dependencies are not installed:

```bash
source .venv/bin/activate     # macOS/Linux
.venv\Scripts\activate        # Windows

pip install -r requirements.txt
```

---

### "Authentication failed (401)" from the LLM

Your API key in `.env` is incorrect or has expired. Update it:

```bash
python nexus_cli.py
# Then: /change-model
```

Or edit `.env` directly:

```
NEXUS_LLM_API_KEY=your-new-key-here
```

Then restart the server.

---

### Database connection fails

1. Verify the URI format matches the examples in [Supported Databases](#supported-databases)
2. Ensure the required driver is installed (`psycopg2-binary` for Postgres, etc.)
3. Check the host/port/credentials are reachable from your machine
4. For SQLite, verify the file path exists and is readable

---

### Port 8000 already in use

```bash
uvicorn nexus_data.engine.app:app --port 8080
```

Then open `http://localhost:8080` in your browser.

---

### Session expired — "Invalid or expired token"

Log out and log back in. Sessions last 7 days. There is no "remember me" option — this is by design for local-first security.

---

### Vector cache returning stale results

```
/clear-cache
```

This wipes the LanceDB semantic cache. The next query will re-run the full pipeline and re-populate the cache.

---

### Conversations not loading after restart

Each conversation's pipeline instance is recreated on first use after a server restart. The conversation history (messages, SQL, summaries) is persisted in `nexus_auth.db` and will reload correctly — the pipeline simply re-initialises on the first query.

---

## Technical Stack

| Component        | Technology                               |
| ---------------- | ---------------------------------------- |
| Backend          | FastAPI + Uvicorn                        |
| Auth             | JWT (python-jose) + bcrypt (passlib)     |
| Auth storage     | SQLite (WAL mode)                        |
| Vector cache     | LanceDB + sentence-transformers          |
| Analytical agent | pandas + matplotlib (local exec)         |
| DB connectivity  | SQLAlchemy 2.x                           |
| Config           | Pydantic V2 + python-dotenv              |
| Frontend         | Vanilla JS + Tailwind CSS (CDN) + jQuery |
| Language         | Python 3.11+                             |

---

## License

Apache License 2.0. See `LICENSE` for details.

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run the test suite (`pytest`)
5. Open a pull request

Please do not commit `.env`, `nexus_auth.db`, `nexus_kb/`, or `data/` — they are git-ignored for good reason.
