"""
nexus_data/librarian/introspector.py
Full schema extraction with:
- AI auto-description (LLM)
- File upload (md/txt)
- Confirmation + edit flow
- Table relation mapping (FK + inferred)
- Schema drift detection (MD5 hash)
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import Engine, inspect, text

from nexus_data.kb.manager import KBManager
from nexus_data.models import TableMeta, ColumnMeta, DatabaseProfile
from nexus_data.core.config_manager import ConfigManager
# Use terminal-safe helpers so the wizard works in VS Code / Git Bash too
from nexus_data.core.setup_wizard import _select, _text, _checkbox, _rich

logger = logging.getLogger(__name__)
_SAMPLE_LIMIT = 5
_ENUM_MAX_DISTINCT = 20       # columns with ≤ this many distinct values → list all
_NUMERIC_TYPES = ("int", "float", "decimal", "numeric", "real", "double", "number", "money", "bigint", "smallint")


class DatabaseIntrospector:
    def __init__(self, engine: Engine, kb: KBManager, config: ConfigManager, llm=None):
        self.engine = engine
        self.kb = kb
        self.config = config
        self.dialect = engine.dialect.name
        self.llm = llm

    # ── Schema extraction ─────────────────────────────────────────────────────

    def _extract_schema(self) -> DatabaseProfile:
        inspector = inspect(self.engine)
        dnt_t = self.config.config.db.do_not_touch_tables
        dnt_c = self.config.config.db.do_not_touch_columns
        saved_td = self.config.config.db.table_descriptions
        saved_cd = self.config.config.db.column_descriptions
        tables: List[TableMeta] = []

        # build indexed-column set per table
        indexed_cols: Dict[str, set] = {}
        for tbl in inspector.get_table_names():
            try:
                idxs = inspector.get_indexes(tbl)
                indexed_cols[tbl] = {c for idx in idxs for c in idx.get("column_names", []) if c}
            except Exception:
                indexed_cols[tbl] = set()

        for table_name in inspector.get_table_names():
            if table_name in dnt_t:
                continue
            restricted = dnt_c.get(table_name, [])
            pk_cols = inspector.get_pk_constraint(table_name).get("constrained_columns", [])
            fk_cols = [c for fk in inspector.get_foreign_keys(table_name)
                       for c in fk.get("constrained_columns", [])]
            tidx = indexed_cols.get(table_name, set())
            columns: List[ColumnMeta] = []

            for col in inspector.get_columns(table_name):
                cn = col["name"]
                if cn in restricted:
                    continue
                ctype = str(col["type"])
                col_extra = self._get_column_extra(table_name, cn, ctype)
                columns.append(ColumnMeta(
                    name=cn, type=ctype,
                    is_primary_key=cn in pk_cols,
                    is_foreign_key=cn in fk_cols,
                    is_indexed=cn in tidx,
                    is_enum=col_extra["is_enum"],
                    sample_values=col_extra["sample_values"],
                    all_values=col_extra["all_values"],
                    col_min=col_extra["col_min"],
                    col_max=col_extra["col_max"],
                    col_avg=col_extra["col_avg"],
                    description=saved_cd.get(table_name, {}).get(cn, "") or col.get("comment", "") or "",
                ))
            try:
                db_comment = inspector.get_table_comment(table_name).get("text", "") or ""
            except Exception:
                db_comment = ""
            tables.append(TableMeta(
                name=table_name, columns=columns,
                description=saved_td.get(table_name, "") or db_comment,
            ))
        return DatabaseProfile(tables=tables, dialect=self.dialect)

    def _get_column_extra(self, table: str, col: str, col_type: str) -> Dict[str, Any]:
        """Gather enum values, numeric stats — single connection per column."""
        result: Dict[str, Any] = {
            "is_enum": False, "sample_values": [], "all_values": [],
            "col_min": None, "col_max": None, "col_avg": None,
        }
        try:
            with self.engine.connect() as conn:
                distinct = conn.execute(
                    text(f'SELECT COUNT(DISTINCT "{col}") FROM "{table}"')
                ).scalar() or 0

                if 0 < distinct <= _ENUM_MAX_DISTINCT:
                    rows = conn.execute(
                        text(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL ORDER BY "{col}"')
                    ).fetchall()
                    result["all_values"] = [r[0] for r in rows]
                    result["sample_values"] = result["all_values"][:_SAMPLE_LIMIT]
                    result["is_enum"] = True
                else:
                    rows = conn.execute(
                        text(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT {_SAMPLE_LIMIT}')
                    ).fetchall()
                    result["sample_values"] = [r[0] for r in rows]

                if any(t in col_type.lower() for t in _NUMERIC_TYPES):
                    row = conn.execute(
                        text(f'SELECT MIN("{col}"), MAX("{col}"), AVG(CAST("{col}" AS FLOAT)) FROM "{table}"')
                    ).fetchone()
                    if row:
                        result["col_min"] = row[0]
                        result["col_max"] = row[1]
                        result["col_avg"] = round(float(row[2]), 2) if row[2] is not None else None
        except Exception:
            pass
        return result

    def _get_row_count(self, table: str) -> int:
        try:
            with self.engine.connect() as conn:
                return conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0
        except Exception:
            return -1

    # ── Schema hash (drift detection) ─────────────────────────────────────────

    def _compute_hash(self, profile: DatabaseProfile) -> str:
        sig = str(sorted([(t.name, sorted(c.name for c in t.columns)) for t in profile.tables]))
        return hashlib.md5(sig.encode()).hexdigest()

    def _check_drift(self, profile: DatabaseProfile) -> bool:
        """Returns True if schema has changed since last init."""
        new_hash = self._compute_hash(profile)
        old_hash = self.config.config.db.schema_hash
        if old_hash and old_hash != new_hash:
            print(f"\n  [WARN] Schema drift detected — database structure changed since last run.")
            return True
        self.config.config.db.schema_hash = new_hash
        self.config.save()
        return False

    # ── AI auto-description ───────────────────────────────────────────────────

    def _ai_auto_describe_all(self, profile: DatabaseProfile) -> DatabaseProfile:
        """Single LLM call for ALL tables/columns at once."""
        if not self.llm:
            print("  [WARN] No LLM available for auto-describe.")
            return profile

        print("\n  [AI] Generating descriptions for all tables and columns…")

        schema_payload: Dict[str, Any] = {}
        for t in profile.tables:
            schema_payload[t.name] = {
                "columns": [
                    {"name": c.name, "type": c.type, "is_pk": c.is_primary_key,
                     "is_fk": c.is_foreign_key, "samples": c.sample_values[:3]}
                    for c in t.columns
                ]
            }

        sys_p = (
            "You are a database documentation expert.\n"
            "Given the following database schema (with sample data), generate human-readable "
            "descriptions for every table and every column.\n\n"
            "Output a single JSON object — NO markdown, NO prose outside JSON:\n"
            "{\n"
            '  "table_name": {\n'
            '    "description": "One sentence about what this table stores.",\n'
            '    "columns": {\n'
            '      "column_name": "Brief phrase (<=10 words) describing this column."\n'
            "    }\n"
            "  }\n"
            "}\n\n"
            "Cover every table and every column. Be concise and business-friendly."
        )

        try:
            raw = self.llm.generate(sys_p, json.dumps(schema_payload, indent=2), max_retries=1)
        except Exception as exc:
            print(f"  [WARN] AI describe failed: {exc}")
            return profile

        clean = re.sub(r"```(?:json)?", "", raw, flags=re.I).replace("```", "").strip()
        match = re.search(r"\{[\s\S]*\}", clean)
        if not match:
            print("  [WARN] Could not parse AI descriptions — skipping.")
            return profile

        try:
            data: Dict[str, Any] = json.loads(match.group())
        except json.JSONDecodeError:
            print("  [WARN] JSON decode failed for AI descriptions — skipping.")
            return profile

        return self._apply_descriptions_dict(profile, data)

    # ── File upload parser ────────────────────────────────────────────────────

    def _parse_metadata_file(self, file_path: str, profile: DatabaseProfile) -> DatabaseProfile:
        """
        Parse md/txt/json metadata file and inject descriptions.
        Supports:
          - JSON: {"table": {"description": "...", "columns": {"col": "..."}}}
          - Markdown: ## TableName / [TableName] sections with col: desc lines
          - Plain text: table name as header, description on next line
        """
        try:
            content = Path(file_path).read_text(encoding="utf-8").strip()
        except Exception as e:
            print(f"  [ERROR] Could not read file: {e}")
            return profile

        if not content:
            print("  [WARN] Metadata file is empty.")
            return profile

        # ── Try JSON first ────────────────────────────────────────────────────
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                profile = self._apply_descriptions_dict(profile, data)
                return profile
        except json.JSONDecodeError:
            pass

        # ── Markdown / plain-text parser ──────────────────────────────────────
        table_lookup_ci: Dict[str, TableMeta] = {t.name.lower(): t for t in profile.tables}
        current_table: Optional[TableMeta] = None
        pending_desc_lines: List[str] = []   # collect description lines after header
        applied_tables, applied_cols = 0, 0

        def _flush_pending(tbl: Optional[TableMeta]) -> int:
            """Apply accumulated description lines to table."""
            if tbl and pending_desc_lines:
                tbl.description = " ".join(pending_desc_lines).strip()
                pending_desc_lines.clear()
                return 1
            pending_desc_lines.clear()
            return 0

        for line in content.splitlines():
            stripped = line.strip()

            # Blank line — flush pending description
            if not stripped:
                applied_tables += _flush_pending(current_table)
                continue

            # Section header: [TableName], ## TableName, # TableName, TableName:
            tm = re.match(
                r"^\[(.+?)\]$"                        # [table]
                r"|^#{1,3}\s+`?([^`\n]+?)`?\s*$"     # ## table  or  ## `table`
                r"|^([A-Za-z_]\w*)\s*:\s*$",          # TableName:
                stripped,
            )
            if tm:
                applied_tables += _flush_pending(current_table)
                raw_name = (tm.group(1) or tm.group(2) or tm.group(3) or "").strip().lower()
                # Strip trailing row-count notes like "(~100 rows)"
                raw_name = re.sub(r"\s*\(.*?\)\s*$", "", raw_name).strip()
                current_table = table_lookup_ci.get(raw_name)
                if not current_table:
                    # Partial match fallback
                    for key, t in table_lookup_ci.items():
                        if key in raw_name or raw_name in key:
                            current_table = t
                            break
                continue

            if current_table is None:
                continue

            # Explicit description prefix: "description: ..."  or  "> ..."
            dm = re.match(r"^(?:description[:\s]+|>\s*)(.+)", stripped, re.I)
            if dm:
                applied_tables += _flush_pending(current_table)
                current_table.description = dm.group(1).strip()
                applied_tables += 1
                continue

            # Column entry: "- col: desc" / "col | desc" / "col: desc" / "* col — desc"
            cm = re.match(
                r"^(?:column[.\s]+|[-*]\s+)?`?(\w+)`?\s*(?:[:\|]|—|-{1,2})\s*(.+)",
                stripped, re.I,
            )
            if cm:
                applied_tables += _flush_pending(current_table)
                col_name_raw = cm.group(1).strip().lower()
                col_desc = cm.group(2).strip()
                col_lookup_ci = {c.name.lower(): c for c in current_table.columns}
                col = col_lookup_ci.get(col_name_raw)
                if col:
                    col.description = col_desc
                    applied_cols += 1
                continue

            # Plain text line after a table header → accumulate as description
            pending_desc_lines.append(stripped)

        # Flush any remaining description lines
        applied_tables += _flush_pending(current_table)

        print(f"  File applied: {applied_tables} table description(s), {applied_cols} column description(s).")
        if applied_tables == 0 and applied_cols == 0:
            print(
                "  [WARN] Nothing matched. Check that table/column names in the file match the database.\n"
                "  Supported formats: JSON, Markdown (## TableName / - col: desc), or plain text."
            )
        return profile

    def _apply_descriptions_dict(self, profile: DatabaseProfile, data: Dict[str, Any]) -> DatabaseProfile:
        """
        Apply a descriptions dict to profile.
        Accepts:
          {"table": {"description": "...", "columns": {"col": "..."}}}
        or the nested AI response format.
        """
        table_lookup_ci = {t.name.lower(): t for t in profile.tables}
        applied_tables, applied_cols = 0, 0

        for tbl_key, tbl_val in data.items():
            t = table_lookup_ci.get(tbl_key.lower())
            if not t:
                continue
            if isinstance(tbl_val, dict):
                desc = tbl_val.get("description", "")
                if desc:
                    t.description = str(desc)
                    applied_tables += 1
                col_descs: Dict[str, str] = tbl_val.get("columns", {})
                col_lookup_ci = {c.name.lower(): c for c in t.columns}
                for col_key, col_desc in col_descs.items():
                    c = col_lookup_ci.get(col_key.lower())
                    if c and col_desc:
                        c.description = str(col_desc)
                        applied_cols += 1

        print(f"  Applied: {applied_tables} table description(s), {applied_cols} column description(s).")
        return profile

    # ── Relation mapping ──────────────────────────────────────────────────────

    def _generate_relation_map(self, profile: DatabaseProfile) -> str:
        inspector = inspect(self.engine)
        lines: List[str] = ["## Table Relationships\n"]
        table_names = {t.name for t in profile.tables}
        has_any = False

        for t in profile.tables:
            try:
                fks = inspector.get_foreign_keys(t.name)
                for fk in fks:
                    from_cols = fk.get("constrained_columns", [])
                    ref_table = fk.get("referred_table", "")
                    ref_cols = fk.get("referred_columns", [])
                    if from_cols and ref_table:
                        lines.append(f"- `{t.name}.{from_cols[0]}` → `{ref_table}.{ref_cols[0] if ref_cols else 'id'}` *(FK)*")
                        has_any = True
            except Exception:
                pass

            for c in t.columns:
                if c.is_foreign_key:
                    continue
                if c.name.endswith("_id") or c.name.endswith("_fk"):
                    ref = re.sub(r"_(id|fk)$", "", c.name)
                    if ref in table_names:
                        lines.append(f"- `{t.name}.{c.name}` → `{ref}.id` *(inferred)*")
                        has_any = True

        if not has_any:
            lines.append("- No relationships detected.")

        return "\n".join(lines) + "\n"

    # ── Confirmation flow (terminal-safe) ─────────────────────────────────────

    def _confirm_descriptions(self, profile: DatabaseProfile) -> DatabaseProfile:
        """Show generated descriptions, let user accept / edit / re-generate."""
        print("\n" + "─" * 60)
        print("  Generated Metadata Preview")
        print("─" * 60)
        for t in profile.tables:
            print(f"\n  Table: {t.name}")
            print(f"    Description: {t.description or '(none)'}")
            for c in t.columns:
                if c.description:
                    print(f"    Column {c.name}: {c.description}")
        print("\n" + "─" * 60)

        choice = _select(
            "How would you like to proceed?",
            choices=["accept", "edit", "regen", "upload"],
            default="accept",
        )
        # Map short keys back for readability
        # _select returns the choice string directly

        if choice == "accept":
            return profile

        if choice == "regen":
            profile = self._ai_auto_describe_all(profile)
            return self._confirm_descriptions(profile)

        if choice == "upload":
            fp = _text("Path to .md, .txt, or .json metadata file:")
            if fp:
                profile = self._parse_metadata_file(fp.strip(), profile)
            return self._confirm_descriptions(profile)

        if choice == "edit":
            for t in profile.tables:
                ans = _text(f"Table '{t.name}' description (ENTER to keep):", default=t.description or "")
                if ans is not None:
                    t.description = ans

                for c in t.columns:
                    if c.is_primary_key or c.is_foreign_key or len(t.columns) <= 8:
                        ans = _text(f"  Column '{c.name}' description (ENTER to keep):",
                                    default=c.description or "")
                        if ans is not None:
                            c.description = ans

            return self._confirm_descriptions(profile)

        return profile

    def _save_descriptions_to_config(self, profile: DatabaseProfile) -> None:
        """Persist table/column descriptions to config.json and sync to active DB entry."""
        td: Dict[str, str] = {}
        cd: Dict[str, Dict[str, str]] = {}
        for t in profile.tables:
            if t.description:
                td[t.name] = t.description
            for c in t.columns:
                if c.description:
                    cd.setdefault(t.name, {})[c.name] = c.description
        self.config.config.db.table_descriptions = td
        self.config.config.db.column_descriptions = cd
        # config.save() syncs config.db → databases[active_db_name] automatically
        self.config.save()
        logger.info("Descriptions saved: %d tables, %d tables with column descriptions.",
                    len(td), len(cd))

    # ── Markdown KB ───────────────────────────────────────────────────────────

    def _generate_markdown_kb(self, profile: DatabaseProfile) -> str:
        md = f"# Database Topology (Dialect: {self.dialect.upper()})\n\n"
        for t in profile.tables:
            count = self._get_row_count(t.name)
            count_note = f" (~{count:,} rows)" if count >= 0 else ""
            md += f"## Table: `{t.name}`{count_note}\n> {t.description or 'No description.'}\n\n**Columns:**\n"
            for c in t.columns:
                tags = []
                if c.is_primary_key: tags.append("PK")
                if c.is_foreign_key: tags.append("FK")
                if c.is_indexed:     tags.append("INDEXED")
                tag_str = f" [{', '.join(tags)}]" if tags else ""
                col_desc = f" — {c.description}" if c.description else ""

                if c.is_enum and c.all_values:
                    fmt = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in c.all_values)
                    value_note = f"  *(ENUM — allowed values: {fmt})*"
                elif c.sample_values:
                    fmt = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in c.sample_values)
                    value_note = f"  *(e.g. {fmt})*"
                else:
                    value_note = ""

                stats_parts = []
                if c.col_min is not None: stats_parts.append(f"min={c.col_min}")
                if c.col_max is not None: stats_parts.append(f"max={c.col_max}")
                if c.col_avg is not None: stats_parts.append(f"avg={c.col_avg}")
                stats_note = f"  *(stats: {', '.join(stats_parts)})*" if stats_parts else ""

                md += f"- `{c.name}` ({c.type}){tag_str}{col_desc}{value_note}{stats_note}\n"
            md += "\n---\n"
        md += "\n" + self._generate_relation_map(profile)
        return md

    # ── Main ──────────────────────────────────────────────────────────────────

    def initialize(self, interactive: bool = True) -> DatabaseProfile:
        print(f"\n[Introspector] Analyzing DB schema…")
        profile = self._extract_schema()
        print(f"  Found {len(profile.tables)} table(s).")

        self._check_drift(profile)

        if interactive:
            has_desc = any(t.description for t in profile.tables)

            if not has_desc:
                choice = _select(
                    "Table/column metadata not found. How would you like to set them up?",
                    choices=["ai", "file", "manual", "skip"],
                    default="ai",
                )

                if choice == "ai":
                    profile = self._ai_auto_describe_all(profile)
                    profile = self._confirm_descriptions(profile)
                    self._save_descriptions_to_config(profile)

                elif choice == "file":
                    fp = _text("Path to .md, .txt, or .json metadata file:")
                    if fp:
                        profile = self._parse_metadata_file(fp.strip(), profile)
                    profile = self._confirm_descriptions(profile)
                    self._save_descriptions_to_config(profile)

                elif choice == "manual":
                    profile = self._confirm_descriptions(profile)
                    self._save_descriptions_to_config(profile)

                # skip → write KB with no descriptions (no save to config needed)

            else:
                print("  Descriptions loaded from config.json.")

        # Always regenerate and write the KB markdown
        md = self._generate_markdown_kb(profile)
        self.kb.write_db_info(md)
        logger.info("db_info.md written to Knowledge Base (%s).", self.kb._dir)
        return profile

    def refresh(self) -> DatabaseProfile:
        """Re-run full interactive init (used by /update-table-info command)."""
        # Clear stored descriptions so the wizard re-runs
        self.config.config.db.table_descriptions = {}
        self.config.config.db.column_descriptions = {}
        self.config.save()
        return self.initialize(interactive=True)

    def apply_descriptions(self, descriptions: Dict[str, Any]) -> None:
        """
        Programmatic API for non-interactive metadata update (used by REST API).
        Accepts: {"table": {"description": "...", "columns": {"col": "..."}}}
        Updates the KB immediately — no wizard, no interaction.
        """
        profile = self._extract_schema()
        profile = self._apply_descriptions_dict(profile, descriptions)
        self._save_descriptions_to_config(profile)
        md = self._generate_markdown_kb(profile)
        self.kb.write_db_info(md)
        logger.info("Schema descriptions updated programmatically via apply_descriptions().")
