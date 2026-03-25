"""
nexus_data/kb/graph_store.py — SQL Semantic Graph Store
Parses executed SQL into a structured node/edge graph and persists query patterns.

Purpose:
  - Every successful SELECT is decomposed into tables (nodes), JOINs (edges),
    conditions, aggregations → stored as a "pattern".
  - When a new query targets the same tables/operation, the closest stored SQL
    is returned as a template example for the LLM (entity reuse).

Node types : table, column, value
Edge types  : join, filter, group_by, order_by, aggregation
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Lightweight SQL parser (no heavy dep beyond stdlib + optional sqlglot) ────

def _parse_sql_graph(sql: str) -> Dict[str, Any]:
    """
    Returns a graph dict:
    {
      "tables": ["users", "orders"],
      "joins": [{"left": "users", "right": "orders", "on": "users.id = orders.user_id"}],
      "conditions": ["category = 'laptop'"],
      "aggregations": ["COUNT", "SUM"],
      "groupings": ["users.id"],
      "orderings": ["total DESC"],
      "nodes": [...],
      "edges": [...]
    }
    Falls back to regex parsing if sqlglot unavailable.
    """
    try:
        return _parse_with_sqlglot(sql)
    except Exception:
        return _parse_with_regex(sql)


def _parse_with_sqlglot(sql: str) -> Dict[str, Any]:
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)

    tables = [t.name.lower() for t in tree.find_all(exp.Table) if t.name]
    joins: List[Dict] = []
    for j in tree.find_all(exp.Join):
        right = j.find(exp.Table)
        on_clause = j.find(exp.On)
        joins.append({
            "right": right.name.lower() if right else "",
            "on": str(on_clause).strip() if on_clause else "",
        })

    conditions: List[str] = []
    where = tree.find(exp.Where)
    if where:
        conditions = [str(c).strip() for c in where.find_all(exp.Condition)]
        if not conditions:
            conditions = [str(where.this).strip()]

    aggs: List[str] = []
    for agg_cls in (exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min):
        if tree.find(agg_cls):
            aggs.append(agg_cls.__name__.upper())

    groups = [str(g).strip() for g in tree.find_all(exp.Group)]
    orders = [str(o).strip() for o in tree.find_all(exp.Order)]

    nodes = [{"id": t, "type": "table"} for t in tables]
    edges = [{"from": joins[i-1]["right"] if i > 0 else tables[0],
              "to": j["right"], "type": "join", "on": j["on"]}
             for i, j in enumerate(joins)]

    return {
        "tables": tables,
        "joins": joins,
        "conditions": conditions,
        "aggregations": aggs,
        "groupings": groups,
        "orderings": orders,
        "nodes": nodes,
        "edges": edges,
    }


def _parse_with_regex(sql: str) -> Dict[str, Any]:
    sql_u = sql.upper()
    tables = re.findall(r"(?:FROM|JOIN)\s+([`\"\[]?\w+[`\"\]]?)", sql_u)
    tables = [t.strip('`"[]').lower() for t in tables]

    join_clauses = re.findall(r"JOIN\s+(\w+)\s+(?:AS\s+\w+\s+)?ON\s+([^WHERE^GROUP^ORDER^LIMIT]+)", sql, re.IGNORECASE)
    joins = [{"right": j[0].lower(), "on": j[1].strip()} for j in join_clauses]

    where_match = re.search(r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)", sql, re.IGNORECASE | re.DOTALL)
    conditions = [where_match.group(1).strip()] if where_match else []

    aggs = [kw for kw in ("COUNT", "SUM", "AVG", "MAX", "MIN") if kw in sql_u]
    groups = re.findall(r"GROUP BY\s+(.+?)(?:HAVING|ORDER BY|LIMIT|$)", sql, re.IGNORECASE)
    orders = re.findall(r"ORDER BY\s+(.+?)(?:LIMIT|$)", sql, re.IGNORECASE)

    nodes = [{"id": t, "type": "table"} for t in dict.fromkeys(tables)]
    edges = [{"from": tables[0] if tables else "", "to": j["right"], "type": "join", "on": j["on"]}
             for j in joins]

    return {
        "tables": list(dict.fromkeys(tables)),
        "joins": joins,
        "conditions": conditions,
        "aggregations": aggs,
        "groupings": groups,
        "orderings": orders,
        "nodes": nodes,
        "edges": edges,
    }


# ── Graph Store ────────────────────────────────────────────────────────────────

class SQLGraphStore:
    """
    Persists query patterns as a JSON graph. Supports:
    - save_pattern(intent, sql)
    - find_template(intent, tables) → optional SQL template for LLM
    - get_full_graph() → all nodes/edges for visualisation
    """
    _MAX_PATTERNS = 500

    def __init__(self, kb_dir: Optional[Path] = None):
        self._dir = kb_dir or Path("./nexus_kb")
        self._path = self._dir / "query_graph.json"
        self._patterns: List[Dict[str, Any]] = self._load()

    def _load(self) -> List[Dict[str, Any]]:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return []

    def _save(self) -> None:
        try:
            self._path.write_text(
                json.dumps(self._patterns[-self._MAX_PATTERNS:], indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("GraphStore save error: %s", exc)

    # ── Public API ────────────────────────────────────────────────────────────

    def save_pattern(self, intent: str, sql: str) -> None:
        """Parse SQL and store its graph pattern."""
        if not sql or not sql.strip().upper().startswith("SELECT"):
            return
        try:
            graph = _parse_sql_graph(sql)
            if not graph["tables"]:
                return
            pattern: Dict[str, Any] = {
                "intent": intent,
                "sql": sql,
                "tables": graph["tables"],
                "graph": graph,
            }
            # Avoid exact duplicate intents
            existing = [p["intent"] for p in self._patterns]
            if intent not in existing:
                self._patterns.append(pattern)
                self._save()
                logger.debug("GraphStore: saved pattern for '%s' (tables=%s)",
                             intent[:60], graph["tables"])
        except Exception as exc:
            logger.warning("GraphStore.save_pattern error: %s", exc)

    def find_template(
        self, intent: str, tables: List[str], min_overlap: float = 0.6
    ) -> Optional[str]:
        """
        Find a stored SQL template whose tables overlap >= min_overlap with
        the requested tables.  Returns the SQL string as a template hint, or None.
        """
        if not tables or not self._patterns:
            return None
        target = set(t.lower() for t in tables)
        best_score = 0.0
        best_sql: Optional[str] = None

        for p in reversed(self._patterns):  # most recent first
            stored = set(t.lower() for t in p.get("tables", []))
            if not stored:
                continue
            overlap = len(target & stored) / max(len(target | stored), 1)
            if overlap > best_score and overlap >= min_overlap:
                best_score = overlap
                best_sql = p["sql"]

        if best_sql:
            logger.info("GraphStore: found template (overlap=%.0f%%) for '%s'",
                        best_score * 100, intent[:60])
        return best_sql

    def get_full_graph(self) -> Dict[str, Any]:
        """
        Merge all stored patterns into a single unified node/edge graph.
        Useful for visualisation or KB mapping.
        """
        all_nodes: Dict[str, Dict] = {}
        all_edges: List[Dict] = []

        for p in self._patterns:
            graph = p.get("graph", {})
            for node in graph.get("nodes", []):
                nid = node["id"]
                if nid not in all_nodes:
                    all_nodes[nid] = node
            for edge in graph.get("edges", []):
                key = (edge.get("from"), edge.get("to"), edge.get("type"))
                if not any(
                    (e.get("from"), e.get("to"), e.get("type")) == key
                    for e in all_edges
                ):
                    all_edges.append(edge)

        return {
            "nodes": list(all_nodes.values()),
            "edges": all_edges,
            "pattern_count": len(self._patterns),
        }

    def export_graph_json(self, path: Optional[Path] = None) -> str:
        """Write full graph to a JSON file and return path string."""
        out = path or (self._dir / "kb_graph_export.json")
        data = self.get_full_graph()
        out.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("Graph exported to %s (%d nodes, %d edges)",
                    out, len(data["nodes"]), len(data["edges"]))
        return str(out)
