"""
nexus_data/analyst/agent.py
Senior-level pandas-based analytical agent. Generates high-quality pandas/matplotlib
code via LLM, validates it with AST, executes in a restricted sandbox, and returns
analysis text + optional base64 PNG chart.

No raw data is sent to LLM — only column names and a 5-row sample for code generation.
"""
from __future__ import annotations

import ast
import base64
import io
import logging
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Modules allowed inside generated code (import-level check only)
_ALLOWED_IMPORTS = frozenset([
    "scipy", "scipy.stats",
])

# Top-level names forbidden in generated code
_BANNED_CALLS = frozenset([
    "open", "exec", "eval", "__import__", "compile", "getattr", "setattr",
    "delattr", "input", "breakpoint", "vars", "dir", "globals",
    "locals", "help", "reload", "importlib",
])

# Chart-intent keywords for auto-detection
_CHART_RE = re.compile(
    r"\b(chart|plot|graph|visuali|bar\s*chart|line\s*chart|pie\s*chart|"
    r"scatter|histogram|heatmap|treemap|area\s*chart|box\s*plot)\b",
    re.I,
)

# Supported chart types
CHART_TYPES = ("bar", "line", "scatter", "pie", "histogram", "box", "heatmap", "area")

# Ensure matplotlib uses the non-interactive Agg backend once at import time
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as _plt_module  # noqa: E402  (after backend set)


@dataclass
class AnalysisResult:
    summary: str = ""
    chart_b64: Optional[str] = None   # base64-encoded PNG
    chart_mime: str = "image/png"
    error: Optional[str] = None


class PandasAgent:
    """Senior-level pandas/matplotlib analytical agent with AST-validated sandbox."""

    SYSTEM_PROMPT = textwrap.dedent("""\
        You are a senior data analyst and Python/pandas expert. Generate clean, accurate,
        production-quality Python code to analyse the DataFrame `df` and answer the user's
        question precisely.

        ## Available objects (already in scope — do NOT write import statements)
        - df           : pandas DataFrame containing all query results
        - pd           : pandas
        - np           : numpy
        - plt          : matplotlib.pyplot  (Agg backend — non-interactive)
        - stats        : scipy.stats  (use directly, e.g. stats.shapiro(data))
        - io, base64   : for PNG encoding
        - math, statistics, datetime, collections, itertools, functools

        ## Output contract
        1. Store your FINAL text answer in `analysis_text` (str). It must be informative,
           precise, and formatted with numbers rounded to 2 decimal places where relevant.
           Include units. If data is empty, say so explicitly.
        2. If chart_type is not None, create a matplotlib figure and store a base64-encoded
           PNG string in `chart_b64`. Use this exact pattern:
               fig, ax = plt.subplots(figsize=(10, 6))
               # ... plotting code ...
               plt.tight_layout()
               buf = io.BytesIO()
               plt.savefig(buf, format='png', bbox_inches='tight', dpi=150)
               plt.close(fig)
               chart_b64 = base64.b64encode(buf.getvalue()).decode()
           If no chart is needed, set: chart_b64 = None
        3. Always set chart_b64 explicitly (None or a string).

        ## Analytical skills — apply as appropriate
        - Descriptive stats: mean, median, std, min, max, quartiles, skewness, kurtosis
        - Distributions: histograms, normality checks (stats.shapiro if n≤5000)
        - Correlations: df.corr(), heatmaps, scatter matrix
        - Aggregations: groupby, pivot_table, resample (for time series)
        - Rankings: nlargest/nsmallest, rank(), cumsum/cumprod
        - Trend analysis: rolling means, pct_change(), period-over-period comparison
        - Outlier detection: IQR method, z-score
        - Categorical analysis: value_counts(), crosstab(), mode()

        ## Chart best practices
        - Set a descriptive title, axis labels, and legend (if multiple series)
        - colorblind-friendly palettes: tab10, viridis, coolwarm
        - Rotate x-axis labels 45° for bar charts with many categories
        - Sort time series by date before plotting; format x-axis dates
        - Pie charts only for ≤7 categories; else use bar
        - Round displayed numbers to 2 decimal places
        - Add value labels on bar charts when n_bars ≤ 20

        ## COLORS — strict rules (violations cause runtime errors)
        - NEVER pass a DataFrame column or Series as the `color=` argument.
        - Use ONLY: a single named color string ('steelblue', 'tab:blue', 'coral'),
          a fixed list of named colors (['tab:blue', 'tab:orange']),
          or a colormap slice: `plt.cm.tab10.colors[:n]` where n = number of bars/lines.
        - For grouped/stacked charts use `color=plt.cm.tab10.colors[:n_groups]`.

        ## Chart type guidance
        - bar        : categorical comparisons, value counts, group totals
        - line       : trends over time, continuous data, cumulative sums
        - scatter    : correlation between two numeric variables
        - pie        : proportions (≤7 slices only)
        - histogram  : distribution of a single numeric column
        - box        : distribution comparison across groups, outlier visualisation
        - heatmap    : correlation matrix or pivot table
        - area       : cumulative trends, stacked time series

        ## STRICT RULES
        1. Do NOT write import statements of any kind — all needed objects are pre-loaded.
        2. Do NOT use: open(), os, sys, subprocess, exec(), eval(), __import__(),
           any socket/network calls, file writes, pip, or importlib.
        3. Do NOT use print() — append to `analysis_text` instead.
        4. `df` is already defined — do NOT try to read files or create sample data.
        5. Always guard against empty DataFrames: `if df.empty: analysis_text = "No data."`
        6. Use `.copy()` to avoid SettingWithCopyWarning on sliced DataFrames.
        7. Wrap risky operations in try/except and include the error in `analysis_text`.
        8. Output ONLY the Python code — no markdown fences, no explanations.
    """)

    def __init__(self, llm) -> None:
        self._llm = llm

    # ── Public API ────────────────────────────────────────────────────────────

    def analyze(
        self,
        user_query: str,
        columns: List[str],
        rows: List[Any],
        wants_chart: bool = False,
        chart_type: Optional[str] = None,
    ) -> AnalysisResult:
        if not rows or not columns:
            return AnalysisResult(error="No data to analyse.")
        try:
            if not wants_chart:
                wants_chart = bool(_CHART_RE.search(user_query))
            if wants_chart and not chart_type:
                chart_type = self._select_chart_type(columns, rows, user_query)

            code = self._generate_code(user_query, columns, rows, wants_chart, chart_type)

            ok, reason = self._validate(code)
            if not ok:
                code = self._sanitise(code)
                ok, reason = self._validate(code)
                if not ok:
                    return AnalysisResult(
                        error=(
                            f"Code validation failed: {reason}. "
                            "Try rephrasing your question."
                        )
                    )

            return self._execute(code, columns, rows)

        except Exception as exc:
            logger.exception("PandasAgent.analyze failed")
            return AnalysisResult(
                error=(
                    f"Analysis failed: {exc}. "
                    "Try rephrasing your question or verify the data is compatible "
                    "with the requested analysis."
                )
            )

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _select_chart_type(columns: List[str], rows: List[Any], query: str) -> str:
        q = query.lower()
        if re.search(r"\bheatmap\b|\bcorrelation matrix\b", q):
            return "heatmap"
        if re.search(r"\bscatter\b|\bcorrelat\b|\brelationship\b|\bvs\b", q):
            return "scatter"
        if re.search(r"\bhistogram\b|\bdistribution\b|\bfrequen", q):
            return "histogram"
        if re.search(r"\bbox\b|\boutlier\b|\bspread\b|\brange\b", q):
            return "box"
        if re.search(r"\barea\b|\bcumulative\b|\bstacked\b", q):
            return "area"
        if re.search(r"\bpie\b|\bproportion\b|\bshare\b|\bbreakdown\b|\bcomposition\b", q):
            return "pie"
        if re.search(r"\btrend\b|\bover time\b|\btime series\b|\bmonth\b|\byear\b"
                     r"|\bweek\b|\bdate\b|\bday\b|\bquarter\b|\bperiod\b", q):
            return "line"

        col_lower = [c.lower() for c in columns]
        has_date_col = any(
            kw in c
            for c in col_lower
            for kw in ("date", "time", "month", "year", "week", "day", "period", "quarter")
        )
        if has_date_col:
            return "line"

        n_cols = len(columns)
        n_rows = len(rows)

        if n_cols == 2 and rows:
            try:
                sample = rows[0]
                vals = list(sample) if isinstance(sample, (list, tuple)) else list(sample.values())
                if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
                    return "scatter"
            except Exception:
                pass

        if n_cols == 1 and n_rows > 20:
            return "histogram"

        return "bar"

    def _generate_code(
        self,
        query: str,
        columns: List[str],
        rows: List[Any],
        wants_chart: bool,
        chart_type: Optional[str],
    ) -> str:
        sample = rows[:5]
        chart_instruction = (
            f"Chart requested: {chart_type}. Create a {chart_type} chart and store in chart_b64."
            if wants_chart and chart_type
            else "No chart needed — set chart_b64 = None."
        )
        user_prompt = (
            f"Columns: {columns}\n"
            f"Sample data (first {len(sample)} rows): {sample}\n"
            f"Total rows: {len(rows)}\n"
            f"User question: {query}\n"
            f"Chart instruction: {chart_instruction}\n\n"
            "Write the Python code now:"
        )
        raw = self._llm.generate(self.SYSTEM_PROMPT, user_prompt)
        raw = re.sub(r"^```[a-zA-Z0-9_]*\n?", "", raw.strip(), flags=re.MULTILINE)
        raw = re.sub(r"\n?```\s*$", "", raw.strip(), flags=re.MULTILINE)
        return raw.strip()

    @staticmethod
    def _sanitise(code: str) -> str:
        """Rewrite print() → analysis_text += and strip any import statements."""
        code = re.sub(
            r"\bprint\s*\((.+?)\)",
            lambda m: f'analysis_text += str({m.group(1)}) + "\\n"',
            code,
        )
        # Strip any import lines the LLM sneaked in (except scipy which is pre-loaded)
        code = re.sub(r"^\s*import\s+(?!scipy).*$", "", code, flags=re.MULTILINE)
        code = re.sub(r"^\s*from\s+(?!scipy).*$", "", code, flags=re.MULTILINE)
        return code

    @staticmethod
    def _strip_color_args(code: str) -> str:
        """Last-resort: remove color=/facecolor= kwargs so matplotlib uses defaults."""
        code = re.sub(r",?\s*\bcolor\s*=\s*(?:df\[.*?\]|[A-Za-z_]\w*(?:\[.*?\])?)", "", code)
        code = re.sub(r",?\s*\bfacecolor\s*=\s*(?:df\[.*?\]|[A-Za-z_]\w*(?:\[.*?\])?)", "", code)
        return code

    def _validate(self, code: str):
        """AST-walk the generated code and reject anything dangerous."""
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return False, f"Syntax error: {e}"

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = (
                    [alias.name for alias in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module or ""]
                )
                for name in names:
                    root = name.split(".")[0]
                    if root not in _ALLOWED_IMPORTS:
                        return False, f"Import '{name}' is not allowed"

            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in _BANNED_CALLS:
                        return False, f"Call '{node.func.id}' is not allowed"
                if isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name):
                        owner = node.func.value.id
                        if owner in ("os", "sys", "subprocess", "socket", "shutil"):
                            return False, f"Call '{owner}.{node.func.attr}' is not allowed"

        return True, None

    def _execute(self, code: str, columns: List[str], rows: List[Any]) -> AnalysisResult:
        import pandas as pd
        import numpy as np
        import math
        import statistics as statistics_mod
        import datetime
        import collections
        import itertools
        import functools

        try:
            from scipy import stats as _scipy_stats
        except Exception:
            _scipy_stats = None

        df = pd.DataFrame(rows, columns=columns)

        _print_lines: List[str] = []

        def _safe_print(*args, **kwargs) -> None:
            sep = kwargs.get("sep", " ")
            _print_lines.append(sep.join(str(a) for a in args))

        _builtins_src = __builtins__ if isinstance(__builtins__, dict) else vars(__builtins__)
        safe_builtins = {
            k: v for k, v in _builtins_src.items()
            if k not in _BANNED_CALLS and k not in ("open", "breakpoint", "input")
        }
        safe_builtins["print"] = _safe_print

        def _make_ns() -> Dict[str, Any]:
            return {
                "__builtins__": safe_builtins,
                "df": df,
                "pd": pd,
                "np": np,
                "plt": _plt_module,
                "io": io,
                "base64": base64,
                "math": math,
                "statistics": statistics_mod,
                "datetime": datetime,
                "collections": collections,
                "itertools": itertools,
                "functools": functools,
                "stats": _scipy_stats,
                "analysis_text": "",
                "chart_b64": None,
                "print": _safe_print,
            }

        ns = _make_ns()
        try:
            exec(code, ns)  # noqa: S102
        except (ValueError, TypeError) as exc:
            exc_str = str(exc).lower()
            if "color" in exc_str or "facecolor" in exc_str:
                # Color argument was a DataFrame column — strip and retry
                logger.warning("Chart color error (%s) — retrying without color args", exc)
                fixed_code = self._strip_color_args(code)
                ns2 = _make_ns()
                try:
                    exec(fixed_code, ns2)  # noqa: S102
                    ns = ns2
                except Exception as exc2:
                    return AnalysisResult(error=f"Execution error: {exc2}")
            else:
                return AnalysisResult(error=f"Execution error: {exc}")
        except Exception as exc:
            return AnalysisResult(error=f"Execution error: {exc}")

        analysis_text = str(ns.get("analysis_text", ""))
        if _print_lines:
            captured = "\n".join(_print_lines)
            analysis_text = f"{analysis_text}\n{captured}".strip() if analysis_text else captured

        return AnalysisResult(
            summary=analysis_text,
            chart_b64=ns.get("chart_b64"),
        )
