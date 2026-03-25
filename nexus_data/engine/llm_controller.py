"""
nexus_data/engine/llm_controller.py
Multi-provider LLM controller with retry, language injection, and schema-aware SQL fix.
"""
from __future__ import annotations
import logging, time
from typing import Any, AsyncGenerator, Optional
import httpx
from nexus_data.core.config_manager import ConfigManager

logger = logging.getLogger(__name__)
_RETRYABLE = {429, 500, 502, 503, 504}


class LLMController:
    def __init__(self, config_manager: ConfigManager):
        self._cfg_mgr = config_manager
        self.config = config_manager.config.llm

    @property
    def output_language(self) -> str:
        return self._cfg_mgr.config.output_language

    def _lang_prefix(self) -> str:
        lang = self.output_language
        if lang.lower() == "english":
            return ""
        return f"IMPORTANT: Respond entirely in {lang}. SQL must stay in English but all explanations, summaries, and descriptions must be in {lang}.\n\n"

    def generate(self, system_prompt: str, user_prompt: str, max_retries: int = 2) -> str:
        """Generate with language injection + exponential-backoff retry."""
        lang_pfx = self._lang_prefix()
        if lang_pfx:
            system_prompt = lang_pfx + system_prompt

        logger.info("LLM: %s / %s", self.config.provider, self.config.model_name)
        last_exc: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                if self.config.provider == "openai":
                    return self._call_openai(system_prompt, user_prompt)
                elif self.config.provider == "anthropic":
                    return self._call_anthropic(system_prompt, user_prompt)
                elif self.config.provider == "google":
                    return self._call_google(system_prompt, user_prompt)
                elif self.config.provider == "openrouter":
                    return self._call_openrouter(system_prompt, user_prompt)
                else:
                    raise ValueError(f"Unsupported provider: {self.config.provider}")
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                code = exc.response.status_code
                if code == 401 or code == 403:
                    raise RuntimeError(
                        f"Authentication failed ({code}) for provider '{self.config.provider}'. "
                        "Check your API key in config.json."
                    ) from exc
                if code == 404:
                    raise RuntimeError(
                        f"Model '{self.config.model_name}' not found for provider '{self.config.provider}'. "
                        "Update model_name in config.json or use /change-model."
                    ) from exc
                if code in _RETRYABLE and attempt < max_retries:
                    time.sleep(2 ** attempt); continue
                raise
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt < max_retries:
                    time.sleep(2 ** attempt); continue
                raise
        raise last_exc  # type: ignore

    def generate_sql_fix(self, bad_sql: str, db_error: Any, db_info: str = "",
                         original_query: str = "") -> str:
        schema = f"\n## Database Schema\n{db_info}\n" if db_info else ""
        original = f"\n## Original User Question\n{original_query}\n" if original_query else ""
        sys_p = ("You are an expert SQL DBA. Fix the broken SQL so it correctly answers the user's question. "
                 "Output ONLY the raw corrected SQL — no markdown, no explanation. Must be SELECT.")
        user_p = f"{schema}{original}## Broken SQL\n{bad_sql}\n\n## Error / Feedback\n{db_error}\n\n## Fixed SQL"
        import re as _re
        sql = self.generate(sys_p, user_p)
        sql = _re.sub(r"```[a-zA-Z0-9_]*", "", sql, flags=_re.IGNORECASE).replace("```", "").strip()
        if sql.lower().startswith("sql\n"):
            sql = sql[4:].strip()
        return sql

    def summarise_result(self, query: str, sql: str, columns: list, rows: list) -> str:
        """Generate a plain-language answer from query results."""
        preview = rows[:5] if rows else []
        sys_p = "You are a data analyst. Write a concise natural-language answer to the user's question based on the SQL result. 1-3 sentences max."
        user_p = f"Question: {query}\nSQL used: {sql}\nColumns: {columns}\nRows (preview): {preview}\nTotal rows: {len(rows)}"
        try:
            return self.generate(sys_p, user_p, max_retries=1)
        except Exception:
            return ""

    def stream(self, *_) -> AsyncGenerator[str, None]:
        raise NotImplementedError

    # ── Providers ─────────────────────────────────────────────────────────────

    def explain_sql(self, sql: str, query: str) -> str:
        """Return a plain-English narration of what a SQL query does."""
        sys_p = (
            "You are a SQL tutor. Explain step-by-step in plain English what the SQL does "
            "and how it answers the user's question. Be concise (max 5 bullet points)."
        )
        user_p = f"User question: {query}\n\nSQL:\n{sql}"
        try:
            return self.generate(sys_p, user_p, max_retries=1)
        except Exception:
            return "Could not generate explanation."

    # ── Providers ─────────────────────────────────────────────────────────────

    def _call_openai(self, sys: str, user: str) -> str:
        key = (self.config.api_key or "").strip()
        r = httpx.post("https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": self.config.model_name,
                  "messages": [{"role": "system", "content": sys}, {"role": "user", "content": user}],
                  "temperature": 0.0}, timeout=90)
        r.raise_for_status()
        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"OpenAI unexpected response structure: {data}") from e

    def _call_anthropic(self, sys: str, user: str) -> str:
        key = (self.config.api_key or "").strip()
        r = httpx.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                     "Content-Type": "application/json"},
            json={"model": self.config.model_name, "max_tokens": 8192,
                  "system": sys, "messages": [{"role": "user", "content": user}],
                  "temperature": 0.0}, timeout=90)
        r.raise_for_status()
        data = r.json()
        try:
            return data["content"][0]["text"]
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"Anthropic unexpected response structure: {data}") from e

    def _call_google(self, sys: str, user: str) -> str:
        key = (self.config.api_key or "").strip()
        model = self.config.model_name
        url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
               f"{model}:generateContent?key={key}")

        # Gemini 2.5 thinking models (flash/pro preview) do not accept temperature=0.0
        # and require thinkingConfig to disable the thinking budget for fast responses.
        is_thinking_model = "2.5" in model
        gen_config: dict = {"maxOutputTokens": 8192}
        if not is_thinking_model:
            gen_config["temperature"] = 0.0

        payload: dict = {
            "system_instruction": {"parts": [{"text": sys}]},
            "contents": [{"role": "user", "parts": [{"text": user}]}],
            "generationConfig": gen_config,
        }
        if is_thinking_model:
            # Disable thinking budget to reduce latency and cost for SQL generation
            payload["generationConfig"]["thinkingConfig"] = {"thinkingBudget": 0}

        r = httpx.post(url, headers={"Content-Type": "application/json"}, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()
        candidates = data.get("candidates", [])
        if not candidates:
            block = data.get("promptFeedback", {}).get("blockReason", "UNKNOWN")
            raise RuntimeError(f"Gemini no candidates. Block reason: {block}. Full response: {data}")
        try:
            return "".join(p.get("text", "") for p in candidates[0]["content"]["parts"])
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"Gemini unexpected response structure: {candidates[0]}") from e

    def _call_openrouter(self, sys: str, user: str) -> str:
        base = (self.config.api_base or "https://openrouter.ai/api/v1").rstrip("/")
        # Strip whitespace/newlines — a trailing newline in the key causes "illegal header" errors
        key = (self.config.api_key or "").strip()
        r = httpx.post(
            f"{base}/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/nexusdata",   # required by OpenRouter
                "X-Title": "NexusData",
            },
            json={
                "model": self.config.model_name,
                "messages": [
                    {"role": "system", "content": sys},
                    {"role": "user",   "content": user},
                ],
                "temperature": 0.1,   # 0.0 rejected by some OpenRouter models
                "max_tokens": 8192,
            },
            timeout=90,
        )
        r.raise_for_status()
        data = r.json()
        # OpenRouter may return an error object with status 200
        if "error" in data:
            raise RuntimeError(f"OpenRouter error: {data['error']}")
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"OpenRouter unexpected response structure: {data}") from e
