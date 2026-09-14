"""Custom LLM translation backends (2026-09-14).

Boss's requirement: translation via any LLM endpoint the user adds — URL,
key, model id, and wire format (Anthropic Messages or OpenAI Chat
Completions) are all per-backend settings, orderable against the built-in
Google backend, with per-backend proxy config, RPM pacing and a dynamic
max_output_tokens.

Safety contract (unchanged from language_translate): the CALLER only ever
hands this module natural-language prose fragments. Tool names, schemas,
arguments, IDs, URLs and code never reach a translator.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Awaitable, Callable

import httpx

# The exact-paste prompt Boss asked for: any AI must return ONLY the
# translated text, nothing added, nothing explained.
DEFAULT_SYSTEM_PROMPT = (
    "You are a translation engine. Translate the user's text to English. "
    "Return ONLY the translation, exactly and completely — no preamble, no "
    "explanation, no quotes, no notes. Preserve meaning, tone and formatting. "
    "Keep code, URLs, JSON, numbers, identifiers and technical terms exactly "
    "as they appear."
)

# Rough chars->tokens for the auto budget: Hindi Devanagari runs ~2.5 chars/
# token on modern tokenizers; English ~4. 2.8 is a safe middle.
_CHARS_PER_TOKEN = 2.8
_AUTO_MULTIPLIER = 1.5
_AUTO_BUFFER_TOKENS = 256

_TIMEOUT_SECONDS = 120.0


def auto_max_tokens(input_chars: int) -> int:
    """max_output_tokens for a chunk: input x 1.5 + buffer (0 = auto)."""
    tokens = max(1, int(input_chars / _CHARS_PER_TOKEN))
    return int(tokens * _AUTO_MULTIPLIER) + _AUTO_BUFFER_TOKENS


class _RPMBucket:
    """Per-backend request pacer. 0/None rpm = unlimited (no pacing)."""

    def __init__(self, rpm: int):
        self.rpm = int(rpm or 0)
        self._tokens = float(self.rpm) if self.rpm else 0.0
        self._window_start = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if not self.rpm:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._window_start
                if elapsed >= 60.0:
                    # new window: refill
                    self._window_start = now
                    self._tokens = float(self.rpm)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = 60.0 - elapsed
            # bucket empty in this window: wait it out, then retry
            await asyncio.sleep(max(0.05, min(wait, 1.0)))


def _proxies_from_row(row: dict) -> list[str]:
    """Ordered proxy URLs from a backend row's custom_proxies/priority."""
    try:
        custom = json.loads(row.get("custom_proxies") or "[]")
    except (TypeError, ValueError):
        custom = []
    try:
        priority = json.loads(row.get("proxy_priority") or "[]")
    except (TypeError, ValueError):
        priority = []
    urls: list[str] = []
    for pid in priority:
        if not isinstance(pid, str):
            continue
        if pid == "direct":
            urls.append("")  # explicit no-proxy exit
        elif pid.startswith("custom_"):
            try:
                u = custom[int(pid.split("_", 1)[1])]
            except (IndexError, TypeError, ValueError):
                continue
            if isinstance(u, str) and u and u not in urls:
                urls.append(u)
    if not urls and custom:
        urls = [u for u in custom if isinstance(u, str) and u]
    return urls


async def _default_http_call(client, url, headers, payload, timeout):
    r = await client.post(url, headers=headers, json=payload, timeout=timeout)
    return r.status_code, r.text


class LLMTranslator:
    """Builds a `Translator` callable from a translation_endpoints row."""

    def __init__(self, row: dict, http_call: Callable = _default_http_call):
        self.row = dict(row)
        self.name = self.row.get("name") or "llm-backend"
        self.api_mode = self.row.get("api_mode") or "chat_completions"
        self.url = (self.row.get("url") or "").rstrip("/")
        self.api_key = self.row.get("api_key") or ""
        self.model = self.row.get("model") or ""
        self.system_prompt = self.row.get("system_prompt") or DEFAULT_SYSTEM_PROMPT
        self.chunk_chars = max(256, int(self.row.get("chunk_chars") or 4000))
        self.max_output_tokens = max(0, int(self.row.get("max_output_tokens") or 0))
        self._bucket = _RPMBucket(self.row.get("rpm") or 0)
        self._http_call = http_call
        self._proxy_urls = _proxies_from_row(self.row)

    # -- request shape -----------------------------------------------------
    def _endpoint_url(self) -> str:
        if self.api_mode == "anthropic_messages":
            base = self.url if self.url.endswith("/v1") else f"{self.url}/v1"
            return f"{base}/messages"
        return f"{self.url}/chat/completions"

    def _headers(self) -> dict:
        if self.api_mode == "anthropic_messages":
            return {"x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"}
        auth = f"Bearer {self.api_key}" if self.api_key else ""
        h = {"content-type": "application/json"}
        if auth:
            h["authorization"] = auth
        return h

    def _payload(self, text: str) -> dict:
        est = self.max_output_tokens or auto_max_tokens(len(text))
        if self.api_mode == "anthropic_messages":
            return {"model": self.model, "max_tokens": est,
                    "system": self.system_prompt,
                    "messages": [{"role": "user", "content": text}]}
        return {"model": self.model, "max_tokens": est,
                "messages": [{"role": "system", "content": self.system_prompt},
                              {"role": "user", "content": text}],
                "stream": False}

    def _extract(self, status: int, body: str) -> str:
        if status != 200:
            raise RuntimeError(f"backend {self.name} HTTP {status}: {body[:200]}")
        try:
            d = json.loads(body)
        except (TypeError, ValueError):
            raise RuntimeError(f"backend {self.name} returned non-JSON") from None
        if self.api_mode == "anthropic_messages":
            content = d.get("content") or []
            texts = [b.get("text", "") for b in content
                     if isinstance(b, dict) and b.get("type") == "text"]
            out = "".join(texts)
        else:
            choices = d.get("choices") or []
            msg = (choices[0].get("message") or {}) if choices else {}
            out = msg.get("content") or ""
            if not isinstance(out, str):
                out = json.dumps(out)
        if not out.strip():
            raise RuntimeError(f"backend {self.name} returned empty translation")
        return out.strip()

    # -- public callable ----------------------------------------------------
    async def __call__(self, text: str) -> str:
        await self._bucket.acquire()
        url = self._endpoint_url()
        headers = self._headers()
        last_err: Exception | None = None
        # proxy chain: this backend's proxies in priority order; empty-string
        # entry = direct. Exhausted -> one bare direct attempt (proxy_fallback
        # mirrors the endpoint semantics but a translator with no working
        # exit must never silently return the source text).
        exits = list(self._proxy_urls) if self._proxy_urls else [""]
        for proxy_url in exits:
            try:
                if proxy_url:
                    transport = httpx.AsyncHTTPTransport(proxy=proxy_url)
                    async with httpx.AsyncClient(
                            verify=False, transport=transport,
                            timeout=_TIMEOUT_SECONDS) as client:
                        status, body = await self._http_call(
                            client, url, headers, self._payload(text),
                            _TIMEOUT_SECONDS)
                else:
                    async with httpx.AsyncClient(
                            verify=False, timeout=_TIMEOUT_SECONDS) as client:
                        status, body = await self._http_call(
                            client, url, headers, self._payload(text),
                            _TIMEOUT_SECONDS)
                return self._extract(status, body)
            except Exception as exc:  # proxy/transport failure -> next exit
                last_err = exc
                continue
        raise RuntimeError(
            f"backend {self.name} failed on all {len(exits)} exits: "
            f"{type(last_err).__name__}: {last_err}"
        ) from last_err


def translator_from_row(row: dict, http_call: Callable = _default_http_call):
    """Convenience: LLMTranslator (callable Translator)."""
    return LLMTranslator(row, http_call=http_call)
