"""Hindi/Hinglish -> English request translation for selected endpoints.

This module changes natural-language text only. Protocol identifiers, tool-call
arguments, URLs, code and JSON fragments are deliberately never sent to the
translator. The gateway enables it per endpoint through ``fx_translate_language``.
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Awaitable, Callable

import httpx

Translator = Callable[[str], Awaitable[str]]

_DEVANAGARI_RE = re.compile(r"[\u0900-\u097f]")
_WORD_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
# High-signal Roman Hindi/Hinglish tokens. Ambiguous English words such as
# "main", "to", "is", "us" are intentionally absent.
_HINGLISH = {
    "aaj", "abhi", "acha", "achha", "apna", "apne", "aap", "aapka", "aapko",
    "agar", "aisa", "aise", "bahut", "bata", "batao", "batana", "bhi", "bhai",
    "chahiye", "dena", "dekho", "dikh", "dikhana", "fir", "hain", "hai", "hoga",
    "hona", "hoon", "hua", "hue", "jab", "jaldi", "kaam", "kaise", "kal", "karna",
    "karo", "karoge", "karta", "karte", "karwa", "karwana", "koi", "kuch", "kya",
    "kyun", "lekin", "liye", "mat", "mera", "mere", "mila", "mujhe", "nahi",
    "naya", "pehle", "phir", "raha", "rahe", "rahi", "rakhna", "sahi", "samajh",
    "sirf", "tab", "thik", "theek", "tha", "thi", "wala", "wali", "warna", "yaar",
    "ye", "yeh", "zara",
}

# Protected spans are copied byte-for-byte into the translated text.
_FENCED_RE = re.compile(r"```.*?```", re.S)
_INLINE_RE = re.compile(r"`[^`\n]*`")
_URL_RE = re.compile(r"(?:https?://|wss?://)[^\s<>()]+")

_RESPONSE_INSTRUCTION = (
    "The user's source language is Hindi/Hinglish. Respond in the user's original "
    "language and script unless the user explicitly requests another language."
)


def needs_translation(text: str) -> bool:
    """Return True for Devanagari or high-confidence Roman Hinglish text."""
    if not isinstance(text, str) or not text.strip():
        return False
    if _DEVANAGARI_RE.search(text):
        return True
    words = [w.lower() for w in _WORD_RE.findall(text)]
    hits = sum(1 for w in words if w in _HINGLISH)
    # One very distinctive imperative is sufficient; otherwise require two
    # signals so technical English does not get mistranslated.
    distinctive = {"batao", "chahiye", "mujhe", "nahi", "karo", "karna", "kyun", "warna"}
    return any(w in distinctive for w in words) or hits >= 2


def _json_ranges(text: str) -> list[tuple[int, int]]:
    """Locate balanced JSON-like object/array spans, respecting strings."""
    out: list[tuple[int, int]] = []
    n = len(text)
    i = 0
    while i < n:
        if text[i] not in "[{":
            i += 1
            continue
        start = i
        stack = [text[i]]
        i += 1
        quoted = False
        escaped = False
        while i < n and stack:
            ch = text[i]
            if quoted:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    quoted = False
            elif ch == '"':
                quoted = True
            elif ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                expected = "[" if ch == "]" else "{"
                if stack[-1] != expected:
                    break
                stack.pop()
            i += 1
        if not stack:
            candidate = text[start:i]
            try:
                json.loads(candidate)
            except Exception:
                continue
            out.append((start, i))
        else:
            i = start + 1
    return out


def _protected_ranges(text: str) -> list[tuple[int, int]]:
    ranges = [(m.start(), m.end()) for rx in (_FENCED_RE, _INLINE_RE, _URL_RE)
              for m in rx.finditer(text)]
    ranges.extend(_json_ranges(text))
    if not ranges:
        return []
    ranges.sort()
    merged = [ranges[0]]
    for start, end in ranges[1:]:
        pstart, pend = merged[-1]
        if start <= pend:
            merged[-1] = (pstart, max(pend, end))
        else:
            merged.append((start, end))
    return merged


def _chunks(text: str, limit: int = 4000) -> list[str]:
    """Split below Google's 5k request limit, preferring sentence boundaries."""
    if len(text) <= limit:
        return [text]
    out = []
    rest = text
    while len(rest) > limit:
        cut = max(rest.rfind(mark, 0, limit) for mark in ("\n", ". ", "! ", "? ", "। "))
        if cut < limit // 2:
            cut = rest.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
        else:
            cut += 1
        out.append(rest[:cut])
        rest = rest[cut:]
    if rest:
        out.append(rest)
    return out


async def _google_translate_batch(texts: list[str]) -> list[str]:
    """Translate many fragments in one HTTP call, preserving their boundaries."""
    if not texts:
        return []
    nonce = "9f4c7b2d"
    separators = [f"<<<CIEL_SEG_{nonce}_{i:06d}>>>" for i in range(1, len(texts))]
    joined_parts: list[str] = []
    for index, text in enumerate(texts):
        if index:
            joined_parts.append("\n\n" + separators[index - 1] + "\n\n")
        joined_parts.append(text)
    joined = "".join(joined_parts)
    if len(joined) > 4500:
        raise ValueError("translation batch exceeds safe upstream limit")
    params = {"client": "gtx", "sl": "auto", "tl": "en", "dt": "t"}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.post(
            "https://translate.googleapis.com/translate_a/single",
            params=params,
            data={"q": joined},
            headers={"user-agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        payload = response.json()
    translated = "".join(part[0] or "" for part in (payload[0] or []) if part)
    if not translated.strip():
        raise ValueError("translator returned empty text")
    parts = [translated]
    for separator in separators:
        next_parts: list[str] = []
        for part in parts:
            next_parts.extend(part.split(separator))
        parts = next_parts
    if len(parts) != len(texts):
        raise ValueError("translator changed segment boundary")
    return [part.strip() for part in parts]


async def _google_translate(text: str) -> str:
    return (await _google_translate_batch([text]))[0]


async def _translate_fragment(text: str, translator: Translator) -> str:
    if not needs_translation(text):
        return text
    # Translation services commonly strip their input. Keep boundary whitespace
    # outside the request so protected code/URL spans never get glued to prose.
    match = re.match(r"^(\s*)(.*?)(\s*)$", text, re.S)
    if match is None:  # defensive; the pattern always matches a string
        return text
    lead, core, trail = match.groups()
    if not core:
        return text
    translated = []
    for chunk in _chunks(core):
        translated.append(await translator(chunk))
    return lead + "".join(translated) + trail


async def translate_text(text: str, translator: Translator = _google_translate) -> tuple[str, bool]:
    """Translate only unprotected natural-language spans in ``text``."""
    if not needs_translation(text):
        return text, False
    ranges = _protected_ranges(text)
    out: list[str] = []
    pos = 0
    changed = False
    for start, end in ranges + [(len(text), len(text))]:
        plain = text[pos:start]
        if plain:
            converted = await _translate_fragment(plain, translator)
            out.append(converted)
            changed = changed or converted != plain
        if end > start:
            out.append(text[start:end])
        pos = end
    return "".join(out), changed


async def _translate_content(content, translator: Translator, *, allow_tool_blocks: bool = True):
    changed = False
    if isinstance(content, str):
        return await translate_text(content, translator)
    if not isinstance(content, list):
        return content, False
    out = []
    for block in content:
        if not isinstance(block, dict):
            out.append(block)
            continue
        item = dict(block)
        kind = item.get("type")
        if kind == "text" and isinstance(item.get("text"), str):
            item["text"], did = await translate_text(item["text"], translator)
            changed |= did
        elif allow_tool_blocks and kind == "tool_result" and isinstance(item.get("content"), (str, list)):
            item["content"], did = await _translate_content(item["content"], translator)
            changed |= did
        # tool_use.input, image/base64 and unknown structured blocks stay exact.
        out.append(item)
    return out, changed


async def _translate_tools(tools, translator: Translator):
    if not isinstance(tools, list):
        return tools, False
    out, changed = [], False
    for tool in tools:
        if not isinstance(tool, dict):
            out.append(tool)
            continue
        item = json.loads(json.dumps(tool))
        target = item.get("function") if isinstance(item.get("function"), dict) else item
        if isinstance(target.get("description"), str):
            target["description"], did = await translate_text(target["description"], translator)
            changed |= did
        out.append(item)
    return out, changed


def _add_response_instruction(body: dict, kind: str) -> None:
    if kind == "anthropic":
        system = body.get("system")
        if isinstance(system, str):
            if _RESPONSE_INSTRUCTION not in system:
                body["system"] = system.rstrip() + "\n\n" + _RESPONSE_INSTRUCTION
        elif isinstance(system, list):
            if not any(isinstance(b, dict) and _RESPONSE_INSTRUCTION in str(b.get("text", "")) for b in system):
                system.append({"type": "text", "text": _RESPONSE_INSTRUCTION})
        else:
            body["system"] = _RESPONSE_INSTRUCTION
        return
    messages = body.setdefault("messages", [])
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "system":
            content = message.get("content")
            if isinstance(content, str):
                if _RESPONSE_INSTRUCTION not in content:
                    message["content"] = content.rstrip() + "\n\n" + _RESPONSE_INSTRUCTION
                return
    messages.insert(0, {"role": "system", "content": _RESPONSE_INSTRUCTION})


async def _translate_request_impl(body_bytes: bytes, kind: str,
                                  translator: Translator) -> tuple[bytes, bool]:
    """Structural translation pass using the supplied fragment translator."""
    body = json.loads(body_bytes)
    if not isinstance(body, dict):
        return body_bytes, False
    messages = body.get("messages")
    if not isinstance(messages, list):
        return body_bytes, False
    out = json.loads(json.dumps(body))
    changed = False

    if kind == "anthropic":
        system = out.get("system")
        if isinstance(system, (str, list)):
            out["system"], did = await _translate_content(system, translator, allow_tool_blocks=False)
            changed |= did
    for message in out["messages"]:
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        # assistant natural text is context; its tool ids/arguments remain exact.
        # OpenAI tool output and Anthropic tool_result text are context too.
        if role in ("system", "user", "assistant", "tool"):
            message["content"], did = await _translate_content(message.get("content"), translator)
            changed |= did

    out["tools"], did = await _translate_tools(out.get("tools"), translator)
    changed |= did
    if not changed:
        return body_bytes, False
    _add_response_instruction(out, kind)
    return json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), True


class _BatchCollector:
    """Stand-in translator that records fragments and leaves stable markers."""
    def __init__(self):
        self.parts: list[str] = []

    async def __call__(self, text: str) -> str:
        index = len(self.parts)
        self.parts.append(text)
        return f"<<<CIEL_XLATE_{index:08d}>>>"


def _replace_markers(value, translations: list[str]):
    if isinstance(value, str):
        for index, translated in enumerate(translations):
            value = value.replace(f"<<<CIEL_XLATE_{index:08d}>>>", translated)
        return value
    if isinstance(value, list):
        return [_replace_markers(item, translations) for item in value]
    if isinstance(value, dict):
        return {key: _replace_markers(item, translations) for key, item in value.items()}
    return value


async def _translate_collected(parts: list[str]) -> list[str]:
    """Translate collected fragments in the fewest <=4.5k-character calls."""
    output: list[str] = []
    batch: list[str] = []
    size = 0
    for part in parts:
        added = len(part) + (40 if batch else 0)
        if batch and size + added > 4000:
            output.extend(await _google_translate_batch(batch))
            batch, size = [], 0
        batch.append(part)
        size += len(part) + (40 if len(batch) > 1 else 0)
    if batch:
        output.extend(await _google_translate_batch(batch))
    return output


async def translate_request(body_bytes: bytes, kind: str,
                            translator: Translator = _google_translate) -> tuple[bytes, bool]:
    """Translate natural-language fields in an Anthropic or OpenAI request body.

    The default Google path batches all request fragments, so a long conversation
    does not become one outbound translation request per message. Any failure fails
    closed: original request bytes are returned unchanged, never half-translated.
    """
    try:
        if translator is not _google_translate:
            return await _translate_request_impl(body_bytes, kind, translator)
        collector = _BatchCollector()
        template, changed = await _translate_request_impl(body_bytes, kind, collector)
        if not changed:
            return body_bytes, False
        translations = await _translate_collected(collector.parts)
        if len(translations) != len(collector.parts):
            raise ValueError("translation batch length mismatch")
        out = _replace_markers(json.loads(template), translations)
        return json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), True
    except Exception:
        return body_bytes, False
