"""Any-language -> English request translation for selected endpoints.

This module changes natural-language text only. Protocol identifiers, tool-call
arguments, URLs, code and JSON fragments are deliberately never sent to the
translator. The gateway enables it per endpoint through ``fx_translate_language``.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import re
from typing import Awaitable, Callable
import unicodedata

import httpx
import langid
import pycld2

Translator = Callable[[str], Awaitable[str]]

_CHUNK_CHAR_LIMIT = 4000
_WAVE_CHUNK_LIMIT = 24
_MAX_PROXY_FALLBACKS = 3
_TRANSLATION_BACKENDS = (
    "https://translate.googleapis.com/translate_a/single",
    "https://translate.google.com/translate_a/single",
)
_DETECTION_SAMPLE_SIZE = 6000
_DETECTION_WINDOW_OVERLAP = 256
_ROUTE_TIMEOUT_SECONDS = 5.0


class LanguageTranslationError(RuntimeError):
    """The endpoint requires English, but translation could not be verified."""


@dataclass(frozen=True)
class TranslationReport:
    body: bytes
    changed: bool
    error: str = ""
    detected_language: str = ""

_NON_LATIN_LETTER_RE = re.compile(
    r"[\u0370-\u052f\u0590-\u08ff\u0900-\u109f\u1100-\u11ff"
    r"\u1200-\u137f\u1780-\u17ff\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]"
)
_WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]+(?:'[A-Za-zÀ-ÖØ-öø-ÿ]+)?")
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

# High-signal words for common Latin-script non-English inputs. This is not a
# language identifier; it is a conservative prefilter before Google's auto-detect.
_NON_ENGLISH_LATIN = {
    # Spanish / Portuguese
    "por", "favor", "revisa", "esta", "este", "ruta", "gracias", "hola", "verifica",
    "obrigado", "obrigada", "voce", "você", "nao", "não", "agora", "isso",
    # French
    "veuillez", "verifier", "vérifier", "cette", "merci", "bonjour", "avec", "pour",
    # German
    "bitte", "uberprufe", "überprüfe", "diese", "dieser", "danke", "nicht", "jetzt",
    # Italian
    "per", "favore", "controlla", "questo", "questa", "grazie", "ciao",
    # Indonesian / Malay / Turkish / Vietnamese (common signals)
    "tolong", "periksa", "ini", "terima", "kasih", "lütfen", "kontrol", "eder", "misin",
    "vui", "lòng", "kiểm", "tra", "này", "cảm", "ơn",
}

_TECHNICAL_ENGLISH = {
    "api", "argument", "arguments", "auth", "authentication", "backend", "boss",
    "bug", "claude", "code", "config", "configuration", "context", "database", "debug",
    "deploy", "docker", "endpoint", "english", "error", "fix", "function", "gateway",
    "github", "graphql", "http", "https", "id", "identifier", "json", "key", "kubernetes",
    "linux", "log", "middleware", "model", "opus", "paragraph", "postgresql", "protocol",
    "proxy", "python", "redis", "request", "response", "result", "route", "routing",
    "server", "sql", "test", "token", "tool", "upstream", "url", "use", "watch",
    "websocket", "tonight",
}

_VALIDATION_LANGUAGES = ("de", "fr", "sw", "hi", "es")
_SUPPORTED_SOURCE_LANGUAGES = frozenset({
    "af", "am", "ar", "as", "az", "be", "bg", "bn", "bs", "ca", "ceb",
    "co", "cs", "cy", "da", "de", "el", "en", "eo", "es", "et", "eu",
    "fa", "fi", "fr", "fy", "ga", "gd", "gl", "gu", "ha", "haw", "he",
    "hi", "hmn", "hr", "ht", "hu", "hy", "id", "ig", "is", "it", "ja",
    "jv", "ka", "kk", "km", "kn", "ko", "ku", "ky", "la", "lb", "lo",
    "lt", "lv", "mg", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my",
    "ne", "nl", "no", "ny", "or", "pa", "pl", "ps", "pt", "ro", "ru",
    "rw", "sd", "si", "sk", "sl", "sm", "sn", "so", "sq", "sr", "st",
    "su", "sv", "sw", "ta", "te", "tg", "th", "tk", "tl", "tr", "ug",
    "uk", "ur", "uz", "vi", "xh", "yi", "yo", "zh", "zu",
})
_LANGUAGE_NAMES = {
    "ar": "Arabic", "bn": "Bengali", "de": "German", "en": "English",
    "es": "Spanish", "fr": "French", "hi": "Hindi", "it": "Italian",
    "ja": "Japanese", "ko": "Korean", "pt": "Portuguese", "ru": "Russian",
    "sw": "Swahili", "ta": "Tamil", "te": "Telugu", "th": "Thai",
    "tr": "Turkish", "ur": "Urdu", "vi": "Vietnamese", "zh": "Chinese",
}

_SCHEMA_LITERAL_KEYS = {"const", "default", "enum", "examples"}


def _messages_for_kind(body: dict, kind: str) -> list:
    """Return conversational items for all supported request dialects."""
    key = "input" if kind == "responses" else "messages"
    items = body.get(key)
    if kind == "responses" and isinstance(items, str):
        return [{"role": "user", "content": items}]
    return items if isinstance(items, list) else []


def _responses_input_for_translation(body: dict):
    """Translate only prose-bearing fields in native Responses ``input``."""
    return body.get("input")


def _latest_user_language(body: dict, kind: str = "openai") -> str:
    for message in reversed(_messages_for_kind(body, kind)):
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            text = " ".join(str(b.get("text", "")) for b in content
                            if isinstance(b, dict) and b.get("type") in ("text", "input_text"))
        else:
            continue
        if not text.strip():
            continue
        if needs_translation(text):
            try:
                code, _score = langid.classify(text)
            except Exception:
                return "the user's original language and script"
            return _LANGUAGE_NAMES.get(code, "the user's original language and script")
        return "English"
    return "the user's original language and script"


def _response_instruction(body: dict, kind: str = "openai") -> str:
    language = _latest_user_language(body, kind)
    if language.startswith("the user's"):
        target = language
    else:
        target = language
    return (
        "The user's source text was translated to English for upstream compatibility. "
        f"Respond in {target} unless the user explicitly requests another language."
    )


# Protected spans are copied byte-for-byte into the translated text.
_FENCED_RE = re.compile(r"```.*?```", re.S)
_INLINE_RE = re.compile(r"`[^`\n]*`")
_URL_RE = re.compile(r"(?:https?://|wss?://)[^\s<>,]+")

def _detection_sample(text: str) -> str:
    if len(text) <= _DETECTION_SAMPLE_SIZE:
        return text
    width = _DETECTION_SAMPLE_SIZE // 3
    middle = max(0, (len(text) - width) // 2)
    return text[:width] + text[middle:middle + width] + text[-width:]


def _cld_language(text: str) -> str:
    try:
        return str(pycld2.detect(text, bestEffort=True)[2][0][1]).lower()
    except Exception:
        return ""


def needs_translation(text: str) -> bool:
    """Conservatively detect text that is likely not English."""
    if not isinstance(text, str) or not text.strip():
        return False
    # Detection is a security gate: sampling first/middle/last creates blind
    # regions where a foreign fragment can pass untranslated.  Scan overlapping
    # bounded windows so words and short phrases crossing a window boundary are
    # still seen whole, without one unbounded classifier input.
    if len(text) > _DETECTION_SAMPLE_SIZE:
        stride = _DETECTION_SAMPLE_SIZE - _DETECTION_WINDOW_OVERLAP
        return any(needs_translation(
            text[start:start + _DETECTION_SAMPLE_SIZE])
            for start in range(0, len(text), stride))
    sample = text
    if _NON_LATIN_LETTER_RE.search(sample):
        return True
    words = [w.lower() for w in _WORD_RE.findall(sample)]
    hinglish_hits = sum(1 for w in words if w in _HINGLISH)
    latin_hits = sum(1 for w in words if w in _NON_ENGLISH_LATIN)
    # Short technical English (e.g. "Fix auth middleware") has too little signal
    # for statistical detection and must never be sent to the translator.
    if words and all(word in _TECHNICAL_ENGLISH for word in words):
        return False
    # One very distinctive Hindi imperative is sufficient; otherwise require two
    # signals to avoid translating ordinary technical English.
    distinctive = {"batao", "chahiye", "mujhe", "nahi", "karo", "karna", "kyun", "warna"}
    if any(w in distinctive for w in words) or hinglish_hits >= 2:
        return True
    # CLD2 reliably recognizes short technical English. Unknown one-word Latin
    # text remains ambiguous even when statistical detectors guess English, so
    # it is routed through Google's auto-detect path and fails closed.
    if len("".join(words)) < 12:
        if any(word in _TECHNICAL_ENGLISH for word in words):
            return False
        if len(words) == 1:
            return True
        return _cld_language(sample) != "en" or langid.classify(sample)[0] != "en"
    try:
        language, _score = langid.classify(sample)
    except Exception:
        return latin_hits >= 1 or any(
            ord(ch) > 127 and ch.isalpha() for ch in sample)
    return language != "en" or latin_hits >= 1


def _json_ranges(text: str) -> list[tuple[int, int]]:
    """Locate balanced JSON spans in one pass, respecting quoted strings."""
    out: list[tuple[int, int]] = []
    stack: list[tuple[str, int]] = []
    quoted = False
    escaped = False
    for i, ch in enumerate(text):
        if quoted:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                quoted = False
            continue
        if ch == '"':
            quoted = True
        elif ch in "[{":
            stack.append((ch, i))
        elif ch in "]}" and stack:
            expected = "[" if ch == "]" else "{"
            opener, start = stack[-1]
            if opener != expected:
                stack.clear()
                continue
            stack.pop()
            if not stack:
                candidate = text[start:i + 1]
                try:
                    json.loads(candidate)
                except Exception:
                    continue
                out.append((start, i + 1))
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


def _chunks(text: str, limit: int = _CHUNK_CHAR_LIMIT) -> list[str]:
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


async def _google_translate_details_batch(
    texts: list[str], proxy_urls: list[str] | None = None,
    source_languages: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Translate fragments independently with bounded proxy/backend fallbacks."""
    if not texts:
        return []
    if source_languages is not None and len(source_languages) != len(texts):
        raise ValueError("source language batch length mismatch")
    semaphore = asyncio.Semaphore(6)

    async def one(
        client: httpx.AsyncClient, backend: str, text: str, source_language: str,
    ) -> tuple[str, str]:
        if len(text) > 4500:
            raise ValueError("translation fragment exceeds safe upstream limit")
        params = {
            "client": "gtx", "sl": source_language or "auto",
            "tl": "en", "dt": "t",
        }
        async with semaphore:
            response = await client.post(
                backend,
                params=params,
                data={"q": text},
                headers={"user-agent": "Mozilla/5.0"},
            )
        response.raise_for_status()
        payload = response.json()
        if (type(payload) is not list or len(payload) < 3
                or type(payload[0]) is not list):
            raise ValueError("translator returned malformed payload")
        translated_parts: list[str] = []
        for segment in payload[0]:
            if (type(segment) is not list or not segment
                    or type(segment[0]) is not str):
                raise ValueError("translator returned malformed translation segment")
            translated_parts.append(segment[0])
        translated = "".join(translated_parts)
        if not translated.strip():
            raise ValueError("translator returned empty text")
        detected_raw = payload[2] if len(payload) > 2 else ""
        if type(detected_raw) is not str:
            raise ValueError("translator returned invalid source-language metadata")
        detected = detected_raw
        # Google occasionally emits the undocumented composite code `zh-CN`
        # here even though all configured validation paths use base ISO codes.
        # Normalize only well-formed regional tags; arbitrary metadata remains
        # fail-closed below.
        if re.fullmatch(r"[A-Za-z]{2,3}[-_][A-Za-z]{2,4}", detected):
            detected = re.split(r"[-_]", detected, maxsplit=1)[0].lower()
        if detected not in _SUPPORTED_SOURCE_LANGUAGES:
            raise ValueError("translator returned unsupported source-language metadata")
        return translated, detected

    proxies = list(dict.fromkeys(proxy_urls or []))[:_MAX_PROXY_FALLBACKS]
    routes = [(proxy, backend) for proxy in proxies + [None]
              for backend in _TRANSLATION_BACKENDS]
    languages = source_languages or [""] * len(texts)
    errors: list[str] = []
    for proxy_url, backend in routes:
        route = f"backend={backend} proxy={proxy_url or 'direct'}"
        try:
            async with httpx.AsyncClient(
                timeout=30.0, follow_redirects=True, proxy=proxy_url,
            ) as client:
                return list(await asyncio.wait_for(
                    asyncio.gather(*(one(client, backend, text, language)
                                     for text, language in zip(texts, languages))),
                    timeout=_ROUTE_TIMEOUT_SECONDS,
                ))
        except Exception as exc:
            errors.append(f"{route}: {type(exc).__name__}: {exc}")
    raise RuntimeError("all translation routes failed | " + " | ".join(errors))


async def _google_translate_batch(texts: list[str]) -> list[str]:
    """Translate fragments independently so each may have a different language."""
    return [translated for translated, _language
            in await _google_translate_details_batch(texts)]


async def _google_translate(text: str) -> str:
    return (await _google_translate_batch([text]))[0]


async def _translate_fragment(text: str, translator: Translator) -> str:
    if not needs_translation(text) and not getattr(translator, "collect_all", False):
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
    if not needs_translation(text) and not getattr(translator, "collect_all", False):
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
        if kind in ("text", "input_text") and isinstance(item.get("text"), str):
            item["text"], did = await translate_text(item["text"], translator)
            changed |= did
        elif allow_tool_blocks and kind == "tool_result" and isinstance(item.get("content"), (str, list)):
            item["content"], did = await _translate_content(item["content"], translator)
            changed |= did
        # tool_use.input, image/base64 and unknown structured blocks stay exact.
        out.append(item)
    return out, changed


async def _translate_schema_descriptions(value, translator: Translator):
    """Translate schema prose while leaving keys, enums, defaults and types exact."""
    if isinstance(value, list):
        changed = False
        out = []
        for item in value:
            converted, did = await _translate_schema_descriptions(item, translator)
            out.append(converted)
            changed |= did
        return out, changed
    if not isinstance(value, dict):
        return value, False
    out, changed = {}, False
    for key, item in value.items():
        if key in _SCHEMA_LITERAL_KEYS:
            # Literal payload examples/defaults become tool arguments and must stay exact.
            out[key], did = item, False
        elif key in ("description", "title") and isinstance(item, str):
            out[key], did = await translate_text(item, translator)
        else:
            out[key], did = await _translate_schema_descriptions(item, translator)
        changed |= did
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
        for schema_key in ("parameters", "input_schema"):
            if schema_key in target:
                target[schema_key], did = await _translate_schema_descriptions(
                    target[schema_key], translator)
                changed |= did
        out.append(item)
    return out, changed


def _add_response_instruction(body: dict, kind: str, instruction: str) -> None:
    """Append the gateway instruction; never trust client-supplied marker text."""
    if kind == "anthropic":
        system = body.get("system")
        if isinstance(system, str):
            body["system"] = system.rstrip() + "\n\n" + instruction
        elif isinstance(system, list):
            system.append({"type": "text", "text": instruction})
        else:
            body["system"] = instruction
        return
    if kind == "responses":
        instructions = body.get("instructions")
        body["instructions"] = (
            instructions.rstrip() + "\n\n" + instruction
            if isinstance(instructions, str) else instruction
        )
        return
    messages = body.setdefault("messages", [])
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "system":
            content = message.get("content")
            if isinstance(content, str):
                message["content"] = content.rstrip() + "\n\n" + instruction
            return
    messages.insert(0, {"role": "system", "content": instruction})


async def _translate_request_impl(body_bytes: bytes, kind: str,
                                  translator: Translator) -> tuple[bytes, bool]:
    """Structural translation pass using the supplied fragment translator."""
    body = json.loads(body_bytes)
    if not isinstance(body, dict):
        return body_bytes, False
    messages = _messages_for_kind(body, kind)
    if not messages and not (kind == "responses" and isinstance(body.get("instructions"), str)):
        return body_bytes, False
    response_instruction = _response_instruction(body, kind)
    out = json.loads(json.dumps(body))
    changed = False

    if kind == "anthropic":
        system = out.get("system")
        if isinstance(system, (str, list)):
            out["system"], did = await _translate_content(system, translator, allow_tool_blocks=False)
            changed |= did
    elif kind == "responses":
        instructions = out.get("instructions")
        if isinstance(instructions, str):
            out["instructions"], did = await translate_text(instructions, translator)
            changed |= did

    if kind == "responses" and isinstance(out.get("input"), str):
        out["input"], did = await translate_text(out["input"], translator)
        changed |= did
    else:
        for message in _messages_for_kind(out, kind):
            if not isinstance(message, dict):
                continue
            role = message.get("role")
            # assistant natural text is context; tool ids/arguments remain exact.
            if role in ("system", "developer", "user", "assistant", "tool"):
                message["content"], did = await _translate_content(message.get("content"), translator)
                changed |= did
            if kind == "responses" and message.get("type") == "function_call_output":
                message["output"], did = await _translate_content(message.get("output"), translator)
                changed |= did

    if "tools" in out:
        out["tools"], did = await _translate_tools(out.get("tools"), translator)
        changed |= did
    if not changed:
        return body_bytes, False
    _add_response_instruction(out, kind, response_instruction)
    return json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), True


class _BatchCollector:
    """Records every natural-language candidate behind unforgeable tokens.

    The default network path lets Google auto-detect every candidate, including short
    text. This avoids both false negatives (``Gracias``) and langid false positives on
    technical English (``Fix auth middleware``). Injected translators keep the
    conservative local detector for deterministic unit tests.
    """
    def __init__(self, *, collect_all: bool = False):
        import secrets
        self.parts: list[str] = []
        self.nonce = secrets.token_hex(16)
        self.collect_all = collect_all

    def marker(self, index: int) -> str:
        return f"<<<CIEL_XLATE_{self.nonce}_{index:08d}>>>"

    async def __call__(self, text: str) -> str:
        if not self.collect_all and not needs_translation(text):
            return text
        index = len(self.parts)
        self.parts.append(text)
        return self.marker(index)


def _replace_markers(value, collector: _BatchCollector, translations: list[str]):
    if isinstance(value, str):
        for index, translated in enumerate(translations):
            value = value.replace(collector.marker(index), translated)
        return value
    if isinstance(value, list):
        return [_replace_markers(item, collector, translations) for item in value]
    if isinstance(value, dict):
        return {key: _replace_markers(item, collector, translations) for key, item in value.items()}
    return value


async def _translate_collected(
    parts: list[str], proxy_urls: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Translate fragments with bounded latency and broad language auto-detection."""
    if type(parts) is not list or any(type(part) is not str for part in parts):
        raise ValueError("translation collection requires plain string list")
    chunks: list[str] = []
    owners: list[int] = []
    for index, part in enumerate(parts):
        for chunk in _chunks(part):
            chunks.append(chunk)
            owners.append(index)
    # Bound each network wave, not the whole request. Large conversations are
    # translated completely in deterministic waves instead of being hard-cut at
    # 64 KB or timing out because one giant gather shares one 35-second deadline.
    details: list[tuple[str, str]] = []
    for start in range(0, len(chunks), _WAVE_CHUNK_LIMIT):
        wave = chunks[start:start + _WAVE_CHUNK_LIMIT]
        if proxy_urls:
            detail_call = _google_translate_details_batch(wave, proxy_urls)
        else:
            detail_call = _google_translate_details_batch(wave)
        wave_details = await detail_call
        if type(wave_details) is not list or len(wave_details) != len(wave):
            raise ValueError(
                f"translation wave length mismatch: expected {len(wave)}, "
                f"received {len(wave_details)}"
            )
        for detail in wave_details:
            if (type(detail) is not tuple or len(detail) != 2
                    or type(detail[0]) is not str or type(detail[1]) is not str):
                raise ValueError("translation wave returned invalid detail")
        details.extend(wave_details)
    if len(details) != len(chunks):
        raise ValueError("translation detail batch length mismatch")
    output = [""] * len(parts)
    languages = [""] * len(parts)
    for index, (value, language) in zip(owners, details):
        output[index] += value
        if language and not languages[index]:
            languages[index] = language
    return output, languages


def _response_instruction_for(language: str) -> str:
    target = _LANGUAGE_NAMES.get(language, "the user's original language and script")
    return (
        "The user's source text was translated to English for upstream compatibility. "
        f"Respond in {target} unless the user explicitly requests another language."
    )


def _latest_user_texts(body: dict, kind: str = "openai") -> list[str]:
    """Return text fields from the latest non-empty user message."""
    for message in reversed(_messages_for_kind(body, kind)):
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return [content]
        if isinstance(content, list):
            texts = [block["text"] for block in content
                     if isinstance(block, dict)
                     and block.get("type") in ("text", "input_text")
                     and isinstance(block.get("text"), str)
                     and block["text"].strip()]
            if texts:
                return texts
    return []


def _replace_response_instruction(body: dict, kind: str, language: str) -> None:
    marker = "The user's source text was translated to English for upstream compatibility."
    replacement = _response_instruction_for(language)
    if kind == "anthropic":
        system = body.get("system")
        if isinstance(system, str) and marker in system:
            body["system"] = re.sub(re.escape(marker) + r".*?(?=\n\n|$)", replacement, system)
        elif isinstance(system, list):
            for block in system:
                if isinstance(block, dict) and marker in str(block.get("text", "")):
                    block["text"] = replacement
        return
    if kind == "responses":
        instructions = body.get("instructions")
        if isinstance(instructions, str) and marker in instructions:
            start = instructions.find(marker)
            body["instructions"] = instructions[:start] + replacement
        return
    for message in body.get("messages") or []:
        if (isinstance(message, dict) and message.get("role") == "system"
                and marker in str(message.get("content", ""))):
            content = message.get("content")
            if isinstance(content, str):
                start = content.find(marker)
                prefix = content[:start]
                message["content"] = prefix + replacement
            return


def _contains_non_latin_letters(text: str) -> bool:
    return _NON_LATIN_LETTER_RE.search(text) is not None


_VALIDATION_WINDOW_SIZE = 1024
_VALIDATION_WINDOW_STRIDE = 768
_VALIDATION_TOKEN_MARGIN = 5.3
_VALIDATION_WINDOW_MARGIN = 12.0
_validation_identifier = None
_validation_language_indices: dict[str, int] = {}


def _source_language_margin(text: str, source_language: str) -> float:
    global _validation_identifier, _validation_language_indices
    if _validation_identifier is None:
        from langid.langid import LanguageIdentifier, model
        _validation_identifier = LanguageIdentifier.from_modelstring(
            model, norm_probs=False)
        _validation_language_indices = {
            language: index
            for index, language in enumerate(_validation_identifier.nb_classes)
        }
    source_index = _validation_language_indices.get(source_language)
    english_index = _validation_language_indices.get("en")
    if source_index is None or english_index is None or source_index == english_index:
        return float("-inf")
    features = _validation_identifier.instance2fv(text)
    scores = _validation_identifier.nb_classprobs(features)
    return float(scores[source_index] - scores[english_index])


def _alphabetic_runs(text: str):
    start = None
    for index, character in enumerate(text):
        if character.isalpha():
            if start is None:
                start = index
        elif start is not None:
            yield text[start:index]
            start = None
    if start is not None:
        yield text[start:]


def _normalized_lexical_tokens(text: str) -> list[str]:
    normalized = unicodedata.normalize("NFKC", text).casefold()
    return [token for token in _alphabetic_runs(normalized)]


def _contains_source_language(text: str, source_language: str) -> bool:
    seen_tokens: set[str] = set()
    for start in range(0, len(text), _VALIDATION_WINDOW_STRIDE):
        window = text[start:start + _VALIDATION_WINDOW_SIZE]
        if _source_language_margin(window, source_language) >= _VALIDATION_WINDOW_MARGIN:
            return True
        for token in _alphabetic_runs(window):
            normalized = token.casefold()
            if normalized in seen_tokens:
                continue
            seen_tokens.add(normalized)
            if _source_language_margin(
                    normalized, source_language) >= _VALIDATION_TOKEN_MARGIN:
                return True
    return False


def _validation_plain_spans(text: str) -> list[str]:
    """Return only output prose; protected URL/code/JSON spans are trusted exact."""
    ranges = _protected_ranges(text)
    spans: list[str] = []
    pos = 0
    for start, end in ranges + [(len(text), len(text))]:
        if start > pos:
            spans.append(text[pos:start])
        pos = end
    return spans


async def _forced_source_residuals(
    texts: list[str], source_language: str,
    proxy_urls: list[str] | None,
) -> list[bool]:
    """Batch forced-source checks for many texts without one call per field."""
    if type(texts) is not list or any(type(text) is not str for text in texts):
        raise ValueError("forced-source validation requires plain string list")
    chunks: list[str] = []
    owners: list[int] = []
    for index, text in enumerate(texts):
        for span in _validation_plain_spans(text):
            for chunk in _chunks(span):
                if chunk.strip():
                    chunks.append(chunk)
                    owners.append(index)
    residuals = [False] * len(texts)
    for start in range(0, len(chunks), _WAVE_CHUNK_LIMIT):
        wave = chunks[start:start + _WAVE_CHUNK_LIMIT]
        wave_owners = owners[start:start + _WAVE_CHUNK_LIMIT]
        languages = [source_language] * len(wave)
        details = await _google_translate_details_batch(
            wave, proxy_urls, source_languages=languages)
        if type(details) is not list or len(details) != len(wave):
            raise ValueError(
                f"validation wave length mismatch: expected {len(wave)}, "
                f"received {len(details)}"
            )
        for owner, original, detail in zip(wave_owners, wave, details):
            if (type(detail) is not tuple or len(detail) != 2
                    or type(detail[0]) is not str or type(detail[1]) is not str):
                raise ValueError("forced-source validation returned invalid detail")
            converted, _detected = detail
            if not converted.strip():
                raise ValueError("forced-source validation returned empty text")
            if unicodedata.normalize("NFKC", converted).casefold() != (
                    unicodedata.normalize("NFKC", original).casefold()):
                residuals[owner] = True
    return residuals


async def _forced_source_residual(
    translated: str, source_language: str, proxy_urls: list[str] | None,
) -> bool:
    return (await _forced_source_residuals(
        [translated], source_language, proxy_urls))[0]


def _has_english_context(text: str) -> bool:
    # No local classifier, vocabulary, or identifier heuristic is authoritative
    # enough to let a natural-language field skip backend validation.  Protected
    # code/URL/JSON spans have already been removed before this point; every
    # remaining field reported as English must be validated fail-closed.
    return False


async def _validate_translations(parts: list[str], translations: list[str],
                                 languages: list[str],
                                 proxy_urls: list[str] | None = None) -> None:
    if type(parts) is not list or type(translations) is not list or type(languages) is not list:
        raise ValueError("translation validation requires plain lists")
    if len(translations) != len(parts) or len(languages) != len(parts):
        raise ValueError("translation batch length mismatch")
    english_indices: list[int] = []
    ambiguous_indices: list[int] = []
    for index, (source, translated, source_language) in enumerate(
            zip(parts, translations, languages)):
        if type(source) is not str or type(translated) is not str or not translated.strip():
            raise ValueError(f"translation result {index} is empty or invalid")
        if (type(source_language) is not str
                or source_language not in _SUPPORTED_SOURCE_LANGUAGES):
            raise ValueError(
                f"translation result {index} has invalid source-language metadata")
        if source_language == "en":
            if translated != source:
                raise ValueError(
                    f"translation result {index} has invalid source-language metadata")
            english_indices.append(index)
            if not _has_english_context(source):
                ambiguous_indices.append(index)
            continue
        invalid = (translated == source or _contains_non_latin_letters(translated))
        if not invalid:
            invalid = await _forced_source_residual(
                translated, source_language, proxy_urls)
        if invalid:
            raise ValueError(f"translation result {index} is not English")

    english_texts = [translations[index] for index in ambiguous_indices]
    for candidate in _VALIDATION_LANGUAGES:
        residuals = await _forced_source_residuals(
            english_texts, candidate, proxy_urls)
        if any(residuals):
            bad = ambiguous_indices[residuals.index(True)]
            raise ValueError(
                f"translation result {bad} has invalid English metadata")


def _batch_texts_for_validation(body_bytes: bytes, kind: str) -> list[str]:
    """All natural-language text values in the TRANSLATED body, for the
    English-only sanity check an LLM backend must pass."""
    try:
        body = json.loads(body_bytes) if isinstance(body_bytes, (bytes, bytearray)) else body_bytes
    except (TypeError, ValueError):
        return []
    texts: list[str] = []
    messages = _messages_for_kind(body, kind) if isinstance(body, dict) else []
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str):
            texts.append(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and isinstance(block.get("text"), str):
                    texts.append(block["text"])
    return texts


async def translate_request_report(body_bytes: bytes, kind: str,
                                   translator: Translator = _google_translate,
                                   proxy_urls: list[str] | None = None,
                                   backends: list | None = None) -> TranslationReport:
    """Translate natural-language fields in an Anthropic or OpenAI request body.

    The default Google path translates request fragments concurrently in bounded
    waves. It reports errors to the caller; an endpoint that requires English must
    fail closed rather than forwarding the original non-English body.

    ``backends`` (2026-09-14): an ORDERED chain the user dragged together.
    Each entry is either the string "google" (the built-in backend) or an
    async callable (an LLMTranslator). Entries are tried in order; the first
    backend that completes the whole translation wins. When one fails the
    next is tried; when every entry fails the report carries the error and
    the ORIGINAL body — never a partially-translated hybrid.
    """
    if backends:
        collected_errors: list[str] = []
        for entry in backends:
            if isinstance(entry, str) and entry == "google":
                # Built-in backend: full Google path (detection + validation).
                rep = await translate_request_report(
                    body_bytes, kind, translator=_google_translate,
                    proxy_urls=proxy_urls)
                if not rep.error:
                    return rep
                collected_errors.append(f"google: {rep.error}")
                continue
            if not callable(entry):
                collected_errors.append(f"unknown backend: {entry!r}")
                continue
            # LLM backend: structural pass with this translator, then the same
            # English-language validation the Google path applies (an LLM can
            # return prose in the wrong language or echo the source).
            try:
                out, changed = await _translate_request_impl(
                    body_bytes, kind, entry)
                if not changed:
                    return TranslationReport(body_bytes, False,
                                             detected_language="en")
                texts = _batch_texts_for_validation(out, kind)
                if texts:
                    invalid = [t for t in texts
                               if _contains_non_latin_letters(t)]
                    if invalid:
                        raise ValueError(
                            f"backend output not English: {invalid[0][:80]}")
                return TranslationReport(out, True)
            except Exception as exc:
                collected_errors.append(
                    f"{type(exc).__name__}: {exc}")
                continue
        return TranslationReport(
            body_bytes, False,
            error="; ".join(collected_errors) or "no translation backend succeeded")
    try:
        if translator is not _google_translate:
            out, changed = await _translate_request_impl(body_bytes, kind, translator)
            return TranslationReport(out, changed)
        # Skip ordinary English context: translating it is unnecessary and can
        # burst-throttle Google before a later Hindi user field is reached.
        # Local language identification is only an optimization and cannot be
        # a security gate: a short foreign phrase can be diluted by surrounding
        # English. Send every unprotected natural-language candidate through
        # backend auto-detection, then validate the returned metadata/output.
        collector = _BatchCollector(collect_all=True)
        template, changed = await _translate_request_impl(body_bytes, kind, collector)
        if not changed:
            return TranslationReport(body_bytes, False)
        translations, languages = await _translate_collected(collector.parts, proxy_urls)
        await _validate_translations(
            collector.parts, translations, languages, proxy_urls)
        if (translations == collector.parts
                and all(language == "en" for language in languages)):
            return TranslationReport(body_bytes, False, detected_language="en")
        out = _replace_markers(json.loads(template), collector, translations)
        # Match the latest user's original text to collected fragments. Later tool
        # schema descriptions must not override the user's response language.
        latest_texts = _latest_user_texts(json.loads(body_bytes), kind)
        user_indices = [index for index, part in enumerate(collector.parts)
                        if any(part.strip() and part in text for text in latest_texts)]
        detected = next((languages[index] for index in reversed(user_indices)
                         if languages[index]), "")
        if not detected and translations == collector.parts:
            return TranslationReport(body_bytes, False)
        if detected:
            _replace_response_instruction(out, kind, detected)
        encoded = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return TranslationReport(encoded, True, detected_language=detected)
    except Exception as exc:
        return TranslationReport(
            body_bytes, False,
            error=f"{type(exc).__name__}: {exc}",
        )


async def translate_request(body_bytes: bytes, kind: str,
                            translator: Translator = _google_translate,
                            proxy_urls: list[str] | None = None) -> tuple[bytes, bool]:
    """Compatibility wrapper for callers that need only body + changed."""
    report = await translate_request_report(body_bytes, kind, translator, proxy_urls)
    return report.body, report.changed
