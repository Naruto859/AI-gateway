"""OpenAI ⇄ Anthropic wire-format translation.

Why this exists
---------------
The gateway can front two kinds of upstream: providers that speak Anthropic's
``/v1/messages`` and providers that speak OpenAI's ``/v1/chat/completions``.
Until now it only rewrote the URL (``_target_url``) and forwarded the body
verbatim, so a client speaking one dialect could only ever reach an endpoint of
the same dialect — a mismatch produced an upstream 400 that looked like a
routing fault.

This module translates the *bodies* in both directions, for non-streaming
responses and for SSE streams, so any client can reach any endpoint:

    client shape        endpoint shape      what happens
    ------------------------------------------------------------------
    anthropic           anthropic           unchanged (no translation)
    openai              openai              unchanged (no translation)
    anthropic           openai              request →OAI, response →Anthropic
    openai              anthropic           request →Anthropic, response →OAI

Design rules
------------
1. **Lossless where the formats overlap, explicit where they don't.** Every
   field that has a counterpart is mapped; fields with no counterpart are
   dropped deliberately (documented inline) rather than passed through, because
   an unknown field is a 400 on a strict upstream.
2. **Tool calls are the hard part and are fully supported** — both the
   assistant's tool *requests* and the user's tool *results*, in history as
   well as in a live stream. Anything less breaks agentic clients on turn two.
3. **Never invent an id.** Anthropic ``tool_use.id`` ⇄ OpenAI
   ``tool_call.id`` are carried across verbatim; a fabricated id produces the
   duplicate/orphan ``tool_use`` 400s that are impossible to debug later.
4. **Stream translation is stateful but allocation-light** — the incremental
   translators are small classes fed one SSE event at a time, emitting zero or
   more output events, so the relay stays streaming (no buffering the whole
   answer).
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

__all__ = [
    "detect_client_shape",
    "needs_translation",
    "oai_request_to_anthropic",
    "anthropic_request_to_oai",
    "oai_response_to_anthropic",
    "anthropic_response_to_oai",
    "AnthropicToOaiStream",
    "OaiToAnthropicStream",
]

# --------------------------------------------------------------------------
# shape detection
# --------------------------------------------------------------------------

_ANTHROPIC_HINTS = ("messages",)
_OAI_HINTS = ("chat/completions", "completions")


def detect_client_shape(path: str, body: Optional[dict] = None) -> str:
    """Return 'anthropic', 'openai' or 'responses' for the INBOUND request.

    Three dialects are in the wild, not two (Ciel, 2026-09-12): Codex CLI
    dropped the chat wire protocol entirely and now speaks only the OpenAI
    *Responses* API (``POST /v1/responses``), so a gateway that knows just
    ``/v1/messages`` and ``/v1/chat/completions`` forwards that body verbatim
    and every upstream answers "not implemented".

    The path is authoritative (``v1/messages`` / ``v1/chat/completions`` /
    ``v1/responses``); the body is only consulted when the path is ambiguous,
    using the fields that exist in exactly one dialect: Responses carries
    ``input`` + ``instructions`` + ``max_output_tokens``, Anthropic has a
    top-level ``system`` string and requires ``max_tokens``, OpenAI Chat
    carries the system prompt as a message with ``role: "system"``.
    """
    p = (path or "").lower().rstrip("/")
    if p.endswith("chat/completions"):
        return "openai"
    if p.endswith("responses"):
        return "responses"
    if p.endswith("messages"):
        return "anthropic"
    if isinstance(body, dict):
        if "max_output_tokens" in body:
            return "responses"
        if "input" in body and "messages" not in body:
            return "responses"
        if "instructions" in body and "input" in body:
            return "responses"
        if isinstance(body.get("system"), (str, list)):
            return "anthropic"
        for m in body.get("messages") or []:
            if isinstance(m, dict) and m.get("role") == "system":
                return "openai"
        if "max_completion_tokens" in body or "frequency_penalty" in body:
            return "openai"
        if "max_tokens" in body and "messages" in body:
            return "anthropic"
    return "anthropic"


DIALECTS = ("anthropic", "openai", "responses")

# Endpoint api_mode values (db column) -> dialect name.
_MODE_TO_DIALECT = {
    "chat_completions": "openai",
    "openai": "openai",
    "openai_responses": "responses",
    "responses": "responses",
    "anthropic_messages": "anthropic",
    "anthropic": "anthropic",
}


def mode_of_dialect(dialect: str) -> str:
    """Canonical DB ``api_mode`` for a wire dialect."""
    if dialect not in DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}")
    return {"anthropic": "anthropic_messages",
            "openai": "openai",
            "responses": "openai_responses"}[dialect]


def dialect_of_mode(endpoint_mode: str) -> str:
    """Normalise an endpoint's api_mode to one of DIALECTS."""
    value = (endpoint_mode or "").strip().lower()
    if value not in _MODE_TO_DIALECT:
        raise ValueError(f"unknown endpoint api_mode {endpoint_mode!r}")
    return _MODE_TO_DIALECT[value]


def needs_translation(client_shape: str, endpoint_mode: str) -> bool:
    """True when the client dialect differs from the endpoint dialect."""
    return (client_shape or "anthropic") != dialect_of_mode(endpoint_mode)


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def _as_text(content: Any) -> str:
    """Flatten any content shape to plain text (used for system prompts)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        out = []
        for b in content:
            if isinstance(b, str):
                out.append(b)
            elif isinstance(b, dict):
                if b.get("type") in (None, "text") and isinstance(b.get("text"), str):
                    out.append(b["text"])
        return "\n".join(x for x in out if x)
    return str(content)


def _oai_image_to_anthropic(url_obj: dict) -> Optional[dict]:
    """OpenAI ``image_url`` block → Anthropic ``image`` block.

    Handles both a data URL (``data:image/png;base64,AAAA``) and a plain http
    URL; Anthropic accepts the latter as ``source.type = "url"``.
    """
    url = ""
    if isinstance(url_obj, dict):
        url = url_obj.get("url") or ""
    elif isinstance(url_obj, str):
        url = url_obj
    if not url:
        return None
    if url.startswith("data:"):
        try:
            header, b64 = url.split(",", 1)
            media = header[5:].split(";")[0] or "image/png"
        except ValueError:
            return None
        return {"type": "image",
                "source": {"type": "base64", "media_type": media, "data": b64}}
    return {"type": "image", "source": {"type": "url", "url": url}}


def _anthropic_image_to_oai(block: dict) -> Optional[dict]:
    src = block.get("source") or {}
    if src.get("type") == "base64":
        media = src.get("media_type") or "image/png"
        return {"type": "image_url",
                "image_url": {"url": f"data:{media};base64,{src.get('data', '')}"}}
    if src.get("type") == "url" and src.get("url"):
        return {"type": "image_url", "image_url": {"url": src["url"]}}
    return None


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


_FINISH_TO_STOP = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "content_filter": "refusal",
}
_STOP_TO_FINISH = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
    "pause_turn": "stop",
}


# --------------------------------------------------------------------------
# REQUEST: OpenAI → Anthropic
# --------------------------------------------------------------------------

def oai_request_to_anthropic(body: dict) -> dict:
    """Translate an OpenAI chat-completions request into an Anthropic one.

    Notable mappings:
      * ``role: "system"`` messages are hoisted into the top-level ``system``.
      * ``assistant.tool_calls`` → ``tool_use`` content blocks.
      * ``role: "tool"`` messages → ``tool_result`` blocks, merged into a
        single user turn when consecutive (Anthropic requires that).
      * ``max_tokens`` is REQUIRED by Anthropic; OpenAI's is optional, so a
        default is supplied rather than sending an invalid request.
    """
    out: Dict[str, Any] = {}
    if body.get("model"):
        out["model"] = body["model"]

    systems: List[str] = []
    msgs: List[Dict[str, Any]] = []

    for m in body.get("messages") or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")

        if role in ("system", "developer"):
            t = _as_text(m.get("content"))
            if t:
                systems.append(t)
            continue

        if role == "tool":
            block = {
                "type": "tool_result",
                "tool_use_id": m.get("tool_call_id") or "",
                "content": _as_text(m.get("content")),
            }
            # Anthropic wants tool results as USER turns, and consecutive
            # results must live in ONE turn.
            if msgs and msgs[-1]["role"] == "user" and isinstance(msgs[-1].get("content"), list) \
                    and msgs[-1]["content"] and msgs[-1]["content"][-1].get("type") == "tool_result":
                msgs[-1]["content"].append(block)
            else:
                msgs.append({"role": "user", "content": [block]})
            continue

        if role == "assistant":
            blocks: List[Dict[str, Any]] = []
            txt = m.get("content")
            if isinstance(txt, str) and txt:
                blocks.append({"type": "text", "text": txt})
            elif isinstance(txt, list):
                for b in txt:
                    if isinstance(b, dict) and b.get("type") == "text" and b.get("text"):
                        blocks.append({"type": "text", "text": b["text"]})
            for tc in m.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") or {}
                args = fn.get("arguments")
                if isinstance(args, str):
                    try:
                        args = json.loads(args) if args.strip() else {}
                    except Exception:
                        args = {"_raw": args}
                blocks.append({
                    "type": "tool_use",
                    "id": tc.get("id") or _new_id("toolu"),
                    "name": fn.get("name") or "",
                    "input": args if isinstance(args, dict) else {},
                })
            if not blocks:
                # An assistant turn with no content at all is rejected by
                # Anthropic; drop it rather than send an empty block.
                continue
            msgs.append({"role": "assistant", "content": blocks})
            continue

        # user (and anything unrecognised, treated as user)
        content = m.get("content")
        if isinstance(content, str):
            msgs.append({"role": "user", "content": content})
        elif isinstance(content, list):
            blocks = []
            for b in content:
                if not isinstance(b, dict):
                    if isinstance(b, str):
                        blocks.append({"type": "text", "text": b})
                    continue
                t = b.get("type")
                if t in ("text", "input_text") and b.get("text"):
                    blocks.append({"type": "text", "text": b["text"]})
                elif t in ("image_url", "input_image"):
                    img = _oai_image_to_anthropic(b.get("image_url") or b.get("image") or b)
                    if img:
                        blocks.append(img)
            msgs.append({"role": "user", "content": blocks or ""})
        elif content is not None:
            msgs.append({"role": "user", "content": _as_text(content)})

    if systems:
        out["system"] = "\n\n".join(systems)
    out["messages"] = msgs

    # Anthropic REQUIRES max_tokens.
    mt = body.get("max_tokens") or body.get("max_completion_tokens")
    try:
        mt = int(mt) if mt else 0
    except (TypeError, ValueError):
        mt = 0
    out["max_tokens"] = mt if mt > 0 else 4096

    for src, dst in (("temperature", "temperature"), ("top_p", "top_p"),
                     ("stream", "stream")):
        if body.get(src) is not None:
            out[dst] = body[src]
    stop = body.get("stop")
    if isinstance(stop, str):
        out["stop_sequences"] = [stop]
    elif isinstance(stop, list) and stop:
        out["stop_sequences"] = [s for s in stop if isinstance(s, str)]

    tools = body.get("tools")
    if isinstance(tools, list) and tools:
        conv = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            fn = t.get("function") if t.get("type") == "function" else t
            if not isinstance(fn, dict) or not fn.get("name"):
                continue
            conv.append({
                "name": fn["name"],
                "description": fn.get("description") or "",
                "input_schema": fn.get("parameters") or {"type": "object", "properties": {}},
            })
        if conv:
            out["tools"] = conv

    tc = body.get("tool_choice")
    if tc == "required":
        out["tool_choice"] = {"type": "any"}
    elif tc == "auto":
        out["tool_choice"] = {"type": "auto"}
    elif tc == "none":
        out["tool_choice"] = {"type": "none"}
    elif isinstance(tc, dict):
        name = ((tc.get("function") or {}).get("name")) or tc.get("name")
        if name:
            out["tool_choice"] = {"type": "tool", "name": name}

    # Deliberately NOT forwarded (no Anthropic counterpart; strict upstreams
    # 400 on unknown keys): n, presence_penalty, frequency_penalty, logprobs,
    # logit_bias, seed, response_format, user, parallel_tool_calls.
    return out


# --------------------------------------------------------------------------
# REQUEST: Anthropic → OpenAI
# --------------------------------------------------------------------------

def anthropic_request_to_oai(body: dict) -> dict:
    """Translate an Anthropic messages request into an OpenAI one."""
    out: Dict[str, Any] = {}
    if body.get("model"):
        out["model"] = body["model"]

    msgs: List[Dict[str, Any]] = []
    sys_text = _as_text(body.get("system"))
    if sys_text:
        msgs.append({"role": "system", "content": sys_text})

    for m in body.get("messages") or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")

        if isinstance(content, str):
            msgs.append({"role": role or "user", "content": content})
            continue
        if not isinstance(content, list):
            msgs.append({"role": role or "user", "content": _as_text(content)})
            continue

        if role == "assistant":
            text_parts: List[str] = []
            tool_calls: List[Dict[str, Any]] = []
            for b in content:
                if not isinstance(b, dict):
                    continue
                t = b.get("type")
                if t == "text" and b.get("text"):
                    text_parts.append(b["text"])
                elif t == "tool_use":
                    tool_calls.append({
                        "id": b.get("id") or _new_id("call"),
                        "type": "function",
                        "function": {
                            "name": b.get("name") or "",
                            "arguments": json.dumps(b.get("input") or {}, ensure_ascii=False),
                        },
                    })
                # 'thinking' / 'redacted_thinking' have no OpenAI counterpart.
            msg: Dict[str, Any] = {"role": "assistant",
                                   "content": "\n".join(text_parts) if text_parts else None}
            if tool_calls:
                msg["tool_calls"] = tool_calls
            if msg["content"] is None and not tool_calls:
                continue
            msgs.append(msg)
            continue

        # user turn: split tool_results out into their own `tool` messages,
        # because OpenAI carries them as a distinct role.
        parts: List[Dict[str, Any]] = []
        pending_tools: List[Dict[str, Any]] = []
        for b in content:
            if not isinstance(b, dict):
                if isinstance(b, str):
                    parts.append({"type": "text", "text": b})
                continue
            t = b.get("type")
            if t == "text" and b.get("text"):
                parts.append({"type": "text", "text": b["text"]})
            elif t == "image":
                img = _anthropic_image_to_oai(b)
                if img:
                    parts.append(img)
            elif t == "tool_result":
                pending_tools.append({
                    "role": "tool",
                    "tool_call_id": b.get("tool_use_id") or "",
                    "content": _as_text(b.get("content")),
                })
        if parts:
            only_text = all(p.get("type") == "text" for p in parts)
            msgs.append({"role": "user",
                         "content": "\n".join(p["text"] for p in parts) if only_text else parts})
        msgs.extend(pending_tools)

    out["messages"] = msgs
    if body.get("max_tokens"):
        out["max_tokens"] = body["max_tokens"]
    for k in ("temperature", "top_p", "stream"):
        if body.get(k) is not None:
            out[k] = body[k]
    if body.get("stop_sequences"):
        out["stop"] = body["stop_sequences"]

    tools = body.get("tools")
    if isinstance(tools, list) and tools:
        conv = []
        for t in tools:
            if not isinstance(t, dict) or not t.get("name"):
                continue
            conv.append({"type": "function", "function": {
                "name": t["name"],
                "description": t.get("description") or "",
                "parameters": t.get("input_schema") or {"type": "object", "properties": {}},
            }})
        if conv:
            out["tools"] = conv

    tc = body.get("tool_choice")
    if isinstance(tc, dict):
        tt = tc.get("type")
        if tt == "any":
            out["tool_choice"] = "required"
        elif tt == "auto":
            out["tool_choice"] = "auto"
        elif tt == "none":
            out["tool_choice"] = "none"
        elif tt == "tool" and tc.get("name"):
            out["tool_choice"] = {"type": "function", "function": {"name": tc["name"]}}

    # 'thinking' config has no OpenAI counterpart and is dropped.
    return out


# --------------------------------------------------------------------------
# RESPONSE (non-stream)
# --------------------------------------------------------------------------

def oai_response_to_anthropic(resp: dict, *, model: str = "") -> dict:
    """OpenAI completion object → Anthropic Message object."""
    choices = resp.get("choices") or []
    choice = choices[0] if choices else {}
    msg = choice.get("message") or {}

    blocks: List[Dict[str, Any]] = []
    txt = msg.get("content")
    if isinstance(txt, str) and txt:
        blocks.append({"type": "text", "text": txt})
    elif isinstance(txt, list):
        for b in txt:
            if isinstance(b, dict) and b.get("text"):
                blocks.append({"type": "text", "text": b["text"]})

    for tc in msg.get("tool_calls") or []:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        args = fn.get("arguments")
        if isinstance(args, str):
            try:
                args = json.loads(args) if args.strip() else {}
            except Exception:
                args = {"_raw": args}
        blocks.append({"type": "tool_use", "id": tc.get("id") or _new_id("toolu"),
                       "name": fn.get("name") or "",
                       "input": args if isinstance(args, dict) else {}})

    usage = resp.get("usage") or {}
    return {
        "id": resp.get("id") or _new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": resp.get("model") or model or "",
        "content": blocks,
        "stop_reason": _FINISH_TO_STOP.get(choice.get("finish_reason") or "", "end_turn"),
        "stop_sequence": None,
        "usage": {
            "input_tokens": usage.get("prompt_tokens") or 0,
            "output_tokens": usage.get("completion_tokens") or 0,
        },
    }


def anthropic_response_to_oai(msg: dict, *, model: str = "") -> dict:
    """Anthropic Message object → OpenAI completion object."""
    text_parts: List[str] = []
    tool_calls: List[Dict[str, Any]] = []
    for b in msg.get("content") or []:
        if not isinstance(b, dict):
            continue
        if b.get("type") == "text" and b.get("text"):
            text_parts.append(b["text"])
        elif b.get("type") == "tool_use":
            tool_calls.append({
                "id": b.get("id") or _new_id("call"),
                "type": "function",
                "function": {"name": b.get("name") or "",
                             "arguments": json.dumps(b.get("input") or {}, ensure_ascii=False)},
            })

    message: Dict[str, Any] = {"role": "assistant",
                               "content": "\n".join(text_parts) if text_parts else None}
    if tool_calls:
        message["tool_calls"] = tool_calls

    usage = msg.get("usage") or {}
    pt = usage.get("input_tokens") or 0
    ct = usage.get("output_tokens") or 0
    return {
        "id": msg.get("id") or _new_id("chatcmpl"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": msg.get("model") or model or "",
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": _STOP_TO_FINISH.get(msg.get("stop_reason") or "", "stop"),
        }],
        "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct},
    }


# --------------------------------------------------------------------------
# STREAM translators
# --------------------------------------------------------------------------

def _sse(event: Optional[str], data: Any) -> bytes:
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    if event:
        return f"event: {event}\ndata: {payload}\n\n".encode("utf-8")
    return f"data: {payload}\n\n".encode("utf-8")


class AnthropicToOaiStream:
    """Anthropic SSE → OpenAI SSE (``chat.completion.chunk`` frames).

    Fed one ``(event, data)`` pair at a time; returns a list of output frames
    (already SSE-encoded bytes). Emits the OpenAI role-priming chunk first,
    streams text deltas, streams tool-call argument deltas, and finishes with
    a ``finish_reason`` chunk plus ``data: [DONE]``.
    """

    def __init__(self, model: str = ""):
        self.model = model
        self.cid = _new_id("chatcmpl")
        self.created = int(time.time())
        self.sent_role = False
        self.tool_index: Dict[int, int] = {}   # anthropic block index -> oai tool index
        self.next_tool = 0
        self.finish = "stop"
        self.done = False
        # Usage must survive the hop. Anthropic reports input_tokens on
        # message_start and output_tokens on message_delta; dropping them here
        # made the figure vanish for every translated stream (and for the
        # anthropic->responses chain, whose terminal envelope must carry it).
        self.usage = {"input_tokens": 0, "output_tokens": 0}

    def _chunk(self, delta: dict, finish: Optional[str] = None) -> bytes:
        payload = {
            "id": self.cid, "object": "chat.completion.chunk",
            "created": self.created, "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }
        if finish is not None and (self.usage["input_tokens"] or self.usage["output_tokens"]):
            pt, ct = self.usage["input_tokens"], self.usage["output_tokens"]
            payload["usage"] = {"prompt_tokens": pt, "completion_tokens": ct,
                                "total_tokens": pt + ct}
        return _sse(None, payload)

    def feed(self, event: Optional[str], data: str) -> List[bytes]:
        out: List[bytes] = []
        if data is None:
            return out
        s = data.strip()
        if not s or s == "[DONE]":
            return out
        try:
            obj = json.loads(s)
        except Exception:
            return out
        if not isinstance(obj, dict):
            return out
        t = obj.get("type") or event

        if t == "message_start":
            m = obj.get("message") or {}
            if m.get("model"):
                self.model = m["model"]
            if m.get("id"):
                self.cid = m["id"]
            mu = m.get("usage") or {}
            if mu.get("input_tokens"):
                self.usage["input_tokens"] = mu["input_tokens"]
            return out

        if t == "content_block_start":
            blk = obj.get("content_block") or {}
            idx = obj.get("index", 0)
            if blk.get("type") == "tool_use":
                oi = self.next_tool
                self.next_tool += 1
                self.tool_index[idx] = oi
                if not self.sent_role:
                    self.sent_role = True
                    out.append(self._chunk({"role": "assistant", "content": ""}))
                out.append(self._chunk({"tool_calls": [{
                    "index": oi, "id": blk.get("id") or _new_id("call"),
                    "type": "function",
                    "function": {"name": blk.get("name") or "", "arguments": ""},
                }]}))
            return out

        if t == "content_block_delta":
            d = obj.get("delta") or {}
            idx = obj.get("index", 0)
            if d.get("type") == "text_delta" and d.get("text"):
                if not self.sent_role:
                    self.sent_role = True
                    out.append(self._chunk({"role": "assistant", "content": ""}))
                out.append(self._chunk({"content": d["text"]}))
            elif d.get("type") == "input_json_delta":
                oi = self.tool_index.get(idx, 0)
                out.append(self._chunk({"tool_calls": [{
                    "index": oi, "function": {"arguments": d.get("partial_json") or ""},
                }]}))
            # 'thinking_delta' has no OpenAI counterpart — dropped.
            return out

        if t == "message_delta":
            sr = (obj.get("delta") or {}).get("stop_reason")
            if sr:
                self.finish = _STOP_TO_FINISH.get(sr, "stop")
            du = obj.get("usage") or {}
            if du.get("output_tokens"):
                self.usage["output_tokens"] = du["output_tokens"]
            if du.get("input_tokens"):
                self.usage["input_tokens"] = du["input_tokens"]
            return out

        if t == "message_stop":
            if not self.sent_role:
                out.append(self._chunk({"role": "assistant", "content": ""}))
                self.sent_role = True
            out.append(self._chunk({}, finish=self.finish))
            out.append(b"data: [DONE]\n\n")
            self.done = True
            return out

        if t == "error":
            err = obj.get("error") or {}
            out.append(_sse(None, {"error": {
                "message": err.get("message") or "upstream error",
                "type": err.get("type") or "api_error",
            }}))
            out.append(b"data: [DONE]\n\n")
            self.done = True
            return out

        # ping and anything unknown: nothing to emit (a bare SSE comment keeps
        # the connection warm without confusing an OpenAI client).
        if t == "ping":
            out.append(b": keepalive\n\n")
        return out

    def finalize(self) -> List[bytes]:
        """Report an Anthropic stream that ended without message_stop."""
        if self.done:
            return []
        out = [_sse(None, {"error": {
            "type": "incomplete_stream",
            "message": "Upstream Anthropic stream ended without message_stop",
        }}), b"data: [DONE]\n\n"]
        self.done = True
        return out


class OaiToAnthropicStream:
    """OpenAI SSE → Anthropic SSE.

    Emits a spec-valid Anthropic event sequence: ``message_start``,
    per-block ``content_block_start`` / ``_delta`` / ``_stop``,
    ``message_delta`` with the stop reason, then ``message_stop``.
    """

    def __init__(self, model: str = ""):
        self.model = model
        self.mid = _new_id("msg")
        self.started = False
        self.text_open = False
        self.block_idx = 0
        self.tools: Dict[int, int] = {}      # oai tool index -> anthropic block index
        self.tool_open: Dict[int, bool] = {}
        self.stop_reason = "end_turn"
        self.usage = {"input_tokens": 0, "output_tokens": 0}
        self.done = False
        self.saw_terminal = False

    def _start(self) -> List[bytes]:
        if self.started:
            return []
        self.started = True
        return [_sse("message_start", {
            "type": "message_start",
            "message": {"id": self.mid, "type": "message", "role": "assistant",
                        "model": self.model, "content": [], "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": self.usage["input_tokens"],
                                  "output_tokens": 0}},
        })]

    def _close_open_blocks(self) -> List[bytes]:
        out = []
        if self.text_open:
            out.append(_sse("content_block_stop",
                            {"type": "content_block_stop", "index": 0}))
            self.text_open = False
        for oi, bidx in sorted(self.tools.items(), key=lambda kv: kv[1]):
            if self.tool_open.get(oi):
                out.append(_sse("content_block_stop",
                                {"type": "content_block_stop", "index": bidx}))
                self.tool_open[oi] = False
        return out

    def feed(self, event: Optional[str], data: str) -> List[bytes]:
        out: List[bytes] = []
        if data is None:
            return out
        s = data.strip()
        if not s:
            return out
        if s == "[DONE]":
            self.saw_terminal = True
            return self.finalize()
        try:
            obj = json.loads(s)
        except Exception:
            return out
        if not isinstance(obj, dict):
            return out

        if obj.get("error"):
            err = obj["error"]
            out.extend(self._start())
            out.append(_sse("error", {"type": "error", "error": {
                "type": (err.get("type") if isinstance(err, dict) else None) or "api_error",
                "message": (err.get("message") if isinstance(err, dict) else str(err)),
            }}))
            self.done = True
            return out

        if obj.get("model"):
            self.model = obj["model"]
        u = obj.get("usage") or {}
        if u:
            self.usage["input_tokens"] = u.get("prompt_tokens") or self.usage["input_tokens"]
            self.usage["output_tokens"] = u.get("completion_tokens") or self.usage["output_tokens"]

        choices = obj.get("choices") or []
        if not choices:
            return out
        ch = choices[0]
        delta = ch.get("delta") or {}

        content = delta.get("content")
        if isinstance(content, str) and content:
            out.extend(self._start())
            if not self.text_open:
                self.text_open = True
                out.append(_sse("content_block_start", {
                    "type": "content_block_start", "index": 0,
                    "content_block": {"type": "text", "text": ""}}))
                self.block_idx = max(self.block_idx, 1)
            out.append(_sse("content_block_delta", {
                "type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": content}}))

        for tc in delta.get("tool_calls") or []:
            if not isinstance(tc, dict):
                continue
            oi = tc.get("index", 0)
            fn = tc.get("function") or {}
            out.extend(self._start())
            if oi not in self.tools:
                bidx = self.block_idx if self.block_idx > 0 else (1 if self.text_open else 0)
                bidx = max(bidx, 1 if self.text_open else 0)
                self.tools[oi] = bidx
                self.block_idx = bidx + 1
                self.tool_open[oi] = True
                out.append(_sse("content_block_start", {
                    "type": "content_block_start", "index": bidx,
                    "content_block": {"type": "tool_use",
                                      "id": tc.get("id") or _new_id("toolu"),
                                      "name": fn.get("name") or "", "input": {}}}))
            args = fn.get("arguments")
            if args:
                out.append(_sse("content_block_delta", {
                    "type": "content_block_delta", "index": self.tools[oi],
                    "delta": {"type": "input_json_delta", "partial_json": args}}))

        fr = ch.get("finish_reason")
        if fr:
            self.stop_reason = _FINISH_TO_STOP.get(fr, "end_turn")
        return out

    def finalize(self) -> List[bytes]:
        if self.done:
            return []
        out: List[bytes] = []
        out.extend(self._start())
        out.extend(self._close_open_blocks())
        if self.saw_terminal:
            out.append(_sse("message_delta", {
                "type": "message_delta",
                "delta": {"stop_reason": self.stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": self.usage["output_tokens"]}}))
            out.append(_sse("message_stop", {"type": "message_stop"}))
            self.done = True
            return out
        out.append(_sse("error", {"type": "error", "error": {
            "type": "incomplete_stream",
            "message": "Upstream OpenAI stream ended without [DONE]",
        }}))
        self.done = True
        return out


def iter_sse_frames(buf: bytes) -> Tuple[List[Tuple[Optional[str], str]], bytes]:
    """Split a byte buffer into complete SSE frames.

    Returns ``(frames, remainder)`` where each frame is ``(event, data)``.
    Handles both LF and CRLF separators, and multi-line ``data:`` fields.
    """
    frames: List[Tuple[Optional[str], str]] = []
    buf = buf.replace(b"\r\n", b"\n")
    while True:
        i = buf.find(b"\n\n")
        if i < 0:
            return frames, buf
        raw, buf = buf[:i], buf[i + 2:]
        event = None
        data_lines: List[str] = []
        for line in raw.split(b"\n"):
            ls = line.decode("utf-8", "replace")
            if ls.startswith("event:"):
                event = ls[6:].strip()
            elif ls.startswith("data:"):
                data_lines.append(ls[5:].lstrip())
            elif ls.startswith(":"):
                continue
        if data_lines or event:
            frames.append((event, "\n".join(data_lines)))


# ==========================================================================
# OpenAI RESPONSES dialect (``POST /v1/responses``)
#
# The third dialect on the wire. Codex CLI 0.140+ removed `wire_api = "chat"`
# entirely, so a Codex client can ONLY speak this shape; forwarding it verbatim
# to a chat/messages provider yields `500 not implemented` (measured against
# justwoker + kktoken, 2026-09-06) or a 200 with zero bytes (camel-hub).
#
# Everything here is a pure structural mapping. Protocol-owned bytes —
# `call_id`, tool names, the `arguments` JSON string, image data URLs — are
# copied through EXACTLY, never re-serialised through a parse/dump round trip
# when they arrive as strings, because a client that pairs a tool result by
# id must still find that id afterwards.
# ==========================================================================

_RESP_STATUS_TO_STOP = {
    "completed": "end_turn",
    "incomplete": "max_tokens",
    "failed": "end_turn",
    "cancelled": "end_turn",
}


def _resp_text_from_content(content: Any) -> str:
    """Flatten a Responses content array to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for b in content:
            if isinstance(b, str):
                parts.append(b)
            elif isinstance(b, dict) and isinstance(b.get("text"), str):
                if b.get("type") in (None, "input_text", "output_text", "text",
                                     "summary_text", "refusal"):
                    parts.append(b["text"])
        return "\n".join(p for p in parts if p)
    return _as_text(content)


def _args_to_string(args: Any) -> str:
    """A Responses ``arguments`` field is always a JSON *string*.

    A string arrives byte-exact and is returned byte-exact — re-encoding it
    would reorder keys and change whitespace, which is exactly the kind of
    silent mutation that breaks a client diffing its own tool call.
    """
    if isinstance(args, str):
        return args
    return json.dumps(args or {}, ensure_ascii=False)


def _args_to_dict(args: Any) -> Dict[str, Any]:
    if isinstance(args, dict):
        return args
    if isinstance(args, str):
        s = args.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
        except Exception:
            return {"_raw": args}
        return parsed if isinstance(parsed, dict) else {"_raw": args}
    return {}


def _resp_image_to_oai(item: dict) -> Optional[dict]:
    """Responses ``input_image`` → OpenAI ``image_url`` block."""
    url = item.get("image_url") or item.get("url")
    if isinstance(url, dict):
        url = url.get("url")
    if not url and item.get("file_id"):
        # A provider-side file id has no portable representation in either
        # Chat Completions or Anthropic Messages.  Dropping it turns an
        # image-only prompt into an empty user message, which is silent data
        # loss; refuse the cross-dialect request instead.
        raise ValueError("Responses input_image.file_id cannot be translated to a URL")
    if not isinstance(url, str) or not url:
        return None
    return {"type": "image_url", "image_url": {"url": url}}


def responses_request_to_oai(body: dict) -> dict:
    """Responses request → OpenAI chat-completions request."""
    out: Dict[str, Any] = {}
    if body.get("model"):
        out["model"] = body["model"]

    msgs: List[Dict[str, Any]] = []
    pending_calls: List[Dict[str, Any]] = []

    def flush_calls() -> None:
        nonlocal pending_calls
        if pending_calls:
            msgs.append({"role": "assistant", "content": None,
                         "tool_calls": pending_calls})
            pending_calls = []

    instructions = body.get("instructions")
    if isinstance(instructions, str) and instructions:
        msgs.append({"role": "system", "content": instructions})

    raw_input = body.get("input")
    items: List[Any]
    if isinstance(raw_input, str):
        items = [{"role": "user", "content": raw_input}] if raw_input else []
    elif isinstance(raw_input, list):
        items = raw_input
    else:
        items = []

    for it in items:
        if isinstance(it, str):
            msgs.append({"role": "user", "content": it})
            continue
        if not isinstance(it, dict):
            raise ValueError("Responses input items must be strings or objects")
        itype = it.get("type")

        if itype == "function_call":
            pending_calls.append({
                "id": it.get("call_id") or it.get("id") or _new_id("call"),
                "type": "function",
                "function": {"name": it.get("name") or "",
                             "arguments": _args_to_string(it.get("arguments"))},
            })
            continue

        if itype == "function_call_output":
            flush_calls()
            msgs.append({
                "role": "tool",
                "tool_call_id": it.get("call_id") or "",
                "content": _resp_text_from_content(it.get("output")),
            })
            continue

        if itype == "reasoning":
            # No chat counterpart; encrypted_content is opaque and must not be
            # invented into a message.
            continue

        if itype == "additional_tools":
            # Codex "code mode" delivers a tools bundle (typically a namespace
            # container) as a developer input item. The bundle itself carries no
            # conversational content; the tools it declares are flattened into
            # the request's tool list by _flatten_responses_tools below.
            continue

        if itype in ("custom_tool_call", "custom_tool_call_output"):
            # Freeform tool traffic behaves like function calls for our
            # purposes; the arguments/input ride the same fields.
            if itype == "custom_tool_call":
                pending_calls.append({
                    "id": it.get("call_id") or it.get("id") or _new_id("call"),
                    "type": "function",
                    "function": {"name": it.get("name") or "",
                                 "arguments": _args_to_string(it.get("input"))},
                })
            else:
                flush_calls()
                msgs.append({
                    "role": "tool",
                    "tool_call_id": it.get("call_id") or "",
                    "content": _resp_text_from_content(it.get("output")),
                })
            continue

        if itype and itype != "message":
            raise ValueError(f"unsupported Responses input item type: {itype}")

        flush_calls()
        role = it.get("role") or ("assistant" if itype == "message" else "user")
        content = it.get("content")

        if role == "assistant":
            txt = _resp_text_from_content(content)
            if txt:
                msgs.append({"role": "assistant", "content": txt})
            continue

        if isinstance(content, str):
            msgs.append({"role": role, "content": content})
            continue
        if isinstance(content, list):
            parts: List[Dict[str, Any]] = []
            for b in content:
                if isinstance(b, str):
                    parts.append({"type": "text", "text": b})
                    continue
                if not isinstance(b, dict):
                    raise ValueError("Responses content blocks must be strings or objects")
                bt = b.get("type")
                if bt in (None, "input_text", "text", "output_text") and isinstance(b.get("text"), str):
                    parts.append({"type": "text", "text": b["text"]})
                elif bt in ("input_image", "image_url", "image"):
                    img = _resp_image_to_oai(b)
                    if img:
                        parts.append(img)
                else:
                    raise ValueError(f"unsupported Responses content block type: {bt}")
            msgs.append({"role": role, "content": parts or ""})
            continue
        if content is not None:
            msgs.append({"role": role, "content": _as_text(content)})

    flush_calls()
    out["messages"] = msgs

    mt = body.get("max_output_tokens") or body.get("max_tokens")
    try:
        mt = int(mt) if mt else 0
    except (TypeError, ValueError):
        mt = 0
    if mt > 0:
        out["max_tokens"] = mt

    for k in ("temperature", "top_p", "stream"):
        if body.get(k) is not None:
            out[k] = body[k]

    # Codex tool shapes and how they bridge to a function-only upstream
    # (behavior matches LiteLLM's Responses bridge):
    #   - `namespace` containers hold REAL function tools nested inside
    #     (multi_agent_v1 -> spawn_agent/send_input/...). Dropping the whole
    #     namespace silently removes those capabilities, so FLATTEN: emit each
    #     nested function tool, discard the container itself.
    #   - `custom` (freeform/grammar) tools become ordinary function tools —
    #     the model sees the description; the grammar is not executable by a
    #     non-OpenAI upstream anyway.
    #   - provider-executed tools (web_search, image_generation, mcp, ...) have
    #     no client-side meaning on an Anthropic-dialect upstream: DROP them.
    _DROPPABLE_RESPONSES_TOOL_TYPES = ("web_search", "web_search_request",
                                       "image_generation", "code_interpreter", "mcp")

    def _flatten_responses_tools(entries, *, depth: int = 0):
        conv = []
        if not isinstance(entries, list):
            return conv
        if depth > 3:
            raise ValueError("Responses tools nested deeper than 3 levels")
        for t in entries:
            if not isinstance(t, dict):
                raise ValueError("Responses tools must be objects")
            ttype = t.get("type")
            if ttype == "namespace":
                conv.extend(_flatten_responses_tools(t.get("tools") or [], depth=depth + 1))
                continue
            if ttype == "custom":
                name = t.get("name") or ""
                if not name:
                    raise ValueError("Responses custom tool is missing a name")
                conv.append({"type": "function", "function": {
                    "name": name,
                    "description": t.get("description") or "",
                    "parameters": {"type": "object", "properties": {}},
                }})
                continue
            if ttype in _DROPPABLE_RESPONSES_TOOL_TYPES:
                continue
            if ttype not in (None, "function"):
                raise ValueError(f"provider-executed Responses tool type {ttype!r} cannot be translated safely")
            name = t.get("name") or ((t.get("function") or {}).get("name"))
            if not name:
                raise ValueError("Responses function tool is missing a name")
            src = t if t.get("parameters") is not None else (t.get("function") or {})
            conv.append({"type": "function", "function": {
                "name": name,
                "description": t.get("description") or src.get("description") or "",
                "parameters": src.get("parameters") or {"type": "object", "properties": {}},
            }})
        return conv

    tools = body.get("tools")
    conv = _flatten_responses_tools(tools) if isinstance(tools, list) else []
    # "additional_tools" input items declare more tools the same way.
    for it in items if isinstance(raw_input, list) else []:
        if isinstance(it, dict) and it.get("type") == "additional_tools" and isinstance(it.get("tools"), list):
            conv.extend(_flatten_responses_tools(it["tools"]))
    if conv:
        out["tools"] = conv

    tc = body.get("tool_choice")
    if isinstance(tc, str) and tc in ("auto", "none", "required"):
        out["tool_choice"] = tc
    elif isinstance(tc, dict):
        name = tc.get("name") or ((tc.get("function") or {}).get("name"))
        if name:
            out["tool_choice"] = {"type": "function", "function": {"name": name}}
    return out


def oai_request_to_responses(body: dict) -> dict:
    """OpenAI chat-completions request → Responses request."""
    out: Dict[str, Any] = {}
    if body.get("model"):
        out["model"] = body["model"]

    systems: List[str] = []
    items: List[Dict[str, Any]] = []

    for m in body.get("messages") or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")

        if role in ("system", "developer"):
            t = _as_text(content)
            if t:
                systems.append(t)
            continue

        if role == "tool":
            items.append({
                "type": "function_call_output",
                "call_id": m.get("tool_call_id") or "",
                "output": [{"type": "input_text", "text": _as_text(content)}],
            })
            continue

        if role == "assistant":
            txt = _as_text(content)
            if txt:
                items.append({"type": "message", "role": "assistant",
                              "content": [{"type": "output_text", "text": txt}]})
            for tc in m.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") or {}
                items.append({
                    "type": "function_call",
                    "call_id": tc.get("id") or _new_id("call"),
                    "name": fn.get("name") or "",
                    "arguments": _args_to_string(fn.get("arguments")),
                })
            continue

        parts: List[Dict[str, Any]] = []
        if isinstance(content, str):
            parts = [{"type": "input_text", "text": content}] if content else []
        elif isinstance(content, list):
            for b in content:
                if isinstance(b, str):
                    parts.append({"type": "input_text", "text": b})
                elif isinstance(b, dict):
                    bt = b.get("type")
                    if bt in (None, "text", "input_text") and isinstance(b.get("text"), str):
                        parts.append({"type": "input_text", "text": b["text"]})
                    elif bt in ("image_url", "input_image"):
                        url = b.get("image_url") or b.get("url")
                        if isinstance(url, dict):
                            url = url.get("url")
                        if isinstance(url, str) and url:
                            parts.append({"type": "input_image", "image_url": url})
        elif content is not None:
            parts = [{"type": "input_text", "text": _as_text(content)}]
        items.append({"role": role or "user", "content": parts})

    if systems:
        out["instructions"] = "\n\n".join(systems)
    out["input"] = items

    mt = body.get("max_tokens") or body.get("max_completion_tokens")
    try:
        mt = int(mt) if mt else 0
    except (TypeError, ValueError):
        mt = 0
    if mt > 0:
        out["max_output_tokens"] = mt

    for k in ("temperature", "top_p", "stream"):
        if body.get(k) is not None:
            out[k] = body[k]

    tools = body.get("tools")
    if isinstance(tools, list) and tools:
        conv = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            fn = t.get("function") if t.get("type") == "function" else t
            if not isinstance(fn, dict) or not fn.get("name"):
                continue
            conv.append({"type": "function", "name": fn["name"],
                         "description": fn.get("description") or "",
                         "parameters": fn.get("parameters") or {"type": "object", "properties": {}}})
        if conv:
            out["tools"] = conv

    tc = body.get("tool_choice")
    if isinstance(tc, str) and tc in ("auto", "none", "required"):
        out["tool_choice"] = tc
    elif isinstance(tc, dict):
        name = ((tc.get("function") or {}).get("name")) or tc.get("name")
        if name:
            out["tool_choice"] = {"type": "function", "name": name}
    return out


def anthropic_request_to_responses(body: dict) -> dict:
    """Anthropic messages request → Responses request (via the chat shape).

    Composed deliberately: the Anthropic→chat mapping already handles
    tool_result merging, image sources and system hoisting, and composing
    keeps one source of truth per concept instead of two near-copies that
    drift.
    """
    return oai_request_to_responses(anthropic_request_to_oai(body))


def responses_request_to_anthropic(body: dict) -> dict:
    """Responses request → Anthropic messages request (via the chat shape)."""
    return oai_request_to_anthropic(responses_request_to_oai(body))


def oai_response_to_responses(resp: dict, *, model: str = "") -> dict:
    """OpenAI completion object → Responses response object."""
    choices = resp.get("choices") or []
    choice = choices[0] if choices else {}
    msg = choice.get("message") or {}

    output: List[Dict[str, Any]] = []
    txt = msg.get("content")
    text = txt if isinstance(txt, str) else _as_text(txt)
    if text:
        output.append({
            "id": _new_id("msg"), "type": "message", "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text}],
        })
    for tc in msg.get("tool_calls") or []:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        output.append({
            "id": _new_id("fc"), "type": "function_call", "status": "completed",
            "name": fn.get("name") or "",
            "call_id": tc.get("id") or _new_id("call"),
            "arguments": _args_to_string(fn.get("arguments")),
        })

    usage = resp.get("usage") or {}
    pt = usage.get("prompt_tokens") or 0
    ct = usage.get("completion_tokens") or 0
    finish = choice.get("finish_reason") or ""
    out: Dict[str, Any] = {
        "id": resp.get("id") or _new_id("resp"),
        "object": "response",
        "created_at": resp.get("created") or int(time.time()),
        "status": "incomplete" if finish == "length" else "completed",
        "model": resp.get("model") or model or "",
        "output": output,
        "usage": {"input_tokens": pt, "output_tokens": ct,
                  "total_tokens": usage.get("total_tokens") or (pt + ct)},
    }
    if finish == "length":
        out["incomplete_details"] = {"reason": "max_output_tokens"}
    return out


def anthropic_response_to_responses(msg: dict, *, model: str = "") -> dict:
    """Anthropic Message object → Responses response object."""
    output: List[Dict[str, Any]] = []
    text_parts: List[str] = []
    calls: List[Dict[str, Any]] = []
    for b in msg.get("content") or []:
        if not isinstance(b, dict):
            continue
        if b.get("type") == "text" and b.get("text"):
            text_parts.append(b["text"])
        elif b.get("type") == "tool_use":
            calls.append({
                "id": _new_id("fc"), "type": "function_call", "status": "completed",
                "name": b.get("name") or "",
                "call_id": b.get("id") or _new_id("call"),
                "arguments": json.dumps(b.get("input") or {}, ensure_ascii=False),
            })
    if text_parts:
        output.append({
            "id": msg.get("id") or _new_id("msg"), "type": "message",
            "status": "completed", "role": "assistant",
            "content": [{"type": "output_text", "text": "\n".join(text_parts)}],
        })
    output.extend(calls)

    usage = msg.get("usage") or {}
    it = usage.get("input_tokens") or 0
    ot = usage.get("output_tokens") or 0
    stop = msg.get("stop_reason") or ""
    out: Dict[str, Any] = {
        "id": msg.get("id") or _new_id("resp"),
        "object": "response",
        "created_at": int(time.time()),
        "status": "incomplete" if stop == "max_tokens" else "completed",
        "model": msg.get("model") or model or "",
        "output": output,
        "usage": {"input_tokens": it, "output_tokens": ot, "total_tokens": it + ot},
    }
    if stop == "max_tokens":
        out["incomplete_details"] = {"reason": "max_output_tokens"}
    return out


def _responses_output_to_parts(resp: dict) -> Tuple[str, List[Dict[str, Any]], str]:
    """Split a Responses object into (text, tool_calls, stop_hint)."""
    text_parts: List[str] = []
    calls: List[Dict[str, Any]] = []
    for item in resp.get("output") or []:
        if not isinstance(item, dict):
            continue
        it = item.get("type")
        if it == "message":
            t = _resp_text_from_content(item.get("content"))
            if t:
                text_parts.append(t)
        elif it == "function_call":
            calls.append({
                "id": item.get("call_id") or item.get("id") or _new_id("call"),
                "name": item.get("name") or "",
                "arguments": _args_to_string(item.get("arguments")),
            })
    status = resp.get("status") or "completed"
    if calls:
        stop = "tool_use"
    elif status == "incomplete":
        stop = "max_tokens"
    else:
        stop = _RESP_STATUS_TO_STOP.get(status, "end_turn")
    return "\n".join(text_parts), calls, stop


def responses_response_to_anthropic(resp: dict, *, model: str = "") -> dict:
    """Responses response object → Anthropic Message object."""
    if resp.get("status") in ("failed", "cancelled") or resp.get("error"):
        err = resp.get("error") or {}
        msg = err.get("message") if isinstance(err, dict) else str(err)
        raise ValueError(f"Responses upstream failure cannot be translated as success: {msg or resp.get('status')}")
    text, calls, stop = _responses_output_to_parts(resp)
    blocks: List[Dict[str, Any]] = []
    if text:
        blocks.append({"type": "text", "text": text})
    for c in calls:
        blocks.append({"type": "tool_use", "id": c["id"], "name": c["name"],
                       "input": _args_to_dict(c["arguments"])})
    usage = resp.get("usage") or {}
    return {
        "id": resp.get("id") or _new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": resp.get("model") or model or "",
        "content": blocks,
        "stop_reason": stop,
        "stop_sequence": None,
        "usage": {"input_tokens": usage.get("input_tokens") or 0,
                  "output_tokens": usage.get("output_tokens") or 0},
    }


def responses_response_to_oai(resp: dict, *, model: str = "") -> dict:
    """Responses response object → OpenAI completion object."""
    if resp.get("status") in ("failed", "cancelled") or resp.get("error"):
        err = resp.get("error") or {}
        msg = err.get("message") if isinstance(err, dict) else str(err)
        raise ValueError(f"Responses upstream failure cannot be translated as success: {msg or resp.get('status')}")
    text, calls, stop = _responses_output_to_parts(resp)
    message: Dict[str, Any] = {"role": "assistant", "content": text or None}
    if calls:
        message["tool_calls"] = [{
            "id": c["id"], "type": "function",
            "function": {"name": c["name"], "arguments": c["arguments"]},
        } for c in calls]
    usage = resp.get("usage") or {}
    pt = usage.get("input_tokens") or 0
    ct = usage.get("output_tokens") or 0
    return {
        "id": resp.get("id") or _new_id("chatcmpl"),
        "object": "chat.completion",
        "created": resp.get("created_at") or int(time.time()),
        "model": resp.get("model") or model or "",
        "choices": [{"index": 0, "message": message,
                     "finish_reason": _STOP_TO_FINISH.get(stop, "stop")}],
        "usage": {"prompt_tokens": pt, "completion_tokens": ct,
                  "total_tokens": usage.get("total_tokens") or (pt + ct)},
    }


# --------------------------------------------------------------------------
# Three-way dispatch
# --------------------------------------------------------------------------

_REQUEST_MAP = {
    ("anthropic", "openai"): anthropic_request_to_oai,
    ("anthropic", "responses"): anthropic_request_to_responses,
    ("openai", "anthropic"): oai_request_to_anthropic,
    ("openai", "responses"): oai_request_to_responses,
    ("responses", "anthropic"): responses_request_to_anthropic,
    ("responses", "openai"): responses_request_to_oai,
}

_RESPONSE_MAP = {
    ("anthropic", "openai"): anthropic_response_to_oai,
    ("anthropic", "responses"): anthropic_response_to_responses,
    ("openai", "anthropic"): oai_response_to_anthropic,
    ("openai", "responses"): oai_response_to_responses,
    ("responses", "anthropic"): responses_response_to_anthropic,
    ("responses", "openai"): responses_response_to_oai,
}


def _check_dialect(name: str, value: str) -> str:
    if value not in DIALECTS:
        raise ValueError(f"unknown {name} dialect {value!r}; expected one of {DIALECTS}")
    return value


def request_to(src: str, dst: str, body: dict) -> dict:
    """Translate a REQUEST body from dialect ``src`` to dialect ``dst``.

    Identity is a passthrough that returns the SAME object — never a
    re-serialised copy, so a same-dialect request keeps its bytes exactly.
    """
    _check_dialect("source", src)
    _check_dialect("target", dst)
    if src == dst:
        return body
    return _REQUEST_MAP[(src, dst)](body)


def response_to(src: str, dst: str, obj: dict, *, model: str = "") -> dict:
    """Translate an assembled RESPONSE object from dialect ``src`` to ``dst``."""
    _check_dialect("source", src)
    _check_dialect("target", dst)
    if src == dst:
        return obj
    return _RESPONSE_MAP[(src, dst)](obj, model=model)


def stream_terminated(dialect: str, event: Optional[str], data: str) -> bool:
    """Did this SSE frame END the stream, in ``dialect``'s own grammar?

    Each dialect says "done" differently, and mistaking a live stream for a
    finished one (or vice versa) is what makes a complete answer get scored
    truncated and the whole provider retried.
    """
    _check_dialect("stream", dialect)
    if dialect == "openai":
        return bool(data) and data.strip() == "[DONE]"
    if dialect == "responses":
        if event in ("response.completed", "response.failed", "response.incomplete"):
            return True
        s = (data or "").strip()
        if s.startswith("{"):
            try:
                t = (json.loads(s) or {}).get("type")
            except Exception:
                return False
            return t in ("response.completed", "response.failed", "response.incomplete")
        return False
    return event == "message_stop"


# --------------------------------------------------------------------------
# STREAM translators for the Responses dialect
#
# Design note: a Responses stream is ITEM-oriented, while Anthropic is
# block-oriented and chat is chunk-oriented. Rather than write six pairwise
# stream translators (and six places for the same bug), everything is funnelled
# through ONE canonical intermediate: the chat-chunk shape, which both existing
# translators already speak. So:
#     anthropic -> responses  =  AnthropicToOai  -> OaiToResponses
#     responses -> anthropic  =  ResponsesToOai  -> OaiToAnthropic
# The chat path is native on both sides, so no double translation happens for
# the pair that matters most (chat <-> responses).
# --------------------------------------------------------------------------


class _ChainStream:
    """Feed frames through two stream translators in sequence."""

    def __init__(self, first, second):
        self.first, self.second = first, second

    @property
    def done(self) -> bool:
        return bool(getattr(self.second, "done", False))

    def _pump(self, produced: List[bytes]) -> List[bytes]:
        out: List[bytes] = []
        for raw in produced:
            for ev, data in iter_sse_frames(raw if raw.endswith(b"\n\n") else raw + b"\n\n")[0]:
                out.extend(self.second.feed(ev, data))
        return out

    def feed(self, event: Optional[str], data: str) -> List[bytes]:
        return self._pump(self.first.feed(event, data))

    def finalize(self) -> List[bytes]:
        out = self._pump(self.first.finalize())
        out.extend(self.second.finalize())
        return out


class OaiToResponsesStream:
    """OpenAI SSE (``chat.completion.chunk``) → Responses SSE.

    Emits the event sequence a Responses client validates: ``response.created``,
    ``response.output_item.added`` for the assistant message, text deltas,
    ``response.output_text.done``, ``function_call`` items with their argument
    deltas, and a terminal ``response.completed`` carrying the assembled output
    plus usage. Every event gets a monotonic ``sequence_number`` — Codex-style
    clients check it.
    """

    def __init__(self, model: str = ""):
        self.model = model
        self.rid = _new_id("resp")
        self.created = int(time.time())
        self.seq = 0
        self.out_index = 0
        self.msg_item_id = _new_id("msg")
        self.msg_open = False
        self.msg_index: Optional[int] = None
        self.text_parts: List[str] = []
        self.tools: Dict[int, Dict[str, Any]] = {}
        self.status = "completed"
        self.incomplete_reason: Optional[str] = None
        self.usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        self.started = False
        self.done = False
        self.saw_terminal = False

    # -- helpers ---------------------------------------------------------
    def _ev(self, name: str, payload: Dict[str, Any]) -> bytes:
        payload.setdefault("type", name)
        payload["sequence_number"] = self.seq
        self.seq += 1
        return _sse(name, payload)

    def _envelope(self, status: str) -> Dict[str, Any]:
        return {"id": self.rid, "object": "response", "status": status,
                "created_at": self.created, "model": self.model}

    def _start(self) -> List[bytes]:
        if self.started:
            return []
        self.started = True
        env = self._envelope("in_progress")
        env["output"] = []
        return [self._ev("response.created", {"response": env})]

    def _open_msg(self) -> List[bytes]:
        if self.msg_open:
            return []
        self.msg_open = True
        self.msg_index = self.out_index
        self.out_index += 1
        return [self._ev("response.output_item.added", {
            "output_index": self.msg_index,
            "item": {"id": self.msg_item_id, "type": "message", "status": "in_progress",
                     "role": "assistant", "content": []},
        })]

    # -- feed ------------------------------------------------------------
    def feed(self, event: Optional[str], data: str) -> List[bytes]:
        out: List[bytes] = []
        if data is None:
            return out
        s = data.strip()
        if not s:
            return out
        if s == "[DONE]":
            self.saw_terminal = True
            return self.finalize()
        try:
            obj = json.loads(s)
        except Exception:
            return out
        if not isinstance(obj, dict):
            return out

        if obj.get("error"):
            err = obj["error"]
            out.extend(self._start())
            env = self._envelope("failed")
            env["output"] = self._items()
            env["error"] = {
                "code": (err.get("type") if isinstance(err, dict) else None) or "api_error",
                "message": (err.get("message") if isinstance(err, dict) else str(err)),
            }
            out.append(self._ev("response.failed", {"response": env}))
            self.done = True
            return out

        if obj.get("model"):
            self.model = obj["model"]
        u = obj.get("usage") or {}
        if u:
            self.usage["input_tokens"] = u.get("prompt_tokens") or self.usage["input_tokens"]
            self.usage["output_tokens"] = u.get("completion_tokens") or self.usage["output_tokens"]
            self.usage["total_tokens"] = (u.get("total_tokens")
                                          or self.usage["input_tokens"] + self.usage["output_tokens"])

        choices = obj.get("choices") or []
        if not choices:
            return out
        ch = choices[0] if isinstance(choices[0], dict) else {}
        delta = ch.get("delta") or {}

        content = delta.get("content")
        if isinstance(content, str) and content:
            out.extend(self._start())
            out.extend(self._open_msg())
            self.text_parts.append(content)
            out.append(self._ev("response.output_text.delta", {
                "item_id": self.msg_item_id, "output_index": self.msg_index,
                "content_index": 0, "delta": content, "logprobs": [],
            }))

        for tc in delta.get("tool_calls") or []:
            if not isinstance(tc, dict):
                continue
            oi = tc.get("index", 0)
            fn = tc.get("function") or {}
            out.extend(self._start())
            if oi not in self.tools:
                idx = self.out_index
                self.out_index += 1
                item_id = _new_id("fc")
                self.tools[oi] = {"item_id": item_id, "output_index": idx,
                                  "call_id": tc.get("id") or _new_id("call"),
                                  "name": fn.get("name") or "", "args": ""}
                out.append(self._ev("response.output_item.added", {
                    "output_index": idx,
                    "item": {"id": item_id, "type": "function_call", "status": "in_progress",
                             "name": self.tools[oi]["name"],
                             "call_id": self.tools[oi]["call_id"], "arguments": ""},
                }))
            else:
                if tc.get("id"):
                    self.tools[oi]["call_id"] = tc["id"]
                if fn.get("name"):
                    self.tools[oi]["name"] = fn["name"]
            args = fn.get("arguments")
            if args:
                self.tools[oi]["args"] += args
                out.append(self._ev("response.function_call_arguments.delta", {
                    "item_id": self.tools[oi]["item_id"],
                    "output_index": self.tools[oi]["output_index"],
                    "delta": args,
                }))

        fr = ch.get("finish_reason")
        if fr == "length":
            self.status = "incomplete"
            self.incomplete_reason = "max_output_tokens"
        return out

    # -- close -----------------------------------------------------------
    def _items(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        text = "".join(self.text_parts)
        if self.msg_open:
            items.append({"id": self.msg_item_id, "type": "message", "status": "completed",
                          "role": "assistant",
                          "content": [{"type": "output_text", "text": text}]})
        for oi in sorted(self.tools):
            t = self.tools[oi]
            items.append({"id": t["item_id"], "type": "function_call", "status": "completed",
                          "name": t["name"], "call_id": t["call_id"], "arguments": t["args"]})
        return items

    def finalize(self) -> List[bytes]:
        if self.done:
            return []
        out: List[bytes] = []
        out.extend(self._start())
        if not self.saw_terminal:
            env = self._envelope("failed")
            env["output"] = self._items()
            env["error"] = {"code": "incomplete_stream",
                            "message": "Upstream stream ended without a terminal marker"}
            out.append(self._ev("response.failed", {"response": env}))
            self.done = True
            return out
        text = "".join(self.text_parts)
        if self.msg_open:
            out.append(self._ev("response.output_text.done", {
                "item_id": self.msg_item_id, "output_index": self.msg_index,
                "content_index": 0, "text": text, "logprobs": [],
            }))
            out.append(self._ev("response.output_item.done", {
                "output_index": self.msg_index,
                "item": {"id": self.msg_item_id, "type": "message", "status": "completed",
                         "role": "assistant",
                         "content": [{"type": "output_text", "text": text}]},
            }))
        for oi in sorted(self.tools):
            t = self.tools[oi]
            out.append(self._ev("response.function_call_arguments.done", {
                "item_id": t["item_id"], "output_index": t["output_index"],
                "arguments": t["args"],
            }))
            out.append(self._ev("response.output_item.done", {
                "output_index": t["output_index"],
                "item": {"id": t["item_id"], "type": "function_call", "status": "completed",
                         "name": t["name"], "call_id": t["call_id"], "arguments": t["args"]},
            }))
        env = self._envelope(self.status)
        env["output"] = self._items()
        env["usage"] = dict(self.usage)
        if self.incomplete_reason:
            env["incomplete_details"] = {"reason": self.incomplete_reason}
        name = "response.incomplete" if self.status == "incomplete" else "response.completed"
        out.append(self._ev(name, {"response": env}))
        self.done = True
        return out


class ResponsesToOaiStream:
    """Responses SSE → OpenAI SSE (``chat.completion.chunk`` frames)."""

    def __init__(self, model: str = ""):
        self.model = model
        self.cid = _new_id("chatcmpl")
        self.created = int(time.time())
        self.sent_role = False
        self.tool_slots: Dict[str, int] = {}     # responses item_id -> oai tool index
        self.next_tool = 0
        self.finish = "stop"
        self.usage: Dict[str, int] = {}
        self.done = False

    def _chunk(self, delta: dict, finish: Optional[str] = None) -> bytes:
        return _sse(None, {
            "id": self.cid, "object": "chat.completion.chunk",
            "created": self.created, "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        })

    def _role(self) -> List[bytes]:
        if self.sent_role:
            return []
        self.sent_role = True
        return [self._chunk({"role": "assistant", "content": ""})]

    def feed(self, event: Optional[str], data: str) -> List[bytes]:
        out: List[bytes] = []
        if data is None:
            return out
        s = data.strip()
        if not s or s == "[DONE]":
            return out
        try:
            obj = json.loads(s)
        except Exception:
            return out
        if not isinstance(obj, dict):
            return out
        t = obj.get("type") or event or ""

        if t == "response.created":
            m = ((obj.get("response") or {}).get("model"))
            if m:
                self.model = m
            return out

        if t == "response.output_text.delta":
            d = obj.get("delta")
            if isinstance(d, str) and d:
                out.extend(self._role())
                out.append(self._chunk({"content": d}))
            return out

        if t == "response.output_item.added":
            item = obj.get("item") or {}
            if item.get("type") == "function_call":
                item_id = item.get("id") or item.get("call_id") or _new_id("fc")
                if item_id not in self.tool_slots:
                    oi = self.next_tool
                    self.next_tool += 1
                    self.tool_slots[item_id] = oi
                    out.extend(self._role())
                    out.append(self._chunk({"tool_calls": [{
                        "index": oi,
                        "id": item.get("call_id") or item_id,
                        "type": "function",
                        "function": {"name": item.get("name") or "",
                                     "arguments": ""},
                    }]}))
                    args = item.get("arguments")
                    if isinstance(args, str) and args:
                        out.append(self._chunk({"tool_calls": [{
                            "index": oi, "function": {"arguments": args}}]}))
                self.finish = "tool_calls"
            return out

        if t == "response.function_call_arguments.delta":
            item_id = obj.get("item_id") or ""
            oi = self.tool_slots.get(item_id)
            if oi is None:
                oi = self.next_tool
                self.next_tool += 1
                self.tool_slots[item_id] = oi
                out.extend(self._role())
                out.append(self._chunk({"tool_calls": [{
                    "index": oi, "id": item_id or _new_id("call"), "type": "function",
                    "function": {"name": "", "arguments": ""}}]}))
            d = obj.get("delta")
            if isinstance(d, str) and d:
                out.append(self._chunk({"tool_calls": [{
                    "index": oi, "function": {"arguments": d}}]}))
            self.finish = "tool_calls"
            return out

        if t in ("response.completed", "response.incomplete"):
            resp = obj.get("response") or {}
            if resp.get("model"):
                self.model = resp["model"]
            if t == "response.incomplete" or resp.get("status") == "incomplete":
                self.finish = "length"
            elif any((i or {}).get("type") == "function_call" for i in resp.get("output") or []):
                self.finish = "tool_calls"
            ru = resp.get("usage") or {}
            if isinstance(ru, dict):
                inp = ru.get("input_tokens") or 0
                outp = ru.get("output_tokens") or 0
                self.usage = {"prompt_tokens": inp, "completion_tokens": outp,
                              "total_tokens": int(ru.get("total_tokens") or (inp + outp))}
            out.extend(self._role())
            terminal = json.loads(self._chunk({}, finish=self.finish).split(b"data: ", 1)[1])
            if self.usage:
                terminal["usage"] = self.usage
            out.append(_sse(None, terminal))
            out.append(b"data: [DONE]\n\n")
            self.done = True
            return out

        if t == "response.failed" or t == "error":
            resp = obj.get("response") or {}
            err = resp.get("error") or obj.get("error") or {}
            out.append(_sse(None, {"error": {
                "message": (err.get("message") if isinstance(err, dict) else str(err))
                           or "upstream error",
                "type": (err.get("code") if isinstance(err, dict) else None) or "api_error",
            }}))
            out.append(b"data: [DONE]\n\n")
            self.done = True
            return out

        return out

    def finalize(self) -> List[bytes]:
        if self.done:
            return []
        # EOF without response.completed/failed/incomplete is a transport
        # truncation. Do not manufacture a successful finish_reason.
        out = [_sse(None, {"error": {
            "type": "incomplete_stream",
            "message": "Upstream Responses stream ended without a terminal event",
        }}), b"data: [DONE]\n\n"]
        self.done = True
        return out


def stream_translator(src: str, dst: str, *, model: str = ""):
    """Return a stream translator object for ``src`` → ``dst``.

    The object exposes ``feed(event, data) -> [bytes]`` and
    ``finalize() -> [bytes]``, matching the two existing translators, so the
    forwarder's stream loop stays one code path for all six directions.
    ``src == dst`` returns None: the caller must relay bytes verbatim rather
    than parse and re-encode a stream it does not need to touch.
    """
    _check_dialect("source", src)
    _check_dialect("target", dst)
    if src == dst:
        return None
    if (src, dst) == ("anthropic", "openai"):
        return AnthropicToOaiStream(model)
    if (src, dst) == ("openai", "anthropic"):
        return OaiToAnthropicStream(model)
    if (src, dst) == ("openai", "responses"):
        return OaiToResponsesStream(model)
    if (src, dst) == ("responses", "openai"):
        return ResponsesToOaiStream(model)
    if (src, dst) == ("anthropic", "responses"):
        return _ChainStream(AnthropicToOaiStream(model), OaiToResponsesStream(model))
    if (src, dst) == ("responses", "anthropic"):
        return _ChainStream(ResponsesToOaiStream(model), OaiToAnthropicStream(model))
    raise ValueError(f"no stream translator for {src!r} -> {dst!r}")
