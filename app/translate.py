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
    """Return 'anthropic' or 'openai' for the INBOUND request.

    The path is authoritative (``v1/messages`` vs ``v1/chat/completions``);
    the body is only consulted when the path is ambiguous, using the two
    fields that exist in exactly one dialect: Anthropic has a top-level
    ``system`` string and requires ``max_tokens``; OpenAI carries the system
    prompt as a message with ``role: "system"``.
    """
    p = (path or "").lower().rstrip("/")
    if p.endswith("chat/completions"):
        return "openai"
    if p.endswith("messages"):
        return "anthropic"
    if isinstance(body, dict):
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


def needs_translation(client_shape: str, endpoint_mode: str) -> bool:
    """True when the client dialect differs from the endpoint dialect."""
    ep = "openai" if endpoint_mode == "openai" else "anthropic"
    return (client_shape or "anthropic") != ep


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

    def _chunk(self, delta: dict, finish: Optional[str] = None) -> bytes:
        return _sse(None, {
            "id": self.cid, "object": "chat.completion.chunk",
            "created": self.created, "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        })

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
        """Close a stream the upstream cut without a message_stop."""
        if self.done:
            return []
        out = []
        if not self.sent_role:
            out.append(self._chunk({"role": "assistant", "content": ""}))
        out.append(self._chunk({}, finish=self.finish))
        out.append(b"data: [DONE]\n\n")
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
        out.append(_sse("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": self.stop_reason, "stop_sequence": None},
            "usage": {"output_tokens": self.usage["output_tokens"]}}))
        out.append(_sse("message_stop", {"type": "message_stop"}))
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
