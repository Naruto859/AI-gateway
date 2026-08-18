"""Core reverse proxy — IMPROVED BRIDGE (sandbox).

Key idea (the "Claude Code logic"): the upstream leg is ALWAYS streamed, even when
the client asked for a non-streaming response. Streaming keeps `ping` events / token
deltas flowing, so the connection never sits idle and the WAF / load balancer never
cuts it mid-generation. Then:

  - client wants streaming  -> relay the SSE bytes through verbatim (pings included)
  - client wants non-stream -> consume the full SSE upstream, ASSEMBLE the final
    JSON message, and return it as one application/json response. The client (e.g.
    Hermes) just waits — exactly as it would for a normal non-streaming call — but
    the upstream leg was crash-proof.

Also: per-request proxy failover, WAF detection, content-filtering, and
content-type normalisation (agentrouter returns non-stream bodies as text/plain).
"""
import time
import json
import random
import socket
import asyncio
import httpx
from starlette.responses import StreamingResponse, Response, JSONResponse
from . import db, proxy_pool, filters


# TCP keepalive so the residential-proxy CONNECT tunnel is NOT idle-dropped while
# the upstream model "thinks" before emitting its first SSE byte. Without this,
# slow generations (large Hermes bodies) close the proxy tunnel mid-flight and the
# assembled stream is "incomplete". All three values are dashboard-tunable.
def _keepalive_opts():
    try:
        idle = int(float(db.get_setting("keepalive_idle", "30") or 30))
        intvl = int(float(db.get_setting("keepalive_intvl", "5") or 5))
        cnt = int(float(db.get_setting("keepalive_cnt", "174") or 174))
    except (TypeError, ValueError):
        idle, intvl, cnt = 30, 5, 174
    return [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle),
        (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl),
        (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt),
    ]


def _build_client(candidates, timeout):
    """httpx.AsyncClient with TCP keepalive and Hedging proxy support."""
    if len(candidates) > 1:
        hedge_urls = ",".join(c["url"] for c in candidates)
        proxy = httpx.Proxy("http://127.0.0.1:9090", headers={"x-hedge-proxies": hedge_urls})
    else:
        proxy = candidates[0]["url"]
    transport = httpx.AsyncHTTPTransport(proxy=proxy, socket_options=_keepalive_opts())
    return httpx.AsyncClient(transport=transport, timeout=timeout)


async def _retry_backoff(retry_num):
    """Claude-Code-style exponential backoff + jitter: min(initial×2^n, max) + 0-25% jitter.
    Spacing out retries prevents the Aliyun WAF from mistaking rapid-fire requests as an attack.
    Both bounds are dashboard-tunable."""
    try:
        initial = float(db.get_setting("retry_initial_delay", "1.0") or 1.0)
        cap = float(db.get_setting("retry_max_delay", "8.0") or 8.0)
    except (TypeError, ValueError):
        initial, cap = 1.0, 8.0
    delay = min(initial * (2 ** retry_num), cap)
    delay *= 0.75 + random.random() * 0.25
    await asyncio.sleep(delay)
HOP_BY_HOP = {
    "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
    "accept-encoding", "x-admin-token",
}


def _build_upstream_headers(request, upstream_key, kind):
    headers = {}
    for k, v in request.headers.items():
        if k.lower() in HOP_BY_HOP:
            continue
        headers[k] = v
    if kind == "openai":
        # OpenAI chat/completions expects a Bearer token
        headers.pop("x-api-key", None)
        headers["authorization"] = f"Bearer {upstream_key}"
    else:
        # Anthropic /v1/messages expects x-api-key
        headers.pop("authorization", None)
        headers["x-api-key"] = upstream_key
        headers.setdefault("anthropic-version", "2023-06-01")
        if db.get_setting("claude_mimicry", "1") == "1":
            # 100% exact Claude Code mimicry: spoof a nodejs environment and the claude-cli UA.
            # Strip all client x-stainless headers to hide Hermes/Python fingerprint.
            for k in list(headers.keys()):
                if k.lower().startswith("x-stainless-"):
                    headers.pop(k)
            headers["user-agent"] = "claude-cli/2.1.177 (external, cli)"
            headers["x-stainless-lang"] = "node"
            headers["x-stainless-package-version"] = "0.33.1"
            headers["x-stainless-os"] = "linux"
            headers["x-stainless-arch"] = "x64"
            headers["x-stainless-runtime"] = "node"
            headers["x-stainless-runtime-version"] = "v22.13.0"
            headers.setdefault("anthropic-beta",
                               "claude-code-20250219,"
                               "fine-grained-tool-streaming-2025-05-14,"
                               "prompt-caching-2025-07-21,"
                               "context-1m-2025-08-07")
        else:
            # Just the basic UA (required by agentrouter) without wiping client's own SDK headers
            headers["user-agent"] = "claude-cli/2.1.177 (external, cli)"
    headers["accept-encoding"] = "identity"
    return headers


def _is_waf(resp_headers, sniff=b""):
    ct = resp_headers.get("content-type", "")
    if "text/html" in ct:
        return True
    if b"aliyun_waf" in sniff[:600]:
        return True
    return False


def _err(message, status=503, etype="api_error"):
    return JSONResponse({"type": "error", "error": {"type": etype, "message": message}},
                        status_code=status)


def _targets(s):
    """Build the ordered list of upstream targets for ONE request.

    Each request starts fresh from the PRIMARY (no stickiness): primary is always
    tried first, then the rest in order. If a target returns an upstream error
    (content-blocked / 4xx after a proxy connected) we fall through to the NEXT
    target. Order:
        1. the primary  (custom endpoint flagged is_primary, OR agentrouter if none)
        2. AgentRouter  (the gateway's built-in upstream) unless disabled / already primary
        3. remaining enabled custom endpoints, by priority

    A target = {name, base, key, mode, agentrouter}:
        base       = upstream base URL (path is appended later)
        key        = api key to send (custom key, else global upstream_key)
        mode       = 'anthropic' | 'openai'
        agentrouter= True only for the built-in agentrouter target (claude-cli
                     fingerprint headers apply only to it)
    """
    ar_base = s.get("endpoint", "https://agentrouter.org").rstrip("/")
    ar_key = s.get("upstream_key") or s.get("gateway_key", "")
    ar_enabled = s.get("agentrouter_enabled", "1") != "0"
    ar_target = {"name": "AgentRouter", "base": ar_base, "key": ar_key,
                 "mode": "anthropic", "agentrouter": True, "model_override": s.get("global_model_override", "")}

    customs = [e for e in db.list_endpoints() if e.get("enabled")]
    def mk(e):
        return {
            "name": e.get("name") or e["url"].replace("https://", "").replace("http://", ""),
            "base": e["url"].rstrip("/"),
            "key": e.get("api_key") or ar_key,
            "mode": "openai" if e.get("api_mode") == "chat_completions" else "anthropic",
            "agentrouter": False,
            "id": e.get("id"),
            "model_override": e.get("model_override") or s.get("global_model_override", ""),
        }
    primary_custom = next((e for e in customs if e.get("is_primary")), None)

    out = []
    if primary_custom:
        out.append(mk(primary_custom))                       # 1. custom primary
        if ar_enabled:
            out.append(ar_target)                            # 2. agentrouter
    elif ar_enabled:
        out.append(ar_target)                                # 1. agentrouter primary
    # 3. remaining enabled customs (skip the primary already added)
    for e in customs:
        if primary_custom and e.get("id") == primary_custom.get("id"):
            continue
        out.append(mk(e))
    # safety: never return empty (fall back to agentrouter even if disabled)
    return out or [ar_target]


def _target_url(base, path, mode):
    """Compose the full upstream URL for a target given the inbound path."""
    base = base.rstrip("/")
    if mode == "openai":
        # OpenAI providers expose /chat/completions; map any messages path to it
        if base.endswith("/chat/completions"):
            return base
        if "/v1" in base:
            return base.rstrip("/") + "/chat/completions"
        return base + "/v1/chat/completions"
    # anthropic: agentrouter & compatibles expose /v1/messages
    if base.endswith("/v1/messages"):
        return base
    return f"{base}/{path}"


def _target_headers(request, target):
    """Headers for a specific target (claude-cli fingerprint only for agentrouter)."""
    kind = "openai" if target["mode"] == "openai" else "anthropic"
    headers = _build_upstream_headers(request, target["key"], kind)
    return headers



def _mutate_body(body_bytes, want_stream=None, model_override=None):
    try:
        d = json.loads(body_bytes)
    except Exception:
        return body_bytes
    if want_stream is not None:
        d["stream"] = want_stream
    if model_override:
        d["model"] = model_override
    return json.dumps(d, ensure_ascii=False).encode("utf-8")


def _strip_thinking(body_bytes):
    """Remove `thinking` / `redacted_thinking` content blocks from the message history.

    Why: real Claude Code replays its previous turns *including* the assistant's
    {"type":"thinking","thinking":"...","signature":"..."} blocks. agentrouter's
    older /v1/messages schema does not accept inbound thinking blocks and rejects
    the whole request with 400 "thinking: Field required" (its validator mismatches
    on the block shape). We are NOT the model — we never need to send these back
    upstream — so we drop them before forwarding. The current turn's generation is
    untouched; only the echoed history is cleaned.

    Returns (body_bytes, n_removed). Non-JSON / unexpected shapes pass through.
    """
    try:
        d = json.loads(body_bytes)
    except Exception:
        return body_bytes, 0
    msgs = d.get("messages")
    if not isinstance(msgs, list):
        return body_bytes, 0
    removed = 0
    changed = False
    for m in msgs:
        c = m.get("content")
        if not isinstance(c, list):
            continue
        kept = []
        for b in c:
            if isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking"):
                removed += 1
                changed = True
                continue
            kept.append(b)
        if not kept:
            # an assistant turn that was ONLY thinking — keep it valid with empty text
            kept = [{"type": "text", "text": ""}]
        m["content"] = kept
    if not changed:
        return body_bytes, 0
    return json.dumps(d, ensure_ascii=False).encode("utf-8"), removed


def _parse_block(block):
    """Parse one raw SSE block (bytes) -> (event:str|None, data:str|None)."""
    event = None
    data = None
    for line in block.split(b"\n"):
        line = line.strip()
        if line.startswith(b"event:"):
            event = line[6:].strip().decode("utf-8", "replace")
        elif line.startswith(b"data:"):
            data = line[5:].strip().decode("utf-8", "replace")
    return event, data


# ---------- assemblers: SSE stream -> single final object ----------
class AnthropicAssembler:
    """Rebuild a /v1/messages Message object from its SSE event stream."""
    def __init__(self):
        self.msg = None
        self._pj = {}   # index -> accumulated tool_use input_json string

    def _ensure(self, i):
        c = self.msg.setdefault("content", [])
        while len(c) <= i:
            c.append({})

    def feed(self, event, data):
        if not data:
            return
        try:
            obj = json.loads(data)
        except Exception:
            return
        t = obj.get("type")
        if t == "message_start":
            self.msg = obj.get("message", {"type": "message", "content": []})
            self.msg.setdefault("content", [])
        elif self.msg is None:
            return
        elif t == "content_block_start":
            i = obj["index"]; self._ensure(i)
            self.msg["content"][i] = obj.get("content_block", {})
            if self.msg["content"][i].get("type") == "tool_use":
                self._pj[i] = ""
        elif t == "content_block_delta":
            i = obj["index"]; self._ensure(i)
            b = self.msg["content"][i]; d = obj.get("delta", {})
            dt = d.get("type")
            if dt == "text_delta":
                b["text"] = b.get("text", "") + d.get("text", "")
            elif dt == "thinking_delta":
                b["thinking"] = b.get("thinking", "") + d.get("thinking", "")
            elif dt == "signature_delta":
                b["signature"] = b.get("signature", "") + d.get("signature", "")
            elif dt == "input_json_delta":
                self._pj[i] = self._pj.get(i, "") + d.get("partial_json", "")
        elif t == "content_block_stop":
            i = obj["index"]
            if i in self._pj:
                try:
                    self.msg["content"][i]["input"] = json.loads(self._pj[i] or "{}")
                except Exception:
                    self.msg["content"][i]["input"] = {}
        elif t == "message_delta":
            self.msg.update(obj.get("delta", {}))
            u = obj.get("usage")
            if u:
                self.msg.setdefault("usage", {}).update(u)
        # message_stop / ping -> nothing

    def result(self):
        return self.msg


def _sse(event, data):
    return (f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n").encode("utf-8")


# a keepalive ping frame (the Anthropic SDK explicitly ignores `ping` events)
PING = _sse("ping", {"type": "ping"})


def _message_to_sse(msg):
    """Reconstruct a complete, spec-valid Anthropic SSE stream from a fully
    assembled Message object. Used by the streaming path after it has buffered
    and validated the whole upstream response, so the client receives one clean
    stream (with a guaranteed message_stop) regardless of upstream flakiness."""
    import copy
    content = msg.get("content", []) or []
    start_msg = copy.deepcopy(msg)
    start_msg["content"] = []
    frames = [_sse("message_start", {"type": "message_start", "message": start_msg})]
    for i, block in enumerate(content):
        bt = block.get("type")
        if bt == "text":
            frames.append(_sse("content_block_start", {"type": "content_block_start", "index": i,
                          "content_block": {"type": "text", "text": ""}}))
            if block.get("text"):
                frames.append(_sse("content_block_delta", {"type": "content_block_delta", "index": i,
                              "delta": {"type": "text_delta", "text": block["text"]}}))
        elif bt == "tool_use":
            frames.append(_sse("content_block_start", {"type": "content_block_start", "index": i,
                          "content_block": {"type": "tool_use", "id": block.get("id"),
                                            "name": block.get("name"), "input": {}}}))
            frames.append(_sse("content_block_delta", {"type": "content_block_delta", "index": i,
                          "delta": {"type": "input_json_delta",
                                    "partial_json": json.dumps(block.get("input", {}), ensure_ascii=False)}}))
        elif bt == "thinking":
            frames.append(_sse("content_block_start", {"type": "content_block_start", "index": i,
                          "content_block": {"type": "thinking", "thinking": ""}}))
            if block.get("thinking"):
                frames.append(_sse("content_block_delta", {"type": "content_block_delta", "index": i,
                              "delta": {"type": "thinking_delta", "thinking": block["thinking"]}}))
            if block.get("signature"):
                frames.append(_sse("content_block_delta", {"type": "content_block_delta", "index": i,
                              "delta": {"type": "signature_delta", "signature": block["signature"]}}))
        else:
            frames.append(_sse("content_block_start", {"type": "content_block_start", "index": i,
                          "content_block": block}))
        frames.append(_sse("content_block_stop", {"type": "content_block_stop", "index": i}))
    frames.append(_sse("message_delta", {"type": "message_delta",
                  "delta": {"stop_reason": msg.get("stop_reason"),
                            "stop_sequence": msg.get("stop_sequence")},
                  "usage": msg.get("usage", {})}))
    frames.append(_sse("message_stop", {"type": "message_stop"}))
    return frames


class OpenAIAssembler:
    """Rebuild a /v1/chat/completions object from its SSE stream (best-effort)."""
    def __init__(self):
        self.obj = None
        self.content = ""
        self.role = "assistant"
        self.finish = None
        self.usage = None

    def feed(self, event, data):
        if not data or data == "[DONE]":
            return
        try:
            o = json.loads(data)
        except Exception:
            return
        if self.obj is None:
            self.obj = {"id": o.get("id"), "object": "chat.completion",
                        "created": o.get("created"), "model": o.get("model"),
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": ""},
                                     "finish_reason": None}]}
        for ch in o.get("choices", []):
            d = ch.get("delta", {})
            if d.get("role"):
                self.role = d["role"]
            if d.get("content"):
                self.content += d["content"]
            if ch.get("finish_reason"):
                self.finish = ch["finish_reason"]
        if o.get("usage"):
            self.usage = o["usage"]

    def result(self):
        if self.obj is None:
            return None
        self.obj["choices"][0]["message"] = {"role": self.role, "content": self.content}
        self.obj["choices"][0]["finish_reason"] = self.finish or "stop"
        if self.usage:
            self.obj["usage"] = self.usage
        return self.obj


async def _consume_assemble(candidates, url, headers, body, timeout, kind):
    """Open an upstream STREAM through `proxy`, assemble the final object.

    Returns ("respond", status, content_type, body_bytes) or ("retry", reason).
    """
    try:
        async with _build_client(candidates, timeout) as client:
            async with client.stream("POST", url, headers=headers, content=body) as r:
                ct = r.headers.get("content-type", "")
                if _is_waf(r.headers):
                    proxy_pool.mark_bad(candidates[0]["id"], "waf")
                    return ("retry", "waf")
                if r.status_code >= 500:
                    proxy_pool.mark_bad(candidates[0]["id"], "5xx")
                    return ("retry", str(r.status_code))
                proxy_pool.mark_good(candidates[0]["id"])
                # If upstream didn't actually stream (4xx error body, or plain JSON),
                # pass it straight through.
                if "text/event-stream" not in ct:
                    raw = await r.aread()
                    out_ct = "application/json" if raw[:1] in (b"{", b"[") else (ct or "application/json")
                    return ("respond", r.status_code, out_ct, raw)
                asm = OpenAIAssembler() if kind == "openai" else AnthropicAssembler()
                buf = b""
                async for chunk in r.aiter_bytes():
                    buf += chunk
                    while b"\n\n" in buf:
                        block, buf = buf.split(b"\n\n", 1)
                        ev, data = _parse_block(block)
                        asm.feed(ev, data)
                if buf.strip():
                    ev, data = _parse_block(buf)
                    asm.feed(ev, data)
                obj = asm.result()
                if obj is None:
                    return ("retry", "empty-stream")
                # Completeness guard: AnthropicAssembler only fills stop_reason/usage on the
                # final message_delta event. If the upstream stream was cut mid-generation we'd
                # otherwise return a structurally-incomplete 200 (no stop_reason, empty content,
                # or a tool_use block with no input) that a strict client rejects. Fail over to
                # the next proxy instead of returning a malformed success.
                if kind == "anthropic":
                    incomplete = obj.get("stop_reason") is None or not obj.get("content")
                    if not incomplete:
                        for b in obj.get("content", []):
                            if b.get("type") == "tool_use" and "input" not in b:
                                incomplete = True
                                break
                    if incomplete:
                        proxy_pool.mark_bad(candidates[0]["id"], "truncated")
                        return ("retry", "truncated")
                return ("respond", 200, "application/json",
                        json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        proxy_pool.mark_bad(candidates[0]["id"], "conn")
        return ("retry", f"{type(e).__name__}")


async def test_endpoint(url, api_mode, api_key, model, message):
    """Send ONE small chat through the pinned/first proxy to a specific endpoint.
    Returns {ok, status, ms, reply, detail}. Token-cheap (max_tokens=20)."""
    candidates = proxy_pool.ordered_for_request(2)
    if not candidates:
        return {"ok": False, "status": 0, "ms": 0, "reply": "", "detail": "no proxies configured"}
    openai = (api_mode == "chat_completions")
    if openai:
        full = url.rstrip("/") + ("/chat/completions" if not url.rstrip("/").endswith("chat/completions") else "")
        body = {"model": model, "max_tokens": 20,
                "messages": [{"role": "user", "content": message}]}
        headers = {"authorization": f"Bearer {api_key}", "content-type": "application/json"}
    else:
        full = url.rstrip("/") + ("/v1/messages" if "/v1/messages" not in url else "")
        body = {"model": model, "max_tokens": 20,
                "messages": [{"role": "user", "content": message}]}
        headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
        if db.get_setting("claude_mimicry", "1") == "1":
            headers["user-agent"] = "claude-cli/2.1.177 (external, cli)"
            headers["x-stainless-lang"] = "node"
            headers["x-stainless-package-version"] = "0.33.1"
            headers["x-stainless-os"] = "linux"
            headers["x-stainless-arch"] = "x64"
            headers["x-stainless-runtime"] = "node"
            headers["x-stainless-runtime-version"] = "v22.13.0"
            headers["anthropic-beta"] = "claude-code-20250219,fine-grained-tool-streaming-2025-05-14,prompt-caching-2025-07-21,context-1m-2025-08-07"
        else:
            headers["user-agent"] = "claude-cli/2.1.177 (external, cli)"
    payload = json.dumps(body).encode()
    t0 = time.time()
    detail = ""
    ep_name = url.replace("https://", "").replace("http://", "")
    for p in candidates:
        try:
            async with _build_client([p], httpx.Timeout(45.0)) as client:
                r = await client.post(full, headers=headers, content=payload)
                ms = int((time.time() - t0) * 1000)
                raw = r.text[:1500]
                if r.status_code < 300:
                    reply = ""
                    try:
                        d = r.json()
                        if openai:
                            reply = d["choices"][0]["message"]["content"]
                        else:
                            reply = "".join(b.get("text", "") for b in d.get("content", []))
                    except Exception:
                        reply = raw[:200]
                    db.add_log(method="POST", path="endpoint/test", status=r.status_code,
                               proxy=p["url"], attempts=1, stream=0, redactions=0, ms=ms,
                               note="test ok", model=model, endpoint=ep_name, source="test")
                    return {"ok": True, "status": r.status_code, "ms": ms,
                            "reply": reply.strip() or "(empty)", "detail": ""}
                detail = f"HTTP {r.status_code}: {raw}"
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
    ms = int((time.time() - t0) * 1000)
    db.add_log(method="POST", path="endpoint/test", status=0, proxy="", attempts=len(candidates),
               stream=0, redactions=0, ms=ms, note="test failed", model=model,
               endpoint=ep_name, source="test", detail=detail[:1500])
    return {"ok": False, "status": 0, "ms": ms, "reply": "", "detail": detail}


async def forward(request, path):
    s = db.get_all_settings()
    endpoint = s.get("endpoint", "https://agentrouter.org").rstrip("/")
    gateway_key = s.get("gateway_key", "")
    upstream_key = s.get("upstream_key") or gateway_key
    require_key = s.get("require_client_key", "1") == "1"
    max_retries = int(s.get("max_retries", "4") or 4)
    conn_to = float(s.get("connect_timeout", "10") or 10)
    timeout = httpx.Timeout(connect=conn_to,
                            read=float(s.get("read_timeout", "900") or 900),
                            write=120.0, pool=20.0)

    # client IP (behind Caddy/Railway -> X-Forwarded-For) for the log detail view
    client_ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                 or (request.client.host if request.client else ""))

    body = await request.body()
    req_text = body.decode("utf-8", "replace")[:3000]
    def _log(**kw):
        kw.setdefault("req_body", req_text)
        db.add_log(**kw)

    # Claude CLI issues an unauthenticated handshake/info call to v1/props on
    # startup (no key header). Exempt such no-key info paths so they don't spam
    # the log with harmless 401s — they carry no chat payload.
    _NOKEY_PATHS = {"v1/props"}
    if require_key and gateway_key and path not in _NOKEY_PATHS:
        ck = request.headers.get("x-api-key", "").strip()
        auth = request.headers.get("authorization", "").strip()
        if not ck and auth.lower().startswith("bearer "):
            ck = auth[7:].strip()
        # accept the global gateway key OR any enabled custom key (mugen_*)
        ok_key = (ck == gateway_key)
        if not ok_key and ck:
            krow = db.get_api_key_by_value(ck)
            if krow:
                ok_key = True
                try:
                    db.touch_api_key(krow["id"])
                except Exception:
                    pass
        if not ok_key:
            masked_ck = f"{ck[:4]}...{ck[-4:]}" if len(ck) > 8 else (ck or "[empty]")
            _log(method=request.method, path=path, status=401, proxy="", attempts=0,
                       stream=0, redactions=0, ms=0, note="invalid gateway key",
                       ip=client_ip, model="", detail=f"Expected gateway key, but received invalid key: '{masked_ck}' (length: {len(ck)})")
            return _err("Invalid gateway key.", 401, "authentication_error")

    # model name (for logs) — parsed before filters mutate the body
    try:
        req_model = (json.loads(body) or {}).get("model", "")
    except Exception:
        req_model = ""
    body, blocked_kw, redactions = filters.apply_filters(body)
    if blocked_kw is not None:
        db.add_log(method=request.method, path=path, status=400, proxy="", attempts=0,
                   stream=0, redactions=0, ms=0, note="blocked by content filter",
                   ip=client_ip, model=req_model)
        return _err("Request blocked by content filter.", 400, "invalid_request_error")

    # Strip echoed thinking blocks from history — agentrouter's schema rejects them
    # with 400 "thinking: Field required". Only relevant for /v1/messages bodies.
    if "chat/completions" not in path:
        body, _thk = _strip_thinking(body)

    try:
        client_wants_stream = bool(json.loads(body).get("stream"))
    except Exception:
        client_wants_stream = "text/event-stream" in request.headers.get("accept", "")

    kind = "openai" if "chat/completions" in path else "anthropic"
    targets = _targets(s)
    base_candidates = proxy_pool.ordered_for_request(max_retries)
    if not base_candidates:
        db.add_log(method=request.method, path=path, status=503, proxy="", attempts=0,
                   stream=0, redactions=0, ms=0, note="no usable proxies",
                   ip=client_ip, model=req_model)
        return _err("No usable proxies configured.", 503)
    t0 = time.time()

    # ---------- client wants STREAM ----------
    # Reliability strategy (Anthropic): BUFFER + assemble the upstream stream
    # server-side while sending only keepalive `ping` frames to the client, then
    # emit ONE clean reconstructed SSE with a guaranteed message_stop. If the
    # upstream stalls or cuts the stream incomplete (agentrouter does this on slow
    # generations — it closes without message_stop), retry on the next proxy
    # transparently: the client has only seen pings, so a retry is invisible.
    #
    # ENDPOINT FAILOVER: every request starts fresh from the PRIMARY target. If a
    # target returns an upstream rejection (content-blocked / 4xx) OR all its
    # proxies fail, we fall through to the NEXT target (agentrouter -> custom2 ->
    # custom3 ...). The client keeps seeing pings, so the switch is invisible.
    if client_wants_stream:

        if kind == "anthropic":
            async def gen():
                attempts = 0
                detail = ""
                last_err = None  # (status, err, target_name, proxy_url) from an upstream 4xx
                logged = False   # ensure we ALWAYS write a log, even if client disconnects
                last_proxy = ""
                last_tgt_name = ""
                try:
                    for tgt in targets:
                        up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
                        url = _target_url(tgt["base"], path, tgt["mode"])
                        theaders = _target_headers(request, tgt)
                        attempted_pids = set()
                        upstream_rejected = False
                        consecutive_5xx = 0
                        for _ in range(max_retries):
                            concurrency = int(db.get_setting("hedging_concurrency", "3"))
                            candidates = [p for p in proxy_pool.ordered_for_request(max_retries * concurrency) if p["id"] not in attempted_pids][:concurrency]
                            if not candidates:
                                break
                            p = candidates[0]
                            attempted_pids.update(c["id"] for c in candidates)
                            attempts += 1
                            last_proxy = p["url"]
                            last_tgt_name = tgt["name"]

                            async def consume(p=p, url=url, theaders=theaders):
                                try:
                                    async with _build_client(candidates, timeout) as client:
                                        async with client.stream("POST", url, headers=theaders, content=up_body) as r:
                                            if _is_waf(r.headers):
                                                proxy_pool.mark_bad(p["id"], "waf")
                                                return ("retry", f"WAF via {p['url']}")
                                            if r.status_code >= 500:
                                                proxy_pool.mark_bad(p["id"], "5xx")
                                                return ("retry", f"{r.status_code} via {p['url']}")
                                            ct = r.headers.get("content-type", "")
                                            if "text/event-stream" not in ct:
                                                raw = await r.aread()
                                                if r.status_code >= 400:
                                                    # genuine upstream rejection — switch to next TARGET
                                                    proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                                                    try:
                                                        err = json.loads(raw)
                                                        if not isinstance(err, dict) or "error" not in err:
                                                            raise ValueError
                                                    except Exception:
                                                        err = {"type": "error", "error": {"type": "api_error",
                                                               "message": f"Upstream HTTP {r.status_code}: "
                                                                          f"{raw[:300].decode('utf-8', 'replace')}"}}
                                                    return ("error", r.status_code, err)
                                                proxy_pool.mark_bad(p["id"], "non-sse")
                                                return ("retry", f"non-SSE {r.status_code} via {p['url']}")
                                            asm = AnthropicAssembler()
                                            saw_stop = False
                                            buf = b""
                                            async for chunk in r.aiter_bytes():
                                                buf += chunk
                                                while b"\n\n" in buf:
                                                    block, buf = buf.split(b"\n\n", 1)
                                                    ev, data = _parse_block(block)
                                                    asm.feed(ev, data)
                                                    if ev == "message_stop":
                                                        saw_stop = True
                                            if buf.strip():
                                                ev, data = _parse_block(buf); asm.feed(ev, data)
                                                if ev == "message_stop":
                                                    saw_stop = True
                                            obj = asm.result()
                                            complete = bool(saw_stop and obj and obj.get("stop_reason") and obj.get("content"))
                                            if complete:
                                                for b in obj.get("content", []):
                                                    if b.get("type") == "tool_use" and "input" not in b:
                                                        complete = False; break
                                            if not complete:
                                                proxy_pool.mark_bad(p["id"], "truncated")
                                                return ("retry", f"incomplete via {p['url']}")
                                            proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                                            return ("ok", obj)
                                except Exception as e:
                                    proxy_pool.mark_bad(p["id"], "conn")
                                    return ("retry", f"{type(e).__name__} via {p['url']}")

                            task = asyncio.ensure_future(consume())
                            # keepalive: ping the client ~every 10s while buffering upstream
                            while not task.done():
                                done, _pending = await asyncio.wait({task}, timeout=10.0)
                                if not done:
                                    yield PING
                            res = task.result()
                            if res[0] == "ok":
                                for frame in _message_to_sse(res[1]):
                                    yield frame
                                _log(method=request.method, path=path, status=200, proxy=p["url"],
                                           attempts=attempts, stream=1, redactions=redactions,
                                           ms=int((time.time() - t0) * 1000), note="ok(buffered)",
                                           ip=client_ip, model=req_model, endpoint=tgt["name"])
                                logged = True
                                return
                            if res[0] == "error":
                                _, status, err = res
                                last_err = (status, err, tgt["name"], p["url"])
                                upstream_rejected = True
                                break  # this target rejected the content — try NEXT target
                            detail = res[1]  # ("retry", reason) -> next proxy, same target
                            
                            if any(x in detail for x in ["500 via", "501 via", "502 via", "503 via", "504 via"]):
                                consecutive_5xx += 1
                            else:
                                consecutive_5xx = 0
                                
                            _log(method=request.method, path=path, status=502, proxy=p["url"],
                                       attempts=attempts, stream=1, redactions=redactions,
                                       ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}",
                                       ip=client_ip, model=req_model, endpoint=tgt["name"])
                                       
                            if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                last_err = (503, {"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} returned 5xx consecutively. Assuming endpoint down."}}, tgt["name"], p["url"])
                                upstream_rejected = True
                                break

                            if "WAF" in detail or "Error" in detail or "Timeout" in detail or "non-SSE" in detail:
                                pass
                            else:
                                await _retry_backoff(attempts)
                        # finished this target (upstream-rejected or all proxies failed) -> next target
                        if not upstream_rejected:
                            detail = detail or f"all proxies failed for {tgt['name']}"
                    # all targets exhausted
                    if last_err:
                        status, err, tname, purl = last_err
                        _log(method=request.method, path=path, status=status, proxy=purl,
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note=f"all targets rejected (last: {tname} {status})",
                                   ip=client_ip, model=req_model, endpoint=tname,
                                   detail=json.dumps(err)[:1500])
                        logged = True
                        yield _sse("error", err)
                        return
                    _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                               stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                               note=f"all targets failed: {detail}",
                               ip=client_ip, model=req_model, endpoint="", detail=str(detail)[:1500])
                    logged = True
                    yield _sse("error", {"type": "error", "error": {"type": "api_error",
                              "message": f"All proxies failed. {detail}"}})
                finally:
                    # Always log, even if client disconnected mid-retry
                    if not logged:
                        _log(method=request.method, path=path, status=499, proxy=last_proxy,
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000),
                                   note=f"client disconnected: {detail or 'mid-retry'}",
                                   ip=client_ip, model=req_model, endpoint=last_tgt_name)

            return StreamingResponse(gen(), media_type="text/event-stream")

        # ---- OpenAI streaming: raw relay across targets/proxies ----
        async def gen_openai():
            attempts = 0
            forwarded = False
            detail = ""
            for tgt in targets:
                if client_wants_stream:
                    up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
                else:
                    up_body = _mutate_body(body, want_stream=False, model_override=tgt.get("model_override"))
                url = _target_url(tgt["base"], path, tgt["mode"])
                theaders = _target_headers(request, tgt)
                attempted_pids = set()
                consecutive_5xx = 0
                for _ in range(max_retries):
                    concurrency = int(db.get_setting("hedging_concurrency", "3"))
                    candidates = [p for p in proxy_pool.ordered_for_request(max_retries * concurrency) if p["id"] not in attempted_pids][:concurrency]
                    if not candidates:
                        break
                    p = candidates[0]
                    attempted_pids.update(c["id"] for c in candidates)
                    attempts += 1
                    client = _build_client(candidates, timeout)
                    try:
                        req = client.build_request("POST", url, headers=theaders, content=up_body)
                        r = await client.send(req, stream=True)
                        if _is_waf(r.headers) or r.status_code >= 500:
                            await r.aclose(); await client.aclose()
                            proxy_pool.mark_bad(p["id"], "waf/5xx"); detail = f"{r.status_code} via {p['url']}"
                            _log(method=request.method, path=path, status=502, proxy=p["url"], attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}", ip=client_ip, model=req_model, endpoint=tgt["name"])
                            if _is_waf(r.headers): pass
                            else:
                                consecutive_5xx += 1
                                if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                    _log(method=request.method, path=path, status=503, proxy=p["url"], attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"smart failover: {tgt['name']} returned 5xx 3 times in a row", ip=client_ip, model=req_model, endpoint=tgt["name"])
                                    detail = f"{tgt['name']} returned 5xx consecutively"
                                    break
                                await _retry_backoff(attempts)
                            continue
                        consecutive_5xx = 0
                        if r.status_code >= 400:
                            # upstream rejection -> next target
                            await r.aclose(); await client.aclose()
                            detail = f"{tgt['name']} {r.status_code}"
                            break
                        proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                        async for chunk in r.aiter_raw():
                            forwarded = True
                            yield chunk
                        await r.aclose(); await client.aclose()
                        _log(method=request.method, path=path, status=200, proxy=p["url"],
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note="ok(relay)",
                                   ip=client_ip, model=req_model, endpoint=tgt["name"])
                        return
                    except Exception as e:
                        try: await client.aclose()
                        except Exception: pass
                        detail = f"{type(e).__name__} via {p['url']}"
                        if forwarded:
                            return
                        proxy_pool.mark_bad(p["id"], "conn"); continue
            yield ("event: error\ndata: " + json.dumps(
                {"type": "error", "error": {"type": "api_error", "message": f"All proxies failed. {detail}"}}
            ) + "\n\n").encode()

        return StreamingResponse(gen_openai(), media_type="text/event-stream")

    # ---------- client wants NON-STREAM: stream upstream, assemble, return JSON ----------
    attempts = 0
    detail = ""
    last = None  # (status, ct, payload, target_name, proxy_url) from an upstream 4xx
    for tgt in targets:
        if client_wants_stream:
            up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
        else:
            up_body = _mutate_body(body, want_stream=False, model_override=tgt.get("model_override"))
        url = _target_url(tgt["base"], path, tgt["mode"])
        theaders = _target_headers(request, tgt)
        tgt_kind = "openai" if tgt["mode"] == "openai" else "anthropic"
        upstream_rejected = False
        attempted_pids = set()
        consecutive_5xx = 0
        for _ in range(max_retries):
            concurrency = int(db.get_setting("hedging_concurrency", "3"))
            candidates = [p for p in proxy_pool.ordered_for_request(max_retries * concurrency) if p["id"] not in attempted_pids][:concurrency]
            if not candidates:
                break
            p = candidates[0]
            attempted_pids.update(c["id"] for c in candidates)
            attempts += 1
            res = await _consume_assemble(candidates, url, theaders, up_body, timeout, tgt_kind)
            if res[0] == "respond":
                _, status, ct, payload = res
                if 200 <= status < 300:
                    _log(method=request.method, path=path, status=status, proxy=p["url"],
                               attempts=attempts, stream=0, redactions=redactions,
                               ms=int((time.time() - t0) * 1000), note="ok(assembled)",
                               ip=client_ip, model=req_model, endpoint=tgt["name"])
                    return Response(content=payload, status_code=status, media_type=ct)
                # upstream rejection (4xx) -> remember, switch to next TARGET
                last = (status, ct, payload, tgt["name"], p["url"])
                upstream_rejected = True
                break
            detail = res[1]
            
            if any(x in detail for x in ["500 via", "501 via", "502 via", "503 via", "504 via"]):
                consecutive_5xx += 1
            else:
                consecutive_5xx = 0
                
            _log(method=request.method, path=path, status=502, proxy=p["url"],
                       attempts=attempts, stream=0, redactions=redactions,
                       ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}",
                       ip=client_ip, model=req_model, endpoint=tgt["name"])
                       
            if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                payload = json.dumps({"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} returned 5xx consecutively. Assuming endpoint down."}}).encode()
                last = (503, "application/json", payload, tgt["name"], p["url"])
                upstream_rejected = True
                break

            if "WAF" in detail or "Error" in detail or "Timeout" in detail or "non-SSE" in detail:
                pass
            else:
                await _retry_backoff(attempts)
        if not upstream_rejected:
            detail = detail or f"all proxies failed for {tgt['name']}"
    # all targets exhausted
    if last:
        status, ct, payload, tname, purl = last
        _log(method=request.method, path=path, status=status, proxy=purl,
                   attempts=attempts, stream=0, redactions=redactions,
                   ms=int((time.time() - t0) * 1000), note=f"all targets rejected (last: {tname} {status})",
                   ip=client_ip, model=req_model, endpoint=tname,
                   detail=payload[:1500].decode("utf-8", "replace"))
        return Response(content=payload, status_code=status, media_type=ct)
    _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
               stream=0, redactions=redactions, ms=int((time.time() - t0) * 1000),
               note=f"all targets failed: {detail}",
               ip=client_ip, model=req_model, endpoint="", detail=str(detail)[:1500])
    return _err(f"All proxies failed. {detail}", 503)
