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
import httpx
from starlette.responses import StreamingResponse, Response, JSONResponse
from . import db, proxy_pool, filters

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
    headers["user-agent"] = db.get_setting("user_agent", "claude-cli/1.0.60 (external, cli)")
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


def _with_stream(body_bytes, want):
    """Return body bytes with the JSON "stream" flag forced to `want`."""
    try:
        d = json.loads(body_bytes)
    except Exception:
        return body_bytes
    d["stream"] = want
    return json.dumps(d, ensure_ascii=False).encode("utf-8")


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


async def _consume_assemble(proxy, url, headers, body, timeout, kind):
    """Open an upstream STREAM through `proxy`, assemble the final object.

    Returns ("respond", status, content_type, body_bytes) or ("retry", reason).
    """
    try:
        async with httpx.AsyncClient(proxy=proxy["url"], timeout=timeout) as client:
            async with client.stream("POST", url, headers=headers, content=body) as r:
                ct = r.headers.get("content-type", "")
                if _is_waf(r.headers):
                    proxy_pool.mark_bad(proxy["id"], "waf")
                    return ("retry", "waf")
                if r.status_code >= 500:
                    proxy_pool.mark_bad(proxy["id"], "5xx")
                    return ("retry", str(r.status_code))
                proxy_pool.mark_good(proxy["id"])
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
                        proxy_pool.mark_bad(proxy["id"], "truncated")
                        return ("retry", "truncated")
                return ("respond", 200, "application/json",
                        json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception as e:
        proxy_pool.mark_bad(proxy["id"], "conn")
        return ("retry", f"{type(e).__name__}")


async def forward(request, path):
    s = db.get_all_settings()
    endpoint = s.get("endpoint", "https://agentrouter.org").rstrip("/")
    gateway_key = s.get("gateway_key", "")
    upstream_key = s.get("upstream_key") or gateway_key
    require_key = s.get("require_client_key", "1") == "1"
    max_retries = int(s.get("max_retries", "4") or 4)
    timeout = httpx.Timeout(connect=float(s.get("connect_timeout", "20") or 20),
                            read=float(s.get("read_timeout", "900") or 900),
                            write=120.0, pool=20.0)

    if require_key and gateway_key:
        ck = request.headers.get("x-api-key", "")
        auth = request.headers.get("authorization", "")
        if not ck and auth.lower().startswith("bearer "):
            ck = auth[7:]
        if ck != gateway_key:
            return _err("Invalid gateway key.", 401, "authentication_error")

    body = await request.body()
    body, blocked_kw, redactions = filters.apply_filters(body)
    if blocked_kw is not None:
        db.add_log(method=request.method, path=path, status=400, proxy="", attempts=0,
                   stream=0, redactions=0, ms=0, note="blocked by content filter")
        return _err("Request blocked by content filter.", 400, "invalid_request_error")

    try:
        client_wants_stream = bool(json.loads(body).get("stream"))
    except Exception:
        client_wants_stream = "text/event-stream" in request.headers.get("accept", "")

    kind = "openai" if "chat/completions" in path else "anthropic"
    headers = _build_upstream_headers(request, upstream_key, kind)
    url = f"{endpoint}/{path}"
    candidates = proxy_pool.ordered_for_request(max_retries)
    if not candidates:
        return _err("No usable proxies configured.", 503)
    t0 = time.time()

    # ---------- client wants STREAM: relay SSE through (pings keep it alive) ----------
    if client_wants_stream:
        up_body = _with_stream(body, True)

        async def gen():
            attempts = 0
            forwarded = False
            detail = ""
            for p in candidates:
                attempts += 1
                client = httpx.AsyncClient(proxy=p["url"], timeout=timeout)
                try:
                    req = client.build_request("POST", url, headers=headers, content=up_body)
                    r = await client.send(req, stream=True)
                    if _is_waf(r.headers):
                        await r.aclose(); await client.aclose()
                        proxy_pool.mark_bad(p["id"], "waf"); detail = f"WAF via {p['url']}"; continue
                    if r.status_code >= 500:
                        await r.aclose(); await client.aclose()
                        proxy_pool.mark_bad(p["id"], "5xx"); detail = f"{r.status_code} via {p['url']}"; continue
                    # The upstream leg MUST actually be an SSE stream. If it isn't — a 4xx JSON
                    # error, a WAF page that slipped the header sniff, or any plain body — do NOT
                    # relay it verbatim under a hardcoded 200 text/event-stream: the client SDK
                    # would then see ZERO message events and raise a bare AssertionError with an
                    # empty message (the exact bug we are fixing).
                    ct = r.headers.get("content-type", "")
                    if "text/event-stream" not in ct:
                        raw = await r.aread()
                        await r.aclose(); await client.aclose()
                        if r.status_code >= 400:
                            # Genuine upstream rejection (e.g. 400 prompt-too-long, 401). The proxy
                            # itself worked; failing over won't help. Surface it as a proper SSE
                            # `error` event so the client raises a diagnosable APIStatusError.
                            proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                            try:
                                err = json.loads(raw)
                                if not isinstance(err, dict) or "error" not in err:
                                    raise ValueError
                            except Exception:
                                err = {"type": "error", "error": {"type": "api_error",
                                       "message": f"Upstream HTTP {r.status_code}: "
                                                  f"{raw[:300].decode('utf-8', 'replace')}"}}
                            db.add_log(method=request.method, path=path, status=r.status_code, proxy=p["url"],
                                       attempts=attempts, stream=1, redactions=redactions,
                                       ms=int((time.time() - t0) * 1000), note=f"upstream non-SSE {r.status_code}")
                            yield ("event: error\ndata: " + json.dumps(err) + "\n\n").encode()
                            return
                        # 2xx but not an event stream — almost certainly an interception/WAF page.
                        # Treat the proxy as bad and fail over to the next candidate.
                        proxy_pool.mark_bad(p["id"], "non-sse")
                        detail = f"non-SSE {r.status_code} via {p['url']}"; continue
                    proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                    async for chunk in r.aiter_raw():
                        forwarded = True
                        yield chunk
                    await r.aclose(); await client.aclose()
                    db.add_log(method=request.method, path=path, status=r.status_code, proxy=p["url"],
                               attempts=attempts, stream=1, redactions=redactions,
                               ms=int((time.time() - t0) * 1000), note="ok")
                    return
                except Exception as e:
                    try: await client.aclose()
                    except Exception: pass
                    detail = f"{type(e).__name__} via {p['url']}"
                    if forwarded:
                        db.add_log(method=request.method, path=path, status=0, proxy=p["url"],
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note=f"mid-stream drop: {detail}")
                        # Bytes were already sent, so a transparent retry is impossible — but the
                        # client MUST get a clean terminator. A silently truncated stream makes the
                        # Anthropic SDK assert on a None final snapshot (the empty-summary error).
                        yield ("event: error\ndata: " + json.dumps(
                            {"type": "error", "error": {"type": "api_error",
                             "message": f"Upstream stream dropped mid-generation: {detail}"}}
                        ) + "\n\n").encode()
                        return
                    proxy_pool.mark_bad(p["id"], "conn"); continue
            db.add_log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                       stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                       note=f"all proxies failed: {detail}")
            yield ("event: error\ndata: " + json.dumps(
                {"type": "error", "error": {"type": "api_error", "message": f"All proxies failed. {detail}"}}
            ) + "\n\n").encode()

        return StreamingResponse(gen(), media_type="text/event-stream")

    # ---------- client wants NON-STREAM: stream upstream, assemble, return JSON ----------
    up_body = _with_stream(body, True)   # ALWAYS stream upstream — this is the fix
    attempts = 0
    detail = ""
    for p in candidates:
        attempts += 1
        res = await _consume_assemble(p, url, headers, up_body, timeout, kind)
        if res[0] == "respond":
            _, status, ct, payload = res
            db.add_log(method=request.method, path=path, status=status, proxy=p["url"],
                       attempts=attempts, stream=0, redactions=redactions,
                       ms=int((time.time() - t0) * 1000), note="ok(assembled)")
            return Response(content=payload, status_code=status, media_type=ct)
        detail = res[1]
    db.add_log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
               stream=0, redactions=redactions, ms=int((time.time() - t0) * 1000),
               note=f"all proxies failed: {detail}")
    return _err(f"All proxies failed. {detail}", 503)
