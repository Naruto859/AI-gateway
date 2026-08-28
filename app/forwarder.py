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
import uuid
import random
import socket
import asyncio
import httpx
from starlette.responses import StreamingResponse, Response, JSONResponse
from . import db, proxy_pool, filters, hedger

# ---------------------------------------------------------------------------
# Dedicated-proxy cooldown.
#
# Dedicated proxies (scrape.do, the phone) are tried before the free pool and are
# NOT tracked in the proxies table, so proxy_pool's health logic cannot skip a dead
# one. When the phone is switched off, every single request spent its first attempt
# discovering that again — 10 of 10 requests in a row in the sandbox logs. The
# cooldown remembers a connect-level failure for a short window and skips that proxy
# meanwhile, then lets it back in automatically so nothing needs manual re-enabling.
#
# Only connect-level failures arm it. An upstream 4xx/5xx is the endpoint's problem,
# not the proxy's, and must not take a good proxy out of rotation.
# ---------------------------------------------------------------------------
_DED_COOLDOWN = {}
_DED_COOLDOWN_MARKERS = ("connecterror", "connecttimeout", "connectionerror",
                         "proxyerror", "connect-failed", "conn")


def _ded_cooldown_sec():
    try:
        return float(db.get_setting("dedicated_cooldown", "60") or 60)
    except (TypeError, ValueError):
        return 60.0


def note_dedicated_failure(pid, detail):
    """Arm the cooldown for a dedicated proxy that could not be reached at all."""
    if isinstance(pid, int) or not pid:
        return
    d = (detail or "").lower()
    if any(m in d for m in _DED_COOLDOWN_MARKERS):
        _DED_COOLDOWN[pid] = time.time() + _ded_cooldown_sec()


def note_dedicated_success(pid):
    if not isinstance(pid, int) and pid:
        _DED_COOLDOWN.pop(pid, None)


def _ded_in_cooldown(pid):
    until = _DED_COOLDOWN.get(pid)
    if until is None:
        return False
    if time.time() >= until:
        _DED_COOLDOWN.pop(pid, None)
        return False
    return True


def dedicated_cooldown_state():
    """For the dashboard: {proxy_id: seconds_remaining}."""
    now = time.time()
    return {k: max(0, round(v - now)) for k, v in _DED_COOLDOWN.items() if v > now}


def _get_dedicated_candidates(tgt, max_needed):
    candidates = []

    scrape_token = tgt.get("scrape_do_token") or ""
    try: custom_proxies = json.loads(tgt.get("custom_proxies") or "[]")
    except: custom_proxies = []
    try: proxy_priority = json.loads(tgt.get("proxy_priority") or "[]")
    except: proxy_priority = []

    for p_id in proxy_priority:
        if p_id == "scrape.do" and scrape_token:
            candidates.append({"id": "scrape", "url": f"http://{scrape_token}&customHeaders=true:@proxy.scrape.do:8080"})
        elif p_id.startswith("custom_"):
            idx = int(p_id.split('_')[1])
            if idx < len(custom_proxies) and custom_proxies[idx]:
                candidates.append({"id": p_id, "url": custom_proxies[idx]})

    if candidates and len(candidates) < max_needed:
        base = list(candidates)
        while len(candidates) < max_needed:
            candidates.extend(base)

    return candidates[:max_needed]

def _resolve_candidates(tgt, attempted_pids, max_needed):
    dedicated = _get_dedicated_candidates(tgt, 99)

    # Try dedicated proxies first, ONE AT A TIME (no hedging for premium proxies).
    # A proxy in cooldown is skipped — see note_dedicated_failure. If every dedicated
    # proxy is cooling down we fall through to the free pool rather than stalling.
    for p in dedicated:
        if p["id"] not in attempted_pids and not _ded_in_cooldown(p["id"]):
            return [p]

    # If all dedicated proxies are exhausted (or none configured), use the free pool with full hedging concurrency
    fallback = tgt.get("proxy_fallback", 1) if dedicated else 1
    if fallback == 1:
        pool = [p for p in proxy_pool.ordered_for_request(max_needed * 3) if p["id"] not in attempted_pids]
        return pool[:max_needed]

    # No fallback allowed. If the only thing standing in the way is a cooldown, honour
    # the operator's "dedicated only" choice and retry the dedicated proxy anyway —
    # returning nothing here would fail the request outright.
    for p in dedicated:
        if p["id"] not in attempted_pids:
            return [p]

    return []


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


def _build_client(candidates, timeout, hedge_id=None):
    """httpx.AsyncClient with TCP keepalive and Hedging proxy support.

    When more than one candidate is raced, `hedge_id` correlates the attempt with
    the hedger's race outcome so health can be credited to the proxy that actually
    carried the request (see hedger.take_outcome).
    """
    if len(candidates) > 1:
        hedge_urls = ",".join(c["url"] for c in candidates)
        hdrs = {"x-hedge-proxies": hedge_urls}
        if hedge_id:
            hdrs["x-hedge-id"] = hedge_id
        proxy = httpx.Proxy(f"http://127.0.0.1:{hedger.hedger_port()}", headers=hdrs)
    else:
        proxy = candidates[0]["url"]
    transport = httpx.AsyncHTTPTransport(proxy=proxy, verify=False, socket_options=_keepalive_opts())
    return httpx.AsyncClient(verify=False, transport=transport, timeout=timeout)


def _new_hedge_id(candidates):
    """Token for the hedger outcome side-channel; None when no race will happen."""
    return uuid.uuid4().hex if len(candidates) > 1 else None


def _ensure_progress(candidates, attempted_pids, before):
    """Guarantee the retry loop advances.

    `attempted_pids` is now filled by _race_used, which runs on every code path —
    but if some future path returns without resolving the race, the same candidate
    list would be handed back and the loop would retry one dead proxy until
    max_retries ran out. Recording candidates[0] keeps that from ever happening.
    """
    if len(attempted_pids) == before:
        attempted_pids.add(candidates[0]["id"])


def _race_used(candidates, hedge_id, attempted_pids=None):
    """Resolve which proxy actually carried this attempt, and blame the ones whose
    CONNECT definitively failed.

    The old code credited and blamed `candidates[0]` unconditionally. With
    hedging_concurrency=10 that meant one proxy absorbed nine others' outcomes, so
    the pool's health numbers slowly became fiction — good exits got retired and
    dead ones stayed in rotation. Proxies that merely lost the race are left
    untouched: being slower than the winner is not a fault.

    `attempted_pids`, when given, is updated with only the proxies that were really
    used or really failed. The caller used to add every raced candidate, so one
    attempt consumed `hedging_concurrency` proxies and a target with max_retries=10
    got barely four real attempts before the pool "ran out".

    Call at most once per hedge_id (the outcome is popped). Returns the URL used.
    """
    outcome = hedger.take_outcome(hedge_id)
    if not outcome:
        # Single candidate, or the request never reached the hedger.
        if attempted_pids is not None:
            attempted_pids.add(candidates[0]["id"])
        return candidates[0]["url"]
    by_url = {c["url"]: c["id"] for c in candidates}
    for furl in outcome.get("failed", []):
        fid = by_url.get(furl)
        if fid is None:
            continue
        if isinstance(fid, int):
            proxy_pool.mark_bad(fid, "connect-failed")
        if attempted_pids is not None:
            attempted_pids.add(fid)
    used = outcome.get("winner") or candidates[0]["url"]
    if attempted_pids is not None:
        wid = by_url.get(used)
        if wid is not None:
            attempted_pids.add(wid)
    return used


def _pid_for(candidates, used_url):
    return next((c["id"] for c in candidates if c["url"] == used_url), None)


def _mark_used_good(candidates, used_url, ms=None):
    pid = _pid_for(candidates, used_url)
    if isinstance(pid, int):
        if ms is None:
            proxy_pool.mark_good(pid)
        else:
            proxy_pool.mark_good(pid, ms)
    else:
        note_dedicated_success(pid)


def _mark_used_bad(candidates, used_url, reason):
    pid = _pid_for(candidates, used_url)
    if isinstance(pid, int):
        proxy_pool.mark_bad(pid, reason)
    else:
        # Dedicated proxies have no DB row, so their only health memory is the cooldown.
        note_dedicated_failure(pid, reason)


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


# Failure reasons that mean "this exit IP is unusable" rather than "the upstream is
# under pressure". The next attempt uses a different proxy, so its rate-limit and WAF
# history is unrelated and waiting only adds latency.
_PROXY_LOCAL_MARKERS = (
    "waf", "non-sse", "truncated", "incomplete", "empty-stream", "proxy switch",
    "connecterror", "connecttimeout", "connectionerror", "readtimeout", "writetimeout",
    "pooltimeout", "readerror", "writeerror", "remoteprotocolerror", "proxyerror",
    "sslerror", "timeoutexception", "conn",
)


def _skip_backoff(detail):
    """True when the retry rotates to a fresh exit IP and should start immediately.

    This replaced a substring test of the form `"Error" in detail`, which matched
    almost every Python exception name (ConnectError, ProxyError, ReadTimeout…) and
    therefore skipped the backoff by accident rather than by decision — while a
    genuine upstream 5xx, the one case that benefits from spacing, fell through to
    the wait. The categories are now named explicitly.
    """
    d = (detail or "").lower()
    return any(m in d for m in _PROXY_LOCAL_MARKERS)
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
    customs = [e for e in db.list_endpoints() if e.get("enabled")]
    def mk(e):
        return {
            "name": e["url"].replace("https://", "").replace("http://", ""),
            "base": e["url"].rstrip("/"),
            "key": e.get("api_key") or s.get("gateway_key", ""),
            "mode": "openai" if e.get("api_mode") == "chat_completions" else "anthropic",
            "agentrouter": False,
            "id": e.get("id"),
            "model_override": e.get("model_override") or s.get("global_model_override", ""),
            "failover_trigger_keywords": e.get("failover_trigger_keywords", ""),
            "endpoint_failover_keywords": e.get("endpoint_failover_keywords", ""),
            "scrape_do_token": e.get("scrape_do_token", ""),
            "custom_proxies": e.get("custom_proxies", "[]"),
            "proxy_priority": e.get("proxy_priority", "[]"),
            "proxy_fallback": e.get("proxy_fallback", 1)
        }
    
    primary_custom = next((e for e in customs if e.get("is_primary")), None)
    out = []
    if primary_custom:
        out.append(mk(primary_custom))
    for e in customs:
        if primary_custom and e.get("id") == primary_custom.get("id"):
            continue
        out.append(mk(e))
    return out


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

    A turn whose content was ONLY thinking is dropped entirely rather than left as
    an empty text block: agentrouter rejects `{"type":"text","text":""}` with its
    own 400 (verified against the live endpoint), so the old "keep it valid with
    empty text" fallback traded one 400 for another. Dropping can leave two
    same-role turns adjacent, which some providers also reject, so neighbours of
    the same role are merged.

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
    kept_msgs = []
    for m in msgs:
        c = m.get("content")
        if not isinstance(c, list):
            kept_msgs.append(m)
            continue
        kept = []
        for b in c:
            if isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking"):
                removed += 1
                changed = True
                continue
            kept.append(b)
        if not kept:
            # nothing but thinking in this turn -> drop the turn
            changed = True
            continue
        m["content"] = kept
        kept_msgs.append(m)
    if not changed:
        return body_bytes, 0
    # merge any same-role neighbours created by dropping a turn
    merged = []
    for m in kept_msgs:
        prev = merged[-1] if merged else None
        if (prev and prev.get("role") == m.get("role")
                and isinstance(prev.get("content"), list) and isinstance(m.get("content"), list)):
            prev["content"] = prev["content"] + m["content"]
            continue
        merged.append(m)
    d["messages"] = merged
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


async def _consume_assemble(candidates, url, headers, body, timeout, kind, attempted_pids=None):
    """Open an upstream STREAM through `proxy`, assemble the final object.

    Returns ("respond", status, content_type, body_bytes, used_url) or
    ("retry", reason, used_url). `used_url` is the proxy that actually carried the
    request, which with hedging is not necessarily candidates[0].
    """
    # Health attribution goes through _race_used(), which asks the hedger which
    # proxy actually won the race, and _mark_used_good/bad, which guard on the id
    # being a pooled int — a dedicated proxy's id is a string ("scrape",
    # "custom_0") and would silently no-op a `WHERE id=?` update.
    hedge_id = _new_hedge_id(candidates)
    try:
        async with _build_client(candidates, timeout, hedge_id) as client:
            async with client.stream("POST", url, headers=headers, content=body) as r:
                ct = r.headers.get("content-type", "")
                used = _race_used(candidates, hedge_id, attempted_pids)
                if _is_waf(r.headers):
                    _mark_used_bad(candidates, used, "waf")
                    return ("retry", "waf", used)
                if r.status_code >= 500:
                    _mark_used_bad(candidates, used, "5xx")
                    return ("retry", str(r.status_code), used)
                _mark_used_good(candidates, used)
                # If upstream didn't actually stream (4xx error body, or plain JSON),
                # pass it straight through.
                if "text/event-stream" not in ct:
                    raw = await r.aread()
                    out_ct = "application/json" if raw[:1] in (b"{", b"[") else (ct or "application/json")
                    return ("respond", r.status_code, out_ct, raw, used)
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
                    _mark_used_bad(candidates, used, "empty-stream")
                    return ("retry", "empty-stream", used)
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
                        # The stream started fine (so the proxy was credited good
                        # above) but was cut mid-generation — that IS a proxy fault,
                        # so revoke the credit.
                        _mark_used_bad(candidates, used, "truncated")
                        return ("retry", "truncated", used)
                return ("respond", 200, "application/json",
                        json.dumps(obj, ensure_ascii=False).encode("utf-8"), used)
    except Exception as e:
        # We may have died before reading the race outcome — resolve it now so the
        # failed CONNECTs still get recorded.
        used = _race_used(candidates, hedge_id, attempted_pids)
        _mark_used_bad(candidates, used, type(e).__name__)
        return ("retry", f"{type(e).__name__}", used)


async def test_endpoint(url, api_mode, api_key, model, message, history=None, max_tokens=None):
    """Send ONE small chat to a specific endpoint, using the SAME proxy chain a real
    request would use. Returns {ok, status, ms, reply, detail}. Token-cheap.

    This used to call `_resolve_candidates(tgt, set(), 2)` exactly once, which returns
    only the FIRST dedicated proxy. When that proxy was down (phone switched off) the
    test reported "ConnectError: All connection attempts failed" for every endpoint —
    and the dashboard chat, which posts here too, failed identically. The endpoint
    itself was fine. Now it walks the chain the router walks: dedicated proxies in
    order, then the free pool, honouring the cooldown.
    """
    tgt = {}
    with db._lock:
        row = db.conn().execute("SELECT * FROM endpoints WHERE url=?", (url,)).fetchone()
        if row: tgt = dict(row)

    tried, candidates = set(), []
    # Walk the chain the same way forward() does, collecting a handful of distinct
    # proxies to try in order.
    for _ in range(4):
        batch = _resolve_candidates(tgt, tried, 3)
        if not batch:
            break
        for c in batch:
            if c["id"] not in tried:
                tried.add(c["id"])
                candidates.append(c)
        if len(candidates) >= 4:
            break
    candidates = candidates[:4]
    # Last resort: no proxy at all. This is an ADMIN diagnostic, not routed traffic —
    # its whole job is to answer "is this endpoint alive?", and when every proxy is
    # down the honest answer requires bypassing them. Without this the panel reported
    # "ConnectError" for endpoints that were actually healthy, which is exactly the
    # wrong diagnosis. Routed traffic never does this; it keeps using proxies only.
    candidates.append({"id": "__direct__", "url": ""})
    openai = (api_mode == "chat_completions")
    # A one-shot Test sends just `message`; the dashboard Chat passes the running
    # `history` so the model can actually hold a conversation instead of answering
    # every turn cold.
    msgs = list(history) if history else [{"role": "user", "content": message}]
    mt = int(max_tokens or 20)
    if openai:
        full = url.rstrip("/") + ("/chat/completions" if not url.rstrip("/").endswith("chat/completions") else "")
        body = {"model": model, "max_tokens": mt, "messages": msgs}
        headers = {"authorization": f"Bearer {api_key}", "content-type": "application/json"}
    else:
        full = url.rstrip("/") + ("/v1/messages" if "/v1/messages" not in url else "")
        body = {"model": model, "max_tokens": mt, "messages": msgs}
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
    last_status = 0
    last_proxy = ""
    attempt = 0
    ep_name = url.replace("https://", "").replace("http://", "")
    # httpx.Timeout(45) sets EVERY phase to 45s, including connect — so four dead
    # proxies took 3 minutes to report. Connect gets the pool's own (short) budget;
    # only the read waits long, because the model genuinely takes a while.
    read_to = float(db.get_setting("admin_test_timeout", "45.0") or 45.0)
    conn_to = float(db.get_setting("connect_timeout", "8") or 8)
    admin_to = httpx.Timeout(connect=conn_to, read=read_to, write=read_to, pool=conn_to)
    for p in candidates:
        attempt += 1
        direct = p["id"] == "__direct__"
        last_proxy = "direct (no proxy)" if direct else p["url"]
        try:
            client = (httpx.AsyncClient(verify=False, timeout=admin_to) if direct
                      else _build_client([p], admin_to))
            async with client:
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
                    if not direct:
                        _mark_used_good([p], p["url"], ms)
                    db.add_log(final=1, method="POST", path="endpoint/test", status=r.status_code,
                               proxy=last_proxy, attempts=attempt, stream=0, redactions=0, ms=ms,
                               note="test ok" + (" (direct)" if direct else ""),
                               model=model, endpoint=ep_name, source="test")
                    return {"ok": True, "status": r.status_code, "ms": ms,
                            "reply": reply.strip() or "(empty)", "detail": "",
                            "proxy": "direct (no proxy)" if direct else p["url"].split("@")[-1],
                            "direct": direct, "attempts": attempt}
                # An HTTP answer means the tunnel worked. Whether to blame the proxy
                # depends on WHO answered: an Aliyun WAF block page means this exit IP
                # is refused, so rotate; a JSON API error means the endpoint refused
                # the request itself and a different IP will not help.
                waf = _is_waf(r.headers, r.content[:600])
                if waf:
                    detail = f"WAF block ({r.status_code}) — this exit IP is refused by the endpoint's firewall"
                    last_status = r.status_code
                    if not direct:
                        _mark_used_bad([p], p["url"], "waf")
                    continue
                detail = f"HTTP {r.status_code}: {raw}"
                last_status = r.status_code
                if r.status_code < 500:
                    break
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
            if not direct:
                _mark_used_bad([p], p["url"], type(e).__name__)
    ms = int((time.time() - t0) * 1000)
    db.add_log(final=1, method="POST", path="endpoint/test", status=last_status or 0, proxy=last_proxy,
               attempts=attempt, stream=0, redactions=0, ms=ms, note="test failed", model=model,
               endpoint=ep_name, source="test", detail=detail[:1500])
    return {"ok": False, "status": last_status or 0, "ms": ms, "reply": "",
            "detail": detail, "attempts": attempt}


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
                            write=float(s.get("write_timeout", "120.0") or 120.0),
                            pool=float(s.get("pool_timeout", "20.0") or 20.0))

    # client IP (behind Caddy/Railway -> X-Forwarded-For) for the log detail view
    client_ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
                 or (request.client.host if request.client else ""))

    body = await request.body()
    req_text = body.decode("utf-8", "replace")[:3000]
    req_id = uuid.uuid4().hex[:16]
    _logged_final = {"done": False}

    def _log(**kw):
        """Write one attempt row.

        `final=True` marks the row that decided the request, which is what stats()
        counts. Interleaved concurrent requests can no longer be mis-grouped, and a
        failed request's total wall-clock time is finally recorded somewhere.
        """
        kw.setdefault("req_body", req_text)
        kw.setdefault("req_id", req_id)
        if kw.pop("final", False) and not _logged_final["done"]:
            _logged_final["done"] = True
            kw["final"] = 1
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
    current_model_log = req_model
    body, blocked_kw, redactions = filters.apply_filters(body)
    if blocked_kw is not None:
        db.add_log(final=1, method=request.method, path=path, status=400, proxy="", attempts=0,
                   stream=0, redactions=0, ms=0, note="blocked by content filter",
                   ip=client_ip, model=current_model_log)
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
        db.add_log(final=1, method=request.method, path=path, status=503, proxy="", attempts=0,
                   stream=0, redactions=0, ms=0, note="no usable proxies",
                   ip=client_ip, model=current_model_log)
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
                current_model_log = req_model
                attempts = 0
                detail = ""
                last_err = None  # (status, err, target_name, proxy_url) from an upstream 4xx
                logged = False   # ensure we ALWAYS write a log, even if client disconnects
                last_proxy = ""
                last_tgt_name = ""
                try:
                    for tgt in targets:
                        current_model_log = tgt.get("model_override") or req_model
                        up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
                        url = _target_url(tgt["base"], path, tgt["mode"])
                        theaders = _target_headers(request, tgt)
                        attempted_pids = set()
                        upstream_rejected = False
                        consecutive_5xx = 0
                        for _ in range(max_retries):
                            concurrency = int(db.get_setting("hedging_concurrency", "3"))
                            candidates = _resolve_candidates(tgt, attempted_pids, concurrency)
                            if not candidates:
                                break
                            # Only the proxies actually used or actually failed are
                            # recorded as attempted (done inside _race_used) — burning
                            # all `hedging_concurrency` candidates per attempt used to
                            # exhaust the pool after ~4 of 10 retries.
                            p = candidates[0]
                            pids_before = len(attempted_pids)
                            attempts += 1
                            last_proxy = p["url"]
                            last_tgt_name = tgt["name"]
                            hedge_id = _new_hedge_id(candidates)

                            async def consume(p=p, url=url, theaders=theaders,
                                              candidates=candidates, hedge_id=hedge_id):
                                # `used` is the proxy that actually won the race; every
                                # health update and log line below must name it, not
                                # candidates[0].
                                used = p["url"]
                                try:
                                    async with _build_client(candidates, timeout, hedge_id) as client:
                                        async with client.stream("POST", url, headers=theaders, content=up_body) as r:
                                            used = _race_used(candidates, hedge_id, attempted_pids)
                                            if _is_waf(r.headers):
                                                _mark_used_bad(candidates, used, "waf")
                                                return ("retry", f"WAF via {used}", used)
                                            ct = r.headers.get("content-type", "")
                                            if r.status_code >= 400 and "text/event-stream" not in ct:
                                                raw = await r.aread()
                                                err_str = str(r.status_code) + " " + raw[:300].decode('utf-8', 'replace')
                                                proxy_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,401,403,unauthorized").split(",") if k.strip()]
                                                ep_kws = [k.strip() for k in tgt.get("endpoint_failover_keywords", "Thinking,model_not_found,invalid_api_key,new_api_error,预扣").split(",") if k.strip()]
                                                
                                                if any(x in err_str for x in ep_kws):
                                                    # Endpoint's own content rejection — the proxy
                                                    # did its job, so it stays credited.
                                                    _mark_used_good(candidates, used, int((time.time() - t0) * 1000))
                                                    try: err = json.loads(raw)
                                                    except: err = {"type": "error", "error": {"message": err_str}}
                                                    return ("error", r.status_code, err, used)
                                                elif any(x in err_str for x in proxy_kws):
                                                    _mark_used_bad(candidates, used, f"{r.status_code} proxy switch")
                                                    return ("retry", f"{r.status_code} proxy switch: {err_str[:100]}", used)
                                                else:
                                                    if r.status_code >= 500:
                                                        _mark_used_bad(candidates, used, "5xx")
                                                        return ("retry", f"{r.status_code} via {used}", used)
                                                    else:
                                                        _mark_used_good(candidates, used, int((time.time() - t0) * 1000))
                                                        try: err = json.loads(raw)
                                                        except: err = {"type": "error", "error": {"message": err_str}}
                                                        return ("error", r.status_code, err, used)
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
                                                _mark_used_bad(candidates, used, "truncated")
                                                return ("retry", f"incomplete via {used}", used)
                                            _mark_used_good(candidates, used, int((time.time() - t0) * 1000))
                                            return ("ok", obj, used)
                                except Exception as e:
                                    # If we failed before the race resolved, resolve it now so
                                    # the failed CONNECTs are still recorded.
                                    if used == p["url"]:
                                        used = _race_used(candidates, hedge_id, attempted_pids)
                                    _mark_used_bad(candidates, used, type(e).__name__)
                                    return ("retry", f"{type(e).__name__} via {used}", used)

                            task = asyncio.ensure_future(consume())
                            ping_int = float(db.get_setting("keepalive_ping_interval", "10.0") or 10.0)
                            # keepalive: ping the client ~every 10s while buffering upstream
                            while not task.done():
                                done, _pending = await asyncio.wait({task}, timeout=ping_int)
                                if not done:
                                    yield PING
                            res = task.result()
                            _ensure_progress(candidates, attempted_pids, pids_before)
                            used_url = res[-1] or p["url"]
                            last_proxy = used_url
                            if res[0] == "ok":
                                for frame in _message_to_sse(res[1]):
                                    yield frame
                                _log(method=request.method, path=path, status=200, proxy=used_url,
                                           attempts=attempts, stream=1, redactions=redactions,
                                           ms=int((time.time() - t0) * 1000), note="ok(buffered)",
                                           ip=client_ip, model=current_model_log, endpoint=tgt["name"], final=True)
                                logged = True
                                return
                            if res[0] == "error":
                                _, status, err, _u = res
                                _log(method=request.method, path=path, status=status, proxy=used_url,
                                     attempts=attempts, stream=1, redactions=redactions,
                                     ms=int((time.time() - t0) * 1000), note=f"upstream rejected: {json.dumps(err)[:100]}",
                                     ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                                last_err = (status, err, tgt["name"], used_url)
                                upstream_rejected = True
                                break  # this target rejected the content — try NEXT target
                            detail = res[1]  # ("retry", reason, used) -> next proxy, same target
                            
                            proxy_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,401,403,unauthorized").split(",") if k.strip()]
                            ep_kws = [k.strip() for k in tgt.get("endpoint_failover_keywords", "Thinking,model_not_found,invalid_api_key,new_api_error,预扣").split(",") if k.strip()]
                            
                            _log(method=request.method, path=path, status=502, proxy=used_url,
                                       attempts=attempts, stream=1, redactions=redactions,
                                       ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}",
                                       ip=client_ip, model=current_model_log, endpoint=tgt["name"])

                            if any(x in detail for x in ep_kws):
                                last_err = (503, {"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} triggered endpoint failover keyword."}}, tgt["name"], used_url)
                                upstream_rejected = True
                                break
                            elif any(x in detail for x in proxy_kws):
                                pass # Just retry proxy
                            else:
                                # Default counting for consecutive unknown errors
                                consecutive_5xx += 1
                                if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                    last_err = (503, {"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} failed consecutively."}}, tgt["name"], used_url)
                                    upstream_rejected = True
                                    break

                            if not _skip_backoff(detail):
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
                                   ip=client_ip, model=current_model_log, endpoint=tname,
                                   detail=json.dumps(err)[:1500], final=True)
                        logged = True
                        yield _sse("error", err)
                        return
                    _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                               stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                               note=f"all targets failed: {detail}",
                               ip=client_ip, model=current_model_log, endpoint="", detail=str(detail)[:1500], final=True)
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
                                   ip=client_ip, model=current_model_log, endpoint=last_tgt_name, final=True)

            return StreamingResponse(gen(), media_type="text/event-stream")

        # ---- OpenAI streaming: raw relay across targets/proxies ----
        async def gen_openai():
            current_model_log = req_model
            attempts = 0
            forwarded = False
            detail = ""
            for tgt in targets:
                current_model_log = tgt.get("model_override") or req_model
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
                    candidates = _resolve_candidates(tgt, attempted_pids, concurrency)
                    if not candidates:
                        break
                    p = candidates[0]
                    pids_before = len(attempted_pids)
                    attempts += 1
                    hedge_id = _new_hedge_id(candidates)
                    used_url = p["url"]
                    client = _build_client(candidates, timeout, hedge_id)
                    try:
                        req = client.build_request("POST", url, headers=theaders, content=up_body)
                        r = await client.send(req, stream=True)
                        used_url = _race_used(candidates, hedge_id, attempted_pids)
                        _ensure_progress(candidates, attempted_pids, pids_before)
                        if _is_waf(r.headers) or r.status_code >= 500:
                            await r.aclose(); await client.aclose()
                            _mark_used_bad(candidates, used_url, "waf/5xx")
                            # `detail` used to be assigned inside the `if type(...) == int:`
                            # one-liner via a semicolon, so with a dedicated proxy it kept
                            # the previous attempt's text — or was empty on attempt 1.
                            detail = f"{r.status_code} via {used_url}"
                            _log(method=request.method, path=path, status=502, proxy=used_url, attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}", ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                            if _is_waf(r.headers): pass
                            else:
                                consecutive_5xx += 1
                                if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                    _log(method=request.method, path=path, status=503, proxy=used_url, attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"smart failover: {tgt['name']} returned 5xx {consecutive_5xx} times in a row", ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                                    detail = f"{tgt['name']} returned 5xx consecutively"
                                    break
                                if not _skip_backoff(detail):
                                    await _retry_backoff(attempts)
                            continue
                        consecutive_5xx = 0
                        if r.status_code >= 400:
                            # upstream rejection -> next target
                            await r.aclose(); await client.aclose()
                            _mark_used_good(candidates, used_url, int((time.time() - t0) * 1000))
                            detail = f"{tgt['name']} {r.status_code}"
                            break
                        _mark_used_good(candidates, used_url, int((time.time() - t0) * 1000))
                        async for chunk in r.aiter_raw():
                            forwarded = True
                            yield chunk
                        await r.aclose(); await client.aclose()
                        _log(method=request.method, path=path, status=200, proxy=used_url,
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note="ok(relay)",
                                   ip=client_ip, model=current_model_log, endpoint=tgt["name"], final=True)
                        return
                    except Exception as e:
                        try: await client.aclose()
                        except Exception: pass
                        _ensure_progress(candidates, attempted_pids, pids_before)
                        if used_url == p["url"]:
                            used_url = _race_used(candidates, hedge_id, attempted_pids)
                        detail = f"{type(e).__name__} via {used_url}"
                        if forwarded:
                            return
                        _mark_used_bad(candidates, used_url, type(e).__name__)
                        _log(method=request.method, path=path, status=502, proxy=used_url, attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}", ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                        fail_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,RemoteProtocolError").split(",") if k.strip()]
                        if any(x in detail for x in fail_kws):
                            consecutive_5xx += 1
                            if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                _log(method=request.method, path=path, status=503, proxy=used_url, attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"smart failover: {tgt['name']} returned 5xx {consecutive_5xx} times in a row", ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                                detail = f"{tgt['name']} returned 5xx consecutively"
                                break
                        else:
                            consecutive_5xx = 0
                        if not _skip_backoff(detail):
                            await _retry_backoff(attempts)
                        continue
            _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                 stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                 note=f"all targets failed: {detail}", ip=client_ip,
                 model=current_model_log, endpoint="", detail=str(detail)[:1500], final=True)
            yield ("event: error\ndata: " + json.dumps(
                {"type": "error", "error": {"type": "api_error", "message": f"All proxies failed. {detail}"}}
            ) + "\n\n").encode()

        return StreamingResponse(gen_openai(), media_type="text/event-stream")

    # ---------- client wants NON-STREAM: stream upstream, assemble, return JSON ----------
    attempts = 0
    detail = ""
    last = None  # (status, ct, payload, target_name, proxy_url) from an upstream 4xx
    for tgt in targets:
        current_model_log = tgt.get("model_override") or req_model
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
            candidates = _resolve_candidates(tgt, attempted_pids, concurrency)
            if not candidates:
                break
            p = candidates[0]
            pids_before = len(attempted_pids)
            attempts += 1
            res = await _consume_assemble(candidates, url, theaders, up_body, timeout,
                                          tgt_kind, attempted_pids)
            _ensure_progress(candidates, attempted_pids, pids_before)
            # The proxy that actually carried the request — with hedging this is the
            # race winner, not necessarily candidates[0]. Logs used to name the wrong
            # exit IP for every hedged attempt.
            used_url = res[-1] or p["url"]
            if res[0] == "respond":
                _, status, ct, payload, _u = res
                if 200 <= status < 300:
                    _log(method=request.method, path=path, status=status, proxy=used_url,
                               attempts=attempts, stream=0, redactions=redactions,
                               ms=int((time.time() - t0) * 1000), note="ok(assembled)",
                               ip=client_ip, model=current_model_log, endpoint=tgt["name"], final=True)
                    return Response(content=payload, status_code=status, media_type=ct)
                # upstream rejection (4xx) -> remember, switch to next TARGET
                err_text = payload[:100].decode('utf-8', 'replace') if isinstance(payload, bytes) else str(payload)[:100]
                _log(method=request.method, path=path, status=status, proxy=used_url,
                     attempts=attempts, stream=0, redactions=redactions,
                     ms=int((time.time() - t0) * 1000), note=f"upstream rejected: {err_text}",
                     ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                last = (status, ct, payload, tgt["name"], used_url)
                upstream_rejected = True
                break
            detail = res[1]
            
            proxy_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,401,403,unauthorized").split(",") if k.strip()]
            ep_kws = [k.strip() for k in tgt.get("endpoint_failover_keywords", "Thinking,model_not_found,invalid_api_key,new_api_error,预扣").split(",") if k.strip()]
            
            _log(method=request.method, path=path, status=502, proxy=used_url,
                       attempts=attempts, stream=0, redactions=redactions,
                       ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}",
                       ip=client_ip, model=current_model_log, endpoint=tgt["name"])

            if any(x in detail for x in ep_kws):
                payload = json.dumps({"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} triggered endpoint failover."}}).encode()
                last = (503, "application/json", payload, tgt["name"], used_url)
                upstream_rejected = True
                break
            elif any(x in detail for x in proxy_kws):
                pass
            else:
                consecutive_5xx += 1
                if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                    payload = json.dumps({"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} failed consecutively."}}).encode()
                    last = (503, "application/json", payload, tgt["name"], used_url)
                    upstream_rejected = True
                    break

            if not _skip_backoff(detail):
                await _retry_backoff(attempts)
        if not upstream_rejected:
            detail = detail or f"all proxies failed for {tgt['name']}"
    # all targets exhausted
    if last:
        status, ct, payload, tname, purl = last
        _log(method=request.method, path=path, status=status, proxy=purl,
                   attempts=attempts, stream=0, redactions=redactions,
                   ms=int((time.time() - t0) * 1000), note=f"all targets rejected (last: {tname} {status})",
                   ip=client_ip, model=current_model_log, endpoint=tname,
                   detail=payload[:1500].decode("utf-8", "replace"), final=True)
        return Response(content=payload, status_code=status, media_type=ct)
    _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
               stream=0, redactions=redactions, ms=int((time.time() - t0) * 1000),
               note=f"all targets failed: {detail}",
               ip=client_ip, model=current_model_log, endpoint="", detail=str(detail)[:1500], final=True)
    return _err(f"All proxies failed. {detail}", 503)
