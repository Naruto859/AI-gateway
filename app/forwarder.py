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
# Exact exception/marker names only. A loose fragment like "conn" would also match
# endpoint names and proxy hostnames that merely contain those letters, turning an
# upstream's own verdict into a fake transport fault — the exact bug class this file
# was cleaned of on 2026-09-02. Keep these specific.
_DED_COOLDOWN_MARKERS = ("connecterror", "connecttimeout", "connectionerror",
                         "proxyerror", "connect-failed")


def _ded_cooldown_sec():
    try:
        return float(db.get_setting("dedicated_cooldown", "60") or 60)
    except (TypeError, ValueError):
        return 60.0


# ---------------------------------------------------------------------------
# Endpoint cooldown.
#
# Measured on the live gateway 2026-09-02: the priority-0 provider's OWN database
# was down, answering every request with
#   500 {"error":{"type":"new_api_error","message":"failed to connect to
#        `user=newapi database=new-api` ... (postgres): tls error"}}
# Failover worked — the next provider served the request — but nothing REMEMBERED
# the outage, so all 4 requests in that window paid the same toll: hit the dead
# provider first, wait ~5s, log a scary 500, then start over. Boss saw a dashboard
# full of red rows for requests that actually succeeded, and every one of those
# rows was avoidable.
#
# So remember it. An endpoint that just condemned ITSELF — its database is down,
# it has no channel for the model, its key is invalid — will condemn itself again
# a second later. Push it to the BACK of the target order for a short window.
#
# Deliberately a reorder and not a skip: if every endpoint is in cooldown the
# original priority order is still walked in full, so this can never turn a
# recoverable request into a hard failure. Worst case it costs the same as today.
# ---------------------------------------------------------------------------
_EP_COOLDOWN = {}

# Failures that are properties of the ENDPOINT and will repeat immediately.
# Deliberately excludes size/context refusals (a property of the payload, so the
# next request may be fine) and anything transport-level (that is the proxy's
# fault — see _proxy_local).
_EP_COOLDOWN_MARKERS = (
    "new_api_error", "model_not_found", "invalid_api_key", "no available channel",
    "database error", "预扣", "订阅额度不足", "quota has been exhausted",
    "insufficient_quota", "billing", "无可用渠道",
)


def _ep_cooldown_sec():
    try:
        return float(db.get_setting("endpoint_cooldown", "120") or 120)
    except (TypeError, ValueError):
        return 120.0


def _note_endpoint_reject(tgt, text):
    """Arm the cooldown when an endpoint's own failure will obviously repeat."""
    if not tgt or not tgt.get("id"):
        return
    if not _fx("fx_endpoint_cooldown", tgt):
        return
    d = (text or "").lower()
    if any(m in d for m in _EP_COOLDOWN_MARKERS):
        _EP_COOLDOWN[tgt["id"]] = time.time() + _ep_cooldown_sec()


def _ep_in_cooldown(eid):
    until = _EP_COOLDOWN.get(eid)
    if until is None:
        return False
    if until <= time.time():
        _EP_COOLDOWN.pop(eid, None)
        return False
    return True


def _order_targets(targets):
    """Healthy endpoints first, recently self-condemned ones last.

    Relative order is preserved inside each group, so an operator's priority
    column still decides everything among healthy endpoints.
    """
    if not targets:
        return targets
    hot = [t for t in targets if not _ep_in_cooldown(t.get("id"))]
    cold = [t for t in targets if _ep_in_cooldown(t.get("id"))]
    return hot + cold if hot else targets


def endpoint_cooldown_state():
    """Seconds remaining per endpoint id — for the dashboard."""
    now = time.time()
    return {k: max(0, round(v - now)) for k, v in _EP_COOLDOWN.items() if v > now}


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
    # Linux rejects TCP_KEEPCNT above 127 with EINVAL, and setsockopt failing takes
    # the WHOLE connection down (httpcore raises ConnectError: [Errno 22] Invalid
    # argument), not just the option. The shipped default of 174 — and the 200 in
    # every live DB — are both over that limit, so these options could only ever
    # have worked by never being applied. Clamp instead of erroring: a caller asking
    # for "probe basically forever" gets the longest the kernel allows.
    # TCP_KEEPIDLE/INTVL are 1..32767 seconds.
    idle = max(1, min(idle, 32767))
    intvl = max(1, min(intvl, 32767))
    cnt = max(1, min(cnt, 127))
    return [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle),
        (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl),
        (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt),
    ]


class _KeepaliveBackend:
    """Applies socket options to proxied connections, which httpcore does not.

    `httpx.AsyncHTTPTransport(socket_options=...)` looks like it covers every
    connection, but httpcore's `AsyncHTTPProxy.create_connection()` builds
    AsyncForwardHTTPConnection / AsyncTunnelHTTPConnection WITHOUT passing
    `socket_options` down (verified in httpcore 1.0.9), even though both classes
    accept it. Only the direct, proxy-less pool honours it.

    Every upstream call in this gateway goes through a proxy, so in practice TCP
    keepalive was configured and never actually applied: the sockets carried the
    system default SO_KEEPALIVE=0 / idle 7200s. A silent tunnel could therefore be
    dropped by any middlebox while a model was still thinking, and the resulting
    clean EOF is indistinguishable from the provider truncating the stream.

    This wrapper sits at the network-backend layer, below that branch, and fills in
    the options when the caller passed none.
    """

    def __init__(self, inner, opts):
        self._inner = inner
        self._opts = opts

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        return await self._inner.connect_tcp(
            host, port, timeout=timeout, local_address=local_address,
            socket_options=socket_options if socket_options else self._opts,
        )

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):
        return await self._inner.connect_unix_socket(
            path, timeout=timeout,
            socket_options=socket_options if socket_options else self._opts,
        )

    async def sleep(self, seconds):
        return await self._inner.sleep(seconds)


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
    opts = _keepalive_opts()
    transport = httpx.AsyncHTTPTransport(proxy=proxy, verify=False, socket_options=opts)
    # httpcore drops socket_options on the proxied path (see _KeepaliveBackend), and
    # every request here is proxied — so wrap the pool's network backend to apply
    # them for real. Guarded by hasattr so a future httpcore that changes this
    # internal name degrades to the old behaviour instead of crashing.
    pool = getattr(transport, "_pool", None)
    if pool is not None and hasattr(pool, "_network_backend"):
        pool._network_backend = _KeepaliveBackend(pool._network_backend, opts)
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
    "sslerror", "timeoutexception",
)

# A SUBSET of the above that CANNOT be the exit IP's fault when it repeats across
# DIFFERENT exit IPs (Ciel, 2026-09-05 — this is Boss's "credit to tha, phir sab
# fail kyun hua" root cause).
#
# Measured on 2026-09-04 18:16-18:39: api.justwoker.icu returned HTTP 200 SSE
# streams that ended with no `message_stop` on ALL FOUR of its proxies in turn
# (OPPO, the 164.68 relay, POCO, scrape.do), then gorouter.app did exactly the
# same on all four. Four unrelated exit IPs producing one identical verdict is
# the upstream's own behaviour — the provider was cutting streams mid-generation.
# But `_proxy_local()` classified each one as a transport fault, so the endpoint
# was never blamed and the router dutifully spent every proxy it had: 8 attempts
# and ~300 SECONDS before it even reached the third provider. The client then saw
# tabitoken's 403 (out of credit) and Boss reasonably concluded "the credit story
# does not add up" — it did not, because the 403 was the last domino, not the
# first.
#
# A CONNECT-level failure is deliberately NOT here: a switched-off phone really
# is proxy-local, and rotating away from it is correct.
_UPSTREAM_STREAM_MARKERS = ("incomplete", "truncated", "empty-stream", "non-sse")


def _fault_class(detail):
    """Name the transport verdict, or None when it is not one of ours."""
    d = (detail or "").lower()
    for m in _UPSTREAM_STREAM_MARKERS:
        if m in d:
            return m
    return None


# Deliberately NOT in _PROXY_LOCAL_MARKERS: a bare "conn". It was in the first
# version of this fix and it was a mistake of my own — `detail` is built from the
# endpoint NAME and the proxy URL (f"{tgt['name']} {status}", f"{type(e).__name__} via
# {used_url}"), so a provider or proxy host with "conn" in it would have had its
# real failures silently reclassified as transport faults and retried forever on
# fresh IPs. It also earned nothing: every genuine case arrives as an httpx
# exception class name, and all of those are listed explicitly above —
# ProxyError already covers the phone-offline shape ("ProxyError: 500 Unable to
# connect", measured 0.8 s when the target is unreachable). A loose marker that
# can only misfire is worse than no marker.


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


# Markers that identify a refusal caused by THE REQUEST'S SIZE. Retrying such a
# request on another PROXY or another KEY of the SAME provider cannot succeed —
# identical bytes reach the identical tokenizer, so the verdict is identical.
# Failing over to a DIFFERENT provider is still worth doing: ceilings and
# tokenizers differ (measured: the same 1,025 KB body was 1.06M tokens and
# refused by api.justwoker.icu, but 288,753 tokens and accepted by
# api.camel-hub.com).
#
# Measured on 2026-09-02 why this matters: one oversized Claude Code turn burned
# 15 upstream attempts across 5 providers and 378 SECONDS of wall clock, most of
# it re-sending the same payload through fresh proxies for a provider that had
# already given its size verdict on attempt 1. Worst observed: 465 s / 23
# attempts. The client saw only keepalive pings throughout, which is why
# /compact appeared to "do nothing" — Claude Code never learned the context was
# full, it just waited.
_REQUEST_FATAL_MARKERS = (
    "context window is full",
    "请精简对话历史",              # new-api: "please condense the conversation history"
    "缩小工具/文件输出",           # new-api: "reduce tool/file output"
    "prompt is too long",
    "maximum context length",
    "context_length_exceeded",
    "request_too_large",
    "request entity too large",
    "input length and `max_tokens` exceed",
)


def _fx(key, tgt=None, default="1"):
    """Is routing-fix `key` enabled — for THIS endpoint?

    Boss's correction (2026-09-02): "ye toggles endpoint settings ke andar hona
    chahiye, na ki proxy settings ke andar — kyunki wo endpoint ki settings hai
    na?" He is right: every one of these rules describes how a PARTICULAR
    provider's failures should be read, so a single global switch is the wrong
    shape. His example is the deciding one — an official/premium endpoint should
    get *fewer* restrictions than a free relay, and a long real conversation on
    it can legitimately repeat itself a lot.

    Resolution order:
      1. this endpoint's own `fx_flags` JSON (only non-default values are stored)
      2. the global `settings` row, which acts as the fleet-wide default
      3. `default` — ON, the measured-correct behaviour

    Read per call: `db.get_setting` is an indexed lookup and a toggle must take
    effect without restarting the gateway.
    """
    if isinstance(tgt, dict):
        flags = tgt.get("_fx_flags")
        if flags is None:
            raw = tgt.get("fx_flags") or ""
            try:
                flags = json.loads(raw) if raw.strip() else {}
            except Exception:
                flags = {}
            if not isinstance(flags, dict):
                flags = {}
            tgt["_fx_flags"] = flags       # parse once per request, not per check
        if key in flags:
            return str(flags[key]) != "0"
    try:
        return db.get_setting(key, default) != "0"
    except Exception:
        return default != "0"


def _request_fatal(status, text, tgt=None):
    """Reason string when this refusal is about the REQUEST's size, else None.

    Correction (measured 2026-09-02, my first version of this was wrong): a size
    refusal is NOT hopeless across providers. The identical 1,025 KB body was
    refused by api.justwoker.icu as "context window is full" and accepted by
    api.camel-hub.com, which counted it as only 288,753 input tokens — a 3.6x
    difference in tokenizer/ceiling for the same bytes. So this classifier must
    only stop the retries that are provably pointless — another PROXY or another
    KEY for the SAME provider, which re-sends identical bytes to the identical
    tokenizer — and must still allow failover to the NEXT provider, which may
    well accept it.
    """
    if status not in (400, 413, 422):
        return None
    if not _fx("fx_size_refusal_stop", tgt):
        return None
    low = (text or "").lower()
    for m in _REQUEST_FATAL_MARKERS:
        if m in low or m in (text or ""):
            return m
    return None


def _match_failover(keywords, status, body_text, tgt=None):
    """Does this refusal match an operator failover rule?

    Replaces a bare `any(x in err_str for x in kws)` substring test where
    `err_str` was the status code CONCATENATED WITH THE RESPONSE BODY. Because the
    default rule lists are numeric ("500,501,502,503,504,524,401,403"), any digit
    run inside the body matched them — and new-api upstreams embed a long numeric
    request id in every error, e.g.

        (request id: 202609011959144518501558268d9d65byLqCso)
                                  ^^^^  contains "501"

    Verified on the live log: 1 of 3 recorded context-full 400s contained such a
    token, so an identical error was classified as a PROXY fault on one request
    and an ENDPOINT fault on the next. That is exactly the "sometimes it retries,
    sometimes it switches, and the log never says why" behaviour Boss reported —
    it was never random, it depended on the digits in the upstream's request id.

    Numeric keywords are therefore matched against the HTTP STATUS ONLY;
    non-numeric keywords are matched against the body, case-insensitively.

    Toggle `fx_status_only_keywords` restores the old body-wide substring match.
    """
    low = (body_text or "").lower()
    strict = _fx("fx_status_only_keywords", tgt)
    for k in keywords:
        k = (k or "").strip()
        if not k:
            continue
        if k.isdigit():
            if int(k) == status:
                return True
            if not strict and k in low:
                return True     # legacy behaviour: digits matched anywhere
        elif k.lower() in low:
            return True
    return False


def _proxy_local(detail, tgt=None):
    """True when this failure happened in OUR transport, not at the provider.

    WAF pages, ConnectError, ReadError, ProxyError, truncated/incomplete streams
    — these describe the exit IP we borrowed, and say nothing about whether the
    provider is healthy. The right response is a different proxy, not a different
    provider.

    Why this matters (measured 2026-09-02): the failover ladder ended in a bare
    `else: consecutive_5xx += 1`, so every one of these transport faults counted
    as a strike AGAINST the endpoint. On Boss's gateway `failover_5xx_threshold`
    is **2**, so two unlucky proxies retired a perfectly good provider — and
    `max_retries=15` was never reached. A real trace of one large request:

        att 1  justwoker  WAF via 100.97.11.41      -> strike 1
        att 2  justwoker  502 via scrape.do         -> (matched proxy rule, ok)
        att 3  justwoker  ConnectError via 8.211…   -> strike 2 -> ABANDONED
        att 4  gorouter   WAF …                     -> strike 1
        att 6  gorouter   ReadError …               -> strike 2 -> ABANDONED
        att 7  agentrouter                          -> 200 OK

    Three providers were walked and two written off, none of which had done
    anything wrong. That is Boss's "kabhi max retry karta hai, kabhi switch kar
    deta hai, aur logs mein kyun nahi dikhta" — the reason was never logged
    because the code never knew it was making a decision.

    Toggle `fx_proxy_not_endpoint` turns this attribution off.
    """
    if not _fx("fx_proxy_not_endpoint", tgt):
        return False
    d = (detail or "").lower()
    return any(m in d for m in _PROXY_LOCAL_MARKERS)


def _match_failover_detail(keywords, detail, tgt=None):
    """Same rules as _match_failover, for an internally-built retry reason.

    `detail` looks like "502 via http://1.2.3.4:8888" or "ConnectError via ...".
    A numeric keyword must match the LEADING status token, never a digit run
    inside a proxy host/port (a proxy on port 8501 otherwise matched the rule
    "501"). Non-numeric keywords still match anywhere in the text.
    """
    d = (detail or "")
    lead = d.split(" ", 1)[0]
    status = int(lead) if lead.isdigit() else -1
    return _match_failover(keywords, status, d, tgt)


def _clip(text, n=100):
    """Truncate for a log `note` without splitting a UTF-8 character.

    The old `f"...{err_full[:100]}"` cut the Chinese context-full message
    mid-codepoint, producing a mojibake tail AND hiding the English half of the
    message — "(Context window is full — reduce conversation history, tool/file
    output, or system prompt.)" never appeared in any log row, which is why the
    real cause went undiagnosed. The full text now always goes to `detail`.
    """
    s = text if isinstance(text, str) else str(text)
    if len(s) <= n:
        return s
    out = s[:n]
    # Trim a trailing lone surrogate/partial sequence introduced by slicing.
    while out and not out[-1].isprintable() and out[-1] not in " \t":
        out = out[:-1]
    return out


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


def _is_waf(resp_headers, sniff=b"", status=None, tgt=None):
    """Does this response look like a WAF/bot-challenge page?

    Boss pushed back on this (2026-09-02): "mera mobile IP residential hai, usme
    Cloudflare nahi aata" — yet the log was full of `WAF via
    http://100.97.11.41:8888`. He was right. Direct tests through that exact
    proxy returned `application/json` 200 three times (500 B, 293 KB, 1,025 KB),
    each with `server: cloudflare` — Cloudflare fronting the provider is normal
    and is NOT a challenge.

    The old rule was `"text/html" in content-type -> WAF`, which also catches an
    nginx 502/413 error PAGE, a captive-portal page from a flaky phone network,
    and any provider that returns HTML for an ordinary error. Those are transport
    or endpoint faults, not bot challenges, so the "WAF" label buried the real
    cause in every log row it touched.

    Strict rules now:
      * a known challenge fingerprint in the body -> WAF, whatever the status;
      * HTML with a 2xx status -> WAF (a JSON API answering 200 with a web page
        means something intercepted the request);
      * HTML with a 4xx/5xx status -> NOT WAF, because the status path already
        classifies it correctly and reports the real reason.
    `fx_strict_waf=0` restores the old any-HTML behaviour.
    """
    ct = resp_headers.get("content-type", "")
    low = sniff[:1200].lower()
    if b"aliyun_waf" in low:
        return True
    if "text/html" not in ct:
        return False
    if not _fx("fx_strict_waf", tgt):
        return True     # legacy: any HTML counts
    for m in (b"cf-browser-verification", b"cf_chl", b"just a moment",
              b"checking your browser", b"attention required", b"captcha",
              b"ddos-guard", b"__cf_bm", b"security check", b"incapsula"):
        if m in low:
            return True
    if status is None:
        return True     # no status to reason with: keep the old, safer verdict
    return 200 <= int(status) < 300


def _err(message, status=503, etype="api_error"):
    return JSONResponse({"type": "error", "error": {"type": etype, "message": message}},
                        status_code=status)


def _endpoint_keys(e, s):
    """Every API key this endpoint may use, primary first, duplicates removed.

    Providers meter quota per key, so an endpoint can hold spares in `extra_keys`.
    Exhausting one key should move to the next key for the SAME provider before the
    provider is abandoned.
    """
    keys = []
    primary = e.get("api_key") or s.get("gateway_key", "")
    if primary:
        keys.append(primary)
    try:
        extra = json.loads(e.get("extra_keys") or "[]")
    except (TypeError, ValueError):
        extra = []
    if isinstance(extra, list):
        for k in extra:
            k = str(k or "").strip()
            if k and k not in keys:
                keys.append(k)
    return keys or [""]


def _label(e):
    """Display name for logs and the routing panel.

    A bare host is ambiguous once the same provider appears more than once (one row per
    key), so an unnamed duplicate is suffixed with its row id. Anything the operator
    named keeps that name.
    """
    if (e.get("name") or "").strip():
        return e["name"].strip()
    return e["url"].replace("https://", "").replace("http://", "")


def _targets(s):
    customs = [e for e in db.list_endpoints() if e.get("enabled")]

    # Disambiguate unnamed rows that share a URL, so their stats and log lines do not
    # merge into one indistinguishable entry.
    host_counts = {}
    for e in customs:
        host = e["url"].replace("https://", "").replace("http://", "")
        host_counts[host] = host_counts.get(host, 0) + 1

    def mk(e):
        host = e["url"].replace("https://", "").replace("http://", "")
        name = _label(e)
        if not (e.get("name") or "").strip() and host_counts.get(host, 0) > 1:
            name = f"{host} #{e.get('id')}"
        return {
            "name": name,
            "base": e["url"].rstrip("/"),
            "key": e.get("api_key") or s.get("gateway_key", ""),
            "keys": _endpoint_keys(e, s),
            "mode": "openai" if e.get("api_mode") == "chat_completions" else "anthropic",
            "agentrouter": False,
            "id": e.get("id"),
            "model_override": e.get("model_override") or s.get("global_model_override", ""),
            "failover_trigger_keywords": e.get("failover_trigger_keywords", ""),
            "endpoint_failover_keywords": e.get("endpoint_failover_keywords", ""),
            "key_failover_keywords": e.get("key_failover_keywords", ""),
            "scrape_do_token": e.get("scrape_do_token", ""),
            "custom_proxies": e.get("custom_proxies", "[]"),
            "proxy_priority": e.get("proxy_priority", "[]"),
            "proxy_fallback": e.get("proxy_fallback", 1),
            # Per-endpoint failure-diagnosis overrides. Only keys the operator
            # actually changed are stored; anything absent falls back to the
            # global setting. See _fx().
            "fx_flags": e.get("fx_flags", ""),
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


def _should_rotate_key(tgt, detail):
    """True when this failure should be retried on the endpoint's NEXT key.

    Empty `key_failover_keywords` means "any refusal rotates" — the sensible default
    for someone who just added a spare key and expects it to be used. A non-empty list
    narrows rotation to those phrases only.

    Endpoint-switch triggers always win: a refusal that a different key would repeat
    (blocked content, unknown model) must not burn every key first.

    A SIZE refusal never rotates: the payload is identical, so every key of this
    provider hits the same tokenizer and the same ceiling. Rotating there just
    multiplies the wait by the number of spare keys.
    """
    if len(tgt.get("keys") or []) < 2:
        return False
    d = (detail or "")
    lead = d.split(" ", 1)[0]
    status = int(lead) if lead.isdigit() else -1
    if _request_fatal(status if status > 0 else 400, d, tgt):
        return False
    ep_kws = [k.strip() for k in (tgt.get("endpoint_failover_keywords") or "").split(",") if k.strip()]
    if _match_failover(ep_kws, status, d, tgt):
        return False
    rules = [k.strip() for k in (tgt.get("key_failover_keywords") or "").split(",") if k.strip()]
    if not rules:
        return True
    return _match_failover(rules, status, d, tgt)


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


def _target_headers(request, target, key=None):
    """Headers for a specific target, optionally with a specific API key.

    `key` overrides target["key"] so the retry loop can re-send the same request on the
    endpoint's next key without rebuilding the target.
    """
    kind = "openai" if target["mode"] == "openai" else "anthropic"
    return _build_upstream_headers(request, key or target["key"], kind)



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


# A JSON `null` payload is the one SSE frame no OpenAI-style client can survive.
# The SDK does `json.loads(data)` and hands the result to its model constructor,
# so `data: null` becomes a literal `None` in the chunk iterator — and the very
# next line of every client is `chunk.choices[0].delta`, which raises
#   AttributeError: 'NoneType' object has no attribute 'choices'
# mid-conversation. Verified against this gateway with the real `openai` SDK:
# 3 good chunks, 1 None, then that exact crash.
#
# It is not in the OpenAI SSE shape and carries no information, so dropping the
# frame loses nothing. Toggle: fx_drop_null_frames (per-endpoint, default ON).
def _is_null_frame(block: bytes) -> bool:
    _ev, data = _parse_block(block)
    return data is not None and data.strip() == "null"


# The relay path reads arbitrary byte chunks (`aiter_raw`), so a frame can be
# split across two reads and cannot be inspected without reassembly. This splits
# on a frame boundary while RE-EMITTING the separator bytes it found, so a
# filtered relay is byte-identical to the raw one apart from the frames
# deliberately dropped. Both LF-LF and CRLF-CRLF are handled: splitting only on
# b"\n\n" would never match a CRLF upstream (b"\r\n\r\n" contains no b"\n\n"),
# and the buffer would then grow until end-of-stream — a hang, not a filter.
def _sse_frames(buf: bytes):
    """Yield (frame_body, separator) for every complete frame; return remainder.

    Used as: ``for body, sep in ...`` via the wrapper below.
    """
    out = []
    while True:
        i_crlf = buf.find(b"\r\n\r\n")
        i_lf = buf.find(b"\n\n")
        if i_crlf < 0 and i_lf < 0:
            break
        if i_crlf >= 0 and (i_lf < 0 or i_crlf <= i_lf):
            out.append((buf[:i_crlf], b"\r\n\r\n"))
            buf = buf[i_crlf + 4:]
        else:
            out.append((buf[:i_lf], b"\n\n"))
            buf = buf[i_lf + 2:]
    return out, buf



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
        # `json.loads` succeeds on bare `null`, `[]`, `3`, `"x"` — all valid JSON,
        # none of them an event object. Without this the next line raised
        # AttributeError: 'NoneType' object has no attribute 'get' and killed the
        # whole assembling task, which the caller reported as a transport fault
        # ("<type> via <proxy>") and retried on another IP — a client-visible
        # failure with a misattributed cause. Verified: feed("", "null") raised
        # before this guard, returns quietly after.
        if not isinstance(obj, dict):
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
        # Same guard as AnthropicAssembler: `null` is valid JSON but not a chunk
        # object, and `o.get(...)` on it raised AttributeError inside the
        # assembling task.
        if not isinstance(o, dict):
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


async def _consume_assemble(candidates, url, headers, body, timeout, kind,
                            attempted_pids=None, tgt=None):
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
                if _is_waf(r.headers, status=r.status_code):
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
                    # A 2xx whose body is not a usable message is NOT a success.
                    #
                    # Measured 2026-09-02: proxy 3.122.224.70:15182 is an open
                    # "echo" server — it answers CONNECT with 200 OK (Server: Oracle
                    # Containers for J2EE) and returns its own diagnostic text
                    # (`REMOTE_ADDR = ... REQUEST_METHOD = POST`) instead of relaying
                    # to the provider at all. The gateway logged five of those as
                    # `ok(assembled)` HTTP 200 in 0.1s and handed the client an
                    # unusable body. An empty or non-JSON 2xx on an API route is a
                    # broken exit, not an answer: retry on another proxy.
                    # Toggle: fx_reject_empty_2xx.
                    if (200 <= r.status_code < 300 and _fx("fx_reject_empty_2xx", tgt)
                            and raw[:1] not in (b"{", b"[")):
                        _mark_used_bad(candidates, used, "non-json-2xx")
                        _peek = raw[:60].decode("utf-8", "replace").replace("\n", " ")
                        return ("retry", f"non-json 2xx via {used}: {_peek!r}", used)
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
                waf = _is_waf(r.headers, r.content[:600], status=r.status_code)
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

    # ---------- METHOD-PRESERVING PASSTHROUGH (Ciel, 2026-09-02) ----------
    # Every upstream call site below is hardcoded to POST, because the whole
    # retry/hedge/assemble machinery was written for /v1/messages. That silently
    # broke every non-POST route: a client's `GET /v1/models` was forwarded as
    # `POST /v1/models`, so all five providers answered
    #   404 {"message":"Invalid URL (POST /v1/models)"}
    # and the gateway then walked the ENTIRE endpoint list collecting the same
    # 404 — 5 attempts, ~11 s, 78 such rows in one 8h window. The dashboard
    # showed "Method: GET" next to an upstream complaining about POST, which is
    # what made it look inexplicable.
    #
    # Body-less informational routes need none of the streaming machinery, so
    # they take a direct, method-correct path: first enabled endpoint, its own
    # key, no proxy hedging, no retry storm. If it fails we return the upstream's
    # own answer rather than fabricating a 404 from a different provider.
    # Toggle: fx_method_passthrough, read from the endpoint we are about to use.
    if request.method in ("GET", "HEAD") and not body:
        tgts = _targets(s)
        if tgts and _fx("fx_method_passthrough", tgts[0]):
            # ...but "first enabled endpoint" alone is not enough. Measured on the
            # friend's gateway 2026-09-05: AgentRouter was dragged to priority 0 and
            # it answers `GET /v1/models` with 405 + a Chinese HTML page (it only
            # serves /v1/messages). That 405 was returned verbatim, so the client's
            # model picker went empty again — "endpoint advertised no models" — even
            # though the very next endpoint (justwoker) answers 200 with a real list.
            # A provider that does not serve an informational route is exactly the
            # case failover exists for. Only route-level rejections (404/405/501) and
            # transport errors move on; every other verdict is still that provider's
            # answer and is returned as-is, so this cannot become the 5-attempt walk
            # that the POST-as-GET defect used to cause.
            _info_failover = _fx("fx_info_failover", tgts[0])
            _chain = []
            for _i, tgt in enumerate(tgts):
                _last = (_i == len(tgts) - 1) or not _info_failover
                url = f"{tgt['base'].rstrip('/')}/{path}"
                hdrs = _target_headers(request, tgt, (tgt.get("keys") or [tgt.get("key", "")])[0])
                t_info = time.time()
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=conn_to, read=30.0,
                                                                      write=30.0, pool=10.0),
                                                 follow_redirects=True) as c:
                        r = await c.request(request.method, url, headers=hdrs)
                    raw = r.content
                    _route_miss = r.status_code in (404, 405, 501)
                    _move_on = _route_miss and not _last
                    _chain.append(f"{tgt['name']} {r.status_code}")
                    _log(method=request.method, path=path, status=r.status_code, proxy="direct",
                         attempts=_i + 1, stream=0, redactions=0,
                         ms=int((time.time() - t_info) * 1000),
                         note=(f"info passthrough ({request.method}) -> {tgt['name']}"
                               + (f": route not served, trying {tgts[_i + 1]['name']}" if _move_on else "")),
                         ip=client_ip, model="", endpoint=tgt["name"],
                         detail=raw[:1500].decode("utf-8", "replace"), final=not _move_on)
                    if _move_on:
                        continue
                    return Response(content=raw, status_code=r.status_code,
                                    media_type=r.headers.get("content-type", "application/json"))
                except Exception as e:
                    _chain.append(f"{tgt['name']} {type(e).__name__}")
                    _move_on = not _last
                    _log(method=request.method, path=path, status=502, proxy="direct",
                         attempts=_i + 1, stream=0, redactions=0,
                         ms=int((time.time() - t_info) * 1000),
                         note=(f"info passthrough failed: {type(e).__name__}"
                               + (f", trying {tgts[_i + 1]['name']}" if _move_on else "")),
                         ip=client_ip, model="", endpoint=tgt["name"],
                         detail=str(e)[:1500], final=not _move_on)
                    if _move_on:
                        continue
                    return _err(f"Upstream unreachable for {request.method} {path}. "
                                f"Tried: {', '.join(_chain)}", 502)

    try:
        client_wants_stream = bool(json.loads(body).get("stream"))
    except Exception:
        client_wants_stream = "text/event-stream" in request.headers.get("accept", "")

    kind = "openai" if "chat/completions" in path else "anthropic"
    targets = _order_targets(_targets(s))
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
                # Emit one keepalive ping BEFORE contacting any upstream.
                #
                # Measured 2026-09-02: a Cloudflare-proxied host held the response
                # headers until the first body byte arrived (headers_at=70.0s on a
                # 1.2M-char request), because CF buffers until the origin produces
                # something. Cloudflare's free plan then aborts with 524 if that
                # takes ~100s — so a slow first upstream killed even a STREAM, and
                # the 417s stream that survived only survived because its headers
                # happened to land at 70s. Sending a ping immediately starts the
                # byte flow in milliseconds, after which the periodic pings keep
                # the timer reset for as long as the generation needs.
                #
                # Safe by protocol: the Anthropic SDK explicitly ignores `ping`
                # events, and StreamingResponse has already committed HTTP 200, so
                # this changes nothing about how errors are delivered (they still
                # arrive as an SSE `error` event, as before).
                if _fx("fx_early_ping", targets[0] if targets else None):
                    yield PING
                try:
                    _chain = []   # one line per target: what this provider actually did
                    for tgt in targets:
                        current_model_log = tgt.get("model_override") or req_model
                        up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
                        url = _target_url(tgt["base"], path, tgt["mode"])
                        attempted_pids = set()
                        upstream_rejected = False
                        consecutive_5xx = 0
                        # Distinct exit IPs that produced the same stream verdict for
                        # THIS endpoint. See _UPSTREAM_STREAM_MARKERS.
                        _stream_faults = {}
                        # This target's own last word, for the chain summary. MUST be
                        # initialised here: `detail` is only bound inside the retry
                        # branch, so a target that fails via the upstream-error path
                        # (size refusal, 403, content-blocked) would otherwise read a
                        # previous target's detail — or raise NameError on target #1.
                        _tgt_verdict = ""
                        # `detail` must exist before the first inner iteration too: the
                        # per-target summary below reads it even when the very first
                        # attempt raised before assignment.
                        detail = ""
                        ep_keys = tgt.get("keys") or [tgt.get("key", "")]
                        key_idx = 0
                        theaders = _target_headers(request, tgt, ep_keys[0])
                        for _ in range(max_retries * max(1, len(ep_keys))):
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
                            # Side channel for stream evidence (see the "incomplete"
                            # branch inside consume()). Fresh per attempt, so one
                            # attempt's numbers can never be logged against another.
                            _ev_box = {}

                            async def consume(p=p, url=url, theaders=theaders,
                                              candidates=candidates, hedge_id=hedge_id,
                                              _ev_box=_ev_box):
                                # `used` is the proxy that actually won the race; every
                                # health update and log line below must name it, not
                                # candidates[0].
                                used = p["url"]
                                try:
                                    async with _build_client(candidates, timeout, hedge_id) as client:
                                        async with client.stream("POST", url, headers=theaders, content=up_body) as r:
                                            used = _race_used(candidates, hedge_id, attempted_pids)
                                            if _is_waf(r.headers, status=r.status_code, tgt=tgt):
                                                _mark_used_bad(candidates, used, "waf")
                                                return ("retry", f"WAF via {used}", used)
                                            ct = r.headers.get("content-type", "")
                                            # ROOT CAUSE FIX (Ciel, 2026-09-02).
                                            # This condition used to be
                                            #   `if r.status_code >= 400 and "text/event-stream" not in ct:`
                                            # which is wrong: new-api upstreams answer a
                                            # REJECTED stream request with HTTP 400 but keep
                                            # `content-type: text/event-stream`, while the body
                                            # is plain JSON (verified against
                                            # api.justwoker.icu — 400, ct=text/event-stream,
                                            # body {"error":{...context window is full...}}).
                                            # So the guard was false, the JSON error fell
                                            # through to the SSE assembler, produced no
                                            # frames, and was reported as
                                            #   ("retry", "incomplete via <proxy>")
                                            # i.e. a PROXY fault. Three consequences, all of
                                            # which Boss saw and could not explain:
                                            #   1. the real reason never reached the log — the
                                            #      note said "incomplete via ...", so no row
                                            #      ever explained why the endpoint switched;
                                            #   2. a healthy proxy was marked bad for someone
                                            #      else's 400, poisoning proxy health;
                                            #   3. the request kept retrying instead of
                                            #      surfacing a verdict the client could act on.
                                            # The status code is authoritative; content-type is
                                            # only used to decide how to READ the body.
                                            # Toggle: fx_status_over_contenttype.
                                            if r.status_code >= 400 and (
                                                    _fx("fx_status_over_contenttype", tgt)
                                                    or "text/event-stream" not in ct):
                                                raw = await r.aread()
                                                err_str = str(r.status_code) + " " + raw[:300].decode('utf-8', 'replace')
                                                body_str = raw[:400].decode('utf-8', 'replace')
                                                proxy_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,401,403,unauthorized").split(",") if k.strip()]
                                                ep_kws = [k.strip() for k in tgt.get("endpoint_failover_keywords", "Thinking,model_not_found,invalid_api_key,new_api_error,预扣").split(",") if k.strip()]

                                                # Size/context refusal: this provider's verdict
                                                # on these bytes is final, so stop spending its
                                                # proxy and key budget. Another provider may
                                                # still accept it (different tokenizer), so this
                                                # is an ("error", ...) — which fails over to the
                                                # NEXT target — not a hard return.
                                                if _request_fatal(r.status_code, body_str, tgt):
                                                    _mark_used_good(candidates, used, int((time.time() - t0) * 1000))
                                                    try: err = json.loads(raw)
                                                    except Exception: err = {"type": "error", "error": {"message": body_str}}
                                                    return ("error", r.status_code, err, used)

                                                if _match_failover(ep_kws, r.status_code, body_str, tgt):
                                                    # Endpoint's own content rejection — the proxy
                                                    # did its job, so it stays credited.
                                                    _mark_used_good(candidates, used, int((time.time() - t0) * 1000))
                                                    try: err = json.loads(raw)
                                                    except: err = {"type": "error", "error": {"message": err_str}}
                                                    return ("error", r.status_code, err, used)
                                                elif _match_failover(proxy_kws, r.status_code, body_str, tgt):
                                                    _mark_used_bad(candidates, used, f"{r.status_code} proxy switch")
                                                    return ("retry", f"{r.status_code} proxy switch: {_clip(body_str)}", used)
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
                                            seen = b""   # first bytes, for diagnosing a non-SSE body
                                            # EVIDENCE FOR AN INCOMPLETE STREAM (Ciel, 2026-09-05).
                                            # Every "incomplete via <proxy>" row Boss ever saw had an
                                            # EMPTY detail column (measured: 36/36 on 2026-09-04), so
                                            # the one question that matters — did the upstream stop
                                            # talking, or did it say something we threw away? — was
                                            # unanswerable after the fact. These four counters cost
                                            # nothing and make the row self-explaining.
                                            _n_bytes = 0
                                            _n_frames = 0
                                            _last_ev = None
                                            _tail = b""
                                            async for chunk in r.aiter_bytes():
                                                if len(seen) < 400:
                                                    seen += chunk[:400]
                                                _n_bytes += len(chunk)
                                                _tail = (_tail + chunk)[-400:]
                                                buf += chunk
                                                while b"\n\n" in buf:
                                                    block, buf = buf.split(b"\n\n", 1)
                                                    ev, data = _parse_block(block)
                                                    _n_frames += 1
                                                    if ev:
                                                        _last_ev = ev
                                                    asm.feed(ev, data)
                                                    if ev == "message_stop":
                                                        saw_stop = True
                                            if buf.strip():
                                                ev, data = _parse_block(buf); asm.feed(ev, data)
                                                if ev == "message_stop":
                                                    saw_stop = True
                                            obj = asm.result()
                                            # A REFUSAL IS A COMPLETE ANSWER (Ciel,
                                            # 2026-09-02). The test below used to require
                                            # `obj.get("content")` to be non-empty, but a
                                            # model declining to answer returns a valid
                                            # message with content=[] and
                                            # stop_reason="refusal" (measured on
                                            # agentrouter: HTTP 200, 0 blocks,
                                            # stop=refusal). Calling that "incomplete"
                                            # blamed the proxy, marked it bad, and
                                            # retried a DETERMINISTIC answer across every
                                            # proxy and every provider — measured cost:
                                            # 11 attempts, 5 providers, 75.8 s, while the
                                            # client saw nothing but pings. A refusal is
                                            # final: deliver it.
                                            # Toggle: fx_refusal_is_answer.
                                            _stop = (obj or {}).get("stop_reason")
                                            _stop_ok = (_fx("fx_refusal_is_answer", tgt)
                                                        and _stop in ("refusal", "stop_sequence",
                                                                      "max_tokens", "end_turn"))
                                            complete = bool(saw_stop and obj and _stop
                                                            and (obj.get("content") or _stop_ok))
                                            if complete:
                                                for b in obj.get("content", []):
                                                    if b.get("type") == "tool_use" and "input" not in b:
                                                        complete = False; break
                                            if not complete:
                                                # An HTTP-200 stream that yielded no usable
                                                # frames is usually a JSON error body served
                                                # with an event-stream content-type. Name that
                                                # in the retry reason instead of the misleading
                                                # "incomplete via <proxy>", which blamed the
                                                # exit IP for the upstream's own answer and is
                                                # why no log row ever explained the switch.
                                                _mark_used_bad(candidates, used, "truncated")
                                                if not obj or not obj.get("content"):
                                                    _peek = seen.decode("utf-8", "replace").strip()
                                                    if _peek.startswith("{"):
                                                        return ("error_body", r.status_code, _peek, used)
                                                # Carry the stream's own vital signs back to the
                                                # caller so the log row can say WHY it was
                                                # incomplete. Without this the note is
                                                # unfalsifiable — see the 2026-09-05 audit where
                                                # all 36 such rows had detail=''.
                                                #
                                                # It goes in a side box, NOT in the tuple and NOT
                                                # in the reason string. Two hard reasons:
                                                #   * every caller reads `res[-1]` as the proxy
                                                #     URL, and the ("error", status, err, used)
                                                #     shape is already 4 long — appending a 4th
                                                #     element to the retry shape would silently
                                                #     make the evidence dict be treated as a
                                                #     proxy URL;
                                                #   * `detail` (= res[1]) is keyword-matched by
                                                #     _match_failover_detail / _proxy_local, so
                                                #     splicing upstream text into it could make a
                                                #     stray "403" or "unauthorized" in the tail
                                                #     trigger a failover rule. That is exactly the
                                                #     numeric-substring defect fixed on 2026-09-02.
                                                _ev_box.update({
                                                    "bytes": _n_bytes,
                                                    "frames": _n_frames,
                                                    "last_event": _last_ev,
                                                    "stop_reason": _stop,
                                                    "blocks": len((obj or {}).get("content") or []),
                                                    "saw_stop": saw_stop,
                                                    "tail": _tail.decode("utf-8", "replace")[-220:],
                                                })
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
                            if res[0] == "error_body":
                                # HTTP 200 but the "stream" was a JSON error object.
                                # Treat it exactly like a 4xx from this provider:
                                # classify it, log the real text, then fail over to the
                                # next target instead of retrying proxies for it.
                                _, _st, _txt, _u = res
                                try:
                                    err = json.loads(_txt)
                                except Exception:
                                    err = {"type": "error", "error": {"message": _txt[:400]}}
                                _fatal = _request_fatal(400, _txt, tgt)
                                _log(method=request.method, path=path, status=400,
                                     proxy=used_url, attempts=attempts, stream=1,
                                     redactions=redactions,
                                     ms=int((time.time() - t0) * 1000),
                                     note=(f"size refusal in 200-stream ({_fatal})" if _fatal
                                           else "error JSON served as event-stream") + f": {_clip(_txt)}",
                                     ip=client_ip, model=current_model_log,
                                     endpoint=tgt["name"], detail=_txt[:1500])
                                last_err = (400, err, tgt["name"], used_url)
                                _tgt_verdict = (f"size refusal ({_fatal})" if _fatal
                                                else "error JSON as event-stream") + f": {_clip(_txt, 70)}"
                                upstream_rejected = True
                                break
                            if res[0] == "error":
                                _, status, err, _u = res
                                err_full = json.dumps(err, ensure_ascii=False)
                                # SIZE REFUSAL: this provider's verdict on these bytes is
                                # final, so stop spending its proxies/keys. A different
                                # provider may still accept the same payload (tokenizers
                                # differ — see _request_fatal), so we fail over to the
                                # NEXT target rather than returning here.
                                _fatal = _request_fatal(status, err_full, tgt)
                                if _fatal:
                                    _log(method=request.method, path=path, status=status,
                                         proxy=used_url, attempts=attempts, stream=1,
                                         redactions=redactions,
                                         ms=int((time.time() - t0) * 1000),
                                         note=f"size refusal — skipping this endpoint's retries ({_fatal}): {_clip(err_full)}",
                                         ip=client_ip, model=current_model_log,
                                         endpoint=tgt["name"], detail=err_full[:1500])
                                    last_err = (status, err, tgt["name"], used_url)
                                    _tgt_verdict = f"size refusal ({_fatal}): {_clip(err_full, 70)}"
                                    upstream_rejected = True
                                    break
                                # Per-key refusal (quota/credit): retry the identical
                                # request on this provider's next key. Nothing has been
                                # sent to the client yet, so the retry is invisible.
                                if key_idx + 1 < len(ep_keys) and _should_rotate_key(tgt, f"{status} {err_full}"):
                                    key_idx += 1
                                    theaders = _target_headers(request, tgt, ep_keys[key_idx])
                                    _log(method=request.method, path=path, status=status, proxy=used_url,
                                         attempts=attempts, stream=1, redactions=redactions,
                                         ms=int((time.time() - t0) * 1000),
                                         note=f"key {key_idx} refused ({status}), trying key {key_idx + 1}: {err_full[:80]}",
                                         ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                                    attempted_pids = set()
                                    consecutive_5xx = 0
                                    continue
                                _log(method=request.method, path=path, status=status, proxy=used_url,
                                     attempts=attempts, stream=1, redactions=redactions,
                                     ms=int((time.time() - t0) * 1000),
                                     note=f"upstream rejected: {_clip(err_full)}",
                                     ip=client_ip, model=current_model_log,
                                     endpoint=tgt["name"], detail=err_full[:1500])
                                # This provider just condemned itself; remember it so
                                # the next request does not pay the same toll first.
                                _note_endpoint_reject(tgt, err_full)
                                last_err = (status, err, tgt["name"], used_url)
                                _tgt_verdict = f"upstream rejected {status}: {_clip(err_full, 70)}"
                                upstream_rejected = True
                                break  # this target rejected the content — try NEXT target
                            detail = res[1]  # ("retry", reason, used) -> next proxy, same target
                            
                            proxy_kws = [k.strip() for k in tgt.get("failover_trigger_keywords", "500,501,502,503,504,524,401,403,unauthorized").split(",") if k.strip()]
                            ep_kws = [k.strip() for k in tgt.get("endpoint_failover_keywords", "Thinking,model_not_found,invalid_api_key,new_api_error,预扣").split(",") if k.strip()]
                            
                            # WRITE THE EVIDENCE (Ciel, 2026-09-05). `detail` stays
                            # untouched — it is keyword-matched below — so the stream's
                            # vital signs go into the `detail` COLUMN of the log row
                            # only, which nothing classifies on. Now the row answers
                            # "did the upstream go quiet, or did it say something?":
                            #   bytes/frames  — how much actually arrived
                            #   last_event    — where the stream stopped
                            #   stop_reason   — None = cut mid-generation
                            #   tail          — the upstream's own last words
                            _evidence = ""
                            if _ev_box:
                                try:
                                    _evidence = json.dumps(_ev_box, ensure_ascii=False)[:1500]
                                except Exception:
                                    _evidence = str(_ev_box)[:1500]
                            _log(method=request.method, path=path, status=502, proxy=used_url,
                                       attempts=attempts, stream=1, redactions=redactions,
                                       ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}",
                                       ip=client_ip, model=current_model_log, endpoint=tgt["name"],
                                       detail=_evidence)

                            if _match_failover_detail(ep_kws, detail, tgt):
                                last_err = (503, {"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} triggered endpoint failover keyword."}}, tgt["name"], used_url)
                                _tgt_verdict = f"failover keyword: {_clip(detail, 70)}"
                                upstream_rejected = True
                                break
                            elif _match_failover_detail(proxy_kws, detail, tgt) or _proxy_local(detail, tgt):
                                # Transport fault (WAF page, ConnectError, ReadError,
                                # truncated stream). Rotate the exit IP; do NOT hold it
                                # against the provider — see _proxy_local().
                                #
                                # ...UNLESS the same STREAM verdict has now repeated on
                                # several different exit IPs. One proxy cutting a stream
                                # is transport; four unrelated proxies cutting it the
                                # same way is the provider. Keep rotating until the
                                # threshold, then move on instead of burning the rest of
                                # the chain's time (measured cost of not doing this:
                                # 8 attempts / ~300s before provider #3 was tried).
                                # Toggle: fx_stream_fault_is_endpoint.
                                _fc = _fault_class(detail)
                                if _fc and _fx("fx_stream_fault_is_endpoint", tgt):
                                    _stream_faults.setdefault(_fc, set()).add(used_url)
                                    try:
                                        _thr = int(db.get_setting("stream_fault_threshold", "2") or 2)
                                    except (TypeError, ValueError):
                                        _thr = 2
                                    if len(_stream_faults[_fc]) >= max(2, _thr):
                                        _ips = len(_stream_faults[_fc])
                                        _msg = (f"Endpoint {tgt['name']} returned '{_fc}' streams "
                                                f"on {_ips} different exit IPs — treating as an "
                                                f"upstream fault, not a proxy fault.")
                                        _log(method=request.method, path=path, status=502,
                                             proxy=used_url, attempts=attempts, stream=1,
                                             redactions=redactions,
                                             ms=int((time.time() - t0) * 1000),
                                             note=f"endpoint fault: {_msg}",
                                             ip=client_ip, model=current_model_log,
                                             endpoint=tgt["name"],
                                             detail=json.dumps(sorted(_stream_faults[_fc]))[:1500])
                                        _EP_COOLDOWN[tgt["id"]] = time.time() + _ep_cooldown_sec()
                                        last_err = (503, {"error": {"type": "api_error", "message": _msg}}, tgt["name"], used_url)
                                        _tgt_verdict = f"'{_fc}' on {_ips} exit IPs (upstream fault)"
                                        upstream_rejected = True
                                        break
                                pass
                            else:
                                # Default counting for consecutive unknown errors
                                consecutive_5xx += 1
                                if consecutive_5xx >= int(db.get_setting("failover_5xx_threshold", "3")):
                                    last_err = (503, {"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} failed consecutively."}}, tgt["name"], used_url)
                                    _tgt_verdict = f"{consecutive_5xx} consecutive unknown errors: {_clip(detail, 60)}"
                                    upstream_rejected = True
                                    break

                            if not _skip_backoff(detail):
                                await _retry_backoff(attempts)
                        # finished this target (upstream-rejected or all proxies failed) -> next target
                        # Record this provider's verdict for the FINAL row (Ciel,
                        # 2026-09-05). Boss's complaint — "sab fail kyun hua, credit
                        # to tha" — was unanswerable because the client-visible row
                        # named only the LAST provider in the chain (tabitoken 403),
                        # while the 300 seconds actually burned upstream of it left no
                        # trace anywhere except scattered intermediate rows. One
                        # compact chain summary makes the whole cascade readable from
                        # the single row the dashboard shows by default.
                        #
                        # `_tgt_verdict` is set on every break path above; when the
                        # target ran out of proxies instead, say so with the attempt
                        # count — "how many exits did we burn here" is the number that
                        # was missing.
                        if not _tgt_verdict:
                            _n_ips = len(attempted_pids) or 1
                            _tgt_verdict = (f"all {_n_ips} exit IP(s) failed: "
                                            f"{_clip(detail or 'no detail', 70)}")
                        _chain.append(f"{tgt['name']}={_tgt_verdict}")
                    # all targets exhausted
                    if last_err:
                        status, err, tname, purl = last_err
                        # The client-visible row now carries the WHOLE chain, not just
                        # the last provider's verdict. Kept in `detail` (never in the
                        # note) so no classifier can match on it.
                        _cd = json.dumps({"final": err, "chain": _chain}, ensure_ascii=False)[:1500]
                        _log(method=request.method, path=path, status=status, proxy=purl,
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note=f"all targets rejected (last error: {tname} {status}) — chain: {_clip(' | '.join(_chain), 260)}",
                                   ip=client_ip, model=current_model_log, endpoint=tname,
                                   detail=_cd, final=True)
                        logged = True
                        yield _sse("error", err)
                        return
                    # No target ever produced an upstream verdict — every one of them
                    # ran out of exit IPs. Same reasoning as the `last_err` row above:
                    # show the whole chain, not just whichever target happened to be
                    # last, so "kaun kitna time kha gaya" is answerable from one row.
                    _cd2 = json.dumps({"final": str(detail), "chain": _chain}, ensure_ascii=False)[:1500]
                    _log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                               stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                               note=(f"all targets failed (no upstream answered) — chain: "
                                     f"{_clip(' | '.join(_chain), 260)}" if _chain
                                     else f"all targets failed: {detail}"),
                               ip=client_ip, model=current_model_log, endpoint="", detail=_cd2, final=True)
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
            # Same reasoning as the anthropic path: start the byte flow before the
            # first upstream call so a CDN in front of us cannot time out waiting
            # for headers. OpenAI-style clients tolerate a leading comment frame,
            # which is the SSE-standard no-op (":" line), so use that rather than
            # an `event: ping` they might not expect.
            if client_wants_stream and _fx("fx_early_ping", targets[0] if targets else None):
                yield b": keepalive\n\n"
            for tgt in targets:
                current_model_log = tgt.get("model_override") or req_model
                if client_wants_stream:
                    up_body = _mutate_body(body, want_stream=True, model_override=tgt.get("model_override"))
                else:
                    up_body = _mutate_body(body, want_stream=False, model_override=tgt.get("model_override"))
                url = _target_url(tgt["base"], path, tgt["mode"])
                attempted_pids = set()
                consecutive_5xx = 0
                ep_keys = tgt.get("keys") or [tgt.get("key", "")]
                key_idx = 0
                theaders = _target_headers(request, tgt, ep_keys[0])
                for _ in range(max_retries * max(1, len(ep_keys))):
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
                        if _is_waf(r.headers, status=r.status_code, tgt=tgt) or r.status_code >= 500:
                            await r.aclose(); await client.aclose()
                            _mark_used_bad(candidates, used_url, "waf/5xx")
                            # `detail` used to be assigned inside the `if type(...) == int:`
                            # one-liner via a semicolon, so with a dedicated proxy it kept
                            # the previous attempt's text — or was empty on attempt 1.
                            detail = f"{r.status_code} via {used_url}"
                            _log(method=request.method, path=path, status=502, proxy=used_url, attempts=attempts, stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000), note=f"retry failed: {detail}", ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                            if _is_waf(r.headers, status=r.status_code, tgt=tgt): pass
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
                            # Upstream refused. Read a little of the body so the rotation
                            # classifier has something to match — the relay path used to
                            # discard it entirely and only kept the status code.
                            try:
                                body_peek = (await r.aread())[:400].decode("utf-8", "replace")
                            except Exception:
                                body_peek = ""
                            await r.aclose(); await client.aclose()
                            _mark_used_good(candidates, used_url, int((time.time() - t0) * 1000))
                            # STOP-ON-FATAL (raw relay path): a size refusal is this
                            # provider's final verdict on these bytes — no other proxy
                            # or key of the same provider can change it. Break to the
                            # NEXT target instead of burning this one's budget.
                            _fatal = _request_fatal(r.status_code, body_peek, tgt)
                            if _fatal:
                                _log(method=request.method, path=path, status=r.status_code,
                                     proxy=used_url, attempts=attempts, stream=1,
                                     redactions=redactions,
                                     ms=int((time.time() - t0) * 1000),
                                     note=f"size refusal — skipping this endpoint's retries ({_fatal}): {_clip(body_peek)}",
                                     ip=client_ip, model=current_model_log,
                                     endpoint=tgt["name"], detail=body_peek[:1500])
                                detail = f"{tgt['name']} {r.status_code} size refusal"
                                break
                            if key_idx + 1 < len(ep_keys) and _should_rotate_key(tgt, f"{r.status_code} {body_peek}"):
                                key_idx += 1
                                theaders = _target_headers(request, tgt, ep_keys[key_idx])
                                _log(method=request.method, path=path, status=r.status_code, proxy=used_url,
                                     attempts=attempts, stream=1, redactions=redactions,
                                     ms=int((time.time() - t0) * 1000),
                                     note=f"key {key_idx} refused ({r.status_code}), trying key {key_idx + 1}",
                                     ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                                attempted_pids = set()
                                consecutive_5xx = 0
                                continue
                            detail = f"{tgt['name']} {r.status_code}"
                            break
                        _mark_used_good(candidates, used_url, int((time.time() - t0) * 1000))
                        # RAW RELAY, MINUS THE FRAMES THAT CRASH CLIENTS (Ciel,
                        # 2026-09-05). This path forwards upstream bytes verbatim, so
                        # an upstream `data: null` frame reached the client untouched
                        # and every OpenAI-style SDK died on it with
                        #   AttributeError: 'NoneType' object has no attribute 'choices'
                        # — mid-chat, after several good chunks, which is exactly what
                        # made it look random. Measured on this gateway: one null
                        # frame in a 10-frame stream from AgentRouter.
                        #
                        # Everything else stays byte-exact: the separator each frame
                        # arrived with is re-emitted, and any trailing partial frame is
                        # flushed at end-of-stream. When the toggle is off, the old
                        # verbatim loop runs unchanged.
                        if _fx("fx_drop_null_frames", tgt):
                            _relay_buf = b""
                            _dropped = 0
                            async for chunk in r.aiter_raw():
                                forwarded = True
                                _relay_buf += chunk
                                _frames, _relay_buf = _sse_frames(_relay_buf)
                                _out = bytearray()
                                for _body, _sep in _frames:
                                    if _is_null_frame(_body):
                                        _dropped += 1
                                        continue
                                    _out += _body + _sep
                                if _out:
                                    yield bytes(_out)
                            if _relay_buf:
                                # Tail with no terminating blank line. Pass it through
                                # unless it is itself a null frame.
                                if not _is_null_frame(_relay_buf):
                                    yield _relay_buf
                                else:
                                    _dropped += 1
                        else:
                            _dropped = 0
                            async for chunk in r.aiter_raw():
                                forwarded = True
                                yield chunk
                        await r.aclose(); await client.aclose()
                        _log(method=request.method, path=path, status=200, proxy=used_url,
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000),
                                   note="ok(relay)" + (f" [{_dropped} null frame(s) dropped]" if _dropped else ""),
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
                        # This arm is reached only from `except Exception` — i.e. the
                        # request never got an HTTP answer, so it is transport by
                        # definition. Counting it toward `failover_5xx_threshold`
                        # retired healthy providers after 2 unlucky proxies.
                        if _match_failover_detail(fail_kws, detail, tgt) and not _proxy_local(detail, tgt):
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
        tgt_kind = "openai" if tgt["mode"] == "openai" else "anthropic"
        upstream_rejected = False
        attempted_pids = set()
        consecutive_5xx = 0
        # Keys for this provider, primary first. A quota refusal rotates to the next one
        # (same URL, same proxy chain) instead of giving up on the provider.
        ep_keys = tgt.get("keys") or [tgt.get("key", "")]
        key_idx = 0
        theaders = _target_headers(request, tgt, ep_keys[0])
        for _ in range(max_retries * max(1, len(ep_keys))):
            concurrency = int(db.get_setting("hedging_concurrency", "3"))
            candidates = _resolve_candidates(tgt, attempted_pids, concurrency)
            if not candidates:
                break
            p = candidates[0]
            pids_before = len(attempted_pids)
            attempts += 1
            res = await _consume_assemble(candidates, url, theaders, up_body, timeout,
                                          tgt_kind, attempted_pids, tgt)
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
                # Upstream refused (4xx). Before abandoning the provider, see whether
                # this refusal is one a DIFFERENT KEY could satisfy — a quota/credit
                # error is per-key, so rotating is the whole point of extra_keys.
                err_full = payload.decode('utf-8', 'replace') if isinstance(payload, bytes) else str(payload)
                err_text = _clip(err_full)
                # STOP-ON-FATAL (non-stream path): a size refusal is this provider's
                # final verdict on these bytes, so stop burning its keys and proxies.
                # Failover to the NEXT provider still happens (see _request_fatal —
                # tokenizers differ), it just no longer costs 4 wasted proxy rounds.
                _fatal = _request_fatal(status, err_full, tgt)
                if _fatal:
                    _log(method=request.method, path=path, status=status, proxy=used_url,
                         attempts=attempts, stream=0, redactions=redactions,
                         ms=int((time.time() - t0) * 1000),
                         note=f"size refusal — skipping this endpoint's retries ({_fatal}): {err_text}",
                         ip=client_ip, model=current_model_log, endpoint=tgt["name"],
                         detail=err_full[:1500])
                    last = (status, ct, payload, tgt["name"], used_url)
                    upstream_rejected = True
                    break
                if key_idx + 1 < len(ep_keys) and _should_rotate_key(tgt, f"{status} {err_full}"):
                    key_idx += 1
                    theaders = _target_headers(request, tgt, ep_keys[key_idx])
                    _log(method=request.method, path=path, status=status, proxy=used_url,
                         attempts=attempts, stream=0, redactions=redactions,
                         ms=int((time.time() - t0) * 1000),
                         note=f"key {key_idx} refused ({status}), trying key {key_idx + 1}: {err_text}",
                         ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                    # A fresh key deserves a fresh proxy budget; the exit IP was fine.
                    attempted_pids = set()
                    consecutive_5xx = 0
                    continue
                _log(method=request.method, path=path, status=status, proxy=used_url,
                     attempts=attempts, stream=0, redactions=redactions,
                     ms=int((time.time() - t0) * 1000), note=f"upstream rejected: {err_text}",
                     ip=client_ip, model=current_model_log, endpoint=tgt["name"])
                _note_endpoint_reject(tgt, err_full)
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

            if _match_failover_detail(ep_kws, detail, tgt):
                payload = json.dumps({"error": {"type": "api_error", "message": f"Endpoint {tgt['name']} triggered endpoint failover."}}).encode()
                last = (503, "application/json", payload, tgt["name"], used_url)
                upstream_rejected = True
                break
            elif _match_failover_detail(proxy_kws, detail, tgt) or _proxy_local(detail, tgt):
                # Transport fault — rotate the exit IP, don't blame the provider.
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
