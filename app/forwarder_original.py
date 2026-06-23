"""Core reverse proxy.

Forwards Anthropic-format requests (/v1/*) to the configured upstream endpoint
THROUGH a residential proxy, so the upstream's WAF sees a clean residential IP
and never our real server IP.

Key behaviours:
  - Streaming (SSE) and non-streaming both supported, passed through verbatim so
    the Anthropic SDK (tool use, computer use, etc.) works natively.
  - Header passthrough with credential override (the client's key is swapped for
    our stored upstream key; hop-by-hop headers stripped).
  - Per-request proxy selection with transparent failover: if a proxy returns a
    WAF page or dies BEFORE any bytes reach the client, we silently try the next
    proxy. One proxy at a time.
  - Content filter applied before anything leaves the gateway.
"""
import time
import json
import httpx
from starlette.responses import StreamingResponse, Response, JSONResponse
from . import db, proxy_pool, filters

# headers we must not forward upstream
HOP_BY_HOP = {
    "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
    "accept-encoding", "x-admin-token",
}


def _build_upstream_headers(request, upstream_key):
    headers = {}
    for k, v in request.headers.items():
        if k.lower() in HOP_BY_HOP:
            continue
        headers[k] = v
    # clean, consistent identity + our credentials (replicates a working request)
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
    return JSONResponse(
        {"type": "error", "error": {"type": etype, "message": message}},
        status_code=status,
    )


async def forward(request, path):
    s = db.get_all_settings()
    endpoint = s.get("endpoint", "https://agentrouter.org").rstrip("/")
    gateway_key = s.get("gateway_key", "")
    upstream_key = s.get("upstream_key") or gateway_key
    require_key = s.get("require_client_key", "1") == "1"
    max_retries = int(s.get("max_retries", "4") or 4)
    timeout = httpx.Timeout(
        connect=float(s.get("connect_timeout", "20") or 20),
        read=float(s.get("read_timeout", "300") or 300),
        write=60.0,
        pool=20.0,
    )

    # ---- client auth against THIS gateway ----
    if require_key and gateway_key:
        ck = request.headers.get("x-api-key", "")
        auth = request.headers.get("authorization", "")
        if not ck and auth.lower().startswith("bearer "):
            ck = auth[7:]
        if ck != gateway_key:
            return _err("Invalid gateway key.", 401, "authentication_error")

    # ---- content filter ----
    body = await request.body()
    body, blocked_kw, redactions = filters.apply_filters(body)
    if blocked_kw is not None:
        db.add_log(method=request.method, path=path, status=400, proxy="",
                   attempts=0, stream=0, redactions=0, ms=0, note="blocked by content filter")
        return _err("Request blocked by content filter.", 400, "invalid_request_error")

    # ---- stream? ----
    try:
        stream = bool(json.loads(body).get("stream"))
    except Exception:
        stream = "text/event-stream" in request.headers.get("accept", "")

    headers = _build_upstream_headers(request, upstream_key)
    url = f"{endpoint}/{path}"
    candidates = proxy_pool.ordered_for_request(max_retries)
    if not candidates:
        return _err("No usable proxies configured. Add proxies in the dashboard.", 503)

    t0 = time.time()

    # ---------- streaming ----------
    if stream:
        async def gen():
            attempts = 0
            forwarded = False
            detail = ""
            for p in candidates:
                attempts += 1
                client = httpx.AsyncClient(proxy=p["url"], timeout=timeout)
                try:
                    req = client.build_request("POST", url, headers=headers, content=body)
                    r = await client.send(req, stream=True)
                    if _is_waf(r.headers):
                        await r.aclose(); await client.aclose()
                        proxy_pool.mark_bad(p["id"], "waf"); detail = f"WAF via {p['url']}"
                        continue
                    if r.status_code >= 500:
                        await r.aclose(); await client.aclose()
                        proxy_pool.mark_bad(p["id"], "5xx"); detail = f"HTTP {r.status_code} via {p['url']}"
                        continue
                    proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                    async for chunk in r.aiter_raw():
                        forwarded = True
                        yield chunk
                    await r.aclose(); await client.aclose()
                    db.add_log(method=request.method, path=path, status=r.status_code,
                               proxy=p["url"], attempts=attempts, stream=1,
                               redactions=redactions, ms=int((time.time() - t0) * 1000), note="ok")
                    return
                except Exception as e:
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    detail = f"{type(e).__name__} via {p['url']}"
                    if forwarded:
                        # bytes already sent to client; cannot safely restart -> let SDK retry
                        db.add_log(method=request.method, path=path, status=0, proxy=p["url"],
                                   attempts=attempts, stream=1, redactions=redactions,
                                   ms=int((time.time() - t0) * 1000), note=f"mid-stream drop: {detail}")
                        return
                    proxy_pool.mark_bad(p["id"], "conn")
                    continue
            db.add_log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
                       stream=1, redactions=redactions, ms=int((time.time() - t0) * 1000),
                       note=f"all proxies failed: {detail}")
            yield ("event: error\ndata: " +
                   json.dumps({"type": "error",
                               "error": {"type": "api_error",
                                         "message": f"All proxies failed. {detail}"}}) +
                   "\n\n").encode()

        return StreamingResponse(gen(), media_type="text/event-stream")

    # ---------- non-streaming ----------
    attempts = 0
    detail = ""
    for p in candidates:
        attempts += 1
        try:
            async with httpx.AsyncClient(proxy=p["url"], timeout=timeout) as client:
                r = await client.post(url, headers=headers, content=body)
                if _is_waf(r.headers, r.content):
                    proxy_pool.mark_bad(p["id"], "waf"); detail = f"WAF via {p['url']}"
                    continue
                if r.status_code >= 500:
                    proxy_pool.mark_bad(p["id"], "5xx"); detail = f"HTTP {r.status_code} via {p['url']}"
                    continue
                proxy_pool.mark_good(p["id"], int((time.time() - t0) * 1000))
                db.add_log(method=request.method, path=path, status=r.status_code, proxy=p["url"],
                           attempts=attempts, stream=0, redactions=redactions,
                           ms=int((time.time() - t0) * 1000), note="ok")
                return Response(
                    content=r.content,
                    status_code=r.status_code,
                    media_type=r.headers.get("content-type", "application/json"),
                )
        except Exception as e:
            detail = f"{type(e).__name__} via {p['url']}"
            proxy_pool.mark_bad(p["id"], "conn")
            continue

    db.add_log(method=request.method, path=path, status=503, proxy="", attempts=attempts,
               stream=0, redactions=redactions, ms=int((time.time() - t0) * 1000),
               note=f"all proxies failed: {detail}")
    return _err(f"All proxies failed. {detail}", 503)
