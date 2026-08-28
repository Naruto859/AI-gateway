"""FastAPI app: dashboard + admin API + the Anthropic passthrough proxy."""
import os
import time
import re
import json
import urllib.parse
import asyncio
import logging
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from . import db, proxy_pool, forwarder, agent, github_fetcher, hedger

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC = os.path.join(BASE, "static")
log = logging.getLogger("gateway")

app = FastAPI(title="AI Gateway", docs_url=None, redoc_url=None)


async def _auto_health_loop():
    """Check all enabled proxies every hour, update status/latency automatically."""
    while True:
        await asyncio.sleep(3600)
        try:
            proxies = db.list_proxies()
            endpoint = db.get_setting("endpoint", "https://agentrouter.org")
            for p in proxies:
                if not p.get("enabled"):
                    continue
                try:
                    res = await proxy_pool.health_check(p["url"], endpoint)
                    db.update_proxy(p["id"], status=res["status"],
                                    exit_ip=res.get("exit_ip", ""),
                                    latency_ms=res.get("latency_ms", 0),
                                    last_checked=time.time(),
                                    note=res.get("detail", ""))
                except Exception:
                    pass
            log.info("auto health check done: %d proxies scanned", len(proxies))
            # Hard-delete proxies that have been soft-disabled for 24+ hours
            proxy_pool.cleanup_dead_proxies()
        except Exception as exc:
            log.warning("auto health check error: %s", exc)


_background_tasks = set()

@app.on_event("startup")
async def _startup():
    t1 = asyncio.create_task(_auto_health_loop())
    t2 = asyncio.create_task(proxy_pool.hot_pool_loop())
    t4 = asyncio.create_task(proxy_pool.background_proxy_checker_loop())
    t5 = asyncio.create_task(proxy_pool.background_cleanup_loop())
    asyncio.create_task(hedger.start_server())
    t3 = asyncio.create_task(github_fetcher.auto_fetch_loop())
    _background_tasks.update([t1, t2, t3])

# warm the DB / defaults at import
db.conn()

_PROXY_RE = re.compile(r"^(?:(?:https?|socks[45]h?)://)?(?:[^:@/\s]+:[^@/\s]+@)?[^:@/\s]+:\d+$")

_SCHEME = {"http": "http", "https": "https", "socks5": "socks5", "socks4": "socks4"}


def _require_admin(token):
    pw = db.get_setting("admin_password", "")
    if not pw or token != pw:
        raise HTTPException(status_code=401, detail="bad admin token")


# Addresses no exit proxy can ever have: loopback, link-local, RFC1918 private
# ranges, the unspecified address, and port 0. The live pool had 0.0.0.0:80 sitting
# at status "ok", so it got raced on real requests and burned an attempt every time.
_UNROUTABLE_HOSTS = re.compile(
    r"^(?:0\.0\.0\.0|127\.\d+\.\d+\.\d+|localhost|169\.254\.\d+\.\d+"
    r"|10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+"
    r"|172\.(?:1[6-9]|2\d|3[01])\.\d+\.\d+"
    r"|\[?::1?\]?)$", re.I)


def is_routable_proxy(url):
    """False for proxies that can never reach the internet from this host."""
    try:
        pr = urllib.parse.urlparse(url if "://" in url else "http://" + url)
        host, port = pr.hostname or "", pr.port
    except Exception:
        return False
    if not host or _UNROUTABLE_HOSTS.match(host):
        return False
    if port is not None and not (0 < port < 65536):
        return False
    return True


def _normalize_proxy(line):
    """Accept the common proxy text formats and return a canonical proxy URL:
      1. IP:PORT                         -> http://IP:PORT
      2. scheme://[user:pass@]IP:PORT    -> unchanged
      3. user:pass@IP:PORT               -> http://user:pass@IP:PORT
      4. IP:PORT:user:pass               -> http://user:pass@IP:PORT
    Returns None for blanks/comments."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    # already has a scheme -> leave as-is (covers format 2)
    if "://" in line:
        return line
    # already has credentials inline (format 3) -> just add scheme
    if "@" in line:
        return "http://" + line
    parts = line.split(":")
    # format 4: host:port:user:pass  -> reorder into user:pass@host:port
    if len(parts) == 4 and parts[1].isdigit():
        host, port, user, pw = parts
        return f"http://{user}:{pw}@{host}:{port}"
    # format 1: host:port
    return "http://" + line


def _build_proxy_url(ptype, host, port, user, pw):
    """Compose a proxy URL from structured fields. socks5 -> socks5h (remote DNS)."""
    scheme = _SCHEME.get((ptype or "http").lower(), "http")
    if scheme == "socks5":
        scheme = "socks5h"   # resolve DNS through the proxy
    auth = ""
    if user:
        auth = f"{user}:{pw}@" if pw else f"{user}@"
    return f"{scheme}://{auth}{host}:{port}"


# ---------------- dashboard ----------------
_NOCACHE = {"Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache", "Expires": "0"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    # never let the browser cache the dashboard — stale HTML/JS was breaking login
    return FileResponse(os.path.join(STATIC, "dashboard.html"), headers=_NOCACHE)


@app.get("/app.js")
async def appjs():
    return FileResponse(os.path.join(STATIC, "app.js"),
                        media_type="application/javascript", headers=_NOCACHE)


@app.get("/health")
async def health():
    return {"ok": True}


# ---------------- admin API ----------------
@app.post("/admin/login")
async def login(payload: dict):
    pw = db.get_setting("admin_password", "")
    ok = (payload.get("password", "") == pw) or not pw
    return {"ok": ok}


@app.get("/admin/state")
async def state(x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    s = db.get_all_settings()
    # never echo the admin password back to the browser
    safe = {k: v for k, v in s.items() if k != "admin_password"}
    proxies = db.list_proxies(limit=200)
    counts = db.proxy_counts()
    return {
        "settings": safe,
        "proxies": proxies,
        "counts": counts,
        "keywords": db.list_keywords(),
        "endpoints": db.list_endpoints(),
        "api_keys": db.list_api_keys(),
        "stats": db.stats(),
        "logs": db.recent_logs(80),
    }


@app.get("/admin/hotpool")
async def hotpool_status(x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    return proxy_pool.get_hot_pool_status()


@app.get("/admin/proxies")
async def proxies_page(x_admin_token: str = Header(default=""),
                       q: str = "", status: str = "", limit: int = 25, offset: int = 0):
    """Paged/filtered proxy list. /admin/state only ships the first 200 rows, so the
    Proxies tab uses this to search and page across the full table."""
    _require_admin(x_admin_token)
    res = db.search_proxies(q=q.strip(), limit=limit, offset=offset, status=status.strip())
    res["counts"] = db.proxy_counts()
    res["limit"] = max(1, min(int(limit), 500))
    res["offset"] = max(0, int(offset))
    return res


@app.post("/admin/settings")
async def upd_settings(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    for k, v in payload.items():
        db.set_setting(k, v)
    return {"ok": True}


@app.post("/admin/proxy/add")
async def proxy_add(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    # structured single add (type/host/port/user/pass) takes priority if host given
    host = (payload.get("host") or "").strip()
    if host:
        url = _build_proxy_url(payload.get("ptype", "http"), host,
                               str(payload.get("port", "")).strip(),
                               (payload.get("user") or "").strip(),
                               (payload.get("password") or "").strip())
        if not _PROXY_RE.match(url):
            raise HTTPException(400, "invalid proxy fields")
        if not is_routable_proxy(url):
            raise HTTPException(400, "that address can never reach the internet "
                                     "(loopback/private/0.0.0.0)")
        added = db.add_proxy(url, ptype=(payload.get("ptype", "http") or "http").lower())
        return {"added": added, "url": url.split("@")[-1]}
    # bulk paste (one per line / comma) — type inferred from scheme
    raw = payload.get("text", "")
    urls = []
    skipped = 0
    for line in raw.replace(",", "\n").splitlines():
        u = _normalize_proxy(line)
        if not u or not _PROXY_RE.match(u):
            continue
        # A pasted list routinely carries 0.0.0.0 / LAN addresses. Letting them in
        # means they get raced on real requests and burn an attempt each time.
        if not is_routable_proxy(u):
            skipped += 1
            continue
        urls.append(u)
    added = db.bulk_add(urls)
    return {"added": added, "parsed": len(urls), "skipped_unroutable": skipped}


# ---------------- endpoints (multi-provider failover) ----------------
@app.post("/admin/endpoint/add")
async def endpoint_add(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    url = (payload.get("url") or "").strip().rstrip("/")
    if not url.startswith("http"):
        raise HTTPException(400, "endpoint must be an http(s) URL")
    mode = payload.get("api_mode", "anthropic_messages")
    added = db.add_endpoint(url, mode, payload.get("api_key", ""), payload.get("model_override", ""))
    return {"added": added}


@app.post("/admin/endpoint/update")
async def endpoint_update(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    eid = payload.pop("id")
    db.update_endpoint(eid, **payload)
    return {"ok": True}


@app.post("/admin/endpoint/delete")
async def endpoint_delete(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    db.delete_endpoint(payload["id"])
    return {"ok": True}




@app.post("/admin/endpoint/primary")
async def endpoint_primary(payload: dict, x_admin_token: str = Header(default="")):
    """Set one endpoint as primary (id=0 -> agentrouter/global endpoint setting)."""
    _require_admin(x_admin_token)
    db.set_primary_endpoint(int(payload.get("id", 0)))
    return {"ok": True}


@app.post("/admin/endpoint/reorder")
async def endpoint_reorder(payload: dict, x_admin_token: str = Header(default="")):
    """Persist the full endpoint try-order. Body: {"ids": [12, 11, 7, ...]}.

    ids[0] is tried first. The first enabled id also becomes primary so the
    dashboard's star can never disagree with the real routing order.
    """
    _require_admin(x_admin_token)
    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        raise HTTPException(400, "ids must be a non-empty list of endpoint ids")
    order = db.reorder_endpoints(ids)
    return {"ok": True, "order": order}


@app.get("/admin/routing")
async def routing_view(x_admin_token: str = Header(default=""), window: int = 86400):
    """The routing-transparency payload: exact try-order + per-endpoint outcomes.

    `order` is the same list `forwarder._targets()` walks, so what the operator
    sees here is literally what the router does — position 1 gets the request
    first, position 2 only if position 1 gives up.
    """
    _require_admin(x_admin_token)
    window = max(300, min(int(window or 86400), 604800))
    stats = db.endpoint_stats(window)
    eps = db.list_endpoints()
    order = []
    pos = 0
    for e in sorted(eps, key=lambda x: (x.get("priority", 0), x.get("id", 0))):
        # logs are keyed by the host form (forwarder._targets builds `name` that
        # way), so stats must be looked up by host even when the operator has
        # given the endpoint a friendly label.
        host = e["url"].replace("https://", "").replace("http://", "")
        label = e.get("name") or host
        s = stats.get(host, {})
        enabled = bool(e.get("enabled"))
        if enabled:
            pos += 1
        try:
            prio = json.loads(e.get("proxy_priority") or "[]")
        except (TypeError, ValueError):
            prio = []
        order.append({
            "id": e.get("id"),
            "name": label,
            "host": host,
            "url": e["url"],
            "enabled": enabled,
            "is_primary": bool(e.get("is_primary")),
            "priority": e.get("priority", 0),
            "position": pos if enabled else None,
            "api_mode": e.get("api_mode"),
            "model_override": e.get("model_override") or "",
            "proxy_chain": prio,
            "proxy_fallback": e.get("proxy_fallback", 1),
            "served": s.get("served", 0),
            "failed": s.get("failed", 0),
            "success_pct": s.get("success_pct", 0.0),
            "avg_ms": s.get("avg_ms", 0),
            "median_ms": s.get("median_ms", 0),
            "last_ts": s.get("last_ts", 0),
        })
    return {"order": order, "window_sec": window,
            "active_count": sum(1 for o in order if o["enabled"]),
            # Dedicated proxies have no row in the proxies table, so a dead phone is
            # otherwise invisible in the UI — the operator only saw slow requests.
            "dedicated_cooldown": forwarder.dedicated_cooldown_state()}


@app.post("/admin/endpoint/test")
async def endpoint_test(payload: dict, x_admin_token: str = Header(default="")):
    """Send ONE tiny chat message through a specific endpoint to verify it works.
    Goes through the proxy pool just like a real request. Token-cheap (max_tokens small)."""
    _require_admin(x_admin_token)
    url = (payload.get("url") or "").strip().rstrip("/")
    mode = payload.get("api_mode", "anthropic_messages")
    key = payload.get("api_key", "") or db.get_setting("upstream_key") or db.get_setting("gateway_key", "")
    model = payload.get("model") or db.get_setting("model_note", "claude-sonnet-4-6")
    msg = payload.get("message", "Reply with exactly: OK")
    history = payload.get("history")
    if history is not None and not isinstance(history, list):
        history = None
    # Chat asks for a real answer; a health probe only needs a token or two.
    max_tokens = 1024 if history else 20
    if not url.startswith("http"):
        raise HTTPException(400, "endpoint must be an http(s) URL")
    res = await forwarder.test_endpoint(url, mode, key, model, msg,
                                        history=history, max_tokens=max_tokens)
    return res


# ---------------- api keys (client-facing) ----------------
def _gen_key():
    import secrets
    return "mugen_" + secrets.token_urlsafe(12).replace("-", "").replace("_", "")[:14]


@app.post("/admin/key/create")
async def key_create(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    name = _gen_key()
    db.add_api_key(name, name, (payload.get("label") or "").strip())
    return {"ok": True, "key": name}


@app.post("/admin/key/delete")
async def key_delete(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    db.delete_api_key(payload["id"])
    return {"ok": True}


# ---------------- embedded dashboard agent ----------------
@app.post("/admin/agent/chat")
async def agent_chat(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    messages = payload.get("messages", [])
    cfg = payload.get("config") or {}
    if not messages:
        raise HTTPException(400, "no messages")
    t0 = time.time()
    try:
        res = await agent.chat(messages, cfg)
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        brain = "custom" if (cfg.get("mode") == "custom" and cfg.get("endpoint")) else "default(gateway)"
        err_msg = f"{type(e).__name__}: {e}"
        db.add_log(method="POST", path="agent/chat", status=500, proxy="", attempts=1,
                   stream=0, redactions=0, ms=ms, note=f"agent error: {err_msg[:60]}",
                   model=cfg.get("model", ""), endpoint=brain, source="agent",
                   detail=err_msg[:1500])
        return {"reply": f"Error: {err_msg[:100]}", "actions": []}
    ms = int((time.time() - t0) * 1000)
    brain = "custom" if (cfg.get("mode") == "custom" and cfg.get("endpoint")) else "default(gateway)"
    acts = ",".join(a.get("tool", "") for a in (res.get("actions") or [])) or "chat"
    status = 200 if not (res.get("reply", "").startswith("Brain error")) else 502
    db.add_log(method="POST", path="agent/chat", status=status, proxy="", attempts=1,
               stream=0, redactions=0, ms=ms, note=f"agent: {acts}",
               model=cfg.get("model", ""), endpoint=brain, source="agent",
               detail=(res.get("reply", "") if status >= 400 else "")[:1500])
    return res


@app.post("/admin/proxy/toggle")
async def proxy_toggle(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    p = db.get_proxy(payload["id"])
    if not p:
        raise HTTPException(404)
    db.update_proxy(p["id"], enabled=0 if p["enabled"] else 1)
    return {"ok": True}


@app.post("/admin/proxy/delete")
async def proxy_delete(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    db.delete_proxy(payload["id"])
    return {"ok": True}


@app.post("/admin/proxy/reset")
async def proxy_reset(payload: dict, x_admin_token: str = Header(default="")):
    """Clear a 'banned'/'unhealthy' flag so it can be retried."""
    _require_admin(x_admin_token)
    db.update_proxy(payload["id"], status="unknown", fail_count=0, note="")
    return {"ok": True}


@app.post("/admin/proxy/test")
async def proxy_test(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    endpoint = db.get_setting("endpoint", "https://agentrouter.org")
    url = payload.get("url")
    if not url and payload.get("id"):
        p = db.get_proxy(payload["id"])
        url = p["url"] if p else None
    if not url:
        raise HTTPException(400, "no proxy url")
    res = await proxy_pool.health_check(url, endpoint)
    if payload.get("id"):
        db.update_proxy(payload["id"], status=res["status"], exit_ip=res["exit_ip"],
                        latency_ms=res["latency_ms"], last_checked=time.time(),
                        note=res["detail"])
    return res


@app.post("/admin/keyword/add")
async def kw_add(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    kid = db.add_keyword(payload["value"], payload.get("mode", "redact"),
                         payload.get("replacement", "[REDACTED]"))
    return {"id": kid}


@app.post("/admin/keyword/delete")
async def kw_del(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    db.delete_keyword(payload["id"])
    return {"ok": True}


@app.post("/admin/keyword/toggle")
async def kw_toggle(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    ks = {k["id"]: k for k in db.list_keywords()}
    k = ks.get(payload["id"])
    if not k:
        raise HTTPException(404)
    db.update_keyword(k["id"], enabled=0 if k["enabled"] else 1)
    return {"ok": True}


@app.post("/admin/logs/clear")
async def logs_clear(x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    db.clear_logs()
    return {"ok": True}


@app.get("/admin/logs")
async def get_logs(x_admin_token: str = Header(default=""),
                   source: str = "", limit: int = 100, offset: int = 0):
    _require_admin(x_admin_token)
    logs = db.recent_logs(min(limit, 500))
    if source:
        logs = [l for l in logs if l.get("source", "") == source]
    return {"logs": logs[offset:offset+limit], "total": len(logs)}


@app.get("/admin/logs/tags")
async def get_log_tags(x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    return {"tags": ["", "test", "agent"]}


# ---------------- the proxy passthrough ----------------
@app.api_route("/v1/{path:path}",
               methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_v1(path: str, request: Request):
    return await forwarder.forward(request, f"v1/{path}")
