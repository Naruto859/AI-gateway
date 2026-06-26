"""FastAPI app: dashboard + admin API + the Anthropic passthrough proxy."""
import os
import time
import re
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from . import db, proxy_pool, forwarder, agent

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC = os.path.join(BASE, "static")

app = FastAPI(title="AI Gateway", docs_url=None, redoc_url=None)

# warm the DB / defaults at import
db.conn()

_PROXY_RE = re.compile(r"^(?:(?:https?|socks[45]h?)://)?(?:[^:@/\s]+:[^@/\s]+@)?[^:@/\s]+:\d+$")

_SCHEME = {"http": "http", "https": "https", "socks5": "socks5", "socks4": "socks4"}


def _require_admin(token):
    pw = db.get_setting("admin_password", "")
    if pw and token != pw:
        raise HTTPException(status_code=401, detail="bad admin token")


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
    proxies = db.list_proxies()
    counts = {"total": len(proxies),
              "ok": sum(1 for p in proxies if p["status"] == "ok"),
              "banned": sum(1 for p in proxies if p["status"] == "banned"),
              "unhealthy": sum(1 for p in proxies if p["status"] == "unhealthy"),
              "unknown": sum(1 for p in proxies if p["status"] == "unknown")}
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
        added = db.add_proxy(url, ptype=(payload.get("ptype", "http") or "http").lower())
        return {"added": added, "url": url.split("@")[-1]}
    # bulk paste (one per line / comma) — type inferred from scheme
    raw = payload.get("text", "")
    urls = []
    for line in raw.replace(",", "\n").splitlines():
        u = _normalize_proxy(line)
        if u and _PROXY_RE.match(u):
            urls.append(u)
    added = db.bulk_add(urls)
    return {"added": added, "parsed": len(urls)}


# ---------------- endpoints (multi-provider failover) ----------------
@app.post("/admin/endpoint/add")
async def endpoint_add(payload: dict, x_admin_token: str = Header(default="")):
    _require_admin(x_admin_token)
    url = (payload.get("url") or "").strip().rstrip("/")
    if not url.startswith("http"):
        raise HTTPException(400, "endpoint must be an http(s) URL")
    mode = payload.get("api_mode", "anthropic_messages")
    added = db.add_endpoint(url, mode, payload.get("api_key", ""))
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


@app.post("/admin/endpoint/update")
async def endpoint_update2(payload: dict, x_admin_token: str = Header(default="")):
    """Toggle enabled, set name/key/mode, set priority for one endpoint."""
    _require_admin(x_admin_token)
    eid = payload.pop("id")
    db.update_endpoint(eid, **payload)
    return {"ok": True}


@app.post("/admin/endpoint/primary")
async def endpoint_primary(payload: dict, x_admin_token: str = Header(default="")):
    """Set one endpoint as primary (id=0 -> agentrouter/global endpoint setting)."""
    _require_admin(x_admin_token)
    db.set_primary_endpoint(int(payload.get("id", 0)))
    return {"ok": True}


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
    if not url.startswith("http"):
        raise HTTPException(400, "endpoint must be an http(s) URL")
    res = await forwarder.test_endpoint(url, mode, key, model, msg)
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
    res = await agent.chat(messages, cfg)
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


# ---------------- the proxy passthrough ----------------
@app.api_route("/v1/{path:path}",
               methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_v1(path: str, request: Request):
    return await forwarder.forward(request, f"v1/{path}")
