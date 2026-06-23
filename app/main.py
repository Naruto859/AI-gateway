"""FastAPI app: dashboard + admin API + the Anthropic passthrough proxy."""
import os
import time
import re
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from . import db, proxy_pool, forwarder

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC = os.path.join(BASE, "static")

app = FastAPI(title="AI Gateway", docs_url=None, redoc_url=None)

# warm the DB / defaults at import
db.conn()

_PROXY_RE = re.compile(r"^(?:https?://)?(?:[^:@/\s]+:[^@/\s]+@)?[^:@/\s]+:\d+$")


def _require_admin(token):
    pw = db.get_setting("admin_password", "")
    if pw and token != pw:
        raise HTTPException(status_code=401, detail="bad admin token")


def _normalize_proxy(line):
    line = line.strip()
    if not line:
        return None
    if not line.startswith("http://") and not line.startswith("https://"):
        line = "http://" + line
    return line


# ---------------- dashboard ----------------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return FileResponse(os.path.join(STATIC, "dashboard.html"))


@app.get("/app.js")
async def appjs():
    return FileResponse(os.path.join(STATIC, "app.js"), media_type="application/javascript")


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
    raw = payload.get("text", "")
    urls = []
    for line in raw.replace(",", "\n").splitlines():
        u = _normalize_proxy(line)
        if u and _PROXY_RE.match(u):
            urls.append(u)
    added = db.bulk_add(urls)
    return {"added": added, "parsed": len(urls)}


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
