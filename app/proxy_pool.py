"""Proxy pool: selection (round-robin), health, and failover bookkeeping.

Design goals (per user):
  - Use one proxy at a time. If it fails, move to the next — never burn many at once.
  - Detect a WAF challenge (the endpoint serving an Aliyun captcha page) and
    mark that proxy 'banned' so it isn't reused.
  - Health check / Test must NOT spend completion tokens: it does a plain GET to
    the API path and inspects whether a WAF HTML page comes back.
"""
import time
import threading
import httpx
from . import db

_rr = {"i": 0}
_rr_lock = threading.Lock()

_RANK = {"ok": 0, "unknown": 1, "unhealthy": 2}


def _next_start(n):
    with _rr_lock:
        i = _rr["i"] % n
        _rr["i"] = (_rr["i"] + 1) % n
        return i


def selectable():
    return [p for p in db.list_proxies() if p["enabled"] and p["status"] != "banned"]


def ordered_for_request(max_n):
    """Return up to max_n proxies to try, healthiest first, rotated for balance."""
    pool = selectable()
    if not pool:
        return []
    start = _next_start(len(pool))
    rotated = pool[start:] + pool[:start]
    rotated.sort(key=lambda p: _RANK.get(p["status"], 3))
    return rotated[:max_n]


def mark_good(pid, latency_ms=0):
    p = db.get_proxy(pid)
    if not p:
        return
    db.update_proxy(
        pid,
        status="ok",
        fail_count=0,
        success_count=p["success_count"] + 1,
        last_used=time.time(),
        latency_ms=latency_ms or p["latency_ms"],
    )


def mark_bad(pid, reason):
    p = db.get_proxy(pid)
    if not p:
        return
    fc = p["fail_count"] + 1
    if reason == "waf":
        status = "banned"
    elif fc >= 3:
        status = "unhealthy"
    else:
        status = p["status"] if p["status"] != "banned" else "unhealthy"
    db.update_proxy(pid, fail_count=fc, status=status, note=reason, last_used=time.time())


def _looks_like_waf(resp):
    ct = resp.headers.get("content-type", "")
    if "text/html" in ct:
        return True
    if resp.status_code == 305:
        return True
    try:
        if "aliyun_waf" in resp.text[:600]:
            return True
    except Exception:
        pass
    return False


async def health_check(proxy_url, endpoint):
    """Token-free check: confirm proxy works AND the endpoint is not WAF-blocked.

    Returns dict(ok, status, exit_ip, latency_ms, detail).
    """
    res = {"ok": False, "status": "unhealthy", "exit_ip": "", "latency_ms": 0, "detail": ""}
    t0 = time.time()
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=httpx.Timeout(25.0)) as client:
            # 1) exit IP (neutral, no endpoint touch)
            try:
                r_ip = await client.get("https://api.ipify.org")
                res["exit_ip"] = r_ip.text.strip()
            except Exception:
                pass
            # 2) WAF check against the real API path (GET = no completion tokens)
            url = endpoint.rstrip("/") + "/v1/messages"
            r = await client.get(url)
            if _looks_like_waf(r):
                res.update(ok=False, status="banned", detail=f"WAF challenge (HTTP {r.status_code})")
            else:
                # any JSON / 4xx (e.g. 400/401/405) means the endpoint is reachable & clean
                res.update(ok=True, status="ok", detail=f"clean (HTTP {r.status_code})")
    except Exception as e:
        res.update(ok=False, status="unhealthy", detail=f"{type(e).__name__}: {e}")
    res["latency_ms"] = int((time.time() - t0) * 1000)
    return res
