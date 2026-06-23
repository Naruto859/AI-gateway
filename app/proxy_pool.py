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
    """Single dedicated proxy — NO rotation, NO failover (per user).

    All requests go through exactly ONE pinned residential exit IP
    (settings.dedicated_proxy_id) so the endpoint always sees the same stable
    "real user" address. The gateway never switches to a different proxy on
    failure: if the pinned proxy has a transient hiccup the request just errors
    and the client retries through the same IP.

    Ban status is intentionally IGNORED for the pinned proxy: a WAF challenge is
    transient (the residential IP/cookie resets), so a single bad response must
    not drop the dedicated proxy out of service. This is what stops the old
    "every proxy gets permanently banned until the pool is empty -> 503" bug.

    Retry behaviour (like Claude Code): on a transient failure — a WAF HTML page
    or an upstream that cuts the stream incomplete — we retry through the SAME
    pinned proxy, NOT a different one. So we return the pinned proxy repeated
    `max_n` times. forward()'s loop then re-attempts on the identical exit IP;
    the endpoint never sees a switch, and the proxy is never dropped. This is
    what fixes the "single try -> instant 503" problem without rotation.

    If no proxy is pinned, the first enabled proxy is used (still single IP,
    repeated for retries).
    """
    enabled = [p for p in db.list_proxies() if p["enabled"]]
    if not enabled:
        return []

    retries = max(1, max_n)
    pin = db.get_setting("dedicated_proxy_id", "")
    if pin:
        try:
            pin_id = int(pin)
        except (TypeError, ValueError):
            pin_id = None
        pinned = next((p for p in enabled if p["id"] == pin_id), None)
        if pinned:
            return [pinned] * retries

    # no valid pin: use the first enabled proxy (single IP, repeated for retries)
    return [enabled[0]] * retries


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
