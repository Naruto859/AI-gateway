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
    """Build the per-request proxy attempt list.

    auto_rotation = "0" (default): ONE dedicated proxy (settings.dedicated_proxy_id),
    repeated max_n times so transient WAF/timeout retries hit the SAME exit IP — the
    endpoint always sees one stable "real user" address, never a switch.

    auto_rotation = "1": pinned proxy first (2 in-place tries), then the rest of the
    enabled pool as failover. When one IP is hard-down/WAF-blocked the request rolls
    to the next IP instead of erroring. Use this with multiple whitelisted IPs.

    Ban status is intentionally IGNORED — a WAF challenge is transient (the residential
    IP/cookie resets), so a single bad response must not drop a proxy out of service.
    This is what stops the old "every proxy banned -> pool empty -> 503" bug.
    """
    enabled = [p for p in db.list_proxies() if p["enabled"]]
    if not enabled:
        return []

    retries = max(1, max_n)
    pin = db.get_setting("dedicated_proxy_id", "")
    auto = db.get_setting("auto_rotation", "0") == "1"
    pinned = None
    if pin:
        try:
            pin_id = int(pin)
        except (TypeError, ValueError):
            pin_id = None
        pinned = next((p for p in enabled if p["id"] == pin_id), None)

    if not auto:
        # ---- single dedicated proxy: same IP repeated for transparent retries ----
        if pinned:
            return [pinned] * retries
        return [enabled[0]] * retries

    # ---- auto rotation ON: pinned first, then the rest as failover ----
    # Each distinct proxy is still retried a couple of times before moving on, so a
    # transient WAF/timeout on the current IP recovers in place; a hard failure rolls
    # to the next IP. Order: pinned (if any) -> health-sorted rest.
    rest = [p for p in enabled if not (pinned and p["id"] == pinned["id"])]
    rest.sort(key=lambda p: _RANK.get(p["status"], 3))
    ordered = ([pinned] if pinned else []) + rest
    # build the attempt list: 2 in-place tries per proxy, capped at `retries`
    seq = []
    for p in ordered:
        seq.append(p); seq.append(p)
        if len(seq) >= retries:
            break
    return seq[:retries] if seq else [enabled[0]] * retries


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
