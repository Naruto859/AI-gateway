"""Proxy pool: selection (round-robin), health, batch-testing, and failover.

Design goals:
  - Use one proxy at a time. If it fails, move to the next.
  - Detect WAF challenges (Aliyun captcha) and mark proxy 'banned'.
  - Health check does NOT spend completion tokens (plain GET).
  - BATCH TEST ENGINE: when 2+ consecutive failures occur, test 10 proxies
    in parallel via fast GET checks (2s timeout) to find working ones.
  - Include banned/unhealthy proxies in batch tests for recovery detection.
  - File lock mechanism to avoid conflict with cron job.
"""
import os
import time
import asyncio
import threading
import logging
import httpx
from . import db

log = logging.getLogger("gateway.proxy_pool")

_rr = {"i": 0}
_rr_lock = threading.Lock()

_RANK = {"ok": 0, "unknown": 1, "unhealthy": 2, "banned": 3}

# Lock file path - cron job creates this while running
LOCK_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "monitor.lock")

# ── Batch test state (in-memory, per-process) ──
_batch_state = {
    "consecutive_fails": 0,        # consecutive proxy failures in request flow
    "ready_queue": [],              # pre-tested working proxy dicts
    "batch_running": False,         # is a batch test currently in progress?
    "last_batch_time": 0,           # timestamp of last batch test
}
_batch_lock = threading.Lock()

# ── Hot Pool state (in-memory verified proxies) ──
_hot_pool = {
    "proxies": [],          # list of verified working proxy dicts
    "last_refresh": 0,      # timestamp of last refresh
    "refreshing": False,    # is a refresh currently running?
    "cycle_tested": 0,      # how many tested in last cycle
    "cycle_passed": 0,      # how many passed in last cycle
    "log": [],              # recent log entries for live view (max 50)
}
_hot_lock = threading.Lock()


def _next_start(n):
    with _rr_lock:
        i = _rr["i"] % n
        _rr["i"] = (_rr["i"] + 1) % n
        return i


def _is_cron_running():
    """Check if the cron monitor job is currently running."""
    return os.path.exists(LOCK_FILE)


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

    BATCH TEST INTEGRATION: if the ready_queue has pre-tested proxies,
    those are placed at the front of the candidate list for minimal latency.

    Ban status is intentionally IGNORED — a WAF challenge is transient (the residential
    IP/cookie resets), so a single bad response must not drop a proxy out of service.
    This is what stops the old "every proxy banned -> pool empty -> 503" bug.
    """
    retries = max(1, max_n)

    # ---- Hot Pool shortcut ----
    hp_enabled = db.get_setting("hot_pool_enabled", "1") == "1"
    if hp_enabled:
        with _hot_lock:
            hp = list(_hot_pool["proxies"])
        if hp:
            # Hot pool proxies first, then fill remaining from DB
            if len(hp) >= retries:
                return hp[:retries]
            # Not enough in hot pool, fill rest from DB
            hp_ids = [p["id"] for p in hp]
            rest = db.get_best_proxies(limit=retries - len(hp), exclude_ids=hp_ids)
            return hp + rest

    pin = db.get_setting("dedicated_proxy_id", "")
    auto = db.get_setting("auto_rotation", "0") == "1"
    pinned = None
    if pin:
        try:
            pin_id = int(pin)
            pinned = db.get_proxy(pin_id)
            if pinned and not pinned["enabled"]:
                pinned = None
        except (TypeError, ValueError):
            pass

    if not auto:
        # ---- single dedicated proxy ----
        if pinned:
            return [pinned] * retries
        best = db.get_best_proxies(limit=1)
        return [best[0]] * retries if best else []

    # ---- auto rotation ON ----
    ready = _pop_ready_proxies()
    ready_ids = [p["id"] for p in ready]
    
    exclude = ready_ids.copy()
    if pinned:
        exclude.append(pinned["id"])
        
    needed = retries - len(ready) - (1 if pinned else 0)
    rest = []
    if needed > 0:
        rest = db.get_best_proxies(limit=needed, exclude_ids=exclude)

    ordered = ([pinned] if pinned else []) + ready + rest
    if ordered:
        return ordered[:retries]
    return db.get_best_proxies(limit=retries)


def _pop_ready_proxies():
    """Pop all pre-tested working proxies from the ready queue."""
    with _batch_lock:
        proxies = list(_batch_state["ready_queue"])
        _batch_state["ready_queue"] = []
        return proxies


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
    # Reset consecutive fail counter on success
    with _batch_lock:
        _batch_state["consecutive_fails"] = 0


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

    # Track consecutive failures and trigger batch test if needed
    with _batch_lock:
        _batch_state["consecutive_fails"] += 1
        consec = _batch_state["consecutive_fails"]
    if consec >= 2:
        _maybe_trigger_batch_test()


def _maybe_trigger_batch_test():
    """Trigger a background batch test if conditions are met."""
    with _batch_lock:
        if _batch_state["batch_running"]:
            return  # already running
        if _is_cron_running():
            log.info("batch-test: skipped (cron job running)")
            return
        # Cooldown: don't batch-test more than once every 10 seconds
        if time.time() - _batch_state["last_batch_time"] < 10:
            return
        _batch_state["batch_running"] = True

    # Fire and forget — runs in the existing event loop
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_run_batch_test())
    except RuntimeError:
        # No running loop (shouldn't happen in FastAPI context)
        with _batch_lock:
            _batch_state["batch_running"] = False


async def _run_batch_test():
    """Test next 10 proxies in parallel using fast GET checks.

    Includes banned/unhealthy proxies for recovery detection.
    Results go into the ready_queue for immediate use by ordered_for_request.
    """
    try:
        endpoint = db.get_setting("endpoint", "https://agentrouter.org")
        candidates = db.get_batch_test_candidates(limit=10)

        if not candidates:
            return

        log.info("batch-test: testing %d proxies (SQL optimized)", len(candidates))

        # Test all candidates in parallel
        tasks = [_fast_check(p, endpoint) for p in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        working = []
        for p, res in zip(candidates, results):
            if isinstance(res, Exception):
                continue
            if res["ok"]:
                working.append(p)
                # Update DB status
                db.update_proxy(p["id"], status="ok",
                                latency_ms=res["latency_ms"],
                                last_checked=time.time(),
                                fail_count=0,
                                enabled=1)  # re-enable if it was disabled
                log.info("batch-test: ✅ %s OK (%dms)", p["url"], res["latency_ms"])
            else:
                db.update_proxy(p["id"], status=res.get("status", "unhealthy"),
                                last_checked=time.time(),
                                note=res.get("detail", "batch_fail"))

        # Sort working by latency (fastest first) and add to ready queue
        working.sort(key=lambda p: p.get("latency_ms", 9999))
        with _batch_lock:
            # Refresh proxy data from DB (status may have been updated)
            refreshed = []
            for p in working:
                fresh = db.get_proxy(p["id"])
                if fresh:
                    refreshed.append(fresh)
            _batch_state["ready_queue"] = refreshed
            _batch_state["consecutive_fails"] = 0  # reset after batch test

        log.info("batch-test: done — %d/%d working, queued for next request",
                 len(working), len(candidates))

    except Exception as exc:
        log.warning("batch-test: error: %s", exc)
    finally:
        with _batch_lock:
            _batch_state["batch_running"] = False
            _batch_state["last_batch_time"] = time.time()


async def _fast_check(proxy_dict, endpoint):
    """Fast pre-flight GET check (2s timeout).

    Tests if the proxy is alive and the endpoint is not WAF-blocked through this IP.
    Does NOT guarantee POST will succeed, but filters ~80-90% of dead/blocked proxies.

    Returns dict(ok, status, latency_ms, detail).
    """
    proxy_url = proxy_dict["url"]
    t0 = time.time()
    try:
        async with httpx.AsyncClient(proxy=proxy_url,
                                     timeout=httpx.Timeout(2.0, connect=2.0, read=2.0)) as client:
            url = endpoint.rstrip("/") + "/v1/messages"
            r = await client.get(url)
            latency = int((time.time() - t0) * 1000)
            if _looks_like_waf(r):
                return {"ok": False, "status": "banned",
                        "latency_ms": latency, "detail": f"WAF (HTTP {r.status_code})"}
            # Any non-WAF response (401/405/400) means endpoint is reachable & clean
            return {"ok": True, "status": "ok",
                    "latency_ms": latency, "detail": f"clean (HTTP {r.status_code})"}
    except Exception as e:
        return {"ok": False, "status": "unhealthy",
                "latency_ms": int((time.time() - t0) * 1000),
                "detail": f"{type(e).__name__}: {e}"}


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


def cleanup_dead_proxies():
    """Hard-delete proxies that have been disabled (enabled=0) for 24+ hours.

    Called by the auto health loop. This is the only place that does hard deletes.
    """
    cutoff = time.time() - 86400  # 24 hours ago
    deleted = db.delete_old_disabled_proxies(cutoff)
    if deleted:
        log.info("cleanup: hard-deleted %d proxies disabled for 24+ hours", deleted)


async def _hot_pool_refresh():
    """Refresh the hot pool: test top candidates, keep best ones."""
    with _hot_lock:
        if _hot_pool["refreshing"]:
            return
        _hot_pool["refreshing"] = True
    
    try:
        pool_size = int(db.get_setting("hot_pool_size", "10"))
        test_count = pool_size * 5  # test 5x pool size to find best
        endpoint = db.get_setting("endpoint", "https://agentrouter.org")
        
        # Get top candidates from DB sorted by latency
        candidates = db.get_hot_pool_candidates(limit=test_count)
        if not candidates:
            _hp_log("⚠️ No candidates found in DB")
            return
        
        _hp_log(f"🔄 Testing {len(candidates)} candidates...")
        
        # Test all in parallel with short timeout
        tasks = [_hot_check(p, endpoint) for p in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        working = []
        for p, res in zip(candidates, results):
            if isinstance(res, Exception):
                continue
            if res["ok"]:
                p["hot_latency"] = res["latency_ms"]
                working.append(p)
        
        # Sort by latency, keep best pool_size
        working.sort(key=lambda p: p.get("hot_latency", 9999))
        best = working[:pool_size]
        
        with _hot_lock:
            _hot_pool["proxies"] = best
            _hot_pool["last_refresh"] = time.time()
            _hot_pool["cycle_tested"] = len(candidates)
            _hot_pool["cycle_passed"] = len(working)
        
        if best:
            fastest = best[0].get("hot_latency", "?")
            slowest = best[-1].get("hot_latency", "?")
            _hp_log(f"✅ Pool refreshed: {len(best)}/{len(candidates)} passed (fastest: {fastest}ms, slowest: {slowest}ms)")
        else:
            _hp_log(f"❌ No working proxies found from {len(candidates)} candidates")
    
    except Exception as exc:
        _hp_log(f"❌ Refresh error: {exc}")
    finally:
        with _hot_lock:
            _hot_pool["refreshing"] = False


def _hp_log(msg):
    """Add a timestamped log entry to hot pool log."""
    import datetime
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    with _hot_lock:
        _hot_pool["log"].append(entry)
        if len(_hot_pool["log"]) > 50:
            _hot_pool["log"] = _hot_pool["log"][-50:]


async def _hot_check(proxy_dict, endpoint):
    """Quick 3-second check for hot pool candidates."""
    proxy_url = proxy_dict["url"]
    t0 = time.time()
    try:
        async with httpx.AsyncClient(proxy=proxy_url,
                                     timeout=httpx.Timeout(3.0, connect=3.0, read=3.0)) as client:
            url = endpoint.rstrip("/") + "/v1/messages"
            r = await client.get(url)
            latency = int((time.time() - t0) * 1000)
            if _looks_like_waf(r):
                return {"ok": False, "latency_ms": latency, "detail": f"WAF ({r.status_code})"}
            return {"ok": True, "latency_ms": latency, "detail": f"clean ({r.status_code})"}
    except Exception as e:
        return {"ok": False, "latency_ms": int((time.time() - t0) * 1000),
                "detail": str(e)[:60]}


async def hot_pool_loop():
    """Background loop that refreshes the hot pool periodically."""
    while True:
        enabled = db.get_setting("hot_pool_enabled", "1") == "1"
        interval = int(db.get_setting("hot_pool_refresh", "120") or 120)
        if enabled:
            await _hot_pool_refresh()
        await asyncio.sleep(interval)


def get_hot_pool_status():
    """Return current hot pool state for the dashboard API."""
    with _hot_lock:
        proxies = []
        for p in _hot_pool["proxies"]:
            proxies.append({
                "id": p.get("id"),
                "url": p.get("url", ""),
                "latency_ms": p.get("hot_latency", p.get("latency_ms", 0)),
                "status": p.get("status", "ok"),
                "exit_ip": p.get("exit_ip", ""),
            })
        return {
            "proxies": proxies,
            "last_refresh": _hot_pool["last_refresh"],
            "refreshing": _hot_pool["refreshing"],
            "cycle_tested": _hot_pool["cycle_tested"],
            "cycle_passed": _hot_pool["cycle_passed"],
            "log": list(_hot_pool["log"]),
        }
