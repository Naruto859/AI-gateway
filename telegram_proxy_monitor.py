"""
Telegram Proxy Monitor - AI Gateway v7
=======================================
- Monitors Telegram groups/bots for new proxy lists
- Tests each proxy for connectivity
- Removes unhealthy proxies from DB
- Adds working proxies to DB
- Runs every 2 hours via cron

Usage:
  First time: python3 telegram_proxy_monitor.py --setup   (for OTP login)
  Cron:       python3 telegram_proxy_monitor.py
"""

import asyncio
import re
import sqlite3
import time
import sys
import os
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
from telethon import TelegramClient
from telethon.tl.types import Message

# ─── CONFIG ─────────────────────────────────────────────────────────────────
API_ID        = 32469520
API_HASH      = "1e373ebfb4b5d4483fdd9c41bd2974d8"
SESSION_FILE  = "/app/data/tg_session"
DB_PATH       = "/app/data/data.db"
LOG_FILE      = "/app/data/proxy_monitor.log"
PROXY_FILE    = "/app/all_proxies.txt"
OUTPUT_FILE   = "/app/working_proxies.json"
LOCK_FILE     = "/app/data/monitor.lock"

# Telegram entities to monitor (username or numeric ID)
# These will be auto-discovered on first run
MONITOR_TARGETS = [
    "freeproxylists",     # bot/channel - will search for it
    "free_proxy_list",    # group - will search
]

# How many hours back to look for messages
LOOKBACK_HOURS = 12

# Proxy test settings
CONCURRENCY    = 50
CONNECT_TIMEOUT = 3
READ_TIMEOUT   = 5
MAX_LATENCY_MS = 8000

# ─── LOGGING ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ─── PROXY EXTRACTION ───────────────────────────────────────────────────────
PROXY_PATTERN = re.compile(
    r'((?:socks5|socks4h?|https?|socks)://)?'
    r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5})'
)

def extract_proxies(text: str) -> list[str]:
    """Extract all proxy URLs from a text message."""
    if not text:
        return []
    proxies = []
    for line in text.splitlines():
        line = line.strip()
        # Match scheme://ip:port or just ip:port
        m = re.match(
            r'^(socks5h?://|socks4h?://|https?://)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})$',
            line
        )
        if m:
            scheme = m.group(1) or "http://"
            ip = m.group(2)
            port = m.group(3)
            proxies.append(f"{scheme}{ip}:{port}")
    return proxies

# ─── PROXY TESTING ──────────────────────────────────────────────────────────
async def test_proxy(url: str, sem: asyncio.Semaphore) -> dict:
    """Test a single proxy. Returns dict with ok, latency_ms, exit_ip."""
    # Skip socks4 (httpx doesn't support)
    if url.startswith("socks4://"):
        return {"url": url, "ok": False, "latency_ms": 0, "exit_ip": "", "detail": "socks4_skip"}

    async with sem:
        t0 = time.time()
        try:
            transport = httpx.AsyncHTTPTransport(proxy=url)
            async with httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, write=5, pool=5),
                follow_redirects=True
            ) as client:
                # Rotate endpoints to avoid 429 Rate Limiting
                ip_endpoints = [
                    "https://api.ipify.org?format=json",
                    "https://icanhazip.com",
                    "https://ifconfig.me/ip"
                ]
                import random
                random.shuffle(ip_endpoints)
                
                exit_ip = ""
                detail = "unknown_err"
                for endpoint in ip_endpoints:
                    try:
                        r = await client.get(endpoint)
                        if r.status_code == 200:
                            if "json" in endpoint:
                                data = r.json()
                                exit_ip = data.get("ip") or data.get("ip_addr")
                            else:
                                exit_ip = r.text.strip()
                            if exit_ip:
                                break
                        elif r.status_code == 429:
                            detail = "429_rate_limit"
                    except Exception as ex:
                        detail = str(ex)[:40]
                        continue
                
                if not exit_ip:
                    raise Exception(detail)
        except Exception as e:
            return {"url": url, "ok": False, "latency_ms": int((time.time()-t0)*1000),
                    "exit_ip": "", "detail": str(e)[:60]}

        latency = int((time.time() - t0) * 1000)
        return {"url": url, "ok": True, "latency_ms": latency, "exit_ip": exit_ip, "detail": "ok"}


async def test_all_proxies(urls: list[str]) -> list[dict]:
    """Test all proxies concurrently in chunks of 200 to avoid file descriptor limits."""
    sem = asyncio.Semaphore(CONCURRENCY)
    chunk_size = 200
    all_results = []
    
    for i in range(0, len(urls), chunk_size):
        chunk = urls[i:i+chunk_size]
        tasks = [test_proxy(url, sem) for url in chunk]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)
        # Log progress
        log.info(f"  Processed {min(i + chunk_size, len(urls))}/{len(urls)} proxy tests...")
        
    return all_results

# ─── DATABASE ───────────────────────────────────────────────────────────────
def get_db_proxies() -> list[str]:
    """Get all proxy URLs currently in DB."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        rows = cur.execute("SELECT url FROM proxies").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def get_enabled_proxies() -> list[str]:
    """Get only ENABLED proxy URLs from DB (for lightweight health checks)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute("SELECT url FROM proxies WHERE enabled=1").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def recover_disabled_ok_proxies() -> int:
    """Re-enable proxies that are disabled but have 'ok' status.
    
    This fixes the situation where the old cron logic mass-disabled
    good proxies due to temporary network issues during testing.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            "UPDATE proxies SET enabled=1 WHERE enabled=0 AND status='ok'"
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()

def remove_proxy(url: str):
    """Soft-disable a proxy (enabled=0) instead of hard delete.
    
    Hard deletion happens via cleanup_dead_proxies() in the gateway's
    auto health loop after 24+ hours of being disabled.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("UPDATE proxies SET enabled=0, last_checked=? WHERE url = ?", 
                     (time.time(), url))
        conn.commit()
    finally:
        conn.close()

def add_proxy(p: dict):
    """Add a working proxy to DB."""
    ptype = "socks5" if p["url"].startswith("socks5") else "http"
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO proxies (url, ptype, enabled, status, exit_ip, latency_ms, last_checked) "
            "VALUES (?, ?, 1, 'ok', ?, ?, ?)",
            (p["url"], ptype, p["exit_ip"], p["latency_ms"], time.time())
        )
        conn.commit()
    finally:
        conn.close()

def proxy_exists(url: str) -> bool:
    """Check if proxy already in DB."""
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT 1 FROM proxies WHERE url = ?", (url,)).fetchone()
        return row is not None
    finally:
        conn.close()

# ─── BOT INTERACTION ────────────────────────────────────────────────────────
async def request_bot_proxies(client, bot_entity) -> list[str]:
    """Request SOCKS5 and HTTP proxies from FreeProxyFastBot."""
    proxies = []
    
    # ── SOCKS5 Sequence ──
    try:
        log.info("  🤖 Requesting SOCKS5 from Bot...")
        await client.send_message(bot_entity, '✅ Working Proxies')
        await asyncio.sleep(2)
        await client.send_message(bot_entity, '🟣 SOCKS5')
        await asyncio.sleep(2)
        await client.send_message(bot_entity, '🔗 With Prefix')
        
        # Wait for file generation
        await asyncio.sleep(12)
        
        async for msg in client.iter_messages(bot_entity, limit=3):
            if msg.media and hasattr(msg.media, 'document') and msg.media.document:
                doc_bytes = await client.download_media(msg.media, bytes)
                doc_text = doc_bytes.decode('utf-8', errors='ignore')
                extracted = extract_proxies(doc_text)[:1500]
                log.info(f"  🤖 Got SOCKS5 file, keeping top {len(extracted)} proxies")
                proxies.extend(extracted)
                break
    except Exception as e:
        log.error(f"Error in SOCKS5 bot loop: {e}")

    # ── HTTP Sequence ──
    try:
        log.info("  🤖 Requesting HTTP from Bot...")
        await client.send_message(bot_entity, '✅ Working Proxies')
        await asyncio.sleep(2)
        await client.send_message(bot_entity, '🔵 HTTP')
        await asyncio.sleep(2)
        await client.send_message(bot_entity, '🔗 With Prefix')
        
        await asyncio.sleep(12)
        
        async for msg in client.iter_messages(bot_entity, limit=3):
            if msg.media and hasattr(msg.media, 'document') and msg.media.document:
                doc_bytes = await client.download_media(msg.media, bytes)
                doc_text = doc_bytes.decode('utf-8', errors='ignore')
                extracted = extract_proxies(doc_text)[:1500]
                log.info(f"  🤖 Got HTTP file, keeping top {len(extracted)} proxies")
                proxies.extend(extracted)
                break
    except Exception as e:
        log.error(f"Error in HTTP bot loop: {e}")
        
    return proxies


# ─── MAIN MONITOR ───────────────────────────────────────────────────────────
async def run_monitor(setup_mode: bool = False):
    """Main monitoring function."""
    log.info("="*60)
    log.info(f"Telegram Proxy Monitor started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Create lock file to signal gateway's batch test engine
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        log.warning(f"Could not create lock file: {e}")

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

    if setup_mode:
        await client.start()  # Interactive login with OTP
        log.info("✅ Session created! You can now run without --setup")
        await client.disconnect()
        return

    await client.start()
    log.info("✅ Connected to Telegram")

    # ── Step 1: Find dialogs matching our targets ──────────────────────────
    all_new_proxies = set()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    async for dialog in client.iter_dialogs():
        name = (dialog.name or "").lower()
        username = dialog.entity.username.lower() if hasattr(dialog.entity, 'username') and dialog.entity.username else ""
        is_fast_bot = username == "freeproxyfastbot"
        
        explicit_targets = {"frprox", "dailyfreeproxies", "letsgetproxy", "mtprotoproxies", "proxylister", "ps_free_proxy_list", "v2rayconfiglist"}

        # Match groups or bots with "proxy" or "proxies" in name, or FreeProxyFastBot, or explicit targets
        if "proxy" not in name and "proxies" not in name and "v2ray" not in name and not is_fast_bot and username not in explicit_targets:
            continue

        log.info(f"📡 Found matching dialog: '{dialog.name}' (id={dialog.id})")

        # If it is the FreeProxyFastBot, trigger the interactive button sequence
        if is_fast_bot:
            bot_proxies = await request_bot_proxies(client, dialog.entity)
            all_new_proxies.update(bot_proxies)
            continue

        # Get recent messages from groups/channels
        msg_count = 0
        proxy_count = 0
        async for msg in client.iter_messages(dialog.id, limit=200):
            if not isinstance(msg, Message):
                continue
            if msg.date < cutoff:
                break

            proxies = []
            text = msg.text or msg.message or ""
            if text:
                proxies.extend(extract_proxies(text))

            # Download and parse text files if attached
            if msg.media and hasattr(msg.media, 'document') and msg.media.document:
                doc = msg.media.document
                # Only check reasonably sized documents (limit 500KB)
                if "text" in (doc.mime_type or "").lower() or doc.size < 500000:
                    try:
                        doc_bytes = await client.download_media(msg.media, bytes)
                        doc_text = doc_bytes.decode('utf-8', errors='ignore')
                        doc_proxies = extract_proxies(doc_text)[:1500]
                        if doc_proxies:
                            proxies.extend(doc_proxies)
                    except Exception as e:
                        log.error(f"  Failed to download/parse document: {e}")

            if proxies:
                msg_count += 1
                proxy_count += len(proxies)
                all_new_proxies.update(proxies)
                log.info(f"  📨 Message from {msg.date.strftime('%H:%M')}: {len(proxies)} proxies found")

        if msg_count > 0:
            log.info(f"  ✓ {proxy_count} total proxies from '{dialog.name}'")

    await client.disconnect()
    log.info(f"Disconnected from Telegram")

    # ── Step 2: Health check ONLY enabled proxies (not all 5000+) ────────
    enabled_proxies = get_enabled_proxies()
    log.info(f"\n🔍 Health check: {len(enabled_proxies)} enabled proxies (not all DB)")

    if enabled_proxies:
        results = await test_all_proxies(enabled_proxies)
        removed = 0
        for r in results:
            if not r["ok"] or r["latency_ms"] > MAX_LATENCY_MS:
                remove_proxy(r["url"])
                removed += 1
                log.info(f"  ⏸️ Disabled: {r['url']} ({r.get('detail', 'slow')})")
        log.info(f"  Disabled {removed} unhealthy proxies (soft-delete)")
    
    # ── Step 2b: Re-enable disabled proxies that are still 'ok' status ──
    recovered = recover_disabled_ok_proxies()
    if recovered:
        log.info(f"  ♻️ Recovered {recovered} disabled-but-ok proxies")

    # ── Step 3: Test & add new proxies ────────────────────────────────────
    # Filter out already-existing ones
    new_to_test = [p for p in all_new_proxies if not proxy_exists(p)][:1500]
    log.info(f"\n🆕 New proxies to test: {len(new_to_test)} (limited to 1500, from Telegram)")

    added = 0
    if new_to_test:
        results = await test_all_proxies(new_to_test)
        working = [r for r in results if r["ok"] and r["latency_ms"] < MAX_LATENCY_MS]
        working_sorted = sorted(working, key=lambda x: x["latency_ms"])

        for p in working_sorted:
            add_proxy(p)
            added += 1
            log.info(f"  ✅ Added: {p['url']} | {p['latency_ms']}ms | IP: {p['exit_ip']}")

    # Remove lock file
    try:
        os.remove(LOCK_FILE)
    except Exception:
        pass

    # ── Summary ──────────────────────────────────────────────────────────
    final_count = len(get_db_proxies())
    log.info(f"\n{'='*60}")
    log.info(f"✅ Done! New proxies tested: {len(new_to_test)} | Added: {added}")
    log.info(f"📊 Total proxies in DB now: {final_count}")
    log.info(f"{'='*60}\n")


# ─── ENTRY POINT ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    setup = "--setup" in sys.argv
    asyncio.run(run_monitor(setup_mode=setup))
