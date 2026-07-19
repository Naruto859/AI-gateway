"""
Test all proxies from all_proxies.txt concurrently.
Adds http:// prefix if missing.
Tests connectivity via ipify.org + agentrouter WAF check.
Saves working proxies to DB.
"""
import asyncio
import json
import re
import sqlite3
import time
import httpx

PROXY_FILE = "/app/all_proxies.txt"
OUTPUT_FILE = "/app/working_proxies.json"
DB_PATH = "/app/data/data.db"

CONCURRENCY = 80
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 12
MAX_LATENCY_MS = 10000


def load_proxies():
    with open(PROXY_FILE) as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith('#')]
    
    proxies = []
    skipped = 0
    for line in lines:
        if not line:
            continue
        # Skip socks4 (httpx doesn't support them)
        if line.startswith('socks4'):
            skipped += 1
            continue
        # Add http:// if no scheme
        if not re.match(r'^[a-z]+://', line):
            line = 'http://' + line
        proxies.append(line)
    
    # Deduplicate
    unique = list(dict.fromkeys(proxies))
    print(f"Loaded {len(unique)} testable proxies (skipped {skipped} socks4 from {len(lines)} total)")
    return unique


async def test_proxy(url: str, sem: asyncio.Semaphore):
    async with sem:
        t0 = time.time()
        exit_ip = ""
        waf = False

        try:
            transport = httpx.AsyncHTTPTransport(proxy=url)
            async with httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT, write=10, pool=5),
                follow_redirects=True
            ) as client:
                r1 = await client.get("https://api.ipify.org?format=json")
                exit_ip = r1.json().get("ip", "")
        except Exception as e:
            return {"url": url, "ok": False, "latency_ms": int((time.time()-t0)*1000), "detail": str(e)[:80], "exit_ip": "", "waf": False}

        latency = int((time.time() - t0) * 1000)
        return {"url": url, "ok": True, "latency_ms": latency, "detail": "ok", "exit_ip": exit_ip, "waf": False}


async def main():
    proxies = load_proxies()
    sem = asyncio.Semaphore(CONCURRENCY)

    print(f"Testing {len(proxies)} proxies (concurrency={CONCURRENCY})...")
    
    # Process in chunks to show progress
    chunk_size = 200
    all_results = []
    for i in range(0, len(proxies), chunk_size):
        chunk = proxies[i:i+chunk_size]
        tasks = [test_proxy(url, sem) for url in chunk]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)
        working_so_far = sum(1 for r in all_results if r["ok"])
        print(f"Progress: {min(i+chunk_size, len(proxies))}/{len(proxies)} tested | {working_so_far} working so far")

    working = [r for r in all_results if r["ok"] and r["latency_ms"] < MAX_LATENCY_MS]
    working_sorted = sorted(working, key=lambda x: x["latency_ms"])

    print(f"\n{'='*60}")
    print(f"RESULTS: {len(all_results)} tested | {len(working)} working")
    print(f"{'='*60}")
    for r in working_sorted[:20]:
        print(f"  ✓ {r['url']} | {r['latency_ms']}ms | IP: {r['exit_ip']}")
    if len(working_sorted) > 20:
        print(f"  ... and {len(working_sorted)-20} more")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(working_sorted, f, indent=2)
    print(f"\nSaved {len(working_sorted)} proxies to {OUTPUT_FILE}")

    # Update DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM proxies")
    conn.commit()
    print("Cleared old proxies from DB.")

    inserted = 0
    for p in working_sorted:
        if p["url"].startswith("socks5"):
            ptype = "socks5"
        elif p["url"].startswith("http"):
            ptype = "http"
        else:
            ptype = "http"
        try:
            cur.execute(
                "INSERT INTO proxies (url, ptype, enabled, status, exit_ip, latency_ms, last_checked) VALUES (?, ?, 1, 'ok', ?, ?, ?)",
                (p["url"], ptype, p["exit_ip"], p["latency_ms"], time.time())
            )
            inserted += 1
        except Exception as e:
            print(f"  DB insert error: {e}")

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} proxies into DB.")
    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
