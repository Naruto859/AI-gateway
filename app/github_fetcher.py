import asyncio
import httpx
import re
import time
import logging
from app import db

log = logging.getLogger(__name__)

# The best auto-updating GitHub raw lists for proxies
SOURCES = [
    # Monosans (highly reliable, tested frequently)
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt",
    # TheSpeedX (large lists)
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    # Proxifly
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
]

GEONODE_API = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc"

_FETCH_INTERVAL = 3600  # Fetch every hour

async def fetch_proxies_from_url(client, url):
    try:
        r = await client.get(url, timeout=15.0)
        if r.status_code == 200:
            return r.text.splitlines()
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
    return []

async def fetch_geonode(client):
    try:
        r = await client.get(GEONODE_API, timeout=15.0)
        if r.status_code == 200:
            data = r.json().get("data", [])
            lines = []
            for p in data:
                ip = p.get("ip")
                port = p.get("port")
                protos = p.get("protocols", ["http"])
                proto = protos[0] if protos else "http"
                if proto not in ["http", "https", "socks4", "socks5"]:
                    proto = "http"
                lines.append(f"{proto}://{ip}:{port}")
            return lines
    except Exception as e:
        log.warning(f"Failed to fetch GeoNode: {e}")
    return []

def parse_and_add(raw_lines):
    added = 0
    with db._lock:
        existing_urls = {row[0] for row in db.conn().execute("SELECT url FROM proxies").fetchall()}
        
    new_proxies = []
    for line in raw_lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
            
        # Basic validation of ip:port
        if not re.match(r"^[^:]+:\d+$", line.split("://")[-1]):
            continue
            
        # Determine protocol
        proto = "http"
        if "socks5.txt" in line or line.startswith("socks5"):
            proto = "socks5"
        elif "socks4" in line:
            proto = "socks4"
            
        # Clean up URL
        if "://" not in line:
            url = f"{proto}://{line}"
        else:
            url = line
            
        if url not in existing_urls:
            new_proxies.append((url, "github-auto", proto))
            existing_urls.add(url)
            
    # Insert in batch
    if new_proxies:
        try:
            if new_proxies:
                c = db.conn()
                c.executemany("INSERT INTO proxies (url, label, ptype, enabled, latency_ms) VALUES (?, ?, ?, 1, 9999)", 
                              [(p[0], p[1], p[2]) for p in new_proxies])
                db.conn().commit()
                added = len(new_proxies)
        except Exception as e:
            log.error(f"Failed to batch insert proxies: {e}")
            
    return added

async def auto_fetch_loop():
    """Background loop that fetches proxies periodically."""
    log.info("Starting GitHub Auto-Fetcher loop...")
    while True:
        # Check if auto-fetch is enabled in DB (default to on for now, can be added to UI later)
        enabled = db.get_setting("github_fetcher_enabled", "1") == "1"
        if enabled:
            try:
                log.info("Fetching fresh proxies from GitHub...")
                async with httpx.AsyncClient() as client:
                    tasks = [fetch_proxies_from_url(client, url) for url in SOURCES]
                    tasks.append(fetch_geonode(client))
                    results = await asyncio.gather(*tasks)
                    
                all_raw_lines = []
                for res in results:
                    all_raw_lines.extend(res)
                    
                if all_raw_lines:
                    # Remove duplicates from the fetched list
                    unique_lines = list(set(all_raw_lines))
                    added = parse_and_add(unique_lines)
                    log.info(f"GitHub Auto-Fetcher: Pulled {len(unique_lines)} unique proxies, added {added} new proxies to DB.")
                else:
                    log.warning("GitHub Auto-Fetcher: No proxies found across all sources.")
                    
            except Exception as e:
                log.error(f"GitHub Auto-Fetcher error: {e}")
                
        await asyncio.sleep(_FETCH_INTERVAL)
