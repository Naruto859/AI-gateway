#!/usr/bin/env python3
"""
Geonode Proxy Fetcher
Fetches high-quality free proxies from Geonode's API and adds them to the Gateway.
Filters: Latency <= 500ms, Anonymity = Elite, Protocols = SOCKS5 / HTTPS.
"""
import urllib.request
import json
import os
import sqlite3

GATEWAY_URL = "http://127.0.0.1:8787"
GEONODE_API = "https://proxylist.geonode.com/api/proxy-list?limit=100&page=1&sort_by=latency&sort_type=asc"

def get_admin_token():
    db_path = os.path.join(os.path.dirname(__file__), "data", "data.db")
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT value FROM settings WHERE key='admin_password'")
        row = c.fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception as e:
        print(f"Failed to read admin password: {e}")
        return ""

def api_call(path, data):
    token = get_admin_token()
    req = urllib.request.Request(f"{GATEWAY_URL}{path}", 
                                 data=json.dumps(data).encode("utf-8"),
                                 headers={"Content-Type": "application/json", "x-admin-token": token},
                                 method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        print(f"API Error on {path}: {e}")
        return None

def fetch_geonode_proxies():
    print("Fetching proxies from Geonode API...")
    req = urllib.request.Request(GEONODE_API, headers={"User-Agent": "Mozilla/5.0"})
    proxies_to_add = []
    
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            data = json.loads(res.read().decode('utf-8'))
            items = data.get("data", [])
            
            for p in items:
                # Filter: Only Elite, Latency <= 500, Uptime > 50%
                if p.get("anonymityLevel") != "elite": continue
                if p.get("latency", 9999) > 500: continue
                if p.get("upTime", 0) < 50: continue
                
                ip = p.get("ip")
                port = p.get("port")
                protocols = p.get("protocols", [])
                
                if "socks5" in protocols:
                    proxies_to_add.append(f"socks5://{ip}:{port}")
                elif "https" in protocols:
                    proxies_to_add.append(f"http://{ip}:{port}")
                    
    except Exception as e:
        print(f"Failed to fetch from Geonode: {e}")
        
    return proxies_to_add

def main():
    proxies = fetch_geonode_proxies()
    if not proxies:
        print("No suitable proxies found matching the filters.")
        return
        
    print(f"Found {len(proxies)} high-quality proxies (<=500ms latency, Elite anonymity).")
    
    res = api_call("/admin/proxy/add", {"proxies": "\n".join(proxies)})
    if res is not None:
        print(f"Success! Added {res.get('added', 0)} new proxies to the Gateway.")

if __name__ == "__main__":
    main()
