"""Seed the gateway DB with endpoint, key, and the proxy list.

Run from /root/ai-gateway. Pass the key/endpoint via env so nothing secret is
hard-coded in source:

    GATEWAY_KEY=sk-xxx python3 seed.py
    GATEWAY_KEY=sk-xxx GATEWAY_ENDPOINT=https://agentrouter.org python3 seed.py
"""
import os
from app import db


def main():
    endpoint = os.environ.get("GATEWAY_ENDPOINT", "https://agentrouter.org")
    key = os.environ.get("GATEWAY_KEY", "")
    admin = os.environ.get("ADMIN_PASSWORD", "")

    db.conn()
    db.set_setting("endpoint", endpoint)
    if key:
        db.set_setting("gateway_key", key)
        db.set_setting("upstream_key", key)
    if admin:
        db.set_setting("admin_password", admin)

    # Proxy list: from the PROXIES env var (Railway / any host where proxies.txt
    # is gitignored) OR proxies.txt (local dev). Env takes priority so secrets
    # never live in the repo. PROXIES may be newline- or comma-separated.
    raw_lines = []
    env_proxies = os.environ.get("PROXIES", "")
    if env_proxies:
        raw_lines = env_proxies.replace(",", "\n").splitlines()
    else:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt")
        if os.path.exists(path):
            with open(path) as f:
                raw_lines = f.readlines()

    urls = []
    for line in raw_lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("http") and "://" not in line:
            line = "http://" + line
        urls.append(line)

    added = db.bulk_add(urls)
    print(f"endpoint   = {endpoint}")
    print(f"key set    = {bool(key)}")
    print(f"admin set  = {bool(admin)}")
    print(f"proxies in file = {len(urls)}  newly added = {added}")
    print(f"total proxies in db = {len(db.list_proxies())}")


if __name__ == "__main__":
    main()
