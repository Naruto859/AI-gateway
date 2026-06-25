#!/usr/bin/env bash
# Railway / container entrypoint.
#   1. seed the DB from env (endpoint, keys, admin password, proxies)
#   2. start uvicorn on 0.0.0.0:$PORT (Railway injects $PORT)
#
# NOTE: on Railway the dashboard is public, so the gateway is protected by:
#   - admin_password  -> guards the /admin/* panel
#   - gateway_key      -> clients must present it (require_client_key=1)
# Set both via Railway variables; nothing secret lives in the repo.
set -e
cd "$(dirname "$0")"

echo "[start] seeding DB from environment…"
python3 seed.py || echo "[start] seed skipped/failed (continuing)"

PORT="${PORT:-8787}"
echo "[start] launching uvicorn on 0.0.0.0:${PORT}"
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
