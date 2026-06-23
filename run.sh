#!/usr/bin/env bash
# Start the AI Gateway.  Usage: ./run.sh [--port 8787] [extra uvicorn args]
set -e
cd "$(dirname "$0")"
PORT="${PORT:-8787}"
# Bind loopback by default: Caddy fronts us, and exposing the admin panel
# (plaintext password) on all interfaces would be a security hole.
HOST="${HOST:-127.0.0.1}"
exec uvicorn app.main:app --host "$HOST" --port "$PORT" "$@"
