#!/usr/bin/env bash
# Start the AI Gateway.  Usage: ./run.sh [--port 8787] [extra uvicorn args]
set -e
cd "$(dirname "$0")"
PORT="${PORT:-8787}"
HOST="${HOST:-0.0.0.0}"
exec uvicorn app.main:app --host "$HOST" --port "$PORT" "$@"
