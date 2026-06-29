# Mugen Routes

**Intelligent AI gateway with proxy routing, failover, and a real-time dashboard.**

Mugen Routes sits between your AI tools (agents, scripts, apps) and upstream LLM providers. It routes requests through residential proxies, automatically retries on failure, and seamlessly fails over between multiple providers — all invisible to your client.

---

## Features

- **Multi-provider failover** — set a primary endpoint and add fallbacks. If the primary returns an error (content-blocked, rate-limited, 4xx/5xx), the request automatically switches to the next provider
- **Proxy pool** — route through residential proxies with health tracking, auto-rotation, and dedicated pinning
- **WAF bypass** — detects Aliyun/Cloudflare WAF challenges and retries on a clean proxy
- **Buffered streaming** — assembles upstream SSE into a complete response before forwarding, preventing truncation and mid-stream drops
- **Custom API keys** — create `mugen_*` keys for your clients; each is tracked with hit counts
- **Content filters** — keyword redaction or blocking before requests leave the gateway
- **Live dashboard** — real-time stats, proxy management, endpoint management, logs with detail view, and an embedded AI assistant
- **Format translation** — Anthropic ↔ OpenAI format auto-conversion when endpoints use different APIs

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/Naruto859/AI-gateway.git
cd AI-gateway
pip install -r requirements.txt
```

### 2. Configure

Create a `.env` file (optional — you can configure everything from the dashboard):

```env
ADMIN_PASSWORD=your-secure-password
GATEWAY_KEY=your-upstream-api-key
GATEWAY_ENDPOINT=https://your-llm-provider.com
```

Or just start the server and configure from the web dashboard.

### 3. Start

```bash
# Quick start
python -m uvicorn app.main:app --host 0.0.0.0 --port 8787

# Or use the start script (auto-seeds DB from env)
bash start.sh
```

The dashboard is now live at `http://localhost:8787`.

### 4. Connect your tools

Point your AI tools to the gateway:

```bash
# Example: set as your Anthropic base URL
export ANTHROPIC_BASE_URL=http://your-server:8787

# Or use with curl
curl -X POST http://your-server:8787/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_GATEWAY_KEY" \
  -d '{
    "model": "claude-sonnet-4-20250514",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

Any Anthropic SDK-compatible tool works out of the box.

---

## Dashboard Guide

Login at `http://your-server:8787` with your admin password.

| Tab | What it does |
|-----|-------------|
| **Timers** | Configure read/connect timeouts, retry backoff, TCP keepalive |
| **Proxies** | Add/remove/test proxies, pin a dedicated exit IP, toggle auto-rotation |
| **Keys** | Create `mugen_*` API keys for your clients to use |
| **Endpoints** | Add upstream LLM providers, set primary, test, enable/disable |
| **Logs** | Live request log with status, latency, attempts, and full error detail |
| **Filters** | Keyword redaction/blocking before requests leave the gateway |

### Adding proxies

Upload a proxy file or paste them — any format is auto-detected:
- `IP:PORT`
- `user:pass@IP:PORT`
- `IP:PORT:user:pass`
- `http://user:pass@IP:PORT`

### Endpoint failover

1. Add your primary provider in the **Endpoints** tab
2. Click **★ Set primary** on it
3. Add one or more fallback providers
4. When the primary fails, requests automatically route to the next available endpoint

Each new request always starts at the primary. If it fails, the gateway tries each fallback in order.

---

## Architecture

```
Client → Mugen Routes → [Proxy Pool] → Upstream Provider
                ↓              ↓
           Dashboard      Health checks
           (port 8787)    (WAF detection)
```

**Stack:** Python 3.11+ · FastAPI · SQLite · Vanilla JS dashboard

| File | Purpose |
|------|---------|
| `app/main.py` | API routes, admin endpoints |
| `app/forwarder.py` | Core proxy logic, SSE buffering, failover |
| `app/proxy_pool.py` | Proxy selection, health tracking, rotation |
| `app/db.py` | SQLite schema and data access |
| `app/agent.py` | Embedded AI assistant (tool-calling) |
| `app/filters.py` | Content keyword filtering |
| `static/dashboard.html` | Single-page dashboard UI |
| `seed.py` | Database seeder (from env vars or files) |
| `start.sh` | Entrypoint script (seed + start) |

---

## Environment Variables

All optional — configure from the dashboard if you prefer.

| Variable | Default | Description |
|----------|---------|-------------|
| `ADMIN_PASSWORD` | — | Dashboard login password |
| `GATEWAY_KEY` | — | Upstream API key (also sets `gateway_key` for client auth) |
| `GATEWAY_ENDPOINT` | `https://agentrouter.org` | Primary upstream URL |
| `PROXIES` | — | Comma or newline-separated proxy list |
| `DEDICATED_PROXY_IP` | — | Auto-pin this exit IP after seeding |
| `PORT` | `8787` | Server listen port |

---

## API Reference

### Proxy passthrough
- `POST /v1/messages` — Anthropic Messages API (proxied)
- `POST /v1/chat/completions` — OpenAI Chat API (proxied, with format translation)

### Admin API (requires `x-admin-token` header)
- `GET /admin/state` — full system state (settings, proxies, endpoints, logs)
- `GET /admin/logs` — filtered log list (`?source=test&limit=50`)
- `POST /admin/settings` — update settings
- `POST /admin/proxy/add` — add proxies
- `POST /admin/proxy/test` — test a proxy
- `POST /admin/endpoint/add` — add upstream endpoint
- `POST /admin/endpoint/test` — test endpoint with a real request
- `POST /admin/key/create` — create a `mugen_*` API key
- `POST /admin/agent/chat` — embedded AI assistant

---

## License

MIT
