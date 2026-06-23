# AI Gateway

A configurable **Anthropic-format reverse proxy** that sits between your AI agents
(Hermes Agent, Claude Code, any Anthropic-SDK client) and a restrictive LLM
endpoint, routing every request through a **residential proxy** so it bypasses the
endpoint's WAF — and keeping the connection alive on long-context requests so
agents never crash mid-task.

FastAPI backend + a Claude-style dark Tailwind dashboard. State (endpoint, keys,
proxies, filters, logs) lives in a local SQLite DB. **No secrets are committed to
this repo** — see [Security](#security).

---

## Why this exists (root cause)

The target endpoint (e.g. `https://agentrouter.org`, Anthropic Messages format)
sits behind an **Aliyun WAF**. The WAF behaves differently by source IP:

- **Residential / home IP** (e.g. where Claude Code runs on the user's laptop) →
  requests pass, real model JSON comes back. This is why Claude Code "just works".
- **Datacenter / VPS IP** (e.g. where Hermes Agent runs on a server) → the WAF
  serves a **slider-CAPTCHA HTML page**: HTTP `200`, `Content-Type: text/html`,
  sets an `acw_tc` cookie. The agent gets HTML instead of JSON → garbage →
  errors like `HTTP 305 / Service Unavailable`, JSON parse failures, crashes.

Things that do **not** bypass it: changing User-Agent, replaying cookies, retries.
The only reliable fix is sending the request from a **clean residential IP**.

**Proven fix:** route the identical request through a residential proxy
(`http://user:pass@ip:port`). Both non-streaming JSON and streaming SSE then work
end-to-end with the real model.

## The second problem: long-context crashes

Agents like Hermes would crash on big/long-running requests even after the WAF was
solved. Root cause: a **non-streaming** HTTP request to the upstream sits idle
while the model "thinks", and the connection gets dropped (~10 min idle). Claude
Code avoids this because it **always streams** — the steady `ping`/`delta` events
keep the socket warm, and its SDK auto-retries.

**The "always-stream bridge" (this gateway's core trick):** the gateway **always
streams from the upstream**, even when the downstream client asked for a
non-streaming response. For a streaming client it relays the SSE as-is. For a
non-streaming client it consumes the upstream stream, assembles the final JSON
body, and returns it once complete. Either way the upstream socket is never idle,
so long-context requests don't drop. This is implemented in
[`app/forwarder.py`](app/forwarder.py).

---

## Architecture

```
  AI agent (Hermes / Claude Code / Anthropic SDK)
        │  ANTHROPIC_BASE_URL = http://<gateway-host>:<port>
        │  x-api-key: <gateway key>
        ▼
  ┌──────────────────────── AI Gateway (FastAPI) ───────────────────────┐
  │  main.py        routes: /v1/* catch-all + admin API + dashboard      │
  │  filters.py     keyword redact / block on system + messages          │
  │  proxy_pool.py  pick ONE residential proxy, health-check, failover   │
  │  forwarder.py   always-stream bridge → upstream, assemble if needed  │
  │  db.py          SQLite: settings, proxies, keywords, logs            │
  └──────────────────────────────────────────────────────────────────────┘
        │  via residential proxy  (http://user:pass@ip:port)
        ▼
  Upstream LLM endpoint behind Aliyun WAF  (Anthropic Messages API)
```

### Request flow
1. Client hits the gateway at `/v1/messages` (Anthropic) with the **gateway key**.
2. Gateway optionally enforces a client key, runs content filters, picks one
   healthy residential proxy.
3. Gateway rewrites auth (`x-api-key` for Anthropic, `Bearer` for OpenAI-style),
   forces a streaming upstream request, and forwards through the proxy.
4. Streaming client → SSE relayed live. Non-streaming client → stream consumed and
   assembled into one JSON response.
5. On a real failure (network error / WAF HTML detected) it fails the proxy over to
   the next one — **one proxy per request**, never burning many at once.

---

## File structure

```
ai-gateway/
├── app/
│   ├── main.py              FastAPI app: /v1/* proxy routes, admin API, dashboard mount
│   ├── forwarder.py         ACTIVE: the always-stream bridge (relay + assemble)
│   ├── forwarder_improved.py  preserved copy of the bridge
│   ├── forwarder_original.py  backup of the original simpler forwarder
│   ├── db.py                SQLite store: settings, proxies, keywords, logs
│   ├── proxy_pool.py        proxy selection, mark_good/mark_bad, WAF-aware health check
│   ├── filters.py           keyword redact / block over system + messages
│   └── __init__.py
├── static/
│   ├── dashboard.html       Claude-style dark dashboard
│   └── app.js               dashboard logic (tabs, fetch admin API)
├── seed.py                  seed DB from env vars + proxies.txt (no hard-coded secrets)
├── run.sh                   start uvicorn (HOST/PORT overridable)
├── requirements.txt         fastapi, uvicorn[standard], httpx
├── .gitignore               excludes data.db, proxies.txt, .env, *.log, venv …
└── README.md
```

**Never committed (gitignored):** `data.db` (all secrets live here), `proxies.txt`
(proxy credentials), `.env`, `*.log`.

---

## Setup

Requires Python 3.11+.

```bash
cd ai-gateway
python3 -m venv .venv && . .venv/bin/activate    # or use uv
pip install -r requirements.txt

# create proxies.txt — one residential proxy per line:
#   http://user:pass@ip:port
#   ip:port:user:pass also accepted (seed.py normalizes)

# seed the DB (secrets via env, nothing hard-coded):
GATEWAY_ENDPOINT=https://your-endpoint.example \
GATEWAY_KEY=sk-REPLACE_ME \
ADMIN_PASSWORD=REPLACE_ME \
python3 seed.py

./run.sh                      # binds 0.0.0.0:8787
# dashboard: http://localhost:8787/   (login with ADMIN_PASSWORD)
```

### Configuration (all editable in the dashboard → Settings)
| Setting | Meaning |
|---|---|
| `endpoint` | Upstream base URL (default `https://agentrouter.org`) |
| `gateway_key` | Key clients must present to the gateway |
| `upstream_key` | Real key the gateway uses upstream |
| `require_client_key` | Enforce the gateway key on inbound requests |
| `admin_password` | Dashboard login |
| `max_retries` / timeouts | Failover + connection tuning |
| `user_agent` | UA sent upstream |

---

## Pointing your agents at the gateway

### Endpoint URLs the gateway exposes
- **Anthropic (Messages API):** `http://<gateway-host>:<port>/v1/messages`
- **OpenAI-style:** `http://<gateway-host>:<port>/v1/chat/completions`

> Note: the current upstream only supports the **Anthropic** format. The OpenAI
> path exists in the gateway but will only work if the upstream account supports
> chat-completions.

### Claude Code
```bash
export ANTHROPIC_BASE_URL=http://<gateway-host>:<port>
export ANTHROPIC_API_KEY=<gateway key>
```

### Hermes Agent (`~/.hermes/config.yaml`)
```yaml
provider: custom
model: claude-opus-4-8
custom_providers:
  - name: custom
    base_url: http://<gateway-host>:<port>
    api_key: <gateway key>
    api_mode: anthropic_messages   # endpoint speaks Anthropic format
```
`display.streaming` only changes terminal rendering — the API always streams
upstream regardless, which is what keeps long-context requests alive.

---

## Dashboard

Claude-style dark UI with tabs:
- **Connection** — endpoint URL, keys, both exposed endpoint URLs, test button.
- **Proxies** — add / bulk-add, test (WAF-aware health check), good/bad status.
- **Filter** — add keyword rules (redact or block) over system + messages.
- **Logs** — recent requests, status, which proxy was used.
- **Settings** — admin password, retries, timeouts, user-agent.

---

## Deployment notes (NAT VPS)

Deployed on a **NAT VPS** with **no dedicated public IP** — inbound only via
**port forwarding** (e.g. host port → VPS 80/443/22). Served over plain HTTP on
the forwarded port via Caddy (`reverse_proxy 127.0.0.1:8787` with
`flush_interval -1` so SSE isn't buffered). Both `ai-gateway` (uvicorn) and
`caddy` run as systemd services.

**HTTPS caveat:** automatic Let's Encrypt (HTTP-01 / TLS-ALPN-01) cannot work on a
NAT VPS because the ACME challenge can't reach standard ports 80/443. Real HTTPS
would need a manual DNS-01 certificate (≈90-day manual renewal) or owning the
parent DNS zone for a Cloudflare Tunnel.

---

## Security

- **All secrets live only in local SQLite (`data.db`) and `proxies.txt`** — both
  gitignored. This repo contains **no API keys, no proxy credentials, no
  passwords**. Use the placeholders above and seed via env vars.
- Never hit the upstream directly from the VPS IP — always through a residential
  proxy (direct hits get the IP WAF-flagged).
- Change the default `admin_password` before exposing the dashboard publicly.
- Rotate any key/password that was ever pasted into a shared channel.

---

## Status

v1 complete and verified end-to-end **through a residential proxy** with model
`claude-opus-4-8`: non-streaming ✅, streaming SSE ✅, tool-use ✅, content filter
(redact + block) ✅, and a live Hermes Agent connected through the gateway ✅.
