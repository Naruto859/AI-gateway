<div align="center">
  <h1>🚀 Mugen AI Gateway</h1>
  <p><strong>Enterprise-Grade LLM Gateway with Intelligent Routing, Proxy Pools, and Failover</strong></p>
</div>

<br>

Mugen AI Gateway is a highly resilient, high-performance reverse proxy for your AI infrastructure. It sits transparently between your applications (agents, CLIs, web apps) and upstream LLM providers (Anthropic, OpenAI, custom endpoints).

By intelligently handling API routing, residential proxy rotation, rate limits, and structural API errors, it ensures that your AI agents never stall and your upstream connections remain robust.

---

## ✨ Enterprise Features

- **🌐 Provider Agnostic Routing**
  Seamlessly route traffic to ANY OpenAI or Anthropic compatible API endpoint. Mix and match providers effortlessly.
  
- **🔄 Intelligent Endpoint Failover**
  Configure a primary endpoint with multiple fallbacks. If an endpoint returns a rate-limit, 4xx, 5xx, or content-block error, the gateway invisibly reroutes the request to the next healthy provider.

- **🛡️ Residential Proxy Pool & WAF Bypass**
  Route upstream connections through a dynamic pool of residential proxies. Built-in health tracking, automatic proxy rotation, and dedicated IP pinning to bypass strict Cloudflare or Aliyun WAF challenges.

- **⚡ Buffered Stream Assembly**
  Assembles raw SSE (Server-Sent Events) from upstream providers into a complete, uninterrupted response before delivering it to the client, preventing mid-generation crashes.

- **🔑 API Key Management**
  Issue custom `mugen_*` API keys for your developers or applications. Track hit counts, restrict access, and monitor usage per key.

- **📊 Real-time Operations Dashboard**
  A beautiful, live web interface to monitor request logs, manage endpoints, configure proxy pools, adjust retry timers, and configure keyword redaction filters on the fly.

- **🔄 Format Auto-Translation**
  Natively translates payloads between OpenAI and Anthropic schemas depending on the upstream target's requirements.

---

## 🛠️ Installation & Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Naruto859/AI-gateway.git
cd AI-gateway
pip install -r requirements.txt
```

### 2. Configuration (Optional)

You can configure everything from the Web Dashboard. If you prefer environment variables, create a `.env` file:

```env
ADMIN_PASSWORD=your-secure-password
GATEWAY_KEY=your-upstream-api-key
GATEWAY_ENDPOINT=https://your-primary-llm-provider.com
PROXIES=http://user:pass@1.2.3.4:8080,http://user:pass@5.6.7.8:8080
```

### 3. Launch the Gateway

```bash
# Start using the auto-seed script
bash start.sh

# Or start manually via Uvicorn
python -m uvicorn app.main:app --host 0.0.0.0 --port 8787
```

**The dashboard is now live at `http://localhost:8787`**

---

## 🔌 Connecting Your AI Applications

Point your agents, CLIs, or SDKs to your Mugen Gateway instance. It works out-of-the-box as a drop-in replacement.

**Example: Anthropic SDK Integration**
```bash
export ANTHROPIC_BASE_URL=http://your-server:8787
export ANTHROPIC_API_KEY=mugen_your_custom_key
```

**Example: Direct cURL Request**
```bash
curl -X POST http://your-server:8787/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: mugen_your_custom_key" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "max_tokens": 1024,
    "messages": [{"role": "user", "content": "Explain quantum computing."}]
  }'
```

---

## 🖥️ Dashboard Overview

Manage your entire AI infrastructure directly from the web UI:

- **Timers:** Configure connect timeouts, read timeouts, TCP keepalives, and exponential backoff retry formulas.
- **Proxies:** Bulk add HTTP/Socks5 proxies. Run health checks, set a dedicated exit IP, and monitor success/fail ratios.
- **Endpoints:** Add any OpenAI/Anthropic compatible upstream URLs. Assign priorities and set your primary routing target.
- **Keys:** Generate and revoke `mugen_*` access keys for client authentication.
- **Logs:** View a live feed of all incoming requests, upstream latency, proxy selection, and detailed error JSONs.

---

## 🏗️ System Architecture

```text
Client Application 
       │
       ▼
[ Mugen AI Gateway ] ──(Keyword Filters)──> [ Request Normalization ]
       │                                            │
   (Dashboard)                                      ▼
       │                            [ Failover Routing Engine ]
       ▼                                            │
[ SQLite Database ]                                 ▼
                                        [ Residential Proxy Pool ]
                                                    │
                                                    ▼
                                    [ Upstream LLM Providers ]
                                     (OpenAI, Anthropic, Custom)
```

**Tech Stack:** Python 3.11+ • FastAPI • SQLite • Vanilla JS • HTTPX

---

## 📄 License

This project is licensed under the MIT License.
