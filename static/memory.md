# Mugen Assistant — Memory

This file holds durable notes the assistant can rely on across sessions. The operator's
environment-specific facts live here so the agent doesn't re-ask.

## Environment
- Gateway live on Railway: https://mugen-routes-production.up.railway.app
- Primary upstream: agentrouter.org (model: claude-* via the gateway's upstream key).
- Whitelisted dedicated proxy: id 85 → 217.181.91.60:3129. After a fresh deploy the
  DB re-seeds and this pin must be re-set or requests 403.

## Known-good test endpoint (operator's own gateway)
- https://proxy.ciel.ryzedns.org/v1 · model qwen-large · OpenAI SDK. Works perfectly.

## Recurring facts
- 503 "无可用渠道" = agentrouter channel down (account-side), NOT a gateway fault.
- Auto-rotation OFF is intentional and correct: one pinned IP for all retries.

(The assistant may append short notes below as it learns the operator's preferences.)
