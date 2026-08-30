"""Embedded dashboard AI agent.

A small assistant that lives in the dashboard's bottom-right chat. It can run
real actions against the routing system (add/test proxies, add/test endpoints,
add filters, change settings, read logs) via a tool-calling loop.

Brain is configurable from the UI:
  - default  -> talk to THIS gateway (agentrouter via the gateway's upstream key)
  - custom   -> a user-supplied {endpoint, api_key, model, sdk: openai|anthropic}

We keep this provider-agnostic by speaking the OpenAI chat/completions tool API
when sdk=openai, and the Anthropic messages tool API when sdk=anthropic.
"""
import asyncio
import json
import os
import time
import httpx
from . import db, proxy_pool

_HERE = os.path.dirname(os.path.abspath(__file__))
_STATIC = os.path.join(os.path.dirname(_HERE), "static")


def _load_prompt():
    """System prompt = AGENT.md + memory.md if present, else the built-in fallback."""
    parts = []
    for fn in ("AGENT.md", "memory.md"):
        try:
            with open(os.path.join(_STATIC, fn)) as f:
                parts.append(f.read())
        except Exception:
            pass
    return "\n\n".join(parts) if parts else _FALLBACK_SYSTEM


_FALLBACK_SYSTEM = """You are Mugen, the assistant built into the Mugen Routes gateway dashboard.
You help the operator manage the routing system. Be extremely concise — one or two
short lines, like a chat. If you did an action, just say what you did."""

# ---- tool definitions (shared shape; adapted per SDK below) ----
TOOLS = [
    {"name": "add_proxies", "description": "Add one or more proxies. Accepts any format text (IP:PORT, user:pass@IP:PORT, IP:PORT:user:pass, scheme://...). One per line.",
     "parameters": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
    {"name": "test_proxy", "description": "Test a proxy by its id against the upstream endpoint (token-free).",
     "parameters": {"type": "object", "properties": {"id": {"type": "integer"}}, "required": ["id"]}},
    {"name": "list_proxies", "description": "List proxies with id, host, status, success count.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "add_endpoint", "description": "Add an upstream endpoint. api_mode is 'anthropic_messages' or 'chat_completions'.",
     "parameters": {"type": "object", "properties": {"url": {"type": "string"}, "api_mode": {"type": "string"}, "api_key": {"type": "string"}}, "required": ["url"]}},
    {"name": "add_filter", "description": "Add a content filter keyword. mode is 'redact' or 'block'.",
     "parameters": {"type": "object", "properties": {"value": {"type": "string"}, "mode": {"type": "string"}}, "required": ["value"]}},
    {"name": "set_setting", "description": "Change a gateway setting (e.g. auto_rotation, read_timeout, max_retries).",
     "parameters": {"type": "object", "properties": {"key": {"type": "string"}, "value": {"type": "string"}}, "required": ["key", "value"]}},
    {"name": "recent_logs", "description": "Read the most recent request logs to diagnose errors.",
     "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}}}},

    # --- inspection -------------------------------------------------------------
    {"name": "list_endpoints", "description": "List upstream endpoints with id, name, url, api_mode, whether enabled, routing position, how many API keys, and the proxy chain. Use before changing anything.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "routing_status", "description": "The exact order the gateway tries endpoints, with per-endpoint served/failed counts, success rate and median latency over the last 24h. Use to answer 'which endpoint is being used' or 'why is it slow'.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "gateway_stats", "description": "Whole-request stats: total requests, success rate, median/average/p95 latency, average attempts, first-try percentage.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "get_settings", "description": "Read current gateway settings (timeouts, retries, hedging, hot pool). Secrets are redacted.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "proxy_counts", "description": "How many proxies exist by status (ok / unknown / unhealthy / banned) and how many are enabled.",
     "parameters": {"type": "object", "properties": {}}},

    # --- maintenance ------------------------------------------------------------
    {"name": "test_proxies_bulk", "description": "Test up to `limit` proxies against the upstream and record each result. Returns how many passed and failed. Use before prune_proxies so the statuses are fresh.",
     "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}, "status": {"type": "string", "description": "only test proxies with this status, e.g. 'unknown'"}}}},
    {"name": "prune_proxies", "description": "Delete proxies that are known bad. `mode`: 'unhealthy' (status unhealthy/banned), 'failing' (fail_count >= min_fails and never succeeded), or 'unroutable' (0.0.0.0, loopback, private ranges). Always report how many were removed.",
     "parameters": {"type": "object", "properties": {"mode": {"type": "string"}, "min_fails": {"type": "integer"}}, "required": ["mode"]}},
    {"name": "update_endpoint", "description": "Change one endpoint by id. Settable: name, enabled (0/1), model_override, api_key, proxy_fallback (0/1), failover_trigger_keywords, endpoint_failover_keywords, key_failover_keywords.",
     "parameters": {"type": "object", "properties": {"id": {"type": "integer"}, "name": {"type": "string"}, "enabled": {"type": "integer"}, "model_override": {"type": "string"}, "api_key": {"type": "string"}, "proxy_fallback": {"type": "integer"}, "failover_trigger_keywords": {"type": "string"}, "endpoint_failover_keywords": {"type": "string"}, "key_failover_keywords": {"type": "string"}}, "required": ["id"]}},
    {"name": "reorder_endpoints", "description": "Set the endpoint try-order. `ids` is the full list, first is tried first. The first enabled one also becomes primary.",
     "parameters": {"type": "object", "properties": {"ids": {"type": "array", "items": {"type": "integer"}}}, "required": ["ids"]}},
    {"name": "test_endpoint", "description": "Send one small real request through an endpoint (by id) using its own keys and proxy chain. Returns status, latency, which proxy carried it, and the reply or the error.",
     "parameters": {"type": "object", "properties": {"id": {"type": "integer"}, "message": {"type": "string"}}, "required": ["id"]}},
]


def _normalize_proxy(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "://" in line:
        return line
    if "@" in line:
        return "http://" + line
    parts = line.split(":")
    if len(parts) == 4 and parts[1].isdigit():
        h, p, u, w = parts
        return f"http://{u}:{w}@{h}:{p}"
    return "http://" + line


async def _run_tool(name, args):
    """Execute one tool call against the DB / proxy pool. Returns a short result dict."""
    try:
        if name == "add_proxies":
            urls = [u for u in (_normalize_proxy(l) for l in args.get("text", "").splitlines()) if u]
            added = db.bulk_add(urls)
            return {"added": added, "parsed": len(urls)}
        if name == "test_proxy":
            p = db.get_proxy(int(args["id"]))
            if not p:
                return {"error": "no such proxy"}
            endpoint = db.get_setting("endpoint", "https://agentrouter.org")
            res = await proxy_pool.health_check(p["url"], endpoint)
            db.update_proxy(p["id"], status=res["status"], exit_ip=res.get("exit_ip", ""),
                            latency_ms=res.get("latency_ms", 0), last_checked=time.time(),
                            note=res.get("detail", ""))
            return {"ok": res["ok"], "status": res["status"], "exit_ip": res.get("exit_ip", "")}
        if name == "list_proxies":
            return {"proxies": [{"id": p["id"], "host": p["url"].split("@")[-1],
                                 "status": p["status"], "ok": p["success_count"]}
                                for p in db.list_proxies()[:50]]}
        if name == "add_endpoint":
            added, eid = db.add_endpoint(args["url"].rstrip("/"),
                                         args.get("api_mode", "anthropic_messages"),
                                         args.get("api_key", ""),
                                         name=args.get("name", ""))
            return {"added": added, "id": eid}
        if name == "add_filter":
            kid = db.add_keyword(args["value"], args.get("mode", "redact"))
            return {"id": kid}
        if name == "set_setting":
            db.set_setting(args["key"], args["value"])
            return {"ok": True, "key": args["key"], "value": args["value"]}
        if name == "recent_logs":
            logs = db.recent_logs(int(args.get("limit", 15)))
            return {"logs": [{"status": l["status"], "note": l["note"], "model": l.get("model"),
                              "ip": l.get("ip"), "detail": (l.get("detail") or "")[:300],
                              "ms": l["ms"]} for l in logs]}
        # --- inspection ---------------------------------------------------------
        if name == "list_endpoints":
            from . import forwarder
            settings = db.get_all_settings()
            order = {t["id"]: i + 1 for i, t in enumerate(forwarder._targets(settings))}
            out = []
            for e in db.list_endpoints():
                try:
                    chain = json.loads(e.get("proxy_priority") or "[]")
                except (TypeError, ValueError):
                    chain = []
                out.append({
                    "id": e["id"],
                    "name": (e.get("name") or "").strip() or e["url"].split("//")[-1],
                    "url": e["url"],
                    "api_mode": e.get("api_mode"),
                    "enabled": bool(e.get("enabled")),
                    "position": order.get(e["id"]),
                    "keys": _key_count(e),
                    "proxy_chain": chain,
                    "proxy_fallback": e.get("proxy_fallback", 1),
                    "model_override": e.get("model_override") or "",
                })
            return {"endpoints": out}
        if name == "routing_status":
            from . import forwarder
            stats = db.endpoint_stats(86400)
            settings = db.get_all_settings()
            out = []
            for i, t in enumerate(forwarder._targets(settings)):
                st = stats.get(t["name"], {})
                out.append({"position": i + 1, "id": t["id"], "name": t["name"],
                            "keys": len(t.get("keys") or []),
                            "served": st.get("served", 0), "failed": st.get("failed", 0),
                            "success_pct": st.get("success_pct", 0),
                            "median_ms": st.get("median_ms", 0)})
            return {"order": out, "window": "24h"}
        if name == "gateway_stats":
            return db.stats(86400)
        if name == "get_settings":
            s_all = db.get_all_settings()
            # Never hand credentials to a model, even the operator's own.
            secret = ("admin_password", "gateway_key", "upstream_key")
            return {"settings": {k: v for k, v in s_all.items()
                                 if not any(x in k for x in secret)}}
        if name == "proxy_counts":
            return db.proxy_counts()

        # --- maintenance --------------------------------------------------------
        if name == "test_proxies_bulk":
            limit = max(1, min(int(args.get("limit", 25)), 200))
            want = (args.get("status") or "").strip()
            rows = [p for p in db.list_proxies()
                    if p.get("enabled") and (not want or p.get("status") == want)][:limit]
            endpoint = db.get_setting("endpoint", "https://agentrouter.org")

            # Concurrent, bounded, and each check individually capped. Sequentially, one
            # dead proxy costs a whole connect timeout, so ten of them could outlast the
            # HTTP request that asked for the test and the operator saw nothing at all.
            sem = asyncio.Semaphore(12)

            async def check(p):
                async with sem:
                    try:
                        return p, await asyncio.wait_for(
                            proxy_pool.health_check(p["url"], endpoint), timeout=12)
                    except Exception:
                        return p, {"ok": False, "status": "unhealthy",
                                   "latency_ms": 0, "detail": "no response"}

            results = await asyncio.gather(*[check(p) for p in rows])
            passed = failed = 0
            for p, res in results:
                db.update_proxy(p["id"], status=res.get("status", "unhealthy"),
                                exit_ip=res.get("exit_ip", ""),
                                latency_ms=res.get("latency_ms", 0), last_checked=time.time(),
                                note=(res.get("detail") or "")[:200])
                if res.get("ok"):
                    passed += 1
                else:
                    failed += 1
            return {"tested": len(rows), "passed": passed, "failed": failed}
        if name == "prune_proxies":
            mode = (args.get("mode") or "").strip()
            removed = db.prune_proxies(mode, int(args.get("min_fails", 3)))
            return {"mode": mode, "removed": removed,
                    "remaining": db.proxy_counts().get("total", 0)}
        if name == "update_endpoint":
            eid = int(args.pop("id"))
            fields = {k: v for k, v in args.items() if v is not None}
            if not fields:
                return {"error": "nothing to change"}
            db.update_endpoint(eid, **fields)
            return {"ok": True, "id": eid, "changed": sorted(fields.keys())}
        if name == "reorder_endpoints":
            ids = args.get("ids") or []
            if not isinstance(ids, list) or not ids:
                return {"error": "ids must be a non-empty list"}
            return {"order": db.reorder_endpoints(ids)}
        if name == "test_endpoint":
            from . import forwarder
            e = next((x for x in db.list_endpoints() if x["id"] == int(args["id"])), None)
            if not e:
                return {"error": "no such endpoint"}
            key = e.get("api_key") or db.get_setting("upstream_key") or db.get_setting("gateway_key", "")
            model = e.get("model_override") or db.get_setting("model_note", "claude-sonnet-4-6")
            res = await forwarder.test_endpoint(
                e["url"], e.get("api_mode", "anthropic_messages"), key, model,
                args.get("message", "Reply with exactly: OK"))
            # Trim the reply: a full model answer would bloat the tool result.
            res["reply"] = (res.get("reply") or "")[:200]
            res["detail"] = (res.get("detail") or "")[:300]
            return res
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
    return {"error": "unknown tool"}


def _key_count(e):
    """Primary + spare keys on an endpoint. Counts only — never the values."""
    n = 1 if (e.get("api_key") or "").strip() else 0
    try:
        extra = json.loads(e.get("extra_keys") or "[]")
    except (TypeError, ValueError):
        extra = []
    if isinstance(extra, list):
        n += len([k for k in extra if str(k or "").strip()])
    return max(1, n)


def _brain_config(cfg):
    """Resolve the brain. cfg may be {} (default=gateway) or a custom dict."""
    if cfg and cfg.get("mode") == "custom" and cfg.get("endpoint"):
        return {
            "sdk": cfg.get("sdk", "openai"),
            "endpoint": cfg["endpoint"].rstrip("/"),
            "api_key": cfg.get("api_key", ""),
            "model": cfg.get("model", "gpt-4o-mini"),
            "direct": True,  # talk straight to the custom endpoint
        }
    # Default: route through THIS gateway on loopback, so the assistant inherits the
    # whole endpoint/proxy/failover chain the operator has configured.
    # The port was hardcoded to 8787, which meant every instance on another port
    # (staging 9787, preview 9790) pointed its assistant at a different gateway — or at
    # nothing at all. Read the port this process is actually serving.
    gk = db.get_setting("gateway_key", "")
    port = os.environ.get("PORT", "8787")
    return {
        "sdk": "anthropic",
        "endpoint": f"http://127.0.0.1:{port}",
        "api_key": gk,
        "model": (db.get_setting("global_model_override", "")
                  or db.get_setting("model_note", "claude-sonnet-4-6")),
        "direct": False,
    }


async def chat(messages, cfg=None, max_rounds=5):
    """Run a tool-calling conversation. `messages` is [{role, content}].
    Returns {reply, actions:[...]} — actions is a list of tool results for the UI."""
    brain = _brain_config(cfg)
    actions = []
    if brain["sdk"] == "openai":
        return await _chat_openai(messages, brain, actions, max_rounds)
    return await _chat_anthropic(messages, brain, actions, max_rounds)


async def _chat_openai(messages, brain, actions, max_rounds):
    url = brain["endpoint"]
    if not url.endswith("/chat/completions"):
        url = url.rstrip("/") + "/chat/completions"
    headers = {"authorization": f"Bearer {brain['api_key']}", "content-type": "application/json"}
    tools = [{"type": "function", "function": {"name": t["name"], "description": t["description"],
              "parameters": t["parameters"]}} for t in TOOLS]
    msgs = [{"role": "system", "content": _load_prompt()}] + messages
    async with httpx.AsyncClient(timeout=httpx.Timeout(90.0)) as client:
        for _ in range(max_rounds):
            body = {"model": brain["model"], "messages": msgs, "tools": tools, "max_tokens": 1024}
            r = await client.post(url, headers=headers, content=json.dumps(body))
            if r.status_code >= 300:
                return {"reply": f"Brain error {r.status_code}: {r.text[:200]}", "actions": actions}
            d = r.json()
            choice = d["choices"][0]["message"]
            calls = choice.get("tool_calls") or []
            if not calls:
                return {"reply": choice.get("content") or "(no reply)", "actions": actions}
            msgs.append(choice)
            for c in calls:
                fn = c["function"]["name"]
                try:
                    fa = json.loads(c["function"].get("arguments") or "{}")
                except Exception:
                    fa = {}
                res = await _run_tool(fn, fa)
                actions.append({"tool": fn, "result": res})
                msgs.append({"role": "tool", "tool_call_id": c["id"], "content": json.dumps(res)})
    return {"reply": "Done.", "actions": actions}


async def _chat_anthropic(messages, brain, actions, max_rounds):
    url = brain["endpoint"].rstrip("/") + "/v1/messages"
    headers = {"x-api-key": brain["api_key"], "anthropic-version": "2023-06-01",
               "content-type": "application/json"}
    tools = [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]}
             for t in TOOLS]
    # anthropic content blocks
    conv = [{"role": m["role"], "content": m["content"]} for m in messages]
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        for _ in range(max_rounds):
            body = {"model": brain["model"], "system": _load_prompt(), "messages": conv,
                    "tools": tools, "max_tokens": 1024}
            r = await client.post(url, headers=headers, content=json.dumps(body))
            if r.status_code >= 300:
                return {"reply": f"Brain error {r.status_code}: {r.text[:200]}", "actions": actions}
            d = r.json()
            blocks = d.get("content", [])
            tool_uses = [b for b in blocks if b.get("type") == "tool_use"]
            text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
            if not tool_uses:
                return {"reply": text or "(no reply)", "actions": actions}
            conv.append({"role": "assistant", "content": blocks})
            results = []
            for tu in tool_uses:
                res = await _run_tool(tu["name"], tu.get("input", {}))
                actions.append({"tool": tu["name"], "result": res})
                results.append({"type": "tool_result", "tool_use_id": tu["id"],
                                "content": json.dumps(res)})
            conv.append({"role": "user", "content": results})
    return {"reply": "Done.", "actions": actions}
