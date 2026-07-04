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
    {"name": "set_setting", "description": "Change a gateway setting (e.g. auto_rotation, read_timeout, max_retries, claude_mimicry).",
     "parameters": {"type": "object", "properties": {"key": {"type": "string"}, "value": {"type": "string"}}, "required": ["key", "value"]}},
    {"name": "recent_logs", "description": "Read the most recent request logs to diagnose errors.",
     "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}}}},
    {"name": "delete_all_proxies", "description": "Delete ALL proxies from the database.",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "process_file_link", "description": "Download a file from a URL, extract IPs, and add them.",
     "parameters": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}},
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
            added = db.add_endpoint(args["url"].rstrip("/"),
                                    args.get("api_mode", "anthropic_messages"),
                                    args.get("api_key", ""))
            return {"added": added}
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
        if name == "delete_all_proxies":
            with db.get_db() as conn:
                conn.execute("DELETE FROM proxies")
                conn.commit()
            return {"ok": True, "message": "All proxies deleted"}
        if name == "process_file_link":
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get(args["url"])
                text = r.text
            urls = [u for u in (_normalize_proxy(l) for l in text.splitlines()) if u]
            added = db.bulk_add(urls)
            return {"added": added, "parsed": len(urls)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}
    return {"error": "unknown tool"}


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
    # default: go through THIS gateway (localhost) using the gateway key
    gk = db.get_setting("gateway_key", "")
    port = os.environ.get("PORT", "8000")
    return {
        "sdk": "anthropic",
        "endpoint": f"http://127.0.0.1:{port}",
        "api_key": gk,
        "model": db.get_setting("model_note", "claude-sonnet-4-6"),
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
