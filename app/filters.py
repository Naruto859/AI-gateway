"""Content-filter middleware.

Before a request leaves the gateway, scan the Anthropic request body
(system prompt + messages) for sensitive keywords:

  - mode 'block'  -> reject the whole request (never reaches the endpoint)
  - mode 'redact' -> replace the keyword with a placeholder, then forward

This keeps things like a real IP, a name, or a system prompt from ever
reaching the upstream model endpoint.
"""
import json
from . import db


def _scan_replace(text, kws):
    hits = 0
    for kw in kws:
        if kw["mode"] == "redact" and kw["value"] and kw["value"] in text:
            hits += text.count(kw["value"])
            text = text.replace(kw["value"], kw["replacement"] or "[REDACTED]")
    return text, hits


def apply_filters(body_bytes):
    """Return (new_body_bytes, blocked_keyword_or_None, redaction_count)."""
    kws = [k for k in db.list_keywords() if k["enabled"] and k["value"]]
    if not kws:
        return body_bytes, None, 0

    try:
        data = json.loads(body_bytes)
    except Exception:
        return body_bytes, None, 0  # not JSON -> pass through untouched

    # block mode: if any blocked keyword appears anywhere, refuse the request
    whole = json.dumps(data, ensure_ascii=False)
    for kw in kws:
        if kw["mode"] == "block" and kw["value"] in whole:
            return body_bytes, kw["value"], 0

    redactions = 0

    def fix(s):
        nonlocal redactions
        s2, h = _scan_replace(s, kws)
        redactions += h
        return s2

    # system can be a string or a list of content blocks
    sys = data.get("system")
    if isinstance(sys, str):
        data["system"] = fix(sys)
    elif isinstance(sys, list):
        for blk in sys:
            if isinstance(blk, dict) and isinstance(blk.get("text"), str):
                blk["text"] = fix(blk["text"])

    # messages[*].content : string or list of blocks
    for m in data.get("messages", []):
        c = m.get("content")
        if isinstance(c, str):
            m["content"] = fix(c)
        elif isinstance(c, list):
            for blk in c:
                if isinstance(blk, dict) and isinstance(blk.get("text"), str):
                    blk["text"] = fix(blk["text"])

    if redactions == 0:
        return body_bytes, None, 0
    return json.dumps(data, ensure_ascii=False).encode("utf-8"), None, redactions
