#!/usr/bin/env python3
"""T2: LLMTranslator — custom LLM translation backend (2026-09-14).

Boss's requirements encoded as tests:
- api_mode-aware request shape (anthropic_messages | chat_completions)
- max_output_tokens: 0=AUTO -> int(input_tokens*1.5)+256; or a fixed user value
- RPM pacing: 30 RPM backend must not fire more than 30 calls in the first
  minute; the 31st waits for the next window (no drops, no exceptions)
- proxy failover: proxy error -> next proxy, then direct-style bare client
- exact-paste system prompt default; custom prompt editable per backend
- chunk_chars: backend's own chunk size (1M-context models -> big chunks)
"""
import asyncio, json, sys, time, unittest
from unittest.mock import patch

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)


class Responder:
    """Minimal stand-in for the HTTP layer; records request shapes.
    Replies are wrapped in a real upstream JSON shape per api_mode."""
    def __init__(self, replies=None, statuses=None, api_mode="chat_completions"):
        self.calls = []
        self.replies = replies or ["translated"]
        self.statuses = statuses or [200]
        self.api_mode = api_mode

    def _wrap(self, text):
        if self.api_mode == "anthropic_messages":
            return json.dumps({"content": [{"type": "text", "text": text}]})
        return json.dumps({"choices": [{"message": {"role": "assistant", "content": text}}]})

    async def __call__(self, client, url, headers, payload, timeout):
        self.calls.append({"url": url, "headers": dict(headers), "payload": payload})
        self._last_timeout = timeout
        i = min(len(self.calls) - 1, len(self.replies) - 1)
        status = self.statuses[min(len(self.calls) - 1, len(self.statuses) - 1)]
        return status, self._wrap(self.replies[i])


def backend_row(**over):
    row = dict(id=7, name="DS", kind="llm", url="https://ds.example/v1",
               api_key="sk-t", model="deepseek-v4.1-flash",
               api_mode="chat_completions", system_prompt="",
               rpm=0, chunk_chars=4000, max_output_tokens=0,
               custom_proxies='["http://p1", "http://p2"]',
               proxy_priority='["custom_0", "custom_1"]',
               proxy_fallback=0, priority=0, enabled=1)
    row.update(over)
    return row


class LLMTranslatorTests(unittest.TestCase):
    def _tr(self, row, responder):
        from app import llm_translate
        return llm_translate.LLMTranslator(row, http_call=responder)

    def test_openai_shape_and_auth(self):
        from app import llm_translate
        r = Responder(replies=["Hello world"])
        tr = self._tr(backend_row(), r)
        out = asyncio.run(tr("नमस्ते दुनिया"))
        self.assertEqual(out, "Hello world")
        c = r.calls[0]
        self.assertTrue(c["url"].endswith("/chat/completions"), c["url"])
        self.assertEqual(c["headers"].get("authorization"), "Bearer sk-t")
        p = c["payload"]
        self.assertEqual(p["model"], "deepseek-v4.1-flash")
        self.assertEqual(p["messages"][0]["role"], "system")
        self.assertIn("exactly", p["messages"][0]["content"].lower())

    def test_anthropic_shape_and_auth(self):
        from app import llm_translate
        r = Responder(replies=["Hello"], api_mode="anthropic_messages")
        tr = self._tr(backend_row(api_mode="anthropic_messages"), r)
        asyncio.run(tr("hi there friend"))
        c = r.calls[0]
        self.assertIn("/v1/messages", c["url"])
        self.assertEqual(c["headers"].get("x-api-key"), "sk-t")
        self.assertIn("anthropic-version", {k.lower() for k in c["headers"]})
        p = c["payload"]
        self.assertEqual(p["messages"][0]["role"], "user")

    def test_auto_max_tokens_scales_with_input(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(), r)
        asyncio.run(tr("नमस्ते"))
        # formula contract: int(max(1, chars/2.8) * 1.5) + 256
        import math
        expected = int(max(1, int(len("नमस्ते") / 2.8)) * 1.5) + 256
        self.assertEqual(r.calls[0]["payload"]["max_tokens"], expected)
        # and a bigger input must yield a bigger budget (dynamic scaling)
        r2 = Responder(replies=["ok"])
        tr2 = self._tr(backend_row(), r2)
        asyncio.run(tr2("नमस्ते " * 100))
        expected2 = int(max(1, int(len("नमस्ते " * 100) / 2.8)) * 1.5) + 256
        self.assertEqual(r2.calls[0]["payload"]["max_tokens"], expected2)
        self.assertGreater(expected2, expected)

    def test_fixed_max_tokens_override(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(max_output_tokens=100), r)
        asyncio.run(tr("नमस्ते"))
        self.assertEqual(r.calls[0]["payload"]["max_tokens"], 100)

    def test_rpm_bucket_paces_31st_call(self):
        from app import llm_translate
        r = Responder(replies=["ok"] * 40)
        tr = self._tr(backend_row(rpm=30), r)
        async def run():
            outs = []
            for _ in range(32):
                outs.append(await tr("नमस्ते"))
            return outs
        t0 = time.monotonic()
        outs = asyncio.run(run())
        dt = time.monotonic() - t0
        self.assertEqual(len(outs), 32)
        # 30 instant + 2 paced: they must have WAITED, not burst
        self.assertGreaterEqual(dt, 0.55)  # paced waits are >= bucket window remainder
        self.assertEqual(len(r.calls), 32)

    def test_proxy_failover_first_dead(self):
        from app import llm_translate
        class Flaky:
            def __init__(self): self.n = 0
            async def __call__(self, client, url, headers, payload, timeout):
                self.n += 1
                if self.n == 1:
                    raise ConnectionError("proxy dead")
                return 200, json.dumps(
                    {"choices": [{"message": {"role": "assistant",
                                              "content": "via-second"}}]})
        f = Flaky()
        tr = self._tr(backend_row(), f)
        out = asyncio.run(tr("नमस्ते"))
        self.assertEqual(out, "via-second")
        self.assertEqual(f.n, 2)

    def test_custom_system_prompt_used_verbatim(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(system_prompt="CUSTOM PROMPT XYZ"), r)
        asyncio.run(tr("नमस्ते"))
        self.assertEqual(r.calls[0]["payload"]["messages"][0]["content"],
                         "CUSTOM PROMPT XYZ")

    def test_big_chunk_chars_sent_whole(self):
        from app import llm_translate
        r = Responder(replies=["big ok"])
        tr = self._tr(backend_row(chunk_chars=8000), r)
        text = "यह एक लंबा हिंदी वाक्य है " * 300  # ~6600 chars
        asyncio.run(tr(text))
        # one call, whole text (under 8000)
        self.assertEqual(len(r.calls), 1)
        sent = r.calls[0]["payload"]["messages"][1]["content"]
        self.assertEqual(sent, text)

    # -- configurable timeout (Boss 2026-09-14: "hardcoded mat rakho") ----------
    def test_timeout_from_row_overrides_default(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(timeout_seconds=12), r)
        asyncio.run(tr("नमस्ते"))
        # the http layer must have RECEIVED the row's timeout, not the module default
        self.assertEqual(r._last_timeout, 12)

    def test_timeout_zero_means_default(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(timeout_seconds=0), r)
        asyncio.run(tr("नमस्ते"))
        self.assertEqual(r._last_timeout, llm_translate.DEFAULT_TIMEOUT_SECONDS)

    def test_responder_records_timeout(self):
        from app import llm_translate
        r = Responder(replies=["ok"])
        tr = self._tr(backend_row(), r)
        asyncio.run(tr("नमस्ते"))
        self.assertEqual(r._last_timeout, llm_translate.DEFAULT_TIMEOUT_SECONDS)

    def test_failure_raises_after_all_proxies(self):
        from app import llm_translate
        class Dead:
            async def __call__(self, client, url, headers, payload, timeout):
                raise ConnectionError("dead")
        tr = self._tr(backend_row(), Dead())
        with self.assertRaises(Exception):
            asyncio.run(tr("नमस्ते"))


if __name__ == "__main__":
    unittest.main(verbosity=1)
