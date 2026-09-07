#!/usr/bin/env python3
"""Integration checks for gateway language-toggle wiring (no network)."""
import asyncio
import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "/root/gw-preview/repo")
from app import forwarder as F


async def fake_translate(body, kind):
    data = json.loads(body)
    data["_language_translated"] = kind
    return json.dumps(data).encode(), True


class PrepareRequestTests(unittest.TestCase):
    def test_toggle_off_keeps_language_body_exact(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"0"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate) as tr:
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertEqual(out, body)
        self.assertFalse(fmt)
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_toggle_on_translates_before_wire_format(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"1"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate) as tr:
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(lang)
        self.assertFalse(fmt)
        self.assertEqual(json.loads(out)["_language_translated"], "anthropic")
        tr.assert_called_once_with(body, "anthropic")

    def test_global_off_endpoint_absent_does_not_translate(self):
        tgt = {"mode": "anthropic", "fx_flags": ""}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate) as tr, \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": "0" if key == "fx_translate_language" else default):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertEqual(out, body)
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_global_on_endpoint_absent_translates(self):
        tgt = {"mode": "anthropic", "fx_flags": ""}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate), \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": "1" if key == "fx_translate_language" else default):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(lang)

    def test_endpoint_off_overrides_global_on(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"0"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate) as tr, \
             patch.object(F.db, "get_setting", return_value="1"):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_endpoint_on_overrides_global_off(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"1"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate), \
             patch.object(F.db, "get_setting", return_value="0"):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(lang)

    def test_endpoint_test_applies_language_toggle_before_http(self):
        """Dashboard Test/Chat must exercise the same language flag as real routing."""
        row = {"url": "https://agentrouter.org", "fx_flags":
               '{"fx_translate_language":"1"}', "custom_proxies": "[]",
               "proxy_priority": "[]", "proxy_fallback": 0}

        class FakeCursor:
            def execute(self, *args, **kwargs): return self
            def fetchone(self): return row

        class FakeResponse:
            status_code = 200
            text = '{"content":[{"type":"text","text":"ok"}]}'
            headers = {"content-type": "application/json"}
            content = text.encode()
            def json(self): return json.loads(self.text)

        captured = {}
        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def post(self, url, headers=None, content=None):
                captured["content"] = content
                return FakeResponse()

        async def fake_translate(body, kind):
            data = json.loads(body)
            data["messages"][0]["content"] = "translated English"
            return json.dumps(data).encode(), True

        with patch.object(F.db, "conn", return_value=FakeCursor()), \
             patch.object(F, "_resolve_candidates", return_value=[]), \
             patch.object(F.httpx, "AsyncClient", return_value=FakeClient()), \
             patch.object(F.language_translate, "translate_request", side_effect=fake_translate), \
             patch.object(F.db, "add_log"), \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": default):
            result = asyncio.run(F.test_endpoint(
                "https://agentrouter.org", "anthropic", "key", "model", "नमस्ते"))
        self.assertTrue(result["ok"])
        sent = json.loads(captured["content"])
        self.assertEqual(sent["messages"][0]["content"], "translated English")

    def test_language_then_format_translation_compose(self):
        tgt = {"mode": "openai", "fx_flags":
               '{"fx_translate_language":"1","fx_translate_format":"1"}'}
        body = json.dumps({"model":"x","max_tokens":20,
                           "messages":[{"role":"user","content":"नमस्ते"}]}).encode()
        with patch.object(F.language_translate, "translate_request", side_effect=fake_translate):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        data = json.loads(out)
        self.assertTrue(lang)
        self.assertTrue(fmt)
        self.assertEqual(data["messages"][0]["content"], "नमस्ते")
        # The wire translator intentionally drops unknown top-level fields; the
        # translated content itself is what must survive composition.


if __name__ == "__main__":
    unittest.main(verbosity=2)
