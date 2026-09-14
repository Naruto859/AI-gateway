#!/usr/bin/env python3
"""Integration checks for gateway language-toggle wiring (no network)."""
import asyncio
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

from starlette.requests import Request

sys.path.insert(0, "/root/gw-preview/repo")
from app import forwarder as F


async def fake_translate(body, kind, **_kwargs):
    data = json.loads(body)
    data["_language_translated"] = kind
    return F.language_translate.TranslationReport(json.dumps(data).encode(), True)


async def collect_response_body(response):
    if hasattr(response, "body_iterator"):
        return b"".join([chunk async for chunk in response.body_iterator])
    return response.body


def make_request(path, body):
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http", "http_version": "1.1", "method": "POST",
        "scheme": "http", "path": path, "raw_path": path.encode(),
        "query_string": b"", "headers": [], "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope, receive)


class PrepareRequestTests(unittest.TestCase):
    def test_toggle_off_keeps_language_body_exact(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"0"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate) as tr:
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertEqual(out, body)
        self.assertFalse(fmt)
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_toggle_on_translates_when_wire_format_already_matches(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"1"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate) as tr:
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(lang)
        self.assertFalse(fmt)
        self.assertEqual(json.loads(out)["_language_translated"], "anthropic")
        tr.assert_called_once_with(body, "anthropic", proxy_urls=None, backends=None)

    def test_toggle_on_passes_endpoint_proxy_priority_to_translator(self):
        tgt = {
            "mode": "anthropic",
            "fx_flags": '{"fx_translate_language":"1"}',
            "custom_proxies": '["http://first", "http://preferred"]',
            "proxy_priority": '["custom_1", "custom_0"]',
        }
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report",
                          side_effect=fake_translate) as tr:
            asyncio.run(F._prepare_request(body, "anthropic", tgt))
        tr.assert_called_once_with(
            body, "anthropic",
            proxy_urls=["http://preferred", "http://first"],
            backends=None,
        )

    def test_global_off_endpoint_absent_does_not_translate(self):
        tgt = {"mode": "anthropic", "fx_flags": ""}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate) as tr, \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": "0" if key == "fx_translate_language" else default):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertEqual(out, body)
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_global_on_endpoint_absent_translates(self):
        tgt = {"mode": "anthropic", "fx_flags": ""}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate), \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": "1" if key == "fx_translate_language" else default):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(lang)

    def test_endpoint_off_overrides_global_on(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"0"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate) as tr, \
             patch.object(F.db, "get_setting", return_value="1"):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertFalse(lang)
        tr.assert_not_called()

    def test_endpoint_on_overrides_global_off(self):
        tgt = {"mode": "anthropic", "fx_flags": '{"fx_translate_language":"1"}'}
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        with patch.object(F.language_translate, "translate_request_report", side_effect=fake_translate), \
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

        async def fake_translate_report(body, kind, **_kwargs):
            data = json.loads(body)
            data["messages"][0]["content"] = "translated English"
            return F.language_translate.TranslationReport(
                json.dumps(data).encode(), True)

        with patch.object(F.db, "conn", return_value=FakeCursor()), \
             patch.object(F, "_resolve_candidates", return_value=[]), \
             patch.object(F.httpx, "AsyncClient", return_value=FakeClient()), \
             patch.object(F.language_translate, "translate_request_report",
                          side_effect=fake_translate_report), \
             patch.object(F.db, "add_log"), \
             patch.object(F.db, "get_setting", side_effect=lambda key, default="": default):
            result = asyncio.run(F.test_endpoint(
                "https://agentrouter.org", "anthropic", "key", "model", "नमस्ते"))
        self.assertTrue(result["ok"])
        sent = json.loads(captured["content"])
        self.assertEqual(sent["messages"][0]["content"], "translated English")

    def test_malformed_translation_proxy_config_fails_closed(self):
        tgt = {
            "name": "strict-endpoint",
            "mode": "anthropic",
            "fx_flags": '{"fx_translate_language":"1"}',
            "custom_proxies": "not-json",
            "proxy_priority": "[]",
        }
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        logs = []
        with patch.object(F.language_translate, "translate_request_report") as tr:
            with self.assertRaises(F.language_translate.LanguageTranslationError) as raised:
                asyncio.run(F._prepare_request(
                    body, "anthropic", tgt, log=lambda **kw: logs.append(kw)))
        tr.assert_not_called()
        self.assertIn("JSONDecodeError", str(raised.exception))
        self.assertIn("upstream_contacted=false", logs[0]["note"])

    def test_endpoint_test_translation_failure_never_posts_upstream(self):
        row = {"url": "https://diagnostic.invalid", "fx_flags":
               '{"fx_translate_language":"1"}', "custom_proxies": "[]",
               "proxy_priority": "[]", "proxy_fallback": 0}

        class FakeCursor:
            def execute(self, *args, **kwargs): return self
            def fetchone(self): return row

        client = MagicMock()
        report = F.language_translate.TranslationReport(
            b"original", False, error="RuntimeError: translator offline")
        with patch.object(F.db, "conn", return_value=FakeCursor()), \
             patch.object(F, "_resolve_candidates", return_value=[]), \
             patch.object(F.httpx, "AsyncClient", client), \
             patch.object(F.language_translate, "translate_request_report",
                          return_value=report), \
             patch.object(F.db, "add_log") as add_log, \
             patch.object(F.db, "get_setting",
                          side_effect=lambda key, default="": default):
            result = asyncio.run(F.test_endpoint(
                "https://diagnostic.invalid", "anthropic", "key", "model", "नमस्ते"))
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 503)
        self.assertEqual(result["attempts"], 0)
        self.assertIn("upstream_contacted=false", result["detail"])
        client.assert_not_called()
        self.assertIn("upstream_contacted=false", add_log.call_args.kwargs["note"])

    def test_endpoint_test_unexpected_translation_exception_never_posts_upstream(self):
        row = {"url": "https://diagnostic.invalid", "fx_flags":
               '{"fx_translate_language":"1"}', "custom_proxies": "[]",
               "proxy_priority": "[]", "proxy_fallback": 0}

        class FakeCursor:
            def execute(self, *args, **kwargs): return self
            def fetchone(self): return row

        client = MagicMock()
        with patch.object(F.db, "conn", return_value=FakeCursor()), \
             patch.object(F, "_resolve_candidates", return_value=[]), \
             patch.object(F.httpx, "AsyncClient", client), \
             patch.object(F.language_translate, "translate_request_report",
                          side_effect=RuntimeError("unexpected translator crash")), \
             patch.object(F.db, "add_log") as add_log, \
             patch.object(F.db, "get_setting",
                          side_effect=lambda key, default="": default):
            result = asyncio.run(F.test_endpoint(
                "https://diagnostic.invalid", "anthropic", "key", "model", "नमस्ते"))
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 503)
        self.assertEqual(result["attempts"], 0)
        self.assertIn("RuntimeError", result["detail"])
        self.assertIn("upstream_contacted=false", result["detail"])
        client.assert_not_called()
        self.assertIn("upstream_contacted=false", add_log.call_args.kwargs["note"])

    def _run_forward_failure(self, path, stream, unexpected=False):
        mode = "openai" if "chat/completions" in path else "anthropic"
        target = {
            "id": 1, "name": "strict-endpoint", "base": "https://upstream.invalid",
            "mode": mode, "key": "key", "keys": ["key"], "fx_flags":
            '{"fx_translate_language":"1"}', "custom_proxies": "[]",
            "proxy_priority": "[]", "proxy_fallback": 0,
        }
        body = json.dumps({
            "model": "model", "stream": stream,
            "messages": [{"role": "user", "content": "नमस्ते"}],
        }, ensure_ascii=False).encode()
        settings = {
            "require_client_key": "0", "max_retries": "1",
            "connect_timeout": "1", "read_timeout": "1",
            "write_timeout": "1", "pool_timeout": "1",
        }
        report = F.language_translate.TranslationReport(
            body, False, error="RuntimeError: deterministic translator failure")
        translate_effect = (RuntimeError("unexpected translator crash")
                            if unexpected else None)
        contacts = {"count": 0}

        def forbidden_client(*_args, **_kwargs):
            contacts["count"] += 1
            raise AssertionError("model upstream contacted")

        with patch.object(F.db, "get_all_settings", return_value=settings), \
             patch.object(F, "_targets", return_value=[target]), \
             patch.object(F, "_order_targets", side_effect=lambda targets: targets), \
             patch.object(F.proxy_pool, "ordered_for_request",
                          return_value=[{"id": 7, "url": "http://proxy.invalid"}]), \
             patch.object(F.language_translate, "translate_request_report",
                          return_value=report, side_effect=translate_effect), \
             patch.object(F, "_build_client", side_effect=forbidden_client), \
             patch.object(F, "_consume_assemble", side_effect=forbidden_client), \
             patch.object(F.db, "add_log") as add_log, \
             patch.object(F.db, "get_setting",
                          side_effect=lambda key, default="": default), \
             patch.object(F.filters, "apply_filters",
                          side_effect=lambda value: (value, None, 0)):
            response = asyncio.run(F.forward(make_request(path, body), path.lstrip("/")))
            payload = asyncio.run(collect_response_body(response))
        self.assertEqual(contacts["count"], 0)
        self.assertIn(b"not forwarded upstream", payload)
        final_logs = [call.kwargs for call in add_log.call_args_list
                      if call.kwargs.get("final") == 1]
        self.assertEqual(len(final_logs), 1)
        self.assertIn("upstream_contacted=false", final_logs[0]["note"])
        return response, payload

    def test_responses_stream_translation_failure_uses_responses_error_grammar(self):
        response, payload = self._run_forward_failure("/v1/responses", True)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertIn(b"event: response.failed", payload)
        self.assertIn(b"translation_failed", payload)

    def test_anthropic_stream_translation_failure_zero_upstream(self):
        response, payload = self._run_forward_failure("/v1/messages", True)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertIn(b"event: error", payload)

    def test_openai_stream_translation_failure_zero_upstream(self):
        response, payload = self._run_forward_failure("/v1/chat/completions", True)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertIn(b'"type": "api_error"', payload)

    def test_anthropic_nonstream_translation_failure_zero_upstream(self):
        response, payload = self._run_forward_failure("/v1/messages", False)
        self.assertEqual(response.status_code, 503)
        self.assertIn(b"original request was not forwarded", payload)

    def test_openai_nonstream_translation_failure_zero_upstream(self):
        response, payload = self._run_forward_failure(
            "/v1/chat/completions", False)
        self.assertEqual(response.status_code, 503)
        self.assertIn(b"original request was not forwarded", payload)

    def test_anthropic_stream_unexpected_translation_exception_zero_upstream(self):
        response, payload = self._run_forward_failure(
            "/v1/messages", True, unexpected=True)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertIn(b"event: error", payload)

    def test_openai_stream_unexpected_translation_exception_zero_upstream(self):
        response, payload = self._run_forward_failure(
            "/v1/chat/completions", True, unexpected=True)
        self.assertEqual(response.media_type, "text/event-stream")
        self.assertIn(b'"type": "api_error"', payload)

    def test_nonstream_unexpected_translation_exception_zero_upstream(self):
        response, payload = self._run_forward_failure(
            "/v1/messages", False, unexpected=True)
        self.assertEqual(response.status_code, 503)
        self.assertIn(b"original request was not forwarded", payload)

    def test_format_then_language_translation_compose(self):
        tgt = {"mode": "openai", "fx_flags":
               '{"fx_translate_language":"1","fx_translate_format":"1"}'}
        body = json.dumps({"model":"x","max_tokens":20,
                           "messages":[{"role":"user","content":"नमस्ते"}]}).encode()
        observed = {}

        async def assert_format_already_applied(translated_body, kind, **_kwargs):
            data = json.loads(translated_body)
            observed["kind"] = kind
            observed["body"] = data
            self.assertEqual(kind, "openai")
            self.assertEqual(data.get("max_tokens"), 20)
            self.assertNotIn("system", data)
            return await fake_translate(translated_body, kind)

        # Format translation is fleet-global; stale endpoint-local values must
        # not control it. It must run first so language processing sees the
        # endpoint-native wire shape (OpenAI here), never the incoming shape.
        with patch.object(F.db, "get_setting", side_effect=lambda k, d=None: "1" if k == "fx_translate_format" else d), \
             patch.object(F.language_translate, "translate_request_report", side_effect=assert_format_already_applied):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        data = json.loads(out)
        self.assertEqual(observed["kind"], "openai")
        self.assertTrue(lang)
        self.assertTrue(fmt)
        self.assertEqual(data["messages"][0]["content"], "नमस्ते")
        # The wire translator intentionally drops unknown top-level fields; the
        # translated content itself is what must survive composition.

    def test_format_translation_fails_closed_for_malformed_cross_dialect_body(self):
        tgt = {"mode": "openai", "fx_flags": '{"fx_translate_format":"1"}'}
        with self.assertRaises(ValueError):
            F._xlate_request(b'{not-json', "anthropic", tgt)

    def test_nonstream_responses_upstream_assembles_with_responses_assembler(self):
        """Regression (2026-09-13): a NON-STREAM /v1/responses request whose upstream
        answers with a Responses SSE stream must be assembled by ResponsesAssembler.
        Line 1484 used to select AnthropicAssembler for kind='responses', which
        understands no Responses frame -> obj=None -> 'empty-stream' retry -> the
        request failed against a perfectly healthy upstream.
        """
        import asyncio, json
        from unittest.mock import patch
        from starlette.responses import Response as StarletteResponse

        sse = b"".join([
            b'event: response.created\ndata: ' + json.dumps({"type": "response.created",
                "response": {"id": "r1", "status": "in_progress", "output": []}}).encode() + b"\n\n",
            b'event: response.output_item.added\ndata: ' + json.dumps({"type": "response.output_item.added",
                "output_index": 0, "item": {"id": "m1", "type": "message", "role": "assistant", "content": []}}).encode() + b"\n\n",
            b'event: response.output_text.delta\ndata: ' + json.dumps({"type": "response.output_text.delta",
                "output_index": 0, "delta": "Hello world"}).encode() + b"\n\n",
            b'event: response.completed\ndata: ' + json.dumps({"type": "response.completed",
                "response": {"id": "r1", "status": "completed", "output": [],
                             "usage": {"input_tokens": 3, "output_tokens": 2}}}).encode() + b"\n\n",
        ])

        class FakeResp:
            status_code = 200
            headers = {"content-type": "text/event-stream"}
            async def aiter_bytes(self):
                yield sse

        class FakeStreamCM:
            def __init__(self, resp): self.resp = resp
            async def __aenter__(self): return self.resp
            async def __aexit__(self, *a): return False

        class FakeClient:
            def __init__(self, resp): self.resp = resp
            def stream(self, method, url, **k): return FakeStreamCM(self.resp)

        class FakeBuildCM:
            async def __aenter__(self): return FakeClient(FakeResp())
            async def __aexit__(self, *a): return False

        def fake_build(candidates, timeout, hedge_id=None):
            return FakeBuildCM()

        tgt = [{"name": "resp-ep", "base": "https://up.example", "key": "sk-x",
                "keys": ["sk-x"], "mode": "responses", "agentrouter": False, "id": 1,
                "model_override": "", "failover_trigger_keywords": "",
                "endpoint_failover_keywords": "", "key_failover_keywords": "",
                "scrape_do_token": "", "custom_proxies": "[]", "proxy_priority": "[]",
                "proxy_fallback": 0, "url": "https://up.example"}]

        body = json.dumps({"model": "x", "stream": False, "input": "Hello",
                           "instructions": "Answer in English"}).encode()

        async def run():
            with patch.object(F, "_targets", return_value=tgt), \
                 patch.object(F, "_build_client", fake_build), \
                 patch.object(F.db, "get_all_settings", return_value={
                     "require_client_key": "0", "gateway_key": "", "max_retries": "2",
                     "connect_timeout": "5", "read_timeout": "30", "write_timeout": "30",
                     "pool_timeout": "10", "endpoint": "https://up.example"}), \
                 patch.object(F.db, "get_setting", side_effect=lambda k, d=None: d or "1"), \
                 patch.object(F.db, "add_log", lambda **k: None), \
                 patch.object(F, "_resolve_candidates",
                              return_value=[{"url": "http://x", "id": 1, "proxy_type": "http"}]):
                resp = await F.forward(make_request("/v1/responses", body), "v1/responses")
                payload = b""
                if hasattr(resp, "body_iterator"):
                    payload = b"".join([c async for c in resp.body_iterator])
                else:
                    payload = resp.body
                return resp, payload

        resp, payload = asyncio.run(run())
        self.assertEqual(resp.status_code, 200)
        obj = json.loads(payload)
        self.assertEqual(obj["status"], "completed")
        self.assertEqual(obj["output"][0]["content"][0]["text"], "Hello world")
        self.assertEqual(obj["usage"]["output_tokens"], 2)

    def test_responses_endpoint_receives_language_translation_after_format_translation(self):
        tgt = {"mode": "responses", "fx_flags":
               '{"fx_translate_language":"1","fx_translate_format":"1"}'}
        body = json.dumps({"model": "x", "max_tokens": 20,
                           "messages": [{"role": "user", "content": "नमस्ते"}]},
                          ensure_ascii=False).encode()
        calls = []

        async def translating(text):
            calls.append(text)
            return "Hello"

        original_report = F.language_translate.translate_request_report

        async def real_report(translated_body, kind, **_kwargs):
            return await original_report(
                translated_body, kind, translator=translating)

        with patch.object(F.db, "get_setting", side_effect=lambda k, d=None: "1" if k == "fx_translate_format" else d), \
             patch.object(F.language_translate, "translate_request_report", side_effect=real_report):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "anthropic", tgt))
        self.assertTrue(fmt)
        self.assertTrue(lang)
        self.assertIn("नमस्ते", calls)
        self.assertEqual(json.loads(out)["input"][0]["content"][0]["text"], "Hello")

    def test_openai_request_formats_to_anthropic_before_language_translation(self):
        tgt = {"mode": "anthropic", "fx_flags":
               '{"fx_translate_language":"1"}'}
        body = json.dumps({"model": "x", "max_tokens": 20,
                           "messages": [
                               {"role": "system", "content": "नियम मानो"},
                               {"role": "user", "content": "नमस्ते"},
                           ]}, ensure_ascii=False).encode()

        async def assert_anthropic_shape(translated_body, kind, **_kwargs):
            data = json.loads(translated_body)
            self.assertEqual(kind, "anthropic")
            self.assertEqual(data.get("system"), "नियम मानो")
            self.assertFalse(any(m.get("role") == "system" for m in data["messages"]))
            return await fake_translate(translated_body, kind)

        with patch.object(F.db, "get_setting", side_effect=lambda k, d=None: "1" if k == "fx_translate_format" else d), \
             patch.object(F.language_translate, "translate_request_report", side_effect=assert_anthropic_shape):
            out, fmt, lang = asyncio.run(F._prepare_request(body, "openai", tgt))
        self.assertTrue(fmt)
        self.assertTrue(lang)
        self.assertEqual(json.loads(out)["_language_translated"], "anthropic")


if __name__ == "__main__":
    unittest.main(verbosity=2)
