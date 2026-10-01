#!/usr/bin/env python3
"""Automated tests for gpt-6-astra function tools translation in AI-Gateway-v7 (Phoenix).

Verifies:
1. Anthropic -> Responses request translation with function tools.
2. Anthropic turn-2 tool_result -> Responses function_call_output translation.
3. Responses -> Anthropic response translation (function_call + reasoning -> tool_use block).
4. Responses -> Anthropic streaming translation (SSE frames) without duplicate/phantom tool calls.
5. Forwarder endpoint resolution for gpt-6-astra on AgentRouter (dialect forced to 'responses').
6. Forwarder target URL and headers correctly target /v1/responses with Bearer auth.
"""

import json
import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import translate, forwarder


class TestGpt6AstraToolsTranslation(unittest.TestCase):
    def test_anthropic_request_to_responses_with_tools(self):
        """Anthropic messages request with tools maps cleanly to Responses format."""
        hermes_req = {
            "model": "claude-opus-4-8",
            "max_tokens": 1024,
            "messages": [
                {"role": "user", "content": "What is the weather in Delhi?"}
            ],
            "tools": [
                {
                    "name": "get_weather",
                    "description": "Get current weather for a city",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string", "description": "City name"}
                        },
                        "required": ["city"],
                    },
                }
            ],
        }

        # Apply model override mutation
        mutated = forwarder._mutate_body(
            json.dumps(hermes_req).encode("utf-8"),
            want_stream=False,
            model_override="gpt-6-astra",
        )
        body_dict = json.loads(mutated)
        self.assertEqual(body_dict["model"], "gpt-6-astra")

        # Translate to Responses
        responses_req = translate.request_to("anthropic", "responses", body_dict)
        self.assertEqual(responses_req["model"], "gpt-6-astra")
        self.assertEqual(responses_req["max_output_tokens"], 1024)
        self.assertIn("tools", responses_req)
        self.assertEqual(len(responses_req["tools"]), 1)
        tool = responses_req["tools"][0]
        self.assertEqual(tool["type"], "function")
        self.assertEqual(tool["name"], "get_weather")
        self.assertEqual(tool["description"], "Get current weather for a city")
        self.assertEqual(tool["parameters"]["required"], ["city"])

        # Input messages format
        self.assertIn("input", responses_req)
        self.assertEqual(len(responses_req["input"]), 1)
        self.assertEqual(responses_req["input"][0]["role"], "user")

    def test_turn2_tool_result_translation(self):
        """Hermes tool_result turn is translated to function_call_output in Responses."""
        hermes_turn2 = {
            "model": "gpt-6-astra",
            "messages": [
                {"role": "user", "content": "What is the weather in Delhi?"},
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "call_weather_001",
                            "name": "get_weather",
                            "input": {"city": "Delhi"},
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": "call_weather_001",
                            "content": "Sunny, 28°C",
                        }
                    ],
                },
            ],
        }

        responses_req = translate.request_to("anthropic", "responses", hermes_turn2)
        items = responses_req["input"]
        # Find function_call and function_call_output
        fc_items = [i for i in items if i.get("type") == "function_call"]
        fco_items = [i for i in items if i.get("type") == "function_call_output"]

        self.assertEqual(len(fc_items), 1)
        self.assertEqual(fc_items[0]["call_id"], "call_weather_001")
        self.assertEqual(fc_items[0]["name"], "get_weather")

        self.assertEqual(len(fco_items), 1)
        self.assertEqual(fco_items[0]["call_id"], "call_weather_001")
        self.assertEqual(fco_items[0]["output"], "Sunny, 28°C")

    def test_responses_response_to_anthropic(self):
        """Upstream Responses response with reasoning and function_call translates to Anthropic."""
        resp_obj = {
            "id": "resp_001",
            "object": "response",
            "created_at": 1790835000,
            "status": "completed",
            "model": "gpt-6-astra",
            "output": [
                {
                    "type": "reasoning",
                    "id": "reas_001",
                    "status": "completed",
                    "summary": [],
                },
                {
                    "type": "function_call",
                    "id": "fc_001",
                    "call_id": "call_weather_001",
                    "name": "get_weather",
                    "arguments": json.dumps({"city": "Delhi"}),
                },
            ],
            "usage": {"input_tokens": 42, "output_tokens": 18},
        }

        ant_resp = translate.response_to("responses", "anthropic", resp_obj)
        self.assertEqual(ant_resp["type"], "message")
        self.assertEqual(ant_resp["role"], "assistant")
        self.assertEqual(ant_resp["stop_reason"], "tool_use")
        self.assertEqual(len(ant_resp["content"]), 1)
        block = ant_resp["content"][0]
        self.assertEqual(block["type"], "tool_use")
        self.assertEqual(block["id"], "call_weather_001")
        self.assertEqual(block["name"], "get_weather")
        self.assertEqual(block["input"], {"city": "Delhi"})

    def test_streaming_responses_to_anthropic_single_clean_tool_block(self):
        """Streaming Responses SSE -> Anthropic SSE emits exactly one tool_use block without phantoms."""
        stream = translate.stream_translator("responses", "anthropic", model="gpt-6-astra")

        # Typical SSE frames from upstream OpenAI Responses API
        events = [
            ("response.created", json.dumps({"type": "response.created", "response": {"id": "resp_1", "model": "gpt-6-astra"}})),
            ("response.output_item.added", json.dumps({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": ""},
            })),
            ("response.function_call_arguments.delta", json.dumps({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": '{"city": "',
            })),
            ("response.function_call_arguments.delta", json.dumps({
                "type": "response.function_call_arguments.delta",
                "output_index": 0,
                "delta": 'Delhi"}',
            })),
            ("response.output_item.done", json.dumps({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": '{"city": "Delhi"}'},
            })),
            ("response.completed", json.dumps({
                "type": "response.completed",
                "response": {
                    "id": "resp_1",
                    "status": "completed",
                    "output": [{"id": "fc_1", "type": "function_call", "call_id": "call_1", "name": "get_weather", "arguments": '{"city": "Delhi"}'}],
                    "usage": {"input_tokens": 10, "output_tokens": 20},
                },
            })),
        ]

        chunks = []
        for ev, data in events:
            chunks.extend(stream.feed(ev, data))
        chunks.extend(stream.finalize())

        text = b"".join(chunks).decode("utf-8")
        # Check that content_block_start appears exactly ONCE
        block_starts = [line for line in text.splitlines() if line.startswith("event: content_block_start")]
        self.assertEqual(len(block_starts), 1, f"Expected exactly 1 content_block_start, got: {block_starts}")

        # Check that tool call name and id are in block start
        self.assertIn('"type": "tool_use"', text)
        self.assertIn('"name": "get_weather"', text)
        self.assertIn('"id": "call_1"', text)
        self.assertIn('"partial_json": "{\\"city\\": \\""', text)
        self.assertIn('"partial_json": "Delhi\\"}"', text)
        self.assertIn('"stop_reason": "tool_use"', text)

    def test_forwarder_targets_gpt6_astra_dialect_resolution(self):
        """Forwarder _targets resolves gpt-6-astra on AgentRouter to dialect 'responses'."""
        endpoint = {
            "id": 12,
            "url": "https://agentrouter.org",
            "api_key": "sk-dummy-test-key",
            "api_mode": "anthropic_messages",  # Even if misconfigured as anthropic_messages in DB
            "model_override": "gpt-6-astra",
            "enabled": 1,
            "is_primary": 1,
        }
        from unittest.mock import patch
        with patch.object(forwarder.db, "list_endpoints", return_value=[endpoint]):
            targets = forwarder._targets({})
        self.assertEqual(len(targets), 1)
        tgt = targets[0]
        self.assertEqual(tgt["mode"], "responses")
        self.assertEqual(tgt["model_override"], "gpt-6-astra")

        # And target URL routes to /v1/responses
        url = forwarder._target_url(tgt["base"], "v1/messages", tgt["mode"])
        self.assertEqual(url, "https://agentrouter.org/v1/responses")

        # And headers use Bearer auth
        from unittest.mock import MagicMock
        req = MagicMock()
        req.headers = {}
        headers = forwarder._target_headers(req, tgt, key=tgt["key"])
        self.assertEqual(headers["authorization"], "Bearer sk-dummy-test-key")
        self.assertNotIn("x-api-key", headers)


    def test_end_to_end_forward_pipeline_anthropic_to_responses_and_back(self):
        """End-to-end forwarder test: Anthropic /v1/messages -> upstream Responses -> Anthropic tool_use."""
        import asyncio
        from unittest.mock import patch, MagicMock

        captured_upstream_requests = []

        class FakeResponse:
            def __init__(self, status_code, content, headers=None):
                self.status_code = status_code
                self._content = content
                self.headers = headers or {"content-type": "application/json"}

            async def aread(self):
                return self._content

            async def aclose(self):
                pass

        from contextlib import asynccontextmanager

        class FakeClient:
            @asynccontextmanager
            async def stream(self, method, url, headers=None, content=None):
                captured_upstream_requests.append({
                    "method": method,
                    "url": str(url),
                    "headers": dict(headers or {}),
                    "content": content,
                })
                # Upstream Responses payload with function_call
                resp_payload = {
                    "id": "resp_astra_99",
                    "object": "response",
                    "status": "completed",
                    "model": "gpt-6-astra",
                    "output": [
                        {
                            "type": "reasoning",
                            "id": "reas_99",
                            "status": "completed",
                            "summary": [],
                        },
                        {
                            "type": "function_call",
                            "call_id": "call_astra_123",
                            "name": "calculator",
                            "arguments": json.dumps({"expression": "21 * 2"}),
                        },
                    ],
                    "usage": {"input_tokens": 30, "output_tokens": 12},
                }
                yield FakeResponse(200, json.dumps(resp_payload).encode("utf-8"))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

        def fake_build(*args, **kwargs):
            return FakeClient()

        tgt = [{
            "id": 12,
            "name": "agentrouter.org",
            "base": "https://agentrouter.org",
            "key": "sk-test-key",
            "keys": ["sk-test-key"],
            "mode": "responses",
            "model_override": "gpt-6-astra",
            "fx_flags": "",
            "scrape_do_token": "",
            "custom_proxies": "[]",
            "proxy_priority": "[]",
            "proxy_fallback": 1,
        }]

        hermes_req = {
            "model": "claude-opus-4-8",
            "max_tokens": 512,
            "stream": False,
            "messages": [{"role": "user", "content": "What is 21 * 2?"}],
            "tools": [{
                "name": "calculator",
                "description": "Calculate math expression",
                "input_schema": {
                    "type": "object",
                    "properties": {"expression": {"type": "string"}},
                    "required": ["expression"],
                },
            }],
        }
        body_bytes = json.dumps(hermes_req).encode("utf-8")

        sent = False
        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        from starlette.requests import Request
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/v1/messages",
            "raw_path": b"/v1/messages",
            "query_string": b"",
            "headers": [(b"content-type", b"application/json"), (b"x-api-key", b"sk-client-key")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        req = Request(scope, receive=receive)

        async def run():
            with patch.object(forwarder, "_targets", return_value=tgt), \
                 patch.object(forwarder, "_build_client", fake_build), \
                 patch.object(forwarder.db, "get_all_settings", return_value={
                     "require_client_key": "0", "gateway_key": "", "max_retries": "1",
                     "connect_timeout": "5", "read_timeout": "30", "write_timeout": "30",
                     "pool_timeout": "10", "endpoint": "https://agentrouter.org"}), \
                 patch.object(forwarder.db, "get_setting", side_effect=lambda k, d=None: d or "1"), \
                 patch.object(forwarder.db, "add_log", lambda **k: None), \
                 patch.object(forwarder, "_resolve_candidates",
                              return_value=[{"url": "direct://", "id": 1, "proxy_type": "direct"}]):
                resp = await forwarder.forward(req, "v1/messages")
                payload = b""
                if hasattr(resp, "body_iterator"):
                    payload = b"".join([c async for c in resp.body_iterator])
                else:
                    payload = resp.body
                return resp, payload

        resp, payload = asyncio.run(run())
        self.assertEqual(resp.status_code, 200)

        # 1. Verify upstream request was translated to /v1/responses with model gpt-6-astra
        self.assertEqual(len(captured_upstream_requests), 1)
        up_req = captured_upstream_requests[0]
        self.assertEqual(up_req["url"], "https://agentrouter.org/v1/responses")
        self.assertEqual(up_req["headers"].get("authorization"), "Bearer sk-test-key")
        up_body = json.loads(up_req["content"].decode("utf-8"))
        self.assertEqual(up_body["model"], "gpt-6-astra")
        self.assertIn("tools", up_body)
        self.assertEqual(up_body["tools"][0]["name"], "calculator")

        # 2. Verify returned client response is in Anthropic format with tool_use block
        client_resp = json.loads(payload.decode("utf-8"))
        self.assertEqual(client_resp["type"], "message")
        self.assertEqual(client_resp["role"], "assistant")
        self.assertEqual(client_resp["stop_reason"], "tool_use")
        self.assertEqual(len(client_resp["content"]), 1)
        tool_block = client_resp["content"][0]
        self.assertEqual(tool_block["type"], "tool_use")
        self.assertEqual(tool_block["id"], "call_astra_123")
        self.assertEqual(tool_block["name"], "calculator")
        self.assertEqual(tool_block["input"], {"expression": "21 * 2"})


if __name__ == "__main__":
    unittest.main()
