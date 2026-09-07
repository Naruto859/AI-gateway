#!/usr/bin/env python3
"""Behavior tests for per-endpoint Hindi/Hinglish request translation."""
import asyncio
import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "/root/gw-preview/repo")
from app import language_translate as L


class FakeTranslator:
    def __init__(self):
        self.seen = []

    async def __call__(self, text):
        self.seen.append(text)
        return "EN<" + text + ">"


class DetectionTests(unittest.TestCase):
    def test_detects_devanagari(self):
        self.assertTrue(L.needs_translation("AgentRouter सिर्फ Hindi रोकता है"))

    def test_detects_roman_hinglish(self):
        self.assertTrue(L.needs_translation("kal mujhe result bata dena"))

    def test_does_not_mistake_technical_english_for_hinglish(self):
        self.assertFalse(L.needs_translation("Use the main API route and keep this key unchanged."))
    def test_real_google_translator_handles_hindi_and_hinglish(self):
        samples = [
            "आप Ciel हैं और context सुरक्षित रखो।",
            "AgentRouter सिर्फ Hindi block karta hai.",
            "कल मुझे result बताना।",
        ]
        output = asyncio.run(L._google_translate_batch(samples))
        self.assertEqual(len(output), len(samples))
        self.assertTrue(all(L.needs_translation(x) is False for x in output))


class RequestTranslationTests(unittest.TestCase):
    def run_translate(self, body, kind="anthropic"):
        fake = FakeTranslator()
        raw, changed = asyncio.run(L.translate_request(
            json.dumps(body, ensure_ascii=False).encode(), kind, translator=fake))
        return json.loads(raw), changed, fake.seen

    def test_anthropic_translates_natural_language_but_preserves_protocol(self):
        body = {
            "model": "claude-opus-4-8",
            "system": "आप Ciel हैं. Keep API names exact.",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "ये URL https://example.com और `max_tokens` मत बदलना"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}},
                ]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "ठीक है"},
                    {"type": "tool_use", "id": "toolu_exact", "name": "read_file", "input": {"path": "/tmp/हिंदी.txt"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_exact", "content": "फाइल नहीं मिली"}
                ]},
            ],
        }
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        self.assertTrue(out["system"].startswith("EN<"))
        self.assertIn("https://example.com", out["messages"][0]["content"][0]["text"])
        self.assertIn("`max_tokens`", out["messages"][0]["content"][0]["text"])
        tool = out["messages"][1]["content"][1]
        self.assertEqual(tool["id"], "toolu_exact")
        self.assertEqual(tool["input"], {"path": "/tmp/हिंदी.txt"})
        result = out["messages"][2]["content"][0]
        self.assertEqual(result["tool_use_id"], "toolu_exact")
        self.assertTrue(result["content"].startswith("EN<"))
        self.assertTrue(any("Respond in the user's original" in s for s in (out["system"] if isinstance(out["system"], list) else [out["system"]])))

    def test_openai_preserves_tool_call_arguments_and_translates_tool_output(self):
        body = {"messages": [
            {"role": "system", "content": "आप Ciel हैं"},
            {"role": "user", "content": "kal mujhe bata dena"},
            {"role": "assistant", "content": "कर रही हूँ", "tool_calls": [{
                "id": "call_exact", "type": "function",
                "function": {"name": "search", "arguments": '{"q":"हिंदी"}'},
            }]},
            {"role": "tool", "tool_call_id": "call_exact", "content": "नतीजा मिला"},
        ]}
        out, changed, _ = self.run_translate(body, "openai")
        self.assertTrue(changed)
        self.assertEqual(out["messages"][2]["tool_calls"][0]["id"], "call_exact")
        self.assertEqual(out["messages"][2]["tool_calls"][0]["function"]["arguments"], '{"q":"हिंदी"}')
        self.assertTrue(out["messages"][3]["content"].startswith("EN<"))

    def test_english_only_body_is_byte_exact(self):
        raw = b'{"model":"x","messages":[{"role":"user","content":"hello world"}]}'
        fake = FakeTranslator()
        out, changed = asyncio.run(L.translate_request(raw, "anthropic", translator=fake))
        self.assertFalse(changed)
        self.assertEqual(out, raw)
        self.assertEqual(fake.seen, [])

    def test_default_path_batches_many_context_fields_into_one_call(self):
        body = {"system": "आप Ciel हैं", "messages": [
            {"role": "user", "content": f"मुझे result बताना {i}"} for i in range(20)
        ]}
        calls = []
        async def fake_batch(parts):
            calls.append(list(parts))
            return ["English " + str(i) for i, _ in enumerate(parts)]
        with patch.object(L, "_google_translate_batch", side_effect=fake_batch):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(json.loads(raw)["messages"]), 20)

    def test_default_batch_path_restores_protected_code_and_url(self):
        body = {"messages": [{"role": "user", "content":
            "यह `API_KEY` और https://x.test/हिंदी बिल्कुल मत बदलना"}]}
        async def fake_batch(parts):
            return ["English " + part for part in parts]
        with patch.object(L, "_google_translate_batch", side_effect=fake_batch):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        text = json.loads(raw)["messages"][0]["content"]
        self.assertIn("`API_KEY`", text)
        self.assertIn("https://x.test/हिंदी", text)

    def test_translation_failure_returns_original_bytes(self):
        body = {"messages": [{"role": "user", "content": "मुझे result बताना"}]}
        raw = json.dumps(body, ensure_ascii=False).encode()
        async def failed_batch(parts):
            raise RuntimeError("translator unavailable")
        with patch.object(L, "_google_translate_batch", side_effect=failed_batch):
            out, changed = asyncio.run(L.translate_request(raw, "anthropic"))
        self.assertFalse(changed)
        self.assertEqual(out, raw)

    def test_code_fences_inline_code_urls_and_json_are_never_sent_to_translator(self):
        body = {"messages": [{"role": "user", "content":
            "यह देखो ```python\nprint('नमस्ते')\n``` और `API_KEY` https://x.test/p?q=हिंदी और {\"name\":\"हिंदी\"}"}]}
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        sent = "\n".join(seen)
        self.assertNotIn("print('नमस्ते')", sent)
        self.assertNotIn("API_KEY", sent)
        self.assertNotIn("https://x.test", sent)
        self.assertNotIn('{"name":"हिंदी"}', sent)
        text = out["messages"][0]["content"]
        self.assertIn("```python\nprint('नमस्ते')\n```", text)
        self.assertIn("`API_KEY`", text)
        self.assertIn("https://x.test/p?q=हिंदी", text)
        self.assertIn('{"name":"हिंदी"}', text)

    def test_preserves_spacing_around_protected_spans(self):
        body = {"messages": [{"role": "user", "content":
            "API key मत बदलना। `max_tokens` same रखो।"}]}
        out, changed, _ = self.run_translate(body)
        self.assertTrue(changed)
        text = out["messages"][0]["content"]
        self.assertRegex(text, r">\s+`max_tokens`\s+EN<")


if __name__ == "__main__":
    unittest.main(verbosity=2)
