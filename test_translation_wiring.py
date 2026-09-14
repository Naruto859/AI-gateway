#!/usr/bin/env python3
"""T4: forwarder wiring — translation_config -> ordered backend chain (2026-09-14).

Contract:
- Endpoint WITHOUT translation_config (legacy): Google-only, byte-for-byte
  the old call — no behavior change for anything already deployed.
- Endpoint WITH translation_config: backends resolved in the user's drag
  order; "google" = builtin, numeric id = translation_endpoints row ->
  LLMTranslator; missing/disabled id = SKIPPED (not fatal — the rest still
  try); chain passed to translate_request_report(backends=[...]).
"""
import asyncio, json, sys, unittest
from unittest.mock import patch

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)

from app import forwarder as F


def tgt_with(config, proxies='[]', priority='[]'):
    return {
        "mode": "anthropic",
        "fx_flags": '{"fx_translate_language":"1"}',
        "custom_proxies": proxies,
        "proxy_priority": priority,
        "translation_config": config,
    }


class WiringTests(unittest.TestCase):
    def test_legacy_no_config_calls_google_exactly_as_before(self):
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        tgt = tgt_with("")  # legacy: empty config
        with patch.object(F.language_translate, "translate_request_report") as tr:
            tr.side_effect = lambda *a, **k: F.language_translate.TranslationReport(body, True)
            asyncio.run(F._prepare_request(body, "anthropic", tgt))
        tr.assert_called_once()
        args, kwargs = tr.call_args
        # legacy: chain resolves to None -> byte-for-byte the old Google path
        self.assertIsNone(kwargs.get("backends"))

    def test_config_resolves_chain_in_drag_order(self):
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        tgt = tgt_with(json.dumps(["3", "google", "5"]))
        fake_rows = {
            3: {"id": 3, "name": "DS", "kind": "llm", "url": "https://a/v1",
                "api_key": "k", "model": "m", "api_mode": "chat_completions",
                "system_prompt": "", "rpm": 0, "chunk_chars": 4000,
                "max_output_tokens": 0, "custom_proxies": "[]",
                "proxy_priority": "[]", "proxy_fallback": 1,
                "priority": 0, "enabled": 1},
            5: {"id": 5, "name": "GLM", "kind": "llm", "url": "https://b/v1",
                "api_key": "k", "model": "g", "api_mode": "anthropic_messages",
                "system_prompt": "", "rpm": 0, "chunk_chars": 4000,
                "max_output_tokens": 0, "custom_proxies": "[]",
                "proxy_priority": "[]", "proxy_fallback": 1,
                "priority": 1, "enabled": 1},
        }
        with patch.object(F.db, "get_translation_endpoint",
                          side_effect=lambda eid: fake_rows.get(eid)), \
             patch.object(F.language_translate, "translate_request_report") as tr:
            tr.side_effect = lambda *a, **k: F.language_translate.TranslationReport(body, True)
            asyncio.run(F._prepare_request(body, "anthropic", tgt))
        _, kwargs = tr.call_args
        backends = kwargs["backends"]
        self.assertEqual(len(backends), 3)
        self.assertEqual(backends[1], "google")
        self.assertEqual(backends[0].name, "DS")       # drag order preserved
        self.assertEqual(backends[2].name, "GLM")

    def test_missing_or_disabled_backend_ids_skipped(self):
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        tgt = tgt_with(json.dumps(["999", "google"]))
        with patch.object(F.db, "get_translation_endpoint",
                          return_value=None), \
             patch.object(F.language_translate, "translate_request_report") as tr:
            tr.side_effect = lambda *a, **k: F.language_translate.TranslationReport(body, True)
            asyncio.run(F._prepare_request(body, "anthropic", tgt))
        _, kwargs = tr.call_args
        self.assertEqual(kwargs["backends"], ["google"])  # dead id skipped, not fatal

    def test_disabled_row_skipped(self):
        body = '{"messages":[{"role":"user","content":"नमस्ते"}]}'.encode()
        tgt = tgt_with(json.dumps(["3", "google"]))
        row = {"id": 3, "name": "DS", "kind": "llm", "url": "https://a",
               "api_key": "k", "model": "m", "api_mode": "chat_completions",
               "system_prompt": "", "rpm": 0, "chunk_chars": 4000,
               "max_output_tokens": 0, "custom_proxies": "[]",
               "proxy_priority": "[]", "proxy_fallback": 1,
               "priority": 0, "enabled": 0}  # DISABLED
        with patch.object(F.db, "get_translation_endpoint",
                          return_value=row), \
             patch.object(F.language_translate, "translate_request_report") as tr:
            tr.side_effect = lambda *a, **k: F.language_translate.TranslationReport(body, True)
            asyncio.run(F._prepare_request(body, "anthropic", tgt))
        _, kwargs = tr.call_args
        self.assertEqual(kwargs["backends"], ["google"])


if __name__ == "__main__":
    unittest.main(verbosity=1)
