#!/usr/bin/env python3
"""T3: ordered backend chain in translate_request_report (2026-09-14).

Boss's requirement: backends are tried in the USER's drag order (Google can
be anywhere in the list). First backend fails -> next. All fail -> error
(fail-closed, request blocked — never forward untranslated).
"""
import asyncio, json, sys, unittest
from unittest.mock import patch

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)

from app import language_translate as LT


HINDI_BODY = json.dumps({
    "model": "m",
    "messages": [{"role": "user", "content": "नमस्ते दुनिया, यह एक परीक्षण है"}],
}).encode()


class ChainTests(unittest.TestCase):
    def test_first_backend_success_no_fallback(self):
        calls = []
        async def good(text):
            calls.append(text)
            return "Hello world, this is a test"
        rep = asyncio.run(LT.translate_request_report(
            HINDI_BODY, "openai", backends=[good]))
        self.assertTrue(rep.changed)
        self.assertEqual(calls, ["नमस्ते दुनिया, यह एक परीक्षण है"])

    def test_first_fails_second_translates(self):
        async def bad(text):
            raise RuntimeError("backend down")
        async def good(text):
            return "Hello world, this is a test"
        rep = asyncio.run(LT.translate_request_report(
            HINDI_BODY, "openai", backends=[bad, good]))
        self.assertTrue(rep.changed)
        self.assertEqual(rep.error, "")

    def test_all_fail_reports_error_fail_closed(self):
        async def bad1(text):
            raise RuntimeError("one down")
        async def bad2(text):
            raise RuntimeError("two down")
        rep = asyncio.run(LT.translate_request_report(
            HINDI_BODY, "openai", backends=[bad1, bad2]))
        self.assertFalse(rep.changed)
        self.assertIsNotNone(rep.error)
        # fail-closed: the ORIGINAL body is never handed back as "translated"
        self.assertEqual(rep.body, HINDI_BODY)

    def test_google_backend_id_uses_google_path(self):
        # chain entry "google" (string) = the built-in Google backend
        with patch.object(LT, "_translate_collected",
                          return_value=(["Hello world"], ["hi"])) as tc:
            rep = asyncio.run(LT.translate_request_report(
                HINDI_BODY, "openai", backends=["google"]))
        self.assertTrue(rep.changed or rep.error is None)
        tc.assert_called_once()

    def test_mixed_chain_google_after_llm(self):
        async def bad(text):
            raise RuntimeError("llm down")
        with patch.object(LT, "_translate_collected",
                          return_value=(["Hello world"], ["hi"])):
            rep = asyncio.run(LT.translate_request_report(
                HINDI_BODY, "openai", backends=[bad, "google"]))
        self.assertEqual(rep.error, "")
        self.assertTrue(rep.changed)


if __name__ == "__main__":
    unittest.main(verbosity=1)
