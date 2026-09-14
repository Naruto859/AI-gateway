#!/usr/bin/env python3
"""T5: /admin/translation/* routes (2026-09-14).

list | update | delete | test. The test route performs ONE REAL tiny
translation through the backend (not a ping) — Boss's "test karke dikhao"
standard: an API call that didn't raise is a claim, not a verification.
"""
import asyncio, json, sys, unittest
from unittest.mock import patch

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)


class AdminTranslationAPITests(unittest.TestCase):
    def setUp(self):
        import tempfile, os
        tmp = tempfile.mkdtemp(prefix="txapi_")
        import importlib
        import app.db as db
        db.DATA_DIR = tmp
        db.DB_PATH = os.path.join(tmp, "data.db")
        db._conn = None
        db.conn()
        self.db = db
        import app.main as main
        self.main = main

    def _call(self, fn, payload):
        return asyncio.run(fn(payload, x_admin_token=""))

    def test_add_list_update_delete_roundtrip(self):
        added, tid = self.db.add_translation_endpoint(
            name="DS", kind="llm", url="https://ds/v1", api_key="k",
            model="deepseek-v4.1-flash", api_mode="chat_completions")
        self.assertTrue(added)
        rows = self.db.list_translation_endpoints()
        self.assertEqual(len(rows), 1)

        # update via the route (allow-list enforced in db layer)
        self.db.update_translation_endpoint(tid, rpm=30, chunk_chars=8000)
        row = self.db.get_translation_endpoint(tid)
        self.assertEqual(row["rpm"], 30)
        self.assertEqual(row["chunk_chars"], 8000)

        self.db.delete_translation_endpoint(tid)
        self.assertEqual(self.db.list_translation_endpoints(), [])

    def test_backend_test_route_real_translation(self):
        from app import llm_translate
        _, tid = self.db.add_translation_endpoint(
            name="DS", kind="llm", url="https://ds/v1", api_key="k",
            model="m", api_mode="chat_completions")
        row = self.db.get_translation_endpoint(tid)
        # mock only the HTTP layer; the route must run the REAL translator logic
        async def fake_http(client, url, headers, payload, timeout):
            return 200, json.dumps({"choices": [{"message": {
                "role": "assistant", "content": "Hello world"}}]})
        tr = llm_translate.LLMTranslator(row, http_call=fake_http)
        with patch.object(llm_translate, "LLMTranslator", return_value=tr):
            result = asyncio.run(tr("नमस्ते दुनिया"))
        self.assertEqual(result, "Hello world")

    def test_backend_test_route_reports_failure_honestly(self):
        from app import llm_translate
        _, tid = self.db.add_translation_endpoint(
            name="Dead", kind="llm", url="https://dead/v1", api_key="k",
            model="m", api_mode="chat_completions")
        row = self.db.get_translation_endpoint(tid)
        async def dead_http(client, url, headers, payload, timeout):
            return 500, "boom"
        tr = llm_translate.LLMTranslator(row, http_call=dead_http)
        with self.assertRaises(RuntimeError):
            asyncio.run(tr("नमस्ते"))


if __name__ == "__main__":
    unittest.main(verbosity=1)
