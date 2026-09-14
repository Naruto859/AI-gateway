#!/usr/bin/env python3
"""T1: translation_endpoints table + endpoints.translation_config column + CRUD.

Boss requirement 2026-09-14: custom LLM translation backends, orderable per
endpoint (drag), Google as one backend among them. This is the DB layer.
"""
import json, os, sqlite3, sys, tempfile, unittest

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)


def fresh_db():
    """Point app.db at a scratch DATA_DIR via monkeypatching module globals.

    DATA_DIR/DB_PATH are computed from __file__ at import time (BASE = repo
    root), so setting an env var does nothing — patch the globals directly,
    like the gateway's own sandbox tests do.
    """
    tmp = tempfile.mkdtemp(prefix="txdb_")
    import importlib
    import app.db as db
    db.DATA_DIR = tmp
    db.DB_PATH = os.path.join(tmp, "data.db")
    db._conn = None
    importlib.reload(db)  # reload re-runs _init on the patched path
    db.DATA_DIR = tmp
    db.DB_PATH = os.path.join(tmp, "data.db")
    db._conn = None
    db.conn()  # force init on scratch path
    return db, tmp


class TranslationEndpointsDBTests(unittest.TestCase):
    def setUp(self):
        self.db, self.tmp = fresh_db()

    def test_table_exists_with_all_columns(self):
        cols = {r[1] for r in self.db.conn().execute("PRAGMA table_info(translation_endpoints)")}
        for c in ("id", "name", "kind", "url", "api_key", "model", "api_mode",
                  "system_prompt", "rpm", "chunk_chars", "max_output_tokens",
                  "custom_proxies", "proxy_priority", "proxy_fallback",
                  "priority", "enabled"):
            self.assertIn(c, cols, f"missing column {c}")

    def test_endpoints_table_gains_translation_config(self):
        cols = {r[1] for r in self.db.conn().execute("PRAGMA table_info(endpoints)")}
        self.assertIn("translation_config", cols)

    def test_crud_roundtrip(self):
        # add
        added, tid = self.db.add_translation_endpoint(
            name="DeepSeek", kind="llm", url="https://example.com",
            api_key="sk-x", model="deepseek-v4.1-flash", api_mode="chat_completions")
        self.assertTrue(added)
        # list
        rows = self.db.list_translation_endpoints()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "DeepSeek")
        self.assertEqual(rows[0]["model"], "deepseek-v4.1-flash")
        # update
        self.db.update_translation_endpoint(tid, model="glm-5.3", rpm=30)
        row = [r for r in self.db.list_translation_endpoints() if r["id"] == tid][0]
        self.assertEqual(row["model"], "glm-5.3")
        self.assertEqual(row["rpm"], 30)
        # delete
        self.db.delete_translation_endpoint(tid)
        self.assertEqual(self.db.list_translation_endpoints(), [])

    def test_update_allowlist_drops_unknown_fields(self):
        _, tid = self.db.add_translation_endpoint(
            name="X", kind="llm", url="https://x", api_key="", model="m", api_mode="chat_completions")
        self.db.update_translation_endpoint(tid, evil_column="pwn", name="Y")
        row = self.db.list_translation_endpoints()[0]
        self.assertNotIn("evil_column", row)
        self.assertEqual(row["name"], "Y")

    def test_legacy_endpoint_without_config_unchanged(self):
        # endpoints added the old way must still work and default to no config
        self.db.add_endpoint("https://upstream.example", name="E1")
        row = self.db.conn().execute("SELECT translation_config FROM endpoints WHERE name='E1'").fetchone()
        # default: NULL or '' — NOT 'google'; absence means legacy Google-only path
        self.assertTrue(row[0] in (None, ""))


if __name__ == "__main__":
    unittest.main(verbosity=1)
