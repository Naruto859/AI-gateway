#!/usr/bin/env python3
"""Guard: the intended password-gated per-endpoint key REVEAL must exist and be
admin-gated, WITHOUT reopening the /admin/state bulk-secret leak (2026-09-24).

Two distinct paths, asserted here end-to-end against a throwaway DB seeded with a
known admin password + real (long) endpoint keys:

1. /admin/state stays MASKED — no full api_key ever rides in the bulk blob.
2. /admin/endpoint/key reveals ONE endpoint's real key ON DEMAND, only with a
   valid admin token; an unauthenticated call is 401 and leaks nothing; a
   different endpoint id returns only its own key; unknown id -> 404; and it
   never carries admin_password / gateway_key / agent_brain.

The app reads its DB from app.db.DB_PATH (derived from the repo path), so we point
db at a temp copy before importing main, and restore afterwards.
"""
import json
import os
import sys
import tempfile
import unittest

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)

# ---- seed a throwaway DB and point app.db at it BEFORE importing app.main ----
import sqlite3
from app import db as _db

_ADMIN_PW = "test-admin-pw-42"
_GATEWAY_KEY = "sk-gatewaykey-should-never-leak-0000000000000000"
_EP12_KEY = "sk-ep12-REAL-secret-key-51chars-000000000000000abcd"
_EP7_KEY = "sk-ep7-REAL-secret-key-different-1111111111111wxyz"
_EP12_EXTRA = ["sk-ep12-spare-AAAA1111", "sk-ep12-spare-BBBB2222"]

_tmpdir = tempfile.mkdtemp(prefix="gw-reveal-test-")
_tmp_db = os.path.join(_tmpdir, "data.db")


def _seed():
    # Build a minimal schema by copying the live schema, then insert known rows.
    src = sqlite3.connect(f"file:{os.path.join(REPO, 'data', 'data.db')}?mode=ro", uri=True)
    ddl = [r[0] for r in src.execute(
        "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL").fetchall()]
    src.close()
    con = sqlite3.connect(_tmp_db)
    for stmt in ddl:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass
    con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('admin_password',?)", (_ADMIN_PW,))
    con.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('gateway_key',?)", (_GATEWAY_KEY,))
    # two endpoints with distinct real keys; ep12 also has spare keys
    con.execute(
        "INSERT INTO endpoints(id,url,api_mode,api_key,extra_keys,priority,is_primary,name) "
        "VALUES(12,'https://agentrouter.org','anthropic_messages',?,?,1,1,'agentrouter')",
        (_EP12_KEY, json.dumps(_EP12_EXTRA)))
    con.execute(
        "INSERT INTO endpoints(id,url,api_mode,api_key,extra_keys,priority,is_primary,name) "
        "VALUES(7,'https://gorouter.example','anthropic_messages',?,'[]',2,0,'gorouter')",
        (_EP7_KEY,))
    con.commit()
    con.close()


_seed()
# Redirect the module-level DB handle to our temp copy.
_db.DB_PATH = _tmp_db
_db._conn = None  # force reconnect against the temp DB

from fastapi.testclient import TestClient  # noqa: E402
from app import main as M  # noqa: E402

client = TestClient(M.app)
AUTH = {"x-admin-token": _ADMIN_PW}


class RevealRouteTests(unittest.TestCase):
    def test_state_stays_masked(self):
        r = client.get("/admin/state", headers=AUTH)
        self.assertEqual(r.status_code, 200)
        body = r.text
        # no full endpoint key, no gateway key, no admin password in the bulk blob
        self.assertNotIn(_EP12_KEY, body)
        self.assertNotIn(_EP7_KEY, body)
        self.assertNotIn(_GATEWAY_KEY, body)
        self.assertNotIn(_ADMIN_PW, body)
        eps = {e["id"]: e for e in r.json()["endpoints"]}
        self.assertTrue(eps[12]["api_key"].startswith("••••"))

    def test_reveal_requires_admin(self):
        r = client.post("/admin/endpoint/key", json={"id": 12})  # no token
        self.assertEqual(r.status_code, 401)
        self.assertNotIn(_EP12_KEY, r.text)

    def test_reveal_returns_real_key_for_that_endpoint(self):
        r = client.post("/admin/endpoint/key", json={"id": 12}, headers=AUTH)
        self.assertEqual(r.status_code, 200)
        j = r.json()
        self.assertEqual(j["api_key"], _EP12_KEY)
        self.assertEqual(j["extra_keys"], _EP12_EXTRA)
        # only this endpoint's key, never global secrets
        self.assertNotIn(_GATEWAY_KEY, r.text)
        self.assertNotIn(_ADMIN_PW, r.text)
        self.assertNotIn(_EP7_KEY, r.text)

    def test_reveal_is_per_endpoint(self):
        r = client.post("/admin/endpoint/key", json={"id": 7}, headers=AUTH)
        self.assertEqual(r.status_code, 200)
        j = r.json()
        self.assertEqual(j["api_key"], _EP7_KEY)
        self.assertNotIn(_EP12_KEY, r.text)

    def test_reveal_unknown_endpoint_404(self):
        r = client.post("/admin/endpoint/key", json={"id": 99999}, headers=AUTH)
        self.assertEqual(r.status_code, 404)

    def test_reveal_bad_token_401(self):
        r = client.post("/admin/endpoint/key", json={"id": 12},
                        headers={"x-admin-token": "wrong"})
        self.assertEqual(r.status_code, 401)
        self.assertNotIn(_EP12_KEY, r.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
