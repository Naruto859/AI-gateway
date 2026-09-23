#!/usr/bin/env python3
"""Guard: a masked secret echoed back from the dashboard must never overwrite
the real stored secret (2026-09-23).

The dashboard loads api_key / scrape_do_token / extra_keys as their masked view
("••••last4") and posts them back on EVERY save. Before this guard, saving an
endpoint after changing only (say) the model destroyed its API key. These tests
assert the pure helpers in app.main that implement the guard.
"""
import json, sys, unittest

sys.path.insert(0, "/root/gw-preview/repo")
from app import main as M


class MaskGuardTests(unittest.TestCase):
    def test_masked_api_key_is_dropped(self):
        stored = "sk-realsecret1234"
        payload = {"api_key": "••••1234", "model_override": "x"}
        M._keep_secret_if_masked(payload, stored)
        self.assertNotIn("api_key", payload)          # preserved (dropped from write)
        self.assertEqual(payload["model_override"], "x")

    def test_real_new_key_passes_through(self):
        payload = {"api_key": "sk-brandnewkey"}
        M._keep_secret_if_masked(payload, "sk-old")
        self.assertEqual(payload["api_key"], "sk-brandnewkey")

    def test_explicit_clear_passes_through(self):
        payload = {"api_key": ""}
        M._keep_secret_if_masked(payload, "sk-old")
        self.assertIn("api_key", payload)
        self.assertEqual(payload["api_key"], "")

    def test_scrape_token_field(self):
        payload = {"scrape_do_token": "••••ABCD"}
        M._keep_secret_if_masked(payload, "token-realABCD", field="scrape_do_token")
        self.assertNotIn("scrape_do_token", payload)

    def test_extra_keys_masked_restored_by_last4(self):
        stored = json.dumps(["sk-aaaa1111", "sk-bbbb2222"])
        payload = {"extra_keys": json.dumps(["••••1111", "••••2222"])}
        M._unmask_extra_keys(payload, stored)
        self.assertEqual(json.loads(payload["extra_keys"]),
                         ["sk-aaaa1111", "sk-bbbb2222"])

    def test_extra_keys_mixed_new_and_masked(self):
        stored = json.dumps(["sk-aaaa1111"])
        payload = {"extra_keys": json.dumps(["••••1111", "sk-cccc3333"])}
        M._unmask_extra_keys(payload, stored)
        self.assertEqual(json.loads(payload["extra_keys"]),
                         ["sk-aaaa1111", "sk-cccc3333"])

    def test_extra_keys_unmatchable_mask_dropped(self):
        stored = json.dumps(["sk-aaaa1111"])
        payload = {"extra_keys": json.dumps(["••••9999"])}  # no stored key ends 9999
        M._unmask_extra_keys(payload, stored)
        self.assertEqual(json.loads(payload["extra_keys"]), [])

    def test_is_masked_detects_bullet_prefix(self):
        self.assertTrue(M._is_masked("••••1234"))
        self.assertFalse(M._is_masked("sk-1234"))
        self.assertFalse(M._is_masked(""))
        self.assertFalse(M._is_masked(None))


if __name__ == "__main__":
    unittest.main(verbosity=1)
