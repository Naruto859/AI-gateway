#!/usr/bin/env python3
"""Unit tests verifying direct routing, round-robin retry cycling, and fallback logic in _resolve_candidates."""
import json
import unittest
import sys

sys.path.insert(0, "/root/gw-preview/repo")
from app import forwarder as F


class DirectFallbackRoutingTests(unittest.TestCase):
    def test_no_proxies_fallback_off_routes_direct(self):
        """When an endpoint has NO proxies and fallback to free pool is OFF, it must go direct."""
        tgt = {
            "name": "agentrouter",
            "custom_proxies": "[]",
            "proxy_priority": "[]",
            "proxy_fallback": 0,
        }
        attempted = set()
        c = F._resolve_candidates(tgt, attempted, 3, attempt_idx=0)
        self.assertEqual(len(c), 1)
        self.assertEqual(c[0]["id"], "__direct__")
        self.assertEqual(c[0]["url"], "")

    def test_no_proxies_fallback_off_retries_direct(self):
        """When direct attempt fails and max_retries is 5, subsequent attempts must also go direct."""
        tgt = {
            "name": "agentrouter",
            "custom_proxies": "[]",
            "proxy_priority": "[]",
            "proxy_fallback": 0,
        }
        attempted = set()
        for attempt in range(5):
            c = F._resolve_candidates(tgt, attempted, 3, attempt_idx=attempt)
            self.assertEqual(len(c), 1, f"Attempt {attempt+1} returned empty candidates")
            self.assertEqual(c[0]["id"], "__direct__")
            attempted.add(c[0]["id"])

    def test_one_proxy_fallback_off_retries_on_same_proxy(self):
        """When 1 dedicated proxy is set and fallback is OFF, all retries stay on that proxy."""
        tgt = {
            "name": "one-proxy-ep",
            "custom_proxies": json.dumps(["http://100.97.11.41:8888"]),
            "proxy_priority": json.dumps(["custom_0"]),
            "proxy_fallback": 0,
        }
        attempted = set()
        for attempt in range(5):
            c = F._resolve_candidates(tgt, attempted, 3, attempt_idx=attempt)
            self.assertEqual(len(c), 1)
            self.assertEqual(c[0]["id"], "custom_0")
            attempted.add("custom_0")

    def test_three_proxies_fallback_off_cycles_round_robin(self):
        """Boss's scenario: 3 proxies configured, fallback OFF, 5 retries.
        Attempts 1->p1, 2->p2, 3->p3, then 4->p1, 5->p2 (round-robin cycle).
        """
        tgt = {
            "name": "three-proxies-ep",
            "custom_proxies": json.dumps(["http://p1:8888", "http://p2:8888", "http://p3:8888"]),
            "proxy_priority": json.dumps(["custom_0", "custom_1", "custom_2"]),
            "proxy_fallback": 0,
        }
        attempted = set()
        history = []
        for attempt in range(5):
            c = F._resolve_candidates(tgt, attempted, 3, attempt_idx=attempt)
            self.assertEqual(len(c), 1)
            history.append(c[0]["id"])
            attempted.add(c[0]["id"])

        self.assertEqual(history, ["custom_0", "custom_1", "custom_2", "custom_0", "custom_1"])

    def test_three_proxies_fallback_on_exhausts_all_before_free_pool(self):
        """When 3 dedicated proxies exist and fallback is ON:
        Attempts 1, 2, 3 MUST try all dedicated proxies first.
        It never prematurely dumps to the free pool while dedicated proxies remain.
        """
        tgt = {
            "name": "three-proxies-fb-on",
            "custom_proxies": json.dumps(["http://p1:8888", "http://p2:8888", "http://p3:8888"]),
            "proxy_priority": json.dumps(["custom_0", "custom_1", "custom_2"]),
            "proxy_fallback": 1,
        }
        attempted = set()
        # Attempt 1
        c1 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=0)
        self.assertEqual(c1[0]["id"], "custom_0")
        attempted.add("custom_0")

        # Attempt 2
        c2 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=1)
        self.assertEqual(c2[0]["id"], "custom_1")
        attempted.add("custom_1")

        # Attempt 3
        c3 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=2)
        self.assertEqual(c3[0]["id"], "custom_2")
        attempted.add("custom_2")

        # Attempt 4 (all 3 dedicated exhausted -> now and only now falls back to free pool)
        c4 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=3)
        self.assertNotEqual(c4[0]["id"], "custom_0")
        self.assertNotEqual(c4[0]["id"], "custom_1")
        self.assertNotEqual(c4[0]["id"], "custom_2")

    def test_custom_then_direct_priority(self):
        """When priority is [custom_0, direct], attempt 1 uses custom_0 and attempt 2 uses direct."""
        tgt = {
            "name": "ep-custom-then-direct",
            "custom_proxies": json.dumps(["http://100.97.11.41:8888"]),
            "proxy_priority": json.dumps(["custom_0", "direct"]),
            "proxy_fallback": 0,
        }
        attempted = set()
        c1 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=0)
        self.assertEqual(c1[0]["id"], "custom_0")
        attempted.add("custom_0")

        c2 = F._resolve_candidates(tgt, attempted, 3, attempt_idx=1)
        self.assertEqual(c2[0]["id"], "__direct__")


if __name__ == "__main__":
    unittest.main()
