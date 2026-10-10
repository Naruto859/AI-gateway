#!/usr/bin/env python3
"""Unit tests verifying direct routing and fallback logic in _resolve_candidates."""
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
        c = F._resolve_candidates(tgt, attempted, 3)
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
        for attempt in range(1, 6):
            c = F._resolve_candidates(tgt, attempted, 3)
            self.assertEqual(len(c), 1, f"Attempt {attempt} returned empty candidates")
            self.assertEqual(c[0]["id"], "__direct__")
            attempted.add(c[0]["id"])

    def test_one_proxy_fallback_off_retries_on_same_proxy(self):
        """When 1 dedicated proxy is set and fallback is OFF, retries stay on that proxy."""
        tgt = {
            "name": "one-proxy-ep",
            "custom_proxies": json.dumps(["http://100.97.11.41:8888"]),
            "proxy_priority": json.dumps(["custom_0"]),
            "proxy_fallback": 0,
        }
        attempted = set()
        # Attempt 1
        c1 = F._resolve_candidates(tgt, attempted, 3)
        self.assertEqual(c1[0]["id"], "custom_0")
        attempted.add("custom_0")

        # Attempt 2 (should retry on custom_0, not fail or return empty)
        c2 = F._resolve_candidates(tgt, attempted, 3)
        self.assertEqual(len(c2), 1)
        self.assertEqual(c2[0]["id"], "custom_0")

    def test_custom_then_direct_priority(self):
        """When priority is [custom_0, direct], attempt 1 uses custom_0 and attempt 2 uses direct."""
        tgt = {
            "name": "ep-custom-then-direct",
            "custom_proxies": json.dumps(["http://100.97.11.41:8888"]),
            "proxy_priority": json.dumps(["custom_0", "direct"]),
            "proxy_fallback": 0,
        }
        attempted = set()
        c1 = F._resolve_candidates(tgt, attempted, 3)
        self.assertEqual(c1[0]["id"], "custom_0")
        attempted.add("custom_0")

        c2 = F._resolve_candidates(tgt, attempted, 3)
        self.assertEqual(c2[0]["id"], "__direct__")


if __name__ == "__main__":
    unittest.main()
