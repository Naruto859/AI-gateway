#!/usr/bin/env python3
"""Regression tests for request-dialect-aware endpoint selection.

Updated 2026-09-15 for the PRIMARY-FIRST hierarchy (Boss): the primary
endpoint is always tried first regardless of dialect; only among the
REMAINING endpoints does native-dialect come before translated. When no
primary is flagged, the old native-first behaviour applies.
"""
import sys
import unittest

sys.path.insert(0, "/root/gw-preview/repo")
from app import forwarder as F


class DialectRoutingTests(unittest.TestCase):
    def setUp(self):
        # No is_primary flags here: plain priority order 1,2,3,4.
        self.targets = [
            {"id": 1, "name": "primary-openai", "mode": "openai"},
            {"id": 2, "name": "anthropic-first", "mode": "anthropic"},
            {"id": 3, "name": "anthropic-second", "mode": "anthropic"},
            {"id": 4, "name": "responses", "mode": "responses"},
        ]

    def test_anthropic_request_orders_native_before_translated(self):
        selected = F._dialect_eligible_targets(self.targets, "anthropic", True)
        self.assertEqual([t["id"] for t in selected], [2, 3, 1, 4])

    def test_openai_request_native_first(self):
        selected = F._dialect_eligible_targets(self.targets, "openai", True)
        self.assertEqual([t["id"] for t in selected], [1, 2, 3, 4])

    def test_responses_request_prefers_responses_native(self):
        selected = F._dialect_eligible_targets(self.targets, "responses", True)
        self.assertEqual([t["id"] for t in selected], [4, 1, 2, 3])

    def test_format_off_keeps_only_native_dialect(self):
        selected = F._dialect_eligible_targets(self.targets, "anthropic", False)
        self.assertEqual([t["id"] for t in selected], [2, 3])

    def test_no_native_endpoint_uses_translated_targets_when_enabled(self):
        targets = [self.targets[0], self.targets[3]]
        selected = F._dialect_eligible_targets(targets, "anthropic", True)
        self.assertEqual([t["id"] for t in selected], [1, 4])


if __name__ == "__main__":
    unittest.main(verbosity=1)
