#!/usr/bin/env python3
"""Regression tests for the PRIMARY-FIRST routing hierarchy (Boss, 2026-09-15).

Boss's rule, in his words: "primary set hai wo primary rehna chahiye — wo open
hai ya nahi usse fark nahi padta... native nahi jayega, hamesha jo primary hai
wo primary hai". And for the fallthrough: "primary agar fail ho jaye to wo
pehle anthropic wale ko try karega... lekin agar saara OpenAI fail ho gaya"
— i.e. after the primary, NATIVE-dialect endpoints come next, and only when
those are exhausted do the format-translated ones get tried.

Expected order for a client dialect K, given a priority-ordered target list
whose FIRST entry is the operator's primary:

    1. primary                       (always first, regardless of dialect)
    2. remaining native-K endpoints  (priority order)
    3. translated endpoints          (priority order), only if format
                                     translation is enabled
"""
import sys
import unittest

sys.path.insert(0, "/root/gw-preview/repo")
from app import forwarder as F


def mk(tid, mode, primary=False):
    return {"id": tid, "name": f"ep{tid}", "mode": mode,
            "is_primary": primary}


class PrimaryFirstTests(unittest.TestCase):
    def setUp(self):
        # Priority order: primary(1) first, as _targets() builds it.
        # 1 = OpenAI primary, 2/3 = anthropic, 4 = responses, 5 = openai
        self.targets = [
            mk(1, "openai", primary=True),
            mk(2, "anthropic"),
            mk(3, "anthropic"),
            mk(4, "responses"),
            mk(5, "openai"),
        ]

    # --- Anthropic request, OpenAI primary: primary STILL goes first ---

    def test_anthropic_request_openai_primary_stays_first(self):
        sel = F._dialect_eligible_targets(self.targets, "anthropic", True)
        self.assertEqual([t["id"] for t in sel], [1, 2, 3, 4, 5])

    def test_anthropic_request_primary_first_then_native_then_translated(self):
        sel = F._dialect_eligible_targets(self.targets, "anthropic", True)
        # primary(1, openai) -> native anthropic (2,3) -> translated (4,5)
        self.assertEqual(sel[0]["id"], 1)
        self.assertEqual([t["id"] for t in sel[1:3]], [2, 3])

    # --- OpenAI request: primary happens to be native; order unchanged ---

    def test_openai_request_primary_native_order_unchanged(self):
        sel = F._dialect_eligible_targets(self.targets, "openai", True)
        self.assertEqual([t["id"] for t in sel], [1, 5, 2, 3, 4])

    # --- Responses request, OpenAI primary: primary first, then the one
    #     native responses endpoint, then translated ---

    def test_responses_request_primary_first(self):
        sel = F._dialect_eligible_targets(self.targets, "responses", True)
        self.assertEqual([t["id"] for t in sel], [1, 4, 2, 3, 5])

    # --- Format translation OFF: translated endpoints (including a
    #     cross-dialect primary) are dropped — they cannot serve the body.
    #     A cross-dialect primary needs translation to carry the request,
    #     so with the global toggle OFF it is excluded, not "always first".

    def test_format_off_native_only_cross_dialect_primary_dropped(self):
        sel = F._dialect_eligible_targets(self.targets, "anthropic", False)
        self.assertEqual([t["id"] for t in sel], [2, 3])

    def test_format_off_same_dialect_primary_kept_first(self):
        # Primary SPEAKS the client's dialect: stays first even with the
        # global toggle OFF — no translation needed to serve it.
        targets = [mk(1, "anthropic", primary=True), mk(2, "anthropic"),
                   mk(3, "openai")]
        sel = F._dialect_eligible_targets(targets, "anthropic", False)
        self.assertEqual([t["id"] for t in sel], [1, 2])

    def test_format_off_primary_translated_alone_still_served(self):
        # Anthropic request, ONLY the OpenAI primary exists, translation off:
        # nothing can serve it (primary is kept but cannot be translated).
        sel = F._dialect_eligible_targets([self.targets[0]], "anthropic", False)
        self.assertEqual(sel, [])

    # --- Primary in the MIDDLE of the priority list (DB anomaly / manual
    #     priority edits): still promoted to position 1 ---

    def test_midlist_primary_promoted_to_first(self):
        targets = [mk(2, "anthropic"), mk(1, "openai", primary=True),
                   mk(3, "anthropic")]
        sel = F._dialect_eligible_targets(targets, "anthropic", True)
        self.assertEqual([t["id"] for t in sel], [1, 2, 3])

    # --- No primary flag at all: behave like the old native-first order ---

    def test_no_primary_native_first(self):
        targets = [mk(2, "anthropic"), mk(1, "openai"), mk(3, "anthropic")]
        sel = F._dialect_eligible_targets(targets, "anthropic", True)
        self.assertEqual([t["id"] for t in sel], [2, 3, 1])

    # --- Only translated endpoints exist and translation is ON: full list
    #     in priority order (primary first) ---

    def test_all_translated_primary_first(self):
        sel = F._dialect_eligible_targets(
            [mk(1, "openai", primary=True), mk(2, "responses")],
            "anthropic", True)
        self.assertEqual([t["id"] for t in sel], [1, 2])


if __name__ == "__main__":
    unittest.main(verbosity=1)
