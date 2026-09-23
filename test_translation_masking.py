#!/usr/bin/env python3
"""T4: whole-text placeholder masking for LLM translation backends (2026-09-23).

Root cause fixed here: the per-fragment path translated each prose span BETWEEN
protected code/URL/JSON spans independently, which shatters sentence grammar for
an LLM backend (measured: "Boss, I `auth.py` I need to fix a bug ..."). LLM
backends now mask every protected span as a nonce placeholder and translate the
WHOLE message in one call, then restore the spans byte-for-byte.

These tests use a deterministic fake translator (no network) so they assert the
CONTRACT, not a live model:
- protected spans (code/URL/JSON/ids) come back byte-for-byte
- the LLM sees ONE whole masked message, not N fragments
- fail-closed: a backend that drops/duplicates a placeholder raises
- the built-in Google collector path is unchanged (fragment path)
"""
import asyncio, json, sys, unittest

REPO = "/root/gw-preview/repo"
sys.path.insert(0, REPO)

from app import language_translate as LT
from app import llm_translate as LLM


class _FakeLLM:
    """Deterministic stand-in for an LLMTranslator callable.

    Records every call. ``transform`` maps the masked prose to a 'translated'
    string while keeping any ``ZZ<nonce>Q<i>ZZ`` placeholder tokens intact,
    emulating a well-behaved model.
    """
    whole_text_masking = True
    chunk_chars = 8000

    def __init__(self, transform=None):
        self.calls = []
        self._transform = transform or (lambda s: s.replace(
            "मुझे", "I need to").replace("ठीक करना है", "must fix")
            .replace("नमस्ते", "hello"))

    async def __call__(self, text):
        self.calls.append(text)
        return self._transform(text)


class MaskingUnitTests(unittest.TestCase):
    def test_mask_restore_roundtrip_byte_for_byte(self):
        text = ("Boss, mujhe `auth.py` ka bug fix karna hai, doc "
                "https://api.example.com/v1/docs par hai, args "
                '{"id":"toolu_bdrk_01ABC","path":"/root/x"} the.')
        masked, spans, nonce = LT._mask_protected(text)
        # every protected span pulled out; none of them remain in the masked prose
        for span in spans:
            self.assertNotIn(span, masked)
        self.assertGreaterEqual(len(spans), 3)
        restored = LT._restore_protected(masked, spans, nonce)
        self.assertEqual(restored, text)

    def test_whole_message_single_call_not_fragmented(self):
        text = ("mujhe `auth.py` ka bug ठीक करना है aur uska doc "
                "https://x.io/y par hai")
        fake = _FakeLLM()
        out, changed = asyncio.run(LT.translate_text(text, fake))
        # ONE call carrying the whole masked message — not one call per fragment
        self.assertEqual(len(fake.calls), 1)
        self.assertTrue(changed)
        # protected spans survive byte-for-byte
        self.assertIn("`auth.py`", out)
        self.assertIn("https://x.io/y", out)
        # no placeholder leaked into the final text
        self.assertNotIn("ZZ", out)

    def test_code_json_url_untouched_through_request(self):
        body = json.dumps({
            "model": "m",
            "messages": [{"role": "user", "content":
                "Boss, mujhe `make_token()` ka bug fix karna hai. Model "
                "claude-opus-4-8 use karo, doc https://api.example.com/v1/docs "
                'par hai aur args {"tool":"x","id":"toolu_bdrk_9"} the.'}],
        }).encode()
        # identity-ish transform: keeps placeholders and Latin, only touches Hindi
        fake = _FakeLLM(transform=lambda s: s.replace(
            "mujhe", "I need to").replace("ka bug fix karna hai", "must fix the bug of")
            .replace("use karo", "use").replace("par hai aur args", "and args")
            .replace("the.", "were."))
        rep = asyncio.run(LT.translate_request_report(body, "openai", backends=[fake]))
        self.assertTrue(rep.changed)
        self.assertEqual(rep.error, "")
        msgs = json.loads(rep.body)["messages"]
        out = next(m["content"] for m in msgs if m.get("role") == "user")
        for needle in ("`make_token()`", "claude-opus-4-8",
                       "https://api.example.com/v1/docs",
                       '{"tool":"x","id":"toolu_bdrk_9"}'):
            self.assertIn(needle, out, needle)

    def test_fail_closed_when_backend_drops_placeholder(self):
        text = "mujhe `auth.py` ka bug fix karna hai"
        # a bad model that eats every placeholder token
        bad = _FakeLLM(transform=lambda s: "I must fix the bug")
        with self.assertRaises(ValueError):
            asyncio.run(LT._translate_text_masked(text, bad))

    def test_fail_closed_when_backend_duplicates_placeholder(self):
        text = "mujhe `auth.py` ka bug fix karna hai"

        def dup(s):
            # echo the whole thing twice -> placeholder count becomes 2
            return s + " " + s
        bad = _FakeLLM(transform=dup)
        with self.assertRaises(ValueError):
            asyncio.run(LT._translate_text_masked(text, bad))

    def test_no_protected_spans_still_translates(self):
        text = "mujhe madad chahiye jaldi"
        fake = _FakeLLM(transform=lambda s: "I need help quickly")
        out, changed = asyncio.run(LT.translate_text(text, fake))
        self.assertTrue(changed)
        self.assertEqual(out, "I need help quickly")

    def test_google_collector_unchanged_uses_fragment_path(self):
        # The built-in collector has collect_all=True and NO whole_text_masking:
        # translate_text must still take the fragment (_protected_ranges) path.
        collector = LT._BatchCollector(collect_all=True)
        self.assertFalse(getattr(collector, "whole_text_masking", False))
        text = "mujhe `auth.py` fix karna hai"
        out, changed = asyncio.run(LT.translate_text(text, collector))
        # collector records the prose fragments as markers; code span is verbatim
        self.assertIn("`auth.py`", out)
        self.assertTrue(changed)


if __name__ == "__main__":
    unittest.main(verbosity=1)
