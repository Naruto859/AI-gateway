#!/usr/bin/env python3
"""Behavior tests for per-endpoint any-language translation."""
import asyncio
import json
import re
import sys
import unicodedata
import unittest
from unittest.mock import patch

sys.path.insert(0, "/root/gw-preview/repo")
from app import language_translate as L


class FakeTranslator:
    def __init__(self):
        self.seen = []

    async def __call__(self, text):
        self.seen.append(text)
        return "EN<" + text + ">"


class DetectionTests(unittest.TestCase):
    def test_detects_devanagari(self):
        self.assertTrue(L.needs_translation("AgentRouter सिर्फ Hindi रोकता है"))

    def test_detects_roman_hinglish(self):
        self.assertTrue(L.needs_translation("kal mujhe result bata dena"))

    def test_detects_multiple_non_english_scripts(self):
        samples = [
            "请检查这个路由",       # Chinese
            "このルートを確認して",  # Japanese
            "تحقق من هذا المسار",    # Arabic
            "এই রুটটি পরীক্ষা করুন", # Bengali
            "Проверь этот маршрут",  # Russian
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(L.needs_translation(text))

    def test_detects_latin_script_non_english(self):
        samples = [
            "Por favor revisa esta ruta",       # Spanish
            "Veuillez vérifier cette route",    # French
            "Bitte überprüfe diese Route",      # German
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(L.needs_translation(text))

    def test_preserves_detected_source_language_instruction(self):
        body = {"messages": [{"role": "user", "content": "请检查这个路由"}]}
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            return [("Check this route", "zh") for _ in parts]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertIn("Respond in Chinese", json.loads(raw)["system"])
        self.assertNotIn("Hindi/Hinglish", json.loads(raw)["system"])

    def test_detects_obscure_latin_script_language_without_dictionary_words(self):
        # Swahili: dynamic translation cannot depend on a small hard-coded vocabulary.
        self.assertTrue(L.needs_translation("Tafadhali angalia njia hii"))

    def test_response_instruction_names_latest_user_language(self):
        body = {"messages": [
            {"role": "user", "content": "पहले वाला संदेश"},
            {"role": "assistant", "content": "ठीक है"},
            {"role": "user", "content": "请检查这个路由"},
        ]}
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            languages = ["hi", "hi", "zh"]
            return [("English", language) for language in languages]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertIn("Chinese", json.loads(raw)["system"])

    def test_latest_english_user_gets_english_response_instruction(self):
        body = {"messages": [
            {"role": "user", "content": "पहले वाला संदेश"},
            {"role": "assistant", "content": "ठीक है"},
            {"role": "user", "content": "Now explain the result clearly in English."},
        ]}
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            self.assertEqual(parts, ["पहले वाला संदेश", "ठीक है",
                                     "Now explain the result clearly in English."])
            return [("English", "hi"), ("English", "hi"),
                    (parts[2], "en")]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertIn("Respond in English", json.loads(raw)["system"])
        self.assertNotIn("Respond in Hindi", json.loads(raw)["system"])

    def test_latest_user_language_is_not_overridden_by_later_tool_schema(self):
        body = {
            "messages": [{"role": "user", "content": "Gracias"}],
            "tools": [{"name": "check", "description": "मार्ग जांचें"}],
        }

        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            self.assertEqual(parts, ["Gracias", "मार्ग जांचें"])
            return [("Thank you", "es"), ("Check route", "hi")]

        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertIn("Respond in Spanish", json.loads(raw)["system"])

    def test_openai_response_language_update_preserves_system_prompt(self):
        body = {"messages": [
            {"role": "system", "content": "आप सहायक हैं"},
            {"role": "user", "content": "Gracias"},
        ]}

        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            self.assertEqual(parts, ["आप सहायक हैं", "Gracias"])
            return [("You are an assistant", "hi"), ("Thank you", "es")]

        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "openai"))
        self.assertTrue(changed)
        system = json.loads(raw)["messages"][0]["content"]
        self.assertIn("You are an assistant", system)
        self.assertIn("Respond in Spanish", system)

    def test_does_not_mistake_technical_english_for_hinglish(self):
        for text in (
            "Fix auth middleware",
            "Use the main API route and keep this key unchanged.",
            "Use GitHub Docker Redis GraphQL Linux Python API ID-123.",
        ):
            with self.subTest(text=text):
                self.assertFalse(L.needs_translation(text))

    def test_long_input_has_no_detection_sampling_blind_spot(self):
        prefix = "This is ordinary English context. " * 250
        suffix = "More ordinary English context. " * 550
        text = prefix + " danke " + suffix
        self.assertGreater(text.find("danke"), L._DETECTION_SAMPLE_SIZE)
        self.assertTrue(L.needs_translation(text))

    def test_foreign_word_straddling_detection_boundaries_is_seen(self):
        filler = ("ordinary English context " * 1200)
        for offset in (5997, 5998, 5999, 11997, 11998, 11999,
                       17997, 17998, 17999):
            with self.subTest(offset=offset):
                text = filler[:offset] + "danke " + filler[offset:23467]
                self.assertEqual(text.find("danke"), offset)
                self.assertTrue(L.needs_translation(text))

    def test_default_path_auto_detects_short_foreign_text_diluted_by_english(self):
        prefix = "ordinary technical English context " * 250
        suffix = " more ordinary technical English context" * 450
        for foreign, language in (
            ("oui", "fr"), ("nein", "de"), ("tak", "pl"),
            ("tafadhali", "sw"), ("tafadhali angalia njia hii", "sw"),
        ):
            with self.subTest(foreign=foreign):
                source = prefix + foreign + suffix
                raw = json.dumps({"messages": [
                    {"role": "user", "content": source},
                ]}).encode()
                calls = []

                async def details(parts, proxy_urls=None, source_languages=None):
                    calls.extend(parts)
                    if source_languages:
                        return [(part, item) for part, item in
                                zip(parts, source_languages)]
                    return [("Fully translated English text", language)
                            for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertTrue(calls)
                self.assertTrue(report.changed)
                self.assertEqual(report.error, "")

    def test_regional_google_source_codes_are_normalized(self):
        class FakeResponse:
            def raise_for_status(self):
                return None
            def json(self):
                return [[['Hello', '你好', None, None]], None, 'zh-CN']

        class FakeClient:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *_args):
                return None
            async def post(self, *_args, **_kwargs):
                return FakeResponse()

        with patch.object(L.httpx, 'AsyncClient', return_value=FakeClient()):
            details = asyncio.run(L._google_translate_details_batch(['你好']))
        self.assertEqual(details, [('Hello', 'zh')])

    def test_responses_string_input_instructions_and_tool_output_translate(self):
        raw = json.dumps({
            "model": "x",
            "instructions": "कठोर नियम मानो",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "नमस्ते"}]},
                {"type": "function_call", "call_id": "call_exact", "name": "terminal",
                 "arguments": '{"command":"echo नमस्ते"}'},
                {"type": "function_call_output", "call_id": "call_exact",
                 "output": "काम पूरा हुआ"},
            ],
        }, ensure_ascii=False).encode()
        seen = []

        async def translator(text):
            seen.append(text)
            return "EN<" + text + ">"

        report = asyncio.run(L.translate_request_report(
            raw, "responses", translator=translator))
        self.assertTrue(report.changed)
        self.assertEqual(report.error, "")
        out = json.loads(report.body)
        self.assertTrue(out["instructions"].startswith("EN<कठोर नियम मानो>"))
        self.assertEqual(out["input"][0]["content"][0]["text"], "EN<नमस्ते>")
        self.assertEqual(out["input"][1]["call_id"], "call_exact")
        self.assertEqual(out["input"][1]["arguments"], '{"command":"echo नमस्ते"}')
        self.assertEqual(out["input"][2]["call_id"], "call_exact")
        self.assertEqual(out["input"][2]["output"], "EN<काम पूरा हुआ>")
        self.assertIn("कठोर नियम मानो", seen)
        self.assertIn("काम पूरा हुआ", seen)

    def test_responses_top_level_string_input_translates(self):
        raw = json.dumps({"model": "x", "input": "नमस्ते"},
                         ensure_ascii=False).encode()

        async def translator(text):
            return "Hello"

        report = asyncio.run(L.translate_request_report(
            raw, "responses", translator=translator))
        self.assertTrue(report.changed)
        self.assertEqual(report.error, "")
        self.assertEqual(json.loads(report.body)["input"], "Hello")

    def test_invalid_source_metadata_type_or_code_fails_atomically(self):
        source = "Bitte prüfe diese Route"
        raw = json.dumps({"messages": [
            {"role": "user", "content": source},
        ]}, ensure_ascii=False).encode()
        class HostileLanguage(str):
            def __hash__(self): return hash("en")
            def __eq__(self, other): return other in ("en", "zz")

        for invalid in (None, "", 7, True, {}, [], "zz", HostileLanguage("zz")):
            with self.subTest(metadata=repr(invalid)):
                async def details(parts, proxy_urls=None, source_languages=None):
                    return [("Check this route", invalid) for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertTrue(report.error)
                self.assertTrue(
                    "source-language metadata" in report.error
                    or "invalid detail" in report.error)

    def test_adversarial_container_and_string_subclasses_fail_closed(self):
        class ListSub(list): pass
        class StringSub(str): pass
        class EmptyHostile(str):
            def strip(self, *args, **kwargs): return "deceptively-nonempty"

        with self.assertRaises(ValueError):
            asyncio.run(L._validate_translations(
                ["source"], ListSub(["English"]), ["de"]))
        with self.assertRaises(ValueError):
            asyncio.run(L._validate_translations(
                ["source"], [EmptyHostile("")], ["de"]))
        with self.assertRaises(ValueError):
            asyncio.run(L._validate_translations(
                ["source"], ["English"], ListSub(["de"])))

    def test_translate_collected_rejects_subclass_and_malformed_details(self):
        class ListSub(list): pass
        class TupleSub(tuple): pass
        class StringSub(str): pass
        invalid_parts = (
            ("source",), ListSub(["source"]), {"source": 1},
            [StringSub("source")], [b"source"], [["source"]],
        )
        for parts in invalid_parts:
            with self.subTest(parts=repr(parts)):
                with self.assertRaises(ValueError):
                    asyncio.run(L._translate_collected(parts))

        invalid_details = (
            (("English", "de"),),
            ListSub([("English", "de")]),
            [["English", "de"]],
            [TupleSub(("English", "de"))],
            [{"English": "de"}],
            [(StringSub("English"), "de")],
        )
        for details in invalid_details:
            with self.subTest(details=repr(details)):
                async def fake(_parts, proxy_urls=None, source_languages=None):
                    return details
                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=fake):
                    with self.assertRaises(ValueError):
                        asyncio.run(L._translate_collected(["source"]))

    def test_google_payload_segments_are_strictly_shaped(self):
        class ListSub(list): pass
        class StringSub(str): pass
        malformed = (
            None, {}, [], [None, None, "de"],
            [["Hello"], None, "de"],
            [[None], None, "de"],
            [[[7]], None, "de"],
            [[["Hello"]], None, 7],
            ListSub([[['Hello']], None, "de"]),
            [ListSub([["Hello"]]), None, "de"],
            [[ListSub(["Hello"])], None, "de"],
            [[[StringSub("Hello")]], None, "de"],
            [[['Hello']], None, "DE"],
            [[['Hello']], None, " de "],
        )

        class FakeResponse:
            def __init__(self, payload): self.payload = payload
            def raise_for_status(self): return None
            def json(self): return self.payload

        for payload in malformed:
            with self.subTest(payload=repr(payload)):
                class FakeClient:
                    def __init__(self, *args, **kwargs): pass
                    async def __aenter__(self): return self
                    async def __aexit__(self, *args): return False
                    async def post(self, *_args, **_kwargs):
                        return FakeResponse(payload)

                with patch.object(L.httpx, "AsyncClient", side_effect=FakeClient):
                    with self.assertRaises(RuntimeError):
                        asyncio.run(L._google_translate_details_batch(["Hallo"]))

    def test_default_path_all_english_is_byte_exact_noop(self):
        raw = b'{"messages":[{"role":"user","content":"Use GitHub and Docker."}]}'

        async def details(parts, proxy_urls=None, source_languages=None):
            return [(part, language) for part, language in
                    zip(parts, source_languages or ["en"] * len(parts))]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=details):
            report = asyncio.run(
                L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertEqual(report.error, "")

    def test_unchanged_foreign_text_mislabeled_en_fails_atomically(self):
        cases = (
            ("Bitte prüfe diese Route", "de"),
            ("Veuillez vérifier cette route", "fr"),
            ("Tafadhali angalia njia hii", "sw"),
            ("kal mujhe result bata dena", "hi"),
            ("Por favor revisa esta ruta", "es"),
        )
        for source, actual_language in cases:
            with self.subTest(language=actual_language):
                raw = json.dumps({"messages": [
                    {"role": "user", "content": source},
                ]}, ensure_ascii=False).encode()

                async def details(parts, proxy_urls=None, source_languages=None):
                    if source_languages:
                        return [(("English translation"
                                  if language == actual_language else part), language)
                                for part, language in zip(parts, source_languages)]
                    return [(part, "en") for part in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertIn("invalid English metadata", report.error)

    def test_long_english_chunk_whitespace_is_preserved(self):
        source = ("Use GitHub Docker Redis GraphQL Linux Python API ID-123. " * 100)
        raw = json.dumps({"messages": [
            {"role": "user", "content": source},
        ]}).encode()

        async def details(parts, proxy_urls=None, source_languages=None):
            # Forced-source translation may alter individual cognates/names;
            # full English context must remain unchanged across the field.
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            return [(part, "en") for part in parts]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=details):
            report = asyncio.run(
                L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertEqual(report.error, "")

    def test_english_validation_call_budget_is_bounded_per_field(self):
        raw = json.dumps({"messages": [
            {"role": "user", "content": "Use GitHub and Docker."},
        ]}).encode()
        calls = []

        async def details(parts, proxy_urls=None, source_languages=None):
            calls.append((list(parts), list(source_languages or [])))
            return [(part, language) for part, language in
                    zip(parts, source_languages or ["en"] * len(parts))]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=details):
            report = asyncio.run(
                L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.error, "")
        self.assertLessEqual(len(calls), 1 + len(L._VALIDATION_LANGUAGES))

    def test_many_english_fields_validation_is_batched(self):
        raw = json.dumps({"messages": [
            {"role": "user", "content": f"English technical field {index}."}
            for index in range(100)
        ]}).encode()
        calls = []

        async def details(parts, proxy_urls=None, source_languages=None):
            calls.append((list(parts), list(source_languages or [])))
            return [(part, language) for part, language in
                    zip(parts, source_languages or ["en"] * len(parts))]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=details):
            report = asyncio.run(
                L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.error, "")
        # One 24-item wave per auto/forced-source pass, not fields × languages.
        waves = (100 + L._WAVE_CHUNK_LIMIT - 1) // L._WAVE_CHUNK_LIMIT
        self.assertLessEqual(len(calls), waves * (1 + len(L._VALIDATION_LANGUAGES)))

    def test_realistic_english_cognates_do_not_false_block(self):
        cases = (
            "This Gift is ready.",
            "Be kind to users.",
            "The chef prepared dinner.",
            "Watch Die Hard tonight.",
            "Deploy Spring Boot now.",
            "The gifted engineer kindly reviewed the Docker API migration in Berlin.",
            "Paris-based developer François checked the Linux routing configuration.",
            "Roman Polanski and Heidi Klum discussed the Gift protocol in Berlin.",
            "Gabriel García Márquez inspired Isabel Allende during the Bogotá conference.",
        )
        for source in cases:
            with self.subTest(source=source):
                raw = json.dumps({"messages": [
                    {"role": "user", "content": source},
                ]}).encode()

                async def details(parts, proxy_urls=None, source_languages=None):
                    # A real backend translates whole prose. For valid English,
                    # forced-source validation must preserve the full sentence;
                    # isolated cognate/name changes are not valid field output.
                    return [(part, language) for part, language in
                            zip(parts, source_languages or ["en"] * len(parts))]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertEqual(report.error, "")

    def test_foreign_text_cannot_bypass_with_english_anchors_or_api_terms(self):
        cases = (
            ("Bitte prüfe the Route", "de"),
            ("Veuillez vérifier the route", "fr"),
            ("Tafadhali angalia the route", "sw"),
            ("kal the result bata dena", "hi"),
            ("Por favor revisa the ruta", "es"),
            ("Bitte prüfe API Docker jetzt", "de"),
            ("Veuillez vérifier API Docker maintenant", "fr"),
            ("Tafadhali angalia API Docker sasa", "sw"),
            ("kal API Docker check karo", "hi"),
            ("Revisa API Docker ahora", "es"),
            ("GitHub Redis server abhi kaam nahi kar raha and the endpoint band hai", "hi"),
            ("Kubernetes Linux deployment aaj test karo and fix the bug jaldi", "hi"),
            ("The engineer reviewed Docker; tafadhali angalia Redis na urekebishe route sasa", "sw"),
            ("Please deploy Kubernetes, kisha hakikisha huduma inafanya kazi vizuri", "sw"),
            ("The backend engineer reviewed Docker, but bittte prüfend Redis jetztt", "de"),
            ("The developer reviewed the API avecc mercis; verifiez Redis maintenant", "fr"),
            ("Please inspect Docker pourr cettes route and bonjours from Paris", "fr"),
            ("The engineer reviewed Docker; tafadhalis angalie Redis na urekebishwe sasa", "sw"),
            ("Please deploy Kubernetes, kishaa hakikishaa huduma inafanyaa kazi vizuri", "sw"),
            ("The engineer reviewed Docker but abhii result bataanaa aur jaldii karoo", "hi"),
            ("Please deploy Redis; mujhee nayaa config chahiyes and verify it now", "hi"),
        )
        for source, actual_language in cases:
            with self.subTest(source=source):
                raw = json.dumps({"messages": [
                    {"role": "user", "content": source},
                ]}, ensure_ascii=False).encode()

                async def details(parts, proxy_urls=None, source_languages=None):
                    if source_languages:
                        return [(("English translation"
                                  if language == actual_language else part), language)
                                for part, language in zip(parts, source_languages)]
                    return [(part, "en") for part in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertIn("invalid English metadata", report.error)

    def test_detects_required_short_latin_foreign_inputs(self):
        for text in (
            "Oui", "Nein", "Tak", "Prego", "Salut", "Ahoj", "Hej",
            "Shalom", "Szia", "Privet", "Namaste", "Tafadhali",
            "Hello, amigo", "Please check, gracias",
        ):
            with self.subTest(text=text):
                self.assertTrue(L.needs_translation(text))

    def test_default_detection_translates_short_foreign_and_preserves_technical_english(self):
        body = {"messages": [
            {"role": "user", "content": "Gracias"},
            {"role": "assistant", "content": "Fix auth middleware"},
        ]}

        async def fake_details(texts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(text, language) for text, language in
                        zip(texts, source_languages)]
            self.assertEqual(texts, ["Gracias", "Fix auth middleware"])
            return [("Thank you", "es"), (texts[1], "en")]

        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        out = json.loads(raw)
        self.assertEqual(out["messages"][0]["content"], "Thank you")
        self.assertEqual(out["messages"][1]["content"], "Fix auth middleware")
        self.assertIn("Spanish", out["system"])

    def test_real_google_batch_handles_mixed_source_languages_independently(self):
        samples = ["hello world", "Por favor revisa esta ruta", "请检查这个路由"]
        output = asyncio.run(L._google_translate_batch(samples))
        self.assertEqual(output, ["hello world", "Please check this route", "Please check this route"])

    def test_real_google_translator_handles_hindi_and_hinglish(self):
        samples = [
            "आप Ciel हैं और context सुरक्षित रखो।",
            "AgentRouter सिर्फ Hindi block karta hai.",
            "कल मुझे result बताना।",
        ]
        output = asyncio.run(L._google_translate_batch(samples))
        self.assertEqual(len(output), len(samples))
        self.assertTrue(all(L.needs_translation(x) is False for x in output))


class RequestTranslationTests(unittest.TestCase):
    def run_translate(self, body, kind="anthropic"):
        fake = FakeTranslator()
        raw, changed = asyncio.run(L.translate_request(
            json.dumps(body, ensure_ascii=False).encode(), kind, translator=fake))
        return json.loads(raw), changed, fake.seen

    def test_responses_provider_executed_tools_are_dropped_not_fatal(self):
        # Codex always sends built-in `web_search` (provider-executed) plus a
        # `namespace` tool; these cannot be executed by an Anthropic-dialect
        # upstream, so format translation must DROP them and keep the function
        # tools instead of failing closed (which killed every Codex request).
        import importlib
        fmt = importlib.import_module("app.translate")
        body = {
            "model": "x",
            "input": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "function", "name": "exec_command",
                 "description": "Run a command",
                 "parameters": {"type": "object", "properties": {}}},
                {"type": "web_search"},
                {"type": "namespace", "name": "multi_agent_v1"},
            ],
        }
        out = fmt.request_to("responses", "anthropic", body)
        names = [t.get("name") for t in out["tools"]]
        self.assertEqual(names, ["exec_command"])
        types = [t.get("type") for t in out["tools"]]
        self.assertNotIn("web_search", types)
        self.assertNotIn("namespace", types)

    def test_anthropic_translates_natural_language_but_preserves_protocol(self):
        body = {
            "model": "claude-opus-4-8",
            "system": "आप Ciel हैं. Keep API names exact.",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "ये URL https://example.com और `max_tokens` मत बदलना"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}},
                ]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "ठीक है"},
                    {"type": "tool_use", "id": "toolu_exact", "name": "read_file", "input": {"path": "/tmp/हिंदी.txt"}},
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_exact", "content": "फाइल नहीं मिली"}
                ]},
            ],
        }
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        self.assertTrue(out["system"].startswith("EN<"))
        self.assertIn("https://example.com", out["messages"][0]["content"][0]["text"])
        self.assertIn("`max_tokens`", out["messages"][0]["content"][0]["text"])
        tool = out["messages"][1]["content"][1]
        self.assertEqual(tool["id"], "toolu_exact")
        self.assertEqual(tool["input"], {"path": "/tmp/हिंदी.txt"})
        result = out["messages"][2]["content"][0]
        self.assertEqual(result["tool_use_id"], "toolu_exact")
        self.assertTrue(result["content"].startswith("EN<"))
        instructions = out["system"] if isinstance(out["system"], list) else [out["system"]]
        self.assertTrue(any("Respond in Hindi" in str(s) for s in instructions))

    def test_openai_preserves_tool_call_arguments_and_translates_tool_output(self):
        body = {"messages": [
            {"role": "system", "content": "आप Ciel हैं"},
            {"role": "user", "content": "kal mujhe bata dena"},
            {"role": "assistant", "content": "कर रही हूँ", "tool_calls": [{
                "id": "call_exact", "type": "function",
                "function": {"name": "search", "arguments": '{"q":"हिंदी"}'},
            }]},
            {"role": "tool", "tool_call_id": "call_exact", "content": "नतीजा मिला"},
        ]}
        out, changed, _ = self.run_translate(body, "openai")
        self.assertTrue(changed)
        self.assertEqual(out["messages"][2]["tool_calls"][0]["id"], "call_exact")
        self.assertEqual(out["messages"][2]["tool_calls"][0]["function"]["arguments"], '{"q":"हिंदी"}')
        self.assertTrue(out["messages"][3]["content"].startswith("EN<"))

    def test_english_only_body_is_byte_exact(self):
        raw = b'{"model":"x","messages":[{"role":"user","content":"hello world"}]}'
        fake = FakeTranslator()
        out, changed = asyncio.run(L.translate_request(raw, "anthropic", translator=fake))
        self.assertFalse(changed)
        self.assertEqual(out, raw)
        self.assertEqual(fake.seen, [])

    def test_default_path_batches_many_context_fields_into_one_call(self):
        body = {"system": "आप Ciel हैं", "messages": [
            {"role": "user", "content": f"मुझे result बताना {i}"} for i in range(20)
        ]}
        calls = []
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            calls.append(list(parts))
            return [("English " + str(i), "hi") for i, _ in enumerate(parts)]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(json.loads(raw)["messages"]), 20)

    def test_english_context_does_not_exhaust_translation_fragment_limit(self):
        # Real Hermes requests can contain more than 64 natural-language fields.
        # The collector must translate the complete bounded payload instead of
        # silently returning the original Hindi request unchanged.
        body = {"system": "English system context", "messages": [
            {"role": "user" if i % 2 == 0 else "assistant",
             "content": f"English context fragment {i}"}
            for i in range(70)
        ]}
        body["messages"].append({"role": "user", "content": "नमस्ते Boss, result बताओ"})
        calls = []

        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            calls.append(list(parts))
            return [("Hello Boss, tell me the result", "hi")
                    if "नमस्ते" in part else
                    (part, "en")
                    for part in parts]

        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        self.assertGreater(len(calls), 1)
        flattened = [part for wave in calls for part in wave]
        self.assertGreater(len(flattened), 64)
        self.assertIn("नमस्ते Boss, result बताओ", flattened)
        self.assertEqual(json.loads(raw)["messages"][-1]["content"],
                         "Hello Boss, tell me the result")

    def test_payload_over_64k_is_chunked_without_hard_failure(self):
        hindi = ("यह एक लंबा अनुवाद परीक्षण वाक्य है। " * 2200).strip()
        self.assertGreater(len(hindi), 64_000)
        body = {"messages": [{"role": "user", "content": hindi}]}
        seen = []

        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            seen.extend(parts)
            return [("translated:" + str(i), "hi") for i, _ in enumerate(parts)]

        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            report = asyncio.run(L.translate_request_report(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertEqual(report.error, "")
        self.assertTrue(report.changed)
        self.assertGreater(len(seen), 16)
        self.assertNotIn("यह एक लंबा", report.body.decode())

    def test_default_batch_path_restores_protected_code_and_url(self):
        body = {"messages": [{"role": "user", "content":
            "यह `API_KEY` और https://x.test/हिंदी बिल्कुल मत बदलना"}]}
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            return [("Keep this exactly unchanged", "hi") for _ in parts]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertTrue(changed)
        text = json.loads(raw)["messages"][0]["content"]
        self.assertIn("`API_KEY`", text)
        self.assertIn("https://x.test/हिंदी", text)

    def test_translation_failure_returns_original_bytes(self):
        body = {"messages": [{"role": "user", "content": "मुझे result बताना"}]}
        raw = json.dumps(body, ensure_ascii=False).encode()
        async def failed_details(parts, proxy_urls=None, source_languages=None):
            raise RuntimeError("translator unavailable")
        with patch.object(L, "_google_translate_details_batch", side_effect=failed_details):
            out, changed = asyncio.run(L.translate_request(raw, "anthropic"))
        self.assertFalse(changed)
        self.assertEqual(out, raw)

    def test_incomplete_translation_batch_returns_original_bytes(self):
        body = {"messages": [
            {"role": "user", "content": "मुझे result बताना"},
            {"role": "assistant", "content": "ठीक है"},
        ]}
        raw = json.dumps(body, ensure_ascii=False).encode()

        async def incomplete_details(parts, proxy_urls=None, source_languages=None):
            self.assertEqual(len(parts), 2)
            return [("Tell me the result", "hi")]

        with patch.object(L, "_google_translate_details_batch", side_effect=incomplete_details):
            out, changed = asyncio.run(L.translate_request(raw, "anthropic"))
        self.assertFalse(changed)
        self.assertEqual(out, raw)

    def test_cross_wave_result_count_mismatch_fails_atomically(self):
        body = {"messages": [{
            "role": "user", "content": f"विस्तृत अनुरोध {index}"
        } for index in range(49)]}
        raw = json.dumps(body, ensure_ascii=False).encode()
        calls = []

        async def mismatched_details(parts, proxy_urls=None, source_languages=None):
            calls.append(list(parts))
            results = [("English result", "hi") for _ in parts]
            return results[:-1] if len(calls) == 2 else results

        with patch.object(L, "_WAVE_CHUNK_LIMIT", 25), \
             patch.object(L, "_google_translate_details_batch",
                          side_effect=mismatched_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertEqual(len(calls), 2)
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertIn("length mismatch", report.error)

    def test_unchanged_translation_result_fails_atomically(self):
        body = {"messages": [{"role": "user", "content": "नमस्ते"}]}
        raw = json.dumps(body, ensure_ascii=False).encode()

        async def unchanged_details(parts, proxy_urls=None, source_languages=None):
            return [(part, "hi") for part in parts]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=unchanged_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertIn("not English", report.error)

    def test_partially_non_english_translation_result_fails_atomically(self):
        body = {"messages": [{
            "role": "user", "content": "कृपया यह अनुरोध जाँचें"
        }]}
        raw = json.dumps(body, ensure_ascii=False).encode()

        async def partial_details(parts, proxy_urls=None, source_languages=None):
            return [("Please check यह अनुरोध", "hi") for _ in parts]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=partial_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertIn("not English", report.error)

    def test_large_translation_validation_scans_full_result_atomically(self):
        source = "कृपया परिणाम बताएं"
        body = {"messages": [{"role": "user", "content": source}]}
        raw = json.dumps(body, ensure_ascii=False).encode()
        residual = "हिंदी"
        translated = "E" * 3000 + residual + "E" * 5013
        self.assertGreater(len(translated), L._DETECTION_SAMPLE_SIZE)
        self.assertNotIn(residual, L._detection_sample(translated))

        async def hidden_residual_details(parts, proxy_urls=None, source_languages=None):
            return [(translated, "hi") for _ in parts]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=hidden_residual_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertIn("not English", report.error)

    def test_source_residuals_after_char_7000_fail_atomically(self):
        cases = (
            ("de", "Bitte prüfe diese Route und sage Danke", "ＤＡＮＫＥ"),
            ("fr", "Veuillez vérifier cette route et dire merci", "merci"),
            ("sw", "Tafadhali angalia njia hii", "tafadhali"),
            ("hi", "kal mujhe result bata dena", "mujhe"),
            ("es", "Por favor revisa esta ruta y di gracias", "gracias"),
        )
        base = ("This is valid English output. " * 400)[:7740].rstrip()
        prefix = base + (" " * (7741 - len(base)))
        for language, source, residual in cases:
            with self.subTest(language=language, residual=residual):
                raw = json.dumps(
                    {"messages": [{"role": "user", "content": source}]},
                    ensure_ascii=False,
                ).encode()
                translated = prefix + residual + " Final English sentence."
                self.assertEqual(translated.index(residual), 7741)
                self.assertGreater(len(translated), L._DETECTION_SAMPLE_SIZE)

                async def hidden_residual_details(parts, proxy_urls=None,
                                                  source_languages=None):
                    if source_languages:
                        normalized_residual = unicodedata.normalize(
                            "NFKC", residual).casefold()
                        replacements = {
                            "de": "thank you", "fr": "thank you",
                            "sw": "please", "hi": "me", "es": "thank you",
                        }
                        converted = []
                        for part in parts:
                            normalized_part = unicodedata.normalize("NFKC", part)
                            converted.append((re.sub(
                                re.escape(normalized_residual),
                                replacements[language], normalized_part,
                                flags=re.IGNORECASE), language))
                        return converted
                    return [(translated, language) for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=hidden_residual_details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertIn("not English", report.error)

    def test_paraphrased_latin_residuals_after_char_7000_fail_atomically(self):
        cases = (
            ("de", "Bitte prüfe diese Route", "danke", "thank you"),
            ("fr", "Veuillez vérifier cette route", "merci", "THANK YOU"),
            ("es", "Por favor revisa esta ruta", "gracias", "thank you"),
            ("hi", "kal result bata dena", "mujhe", "me"),
        )
        base = ("This is valid English output. " * 400)[:7740].rstrip()
        prefix = base + (" " * (7741 - len(base)))
        for language, source, residual, forced_word in cases:
            with self.subTest(language=language, residual=residual):
                raw = json.dumps(
                    {"messages": [{"role": "user", "content": source}]},
                    ensure_ascii=False,
                ).encode()
                translated = "Please inspect this route. " + prefix + residual
                self.assertGreater(translated.index(residual), 7000)
                self.assertNotIn(
                    residual, {token.casefold() for token in L._alphabetic_runs(source)})
                calls = []

                async def hidden_residual_details(parts, proxy_urls=None,
                                                  source_languages=None):
                    calls.append((list(parts), source_languages))
                    if source_languages:
                        return [(part.replace(residual, forced_word), language)
                                for part in parts]
                    return [(translated, language) for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=hidden_residual_details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertIn("not English", report.error)
                forced_calls = [call for call in calls if call[1]]
                if not forced_calls:
                    self.assertIn(
                        residual,
                        {token.casefold() for token in
                         L._alphabetic_runs(translated)},
                    )
                else:
                    self.assertTrue(all(item == language
                                        for item in forced_calls[0][1]))
                    self.assertTrue(any(residual in part
                                        for part in forced_calls[0][0]))

    def test_forced_source_validation_accepts_technical_english_and_places(self):
        cases = (
            ("de", "Bitte prüfe diese Route", "Berlin and Munich"),
            ("fr", "Veuillez vérifier cette route", "Paris and Lyon"),
            ("es", "Por favor revisa esta ruta", "Madrid and Toledo"),
            ("hi", "kal result bata dena", "Delhi and Mumbai"),
        )
        for language, source, places in cases:
            with self.subTest(language=language):
                raw = json.dumps(
                    {"messages": [{"role": "user", "content": source}]},
                    ensure_ascii=False,
                ).encode()
                translated = (
                    "Use Linux, Python, Docker, Redis, and GraphQL in " + places + "."
                )
                calls = []

                async def technical_details(parts, proxy_urls=None,
                                            source_languages=None):
                    calls.append((list(parts), source_languages))
                    if source_languages:
                        return [(part, language) for part in parts]
                    return [(translated, language) for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=technical_details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertTrue(report.changed)
                self.assertEqual(report.error, "")
                self.assertEqual(
                    json.loads(report.body)["messages"][0]["content"], translated)
                self.assertTrue(any(source_languages == [language]
                                    for _parts, source_languages in calls))

    def test_missing_or_english_source_metadata_fails_atomically(self):
        source = "Bitte prüfe diese Route"
        translated = "Please inspect this route; danke"
        raw = json.dumps({"messages": [{"role": "user", "content": source}]}, ensure_ascii=False).encode()
        for metadata in ("", "en"):
            with self.subTest(metadata=metadata):
                async def bad_metadata(parts, proxy_urls=None, source_languages=None):
                    return [(translated, metadata) for _ in parts]
                with patch.object(L, "_google_translate_details_batch", side_effect=bad_metadata):
                    report = asyncio.run(L.translate_request_report(raw, "anthropic"))
                self.assertFalse(report.changed)
                self.assertEqual(report.body, raw)
                self.assertIn("invalid source-language metadata", report.error)

    def test_forced_source_typeerror_fails_atomically(self):
        source = "Bitte prüfe diese Route"
        translated = "Please inspect this route; danke"
        raw = json.dumps({"messages": [{"role": "user", "content": source}]}, ensure_ascii=False).encode()
        async def typeerror_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                raise TypeError("source_languages validator exploded")
            return [(translated, "de") for _ in parts]
        with patch.object(L, "_google_translate_details_batch", side_effect=typeerror_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertFalse(report.changed)
        self.assertEqual(report.body, raw)
        self.assertIn("TypeError", report.error)

    def test_contextual_validation_accepts_names_cognates_and_embedded_english(self):
        cases = (
            ("de", "Bitte prüfe Berlin Docker und Redis", "Please check Berlin Docker and Redis."),
            ("fr", "Ce point important concerne Paris", "This important point concerns Paris."),
            ("sw", "Tafadhali angalia Nairobi routing", "Please check Nairobi routing."),
            ("hi", "GitHub check karke batao", "Check GitHub and tell me."),
            ("es", "Revisa Madrid y GraphQL", "Check Madrid and GraphQL."),
        )
        for language, source, translated in cases:
            with self.subTest(language=language):
                raw = json.dumps({"messages": [{"role": "user", "content": source}]}, ensure_ascii=False).encode()
                async def contextual_details(parts, proxy_urls=None, source_languages=None):
                    if source_languages:
                        return [(part, language) for part in parts]
                    return [(translated, language) for _ in parts]
                with patch.object(L, "_google_translate_details_batch", side_effect=contextual_details):
                    report = asyncio.run(L.translate_request_report(raw, "anthropic"))
                self.assertTrue(report.changed)
                self.assertEqual(report.error, "")
                self.assertEqual(json.loads(report.body)["messages"][0]["content"], translated)

    def test_source_aware_validation_accepts_mixed_source_technical_terms(self):
        cases = (
            (
                "de",
                "Bitte prüfe API JSON und model Claude-Opus-5",
                "Please check API JSON and model Claude-Opus-5.",
            ),
            (
                "fr",
                "Veuillez vérifier PostgreSQL endpoint et HTTP protocol",
                "Please verify the PostgreSQL endpoint and HTTP protocol.",
            ),
            (
                "sw",
                "Tafadhali angalia Kubernetes proxy routing",
                "Please check Kubernetes proxy routing.",
            ),
        )
        for language, source, output in cases:
            with self.subTest(language=language):
                raw = json.dumps(
                    {"messages": [{"role": "user", "content": source}]},
                    ensure_ascii=False,
                ).encode()

                async def technical_details(parts, proxy_urls=None,
                                            source_languages=None):
                    if source_languages:
                        return [(part, language) for part in parts]
                    return [(output, language) for _ in parts]

                with patch.object(L, "_google_translate_details_batch",
                                  side_effect=technical_details):
                    report = asyncio.run(
                        L.translate_request_report(raw, "anthropic"))
                self.assertTrue(report.changed)
                self.assertEqual(report.error, "")
                self.assertEqual(
                    json.loads(report.body)["messages"][0]["content"], output)

    def test_residual_in_protected_spans_remains_byte_exact(self):
        source = "Bitte prüfe https://example.test/danke und `danke`"
        raw = json.dumps(
            {"messages": [{"role": "user", "content": source}]},
            ensure_ascii=False,
        ).encode()

        async def technical_details(parts, proxy_urls=None,
                                    source_languages=None):
            if source_languages:
                return [(part, "de") for part in parts]
            self.assertEqual(parts, ["Bitte prüfe", "und"])
            return [("Please check", "de"), ("and", "de")]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=technical_details):
            report = asyncio.run(L.translate_request_report(raw, "anthropic"))
        self.assertTrue(report.changed)
        self.assertEqual(report.error, "")
        output = json.loads(report.body)["messages"][0]["content"]
        self.assertIn("https://example.test/danke", output)
        self.assertIn("`danke`", output)

    def test_marker_shaped_text_in_protected_fields_is_byte_exact(self):
        marker = "<<<CIEL_XLATE_00000000>>>"
        body = {"messages": [{"role": "user", "content": "इसे पढ़ो `" + marker + "`"},
                             {"role": "assistant", "content": "ठीक है", "tool_calls": [{
                                 "id": marker, "type": "function",
                                 "function": {"name": "x", "arguments": json.dumps({"v": marker})},
                             }]}]}
        async def fake_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            return [("English", "hi") for _ in parts]
        with patch.object(L, "_google_translate_details_batch", side_effect=fake_details):
            raw, changed = asyncio.run(L.translate_request(
                json.dumps(body, ensure_ascii=False).encode(), "openai"))
        self.assertTrue(changed)
        out = json.loads(raw)
        user = next(m for m in out["messages"] if m.get("role") == "user")
        assistant = next(m for m in out["messages"] if m.get("role") == "assistant")
        self.assertIn("`" + marker + "`", user["content"])
        call = assistant["tool_calls"][0]
        self.assertEqual(call["id"], marker)
        self.assertEqual(json.loads(call["function"]["arguments"])["v"], marker)

    def test_absent_tools_field_stays_absent(self):
        body = {"messages": [{"role": "user", "content": "请检查这个路由"}]}
        out, changed, _ = self.run_translate(body)
        self.assertTrue(changed)
        self.assertNotIn("tools", out)

    def test_unmatched_braces_do_not_cause_quadratic_scan(self):
        import time
        text = "{" * 10_000 + " मुझे बताओ"
        started = time.monotonic()
        L._protected_ranges(text)
        self.assertLess(time.monotonic() - started, 0.5)

    def test_developer_input_text_and_nested_schema_descriptions_translate(self):
        body = {
            "messages": [
                {"role": "developer", "content": "आप नियम ध्यान से मानें"},
                {"role": "user", "content": [{"type": "input_text", "text": "请检查这个路由"}]},
            ],
            "tools": [{"type": "function", "function": {
                "name": "check", "description": "मार्ग जांचें",
                "parameters": {"type": "object", "properties": {
                    "path": {"type": "string", "description": "जांचने वाला मार्ग"}
                }},
            }}],
        }
        out, changed, _ = self.run_translate(body, "openai")
        self.assertTrue(changed)
        developer = next(m for m in out["messages"] if m.get("role") == "developer")
        user = next(m for m in out["messages"] if m.get("role") == "user")
        self.assertTrue(developer["content"].startswith("EN<"))
        self.assertTrue(user["content"][0]["text"].startswith("EN<"))
        schema = out["tools"][0]["function"]["parameters"]
        self.assertTrue(schema["properties"]["path"]["description"].startswith("EN<"))
        self.assertEqual(schema["properties"]["path"]["type"], "string")

    def test_nested_schema_default_payload_is_never_translated(self):
        default_payload = {
            "description": "इसे जस का तस रखें",
            "nested": {"title": "请勿翻译", "value": "नमस्ते"},
        }
        body = {
            "messages": [{"role": "user", "content": "मार्ग जांचें"}],
            "tools": [{"type": "function", "function": {
                "name": "check",
                "parameters": {"type": "object", "properties": {
                    "options": {
                        "type": "object",
                        "description": "विकल्प बताएं",
                        "default": default_payload,
                        "examples": [{"description": "请勿翻译"}],
                        "const": {"title": "न बदलें"},
                        "enum": [{"description": "मत बदलो"}],
                    }
                }},
            }}],
        }
        out, changed, seen = self.run_translate(body, "openai")
        self.assertTrue(changed)
        options = out["tools"][0]["function"]["parameters"]["properties"]["options"]
        self.assertTrue(options["description"].startswith("EN<"))
        self.assertEqual(options["default"], default_payload)
        self.assertEqual(options["examples"], [{"description": "请勿翻译"}])
        self.assertEqual(options["const"], {"title": "न बदलें"})
        self.assertEqual(options["enum"], [{"description": "मत बदलो"}])
        sent = "\n".join(seen)
        for protected in ("इसे जस का तस रखें", "请勿翻译", "न बदलें", "मत बदलो"):
            self.assertNotIn(protected, sent)

    def test_code_fences_inline_code_urls_and_json_are_never_sent_to_translator(self):
        body = {"messages": [{"role": "user", "content":
            "यह देखो ```python\nprint('नमस्ते')\n``` और `API_KEY` https://x.test/p?q=हिंदी और {\"name\":\"हिंदी\"}"}]}
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        sent = "\n".join(seen)
        self.assertNotIn("print('नमस्ते')", sent)
        self.assertNotIn("API_KEY", sent)
        self.assertNotIn("https://x.test", sent)
        self.assertNotIn('{"name":"हिंदी"}', sent)
        text = out["messages"][0]["content"]
        self.assertIn("```python\nprint('नमस्ते')\n```", text)
        self.assertIn("`API_KEY`", text)
        self.assertIn("https://x.test/p?q=हिंदी", text)
        self.assertIn('{"name":"हिंदी"}', text)

    def test_parenthesized_url_is_byte_exact_and_never_translated(self):
        url = "https://x.test/मार्ग(हिंदी)"
        body = {"messages": [{"role": "user", "content": f"यह URL {url} मत बदलना"}]}
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        self.assertIn(url, out["messages"][0]["content"])
        self.assertNotIn("(हिंदी)", "\n".join(seen))

    def test_balanced_parenthesized_url_stops_before_trailing_prose(self):
        url = "https://x.test/मार्ग(हिंदी)"
        body = {"messages": [{"role": "user", "content": f"यह URL {url},फिर इसे बदलो"}]}
        out, changed, seen = self.run_translate(body)
        self.assertTrue(changed)
        self.assertIn(url, out["messages"][0]["content"])
        sent = "\n".join(seen)
        self.assertNotIn("(हिंदी)", sent)
        self.assertIn("इसे बदलो", sent)

    def test_payload_strictly_over_four_million_realistic_hindi_hinglish(self):
        paragraphs = [
            "आज gateway की request pipeline का विस्तृत परीक्षण करना है ताकि हर संदेश सुरक्षित तरीके से English में बदले और कोई उपयोगी संदर्भ न छूटे। ",
            "कृपया routing की पूरी स्थिति समझाकर बताओ, lekin model name, tool id, arguments aur configuration values bilkul mat badalna। ",
            "जब upstream उपलब्ध न हो तब सही कारण log करना और साफ बताना कि request आगे भेजी गई थी या पहले ही रोक दी गई थी। ",
            "लंबी बातचीत में bounded waves का उपयोग करो ताकि memory नियंत्रित रहे और हर paragraph का क्रम उसी तरह बना रहे। ",
            "Code fence, inline command, URL और JSON उदाहरण जस के तस रहने चाहिए क्योंकि इनका एक byte बदलना भी integration को तोड़ सकता है। ",
        ]
        prose = ""
        index = 0
        while len(prose) <= 4_000_000:
            prose += f"अनुच्छेद {index}: {paragraphs[index % len(paragraphs)]}"
            index += 1
        protected = (
            "\n```python\nMODEL = 'claude-opus-4-8'\nprint(MODEL)\n```\n"
            "`max_tokens` https://example.test/मार्ग?q=हिंदी "
            '{"model":"claude-opus-4-8","tool_id":"toolu_exact","text":"नमस्ते"}'
        )
        content = prose + protected
        body = {"model": "claude-opus-4-8", "messages": [{
            "role": "user", "content": content,
        }]}
        calls = []

        async def deterministic_details(parts, proxy_urls=None, source_languages=None):
            if source_languages:
                return [(part, language) for part, language in
                        zip(parts, source_languages)]
            calls.append(list(parts))
            return [("English translated paragraph. ", "hi") for _ in parts]

        with patch.object(L, "_google_translate_details_batch",
                          side_effect=deterministic_details):
            report = asyncio.run(L.translate_request_report(
                json.dumps(body, ensure_ascii=False).encode(), "anthropic"))
        self.assertGreater(len(content), 4_000_000)
        self.assertEqual(report.error, "")
        self.assertTrue(report.changed)
        self.assertGreater(sum(len(wave) for wave in calls), 1000)
        self.assertTrue(all(len(wave) <= L._WAVE_CHUNK_LIMIT for wave in calls))
        out = json.loads(report.body)
        translated = out["messages"][0]["content"]
        self.assertIn(protected, translated)
        self.assertEqual(out["model"], "claude-opus-4-8")
        self.assertNotIn("आज gateway", translated)

    def test_translation_route_fallback_uses_next_backend_and_logs_causes(self):
        class FakeResponse:
            def __init__(self, ok):
                self.ok = ok

            def raise_for_status(self):
                if not self.ok:
                    raise RuntimeError("primary backend unavailable")

            def json(self):
                return [[["Hello", None, None, None]], None, "hi"]

        called = []

        class FakeClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def post(self, url, **_kwargs):
                called.append(url)
                return FakeResponse(url == L._TRANSLATION_BACKENDS[1])

        with patch.object(L.httpx, "AsyncClient", return_value=FakeClient()):
            result = asyncio.run(L._google_translate_details_batch(["नमस्ते"]))
        self.assertEqual(result, [("Hello", "hi")])
        self.assertEqual(called, list(L._TRANSLATION_BACKENDS))

    def test_slow_proxy_times_out_per_route_then_direct_succeeds(self):
        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return [[["Hello", None, None, None]], None, "hi"]

        called = []

        class FakeClient:
            def __init__(self, proxy=None, **_kwargs):
                self.proxy = proxy

            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False

            async def post(self, url, **_kwargs):
                called.append((self.proxy, url))
                if self.proxy:
                    await asyncio.Event().wait()
                return FakeResponse()

        with patch.object(L, "_ROUTE_TIMEOUT_SECONDS", 0.02), \
             patch.object(L.httpx, "AsyncClient", side_effect=FakeClient):
            result = asyncio.run(L._google_translate_details_batch(
                ["नमस्ते"], proxy_urls=["http://dead-proxy"]))
        self.assertEqual(result, [("Hello", "hi")])
        self.assertEqual(called, [
            ("http://dead-proxy", L._TRANSLATION_BACKENDS[0]),
            ("http://dead-proxy", L._TRANSLATION_BACKENDS[1]),
            (None, L._TRANSLATION_BACKENDS[0]),
        ])

    def test_all_translation_route_failures_include_every_exact_cause(self):
        class FakeClient:
            def __init__(self, proxy=None, **_kwargs):
                self.proxy = proxy

            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def post(self, url, **_kwargs):
                raise RuntimeError(
                    f"blocked proxy={self.proxy or 'direct'} backend={url}")

        with patch.object(L.httpx, "AsyncClient", side_effect=FakeClient):
            with self.assertRaises(RuntimeError) as raised:
                asyncio.run(L._google_translate_details_batch(
                    ["नमस्ते"], proxy_urls=["http://proxy-one"]))
        detail = str(raised.exception)
        for backend in L._TRANSLATION_BACKENDS:
            self.assertIn(backend, detail)
        self.assertIn("proxy=http://proxy-one", detail)
        self.assertIn("proxy=direct", detail)
        self.assertIn("RuntimeError", detail)
        expected_routes = (
            ("http://proxy-one", L._TRANSLATION_BACKENDS[0]),
            ("http://proxy-one", L._TRANSLATION_BACKENDS[1]),
            ("direct", L._TRANSLATION_BACKENDS[0]),
            ("direct", L._TRANSLATION_BACKENDS[1]),
        )
        for proxy, backend in expected_routes:
            self.assertIn(
                f"backend={backend} proxy={proxy}: RuntimeError: "
                f"blocked proxy={proxy} backend={backend}",
                detail,
            )

    def test_preserves_spacing_around_protected_spans(self):
        body = {"messages": [{"role": "user", "content":
            "API key मत बदलना। `max_tokens` same रखो।"}]}
        out, changed, _ = self.run_translate(body)
        self.assertTrue(changed)
        text = out["messages"][0]["content"]
        self.assertRegex(text, r">\s+`max_tokens`\s+EN<")


if __name__ == "__main__":
    unittest.main(verbosity=2)
