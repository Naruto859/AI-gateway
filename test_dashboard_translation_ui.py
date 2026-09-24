#!/usr/bin/env python3
"""Regression tests for saved LLM backend values in the dashboard UI.

The dashboard has no separate frontend package/test runner. These tests enforce
its server-contract wiring at source level: the saved-backend list must use the
GET route, and the edit modal must map non-secret fields directly from that row
while keeping API keys masked.
"""
import re
import unittest
from pathlib import Path


DASHBOARD = Path(__file__).parent / "static" / "dashboard.html"


class DashboardTranslationUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = DASHBOARD.read_text(encoding="utf-8")

    def test_saved_backend_list_uses_get_route(self):
        match = re.search(
            r"async function _txLoadBackends\(\) \{(?P<body>.*?)\n  \}",
            self.source,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        self.assertIn("apiGet('/admin/translation/list')", body)
        self.assertNotIn("api('/admin/translation/list')", body)

    def test_edit_modal_populates_saved_non_secret_fields(self):
        match = re.search(
            r"function _txOpenBackendModal\(editId\) \{(?P<body>.*?)\n  \}",
            self.source,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        body = match.group("body")
        # (input id, DB row field) — non-secret fields must map straight from the row
        for input_id, field in (
            ("txbName", "name"),
            ("txbUrl", "url"),
            ("txbModel", "model"),
            ("txbPrompt", "system_prompt"),
            ("txbRpm", "rpm"),
            ("txbChunk", "chunk_chars"),
        ):
            self.assertRegex(body, rf"\$\('#{input_id}'\)\.value = row\.{field}")
        self.assertIn("$('#txbKey').value = '';", body)
        self.assertIn("row.api_key ? '•••• saved (blank = keep)' : 'sk-…'", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
