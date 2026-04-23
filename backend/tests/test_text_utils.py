from __future__ import annotations

import unittest

from app.utils.text import strip_html


class TextUtilsTest(unittest.TestCase):
    def test_strip_html_removes_encoded_tags_and_decodes_entities(self) -> None:
        raw = "&lt;div class=&quot;content&quot;&gt;Build dashboards &amp; reports&lt;/div&gt;"

        self.assertEqual(strip_html(raw), "Build dashboards & reports")

    def test_strip_html_removes_script_and_style_blocks(self) -> None:
        raw = "<style>.x{}</style><p>Readable text</p><script>alert('x')</script>"

        self.assertEqual(strip_html(raw), "Readable text")


if __name__ == "__main__":
    unittest.main()
