"""Offline acceptance checks for the static corporate site."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "corporate_static"))
from build import build  # noqa: E402


class CorporateStaticSiteTests(unittest.TestCase):
    def test_export_is_backend_independent(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            destination = build(Path(folder))
            html = (destination / "index.html").read_text(encoding="utf-8")
            self.assertIn("Litoral Trace", html)
            self.assertIn("https://lacey.litoraltrace.com/", html)
            self.assertIn("rel=\"canonical\"", html)
            self.assertIn("data-public-nav-trigger", html)
            self.assertIn("aria-expanded", html)
            self.assertNotIn("{%", html)
            self.assertNotIn("{{", html)
            self.assertNotIn('href="/login"', html)
            self.assertNotIn('href="/regional-intelligence"', html)
            self.assertNotIn('src="/static/', html)
            self.assertNotIn('href="/static/', html)
            self.assertNotIn("htmx.min.js", html)
            self.assertNotIn("leaflet.js", html)

            for path in (
                "index.html",
                "404.html",
                "robots.txt",
                "sitemap.xml",
                "assets/app.css",
                "assets/fontawesome/css/all.min.css",
                "assets/fontawesome/webfonts/fa-solid-900.woff2",
            ):
                self.assertTrue((destination / path).is_file(), path)

    def test_export_rejects_unrendered_backend_links(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            html = (build(Path(folder)) / "index.html").read_text(encoding="utf-8")
            self.assertIn('href="/#regional-intelligence"', html)
            self.assertIn('href="https://lacey.litoraltrace.com/"', html)
            self.assertNotIn("certificaciones EUDR", html)


if __name__ == "__main__":
    unittest.main()
