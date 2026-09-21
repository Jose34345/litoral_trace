from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC = ROOT / "src" / "litoral_trace" / "static"


class LaceySocialMetadataTests(unittest.TestCase):
    def test_lacey_landing_has_complete_absolute_social_metadata(self):
        source = (TEMPLATES / "public" / "lacey.html").read_text(encoding="utf-8")

        title = "Litoral Trace — U.S. Lacey Act Compliance Automation"
        description = (
            "From supplier and shipment documents to structured Lacey Act data: "
            "species, BOM, country of harvest, supplier evidence, exception review "
            "and review-ready LAWGS XML."
        )
        image_url = "https://lacey.litoraltrace.com/static/img/litoral-trace-lacey-og.png"

        self.assertIn(f"{{% block title %}}{title}{{% endblock %}}", source)
        self.assertIn(f'<meta name="description" content="{description}">', source)
        self.assertIn('<link rel="canonical" href="https://lacey.litoraltrace.com/">', source)
        self.assertIn('<meta property="og:type" content="website">', source)
        self.assertIn('<meta property="og:site_name" content="Litoral Trace">', source)
        self.assertIn(f'<meta property="og:title" content="{title}">', source)
        self.assertIn(f'<meta property="og:description" content="{description}">', source)
        self.assertIn('<meta property="og:url" content="https://lacey.litoraltrace.com/">', source)
        self.assertIn(f'<meta property="og:image" content="{image_url}">', source)
        self.assertIn('<meta property="og:image:width" content="1200">', source)
        self.assertIn('<meta property="og:image:height" content="627">', source)
        self.assertIn('<meta property="og:image:type" content="image/png">', source)
        self.assertIn('<meta property="og:image:alt" content="Litoral Trace U.S. Lacey Act Compliance Automation">', source)
        self.assertIn('<meta name="twitter:card" content="summary_large_image">', source)
        self.assertIn(f'<meta name="twitter:title" content="{title}">', source)
        self.assertIn(f'<meta name="twitter:description" content="{description}">', source)
        self.assertIn(f'<meta name="twitter:image" content="{image_url}">', source)


    def test_lacey_social_card_is_a_public_static_png_asset(self):
        asset = STATIC / "img" / "litoral-trace-lacey-og.png"

        self.assertTrue(asset.is_file())
        self.assertTrue(asset.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
