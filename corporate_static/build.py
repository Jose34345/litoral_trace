"""Build a self-contained static copy of the existing corporate landing.

This intentionally uses only the public Jinja templates and bundled static assets.
No production database, secrets, application startup, or web requests are needed.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
ASSETS = ROOT / "src" / "litoral_trace" / "static"
OUTPUT = ROOT / "corporate_static" / "dist"
LACEY_URL = "https://lacey.litoraltrace.com/"


def _static_url(name: str, path: str) -> str:
    if name != "static":
        raise ValueError(f"Unexpected URL route in static export: {name}")
    return "/static/" + path.lstrip("/")


MOBILE_NAV_SCRIPT = """
<script>
(() => {
  const trigger = document.querySelector('[data-public-nav-trigger]');
  const panel = document.querySelector('[data-public-nav-panel]');
  if (!trigger || !panel) return;
  const close = () => { panel.hidden = true; trigger.setAttribute('aria-expanded', 'false'); };
  trigger.addEventListener('click', () => {
    const open = panel.hidden;
    panel.hidden = !open;
    trigger.setAttribute('aria-expanded', String(open));
  });
  panel.addEventListener('click', (event) => {
    if (event.target.closest('a')) close();
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') close();
  });
})();
</script>
"""


def build(output: Path = OUTPUT) -> Path:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html", "xml"]),
        keep_trailing_newline=True,
    )
    env.globals["url_for"] = _static_url
    document = env.get_template("public/home.html").render()

    # Remove app-only assets (HTMX, Leaflet, form and dashboard scripts).
    document = re.sub(
        r'\s*<link\s+rel="stylesheet"\s+href="/static/[^"]+"\s*>',
        "",
        document,
    )
    document = re.sub(
        r'\s*<script\b[^>]*\bsrc="/static/[^"]+"[^>]*></script>',
        "",
        document,
    )

    # A static site cannot serve the authenticated application or regional API.
    # All product access remains in the existing Lacey application.
    document = document.replace('href="/login"', f'href="{LACEY_URL}"')
    document = document.replace(
        'href="/regional-intelligence"',
        'href="/#regional-intelligence"',
    )

    # Keep the corporate layout but make the principal CTA lead to the live product.
    cta_pattern = (
        r'(<a href=")mailto:comercial@litoraltrace\.com\?subject='
        r'Solicitud%20de%20demo%20Litoral%20Trace'
        r'(" class="inline-flex items-center gap-2 rounded-lg bg-emerald-400[^"]*">)'
        r'Solicitar demostración'
    )
    document, count = re.subn(
        cta_pattern,
        lambda match: match.group(1) + LACEY_URL + match.group(2)
        + "Explorar Lacey Act",
        document,
        count=1,
    )
    if count != 1:
        raise RuntimeError("Could not identify the existing hero CTA")

    # The current product does not offer EUDR certification.
    document = document.replace(
        "Litoral Trace no emite certificaciones EUDR",
        "Litoral Trace no emite certificaciones regulatorias",
    )
    document = document.replace(
        "</head>",
        '\n    <meta name="description" content="Litoral Trace organiza evidencia documental y trazabilidad para cadenas forestales. Conocé el producto Lacey Act para importadores de Estados Unidos.">'
        '\n    <link rel="canonical" href="https://litoraltrace.com/">'
        '\n    <link rel="stylesheet" href="/assets/app.css">'
        '\n    <link rel="stylesheet" href="/assets/fontawesome/css/all.min.css">'
        "\n</head>",
        1,
    )
    document = document.replace("</body>", MOBILE_NAV_SCRIPT + "\n</body>", 1)

    if any(x in document for x in ("{{", "{%", 'href="/login"', "/static/")):
        raise RuntimeError("The page contains unresolved Jinja or backend-only references")

    output.mkdir(parents=True, exist_ok=True)
    (output / "index.html").write_text(document, encoding="utf-8")
    css_dest = output / "assets" / "app.css"
    css_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ASSETS / "dist" / "app.css", css_dest)

    icon_source = ASSETS / "vendor" / "fontawesome"
    icon_dest = output / "assets" / "fontawesome"
    (icon_dest / "css").mkdir(parents=True, exist_ok=True)
    (icon_dest / "webfonts").mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        icon_source / "css" / "all.min.css",
        icon_dest / "css" / "all.min.css",
    )
    for font in (icon_source / "webfonts").glob("*.woff2"):
        shutil.copyfile(font, icon_dest / "webfonts" / font.name)

    (output / "robots.txt").write_text(
        "User-agent: *\nAllow: /\nSitemap: https://litoraltrace.com/sitemap.xml\n",
        encoding="utf-8",
    )
    (output / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        '<url><loc>https://litoraltrace.com/</loc></url></urlset>\n',
        encoding="utf-8",
    )
    (output / "404.html").write_text(
        '<!doctype html><html lang="es"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Página no encontrada | Litoral Trace</title>'
        '<body style="font:16px system-ui;padding:8vw;max-width:42rem">'
        '<h1>Página no encontrada</h1><p>Esta página ya no está disponible.</p>'
        '<p><a href="/">Volver a Litoral Trace</a> · '
        '<a href="https://lacey.litoraltrace.com/">Abrir Lacey Act</a></p>'
        '</body></html>\n',
        encoding="utf-8",
    )
    return output


if __name__ == "__main__":
    built = build()
    print(f"Static corporate landing generated: {built}")
