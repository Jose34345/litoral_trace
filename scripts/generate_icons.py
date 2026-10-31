from pathlib import Path
from io import BytesIO

import cairosvg
from PIL import Image

# Input / output paths
BASE_DIR = Path(__file__).resolve().parent.parent
SVG_PATH = BASE_DIR / "src" / "litoral_trace" / "static" / "img" / "logo.svg"
STATIC_DIR = BASE_DIR / "src" / "litoral_trace" / "static"

FAVICON_ICO = STATIC_DIR / "favicon.ico"
APPLE_TOUCH_ICON = STATIC_DIR / "apple-touch-icon.png"

# Temporary PNG sizes for favicon
FAVICON_SIZES = [16, 32, 48]
APPLE_SIZE = 180


def svg_to_png(svg_path: Path, size: int) -> Image.Image:
    """
    Render an SVG to a Pillow RGBA image at the given square size.
    """
    png_bytes = cairosvg.svg2png(
        url=str(svg_path),
        output_width=size,
        output_height=size,
        background_color=None,
    )
    return Image.open(BytesIO(png_bytes)).convert("RGBA")


def main() -> None:
    if not SVG_PATH.exists():
        raise FileNotFoundError(f"SVG not found: {SVG_PATH}")

    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    # Render once at the largest favicon size; Pillow writes the requested
    # embedded resolutions into a single ICO container.
    favicon_source = svg_to_png(SVG_PATH, max(FAVICON_SIZES))

    # Save multi-resolution favicon.ico
    favicon_source.save(
        FAVICON_ICO,
        format="ICO",
        sizes=[(s, s) for s in FAVICON_SIZES],
    )

    # Save Apple touch icon
    apple_img = svg_to_png(SVG_PATH, APPLE_SIZE)
    apple_img.save(APPLE_TOUCH_ICON, format="PNG")

    print("Generated:")
    print(f" - {FAVICON_ICO}")
    print(f" - {APPLE_TOUCH_ICON}")


if __name__ == "__main__":
    main()
