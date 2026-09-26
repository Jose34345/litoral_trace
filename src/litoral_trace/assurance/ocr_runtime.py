"""Verified Tesseract bootstrap for native cloud runtimes without OS packages.

Native system Tesseract remains preferred.  When explicitly enabled, this module
can bootstrap a pinned portable Linux binary plus pinned English/Spanish
language data into an ephemeral runtime directory.  Every downloaded asset is
verified by SHA-256 before it becomes executable or visible through PATH.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import platform
import shutil
import subprocess
import threading
import urllib.request
from urllib.parse import urlparse


_PORTABLE_ENABLED_ENV = "LT_ASSURANCE_PORTABLE_TESSERACT_ENABLED"
_RUNTIME_DIR_ENV = "LT_ASSURANCE_OCR_RUNTIME_DIR"
_DEFAULT_RUNTIME_DIR = "/tmp/litoral-trace-ocr"
_REQUIRED_LANGUAGES = frozenset({"eng", "spa"})
_DOWNLOAD_TIMEOUT_SECONDS = 30
_BINARY_MAX_BYTES = 100 * 1024 * 1024
_LANGUAGE_MAX_BYTES = 20 * 1024 * 1024
_RUNTIME_LOCK = threading.Lock()

_BINARY_ASSETS = {
    "x86_64": (
        "https://github.com/DanielMYT/tesseract-static/releases/download/"
        "tesseract-5.5.3/tesseract.x86_64",
        "1ee53ab818ba128de01ba631e09911c5b6ed6cb63c741323097f209b797f68b0",
    ),
    "amd64": (
        "https://github.com/DanielMYT/tesseract-static/releases/download/"
        "tesseract-5.5.3/tesseract.x86_64",
        "1ee53ab818ba128de01ba631e09911c5b6ed6cb63c741323097f209b797f68b0",
    ),
    "aarch64": (
        "https://github.com/DanielMYT/tesseract-static/releases/download/"
        "tesseract-5.5.3/tesseract.aarch64",
        "7c14e7272fe24e397e665e02151a7ba494e966a8543e4bd0437085a6196f126d",
    ),
    "arm64": (
        "https://github.com/DanielMYT/tesseract-static/releases/download/"
        "tesseract-5.5.3/tesseract.aarch64",
        "7c14e7272fe24e397e665e02151a7ba494e966a8543e4bd0437085a6196f126d",
    ),
}

_LANGUAGE_ASSETS = {
    "eng": (
        "https://github.com/tesseract-ocr/tessdata_fast/raw/4.1.0/eng.traineddata",
        "7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2",
    ),
    "spa": (
        "https://github.com/tesseract-ocr/tessdata_fast/raw/4.1.0/spa.traineddata",
        "6f2e04d02774a18f01bed44b1111f2cd7f3ba7ac9dc4373cd3f898a40ea6b464",
    ),
}


class PortableOCRRuntimeError(RuntimeError):
    """Raised when a portable OCR asset cannot be trusted or prepared safely."""


@dataclass(frozen=True, slots=True)
class TesseractRuntime:
    ready: bool
    binary: str | None
    source: str
    error_code: str | None = None


def _env_enabled(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_verified(
    *,
    url: str,
    expected_sha256: str,
    destination: Path,
    max_bytes: int,
) -> None:
    """Download one pinned HTTPS asset and atomically install it after SHA-256 verification."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise PortableOCRRuntimeError("portable OCR assets must use HTTPS")
    if max_bytes <= 0:
        raise PortableOCRRuntimeError("portable OCR asset size limit is invalid")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.tmp")
    temporary.unlink(missing_ok=True)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "LitoralTrace-OCR-Runtime/1"},
    )
    digest = hashlib.sha256()
    total = 0
    try:
        with urllib.request.urlopen(request, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response:
            raw_length = response.headers.get("Content-Length")
            if raw_length:
                try:
                    declared_length = int(raw_length)
                except ValueError as exc:
                    raise PortableOCRRuntimeError("invalid portable OCR Content-Length") from exc
                if declared_length < 0 or declared_length > max_bytes:
                    raise PortableOCRRuntimeError("portable OCR asset exceeds size limit")

            with temporary.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise PortableOCRRuntimeError("portable OCR asset exceeds size limit")
                    digest.update(chunk)
                    handle.write(chunk)

        if total <= 0:
            raise PortableOCRRuntimeError("portable OCR asset is empty")
        if digest.hexdigest().lower() != expected_sha256.lower():
            raise PortableOCRRuntimeError("portable OCR asset SHA-256 mismatch")
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _ensure_verified_asset(
    *,
    url: str,
    expected_sha256: str,
    destination: Path,
    max_bytes: int,
) -> None:
    if destination.is_file():
        if _sha256(destination).lower() == expected_sha256.lower():
            return
        destination.unlink(missing_ok=True)
    _download_verified(
        url=url,
        expected_sha256=expected_sha256,
        destination=destination,
        max_bytes=max_bytes,
    )


def _verify_runtime(binary: str, *, tessdata_dir: Path | None = None) -> bool:
    environment = os.environ.copy()
    if tessdata_dir is not None:
        environment["TESSDATA_PREFIX"] = str(tessdata_dir)
    try:
        completed = subprocess.run(
            [binary, "--list-langs"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if completed.returncode != 0:
        return False
    languages = {
        line.strip()
        for line in completed.stdout.splitlines()
        if line.strip() and not line.lower().startswith("list of available languages")
    }
    return _REQUIRED_LANGUAGES.issubset(languages)


def _portable_binary_asset() -> tuple[str, str]:
    machine = platform.machine().strip().lower()
    try:
        return _BINARY_ASSETS[machine]
    except KeyError as exc:
        raise PortableOCRRuntimeError(f"unsupported portable OCR architecture: {machine or 'unknown'}") from exc


def _prepend_path(directory: Path) -> None:
    directory_text = str(directory)
    current = os.environ.get("PATH", "")
    parts = [part for part in current.split(os.pathsep) if part]
    if parts and parts[0] == directory_text:
        return
    os.environ["PATH"] = os.pathsep.join([directory_text, *[part for part in parts if part != directory_text]])


def ensure_tesseract_runtime() -> TesseractRuntime:
    """Return a verified native or explicitly enabled portable Tesseract runtime.

    The portable path is fail-closed: no unverified bytes are executed and a failed
    bootstrap is reported as unavailable rather than silently degrading readiness.
    """
    native = shutil.which("tesseract")
    if native and _verify_runtime(native):
        return TesseractRuntime(True, native, "native")

    if not _env_enabled(_PORTABLE_ENABLED_ENV):
        error_code = (
            "OCR_TESSERACT_LANGUAGES_UNAVAILABLE"
            if native
            else "OCR_TESSERACT_BINARY_UNAVAILABLE"
        )
        return TesseractRuntime(False, None, "unavailable", error_code)

    with _RUNTIME_LOCK:
        try:
            runtime_dir = Path(os.getenv(_RUNTIME_DIR_ENV, _DEFAULT_RUNTIME_DIR)).expanduser()
            runtime_dir.mkdir(parents=True, exist_ok=True)
            tessdata_dir = runtime_dir / "tessdata"
            binary = runtime_dir / "tesseract"

            binary_url, binary_sha = _portable_binary_asset()
            _ensure_verified_asset(
                url=binary_url,
                expected_sha256=binary_sha,
                destination=binary,
                max_bytes=_BINARY_MAX_BYTES,
            )
            for language, (url, expected_sha) in _LANGUAGE_ASSETS.items():
                _ensure_verified_asset(
                    url=url,
                    expected_sha256=expected_sha,
                    destination=tessdata_dir / f"{language}.traineddata",
                    max_bytes=_LANGUAGE_MAX_BYTES,
                )

            binary.chmod(binary.stat().st_mode | 0o111)
            previous_path = os.environ.get("PATH")
            previous_tessdata = os.environ.get("TESSDATA_PREFIX")
            _prepend_path(runtime_dir)
            os.environ["TESSDATA_PREFIX"] = str(tessdata_dir)
            if not _verify_runtime(str(binary), tessdata_dir=tessdata_dir):
                if previous_path is None:
                    os.environ.pop("PATH", None)
                else:
                    os.environ["PATH"] = previous_path
                if previous_tessdata is None:
                    os.environ.pop("TESSDATA_PREFIX", None)
                else:
                    os.environ["TESSDATA_PREFIX"] = previous_tessdata
                raise PortableOCRRuntimeError("portable Tesseract language verification failed")
            return TesseractRuntime(True, str(binary), "portable")
        except Exception:
            return TesseractRuntime(
                False,
                None,
                "unavailable",
                "OCR_PORTABLE_RUNTIME_BOOTSTRAP_FAILED",
            )
