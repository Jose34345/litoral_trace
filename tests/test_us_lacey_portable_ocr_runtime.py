from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


def _runtime_module():
    from litoral_trace.assurance import ocr_runtime

    return ocr_runtime


def test_portable_runtime_bootstraps_verified_binary_and_languages(tmp_path, monkeypatch):
    runtime = _runtime_module()
    monkeypatch.setenv("LT_ASSURANCE_PORTABLE_TESSERACT_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_OCR_RUNTIME_DIR", str(tmp_path / "ocr-runtime"))
    monkeypatch.setattr(runtime.shutil, "which", lambda _name: None)
    monkeypatch.setattr(runtime.platform, "machine", lambda: "x86_64")

    downloaded: list[str] = []

    def fake_download(*, url, expected_sha256, destination, max_bytes):
        del expected_sha256, max_bytes
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"verified-runtime-asset")
        downloaded.append(url)

    monkeypatch.setattr(runtime, "_download_verified", fake_download)
    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="List of available languages in tessdata (2):\neng\nspa\n",
            stderr="",
        ),
    )

    result = runtime.ensure_tesseract_runtime()

    assert result.ready is True
    assert result.source == "portable"
    assert result.binary is not None
    binary = Path(result.binary)
    assert binary.is_file()
    assert os.access(binary, os.X_OK)
    assert Path(os.environ["TESSDATA_PREFIX"]).name == "tessdata"
    assert str(binary.parent) == os.environ["PATH"].split(os.pathsep)[0]
    assert any(url.endswith("tesseract.x86_64") for url in downloaded)
    assert any(url.endswith("eng.traineddata") for url in downloaded)
    assert any(url.endswith("spa.traineddata") for url in downloaded)


def test_verified_download_rejects_hash_mismatch_and_leaves_no_destination(tmp_path, monkeypatch):
    runtime = _runtime_module()
    payload = b"tampered"

    class _Response:
        headers = {"Content-Length": str(len(payload))}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self, _size=-1):
            if getattr(self, "_done", False):
                return b""
            self._done = True
            return payload

    monkeypatch.setattr(runtime.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response())
    destination = tmp_path / "asset"

    with pytest.raises(runtime.PortableOCRRuntimeError):
        runtime._download_verified(
            url="https://example.invalid/asset",
            expected_sha256=hashlib.sha256(b"expected").hexdigest(),
            destination=destination,
            max_bytes=1024,
        )

    assert destination.exists() is False
    assert list(tmp_path.glob("*.tmp")) == []


def test_portable_runtime_fails_closed_when_asset_bootstrap_fails(tmp_path, monkeypatch):
    runtime = _runtime_module()
    monkeypatch.setenv("LT_ASSURANCE_PORTABLE_TESSERACT_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_OCR_RUNTIME_DIR", str(tmp_path / "ocr-runtime"))
    monkeypatch.setattr(runtime.shutil, "which", lambda _name: None)
    monkeypatch.setattr(runtime.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        runtime,
        "_download_verified",
        lambda **_kwargs: (_ for _ in ()).throw(runtime.PortableOCRRuntimeError("hash mismatch")),
    )

    result = runtime.ensure_tesseract_runtime()

    assert result.ready is False
    assert result.binary is None
    assert result.source == "unavailable"
    assert result.error_code == "OCR_PORTABLE_RUNTIME_BOOTSTRAP_FAILED"


def test_native_tesseract_wins_without_portable_download(monkeypatch):
    runtime = _runtime_module()
    monkeypatch.delenv("LT_ASSURANCE_PORTABLE_TESSERACT_ENABLED", raising=False)
    monkeypatch.setattr(runtime.shutil, "which", lambda name: "/usr/bin/tesseract" if name == "tesseract" else None)

    result = runtime.ensure_tesseract_runtime()

    assert result.ready is True
    assert result.binary == "/usr/bin/tesseract"
    assert result.source == "native"
