"""Provider adapters for AI Extraction Shadow v1.

Provider choice is runtime configuration only. Qwen/Ollama is the free local path;
Mistral OCR is the low-cost hosted path. Neither adapter mutates operational data.
"""
from __future__ import annotations

from dataclasses import dataclass
import base64
from io import BytesIO
import json
import mimetypes
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .ai_shadow import (
    AI_FIELDS,
    AIExtractionResult,
    AIShadowError,
    extraction_result_from_payload,
)


AI_SHADOW_OFF = "OFF"
AI_SHADOW_SHADOW = "SHADOW"
PROVIDER_QWEN_OLLAMA = "qwen_ollama"
PROVIDER_MISTRAL_OCR = "mistral_ocr"


@dataclass(frozen=True, slots=True)
class AIProviderConfig:
    mode: str
    provider: str
    model: str
    base_url: str
    api_key: str | None
    timeout_seconds: float
    max_pages: int
    allow_external: bool

    @classmethod
    def from_env(cls) -> "AIProviderConfig":
        mode = os.getenv("US_LACEY_AI_SHADOW_MODE", "off").strip().upper()
        if mode not in {AI_SHADOW_OFF, AI_SHADOW_SHADOW}:
            mode = AI_SHADOW_OFF
        provider = os.getenv("US_LACEY_AI_PROVIDER", PROVIDER_QWEN_OLLAMA).strip().lower()
        if provider == PROVIDER_MISTRAL_OCR:
            default_model = "mistral-ocr-latest"
            default_url = "https://api.mistral.ai/v1/ocr"
        else:
            provider = PROVIDER_QWEN_OLLAMA
            default_model = "qwen2.5vl:7b"
            default_url = "http://127.0.0.1:11434/api/chat"
        model = os.getenv("US_LACEY_AI_MODEL", default_model).strip() or default_model
        base_url = os.getenv("US_LACEY_AI_BASE_URL", default_url).strip() or default_url
        api_key = os.getenv("US_LACEY_AI_API_KEY", "").strip() or None
        try:
            timeout_seconds = max(5.0, min(float(os.getenv("US_LACEY_AI_TIMEOUT_SECONDS", "90")), 300.0))
        except ValueError:
            timeout_seconds = 90.0
        try:
            max_pages = max(1, min(int(os.getenv("US_LACEY_AI_MAX_PAGES", "8")), 32))
        except ValueError:
            max_pages = 8
        allow_external = os.getenv("US_LACEY_AI_ALLOW_EXTERNAL", "0").strip().lower() in {"1", "true", "yes", "on"}
        return cls(mode, provider, model, base_url, api_key, timeout_seconds, max_pages, allow_external)


def ai_shadow_enabled(config: AIProviderConfig | None = None) -> bool:
    return (config or AIProviderConfig.from_env()).mode == AI_SHADOW_SHADOW


def ai_shadow_engine_version(config: AIProviderConfig) -> str:
    raw = f"ai-shadow-v1:{config.provider}:{config.model}"
    return raw[:100]


_CANDIDATE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field_key": {"type": "string", "enum": list(AI_FIELDS)},
                    "value": {"type": "string"},
                    "evidence_class": {"type": "string", "enum": ["EXPLICIT", "DERIVED", "INFERRED"]},
                    "page": {"type": "integer", "minimum": 1},
                    "source_text": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "bbox": {
                        "anyOf": [
                            {"type": "array", "minItems": 4, "maxItems": 4, "items": {"type": "number"}},
                            {"type": "null"},
                        ]
                    },
                    "reason": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                },
                "required": ["field_key", "value", "evidence_class", "page", "source_text", "confidence", "bbox", "reason"],
            },
        }
    },
    "required": ["candidates"],
}

_PROMPT = """Extract candidate values for a U.S. Lacey Act declaration review from the supplied document.
Return only fields supported by the document and only the JSON schema requested.
Rules:
- Every candidate needs the exact source text and 1-indexed page number.
- EXPLICIT means the value is stated in the document.
- DERIVED is allowed only for a deterministic transformation of explicit text.
- INFERRED means reasoning beyond explicit evidence; mark it INFERRED even if plausible.
- Never infer country_of_harvest from country of origin, exporter address, manufacturer address, port of lading, vessel route, or shipper location.
- Never use shipment gross weight as plant_quantity unless the document explicitly identifies it as the quantity of plant material for the declaration.
- Never invent HTS, MID/manufacturer_id, filing entry reference, or importer information.
- Generic labels such as Seal Number, Equipment Description, City, State Province, Zip Code, Marks and Numbers, or a URL are not container_number, consignee_name, or description values.
- Prefer exact documentary evidence over contextual guesses.
- If a field is absent, emit no candidate for that field.
"""


def _post_json(*, url: str, payload: dict[str, object], timeout: float, headers: dict[str, str] | None = None) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    merged_headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        merged_headers.update(headers)
    request = Request(url, data=body, headers=merged_headers, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except HTTPError as exc:
        raise AIShadowError(f"AI provider returned HTTP {exc.code}.") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise AIShadowError("AI provider request failed.") from exc
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AIShadowError("AI provider returned invalid JSON.") from exc
    if not isinstance(parsed, dict):
        raise AIShadowError("AI provider returned an invalid response object.")
    return parsed


def _render_pdf_pages(content: bytes, max_pages: int) -> list[bytes]:
    try:
        import pypdfium2 as pdfium
    except ImportError as exc:
        raise AIShadowError("PDF vision rendering is unavailable.") from exc
    try:
        document = pdfium.PdfDocument(content)
        rendered: list[bytes] = []
        for index in range(min(len(document), max_pages)):
            page = document[index]
            bitmap = page.render(scale=1.5)
            image = bitmap.to_pil()
            stream = BytesIO()
            image.save(stream, format="PNG")
            rendered.append(stream.getvalue())
        return rendered
    except Exception as exc:
        raise AIShadowError("Unable to render PDF pages for local vision extraction.") from exc


def _document_images(filename: str, content: bytes, max_pages: int) -> list[bytes]:
    lower = filename.casefold()
    if content.startswith(b"%PDF") or lower.endswith(".pdf"):
        return _render_pdf_pages(content, max_pages)
    if lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
        return [content]
    raise AIShadowError("Qwen/Ollama shadow v1 supports PDF and image documents only.")


class QwenOllamaProvider:
    name = PROVIDER_QWEN_OLLAMA

    def __init__(self, config: AIProviderConfig) -> None:
        self.config = config
        self.model = config.model

    def extract(self, *, filename: str, content: bytes) -> AIExtractionResult:
        images = _document_images(filename, content, self.config.max_pages)
        candidates: list[dict[str, object]] = []
        started = time.monotonic()
        for page_number, image in enumerate(images, start=1):
            payload = {
                "model": self.model,
                "stream": False,
                "format": _CANDIDATE_SCHEMA,
                "options": {"temperature": 0},
                "messages": [
                    {
                        "role": "user",
                        "content": _PROMPT + f"\nThis image is page {page_number}. Every candidate page must be {page_number}.",
                        "images": [base64.b64encode(image).decode("ascii")],
                    }
                ],
            }
            response = _post_json(url=self.config.base_url, payload=payload, timeout=self.config.timeout_seconds)
            message = response.get("message")
            if not isinstance(message, dict):
                raise AIShadowError("Ollama response is missing message content.")
            content_text = message.get("content")
            if not isinstance(content_text, str):
                raise AIShadowError("Ollama response content is invalid.")
            try:
                page_payload = json.loads(content_text)
            except json.JSONDecodeError as exc:
                raise AIShadowError("Ollama structured output is invalid JSON.") from exc
            if not isinstance(page_payload, dict) or not isinstance(page_payload.get("candidates"), list):
                raise AIShadowError("Ollama structured output is missing candidates.")
            for item in page_payload["candidates"]:
                if isinstance(item, dict):
                    item = dict(item)
                    item["page"] = page_number
                    candidates.append(item)
        elapsed = int((time.monotonic() - started) * 1000)
        return extraction_result_from_payload(
            payload={"candidates": candidates}, provider=self.name, model=self.model,
            page_count=len(images), latency_ms=elapsed,
        )


class MistralOcrProvider:
    name = PROVIDER_MISTRAL_OCR

    def __init__(self, config: AIProviderConfig) -> None:
        if not config.allow_external:
            raise AIShadowError("External AI provider is disabled by policy.")
        if not config.api_key:
            raise AIShadowError("Mistral OCR requires US_LACEY_AI_API_KEY.")
        self.config = config
        self.model = config.model

    def extract(self, *, filename: str, content: bytes) -> AIExtractionResult:
        mime = mimetypes.guess_type(filename)[0] or ("application/pdf" if content.startswith(b"%PDF") else "application/octet-stream")
        data_url = f"data:{mime};base64,{base64.b64encode(content).decode('ascii')}"
        payload = {
            "model": self.model,
            "document": {"type": "document_url", "document_url": data_url},
            "include_blocks": True,
            "confidence_scores_granularity": "block",
            "document_annotation_prompt": _PROMPT,
            "document_annotation_format": {
                "type": "json_schema",
                "json_schema": {"name": "lacey_ai_shadow_v1", "schema": _CANDIDATE_SCHEMA, "strict": True},
            },
        }
        # Document annotations are limited to eight pages. For PDFs, send only
        # valid 0-indexed pages so short documents never receive out-of-range
        # page requests and long documents remain fail-bounded in v1.
        if content.startswith(b"%PDF") or filename.casefold().endswith(".pdf"):
            try:
                from pypdf import PdfReader
                page_count = len(PdfReader(BytesIO(content)).pages)
            except Exception as exc:
                raise AIShadowError("Unable to inspect PDF pages for Mistral OCR.") from exc
            payload["pages"] = list(range(min(page_count, self.config.max_pages, 8)))
            if not payload["pages"]:
                raise AIShadowError("PDF contains no processable pages.")
        started = time.monotonic()
        response = _post_json(
            url=self.config.base_url, payload=payload, timeout=self.config.timeout_seconds,
            headers={"Authorization": f"Bearer {self.config.api_key}"},
        )
        annotation = response.get("document_annotation")
        if isinstance(annotation, str):
            try:
                annotation = json.loads(annotation)
            except json.JSONDecodeError as exc:
                raise AIShadowError("Mistral document annotation is invalid JSON.") from exc
        if not isinstance(annotation, dict):
            raise AIShadowError("Mistral OCR response is missing document_annotation.")
        elapsed = int((time.monotonic() - started) * 1000)
        pages = response.get("pages")
        page_count = len(pages) if isinstance(pages, list) else None
        return extraction_result_from_payload(
            payload=annotation, provider=self.name, model=self.model,
            page_count=page_count, latency_ms=elapsed,
        )


def build_ai_provider(config: AIProviderConfig | None = None):
    config = config or AIProviderConfig.from_env()
    if config.mode != AI_SHADOW_SHADOW:
        return None
    if config.provider == PROVIDER_MISTRAL_OCR:
        return MistralOcrProvider(config)
    return QwenOllamaProvider(config)
