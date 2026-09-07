"""Gemini Interactions adapter for the U.S. Lacey AI shadow pipeline.

The adapter is deliberately stateless (``store=false``) and remains non-authoritative.
Every extracted candidate is still passed through the existing exact-source evidence
verification and human-review boundary before it can influence the preparation record.
"""
from __future__ import annotations

import base64
import json
import time
from typing import Mapping

from .ai_providers import (
    AIProviderConfig,
    _CANDIDATE_SCHEMA,
    _PROMPT,
    _document_images,
    _post_json,
)
from .ai_shadow import AIExtractionResult, AIShadowError, extraction_result_from_payload


PROVIDER_GEMINI = "gemini"
GEMINI_INTERACTIONS_URL = "https://generativelanguage.googleapis.com/v1beta/interactions"


def gemini_output_text(response: Mapping[str, object]) -> str:
    """Return model text from a Gemini Interactions API response."""
    direct = response.get("output_text")
    if isinstance(direct, str) and direct:
        return direct
    steps = response.get("steps")
    if not isinstance(steps, list):
        raise AIShadowError("Gemini response is missing interaction steps.")
    for step in steps:
        if not isinstance(step, Mapping) or step.get("type") != "model_output":
            continue
        content = step.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if (
                isinstance(part, Mapping)
                and part.get("type") == "text"
                and isinstance(part.get("text"), str)
            ):
                return str(part["text"])
    raise AIShadowError("Gemini response contains no structured output text.")


class GeminiInteractionsProvider:
    """Gemini multimodal extraction through the stateless Interactions API."""

    name = PROVIDER_GEMINI

    def __init__(self, config: AIProviderConfig) -> None:
        if not config.allow_external:
            raise AIShadowError("External AI provider is disabled by policy.")
        if not config.api_key:
            raise AIShadowError("Gemini requires US_LACEY_GEMINI_API_KEY or US_LACEY_AI_API_KEY.")
        self.config = config
        self.model = config.model

    def extract(self, *, filename: str, content: bytes) -> AIExtractionResult:
        images = _document_images(filename, content, self.config.max_pages)
        candidates: list[dict[str, object]] = []
        started = time.monotonic()
        for page_number, image in enumerate(images, start=1):
            payload: dict[str, object] = {
                "model": self.model,
                # Shipping documents can contain confidential commercial data. Keep
                # Interactions stateless so server-side conversation state is not used.
                "store": False,
                "input": [
                    {
                        "type": "text",
                        "text": (
                            _PROMPT
                            + f"\nThis image is page {page_number}. "
                            f"Every candidate page must be {page_number}."
                        ),
                    },
                    {
                        "type": "image",
                        "mime_type": "image/png",
                        "data": base64.b64encode(image).decode("ascii"),
                    },
                ],
                # Low thinking is supported by current Gemini 3 models and is enough
                # for page extraction; expensive reasoning is reserved for review.
                "generation_config": {"thinking_level": "low"},
                "response_format": {
                    "type": "text",
                    "mime_type": "application/json",
                    "schema": _CANDIDATE_SCHEMA,
                },
            }
            response = _post_json(
                url=self.config.base_url,
                payload=payload,
                timeout=self.config.timeout_seconds,
                headers={"x-goog-api-key": self.config.api_key},
            )
            try:
                page_payload = json.loads(gemini_output_text(response))
            except json.JSONDecodeError as exc:
                raise AIShadowError("Gemini structured output is invalid JSON.") from exc
            if not isinstance(page_payload, dict) or not isinstance(
                page_payload.get("candidates"), list
            ):
                raise AIShadowError("Gemini structured output is missing candidates.")
            for item in page_payload["candidates"]:
                if isinstance(item, dict):
                    candidate = dict(item)
                    # Bind provenance to the page actually sent, regardless of model text.
                    candidate["page"] = page_number
                    candidates.append(candidate)
        elapsed = int((time.monotonic() - started) * 1000)
        return extraction_result_from_payload(
            payload={"candidates": candidates},
            provider=self.name,
            model=self.model,
            page_count=len(images),
            latency_ms=elapsed,
        )
