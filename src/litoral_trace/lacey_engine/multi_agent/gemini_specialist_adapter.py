"""Scoped Gemini adapter for specialized extraction.

This module deliberately reuses the existing shared HTTP transport, image rendering,
provider configuration, candidate schema, Gemini response parser, and AI-shadow payload
conversion.  It does not change the legacy ``GeminiInteractionsProvider`` path.
"""
from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import replace
import json
import time

from ..ai_providers import (
    AIProviderConfig,
    _CANDIDATE_SCHEMA,
    _PROMPT,
    _document_images,
    _post_json,
)
from ..ai_shadow import AIExtractionResult, AIShadowError, extraction_result_from_payload
from ..gemini_provider import gemini_output_text, gemini_usage


def _sum_reported(values: list[int | None]) -> int | None:
    reported = [value for value in values if value is not None]
    return sum(reported) if reported else None


class GeminiSpecialistProvider:
    """Gemini structured extraction with a closed field set per specialist call."""

    name = "gemini"

    def __init__(self, config: AIProviderConfig) -> None:
        if not config.allow_external:
            raise AIShadowError("External AI provider is disabled by policy.")
        if not config.api_key:
            raise AIShadowError("Gemini requires US_LACEY_GEMINI_API_KEY or US_LACEY_AI_API_KEY.")
        self.config = config
        self.model = config.model

    def extract_scoped(
        self,
        *,
        filename: str,
        content: bytes,
        pages: tuple[int, ...],
        allowed_fields: frozenset[str],
        prompt: str,
    ) -> AIExtractionResult:
        if not pages:
            return AIExtractionResult(
                provider=self.name,
                model=self.model,
                schema_version="lacey_ai_shadow_v1",
                candidates=(),
                page_count=0,
                latency_ms=0,
            )
        if min(pages) < 1:
            raise AIShadowError("Specialist pages must be 1-indexed.")
        if max(pages) > self.config.max_pages:
            raise AIShadowError(
                f"Specialist page {max(pages)} exceeds configured max_pages={self.config.max_pages}."
            )

        all_images = _document_images(filename, content, max(pages))
        schema = _scoped_schema(allowed_fields)
        candidates: list[dict[str, object]] = []
        input_tokens: list[int | None] = []
        output_tokens: list[int | None] = []
        total_tokens: list[int | None] = []
        started = time.monotonic()

        for page_number in pages:
            try:
                image = all_images[page_number - 1]
            except IndexError as exc:
                raise AIShadowError(
                    f"Specialist requested page {page_number}, but the rendered document is shorter."
                ) from exc

            payload: dict[str, object] = {
                "model": self.model,
                "store": False,
                "input": [
                    {
                        "type": "text",
                        "text": (
                            _PROMPT
                            + "\n\nSPECIALIST DOMAIN:\n"
                            + prompt.strip()
                            + "\n\nCLOSED FIELD CONTRACT:\n"
                            + ", ".join(sorted(allowed_fields))
                            + "\nReturn no field outside that list. "
                            + f"This image is page {page_number}; every candidate page must be {page_number}."
                        ),
                    },
                    {
                        "type": "image",
                        "mime_type": "image/png",
                        "data": base64.b64encode(image).decode("ascii"),
                    },
                ],
                "generation_config": {"thinking_level": "low"},
                "response_format": {
                    "type": "text",
                    "mime_type": "application/json",
                    "schema": schema,
                },
            }
            response = _post_json(
                url=self.config.base_url,
                payload=payload,
                timeout=self.config.timeout_seconds,
                headers={"x-goog-api-key": self.config.api_key},
            )
            page_input, page_output, page_total = gemini_usage(response)
            input_tokens.append(page_input)
            output_tokens.append(page_output)
            total_tokens.append(page_total)
            try:
                page_payload = json.loads(gemini_output_text(response))
            except json.JSONDecodeError as exc:
                raise AIShadowError("Gemini specialist structured output is invalid JSON.") from exc
            if not isinstance(page_payload, dict) or not isinstance(
                page_payload.get("candidates"), list
            ):
                raise AIShadowError("Gemini specialist output is missing candidates.")
            for item in page_payload["candidates"]:
                if isinstance(item, dict):
                    candidate = dict(item)
                    candidate["page"] = page_number
                    candidates.append(candidate)

        elapsed = int((time.monotonic() - started) * 1000)
        result = extraction_result_from_payload(
            payload={"candidates": candidates},
            provider=self.name,
            model=self.model,
            page_count=len(pages),
            latency_ms=elapsed,
        )
        return replace(
            result,
            input_tokens=_sum_reported(input_tokens),
            output_tokens=_sum_reported(output_tokens),
            total_tokens=_sum_reported(total_tokens),
        )


def _scoped_schema(allowed_fields: frozenset[str]) -> dict[str, object]:
    if not allowed_fields:
        raise AIShadowError("Specialist allowed_fields cannot be empty.")
    schema = deepcopy(_CANDIDATE_SCHEMA)
    item_properties = schema["properties"]["candidates"]["items"]["properties"]
    item_properties["field_key"]["enum"] = sorted(allowed_fields)
    schema["properties"]["candidates"]["description"] = (
        "Evidence-backed occurrences for this specialist only. Repeated field keys are expected "
        "for line-item tables; never merge different rows into one value."
    )
    item_properties["source_text"]["description"] = (
        "Exact supporting text. For commercial tables include the complete source row when "
        "possible, including SKU or line number and quantity, so deterministic line binding can "
        "run later without asking the model to invent identity."
    )
    return schema
