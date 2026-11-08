"""Runtime selection for U.S. Lacey AI extraction architecture."""
from __future__ import annotations

from enum import Enum
import logging
import os
from typing import Mapping


LOGGER = logging.getLogger(__name__)
AI_ARCHITECTURE_ENV = "LT_AI_ARCHITECTURE"


class AIArchitecture(str, Enum):
    """Non-authoritative AI extractor execution modes."""

    LEGACY = "legacy"
    SPECIALIZED = "specialized"
    SHADOW = "shadow"


def ai_architecture(environ: Mapping[str, str] | None = None) -> AIArchitecture:
    """Return a safe extractor architecture; unknown values fall back to legacy."""
    env = os.environ if environ is None else environ
    raw = str(env.get(AI_ARCHITECTURE_ENV, AIArchitecture.LEGACY.value)).strip().lower()
    try:
        return AIArchitecture(raw)
    except ValueError:
        LOGGER.warning(
            "Invalid LT_AI_ARCHITECTURE value; falling back to legacy.",
            extra={"lt_ai_architecture": raw},
        )
        return AIArchitecture.LEGACY
