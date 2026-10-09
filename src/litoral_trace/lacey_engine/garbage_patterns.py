"""Defense-in-depth rejection of labels mistakenly emitted as values."""
from __future__ import annotations
import re

_LABEL_ONLY = re.compile(
    r"^(?:seal number(?: \d+)?|container (?:length|height|width|type)|"
    r"equipment description(?: code)?|load status|address line \d+|city|"
    r"state province|zip code|country code|marks and numbers \d+)$", re.I
)


def is_label_garbage(value: str) -> bool:
    return bool(_LABEL_ONLY.fullmatch(" ".join(value.split())))
