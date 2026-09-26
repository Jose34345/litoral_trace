"""Static routing contracts for customer-facing FastAPI applications.

Starlette resolves routes in registration order.  A path such as
``/review/{field_id}`` therefore matches the literal ``/review/accept-supported``
before Pydantic gets a chance to reject the non-integer value.  These helpers
make that class of production bug a CI-enforced contract.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from collections.abc import Iterable


_PARAM = re.compile(r"^\{(?P<name>[^}:]+)(?::(?P<converter>[^}]+))?\}$")


@dataclass(frozen=True, slots=True)
class RouteShadow:
    method: str
    earlier_path: str
    later_path: str
    segment_index: int


def _methods(route) -> frozenset[str]:
    return frozenset(
        str(method).upper()
        for method in (getattr(route, "methods", None) or ())
        if str(method).upper() not in {"HEAD", "OPTIONS"}
    )


def _segments(path: str) -> tuple[str, ...]:
    return tuple(segment for segment in str(path).strip("/").split("/") if segment)


def _unconstrained_parameter(segment: str) -> bool:
    match = _PARAM.fullmatch(segment)
    if match is None:
        return False
    converter = (match.group("converter") or "str").lower()
    # Starlette's default/string converter accepts arbitrary literal text.  Typed
    # converters such as int/float/uuid cannot shadow an unrelated action slug.
    return converter in {"str", "path"}


def find_dynamic_literal_route_shadows(routes: Iterable[object]) -> tuple[RouteShadow, ...]:
    """Return ordered-route cases where a dynamic path can steal a later literal.

    This intentionally focuses on the dangerous pattern that caused the U.S.
    Lacey bulk-review 422: an earlier unconstrained parameter in the same path
    position as a later literal, with at least one overlapping HTTP method.
    """
    route_list = [route for route in routes if getattr(route, "path", None)]
    shadows: list[RouteShadow] = []
    for earlier_index, earlier in enumerate(route_list):
        earlier_segments = _segments(earlier.path)
        earlier_methods = _methods(earlier)
        if not earlier_methods:
            continue
        for later in route_list[earlier_index + 1 :]:
            later_segments = _segments(later.path)
            if len(earlier_segments) != len(later_segments):
                continue
            methods = earlier_methods & _methods(later)
            if not methods:
                continue

            shadow_segment: int | None = None
            compatible = True
            for index, (left, right) in enumerate(zip(earlier_segments, later_segments)):
                if left == right:
                    continue
                left_param = _PARAM.fullmatch(left)
                right_param = _PARAM.fullmatch(right)
                if left_param is None:
                    compatible = False
                    break
                if right_param is None and _unconstrained_parameter(left):
                    shadow_segment = index
                    continue
                # Parameter-vs-parameter and typed-parameter-vs-literal are not
                # this contract's literal-shadow case.
                compatible = False
                break

            if compatible and shadow_segment is not None:
                for method in sorted(methods):
                    shadows.append(
                        RouteShadow(
                            method=method,
                            earlier_path=str(earlier.path),
                            later_path=str(later.path),
                            segment_index=shadow_segment,
                        )
                    )
    return tuple(shadows)
