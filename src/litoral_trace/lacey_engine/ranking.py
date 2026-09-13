from __future__ import annotations
from .domain import AdmittedCandidate, FieldStatus, RawCandidate, ResolvedField
from .source_authority import authority


def resolve(field_key: str, raw_candidates: list[RawCandidate], make_candidate) -> ResolvedField:
    candidates = sorted((make_candidate(raw, authority(field_key, make_candidate.document_type_for(raw))) for raw in raw_candidates), key=lambda item: item.score, reverse=True)
    if not candidates:
        return ResolvedField(field_key, FieldStatus.MISSING, None, None)
    distinct = {candidate.raw.normalized_value for candidate in candidates}
    winner = candidates[0]
    if len(distinct) > 1 and len(candidates) > 1 and candidates[1].score >= winner.score - 5:
        return ResolvedField(field_key, FieldStatus.CONFLICT, None, None, tuple(candidates))
    return ResolvedField(field_key, FieldStatus.MATCHED, winner.raw.normalized_value, winner, tuple(candidates))
