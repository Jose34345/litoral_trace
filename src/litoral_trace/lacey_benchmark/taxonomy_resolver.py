from __future__ import annotations

from difflib import SequenceMatcher

from litoral_trace.lacey_benchmark.special_use import SpecialUseRegistry
from litoral_trace.lacey_benchmark.taxonomy_contracts import (
    TaxonCandidate,
    TaxonResolution,
    TaxonResolutionStatus,
)
from litoral_trace.lacey_benchmark.taxonomy_snapshot import (
    TaxonomySnapshot,
    normalize_taxon_text,
)


class TaxonomyResolver:
    """Deterministic exact resolver plus review-only candidate generation.

    Critical safety contract: aliases and fuzzy candidates can assist review but do
    not silently become authoritative accepted taxa.
    """

    def __init__(
        self,
        snapshot: TaxonomySnapshot,
        special_use: SpecialUseRegistry | None = None,
        *,
        candidate_review_threshold: float = 0.85,
        max_candidates: int = 5,
    ) -> None:
        if not 0.0 <= candidate_review_threshold <= 1.0:
            raise ValueError("candidate_review_threshold must be between 0 and 1")
        if max_candidates < 1:
            raise ValueError("max_candidates must be positive")
        self.snapshot = snapshot
        self.special_use = special_use
        self.candidate_review_threshold = candidate_review_threshold
        self.max_candidates = max_candidates

    def resolve(
        self,
        *,
        scientific_name: str | None = None,
        genus: str | None = None,
        species: str | None = None,
    ) -> TaxonResolution:
        clean_name = self._clean(scientific_name)
        clean_genus = self._clean(genus)
        clean_species = self._clean(species)

        if self.special_use is not None and clean_genus and clean_species:
            designation = self.special_use.resolve(clean_genus, clean_species)
            if designation is not None:
                return TaxonResolution(
                    status=TaxonResolutionStatus.SPECIAL_USE,
                    review_required=True,
                    genus=designation.genus,
                    species=designation.species,
                    matched_value=f"{designation.genus}/{designation.species}",
                )

        if clean_name:
            exact = self._resolve_exact_name(clean_name)
            if exact is not None:
                return exact

        if clean_genus and clean_species:
            # Some source documents place the full binomial in the species cell.
            species_parts = clean_species.split()
            if len(species_parts) >= 2 and normalize_taxon_text(species_parts[0]) == normalize_taxon_text(clean_genus):
                full_name_result = self._resolve_exact_name(clean_species)
                if full_name_result is not None:
                    return TaxonResolution(
                        status=TaxonResolutionStatus.GENUS_SPECIES,
                        canonical_source_id=full_name_result.canonical_source_id,
                        canonical_scientific_name=full_name_result.canonical_scientific_name,
                        genus=full_name_result.genus,
                        species=full_name_result.species,
                        review_required=False,
                        matched_value=f"{clean_genus} + {clean_species}",
                    )
            pair_key = (normalize_taxon_text(clean_genus), normalize_taxon_text(clean_species))
            canonical_id = self.snapshot.pair_index.get(pair_key)
            if canonical_id:
                return self._authoritative(
                    canonical_id,
                    TaxonResolutionStatus.GENUS_SPECIES,
                    matched_value=f"{clean_genus} + {clean_species}",
                )

        if clean_species and not clean_genus and not clean_name:
            ids = self.snapshot.epithet_index.get(normalize_taxon_text(clean_species), ())
            candidates = tuple(self._candidate(source_id, 1.0, "species_epithet") for source_id in ids)
            return TaxonResolution(
                status=TaxonResolutionStatus.AMBIGUOUS,
                canonical_source_id=None,
                review_required=True,
                matched_value=clean_species,
                candidates=candidates,
            )

        query = clean_name or (
            f"{clean_genus} {clean_species}" if clean_genus and clean_species else None
        )
        if query:
            candidates = self._candidate_matches(query)
            if candidates:
                return TaxonResolution(
                    status=(
                        TaxonResolutionStatus.CANDIDATE
                        if len(candidates) == 1
                        else TaxonResolutionStatus.AMBIGUOUS
                    ),
                    canonical_source_id=None,
                    review_required=True,
                    matched_value=query,
                    candidates=candidates,
                )

        return TaxonResolution(
            status=TaxonResolutionStatus.NOT_FOUND,
            canonical_source_id=None,
            review_required=True,
            matched_value=query,
        )

    def _resolve_exact_name(self, value: str) -> TaxonResolution | None:
        key = normalize_taxon_text(value)
        canonical_id = self.snapshot.accepted_name_index.get(key)
        if canonical_id:
            return self._authoritative(
                canonical_id,
                TaxonResolutionStatus.EXACT_ACCEPTED,
                matched_value=value,
            )
        canonical_id = self.snapshot.synonym_name_index.get(key)
        if canonical_id:
            return self._authoritative(
                canonical_id,
                TaxonResolutionStatus.EXACT_SYNONYM,
                matched_value=value,
            )
        alias_ids = self.snapshot.alias_index.get(key, ())
        if len(alias_ids) == 1:
            record = self.snapshot.canonical_record(alias_ids[0])
            return TaxonResolution(
                status=TaxonResolutionStatus.EXACT_ALIAS,
                canonical_source_id=record.source_id,
                canonical_scientific_name=record.scientific_name,
                genus=record.genus,
                species=record.species,
                review_required=True,
                matched_value=value,
                candidates=(self._candidate(record.source_id, 1.0, "alias"),),
            )
        if len(alias_ids) > 1:
            return TaxonResolution(
                status=TaxonResolutionStatus.AMBIGUOUS,
                canonical_source_id=None,
                review_required=True,
                matched_value=value,
                candidates=tuple(
                    self._candidate(source_id, 1.0, "alias") for source_id in alias_ids
                ),
            )
        return None

    def _authoritative(
        self,
        canonical_id: str,
        status: TaxonResolutionStatus,
        *,
        matched_value: str,
    ) -> TaxonResolution:
        record = self.snapshot.canonical_record(canonical_id)
        return TaxonResolution(
            status=status,
            canonical_source_id=record.source_id,
            canonical_scientific_name=record.scientific_name,
            genus=record.genus,
            species=record.species,
            review_required=False,
            matched_value=matched_value,
        )

    def _candidate_matches(self, query: str) -> tuple[TaxonCandidate, ...]:
        query_key = normalize_taxon_text(query)
        best_by_taxon: dict[str, TaxonCandidate] = {}
        for term, canonical_id, provenance in self.snapshot.candidate_terms():
            score = SequenceMatcher(None, query_key, term).ratio()
            if score < self.candidate_review_threshold or score >= 1.0:
                continue
            candidate = self._candidate(canonical_id, score, provenance)
            previous = best_by_taxon.get(canonical_id)
            if previous is None or candidate.score > previous.score:
                best_by_taxon[canonical_id] = candidate
        ordered = sorted(
            best_by_taxon.values(),
            key=lambda item: (-item.score, item.scientific_name.casefold(), item.canonical_source_id),
        )
        # Candidate matching is review assistance only. A clearly best result is
        # represented as one CANDIDATE; near-ties remain AMBIGUOUS.
        if len(ordered) > 1 and ordered[0].score - ordered[1].score >= 0.08:
            ordered = ordered[:1]
        return tuple(ordered[: self.max_candidates])

    def _candidate(self, canonical_id: str, score: float, matched_on: str) -> TaxonCandidate:
        record = self.snapshot.canonical_record(canonical_id)
        return TaxonCandidate(
            canonical_source_id=record.source_id,
            scientific_name=record.scientific_name,
            score=score,
            matched_on=matched_on,
        )

    @staticmethod
    def _clean(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = " ".join(value.split())
        return cleaned or None
