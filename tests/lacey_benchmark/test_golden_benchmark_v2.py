from __future__ import annotations

from pathlib import Path

from litoral_trace.lacey_benchmark.contracts import (
    BenchmarkTelemetry,
    DocumentPrediction,
    FieldDecisionStatus,
    FieldObservation,
)
from litoral_trace.lacey_benchmark.corpus import (
    load_field_truth,
    load_router_corpus_manifest,
    materialize_router_corpus,
)
from litoral_trace.lacey_benchmark.evaluator import GoldenBenchmarkEvaluator
from litoral_trace.lacey_engine.multi_agent.router import classify_page


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "benchmarks/lacey/v1/router_corpus_manifest.json"
FIELD_TRUTH = ROOT / "benchmarks/lacey/v1/field_truth.json"


def _observation_from_truth(field, *, status=FieldDecisionStatus.SAFE_SUGGESTION):
    return FieldObservation(
        field_key=field.field_key,
        value=field.value,
        status=status,
        document_id=field.document_id,
        page=field.page,
        line_item_key=field.line_item_key,
        evidence_text=field.evidence_text,
        evidence_verified=True,
    )


def test_versioned_router_corpus_materializes_105_documents_with_required_coverage():
    manifest = load_router_corpus_manifest(MANIFEST)
    documents = materialize_router_corpus(manifest)

    assert manifest.version == "lacey-golden-v1"
    assert manifest.document_count == 105
    assert len(documents) == 105
    assert len({item.document_id for item in documents}) == 105
    assert {item.language for item in documents} == {"en", "es", "pt"}
    assert {item.modality for item in documents} == {"digital_text", "ocr_simulated"}
    assert {item.expected_type.value for item in documents} == {
        "COMMERCIAL_INVOICE",
        "PACKING_LIST",
        "BILL_OF_LADING",
        "ENTRY_WORKSHEET",
        "BOTANICAL_DECLARATION",
        "SUPPLIER_ORIGIN",
        "ARRIVAL_NOTICE",
    }


def test_current_router_scores_all_105_versioned_documents():
    manifest = load_router_corpus_manifest(MANIFEST)
    documents = materialize_router_corpus(manifest)
    predictions = tuple(
        DocumentPrediction(
            document_id=item.document_id,
            predicted_type=classify_page(
                item.pages[1],
                filename=item.filename,
            ).document_type,
        )
        for item in documents
    )

    score = GoldenBenchmarkEvaluator.evaluate_document_classification(
        documents,
        predictions,
    )

    assert score.total == 105
    assert score.correct == 105
    assert score.accuracy == 1.0


def test_field_truth_is_source_linked_and_line_scoped_where_required():
    truth = load_field_truth(FIELD_TRUTH)

    assert truth.version == "lacey-golden-v1"
    assert len(truth.fields) >= 20
    for field in truth.fields:
        assert field.document_id
        assert field.page >= 1
        assert field.evidence_text.strip()
        if field.field_key in {
            "description",
            "hts_code",
            "entered_value",
            "genus",
            "species",
            "country_of_harvest",
            "plant_quantity",
            "metric_unit",
        }:
            assert field.line_item_key is not None


def test_scorecard_detects_false_safe_wrong_line_and_review_friction():
    truth = load_field_truth(FIELD_TRUTH)
    fields = truth.fields[:4]

    observations = [
        _observation_from_truth(fields[0]),
        _observation_from_truth(fields[1], status=FieldDecisionStatus.REVIEW_REQUIRED),
        FieldObservation(
            field_key=fields[2].field_key,
            value="WRONG-VALUE",
            status=FieldDecisionStatus.SAFE_SUGGESTION,
            document_id=fields[2].document_id,
            page=fields[2].page,
            line_item_key=fields[2].line_item_key,
            evidence_text=fields[2].evidence_text,
            evidence_verified=True,
        ),
        FieldObservation(
            field_key=fields[3].field_key,
            value=fields[3].value,
            status=FieldDecisionStatus.SAFE_SUGGESTION,
            document_id=fields[3].document_id,
            page=fields[3].page,
            line_item_key="SKU:WRONG-LINE",
            evidence_text=fields[3].evidence_text,
            evidence_verified=True,
        ),
    ]

    score = GoldenBenchmarkEvaluator.evaluate_fields(
        expected=fields,
        observed=observations,
        telemetry=(
            BenchmarkTelemetry(latency_ms=1000, cost_usd=0.02),
            BenchmarkTelemetry(latency_ms=2000, cost_usd=0.03),
            BenchmarkTelemetry(latency_ms=4000, cost_usd=0.05),
        ),
    )

    assert score.false_safe_count == 2
    assert score.false_safe_rate == 0.5
    assert score.human_review_rate == 0.25
    assert score.line_binding_accuracy < 1.0
    assert score.safe_gate_passed is False
    assert score.latency_p50_ms == 2000
    assert score.latency_p95_ms == 4000
    assert score.total_cost_usd == 0.10


def test_false_safe_zero_is_the_hard_safety_gate():
    truth = load_field_truth(FIELD_TRUTH)
    observations = tuple(_observation_from_truth(field) for field in truth.fields)

    score = GoldenBenchmarkEvaluator.evaluate_fields(
        expected=truth.fields,
        observed=observations,
    )

    assert score.field_precision == 1.0
    assert score.field_recall == 1.0
    assert score.line_binding_accuracy == 1.0
    assert score.evidence_verification_accuracy == 1.0
    assert score.false_safe_count == 0
    assert score.false_safe_rate == 0.0
    assert score.human_review_rate == 0.0
    assert score.safe_gate_passed is True
