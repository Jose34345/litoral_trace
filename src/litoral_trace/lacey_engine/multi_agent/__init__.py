"""Multi-extractor orchestration primitives for the Lacey engine."""

from .contracts import (
    AgentTask,
    CandidateEnvelope,
    DocumentType,
    MultiAgentExtractionResult,
    OperationStatus,
    RoutedDocument,
    SpecialistExecutionState,
    SpecialistExecutionStatus,
    SpecialistFailure,
    SpecialistResult,
    SpecialistRole,
)
from .orchestrator import (
    orchestrate_specialists,
    run_specialist,
    specialist_concurrency_limit,
)
from .router import (
    AmbiguousDocumentClassifier,
    RoutingAssignment,
    RoutingClassification,
    RoutingPlan,
    build_routing_plan,
    classify_page,
    route_document,
    router_document_accuracy,
)

__all__ = [
    "AgentTask",
    "AmbiguousDocumentClassifier",
    "CandidateEnvelope",
    "DocumentType",
    "MultiAgentExtractionResult",
    "OperationStatus",
    "RoutedDocument",
    "RoutingAssignment",
    "RoutingClassification",
    "RoutingPlan",
    "SpecialistExecutionState",
    "SpecialistExecutionStatus",
    "SpecialistFailure",
    "SpecialistResult",
    "SpecialistRole",
    "build_routing_plan",
    "classify_page",
    "orchestrate_specialists",
    "route_document",
    "router_document_accuracy",
    "run_specialist",
    "specialist_concurrency_limit",
]
