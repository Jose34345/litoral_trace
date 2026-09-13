"""Multi-extractor orchestration primitives for the Lacey engine."""

from .contracts import (
    AgentTask,
    CandidateEnvelope,
    DocumentType,
    MultiAgentExtractionResult,
    RoutedDocument,
    SpecialistFailure,
    SpecialistResult,
    SpecialistRole,
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
    "RoutedDocument",
    "RoutingAssignment",
    "RoutingClassification",
    "RoutingPlan",
    "SpecialistFailure",
    "SpecialistResult",
    "SpecialistRole",
    "build_routing_plan",
    "classify_page",
    "route_document",
    "router_document_accuracy",
]
