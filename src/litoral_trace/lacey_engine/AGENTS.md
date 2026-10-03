# Lacey Document Engine Agent Guide

This package owns document understanding and candidate production. Its job is to turn source documents into evidence-backed observations/candidates, not to own customer accounts, billing, final workflow state, or canonical regulatory approval.

## Core responsibilities
- Admission/classification: `admission.py`, `classifier.py`
- Layout/segmentation: `layout_parser.py`, `segmentation.py`
- Domain contracts/serialization: `domain.py`, `serialization.py`, `shipment.py`
- Source authority/ranking: `source_authority.py`, `ranking.py`
- AI provider/routing/shadow infrastructure: `ai_providers.py`, `ai_routing.py`, `ai_shadow.py`, `gemini_provider.py`
- Semantic evidence representation: `semantic_graph.py`
- Multi-agent orchestration: `multi_agent/`

## Multi-agent ownership
- Routing: `multi_agent/router.py`
- Contracts: `multi_agent/contracts.py`
- Authority: `multi_agent/authority.py`
- SKU/line binding: `multi_agent/line_binding.py`
- Fusion/resolution/judging: `multi_agent/fusion.py`, `resolver.py`, `field_judge.py`
- Runtime/adapters: `multi_agent/specialist_runtime.py`, `gemini_specialist_adapter.py`
- Specialists: `multi_agent/specialists/botanical.py`, `commercial.py`, `customs.py`, `logistics.py`

## Authority rule
An extraction result, AI result, specialist result, or fused candidate is still not automatically canonical U.S. Lacey truth. Final application-level reconciliation/publication belongs to `src/litoral_trace/us_lacey/`.

## Guardrails
- Preserve evidence anchors and source authority.
- Preserve fail-closed behavior for ambiguous cross-document identities.
- Do not silently invent species, country, quantity, HTS, or regulatory status.
- Do not make billing/auth/customer-state decisions here.
- Do not duplicate source-set lifecycle or human-review state here.
- New document-specialist behavior must have focused tests under `tests/lacey_engine/`.

## Current commercial direction
Future BOM/product-composition parsing may consume this engine, but reusable product structure should not be hidden inside an AI specialist. Future taxonomy/regulatory decision logic should remain explicit, versionable and auditable rather than being embedded solely in model prompts.

Read root `AGENTS.md` and `docs/us-lacey/` canonical docs before changes.