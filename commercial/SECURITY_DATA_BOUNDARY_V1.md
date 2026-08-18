# Litoral Trace V1 — Security and Data Boundary

This document is a customer-facing product boundary for discovery, pilots and first-customer proposals. It is not a certification, legal opinion or substitute for a signed data-processing agreement where one is required.

## What Litoral Trace does

Litoral Trace centralizes tenant-scoped traceability records, lot/geolocation data, evidence documents, satellite-analysis workflows, batch imports and audit/evidence history so an organization can organize and retrieve evidence supporting its due-diligence process.

## Security controls implemented in the V1 architecture

- tenant-scoped PostgreSQL data model with Row Level Security on protected business tables
- separate runtime, worker and migration database responsibilities
- role-based access control
- persistent refresh sessions and session revocation/rotation controls
- HTTPS-oriented production ingress with authentication rate limiting and browser security headers
- private S3-compatible Vault storage contract with TLS verification and integrity metadata
- server-generated/tenant-scoped evidence handling rather than public business filenames
- audit trail for relevant security/business operations
- secure XLSX ingestion with explicit validation and atomic/idempotent database behavior
- CI release gates for Python, frontend, production image, PostgreSQL/PostGIS, Satellite, Vault/MinIO and Batch acceptance
- backup/recovery and disaster-recovery controls documented separately

## Data ownership and accuracy boundary

The customer remains responsible for the accuracy, completeness, authorization and legal basis of business data supplied to Litoral Trace, including supplier information, lot identifiers, coordinates/polygons, documents and declarations.

Litoral Trace can validate technical and structural properties and preserve evidence relationships, but it does not independently establish that customer-supplied factual statements are legally or factually correct.

## Satellite-analysis boundary

Satellite and geospatial outputs are analytical evidence. They must not be described as automatic legal proof of absence of deforestation, regulator approval, certification, or a guarantee that a transaction complies with EUDR or any other law.

## Compliance boundary

Litoral Trace is traceability/evidence infrastructure. The platform does not:

- act as a regulator or certification body
- issue an official EUDR certificate
- provide a guaranteed legal-compliance outcome
- replace customer, importer/operator/trader or professional-adviser responsibilities
- silently resolve contradictory or incomplete source data on the customer's behalf

## Operational environments

A controlled pilot may operate with reduced production commitments while the customer keeps original source records. Production use is accepted only after the applicable Litoral Trace V1 go-live and disaster-recovery gates are documented as PASS.

## Incident and support boundary

Operational incidents are handled through the documented support/escalation route. Security-sensitive information, credentials, database URLs, object-store credentials, tokens and private keys must never be sent in ordinary tickets, email bodies, chat messages or GitHub issues.