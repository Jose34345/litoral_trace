# Specialized inference cache implementation plan

## Goal
Avoid repeating expensive specialized AI inference when a new operation contains the same document bytes under the same computational contract, while keeping operation/review/audit state independent.

## Regression first
1. Add a deterministic cache-identity regression showing operation IDs and operation-document IDs do not affect computational identity.
2. Show document hashes/roles plus engine/model/schema/ruleset configuration do affect identity.
3. Add a service regression showing an eligible cache hit bypasses the specialized provider execution path.
4. Show a model/schema/version change produces a cache miss.

## Implementation
1. Introduce a content-addressed specialized computation fingerprint based on tenant, ordered source hashes/roles/routing inputs and versioned AI contract inputs; exclude operation/admin IDs.
2. Persist enough versioned specialized result data to safely replay computational output.
3. Look up the complete cached source set before calling the specialized provider.
4. On a hit, rebind immutable evidence to the current operation/source documents and run only the current operation's projection/audit materialization.
5. Never reuse human decisions, field review state, operation status, or operation IDs.

## Verification
Run focused cache tests, specialized shadow/projection regressions, Engine2 service tests and full repository CI. On production, repeat the identical seven-document package and verify zero external specialized inference calls on the second operation.
