# U.S. Lacey Canonical Engineering Documentation

This directory is the current navigation/control plane for agents and engineers working on the U.S. Lacey product.

## Read order
1. `ARCHITECTURE.md` — bounded contexts and ownership
2. `CAPABILITIES.toml` — machine-readable capability map
3. `INVARIANTS.md` — safety/authority rules that must survive changes
4. `PIPELINE.md` — runtime data flow
5. `DATA_MODEL.md` — persistence/evidence model map
6. `TEST_MATRIX.md` — what to run when a capability changes
7. `ROADMAP.md` — current commercial/engineering order
8. `CLEANUP_CANDIDATES.md` — debt candidates; not permission to delete them

## Authority
Current code + migrations + executable tests are the ultimate implementation truth. These docs describe that implementation and the intended boundaries. When older plans, checklists, PR descriptions or archived design notes conflict with this directory, treat the material here as the current navigation source and verify against code/tests before changing behavior.

## Current product direction
The near-term objective is not to add more generic extraction features. It is to turn the existing document/evidence infrastructure into a source-linked, human-reviewable U.S. Lacey work-reduction product: structured product composition/BOM, taxonomy resolution, deterministic regulatory rules, exception management and review packages.

## What this control plane does not do
It does not reorganize runtime packages, change API/DB contracts, alter RLS, modify regulatory behavior, change source-set semantics, or remove historical files. Those changes require separate evidence-backed work.