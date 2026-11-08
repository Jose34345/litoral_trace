# DI-01 + DI-03 integration acceptance

Integration branch: `fix/di01-canonical-identity-after-di03`.
Original DI-01: `015412e7fc6ac73799dcf81854ef988180cfbd37`, original base `1a245702`.
Merged development base: `e5b25d6d811064414545d394e3879082fff1c55a` (DI-03).
Git merge is non-destructive, with both parents preserved and no conflicts.
The original DI-01 worktree, uncommitted test and private fixture stay intact.

## Scope and authority

No additional production algorithm was introduced. The canonical class guard is
byte-identical to DI-01; the parser and same-block source-conflict correction are
byte-identical to integrated DI-03. DI-02, cache, OCR, migrations, RLS configuration,
workers and experimental regulatory modes are unchanged against the updated base.

DI-01 validates complete discovered classes: when two source-local rows share a
document, the whole incompatible class abstains instead of choosing a partial
matching. It keeps every identity/evidence and flags review on lines and composed
shipment description. Real field conflicts remain conflicts and cannot be closed
automatically. Compatible classes continue to join. This is a specific transitive
class fix, not a claim that every possible weak identity association is solved.

## Reproducible comparison

The exact original audit test remains unedited outside Git. The current dedicated
DI-01 file has 18 parametrized cases; integrated DI-03 has 41 cases, including
multiple different consignees in a single block. The same 60 contracts were run:

| Source version | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Original base 1a245702 | 20 | 40 | 0 |
| DI-01 only 015412e7 | 36 | 24 | 0 |
| DI-03 only e5b25d6 (isolated source snapshot) | 44 | 16 | 0 |
| DI-01 + DI-03 | 60 | 0 | 0 |

For the original synthetic three-row case, the base publishes one incorrect
entity; the combination keeps three source-local entities, all review-required.
All three original document/line evidence pairs survive and the input payload is
unchanged. Compatible joins, repeated values, swapped order, independent classes,
real conflicts, ambiguous-description review and PostgreSQL publication are tested.
These are regression-case results, not global product accuracy statistics.

## Real PDF and preservation

The private fixture was read in place with `process_bundle()`, never copied into
this worktree or Git. Name, address and integral contracts all passed. No new name,
address or source-document literal was added to public tests/documentation.

Of the original 21 assertions, 19 remain AST-identical; address status/equality
moved to the DI-03 helper, with the documented literal-country reference correction
and exact legacy body retained. Seven missing structural assertions were recovered
from the uncommitted DI-01 test: multiple logical documents, type consistency,
nonempty blocks, block ranges, candidate membership and original block/text equality.
No old test file was copied over the integrated DI-03 version.

An independent experiment confirms exact physical/logical block equality in both
base and combination: 8 pages, logical ranges 1-3 / 4-8, 2737 blocks, 159 distinct
(page, table, row) identities and 89 TABLE_ROW blocks. The first temporary metric
mistakenly equated the latter two counts; its red log is retained. Correct counts
are identical in both versions and no source block disappeared. Heuristic document
classification is not promoted to verified document type. Missing plant quantity,
HTS and harvest country remain missing in this PDF.

## Final local validation

| Gate | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Combined original/DI-01/DI-03 contracts | 60 | 0 | 0 |
| Engine, cross-document identity, golden/adversarial, benchmarks, lifecycle | 483 | 0 | 3 |
| Full general CI equivalent | 2761 | 0 | 366 |
| PostgreSQL canonical, CAS/source sets, stale generations, persistence, dossier, RLS | 64 | 0 | 0 |
| PostgreSQL worker/E2E/Pack1/Pack2/plywood | 10 | 0 | 0 |
| Physical/logical conservation experiment, base and combined | 2 | 0 | 0 |

The three focused PostgreSQL golden skips ran in the PostgreSQL group. General
skips are existing conditional integrations, including the DI-01 PostgreSQL test
which ran explicitly above. Counts overlap; do not add them as independent samples.
Pack2 net-material mass remains 288 kg under its positive, ambiguity, gross/net,
line/total and SKU-binding contracts; Pack1 botanical gaps remain review-required.
Python syntax (main.py/src/tests), tracked TOML, single Alembic head 079 and
`git diff --check` pass. General suite reports five existing warnings.

Python 3.11.9 and the existing environment were reused. PostgreSQL 17.11/PostGIS
3.6 used two new synthetic databases in the verified local temporary cluster;
CI uses PostGIS 3.5. No production DB, external AI call, deployment or PR merge was
performed. Local logs/commands/JUnit, fixture hashes and backups stay outside Git.
A first measurement harness assumed a DI-01-only dataclass attribute existed in
base; its diagnostic was corrected without changing any production code.

## Independent review and disposition

An independent read-only code reviewer found no Critical/Important/Minor issue.
It confirmed class compatibility, review propagation, source preservation, DI-03
retention and the recovered original PDF invariants. Runtime/DB results above were
executed by the coordinator, not inferred from static review.

GO for the exclusive DI-01 PR, subject to final allowlist/hash checks. Merge remains
pending remote CI/PostgreSQL gates and human approval. The conservative class-wide
abstention may retain more manual identity review; it does not invent a matching or
repair historical publications. Rollback is a normal revert of the DI-01 change;
no schema rollback is needed. Do not merge or deploy during this execution.
