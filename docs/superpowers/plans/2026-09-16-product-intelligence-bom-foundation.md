# Product Intelligence / BOM Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable, deterministic Product Intelligence foundation that converts explicit CSV/XLSX BOM tables into source-backed `Product/SKU -> Component -> Material` composition without changing current U.S. Lacey runtime behavior.

**Architecture:** Reuse `src/litoral_trace/assurance/parsers.py` as the single CSV/XLSX parsing authority. Add a pure `src/litoral_trace/product_intelligence/` package for domain contracts, unit normalization, BOM header binding and row transformation. Keep persistence, taxonomy, regulatory decisions and canonical U.S. Lacey publication out of scope.

**Tech Stack:** Python 3.11, stdlib dataclasses/enums/Decimal, existing Assurance `ParsedDocument`/`ParsedTable`, pytest, existing CI/GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-16-product-intelligence-bom-foundation-design.md`

## Global Constraints
- Do not modify current U.S. Lacey canonical truth, source-set lifecycle, workers, review, exports, auth, billing, RLS or migrations.
- Do not add external dependencies.
- Reuse the existing Assurance CSV/XLSX parsers; do not introduce a second file parser.
- Use `Decimal` for quantities and mass calculations.
- Unsupported or ambiguous input must produce explicit issues or fail closed; never infer missing business facts.
- Preserve raw values and source linkage alongside normalized values.
- Keep distinct SKU identities isolated.
- No output from Product Intelligence is canonical regulatory truth in this phase.

---

### Task 1: Product Intelligence domain contracts and mass normalization

**Files:**
- Create: `src/litoral_trace/product_intelligence/__init__.py`
- Create: `src/litoral_trace/product_intelligence/domain.py`
- Create: `src/litoral_trace/product_intelligence/units.py`
- Create: `tests/product_intelligence/test_domain.py`
- Create: `tests/product_intelligence/test_units.py`

**Interfaces:**
- Produces `SourceAnchor`, `MassValue`, `Material`, `Component`, `SkuComposition`, `BomIssueSeverity`, `BomIssue`, `BomIngestionResult`.
- Produces `normalize_mass(raw_value: object, raw_unit: object) -> MassValue` and `MassNormalizationError`.
- Later tasks import only these public contracts.

- [ ] **Step 1: Write failing domain tests**

```python
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from litoral_trace.product_intelligence.domain import MassValue, SourceAnchor


def test_source_anchor_and_mass_value_are_immutable():
    anchor = SourceAnchor(table_name="BOM", sheet="BOM", row=2)
    mass = MassValue(raw_value="500", raw_unit="g", kilograms=Decimal("0.5"))
    with pytest.raises(FrozenInstanceError):
        anchor.row = 3
    with pytest.raises(FrozenInstanceError):
        mass.raw_unit = "kg"
```

- [ ] **Step 2: Run domain test and verify RED**

Run: `python -m pytest tests/product_intelligence/test_domain.py -q`
Expected: import/module failure because `product_intelligence` does not exist.

- [ ] **Step 3: Implement minimal immutable domain dataclasses/enums**

Use `@dataclass(frozen=True, slots=True)` and tuples for collections. `BomIssueSeverity` values: `WARNING`, `ERROR`.

- [ ] **Step 4: Run domain tests and verify GREEN**

Run: `python -m pytest tests/product_intelligence/test_domain.py -q`
Expected: PASS.

- [ ] **Step 5: Write failing unit-normalization tests**

```python
from decimal import Decimal
import pytest

from litoral_trace.product_intelligence.units import MassNormalizationError, normalize_mass

@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        ("1", "kg", Decimal("1")),
        ("500", "g", Decimal("0.5")),
        ("2", "lb", Decimal("0.90718474")),
        ("4", "oz", Decimal("0.113398092500")),
    ],
)
def test_normalize_mass_preserves_raw_and_converts_to_kg(value, unit, expected):
    result = normalize_mass(value, unit)
    assert result.raw_value == value
    assert result.raw_unit == unit
    assert result.kilograms == expected


def test_normalize_mass_rejects_unknown_unit():
    with pytest.raises(MassNormalizationError):
        normalize_mass("1", "ton")
```

- [ ] **Step 6: Run unit tests and verify RED**

Run: `python -m pytest tests/product_intelligence/test_units.py -q`
Expected: missing implementation.

- [ ] **Step 7: Implement deterministic Decimal normalization**

Normalize unit spelling with trimmed casefolded strings and explicit alias map. Conversion factors: `1`, `0.001`, `0.45359237`, `0.028349523125`.

- [ ] **Step 8: Run Task 1 tests**

Run: `python -m pytest tests/product_intelligence/test_domain.py tests/product_intelligence/test_units.py -q`
Expected: PASS.

- [ ] **Step 9: Commit**

Commit message: `feat(product-intelligence): add domain and mass normalization`

---

### Task 2: Deterministic BOM schema/header binding

**Files:**
- Create: `src/litoral_trace/product_intelligence/bom_schema.py`
- Create: `tests/product_intelligence/test_bom_schema.py`

**Interfaces:**
- Consumes header strings from existing `ParsedTable.headers`.
- Produces `BomColumnBinding` with `sku`, `product_name`, `component`, `material`, `quantity`, `mass_value`, `mass_unit` physical header names.
- Produces `bind_bom_headers(headers: tuple[str, ...]) -> BomColumnBinding`.
- Raises `BomSchemaError` for missing required fields or duplicate/ambiguous canonical mappings.

- [ ] **Step 1: Write failing alias-binding tests**

Cover exact canonical headers and aliases: `Item Number`, `Part`, `Material Description`, `Qty`, `Weight`, `UOM`.

- [ ] **Step 2: Write failing ambiguity/missing-header tests**

Verify a table with both `SKU` and `Item Number` fails because both map to `sku`; verify missing `material` fails.

- [ ] **Step 3: Run schema tests and verify RED**

Run: `python -m pytest tests/product_intelligence/test_bom_schema.py -q`
Expected: missing module/functions.

- [ ] **Step 4: Implement normalized deterministic header matching**

Normalize headers using whitespace collapse + casefold. Keep alias sets finite and explicit. Never fuzzy-match headers.

- [ ] **Step 5: Run schema tests and verify GREEN**

Run: `python -m pytest tests/product_intelligence/test_bom_schema.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Commit message: `feat(product-intelligence): bind explicit BOM schema`

---

### Task 3: BOM row ingestion and SKU isolation

**Files:**
- Create: `src/litoral_trace/product_intelligence/bom_ingestion.py`
- Create: `tests/product_intelligence/test_bom_ingestion.py`

**Interfaces:**
- Consumes `ParsedTable` from `litoral_trace.assurance.parsers` plus optional `document_id`.
- Produces `BomIngestionResult`.
- Public function: `ingest_bom_table(table: ParsedTable, *, document_id: str | None = None) -> BomIngestionResult`.

- [ ] **Step 1: Write failing simple BOM ingestion test**

Build a `ParsedTable` with one SKU and one component. Assert one composition, one component, normalized material name, preserved raw description and source table/sheet/row.

- [ ] **Step 2: Write failing multi-component and multi-SKU tests**

Assert rows for `SKU-A` and `SKU-B` form separate `SkuComposition` objects and no component crosses the SKU boundary.

- [ ] **Step 3: Write failing invalid-row tests**

Cover missing SKU/component/material, invalid quantity, invalid mass and mass-without-unit. Valid independent rows must still be returned; bad rows must create `BomIssue` records.

- [ ] **Step 4: Run ingestion tests and verify RED**

Run: `python -m pytest tests/product_intelligence/test_bom_ingestion.py -q`
Expected: missing implementation.

- [ ] **Step 5: Implement row transformation**

Use `bind_bom_headers`; parse optional `quantity` via `Decimal`; call `normalize_mass` only when mass is present; generate stable component keys from normalized SKU + row ordinal; preserve original data; group by exact normalized SKU.

- [ ] **Step 6: Preserve deterministic source row offsets**

If `table.source.row` is the 1-based header row, first data row is `table.source.row + 1`. Use ordinal progression from `table.rows` and retain existing locator/sheet/table values. Do not fabricate page/bbox.

- [ ] **Step 7: Run ingestion tests and verify GREEN**

Run: `python -m pytest tests/product_intelligence/test_bom_ingestion.py -q`
Expected: PASS.

- [ ] **Step 8: Commit**

Commit message: `feat(product-intelligence): ingest explicit BOM rows`

---

### Task 4: CSV/XLSX golden integration through the existing Assurance parser

**Files:**
- Create: `tests/product_intelligence/test_bom_parser_integration.py`

**Interfaces:**
- Consumes `parse_csv` and `parse_xlsx` from `litoral_trace.assurance.parsers`.
- Consumes `ingest_bom_table`.
- No production parser changes unless a failing test exposes a real parser incompatibility; any such change must be minimal and regression-tested separately.

- [ ] **Step 1: Write a CSV golden test**

Generate CSV bytes in the test with two SKUs and mixed units. Parse with existing Assurance parser, then ingest the first table. Assert composition and conversions.

- [ ] **Step 2: Run CSV golden test and verify behavior**

Run: `python -m pytest tests/product_intelligence/test_bom_parser_integration.py -k csv -q`
Expected before integration completion: FAIL until imports/contracts are complete; after Tasks 1-3 it should PASS without changing Assurance parser.

- [ ] **Step 3: Write an XLSX golden test using openpyxl**

Create workbook bytes in-memory with canonical/alias headers, multiple components and two SKUs. Parse with `parse_xlsx`, ingest and assert sheet/source row linkage.

- [ ] **Step 4: Add incomplete-row mixed golden case**

Ensure a valid row survives while an invalid row yields an explicit issue.

- [ ] **Step 5: Run Product Intelligence suite**

Run: `python -m pytest tests/product_intelligence -q`
Expected: PASS.

- [ ] **Step 6: Run existing Assurance parser regression tests**

Run the repository's existing parser-normalization tests and any `tests/test_assurance_*parser*` matches discovered in the tree.
Expected: unchanged behavior and PASS.

- [ ] **Step 7: Commit**

Commit message: `test(product-intelligence): add CSV XLSX BOM golden coverage`

---

### Task 5: Architecture map and public package contract

**Files:**
- Modify: `docs/us-lacey/CAPABILITIES.toml`
- Modify: `docs/us-lacey/ARCHITECTURE.md`
- Modify: `docs/us-lacey/PIPELINE.md`
- Modify: `docs/us-lacey/TEST_MATRIX.md`
- Modify: `docs/us-lacey/ROADMAP.md`
- Modify/Create as needed: `src/litoral_trace/product_intelligence/__init__.py`
- Test: existing architecture-doc validation test.

**Interfaces:**
- Capability `product_intelligence_bom` marked `ACTIVE` only after production code/tests exist.
- Explicitly record that it is non-canonical and has no regulatory authority.

- [ ] **Step 1: Update CAPABILITIES with implementation/test paths and authority**

Record owner `product_intelligence`, status `ACTIVE`, authority `OBSERVATION/DETERMINISTIC_NORMALIZATION`, and no persistence/migrations.

- [ ] **Step 2: Update architecture/pipeline docs**

Insert `existing parser -> Product Intelligence/BOM -> future taxonomy` without changing the documented current canonical publication path.

- [ ] **Step 3: Update TEST_MATRIX and ROADMAP**

Mark BOM foundation delivered; leave taxonomy/regulatory rules as NEXT.

- [ ] **Step 4: Run architecture docs validation**

Run: `python scripts/validate_us_lacey_architecture_docs.py`
Expected: PASS.

- [ ] **Step 5: Run architecture-doc test**

Run: `python -m pytest tests/test_us_lacey_architecture_docs.py tests/test_us_lacey_safe_cleanup_contract.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

Commit message: `docs(product-intelligence): register BOM capability`

---

### Task 6: Full regression and PR gate

**Files:**
- No functional files expected unless failures reveal issues attributable to this branch.

- [ ] **Step 1: Compile**

Run: `python -m compileall main.py src tests`
Expected: PASS.

- [ ] **Step 2: Run full pytest**

Run: `python -m pytest -q -rs`
Expected: 0 failures; existing documented skips/warnings only.

- [ ] **Step 3: Confirm migration head unchanged**

Run repository Alembic head check used by CI.
Expected canonical head remains `048_lacey_source_set_revisions` unless base branch independently advanced.

- [ ] **Step 4: Open PR against `feature/us-lacey-pilot-platform`**

PR title: `feat(product-intelligence): add explicit BOM foundation`

PR body must state: no migrations, no persistence, no canonical U.S. Lacey behavior change, no new dependencies, reuse of existing parser authority, and test evidence.

- [ ] **Step 5: Wait for CI and U.S. Lacey PostgreSQL Gate**

Both must be SUCCESS on the exact PR head SHA before marking ready to merge.

- [ ] **Step 6: Review diff for scope creep**

Confirm no taxonomy, regulatory decision, auth, billing, RLS, worker, canonical truth or export changes entered the PR.

- [ ] **Step 7: Final commit only if CI-driven fixes were necessary**

Use a narrowly-scoped fix commit and rerun exact-SHA gates.
