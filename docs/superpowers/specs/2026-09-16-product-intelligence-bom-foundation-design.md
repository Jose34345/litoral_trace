# Product Intelligence / BOM Foundation Design

## Goal
Create a reusable Product Intelligence foundation for explicit CSV/XLSX bills of materials (BOMs) that turns source-backed tabular data into a structured `Product -> SKU -> Component -> Material` model without changing current U.S. Lacey canonical truth, worker behavior, regulatory decisions, billing, auth, RLS, or export contracts.

## Why now
The current U.S. Lacey stack is strong at document ingestion, parsing, evidence, source-set lifecycle, reconciliation, review, and export. The next commercial step is to reduce human work by reconstructing product composition before taxonomy and deterministic regulatory reasoning. This foundation must reuse existing parsers/provenance instead of creating a parallel document engine.

## Scope
### In scope
- New reusable package: `src/litoral_trace/product_intelligence/`.
- Immutable domain types for product identity, SKU, component, material, quantities/weights, original/normalized units, and source references.
- Deterministic explicit-BOM ingestion from existing parsed CSV/XLSX tables.
- Header alias resolution for a small, explicit initial schema.
- Unit normalization for mass values into kilograms while preserving raw values.
- Strict row validation and explicit issue reporting for incomplete/ambiguous rows.
- Cross-SKU isolation: a row can never silently attach to another SKU.
- Source provenance from parsed table/sheet/row metadata into Product Intelligence output.
- Golden synthetic tests covering simple BOM, multi-component product, mixed units, incomplete rows, and multiple SKUs.
- Architecture docs/CAPABILITIES update so future agents know the new owner and boundaries.

### Out of scope
- Taxonomy resolution.
- Species inference.
- De minimis/composite/special-use regulatory decisions.
- PDF free-form BOM reconstruction.
- Database persistence or Alembic migrations.
- Changes to canonical shipment truth.
- Changes to U.S. Lacey review/export behavior.
- New AI calls or autonomous agents.
- New external dependencies.

## Architectural placement
`product_intelligence` is reusable domain logic and must not live inside `lacey_engine` prompts or U.S.-specific regulatory code.

Data flow for this phase:

`CSV/XLSX bytes -> existing assurance parser -> ParsedDocument/ParsedTable -> BOM schema resolver -> Product Intelligence domain objects + issues`

The package consumes already-parsed tables and source metadata. It does not own raw-file parsing. This preserves one parsing authority and avoids duplicate XLSX/CSV behavior.

## Package structure
- `src/litoral_trace/product_intelligence/__init__.py`: public stable exports only.
- `src/litoral_trace/product_intelligence/domain.py`: immutable dataclasses/enums and result contracts.
- `src/litoral_trace/product_intelligence/units.py`: deterministic mass-unit parsing and kg normalization.
- `src/litoral_trace/product_intelligence/bom_schema.py`: explicit supported headers/aliases and field binding.
- `src/litoral_trace/product_intelligence/bom_ingestion.py`: row-to-domain transformation and issue collection.

No persistence layer is introduced in this phase.

## Domain model
### SourceAnchor
Carries source linkage available from the existing parsed-table layer:
- `document_id: str | None`
- `table_name: str`
- `sheet: str | None`
- `row: int | None`
- `column: int | None`
- `locator: str | None`

This is not a replacement for semantic evidence. It is a transport-level reference so later integration can map the Product Intelligence result to existing evidence entities.

### MassValue
- `raw_value: str`
- `raw_unit: str`
- `kilograms: Decimal`

Normalization is deterministic. Unsupported/ambiguous units produce an issue rather than an invented conversion.

### Material
- `name_raw: str`
- `name_normalized: str`
- `mass: MassValue | None`
- `source: SourceAnchor`

No plant/non-plant or taxonomic classification exists yet.

### Component
- `component_key: str`
- `description_raw: str`
- `material: Material`
- `quantity: Decimal | None`
- `source: SourceAnchor`

### SkuComposition
- `sku: str`
- `product_name: str | None`
- `components: tuple[Component, ...]`

### BomIssue
Machine-readable issue rather than exception for row-level business-data problems:
- `code`
- `message`
- `severity`
- `source`

Initial issue codes include: `MISSING_SKU`, `MISSING_COMPONENT`, `MISSING_MATERIAL`, `INVALID_QUANTITY`, `INVALID_MASS`, `UNSUPPORTED_MASS_UNIT`, `MISSING_REQUIRED_HEADERS`.

### BomIngestionResult
- `compositions: tuple[SkuComposition, ...]`
- `issues: tuple[BomIssue, ...]`

The result may contain valid compositions plus issues from independent rows. File-level structural failures remain fail-closed.

## Initial BOM contract
The importer accepts a deliberately narrow explicit schema with aliases. Canonical fields:
- `sku` (required)
- `product_name` (optional)
- `component` (required)
- `material` (required)
- `quantity` (optional)
- `mass_value` (optional)
- `mass_unit` (required when `mass_value` is present)

Initial aliases may include common variants such as `SKU`, `Item`, `Item Number`, `Product`, `Component`, `Part`, `Material`, `Material Description`, `Qty`, `Quantity`, `Weight`, `Mass`, `Weight Unit`, `UOM`.

Alias matching is normalized and deterministic. If two columns map to the same canonical field, ingestion fails that table as ambiguous instead of selecting one silently.

## Unit rules
Use `Decimal`, never binary float, for BOM quantities and masses.

Initial supported mass units:
- kg, kilogram, kilograms -> kg
- g, gram, grams -> kg / 1000
- lb, lbs, pound, pounds -> kg using exact factor `0.45359237`
- oz, ounce, ounces -> kg using exact factor `0.028349523125`

Raw value/unit are preserved.

No dimensional inference from column name is allowed unless the unit is explicitly encoded in a supported header variant in a future separately-reviewed change.

## Provenance and row identity
Existing assurance tables currently expose table-level source metadata and row dictionaries. The BOM adapter must derive row numbers deterministically from table order relative to the parsed header when possible and preserve the table source locator. It must never fabricate page/bbox evidence that the parser does not provide.

A later integration phase may replace/augment `SourceAnchor` with semantic-evidence IDs. This phase keeps the core independent of U.S. Lacey persistence.

## Safety behavior
- Missing/invalid business data must not be inferred.
- Ambiguous headers fail closed for that table.
- Invalid rows do not contaminate valid rows.
- Same SKU rows are grouped only by exact normalized SKU identity; distinct SKU strings remain distinct.
- Components from one SKU cannot be attached to another.
- Duplicate component rows are preserved unless they are byte-for-byte/domain-identical; no speculative reconciliation in this phase.
- No output from this package is canonical regulatory truth.

## Integration boundary
This PR may expose a small adapter/helper callable by U.S. Lacey in future, but it must not wire Product Intelligence into current canonical publication or user-facing export. Current behavior must remain unchanged unless an explicit future feature flag/integration PR is approved.

## Testing
### Unit tests
- domain immutability/basic invariants;
- unit conversions and invalid unit handling;
- header alias resolution and ambiguity;
- row validation;
- grouping by SKU;
- multi-SKU isolation;
- raw + normalized values preserved;
- source anchors preserved.

### Golden BOM tests
Synthetic fixtures generated in tests for:
1. one SKU / one component;
2. one SKU / several components;
3. mixed kg/g/lb/oz;
4. incomplete rows mixed with valid rows;
5. two SKUs in one CSV/XLSX;
6. ambiguous duplicate header mapping.

### Regression
- existing assurance parser tests remain unchanged and green;
- full CI remains green;
- no migration/PostgreSQL gate should be functionally affected because persistence is out of scope, but the existing U.S. Lacey gate should still be run before merge because this becomes a dependency of the commercial branch.

## Success criteria
- Explicit CSV/XLSX BOMs can be converted deterministically to structured product composition.
- Every accepted material/component retains source linkage available from the parser.
- No row crosses SKU boundaries.
- Unsupported/ambiguous data becomes an explicit issue, never silent inference.
- No current U.S. Lacey runtime behavior changes.
- No migrations/dependencies are added.
- Full CI and U.S. Lacey PostgreSQL gate remain green.

## Follow-on work
After this foundation is merged and stable:
1. Taxonomy Resolver consumes `Material` names and returns versioned candidates.
2. Regulatory rules consume composition/taxonomy plus shipment context.
3. Exception-first review surfaces ambiguity and missing evidence.
4. Source-linked review package exposes these decisions to customers.
