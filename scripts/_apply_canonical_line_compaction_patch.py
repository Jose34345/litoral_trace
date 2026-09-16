from __future__ import annotations

from pathlib import Path

TARGET = Path("src/litoral_trace/us_lacey/canonical_shipment_truth.py")
SELF = Path("scripts/_apply_canonical_line_compaction_patch.py")
WORKFLOW = Path(".github/workflows/_canonical_line_compaction_apply.yml")

text = TARGET.read_text(encoding="utf-8")
old = '''def _ensure_plant_line_count(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    needed: int,
) -> tuple[UsLaceyPpqPlantLine, ...]:
    lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == organization_id,
                UsLaceyPpqPlantLine.operation_id == operation.id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )
    existing_refs = {str(line.line_reference) for line in lines}
    next_ordinal = max((int(line.ordinal) for line in lines), default=0) + 1
    while len(lines) < needed:
        ordinal = next_ordinal
        reference = f"CANONICAL-{ordinal}"
        suffix = 1
        while reference in existing_refs:
            suffix += 1
            reference = f"CANONICAL-{ordinal}-{suffix}"
        line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation.id,
            line_reference=reference,
            ordinal=ordinal,
        )
        session.add(line)
        session.flush()
        session.add(
            UsLaceyPlantDeclaration(
                organization_id=organization_id,
                plant_line_id=line.id,
                ordinal=1,
            )
        )
        for contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=organization_id,
                    operation_id=operation.id,
                    merchandise_line_reference=reference,
                    field_name=contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )
        lines.append(line)
        existing_refs.add(reference)
        next_ordinal += 1
    operation.merchandise_line_count = max(
        int(operation.merchandise_line_count or 0), len(lines)
    )
    session.flush()
    return tuple(lines)
'''
new = '''def _line_has_human_review(
    session,
    *,
    organization_id: int,
    operation_id: int,
    plant_line_id: int,
) -> bool:
    fields = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == organization_id,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.plant_line_id == plant_line_id,
        )
    ).all()
    return any(
        field.reviewed_at is not None
        or field.reviewed_by_user_id is not None
        or bool(str(field.human_value or "").strip())
        for field in fields
    )


def _ensure_plant_line_count(
    session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    needed: int,
) -> tuple[UsLaceyPpqPlantLine, ...]:
    lines = list(
        session.scalars(
            select(UsLaceyPpqPlantLine)
            .where(
                UsLaceyPpqPlantLine.organization_id == organization_id,
                UsLaceyPpqPlantLine.operation_id == operation.id,
            )
            .order_by(UsLaceyPpqPlantLine.ordinal.asc(), UsLaceyPpqPlantLine.id.asc())
        ).all()
    )

    if len(lines) > needed:
        surplus = lines[needed:]
        for line in surplus:
            if not str(line.line_reference).startswith("CANONICAL-"):
                raise RuntimeError("CANONICAL_STALE_LINE_REQUIRES_REVIEW")
            if _line_has_human_review(
                session,
                organization_id=organization_id,
                operation_id=int(operation.id),
                plant_line_id=int(line.id),
            ):
                raise RuntimeError("CANONICAL_STALE_LINE_REQUIRES_REVIEW")
        for line in surplus:
            session.delete(line)
        session.flush()
        lines = lines[:needed]

    existing_refs = {str(line.line_reference) for line in lines}
    next_ordinal = max((int(line.ordinal) for line in lines), default=0) + 1
    while len(lines) < needed:
        ordinal = next_ordinal
        reference = f"CANONICAL-{ordinal}"
        suffix = 1
        while reference in existing_refs:
            suffix += 1
            reference = f"CANONICAL-{ordinal}-{suffix}"
        line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation.id,
            line_reference=reference,
            ordinal=ordinal,
        )
        session.add(line)
        session.flush()
        session.add(
            UsLaceyPlantDeclaration(
                organization_id=organization_id,
                plant_line_id=line.id,
                ordinal=1,
            )
        )
        for contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=organization_id,
                    operation_id=operation.id,
                    merchandise_line_reference=reference,
                    field_name=contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )
        lines.append(line)
        existing_refs.add(reference)
        next_ordinal += 1
    operation.merchandise_line_count = len(lines)
    session.flush()
    return tuple(lines)
'''

count = text.count(old)
if count != 1:
    raise SystemExit(f"expected exactly one plant-line helper block, found {count}")
text = text.replace(old, new, 1)
TARGET.write_text(text, encoding="utf-8")
SELF.unlink()
WORKFLOW.unlink()
