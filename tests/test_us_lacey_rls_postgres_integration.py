from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.us_lacey.operations import UsLaceyOperationService


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL") or os.environ.get("MIGRATION_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="US Lacey RLS acceptance requires isolated runtime and owner URLs.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _tenant(conn, organization_id: int) -> None:
    conn.execute(
        text("SELECT set_config('app.current_organization_id', :organization_id, true)"),
        {"organization_id": str(organization_id)},
    )


def _create_org(conn, *, label: str, suffix: str) -> int:
    org_id = int(
        conn.execute(
            text("SELECT nextval(pg_get_serial_sequence('public.organizations', 'id'))")
        ).scalar_one()
    )
    _tenant(conn, org_id)
    conn.execute(
        text("""
            INSERT INTO organizations (
                id, name, slug, tax_id, tier, description, is_active
            ) VALUES (
                :id, :name, :slug, :tax_id, 'pro', 'US Lacey RLS acceptance', true
            )
        """),
        {
            "id": org_id,
            "name": f"US Lacey Tenant {label.upper()} {suffix}",
            "slug": f"us-lacey-rls-{label}-{suffix}",
            "tax_id": f"US-{label.upper()}-{suffix}",
        },
    )
    return org_id


@pytest.fixture(scope="module")
def fixture():
    suffix = uuid4().hex[:9]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    values: dict[str, int] = {}
    with owner.begin() as conn:
        for label in ("a", "b"):
            org_id = _create_org(conn, label=label, suffix=suffix)
            values[f"org_{label}"] = org_id
            _tenant(conn, org_id)
            operation_id = conn.execute(
                text("""
                    INSERT INTO us_lacey_operations (
                        organization_id,
                        client_reference,
                        status,
                        document_count,
                        merchandise_line_count
                    ) VALUES (
                        :org,
                        :reference,
                        'NEW',
                        0,
                        1
                    ) RETURNING id
                """),
                {"org": org_id, "reference": f"RLS-{label.upper()}-{suffix}"},
            ).scalar_one()
            values[f"operation_{label}"] = int(operation_id)
    yield {**values, "owner": owner, "runtime": runtime, "suffix": suffix}
    runtime.dispose()
    owner.dispose()


def test_without_tenant_context_runtime_sees_no_us_operations(fixture):
    with fixture["runtime"].begin() as conn:
        assert conn.execute(text("SELECT id FROM us_lacey_operations")).fetchall() == []


def test_company_a_cannot_read_company_b_operation(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        rows = conn.execute(
            text("SELECT id, organization_id FROM us_lacey_operations ORDER BY id")
        ).fetchall()
    assert rows
    assert {row[1] for row in rows} == {fixture["org_a"]}
    assert fixture["operation_b"] not in {row[0] for row in rows}


def test_company_a_cannot_insert_operation_for_company_b(fixture):
    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text("""
                    INSERT INTO us_lacey_operations (
                        organization_id,
                        client_reference,
                        status,
                        document_count,
                        merchandise_line_count
                    ) VALUES (
                        :other_org,
                        'CROSS-TENANT-DENIED',
                        'NEW',
                        0,
                        1
                    )
                """),
                {"other_org": fixture["org_b"]},
            )


def test_company_a_cannot_update_company_b_operation(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        result = conn.execute(
            text("""
                UPDATE us_lacey_operations
                SET review_result = 'SHOULD_NOT_WRITE'
                WHERE id = :operation_b
            """),
            {"operation_b": fixture["operation_b"]},
        )
        assert result.rowcount == 0


def test_new_two_line_operation_starts_with_ppq_missing_fields_and_no_guesses(fixture):
    SessionFactory = sessionmaker(
        bind=fixture["runtime"],
        autoflush=False,
        expire_on_commit=False,
    )
    service = UsLaceyOperationService(session_factory=SessionFactory)
    snapshot = service.create_operation(
        organization_id=fixture["org_a"],
        created_by_user_id=None,
        client_reference=f"NO-GUESSES-{fixture['suffix']}",
        line_references=("1", "2"),
    )

    assert snapshot.merchandise_line_count == 2
    assert snapshot.missing_field_count == 26
    assert snapshot.review_field_count == 0

    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        row = conn.execute(
            text("""
                SELECT
                    count(*) AS total_fields,
                    count(*) FILTER (WHERE field_status = 'MISSING') AS missing_fields,
                    count(*) FILTER (WHERE normalized_value IS NOT NULL) AS guessed_values,
                    max(confidence) AS max_confidence
                FROM us_lacey_operation_fields
                WHERE operation_id = (
                    SELECT id FROM us_lacey_operations
                    WHERE organization_id = :org AND public_id = :public_id
                )
            """),
            {"org": fixture["org_a"], "public_id": snapshot.public_id},
        ).mappings().one()
    assert row["total_fields"] == 28
    assert row["missing_fields"] == 26
    assert row["guessed_values"] == 0
    assert float(row["max_confidence"]) == 0.0


def test_plant_line_supports_ordered_multi_declarations_without_overwriting(fixture):
    SessionFactory = sessionmaker(
        bind=fixture["runtime"],
        autoflush=False,
        expire_on_commit=False,
    )
    service = UsLaceyOperationService(session_factory=SessionFactory)
    snapshot = service.create_operation(
        organization_id=fixture["org_a"],
        created_by_user_id=None,
        client_reference=f"MULTI-DECLARATION-{fixture['suffix']}",
        line_references=("1",),
    )

    initial = service.get_detail(
        organization_id=fixture["org_a"], operation_public_id=snapshot.public_id
    )
    assert [(item.line_reference, item.ordinal, item.species) for item in initial.plant_declarations] == [
        ("1", 1, None)
    ]
    assert initial.plant_declarations[0].country_of_harvest is None
    optional_fields = {
        field.field_name: field for field in initial.fields if field.scope == "SHIPMENT"
    }
    assert optional_fields["container_number"].status == "MATCHED"
    assert optional_fields["container_number"].effective_value is None
    assert optional_fields["bill_of_lading"].status == "MATCHED"

    second = service.upsert_plant_declaration(
        organization_id=fixture["org_a"],
        operation_public_id=snapshot.public_id,
        line_reference="1",
        genus="Quercus",
        species="Quercus rubra",
        country_of_harvest="Canada",
        quantity="12",
        unit="kg",
        original_values={"species": "Quercus rubra", "country_of_harvest": "Canada"},
        source_locator="sheet:Plants;data_row:2",
        extractor="test-evidence",
        extractor_version="1",
        confidence=0.91,
    )
    assert second.ordinal == 2
    service.upsert_plant_declaration(
        organization_id=fixture["org_a"],
        operation_public_id=snapshot.public_id,
        line_reference="1",
        ordinal=1,
        genus="Quercus",
        species="Quercus alba",
        country_of_harvest="United States",
        quantity="10",
        unit="kg",
        original_values={"species": "Quercus alba", "country_of_harvest": "United States"},
        source_locator="sheet:Plants;data_row:1",
        extractor="test-evidence",
        extractor_version="1",
        confidence=0.92,
    )
    service.upsert_plant_declaration(
        organization_id=fixture["org_a"],
        operation_public_id=snapshot.public_id,
        line_reference="1",
        ordinal=2,
        species="Quercus rubra updated",
        country_of_harvest="Mexico",
    )
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        conn.execute(
            text("""
                UPDATE us_lacey_operation_fields
                SET human_value = 'Human-reviewed species',
                    field_status = 'MATCHED',
                    validation_status = 'VALID'
                WHERE organization_id = :org
                  AND operation_id = (
                      SELECT id FROM us_lacey_operations
                      WHERE organization_id = :org AND public_id = :public_id
                  )
                  AND field_name = 'species'
            """),
            {"org": fixture["org_a"], "public_id": snapshot.public_id},
        )

    detail = service.get_detail(
        organization_id=fixture["org_a"], operation_public_id=snapshot.public_id
    )
    declarations = [item for item in detail.plant_declarations if item.line_reference == "1"]
    assert [(item.ordinal, item.species, item.country_of_harvest) for item in declarations] == [
        (1, "Quercus alba", "United States"),
        (2, "Quercus rubra updated", "Mexico"),
    ]
    assert declarations[1].quantity == "12"
    assert declarations[1].unit == "kg"
    assert declarations[1].original_values == {
        "species": "Quercus rubra", "country_of_harvest": "Canada"
    }
    assert declarations[1].source_locator == "sheet:Plants;data_row:2"
    assert declarations[1].confidence == 0.91
    reviewed_species = next(field for field in detail.fields if field.field_name == "species")
    assert reviewed_species.effective_value == "Human-reviewed species"
