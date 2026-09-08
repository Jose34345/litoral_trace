from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.staticfiles import StaticFiles

from litoral_trace.us_lacey.bulk_review import UsLaceyBulkAcceptResult
from litoral_trace.us_lacey.csrf import UsLaceyCsrfError
from litoral_trace.web import us_lacey_intelligent_workflow as workflow
from litoral_trace.web.route_contracts import find_dynamic_literal_route_shadows
from litoral_trace.web.us_lacey_unified_app import app as unified_app


def test_route_shadow_detector_reproduces_the_original_accept_supported_bug():
    sample = FastAPI()

    @sample.post("/operations/{operation_id}/review/{field_id}")
    def field_review(operation_id: str, field_id: int):
        return {"operation_id": operation_id, "field_id": field_id}

    @sample.post("/operations/{operation_id}/review/accept-supported")
    def accept_supported(operation_id: str):
        return {"operation_id": operation_id}

    shadows = find_dynamic_literal_route_shadows(sample.routes)
    assert len(shadows) == 1
    assert shadows[0].method == "POST"
    assert shadows[0].earlier_path.endswith("/review/{field_id}")
    assert shadows[0].later_path.endswith("/review/accept-supported")


def test_typed_field_route_cannot_shadow_a_literal_review_action():
    sample = FastAPI()

    @sample.post("/operations/{operation_id}/review/{field_id:int}")
    def field_review(operation_id: str, field_id: int):
        return {"operation_id": operation_id, "field_id": field_id}

    @sample.post("/operations/{operation_id}/review/accept-supported")
    def accept_supported(operation_id: str):
        return {"operation_id": operation_id}

    assert find_dynamic_literal_route_shadows(sample.routes) == ()


def test_unified_us_lacey_review_routes_have_no_dynamic_literal_shadow():
    review_routes = [
        route
        for route in unified_app.routes
        if "/operations/" in str(getattr(route, "path", ""))
        and "/review" in str(getattr(route, "path", ""))
    ]
    assert find_dynamic_literal_route_shadows(review_routes) == ()


def _standalone_review_app(monkeypatch, calls: list[dict]):
    app = FastAPI()
    app.mount(
        "/static",
        StaticFiles(directory="src/litoral_trace/static"),
        name="static",
    )
    app.include_router(workflow.router)
    identity = SimpleNamespace(
        organization_id=314,
        user_id=2718,
        email="reviewer@example.com",
    )
    monkeypatch.setattr(
        workflow,
        "_identity_and_entitlement",
        lambda _token, require_slot=False: (identity, SimpleNamespace()),
    )
    monkeypatch.setattr(workflow, "verify_us_lacey_csrf", lambda **_kwargs: None)

    def accept_bulk(**kwargs):
        calls.append(dict(kwargs))
        return UsLaceyBulkAcceptResult(accepted_count=2, operation_status="REVIEW_REQUIRED")

    monkeypatch.setattr(workflow, "accept_supported_us_lacey_fields", accept_bulk)
    return app


def test_canonical_and_legacy_bulk_urls_reach_bulk_handler_not_field_parser(monkeypatch):
    calls: list[dict] = []
    app = _standalone_review_app(monkeypatch, calls)
    operation_id = "80d62297-c3b5-4890-8217-a6131f89c315"

    with TestClient(app, follow_redirects=False) as client:
        canonical = client.post(
            f"/operations/{operation_id}/review/actions/accept-supported",
            data={"csrf_token": "test-token"},
        )
        legacy = client.post(
            f"/operations/{operation_id}/review/accept-supported",
            data={"csrf_token": "test-token"},
        )

    assert canonical.status_code == 303
    assert legacy.status_code == 303
    assert canonical.headers["location"] == f"/operations/{operation_id}"
    assert legacy.headers["location"] == f"/operations/{operation_id}"
    assert len(calls) == 2
    assert {call["organization_id"] for call in calls} == {314}
    assert all(call["operation_public_id"] == operation_id for call in calls)


def test_bulk_url_fails_closed_on_invalid_csrf_without_422_route_confusion(monkeypatch):
    calls: list[dict] = []
    app = _standalone_review_app(monkeypatch, calls)
    operation_id = "80d62297-c3b5-4890-8217-a6131f89c315"

    def reject_csrf(**_kwargs):
        raise UsLaceyCsrfError("Invalid CSRF token.")

    monkeypatch.setattr(workflow, "verify_us_lacey_csrf", reject_csrf)
    with TestClient(app, follow_redirects=False) as client:
        response = client.post(
            f"/operations/{operation_id}/review/actions/accept-supported",
            data={"csrf_token": "invalid"},
        )

    assert response.status_code == 400
    assert response.status_code != 422
    assert calls == []
