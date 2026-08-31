"""Free-tier ASGI entrypoint for the private U.S. Lacey pilot.

Render's free tier does not provide a separate background-worker service. This
entrypoint keeps the customer portal unchanged and runs the durable U.S. Lacey
queue consumer in a daemon thread while the web instance is awake.

The queue still uses ``US_LACEY_WORKER_DATABASE_URL`` and therefore preserves
the dedicated least-privilege PostgreSQL worker role. The free deployment is a
private-beta convenience only: when Render spins the web service down, queue
processing pauses and resumes on the next wake-up.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import html
import logging
import os
import socket
import threading
import time
from uuid import uuid4

from fastapi.responses import HTMLResponse

from litoral_trace.us_lacey.jobs import recover_stale_us_lacey_jobs
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_database_url
from litoral_trace.web.us_lacey_pilot_app import app


_LOG = logging.getLogger("litoral_trace.us_lacey.inline_worker")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = str(os.environ.get(name, default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be numeric.") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} is outside the supported range.")
    return value


def _int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = str(os.environ.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    if value < minimum or value > maximum:
        raise RuntimeError(f"{name} is outside the supported range.")
    return value


def _inline_worker_loop(stop_event: threading.Event) -> None:
    poll_seconds = _float_env(
        "US_LACEY_WORKER_POLL_SECONDS", 2.0, minimum=0.25, maximum=30.0
    )
    recovery_every = _int_env(
        "US_LACEY_WORKER_RECOVERY_EVERY_SECONDS", 60, minimum=30, maximum=3600
    )
    stale_after = _int_env(
        "US_LACEY_WORKER_STALE_AFTER_SECONDS", 600, minimum=60, maximum=86400
    )
    worker_id = f"inline-{socket.gethostname()}-{uuid4().hex[:12]}"
    next_recovery = 0.0

    _LOG.info("us_lacey_inline_worker_started worker_id=%s", worker_id)
    while not stop_event.is_set():
        now = time.monotonic()
        if now >= next_recovery:
            try:
                retried, failed = recover_stale_us_lacey_jobs(
                    stale_after_seconds=stale_after
                )
                if retried or failed:
                    _LOG.warning(
                        "stale_jobs_recovered retried=%s failed=%s",
                        retried,
                        failed,
                    )
            except Exception:
                _LOG.exception("stale_job_recovery_failed")
            next_recovery = now + recovery_every

        try:
            result = process_one_us_lacey_job(worker_id=worker_id)
            if result.claimed:
                _LOG.info(
                    "job_processed job_id=%s job_status=%s document_status=%s "
                    "operation_status=%s projected=%s conflicts=%s",
                    result.job_id,
                    result.job_status,
                    result.document_status,
                    result.operation_status,
                    result.projected_count,
                    result.conflict_count,
                )
                continue
        except Exception:
            _LOG.exception("inline_worker_iteration_failed")
            if stop_event.wait(min(5.0, max(1.0, poll_seconds * 2.0))):
                break
            continue

        stop_event.wait(poll_seconds)

    _LOG.info("us_lacey_inline_worker_stopped worker_id=%s", worker_id)


def _start_inline_worker() -> None:
    if not _bool_env("US_LACEY_INLINE_WORKER_ENABLED", default=False):
        _LOG.info("us_lacey_inline_worker_disabled")
        return

    # Fail closed before accepting traffic if the dedicated worker URL is absent,
    # points at another database, or reuses the web runtime role.
    get_us_lacey_worker_database_url()

    existing = getattr(app.state, "us_lacey_inline_worker_thread", None)
    if existing is not None and existing.is_alive():
        return

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_inline_worker_loop,
        args=(stop_event,),
        name="us-lacey-inline-worker",
        daemon=True,
    )
    app.state.us_lacey_inline_worker_stop = stop_event
    app.state.us_lacey_inline_worker_thread = thread
    thread.start()


def _stop_inline_worker() -> None:
    stop_event = getattr(app.state, "us_lacey_inline_worker_stop", None)
    thread = getattr(app.state, "us_lacey_inline_worker_thread", None)
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive():
        thread.join(timeout=10.0)


# FastAPI/Starlette's current lifecycle API is lifespan-based. Preserve the
# portal's existing lifespan context and wrap only the free-tier queue runner
# around it so future portal startup/shutdown behavior is not discarded.
_original_lifespan_context = app.router.lifespan_context


@asynccontextmanager
async def _free_lifespan(application):
    async with _original_lifespan_context(application) as state:
        _start_inline_worker()
        try:
            yield state
        finally:
            _stop_inline_worker()


app.router.lifespan_context = _free_lifespan


def _legal_response(title: str, version_env: str, sections: list[tuple[str, str]]) -> HTMLResponse:
    version = html.escape(str(os.environ.get(version_env, "2026-08-30-v1")).strip())
    section_html = "".join(
        f"<h2>{html.escape(heading)}</h2><p>{body}</p>" for heading, body in sections
    )
    content = f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{html.escape(title)} · Litoral Trace</title>
        <style>
          body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; color: #11261c; background: #f7faf8; }}
          main {{ max-width: 860px; margin: 0 auto; padding: 48px 24px 72px; }}
          a {{ color: #155d3b; }}
          h1 {{ font-size: clamp(2rem, 5vw, 3rem); line-height: 1.05; }}
          h2 {{ margin-top: 2rem; }}
          .meta {{ color: #52635b; }}
          .notice {{ background: white; border: 1px solid #d8e2dc; border-radius: 14px; padding: 18px 20px; }}
        </style>
      </head>
      <body>
        <main>
          <p><a href="/signup">← Back to account creation</a></p>
          <h1>{html.escape(title)}</h1>
          <p class="meta">Version {version} · Private U.S. Lacey beta</p>
          <div class="notice"><strong>Important:</strong> Litoral Trace is a software workflow and evidence-management tool. It does not provide legal advice, does not make an automatic legal-compliance determination, and does not represent that it has a live ACE/LAWGS filing integration.</div>
          {section_html}
          <p>Questions about this beta can be sent to <a href="mailto:comercial@litoraltrace.com">comercial@litoraltrace.com</a>.</p>
        </main>
      </body>
    </html>
    """
    response = HTMLResponse(content=content)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response


@app.get("/legal/terms", response_class=HTMLResponse)
def us_lacey_terms() -> HTMLResponse:
    return _legal_response(
        "Terms of Service",
        "US_LACEY_TERMS_VERSION",
        [
            ("Purpose", "The service helps authorized business users organize shipment and supplier documents, extract structured data, reconcile fields, surface missing or conflicting information, retain evidence, support human review, and prepare export or pre-filing work products."),
            ("Customer responsibility", "You are responsible for the accuracy, completeness, legality, and authorization of the information and documents you submit, and for all filing, import, customs, regulatory, and legal decisions made using the service."),
            ("No legal determination", "Outputs are operational assistance only. They must be reviewed by an appropriately qualified person before being relied upon for a filing, declaration, compliance decision, or representation to a regulator, broker, customer, or other third party."),
            ("Private beta", "Features, workflows, limits, and availability may change during the private beta. Beta access may be suspended when necessary to protect security, data integrity, or service reliability."),
            ("Fees and activation", "Any applicable private-beta fee, operation limit, and activation status are shown in the account billing workflow. Access to operational features may remain blocked until the agreed payment or manual activation is confirmed."),
        ],
    )


@app.get("/legal/privacy", response_class=HTMLResponse)
def us_lacey_privacy() -> HTMLResponse:
    return _legal_response(
        "Privacy Notice",
        "US_LACEY_PRIVACY_VERSION",
        [
            ("Information processed", "The service may process company and account details, uploaded business documents, extracted document data, operation metadata, audit records, technical request information such as IP address and user agent, and support communications."),
            ("Why it is processed", "Information is used to provide the requested workflow, authenticate users, isolate organizations, process and store evidence, troubleshoot the service, prevent abuse, maintain auditability, and provide support."),
            ("Infrastructure", "The private U.S. beta uses isolated U.S.-region cloud infrastructure for application hosting, database services, and evidence storage. Service providers may process data solely as needed to operate those components."),
            ("Sharing", "Litoral Trace does not sell customer conversation or business-document data to advertisers. Data may be disclosed when necessary to operate the service, comply with law, protect rights or security, or when directed by an authorized customer."),
            ("Retention and deletion", "Data is retained as needed to provide the beta, preserve required audit evidence, investigate incidents, and satisfy legitimate contractual or legal obligations. Deletion requests for beta data can be submitted to the contact address below and will be evaluated against those obligations."),
            ("Security", "The service uses tenant isolation, least-privilege database roles, row-level security, private object storage, transport encryption, and audit controls. No Internet service can guarantee absolute security."),
        ],
    )


@app.get("/legal/private-beta", response_class=HTMLResponse)
def us_lacey_private_beta_terms() -> HTMLResponse:
    return _legal_response(
        "Private Beta Terms",
        "US_LACEY_BETA_TERMS_VERSION",
        [
            ("Beta status", "This environment is an early private beta intended for controlled evaluation with authorized businesses. It is not offered with a production service-level commitment."),
            ("Free-tier behavior", "During the free infrastructure phase, the web instance can sleep after inactivity. Background document processing pauses while the instance is asleep and resumes after the service wakes."),
            ("Authorized documents only", "You may upload only documents and data that your organization is authorized to provide and process. Do not upload unrelated personal, confidential, export-controlled, or regulated material unless its use is specifically authorized and appropriate for the service."),
            ("Human review required", "Extraction, reconciliation, exception detection, and generated work products can contain errors or omissions. A qualified human must review material outputs before they are used operationally."),
            ("No live government filing", "The private beta does not claim live ACE/LAWGS submission. Exported or prepared data is a work product for review and downstream use, not proof that a government filing was accepted."),
            ("Changes and feedback", "Because this is a beta, functionality and limits may change. Operational feedback may be used to improve the product, while customer documents and tenant data remain subject to the Privacy Notice."),
        ],
    )
