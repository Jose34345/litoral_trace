"""Server-rendered operational workspace for the isolated U.S. Lacey portal."""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from litoral_trace.web.us_lacey_portal_views import safe, shell


def _status_badge(value: object) -> str:
    return f'<span class="badge">{safe(str(value or "UNKNOWN").replace("_", " "))}</span>'


def render_operations(*, identity, operations: Sequence, entitlement) -> str:
    if operations:
        rows = "".join(
            f"""
            <tr>
              <td><a href="/operations/{safe(item.public_id)}"><strong>{safe(item.client_reference)}</strong></a></td>
              <td>{_status_badge(item.status)}</td>
              <td>{safe(item.document_count)}</td>
              <td>{safe(item.merchandise_line_count)}</td>
              <td>{safe(item.created_at.strftime('%Y-%m-%d %H:%M'))}</td>
            </tr>
            """
            for item in operations
        )
        table = f"""
        <div class="table-wrap"><table>
          <thead><tr><th>Reference</th><th>Status</th><th>Documents</th><th>Lines</th><th>Created</th></tr></thead>
          <tbody>{rows}</tbody>
        </table></div>
        """
    else:
        table = '<div class="warn">No operations yet. Create the first shipment workspace to begin.</div>'

    new_action = (
        '<a class="button" href="/operations/new">New operation</a>'
        if entitlement.remaining_operations > 0
        else '<span class="muted">Current operation limit reached.</span>'
    )
    body = f"""
    <h1>Operations</h1>
    <p>{safe(identity.legal_name)} · document preparation workspace.</p>
    <section class="card compact">
      <div class="actions">{new_action}<a class="button secondary" href="/billing">Billing</a></div>
      <p class="muted">{safe(entitlement.used_operations)} of {safe(entitlement.monthly_operation_limit)} operation slots used; {safe(entitlement.remaining_operations)} remaining.</p>
    </section>
    <section class="card"><h2>Recent operations</h2>{table}</section>
    """
    return shell("Operations", body, authenticated=True)


def render_new_operation(*, identity, entitlement, csrf_token: str, error: str | None = None) -> str:
    notice = f'<div class="error">{safe(error)}</div>' if error else ""
    body = f"""
    <h1>New operation</h1>
    <p>Create one preparation workspace. This consumes one operation slot only after the database transaction succeeds.</p>
    <section class="card">{notice}
      <div class="meta">
        <div class="metric"><small>Available slots</small><br><strong>{safe(entitlement.remaining_operations)}</strong></div>
        <div class="metric"><small>Account</small><br><strong>{safe(identity.account_status)}</strong></div>
      </div>
      <form method="post" action="/operations/new">
        <input type="hidden" name="csrf_token" value="{safe(csrf_token)}">
        <label for="client_reference">Client / shipment reference</label>
        <input id="client_reference" name="client_reference" required maxlength="255" placeholder="PO-1048 / Shipment 2026-08-30">
        <div class="row"><div>
          <label for="importer_name">Importer</label>
          <input id="importer_name" name="importer_name" maxlength="255">
        </div><div>
          <label for="supplier_name">Supplier</label>
          <input id="supplier_name" name="supplier_name" maxlength="255">
        </div></div>
        <div class="row"><div>
          <label for="consignee_name">Consignee</label>
          <input id="consignee_name" name="consignee_name" maxlength="255">
        </div><div>
          <label for="broker_name">Customs broker</label>
          <input id="broker_name" name="broker_name" maxlength="255">
        </div></div>
        <div class="row"><div>
          <label for="operation_date">Operation date</label>
          <input id="operation_date" name="operation_date" type="date">
        </div><div>
          <label for="line_references">Merchandise line references</label>
          <input id="line_references" name="line_references" placeholder="1, 2, 3">
        </div></div>
        <p class="muted">If line references are omitted, line 1 is created automatically.</p>
        <div class="actions"><button type="submit">Create operation</button><a class="button secondary" href="/operations">Cancel</a></div>
      </form>
    </section>
    """
    return shell("New operation", body, authenticated=True)


def render_operation_detail(
    *,
    identity,
    detail,
    upload_csrf: str,
    complete_csrf: str,
    review_csrf: Mapping[int, str],
    error: str | None = None,
    notice: str | None = None,
) -> str:
    banner = ""
    if error:
        banner = f'<div class="error">{safe(error)}</div>'
    elif notice:
        banner = f'<div class="ok">{safe(notice)}</div>'

    documents = "".join(
        f"""
        <div class="item">
          <div class="actions"><strong>{safe(doc.filename)}</strong>{_status_badge(doc.processing_status)}{_status_badge(doc.job_status or 'NOT QUEUED')}</div>
          <div class="muted">Role: {safe(doc.document_role)} · version {safe(doc.version_number)}</div>
          {f'<div class="error">Processing code: {safe(doc.last_error_code)}</div>' if doc.last_error_code else ''}
        </div>
        """
        for doc in detail.documents
    ) or '<div class="warn">No documents uploaded yet.</div>'

    exception_fields = [field for field in detail.fields if field.status in {"MISSING", "REVIEW"}]
    settled_fields = [field for field in detail.fields if field.status not in {"MISSING", "REVIEW"}]

    exception_cards = []
    for field in exception_fields:
        proposed = field.proposed_value or ""
        source = "No extracted source yet."
        if field.source_assurance_document_id is not None:
            source = (
                f"Evidence document #{field.source_assurance_document_id}"
                + (f" · page {field.source_page}" if field.source_page else "")
                + (f" · {field.source_locator}" if field.source_locator else "")
            )
        accept_button = ""
        if proposed:
            accept_button = f"""
              <form method="post" action="/operations/{safe(detail.public_id)}/review/{safe(field.id)}">
                <input type="hidden" name="csrf_token" value="{safe(review_csrf[field.id])}">
                <input type="hidden" name="action" value="accept">
                <button type="submit">Accept extracted</button>
              </form>
            """
        exception_cards.append(
            f"""
            <div class="item">
              <div class="actions"><strong>Line {safe(field.line_reference)} · {safe(field.label)}</strong>{_status_badge(field.status)}</div>
              <p><strong>Proposed:</strong> {safe(proposed) if proposed else '<em>missing</em>'}</p>
              <p class="muted">Confidence: {safe(round(field.confidence * 100, 1))}% · {safe(source)}</p>
              <div class="field-review">
                <div>{accept_button}</div>
                <form method="post" action="/operations/{safe(detail.public_id)}/review/{safe(field.id)}">
                  <input type="hidden" name="csrf_token" value="{safe(review_csrf[field.id])}">
                  <input type="hidden" name="action" value="edit">
                  <input name="value" maxlength="4000" required placeholder="Enter reviewed value" value="{safe(field.effective_value or '')}">
                  <button class="secondary" type="submit">Save reviewed value</button>
                </form>
                <form method="post" action="/operations/{safe(detail.public_id)}/review/{safe(field.id)}">
                  <input type="hidden" name="csrf_token" value="{safe(review_csrf[field.id])}">
                  <input type="hidden" name="action" value="not_required">
                  <button class="secondary" type="submit">Mark not required</button>
                </form>
              </div>
            </div>
            """
        )
    exceptions_html = "".join(exception_cards) or '<div class="ok">No unresolved preparation fields remain.</div>'

    settled_rows = "".join(
        f"<tr><td>{safe(field.line_reference)}</td><td>{safe(field.label)}</td><td>{safe(field.effective_value or '')}</td><td>{_status_badge(field.status)}</td></tr>"
        for field in settled_fields
    ) or '<tr><td colspan="4" class="muted">No reviewed fields yet.</td></tr>'

    export_actions = ""
    if detail.status == "COMPLETED":
        export_actions = f"""
        <section class="card"><h2>Export preparation package</h2>
          <p>The human document review is complete. These files are preparation outputs, not a legal compliance determination or ACE/LAWGS filing.</p>
          <div class="actions">
            <a class="button" href="/operations/{safe(detail.public_id)}/export.xlsx">Download XLSX</a>
            <a class="button secondary" href="/operations/{safe(detail.public_id)}/export.csv">Download CSV</a>
          </div>
        </section>
        """

    body = f"""
    <div class="actions"><a href="/operations">← Operations</a>{_status_badge(detail.status)}</div>
    <h1>{safe(detail.client_reference)}</h1>
    <p>{safe(identity.legal_name)} · exception-first preparation review.</p>
    {banner}
    <section class="card compact"><div class="meta">
      <div class="metric"><small>Documents</small><br><strong>{safe(detail.document_count)}</strong></div>
      <div class="metric"><small>Merchandise lines</small><br><strong>{safe(detail.merchandise_line_count)}</strong></div>
      <div class="metric"><small>Importer</small><br><strong>{safe(detail.importer_name or '—')}</strong></div>
      <div class="metric"><small>Supplier</small><br><strong>{safe(detail.supplier_name or '—')}</strong></div>
    </div></section>

    <section class="card"><h2>1. Upload source documents</h2>
      <p>Original files are preserved in the private evidence vault before processing is queued.</p>
      <form method="post" enctype="multipart/form-data" action="/operations/{safe(detail.public_id)}/upload">
        <input type="hidden" name="csrf_token" value="{safe(upload_csrf)}">
        <label for="document_role">Document role</label>
        <select id="document_role" name="document_role">
          <option value="SUPPLIER_SHEET">Supplier sheet</option><option value="COMMERCIAL_INVOICE">Commercial invoice</option>
          <option value="PACKING_LIST">Packing list</option><option value="BILL_OF_LADING">Bill of lading</option>
          <option value="SUPPLIER_DECLARATION">Supplier declaration</option><option value="CERTIFICATE">Certificate</option><option value="OTHER">Other</option>
        </select>
        <label for="document">PDF, XLSX, XLS or CSV</label>
        <input id="document" name="document" type="file" accept=".pdf,.xlsx,.xls,.csv" required>
        <p><button type="submit">Store and queue document</button></p>
      </form>
      <div class="list">{documents}</div>
    </section>

    <section class="card"><h2>2. Exceptions to resolve</h2>
      <p>Missing or uncertain fields appear first. Evidence provenance stays attached to extracted values.</p>
      <div class="list">{exceptions_html}</div>
    </section>

    <section class="card"><h2>3. Reviewed data</h2>
      <div class="table-wrap"><table><thead><tr><th>Line</th><th>Field</th><th>Effective value</th><th>Status</th></tr></thead><tbody>{settled_rows}</tbody></table></div>
    </section>

    <section class="card"><h2>4. Complete human review</h2>
      <p>Completion is blocked while processing jobs, missing fields, review fields, conflicts or failed jobs remain.</p>
      <form method="post" action="/operations/{safe(detail.public_id)}/complete">
        <input type="hidden" name="csrf_token" value="{safe(complete_csrf)}">
        <button type="submit" {'disabled' if detail.status == 'COMPLETED' else ''}>Complete review</button>
      </form>
    </section>
    {export_actions}
    """
    return shell(detail.client_reference, body, authenticated=True)
