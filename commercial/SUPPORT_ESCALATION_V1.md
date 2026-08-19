# Litoral Trace V1 — Support and Escalation

**Versión operativa:** 1.0  
**Vigencia:** 19 de agosto de 2026

## Customer contact route

Primary customer support address: **`comercial@litoraltrace.com`**. This mailbox is active and is the documented customer-facing route until a dedicated support mailbox or ticket portal is introduced.

Do not use personal chat accounts as the authoritative incident route. A proposal may nominate an additional contact, but customer incidents must remain traceable through the documented support channel.

## Operational ownership and support window

**Primary operator / escalation owner:** José David Lezcano.  
**Business support window:** Monday to Friday, **09:00–18:00 Argentina time (ART, UTC-3)**, excluding Argentine national holidays unless a signed proposal states otherwise.  
**After-hours:** no 24x7 customer SLA is included by default. Automated monitoring may detect technical incidents outside the business window; response outside the window is best-effort unless a signed production agreement explicitly provides another coverage model.

The times below are internal service objectives for triage and communication, not contractual SLA guarantees unless repeated in a signed proposal or order of service.

## Severity model

### P0 — Critical

Examples: service broadly unavailable, confirmed tenant-isolation concern, unrecoverable production-data risk, or an incident requiring immediate containment.

Target handling: prioritize containment and recovery above feature work, preserve operational evidence, and use the relevant runbook. During the active support window, target initial acknowledgement within **2 business hours**.

### P1 — High

Examples: a major customer workflow is blocked, repeated Vault/storage failure, failed scheduled backup, sustained Satellite worker failure, or authentication unavailable for a customer organization.

Target handling: investigate with priority during the active support period and provide a workaround or next update when available. Target initial acknowledgement within **4 business hours**.

### P2 — Normal

Examples: isolated workflow defect with workaround, data-import question, UX issue, configuration request, or non-critical report problem.

Target handling: queue for normal business support. Target initial acknowledgement within **1 business day**.

### P3 — Request / Improvement

Examples: new report, integration, UI improvement, or feature request. These are product requests rather than incidents and may be routed to the product backlog. Target acknowledgement within **3 business days**.

## What to include in a support request

Provide only the minimum material needed for triage:

- organization/tenant name;
- affected workflow or screen;
- approximate timestamp and timezone;
- user-visible error or job/reference ID if available;
- business impact and whether a workaround exists.

Do **not** send passwords, refresh tokens, API secrets, private keys, full database URLs, raw production dumps or unnecessary customer datasets by ordinary email.

## Pilot support expectation

Controlled pilots do not carry a 24x7 SLA unless a signed proposal explicitly says otherwise. The business support window above is the default operating commitment for V1 pilots.

## Production support expectation

Do not promise a production SLA that is not operationally staffed. For first production customers, any specific response-time, availability, RPO/RTO or after-hours commitment must be written in the signed proposal and must match the monitoring, alerting, backup and recovery capabilities actually in service.

## Escalation path

- customer-facing problem → `comercial@litoraltrace.com`;
- P1/P0 runtime issue → primary operator + operational incident channel/runbook;
- suspected security/privacy event → preserve evidence, limit sensitive details and escalate as a security incident;
- recovery requirement → Disaster Recovery Runbook;
- commercial or scope dispute → signed proposal and applicable Terms;
- privacy/data-right request → privacy route documented in `PRIVACY_NOTICE_V1.md`.

## Information handling

Sensitive operational material must use the approved secure provisioning or incident-handling route rather than ordinary support messages. Public GitHub Issues must never contain customer secrets, credentials, private URLs, object keys or raw customer payloads.

## Review trigger

Revisit this document before the first production customer, whenever support staffing changes, or before committing to a contractual SLA beyond the default V1 window.
