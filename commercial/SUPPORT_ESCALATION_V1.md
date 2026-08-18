# Litoral Trace V1 — Support and Escalation

## Customer contact route

Primary customer support address: `comercial@litoraltrace.com` until a dedicated support mailbox or ticket portal is introduced.

A proposal may nominate an additional named contact, but customers should have one documented route rather than relying on personal chat accounts.

## Severity model

### P0 — Critical

Examples: service broadly unavailable, confirmed tenant-isolation concern, unrecoverable production-data risk, or an incident requiring immediate containment.

Target handling: prioritize containment and recovery above feature work, preserve operational evidence, and use the relevant runbook.

### P1 — High

Examples: a major customer workflow is blocked, repeated Vault/storage failure, failed scheduled backup, sustained Satellite worker failure, or authentication unavailable for a customer organization.

Target handling: investigate with priority during the active support period and provide a workaround or next update when available.

### P2 — Normal

Examples: isolated workflow defect with workaround, data-import question, UX issue, configuration request, or non-critical report problem.

Target handling: queue for normal business support.

### P3 — Request / Improvement

Examples: new report, integration, UI improvement, or feature request. These are product requests rather than incidents and may be routed to the product backlog.

## Pilot support expectation

Unless a signed proposal states otherwise, controlled pilots do not carry a 24x7 SLA. The commercial proposal must state the available support window and any promised response targets before a customer relies on them.

## Production support expectation

Do not promise a production SLA that is not operationally staffed. For first production customers, any specific response-time or availability commitment must be written in the signed proposal and must match the monitoring, alerting, backup, and recovery capabilities actually in service.

## Escalation path

- customer-facing problem → primary support route
- P1/P0 runtime issue → operator incident channel and operational runbook
- suspected security/privacy event → preserve evidence, limit sensitive details, and escalate as a security incident
- recovery requirement → Disaster Recovery Runbook
- commercial or scope dispute → signed proposal and applicable Terms

## Information handling

Sensitive operational material must use the approved secure provisioning or incident-handling route rather than ordinary support messages.