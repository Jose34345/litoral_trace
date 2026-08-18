# Litoral Trace V1 — Customer Onboarding Checklist

Use this checklist for controlled pilots and first production customers. A production customer additionally requires all applicable V1 go-live gates to be green.

## Commercial and ownership

- signed proposal identifies legal/customer entity, commercial owner and operational owner
- selected plan, pilot/production status, limits and price are explicit
- billing contact and support contact are recorded
- product boundaries and no-certification language are acknowledged

## Tenant and access

- create the customer organization/tenant through the reviewed admin workflow
- create only named users required for onboarding
- assign least-privilege roles
- require unique credentials; never share demo credentials
- verify login, refresh and logout for an ordinary customer user
- verify a user from another tenant cannot read the customer tenant

## Data intake

- receive the customer's representative XLSX through the agreed secure channel
- preserve the customer's original source file outside any transformation step
- validate required fields and mapping before import
- import a controlled first batch
- reconcile imported lot count and identifiers with the customer's source
- document rejected rows or data-quality gaps rather than silently correcting business data

## Geospatial and satellite workflow

- review representative lot coordinates/polygons with the customer
- confirm the customer, not Litoral Trace, is the authoritative source for supplied origin data
- run representative satellite analyses where the workflow calls for them
- explain the output as evidence/analysis rather than legal proof or certification

## Vault and evidence

- load representative evidence documents
- verify each document is linked to the intended tenant/workflow object
- verify download/read access using an authorized customer role
- verify unavailable/deleted lifecycle states behave as expected

## Audit and closeout

- confirm relevant actions appear in the audit/evidence trail
- walk the operational owner through XLSX → lot → geolocation → satellite → Vault → evidence/audit
- record open data-quality or workflow issues
- confirm support/escalation route
- agree pilot review date or production go-live date

## Production-only final gate

Do not declare the customer production-live until the release candidate, disaster-recovery acceptance, operational alerting, backup/recovery requirements and production infrastructure gates applicable to Litoral Trace V1 are documented as PASS.