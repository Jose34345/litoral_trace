# US GTM Weekend — verified progress

**Issue:** #137  
**Experiment branch:** `experiment/us-lacey-gtm-weekend`  
**Draft PR:** #138  
**Rule:** a checkbox is green only when its artifact/evidence exists. Market-response tasks remain open until real people respond.

## Authorization
- [x] U1. User explicitly authorized automatic execution.
- [x] U2. Public deployment approved and promoted to an isolated Render Free service.
- [ ] U3. Outreach sent from user-owned email/LinkedIn identity.
- [ ] U4. Founder voice/video recording if used.
- [ ] U5. Real discovery calls.

## Guardrails
- [x] 01. No full Litoral Trace translation.
- [x] 02. No ACE/LAWGS filing feature built.
- [x] 03. No “Lacey compliant”, automated-filing or legal-advice claim in landing contract.
- [x] 04. No production Assurance rules modified.
- [x] 05. No paid ads purchased.
- [x] 06. No mass/non-personalized outreach sent.
- [x] 07. No Lacey product feature built without customer evidence.

## Cycle 1 — Positioning and message
- [x] 08. English category defined: `Document intelligence for Lacey Act readiness`.
- [x] 09. Pain-led hero defined.
- [x] 10. Extract / Reconcile / Evidence defined.
- [x] 11. Single CTA: `Test one completed shipment`.
- [x] 12. Regulatory/commercial disclaimers defined.

**Evidence:** `LANDING_COPY_V1.md`.

## Cycle 2 — Landing architecture
- [x] 13. One-page structure specified.
- [x] 14. Desktop/mobile hierarchy specified.
- [x] 15. LT visual language reused without product navigation.
- [x] 16. Evidence-review mockup designed without false filing claims.

**Evidence:** `LANDING_UI_SPEC.md`.

## Cycle 3 — Isolated implementation
- [x] 17. Separate branch created.
- [x] 18. English `/lacey` microsite implemented via standalone ASGI app.
- [x] 19. Main production/Assurance app left unchanged.
- [x] 20. Responsive and accessibility contracts implemented and tested in CI.

**Evidence:** `lacey_gtm.py`, `lacey_experiment_app.py`, `public/lacey.html`, `test_lacey_gtm_landing.py`.

## Cycle 4 — Conversion
- [x] 21. Five-field form implemented.
- [x] 22. Yes / Maybe / No historical-shipment field implemented.
- [x] 23. Thank-you state + 15-minute email handoff implemented.
- [x] 24. Aggregate funnel tracking implemented without PII labels.

**Evidence:** landing + event endpoint + CI tests.

## Cycle 5 — Commercial/regulatory QA
- [x] 25. Copy audited against prohibited claims.
- [x] 26. ACE/LAWGS described only as downstream filing destinations, not live integrations.
- [x] 27. Beta explicitly framed as preparation/evidence software.
- [x] 28. Real-browser desktop/mobile visual QA + five-field form flow verified for the deployed beta build.

**Evidence:** public Render service `https://litoral-trace-us-lacey-beta.onrender.com/lacey` confirmed Live; user-visible desktop render confirmed; Lacey Visual QA #2 passed at 1440x1000 and 390x844 with no horizontal overflow, CTA-to-form navigation, five usable fields and valid completed-form state; Lacey Preview Smoke #8 passed; CI #830 passed.

## Cycle 6 — 30 real prospects
- [x] 29. 15 customs-broker prospects selected with public Lacey/PGA/wood/furniture evidence.
- [x] 30. 15 importer/trade-compliance prospects selected with public sourcing/Lacey evidence.
- [x] 31. Company/role/contact/channel/fit/source recorded.
- [x] 32. Generic contacts without concrete fit excluded from the 30.

**Evidence:** `PROSPECTS_V1.csv`.

## Cycle 7 — Outbound batch 1
- [x] 33. Broker message created.
- [x] 34. Importer/trade-compliance variant created.
- [x] 35. One-line follow-up created.
- [x] 36. First 10 messages personalized by company workflow evidence.

**Evidence:** `OUTBOUND_V1.md`.

## Cycle 8 — Batch 2 and channels
- [x] 37. Prospects 11–30 personalized.
- [x] 38. Email / phone / LinkedIn / web-form channel classified.
- [x] 39. Contact order prioritized by learning/reachability.
- [x] 40. Pipeline status model created (`Not contacted`, replies/call/shipment etc.).

**Evidence:** `OUTBOUND_V1.md`, `PROSPECTS_V1.csv`, `VALIDATION_SCORECARD.csv`.

## Cycle 9 — Community discovery
- [x] 41. Genuine non-spam broker-community question prepared.
- [x] 42. Bottleneck options cover species/harvest country, re-keying, reconciliation, follow-up and evidence.
- [x] 43. Reply-to-interview follow-ups prepared.

**Evidence:** `COMMUNITY_DISCOVERY.md`. Posting remains a user-identity action, not falsely marked as sent.

## Cycle 10 — 90-second demo
- [x] 44. English storyboard created.
- [x] 45. 60–90 second voice/caption script created.
- [x] 46. Demo restricted to real LT capabilities with Lacey-specific fields framed as beta targets.
- [x] 47. Final CTA defined for 3 U.S. companies.

**Evidence:** `DEMO_90S_SCRIPT.md`. Recording itself is optional/user-presence work.

## Cycle 11 — Validation board
- [x] 48. Commercial metrics defined.
- [x] 49. Vanity metrics separated from demand signals.
- [x] 50. Objection + literal pain-statement fields created.
- [x] 51. GREEN/YELLOW/RED rules defined before seeing results.

**Evidence:** `VALIDATION_SCORECARD.csv`, `GTM_DECISION_RULES.md`.

## Cycle 12 — Evidence-driven iteration
- [ ] 52. Review first real responses/behavior.
- [ ] 53. Identify best-performing qualified message from actual evidence.
- [ ] 54. Change hero/CTA only if actual evidence warrants it.
- [ ] 55. Prepare Monday v2 using weekend response data.

**Prepared but not counted as completion:** `MONDAY_EXECUTION.md` contains the baseline Wave A runbook. A data-driven v2 cannot exist until there are real responses.

## Current gate

**Technical/marketing preparation:** complete for outreach.  
**Public preview:** LIVE on isolated Render service.  
**Custom domain:** pending DNS connection for `lacey.litoraltrace.com`.  
**Real outbound/community posts:** pending U3 / identity actions.  
**Market validation:** NOT YET SCORED — no real conversations have occurred yet.
