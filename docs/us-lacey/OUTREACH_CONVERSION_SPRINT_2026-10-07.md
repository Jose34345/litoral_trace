# Outreach Conversion Sprint — 2026-10-07

## Purpose
Increase qualified buyer conversations without asking unknown U.S. importers or customs brokers to upload sensitive documents on their first visit. Preserve the anonymous five-shipment evaluation and existing outreach referral URLs.

## New attributed demo links
New campaigns may append a *known*, fixed persona to an existing individual referral link:
- Importer: `https://lacey.litoraltrace.com/sandbox/ref/<slug>?demo=importer`
- Broker: `https://lacey.litoraltrace.com/sandbox/ref/<slug>?demo=broker`

Default `/sandbox/ref/<slug>` still opens `/sandbox/start`. Invalid persona values fall back to the original landing page. The first-party, HttpOnly, SameSite attribution cookie and its 7-day expiry stay unchanged. The URL contains no customer's private information.

Admin exposes all three per-prospect URLs, without modifying previous campaigns.

## Conversion sequence
1. Outreach email asks whether the business prepares Lacey documentation manually or delegates it to a broker. Do **not** force a link into the first email.
2. After interest, share the matched synthetic sample. This is read-only, does not use customer documents and does not consume an evaluation shipment.
3. Sample 1 demonstrates review exceptions; sample 2 demonstrates reusable evidence.
4. Offer a 15-minute **request by email**, `comercial@litoraltrace.com`; this is not an automated booking and is not counted as a booked meeting.
5. Uploading the customer's own shipment remains opt-in through `/sandbox/start`; privacy, consent, 4-hour raw retention and the five-shipment cap remain unchanged.
6. Only in a guided conversation, invite a company to test actual, appropriately authorized documents.

## Attribution and safety
- `LINK_OPENED` = raw HTTP hit, which may be a corporate scanner, never an email read.
- `HUMAN_VISIT` = browser visible for >=4 seconds or real interaction, with `navigator.webdriver !== true`; **likely** human only, not proof.
- `SAMPLE_STARTED`, `SAMPLE_REUSE_REACHED`, `SAMPLE_COMPLETED` = browser POST (not a speculative GET).
- Server ignores unknown personas and unexpected steps. No raw IP, user agent, document contents, supplier names or email addresses are written by this instrumentation.
- Telemetry failures fail open and never block the sample, first-party browsing or sandbox provisioning.
- Mailto links do not imply a meeting request was received: count accepted replies in the commercial mailbox manually.
- Allow for browser/assistant privacy controls and mail clients which suppress JavaScript or mailto handling. '0 signals' does not prove no person visited.

## Acceptance criteria
- Old URLs keep 303 -> `/sandbox/start`.
- Direct persona URLs keep 303 -> `/try/importer` or `/try/broker` and retain attribution.
- GET /try must never count a human/sample product engagement by itself.
- Browser POST with allowed persona/step creates only allowed idempotent funnel events.
- No cookie = no outreach attribution.
- No new tenant is created merely from opening a sample.
- No DB grant widening, migration, alteration to evaluation quotas or document retention.
- Tests green; CSS drift checked; production smoke GETs 200 on both sample pages and sandbox start.

## Next-day sales measurement (a 30-day test)
Separate by segment (importers vs brokers), campaign and person. Track targeted accounts, validated deliveries, human replies, positive discovery responses, booked consultations, sample engagement signals, and first customer-run shipment. Do not confuse raw link hits with read receipts or passive sample page GETs with human usage.

Use a three-touch sequence with an informed question first, sample proof second, and short conversation offer third. Honor opt-outs and comply with US commercial-email requirements. Reassess positioning after 100–150 verified target accounts; re-evaluate the segment after 300 multi-touch accounts. This is a decision framework, not a promise of conversions.
