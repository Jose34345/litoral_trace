# U.S. Lacey GTM — decision rules

## North-star evidence

The experiment does not validate demand through page views, likes or polite replies. The strongest signal is a qualified professional agreeing to walk through one completed historical shipment/entry and exposing the manual workflow.

## Funnel metrics

### Commercial signals
1. Qualified prospects contacted
2. Meaningful replies
3. Qualified conversations
4. 15-minute calls accepted
5. Historical shipment willingness
6. Historical shipment actually reviewed

### Diagnostic web signals
- landing visits
- CTA clicks
- form starts
- form submits

Web signals help diagnose copy/conversion but do not substitute for commercial evidence.

## Qualified conversation definition

Count only a person with direct exposure to Lacey/trade-compliance operations who discusses at least one real workflow step: supplier-data collection, species/harvest-country review, re-keying, reconciliation, filing preparation, or evidence retention.

Do not count:
- auto-replies;
- generic sales teams forwarding the message;
- “interesting product” without workflow detail;
- social likes/upvotes;
- a person outside import/compliance who cannot describe the process.

## Pain score

- **0:** no relevant manual pain.
- **1:** rare inconvenience.
- **2:** repeated but small/manual work.
- **3:** material recurring follow-up/re-keying/reconciliation.
- **4:** acute operational pain, high volume, delays, dedicated staff or meaningful risk/cost.

## Sprint decision

### GREEN
All of the following:
- >=2 qualified conversations;
- >=1 broker/importer says Yes to reviewing a completed historical shipment;
- at least one pain statement scores 3–4 and maps to a capability LT could plausibly automate.

**Action:** review the historical shipment before writing a Lacey product backlog.

### YELLOW
Examples:
- qualified people recognize repeated pain but nobody shares a historical shipment;
- calls happen but trust/data-sharing is the blocker;
- pain is real but primarily outside document intake/reconciliation;
- landing gets qualified engagement but CTA creates hesitation.

**Action:** diagnose trust, offer framing and ICP. Do not build a full module yet.

### RED
After approximately 40–50 genuinely qualified contacts:
- no qualified calls, or
- professionals consistently say the document workflow is already structured/automated and low-friction, or
- the pain exists but buyers do not value solving it.

**Action:** stop the Lacey vertical build and reallocate effort. Do not reinterpret weak signals as product-market fit.

## Product-build gate after a shipment is obtained

Before opening a Lacey implementation sprint, document:
1. exact source files used in the historical entry;
2. every field manually copied or requested;
3. missing/conflicting values and where they were resolved;
4. time spent by broker/importer/supplier;
5. which evidence had to be retained;
6. which needs repeat across >=3 independent organizations.

Only repeated needs become roadmap candidates.

## Minimum analytical report per call

Record:
- current workflow diagram;
- actors involved;
- files/tools used;
- time estimate;
- primary bottleneck;
- workaround;
- consequence of missing data;
- willingness to test;
- exact phrases used by the prospect.

Literal prospect language should feed future landing copy. Do not overwrite it with internal marketing terminology.
