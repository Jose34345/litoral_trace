# Render deployment — U.S. Lacey beta

The Lacey market-validation microsite is intentionally isolated from the authenticated Litoral Trace application.

## Target architecture

- Render service: `litoral-trace-us-lacey-beta`
- Service code branch: `experiment/us-lacey-gtm-weekend`
- Runtime: Python 3.11
- Plan: Free
- Region: Virginia
- Health check: `/health`
- Start command: `uvicorn litoral_trace.web.lacey_experiment_app:app --host 0.0.0.0 --port $PORT`
- Database: none
- S3/Vault: none
- JWT/authentication: none
- EUDR/Assurance runtime: none
- Auto-deploy: after CI checks pass

The canonical Blueprint is `deploy/render-lacey.yaml`.

## One-click provisioning branch

For Render's Deploy to Render flow, a dedicated branch named `deploy/us-lacey-render` mirrors only the Lacey Blueprint into its root `render.yaml`. This branch is infrastructure-only and MUST NOT be merged into production or `feature/assurance-v1`.

Deploy URL:

`https://render.com/deploy?repo=https://github.com/Jose34345/litoral_trace/tree/deploy/us-lacey-render`

Render will ask the authenticated workspace owner to review and approve creation of the Free web service. That approval cannot be performed safely from GitHub alone.

## First-deploy acceptance

Do not attach `lacey.litoraltrace.com` until all checks below pass on the generated `*.onrender.com` URL:

- `/health` returns HTTP 200 and `{"status":"healthy"}`.
- `/lacey` returns HTTP 200.
- desktop hero/CTA/form render correctly.
- mobile hero/CTA/form render correctly.
- the form opens the pre-filled email flow.
- no authenticated LT navigation appears.
- no production/Assurance credentials are configured.

After visual QA, add `lacey.litoraltrace.com` as a custom domain in Render and create only the DNS record Render requests.
