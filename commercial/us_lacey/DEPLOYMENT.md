# U.S. Lacey beta deployment

## Public beta

- URL: `https://litoral-trace-us-lacey-beta.onrender.com/lacey`
- Host: Render
- Plan: Free
- Region: Virginia
- Code branch: `experiment/us-lacey-gtm-weekend`
- Blueprint branch: `deploy/us-lacey-render`
- Runtime: isolated FastAPI/uvicorn microsite
- Database: none
- Production `litoraltrace.com`: unchanged
- Assurance staging: unchanged

## Verified gates

- Lacey Preview Smoke #8: PASS
- Lacey Visual QA #2: PASS
  - desktop viewport 1440x1000
  - mobile viewport 390x844
  - no horizontal overflow
  - CTA reaches private-beta form
  - five qualification fields usable
  - completed form passes browser validity
- CI #830: PASS

## Custom-domain target

Preferred public URL: `https://lacey.litoraltrace.com`

Do not guess the DNS target. In Render, add `lacey.litoraltrace.com` as a custom domain and use the exact DNS record Render returns. Keep the apex/root `litoraltrace.com` unchanged.
