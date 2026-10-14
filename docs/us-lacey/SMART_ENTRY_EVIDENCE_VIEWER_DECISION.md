# Smart Entry evidence viewer decision

The existing authenticated source endpoint remains the only document-byte route.

The native browser iframe is not retained for this workflow: supported browsers do
not expose reliable, application-controlled zoom or page navigation for an embedded
PDF. The P0 acceptance criteria require real page navigation and bounded zoom, so
the application vendors PDF.js 4.10.38 locally through `npm run build:vendor`.

PDF.js receives only the object URL created from the one authorized, no-store fetch
made by the ephemeral inspection session. No PDF bytes, page data, or inspection
state is persisted in browser storage. Releasing the session destroys the PDF.js
document/render task and revokes the object URL.
