/* Presentation-only preference. Does not modify auth, tenancy or workspace data. */
(() => {
  const shell = document.querySelector('.lt-app-shell');
  const toggle = document.querySelector('[data-app-sidebar-toggle]');
  if (!shell || !toggle) return;
  const key = 'litoraltrace.sidebar.collapsed.v1';
  const icon = toggle.querySelector('i');
  document.querySelectorAll('.lt-sidebar .lt-nav-link').forEach((link) => {
    const label = link.querySelector('span:not(.lt-nav-icon)')?.textContent?.trim();
    if (label) { link.setAttribute('aria-label', label); link.setAttribute('title', label); }
  });
  const apply = (collapsed) => {
    shell.dataset.sidebarCollapsed = collapsed ? 'true' : 'false';
    toggle.setAttribute('aria-expanded', String(!collapsed));
    toggle.setAttribute('aria-label', collapsed ? 'Expand navigation' : 'Collapse navigation');
    toggle.setAttribute('title', collapsed ? 'Expand navigation' : 'Collapse navigation');
    if (icon) {
      icon.classList.toggle('fa-angles-right', collapsed);
      icon.classList.toggle('fa-angles-left', !collapsed);
    }
  };
  let collapsed = false;
  try { collapsed = window.localStorage.getItem(key) === 'true'; } catch (_) {}
  apply(collapsed);
  toggle.addEventListener('click', () => {
    collapsed = !collapsed;
    apply(collapsed);
    try { window.localStorage.setItem(key, String(collapsed)); } catch (_) {}
  });
})();
