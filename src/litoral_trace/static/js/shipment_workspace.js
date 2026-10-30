(() => {
  function updateShell() {
    const slot = document.querySelector('[data-package-applicability-slot]');
    const source = document.getElementById('shipment-applicability-slot');
    if (slot && source && source.parentElement !== slot) slot.append(source);
    let target; try { target=document.getElementById(decodeURIComponent(location.hash.slice(1))); } catch { target=null; }
    if (target) for (let p=target; p; p=p.parentElement) if (p.tagName === 'DETAILS') p.open=true;
  }
  document.addEventListener('click', async (event) => {
    const copy = event.target.closest('[data-copy-operation-id]');
    if (copy) { try { await navigator.clipboard.writeText(document.querySelector('[data-operation-id-value]').textContent); copy.textContent='Copied'; } catch { copy.textContent='Select the ID to copy'; } }
    if (event.target.closest('[data-open-shipment-audit]')) { const audit=document.querySelector('[data-unified-audit]'); if (audit) audit.open=true; }
  });
  document.addEventListener('htmx:beforeSwap', (event) => {
    if (event.detail.shouldSwap === false) return;
    const source=document.getElementById('shipment-applicability-slot');
    const staging=document.querySelector('[data-applicability-staging]');
    if (source && staging && event.detail.target?.contains(source)) staging.append(source);
  });
  document.addEventListener('htmx:afterSettle', updateShell);
  window.addEventListener('hashchange', updateShell);
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',updateShell);else updateShell();
})();