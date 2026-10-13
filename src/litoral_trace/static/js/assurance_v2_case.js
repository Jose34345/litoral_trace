(() => {
  'use strict';
  const root = document.querySelector('[data-assurance-case]');
  if (!root) return;
  const op = encodeURIComponent(root.dataset.operation);
  const csrf_token = root.dataset.csrf;
  const base = '/assurance-v2/operations/' + op;
  async function post(path, payload) {
    const response = await fetch(base + path, {
      method:'POST', credentials:'same-origin',
      headers:{'Content-Type':'application/json', 'Accept':'application/json'},
      body:JSON.stringify({...payload, csrf_token})
    });
    const data = await response.json();
    if (!response.ok) throw new Error((data.detail || (data.blockers || []).map(x => x.code).join(', ') || 'Request failed'));
    return data;
  }
  // Passive UX timing: browser-observed focused intervals only. No field
  // values, email, raw documents or browsing history enter telemetry.
  // This is not a legally authoritative timer or a time-savings estimate.
  function watchReviewAttention(form) {
    let elapsedMs = 0;
    let startedAt = null;
    const stop = () => {
      if (startedAt !== null) {
        elapsedMs += Math.max(0, performance.now() - startedAt);
        startedAt = null;
      }
    };
    const start = () => {
      if (!document.hidden && document.hasFocus() && startedAt === null) {
        startedAt = performance.now();
      }
    };
    form.addEventListener('focusin', start);
    form.addEventListener('focusout', stop);
    form.addEventListener('pointerdown', start);
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) stop(); else if (form.contains(document.activeElement)) start();
    });
    window.addEventListener('blur', stop);
    return () => {
      stop();
      return Math.max(0, Math.min(3600, Math.round(elapsedMs / 1000)));
    };
  }
  const botanicalForm = document.querySelector('[data-botanical-batch]');
  const botanicalObservedSeconds = botanicalForm ? watchReviewAttention(botanicalForm) : null;
  if (botanicalForm) botanicalForm.addEventListener('submit', async event => {
    event.preventDefault();
    const result = botanicalForm.querySelector('.botanical-batch-result');
    const button = botanicalForm.querySelector('button[type="submit"]');
    const selected = [...botanicalForm.querySelectorAll('.botanical-batch-checkbox:checked')]
      .map(c => ({
        memory_id: c.dataset.memoryId,
        field_name: c.dataset.fieldName,
        line_reference: c.dataset.line,
      }));
    const reason = botanicalForm.querySelector('[name="reason"]')?.value.trim() || '';
    const confirmed = botanicalForm.querySelector('[name="explicit_confirmation"]')?.checked === true;
    if (!selected.length || !confirmed || reason.length < 6) {
      result.textContent = 'Select the verified claims, inspect each original source and explicitly confirm the applicable origin.';
      return;
    }
    button.disabled = true;
    result.textContent = 'Rechecking original document integrity, authority and validity…';
    try {
      const receipt = await post('/botanical-batch', {
        selected, reason, explicit_confirmation: true,
        active_review_seconds: botanicalObservedSeconds(),
      });
      if (receipt.status !== 'RECORDED' || receipt.authorized_count !== selected.length) {
        throw new Error('Historical evidence was not verified');
      }
      window.location.reload();
    } catch (error) {
      result.textContent = 'No historical evidence was authorized: ' + error.message;
      button.disabled = false;
    }
  });
  const batchForm = document.querySelector('[data-batch-review]');
  const batchObservedSeconds = batchForm ? watchReviewAttention(batchForm) : null;
  // Selection is never approval. Reviewers may mark inspected sources together,
  // but the existing explicit consent, comment and locked authority writer remain.
  if (batchForm) {
    const selectAll = batchForm.querySelector('[data-select-inspected]');
    const checks = [...batchForm.querySelectorAll('.batch-review-checkbox:not(:disabled)')];
    const counter = batchForm.querySelector('[data-batch-selected-count]');
    const updateCount = () => {
      const selected = checks.filter(box => box.checked).length;
      if (counter) counter.textContent = selected + ' of ' + checks.length + ' eligible values selected for explicit review';
      if (selectAll) {
        selectAll.checked = checks.length > 0 && selected === checks.length;
        selectAll.indeterminate = selected > 0 && selected < checks.length;
      }
    };
    if (selectAll) selectAll.addEventListener('change', () => {
      checks.forEach(box => { box.checked = selectAll.checked; });
      updateCount();
    });
    checks.forEach(box => box.addEventListener('change', updateCount));
    updateCount();
  }
  if (batchForm) batchForm.addEventListener('submit', async event => {
    event.preventDefault();
    const status = batchForm.querySelector('.batch-review-result');
    const button = batchForm.querySelector('button[type="submit"]');
    const selected = [...batchForm.querySelectorAll('.batch-review-checkbox:checked')].map(c => ({
      field_id: Number(c.value), claim_id: c.dataset.claimId,
    }));
    const reason = batchForm.querySelector('[name="reason"]')?.value.trim() || '';
    const confirmed = batchForm.querySelector('[name="explicit_confirmation"]')?.checked === true;
    if (!selected.length || !confirmed || reason.length < 6) {
      status.textContent = 'Select source-backed values, inspect their PDFs, and explicitly confirm your review.';
      return;
    }
    button.disabled = true;
    status.textContent = 'Validating selected document evidence and recording the entire review atomically…';
    try {
      const receipt = await post('/batch-review', {
        selected, reason, explicit_confirmation: true,
        active_review_seconds: batchObservedSeconds(),
      });
      if (receipt.status !== 'RECORDED' || receipt.accepted_count !== selected.length) {
        throw new Error('Batch review was not confirmed');
      }
      window.location.reload();
    } catch (error) {
      status.textContent = 'Nothing approved: ' + error.message;
      button.disabled = false;
    }
  });
  document.querySelectorAll('.decision-form').forEach(form => {
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const result = form.querySelector('.decision-result');
      result.textContent = 'Sending to authority service…';
      const controls = Object.fromEntries(new FormData(form).entries());
      try {
        const receipt = await post('/decisions', {...controls, exception_id:form.dataset.exception});
        result.textContent = 'Review command received: ' + (receipt.status || 'pending') + '. Refresh to see authoritative state.';
      } catch (error) { result.textContent = 'Not recorded: ' + error.message; }
    });
  });
  document.querySelectorAll('.reuse-form').forEach(form => {
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const result = form.querySelector('.reuse-result');
      const button = form.querySelector('button[type="submit"]');
      const reason = form.querySelector('[name="reason"]')?.value.trim() || '';
      if (!reason) { result.textContent = 'A reviewer reason is required.'; return; }
      button.disabled = true;
      result.textContent = 'Checking current documents and historical evidence…';
      try {
        const receipt = await post('/reuses', {
          memory_id: form.dataset.memory, field_name: form.dataset.field,
          line_reference: form.dataset.line, reason
        });
        if (receipt.status !== 'VERIFIED_REUSE') throw new Error('Reuse was not verified');
        window.location.reload();
      } catch (error) {
        result.textContent = 'Reuse not applied: ' + error.message;
        button.disabled = false;
      }
    });
  });
  document.querySelectorAll('.identity-form').forEach(form => {
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const result = form.querySelector('.identity-result');
      const button = form.querySelector('button[type="submit"]');
      const reason = form.querySelector('[name="reason"]')?.value.trim() || '';
      if (!reason) { result.textContent = 'A reviewer reason is required.'; return; }
      button.disabled = true;
      result.textContent = 'Checking current supplier, SKU and line…';
      try {
        const receipt = await post('/identities', {
          line_reference: form.dataset.line, supplier_id: Number(form.dataset.supplier),
          product_id: Number(form.dataset.product), reason
        });
        if (receipt.status !== 'HUMAN_CONFIRMED') throw new Error('Identity was not confirmed');
        window.location.reload();
      } catch (error) {
        result.textContent = 'Identity not confirmed: ' + error.message;
        button.disabled = false;
      }
    });
  });
  const authorizationForm = document.querySelector('#authorize-export-form');
  if (authorizationForm) authorizationForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const status = authorizationForm.querySelector('.authorization-result');
    const button = authorizationForm.querySelector('button[type="submit"]');
    const confirmed = authorizationForm.querySelector('[name="explicit_confirmation"]')?.checked === true;
    const reason = authorizationForm.querySelector('[name="reason"]')?.value.trim() || '';
    if (!confirmed || !reason) {
      status.textContent = 'Explicit confirmation and a reason are required.';
      return;
    }
    button.disabled = true;
    status.textContent = 'Recording your authorization…';
    try {
      const result = await post('/authorize-export', {reason, explicit_confirmation: true});
      if (result.status !== 'AUTHORIZED') throw new Error('Authorization not confirmed');
      // Always re-read the case from the server. No browser-local
      // authority is inferred from the receipt alone.
      window.location.reload();
    } catch (error) {
      status.textContent = 'Authorization not recorded: ' + error.message;
      button.disabled = false;
    }
  });
  const button = document.querySelector('#issue-package');
  if (button) button.addEventListener('click', async () => {
    const result = document.querySelector('#package-result');
    button.disabled = true;
    try {
      const payload = await post('/issue', {});
      const fp = payload.package_fingerprint;
      result.textContent = 'Package persisted · ' + fp + ' · ';
      for (const [kind,label] of [['manifest','Manifest'],['ppq505_preparation_json','PPQ 505 preparation'],['lacey_excel','Excel'],['lawgs_xml','LAWGS XML']]) {
        const a = document.createElement('a');
        a.href = base + '/packages/' + fp + '/' + kind;
        a.textContent = label;
        a.style.marginRight = '12px';
        result.appendChild(a);
      }
    } catch(error) { result.textContent = error.message; }
    finally { button.disabled = false; }
  });
})();


/* Task links reveal only the requested evidence section (and exception).
   A long audit ledger must never open automatically as a wall of records. */
function revealEvidenceAnchor(id) {
  const target = document.getElementById(id);
  if (!target) return false;
  for (let node = target; node; node = node.parentElement) {
    if (node.tagName === 'DETAILS') node.open = true;
  }
  return true;
}

document.addEventListener('click', function (event) {
  const anchor = event.target.closest("a[href^='#']");
  if (!anchor) return;
  const id = anchor.getAttribute('href').slice(1);
  if (id) revealEvidenceAnchor(id);
});

function revealEvidenceHash() {
  const raw = window.location.hash.slice(1);
  if (!raw) return;
  let id;
  try { id = decodeURIComponent(raw); }
  catch (_) { return; }
  if (revealEvidenceAnchor(id)) {
    // Native hash scrolling sometimes precedes opening hidden ancestors.
    requestAnimationFrame(() => document.getElementById(id)?.scrollIntoView({block:'start'}));
  }
}
window.addEventListener('hashchange', revealEvidenceHash);
revealEvidenceHash();
