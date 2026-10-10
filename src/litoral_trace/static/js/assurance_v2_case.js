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