'use strict';
const escapeText = (value) =>
  String(value ?? '').replace(
    /[&<>"']/g,
    (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char],
  );
const usd = (value) =>
  new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number(value));
const customer = (row) => escapeText(row.customer?.fullName || `Customer ${row.customerId || ''}`);
const link = (kind, id, label) =>
  `<a href="https://secure.copilotcrm.com/${kind}/${encodeURIComponent(id)}" target="_blank" rel="noopener noreferrer">${escapeText(label)}</a>`;
const table = (headers, rows) =>
  rows.length
    ? `<table><thead><tr>${headers.map((value) => `<th>${escapeText(value)}</th>`).join('')}</tr></thead><tbody>${rows.map((row) => `<tr>${row.map((value) => `<td>${value}</td>`).join('')}</tr>`).join('')}</tbody></table>`
    : '<p class="muted">No records in this review.</p>';
async function get(path) {
  const token = localStorage.getItem('adminToken');
  const response = await fetch(path, { headers: { Authorization: `Bearer ${token || ''}` } });
  const data = await response.json();
  if (!response.ok || data.success === false)
    throw new Error(data.error || 'This Homeworks source is unavailable.');
  return data;
}
async function refresh() {
  const button = document.getElementById('refresh'),
    status = document.getElementById('status');
  button.disabled = true;
  status.textContent = 'Checking Homeworks…';
  try {
    const data = await get(
      '/api/homeworks/review?start_date=' + encodeURIComponent(document.getElementById('since').value),
    );
    document.getElementById('summary').innerHTML = [
      ['Unsent invoices', data.unsentInvoices.length],
      ['Unsent balance', usd(data.unsentBalance)],
      ['Open past visits', data.oldOpenVisits.length],
      ['Estimate changes', data.estimateChanges.length],
    ]
      .map(
        ([label, value]) =>
          `<div class="panel"><div class="metric">${escapeText(value)}</div><div class="muted">${escapeText(label)}</div></div>`,
      )
      .join('');
    document.getElementById('invoices').innerHTML = table(
      ['Customer', 'Invoice', 'Date', 'Balance', 'Contact'],
      data.unsentInvoices.map((row) => [
        customer(row),
        link('finances/invoices/view', row.id, '#' + row.number),
        escapeText(row.date),
        usd(Number(row.total) - Number(row.paidAmount)),
        escapeText(
          row.customer?.emailSendability === 'NO_EMAIL'
            ? 'No email on file'
            : row.customer?.email || 'Check contact',
        ),
      ]),
    );
    document.getElementById('visits').innerHTML = table(
      ['Customer', 'Service', 'Date', 'Billing'],
      data.oldOpenVisits.map((row) => [
        customer(row),
        escapeText(row.title),
        escapeText(row.startDate),
        row.isInvoiceable ? 'Invoiceable' : 'Not invoiceable',
      ]),
    );
    document.getElementById('estimates').innerHTML = table(
      ['Customer', 'Estimate', 'Status', 'Requested changes'],
      data.estimateChanges.map((row) => [
        customer(row),
        link('finances/estimates/view', row.id, '#' + row.number),
        escapeText(row.status),
        escapeText(row.requestedChanges),
      ]),
    );
    document.getElementById('messages').innerHTML = table(
      ['Customer', 'Status', 'Date', 'Message'],
      data.failedMessages.map((row) => [
        customer(row),
        escapeText(row.status),
        escapeText(row.sentAt),
        escapeText(row.text),
      ]),
    );
    document.getElementById('updated').textContent = 'Checked ' + new Date(data.asOf).toLocaleString();
    status.textContent = 'Review ready.';
    status.className = '';
  } catch (error) {
    status.textContent = error.message;
    status.className = 'error';
    document.getElementById('summary').innerHTML = '';
    for (const id of ['invoices', 'visits', 'estimates', 'messages'])
      document.getElementById(id).textContent = 'Unavailable. Refresh when the connection is restored.';
  } finally {
    button.disabled = false;
  }
}
document.getElementById('refresh').addEventListener('click', refresh);
document.getElementById('snow-refresh').addEventListener('click', async () => {
  const box = document.getElementById('snow');
  box.textContent = 'Checking current estimates and dispatch lists…';
  try {
    const data = await get(
      '/api/homeworks/snow-readiness?season=' + encodeURIComponent(document.getElementById('season').value),
    );
    box.innerHTML =
      `<p>${data.acceptedPropertyCount} accepted properties in this review · ${data.currentTemplateCount} current-season dispatch lists</p><p class="muted">${escapeText(data.scope)}</p>` +
      table(
        ['Issue', 'Property', 'Estimate'],
        data.issues.map((row) => [
          escapeText(row.type.replace(/_/g, ' ')),
          escapeText(row.propertyId || ''),
          escapeText(row.number || row.service?.number || ''),
        ]),
      ) +
      `<h3>Prior-season lists</h3>` +
      table(
        ['List', 'Entries'],
        data.legacyTemplates.map((row) => [escapeText(row.name), escapeText(row.propertyCount)]),
      );
  } catch (error) {
    box.textContent = error.message;
  }
});
document.getElementById('agreement-refresh').addEventListener('click', async () => {
  const box = document.getElementById('agreements');
  box.textContent = 'Checking signed agreements…';
  try {
    const data = await get('/api/homeworks/agreements');
    box.innerHTML =
      table(
        ['Customer', 'Estimate', 'Signed', 'Recorded attachments'],
        data.agreements.map((row) => [
          escapeText(row.customer_name),
          escapeText(row.quote_number),
          escapeText(row.contract_signed_at),
          escapeText(
            (row.uploadStatus || []).length
              ? row.uploadStatus
                  .map(
                    (item) =>
                      'Property ' +
                      item.property_id +
                      ': ' +
                      item.status +
                      (item.error ? ' (' + item.error + ')' : ''),
                  )
                  .join('; ')
              : (row.attachments || []).filter((event) => event.type === 'snow_pdf_synced').length +
                  ' previous property uploads recorded',
          ),
        ]),
      ) + (data.limited ? '<p class="muted">Showing the latest 200 signed agreements.</p>' : '');
  } catch (error) {
    box.textContent = error.message;
  }
});
refresh();
