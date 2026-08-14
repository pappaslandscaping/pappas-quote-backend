const fs = require('fs');
const path = require('path');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const communicationsHtml = fs.readFileSync(path.join(__dirname, '..', 'public', 'communications.html'), 'utf8');

function routeSource(startMarker, endMarker) {
  const start = serverSource.indexOf(startMarker);
  expect(start).toBeGreaterThan(-1);
  const end = serverSource.indexOf(endMarker, start);
  expect(end).toBeGreaterThan(start);
  return serverSource.slice(start, end);
}

describe('TwilioConnect app communication search and history', () => {
  test('message conversations support server-side search and are not capped at 100', () => {
    const source = routeSource(
      "app.get('/api/app/messages/conversations'",
      "// Get messages for a specific conversation",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim();');
    expect(source).toContain('matching_conversations AS');
    expect(source).toContain('m.body ILIKE');
    expect(source).toContain('c.name ILIKE');
    expect(source).toContain('LIMIT $');
    expect(source).not.toContain('LIMIT 100');
  });

  test('message threads return full conversation history and support search', () => {
    const source = routeSource(
      "app.get('/api/app/messages/thread/:phoneNumber'",
      "// AI reply suggestion",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim();');
    expect(source).toContain('body ILIKE');
    expect(source).toContain('ORDER BY created_at ASC');
    expect(source).not.toContain('LIMIT 100');
  });

  test('voicemail endpoint requests larger history and applies search before returning', () => {
    const source = routeSource(
      "app.get('/api/app/voicemails'",
      "app.post('/api/app/voicemails/:id/play'",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim().toLowerCase();');
    expect(source).toContain('limit=${voicemailLimit}');
    expect(source).toContain('filteredVoicemails');
    expect(source).toContain('transcription');
    expect(source).not.toContain('limit=100');
  });
});

describe('YardDesk manual invoice text composer', () => {
  test('is visible on communications.html and cannot send without manual review', () => {
    expect(communicationsHtml).toContain('Use Invoice Reminder Details');
    expect(communicationsHtml).toContain('Help Me Write It with AI');
    expect(communicationsHtml).toContain('I reviewed the customer, confirmed the phone number, past-due balance, invoice link, and message.');
    expect(communicationsHtml).toContain("if (!document.getElementById('sms-reviewed').checked) return;");
    expect(communicationsHtml).toContain('confirm_send: true');
    expect(communicationsHtml).toContain('It has no automatic or scheduled sending.');
  });

  test('AI only writes into the editable composer and never invokes the send endpoint', () => {
    expect(communicationsHtml).toContain("fetch('/api/app/ai/draft'");
    expect(communicationsHtml).toContain("document.getElementById('sms-compose-body').value = draft;");
    expect(communicationsHtml).toContain('AI draft prepared. Nothing was sent.');
  });
});


describe('YardDesk invoice text selection', () => {
  test('shows every unpaid issued invoice without requiring an overdue date', () => {
    expect(communicationsHtml).toContain("balance > 0.005");
    expect(communicationsHtml).toContain("!['paid', 'void', 'draft', 'cancelled', 'canceled'].includes(status)");
    expect(communicationsHtml).not.toContain("status === 'overdue' ||");
    expect(communicationsHtml).toContain('No invoices with an outstanding balance were found.');
  });
});

describe('YardDesk customer balance context', () => {
  test('distinguishes total outstanding from the selected past-due invoice', () => {
    expect(communicationsHtml).toContain('invoiceTextPreview.total_outstanding');
    expect(communicationsHtml).toContain('Total outstanding:');
    expect(communicationsHtml).toContain('Currently past due:');
    expect(communicationsHtml).toContain("the customer's total outstanding balance is");
    expect(communicationsHtml).toContain('Clearly distinguish the total owed from the past-due portion.');
  });
});

describe('YardDesk invoice AI drafting', () => {
  test('can draft after preview even before the invoice link is pasted', () => {
    expect(communicationsHtml).toContain("document.getElementById('sms-ai-btn').disabled = false;");
    expect(communicationsHtml).not.toContain("if (!invoiceUrl) return alert('Paste the Copilot invoice link first.')");
    expect(communicationsHtml).toContain("invoiceUrl ? \` Include this exact link unchanged:");
    expect(communicationsHtml).toContain('AI draft prepared. Paste the Copilot invoice link before sending. Nothing was sent.');
  });
});

describe('YardDesk invoice phone fallback', () => {
  test('allows a reviewed phone number when the imported invoice is not linked', () => {
    expect(communicationsHtml).toContain('Customer phone number');
    expect(communicationsHtml).toContain('optional if YardDesk already has it');
    expect(communicationsHtml).toContain("phone: document.getElementById('sms-phone').value.trim()");
  });
});

describe('YardDesk current Copilot invoice data', () => {
  test('refreshes Copilot before listing invoice text choices', () => {
    expect(communicationsHtml).toContain("fetch('/api/copilot/invoices/sync'");
    expect(communicationsHtml).toContain('maxPages: 25');
    expect(communicationsHtml).toContain("detailMode: 'missing'");
    expect(communicationsHtml).toContain("fetch('/api/invoices?limit=25000')");
    expect(communicationsHtml).toContain("'Authorization': `Bearer ${token}`");
  });
});

describe('YardDesk invoice text search', () => {
  test('requires a short search and filters by customer, invoice number, or amount', () => {
    expect(communicationsHtml).toContain('Find an outstanding invoice');
    expect(communicationsHtml).toContain('Type Kevin Hopp, an invoice number, or an amount');
    expect(communicationsHtml).toContain('if (query.length < 2)');
    expect(communicationsHtml).toContain('customer.includes(query) || number.includes(query) || amount.includes(query)');
    expect(communicationsHtml).toContain('.slice(0, 50)');
  });
});

describe('YardDesk general manual text composer', () => {
  test('supports customer texts about topics beyond invoices', () => {
    expect(communicationsHtml).toContain('New Text');
    expect(communicationsHtml).toContain('General message');
    expect(communicationsHtml).toContain('Scheduling or arrival time');
    expect(communicationsHtml).toContain('Service update');
    expect(communicationsHtml).toContain('Quote follow-up');
    expect(communicationsHtml).toContain('Weather delay');
    expect(communicationsHtml).toContain('What do you want to say?');
  });

  test('AI drafts only and every text requires review and confirmation', () => {
    expect(communicationsHtml).toContain('AI draft prepared. Review and edit it. Nothing was sent.');
    expect(communicationsHtml).toContain("if (!generalTextRecipient || !document.getElementById('general-sms-reviewed').checked) return;");
    expect(communicationsHtml).toContain('I reviewed the customer, phone number, and message.');
    expect(communicationsHtml).toContain('Send this text to');
    expect(communicationsHtml).toContain('It has no automatic or scheduled sending.');
  });
});
