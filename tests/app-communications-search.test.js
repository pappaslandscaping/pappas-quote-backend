const fs = require('fs');
const path = require('path');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const communicationsHtml = fs.readFileSync(path.join(__dirname, '..', 'public', 'communications.html'), 'utf8');
const communicationsRouteSource = fs.readFileSync(path.join(__dirname, '..', 'routes', 'communications.js'), 'utf8');

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
    expect(source).toContain('if (searchTerm) {\n      query += `\n        LEFT JOIN customers c');
    expect(source).toContain('FROM homeworks_phone_matches');
    expect(source).not.toContain('lookupAppCustomerByPhone(conv.normalized_phone)');
    expect(source).not.toContain('UPDATE messages');
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


  test('seeds the requested invoice SMS template with account balance and invoice link fields', () => {
    expect(serverSource).toContain("'invoice_ready_sms'");
    expect(serverSource).toContain('Total account balance due: \\${customer_account_balance}');
    expect(serverSource).toContain('{invoices_links}');
    expect(serverSource).toContain("'customer_account_balance'");
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
    expect(communicationsHtml).toContain('I reviewed the recipient, phone number, permission to text, and message.');
    expect(communicationsHtml).toContain('Send this text to');
    expect(communicationsHtml).toContain('It has no automatic or scheduled sending.');
  });

  test('allows a valid standalone phone number without creating a customer', () => {
    expect(communicationsHtml).toContain('Customer name or standalone phone number');
    expect(communicationsHtml).toContain('Find or Use Number');
    expect(communicationsHtml).toContain("name: 'One-time recipient', phone, isOneTime: true");
    expect(communicationsHtml).toContain('This will not create a customer record.');
    expect(communicationsHtml).toContain('One-time number — no customer record will be created');
    expect(communicationsRouteSource).toContain("error: 'Enter a valid 10-digit US phone number'");
  });

  test('sends through the authenticated YardDesk SMS endpoint', () => {
    expect(communicationsHtml).toContain("fetch(`${API}/api/messages/send`");
    expect(communicationsHtml).toContain("'Authorization': `Bearer ${token}`");
    expect(serverSource).toContain('authenticateToken, twilioClient: twilioAppMessagingClient, smsReplyClient: twilioAppMessagingClient');
    expect(communicationsHtml).toContain("data.error || data.message || 'Text failed to send'");
  });

  test('loads saved SMS templates and routes invoice templates through verified invoice data', () => {
    expect(communicationsHtml).toContain('Text template');
    expect(communicationsHtml).toContain('Choose a saved SMS template...');
    expect(communicationsHtml).toContain("fetch('/api/templates')");
    expect(communicationsHtml).toContain("['sms', 'both'].includes(channel)");
    expect(communicationsHtml).toContain('applyGeneralSmsTemplate');
    expect(communicationsHtml).toContain('invoiceSmsTemplateVars');
  });
});


describe('YardDesk guided invoice AI rewriting', () => {
  test('accepts custom instructions and safe quick rewrite choices', () => {
    expect(communicationsHtml).toContain('Tell AI what you want to change');
    expect(communicationsHtml).toContain('Friendlier');
    expect(communicationsHtml).toContain('Shorter');
    expect(communicationsHtml).toContain('More Professional');
    expect(communicationsHtml).toContain('Polite but Firm');
    expect(communicationsHtml).toContain('applyInvoiceAiInstruction');
    expect(communicationsHtml).toContain('AI only rewrites the editable draft. It cannot approve or send the text.');
  });

});


describe('YardDesk invoice composer layout', () => {
  test('keeps invoice details, AI help, and the recipient summary compact and visible', () => {
    expect(communicationsHtml).toContain('invoice-lookup-grid');
    expect(communicationsHtml).toContain('invoice-contact-grid');
    expect(communicationsHtml).toContain('invoice-ai-panel');
    expect(communicationsHtml).toContain('invoice-ai-chips');
    expect(communicationsHtml).toContain('Refresh Customer Details');
    expect(communicationsHtml).toContain('#invoice-sms-modal .email-preview-panel');
    expect(communicationsHtml).toContain('position: sticky');
  });
});

