const INVOICE_FOLLOWUP_TEMPLATE = {
  name: 'Invoice Follow-Up',
  slug: 'invoice_followup_sms',
  category: 'invoices',
  channel: 'sms',
  sms_body: [
    "Hi {customer_first_name}! This is Theresa from Pappas & Co. Landscaping. I'm following up on your open {invoice_word}. The balance is " + '$' + "{balance_due}.",
    '{invoice_details}',
    'You can pay using the {payment_link_word} above or through Zelle at {company_email}.',
    "If you've already sent payment, please disregard this message. Thank you!",
  ].join('\n\n'),
  variables: ['customer_first_name', 'invoice_word', 'balance_due', 'invoice_details', 'payment_link_word', 'company_email'],
};
function invoiceDetails(preview) {
  return preview.invoices.map(row => 'Invoice #' + row.number + ': $' + Number(row.balance).toFixed(2) + '\n' + row.url).join('\n\n');
}
function renderInvoiceFollowupTemplate(preview) {
  const values = {
    customer_first_name: String(preview.customer_name || '').trim().split(/\s+/)[0] || 'there',
    invoice_word: preview.invoices.length === 1 ? 'invoice' : 'invoices',
    balance_due: Number(preview.balance).toFixed(2),
    invoice_details: invoiceDetails(preview),
    payment_link_word: preview.invoices.length === 1 ? 'link' : 'links',
    company_email: 'hello@pappaslandscaping.com',
  };
  return INVOICE_FOLLOWUP_TEMPLATE.sms_body.replace(/\{([a-z_]+)\}/g, (match, key) => values[key] ?? match);
}
module.exports = { INVOICE_FOLLOWUP_TEMPLATE, invoiceDetails, renderInvoiceFollowupTemplate };
