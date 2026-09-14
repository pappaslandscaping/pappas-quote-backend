function normalizedMatchValue(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toLowerCase();
}

function matchesInvoiceCustomer(invoice, { customerName, customerEmail, copilotCustomerId } = {}) {
  const expectedName = normalizedMatchValue(customerName);
  const expectedEmail = normalizedMatchValue(customerEmail);
  const expectedId = normalizedMatchValue(copilotCustomerId);
  if (!expectedName && !expectedEmail && !expectedId) return true;

  return (
    (expectedId && normalizedMatchValue(invoice.copilot_customer_id) === expectedId) ||
    (expectedEmail && normalizedMatchValue(invoice.customer_email) === expectedEmail) ||
    (expectedName && normalizedMatchValue(invoice.customer_name) === expectedName)
  );
}

module.exports = { matchesInvoiceCustomer, normalizedMatchValue };
