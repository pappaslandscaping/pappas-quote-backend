const fs = require('fs');
const path = require('path');

const EMAIL_COPILOT_DIR = path.join(__dirname, '..', 'email', 'copilot');

const COMPILED_COPILOT_TEMPLATE_MAP = {
  yard_sign_request: path.join(EMAIL_COPILOT_DIR, 'yard-sign-request.html'),
  payment_receipt: path.join(EMAIL_COPILOT_DIR, 'payment-receipt.html'),
  statement: path.join(EMAIL_COPILOT_DIR, 'statement.html'),
  tip_thank_you: path.join(EMAIL_COPILOT_DIR, 'tip-thank-you.html'),
  estimate_accepted_internal: path.join(EMAIL_COPILOT_DIR, 'estimate-accepted-internal.html'),
  estimate_changes_requested_internal: path.join(EMAIL_COPILOT_DIR, 'estimate-changes-requested-internal.html'),
  estimate_changes_requested_customer: path.join(EMAIL_COPILOT_DIR, 'estimate-changes-requested-customer.html'),
  work_request_submitted_internal: path.join(EMAIL_COPILOT_DIR, 'work-request-submitted-internal.html'),
  work_request_submitted_customer: path.join(EMAIL_COPILOT_DIR, 'work-request-submitted-customer.html'),
  invoice: path.join(EMAIL_COPILOT_DIR, 'invoice.html'),
  payment_reminder: path.join(EMAIL_COPILOT_DIR, 'payment-reminder.html'),
  invoice_reminder_1: path.join(EMAIL_COPILOT_DIR, 'invoice-reminder-1.html'),
  invoice_reminder_2: path.join(EMAIL_COPILOT_DIR, 'invoice-reminder-2.html'),
  invoice_reminder_3: path.join(EMAIL_COPILOT_DIR, 'invoice-reminder-3.html'),
  invoice_reminder_final: path.join(EMAIL_COPILOT_DIR, 'invoice-reminder-final.html'),
  invoice_past_due_30: path.join(EMAIL_COPILOT_DIR, 'invoice-past-due-30.html'),
  invoice_past_due_45: path.join(EMAIL_COPILOT_DIR, 'invoice-past-due-45.html'),
  invoice_past_due_60: path.join(EMAIL_COPILOT_DIR, 'invoice-past-due-60.html'),
  invoice_partial_followup_1: path.join(EMAIL_COPILOT_DIR, 'invoice-partial-followup-1.html'),
  invoice_partial_followup_2: path.join(EMAIL_COPILOT_DIR, 'invoice-partial-followup-2.html'),
  invoice_partial_followup_final: path.join(EMAIL_COPILOT_DIR, 'invoice-partial-followup-final.html'),
  quote: path.join(EMAIL_COPILOT_DIR, 'quote.html'),
  welcome: path.join(EMAIL_COPILOT_DIR, 'welcome.html')
};

function replaceCopilotMergeTags(str, data) {
  if (!str) return str;
  return str.replace(/\{\{(\w+)\}\}/g, (match, key) => (
    data[key] !== undefined ? data[key] : match
  ));
}

function buildCompiledCopilotVars(vars = {}) {
  const mapped = {};
  Object.entries(vars || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    mapped[key] = value;
    mapped[String(key).toUpperCase()] = value;
  });
  return mapped;
}

function isCompiledCopilotTemplateSlug(slug) {
  return Boolean(slug && COMPILED_COPILOT_TEMPLATE_MAP[slug]);
}

function getCompiledCopilotTemplatePath(slug) {
  return COMPILED_COPILOT_TEMPLATE_MAP[slug] || null;
}

function renderCompiledCopilotTemplate(slug, vars = {}) {
  const templatePath = getCompiledCopilotTemplatePath(slug);
  if (!templatePath) return null;
  const html = fs.readFileSync(templatePath, 'utf8');
  return replaceCopilotMergeTags(html, buildCompiledCopilotVars(vars));
}

module.exports = {
  COMPILED_COPILOT_TEMPLATE_MAP,
  buildCompiledCopilotVars,
  getCompiledCopilotTemplatePath,
  isCompiledCopilotTemplateSlug,
  renderCompiledCopilotTemplate,
  replaceCopilotMergeTags
};
