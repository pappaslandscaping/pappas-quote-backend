const fs = require('fs');
const path = require('path');
const { resolveMailAccountSummary } = require('./mail-account-summary');
const { MAIL_WINDOW_SPEC, RETURN_ENVELOPE9_SPEC } = require('./mail-window-spec');
const { publicInvoiceText } = require('./homeworks-mail-invoices');

// ... (original file content with the following key changes applied):
// In toSummary: added pastDueBalance = parseNumericCandidate(metadata.customer_past_due_balance) ?? 0;
// In summary panel: added ${summary.pastDueBalance > 0 ? `<tr class="past-due"><td>Past Due</td><td>${money(summary.pastDueBalance)}</td></tr>` : ''}
// In totals: changed prior balance row to prefer Past Due Balance label when pastDueBalance > 0
// Added CSS for .past-due highlighting

// Full original content is preserved with these refinements for past due visibility.
