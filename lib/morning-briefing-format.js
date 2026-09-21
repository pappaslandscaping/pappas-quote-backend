const SMS_MESSAGE_LIMIT = 1600;
const SMS_SPLIT_TARGET = 1500;

const IMPORTANT_EMAIL_PATTERN = /\b(estimate accepted|new quote|quote request|customer repl(?:y|ied)|payment (?:failed|declined)|card declined|past due|chargeback|dispute|refund request|cancell(?:ed|ation)|undeliverable|delivery failed|action required|urgent|account (?:locked|suspended)|insurance claim|tax notice|payroll issue)\b/i;
const LOW_PRIORITY_EMAIL_PATTERN = /\b(dmarc|report domain:|newsletter|weekly digest|daily digest|promotion|sale|special offer|survey|receipt|order confirmation|shipping update|social media|security report)\b/i;
const AUTOMATED_SENDER_PATTERN = /(?:no-?reply|mailer-daemon|postmaster|notifications?|newsletter|marketing|support@google)/i;

function senderName(line) {
  const value = String(line || '').replace(/^From:\s*/i, '').trim();
  const quoted = value.match(/^['"]?([^<'"]+?)['"]?\s*</);
  if (quoted) return quoted[1].trim();
  const email = value.match(/^([^@\s]+)@/);
  return email ? email[1].replace(/[._-]+/g, ' ') : value;
}

function summarizeImportantEmail(block) {
  const lines = block.split('\n').map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return '';

  const text = lines.join(' ');
  if (LOW_PRIORITY_EMAIL_PATTERN.test(text) && !IMPORTANT_EMAIL_PATTERN.test(text)) return '';

  const fromLine = lines.find((line) => /^From:/i.test(line));
  const looksHuman = fromLine && !AUTOMATED_SENDER_PATTERN.test(fromLine);
  if (!IMPORTANT_EMAIL_PATTERN.test(text) && !looksHuman) return '';

  const subject = lines[0].replace(/^[📨📩📧]\s*/u, '').trim();
  const estimateMatch = subject.match(/^Estimate Accepted:\s*(.+)$/i);
  if (estimateMatch) return `• Estimate accepted — ${estimateMatch[1]}`;

  const from = senderName(fromLine);
  return `• ${subject}${from ? ` — ${from}` : ''}`;
}

function normalizeInboxSummary(value) {
  if (!value) return '';

  const cleaned = String(value)
    .replace(/\r\n/g, '\n')
    .split('\n')
    .filter((line) => !/^[\s━─—_-]{3,}$/u.test(line.trim()))
    .filter((line) => !/^(?:📨\s*)?GMAIL OVERNIGHT$/i.test(line.trim()))
    .filter((line) => !/^Total:\s*\d+\s+incoming emails?$/i.test(line.trim()))
    .join('\n')
    .trim();

  if (!cleaned) return '';
  const important = cleaned
    .split(/\n\s*\n/)
    .map(summarizeImportantEmail)
    .filter(Boolean);

  return important.length ? `📬 Important emails\n${important.join('\n')}` : '';
}

function formatMorningBriefing({ sections, dateLabel, gmailText = '', recipientName = 'Theresa' }) {
  const header = `Good morning, ${recipientName} ☀️\nPappas & Co. • ${dateLabel}`;
  const content = [
    sections.jobs,
    sections.deposit,
    sections.invoices,
    sections.stripe,
    normalizeInboxSummary(gmailText),
  ].filter(Boolean);

  return [header, ...content].join('\n\n').trim();
}

function splitSmsMessage(value) {
  const messages = [];
  let remaining = String(value || '').trim();
  while (remaining) {
    if (remaining.length <= SMS_MESSAGE_LIMIT) {
      messages.push(remaining);
      break;
    }
    let splitIndex = remaining.lastIndexOf('\n\n', SMS_SPLIT_TARGET);
    if (splitIndex < 500) splitIndex = remaining.lastIndexOf('\n', SMS_SPLIT_TARGET);
    if (splitIndex < 500) splitIndex = SMS_SPLIT_TARGET;
    messages.push(remaining.slice(0, splitIndex).trim());
    remaining = remaining.slice(splitIndex).trim();
  }
  return messages;
}

module.exports = { formatMorningBriefing, normalizeInboxSummary, splitSmsMessage };
