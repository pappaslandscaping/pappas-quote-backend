const crypto = require('crypto');

function phoneDigits(value) {
  const digits = String(value || '').replace(/\D/g, '');
  if (digits.length === 10) return `1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return digits;
  return digits;
}

function toE164(value) {
  const digits = phoneDigits(value);
  return digits ? `+${digits}` : null;
}

function signatureFor(customerDigits, businessDigits, secret) {
  return crypto
    .createHmac('sha256', String(secret || ''))
    .update(`${customerDigits}:${businessDigits}`)
    .digest('hex')
    .slice(0, 16);
}

function buildSmsReplyAddress({ customerPhone, businessPhone, domain, secret }) {
  const customerDigits = phoneDigits(customerPhone);
  const businessDigits = phoneDigits(businessPhone);
  const cleanDomain = String(domain || '').trim().toLowerCase();
  if (!customerDigits || !businessDigits || !cleanDomain || !secret) return null;
  const sig = signatureFor(customerDigits, businessDigits, secret);
  return `sms+${customerDigits}.${businessDigits}.${sig}@${cleanDomain}`;
}

function extractEmailAddress(value) {
  const text = String(value || '').trim();
  const match = text.match(/<([^>]+)>/);
  return (match ? match[1] : text).trim().toLowerCase();
}

function parseSmsReplyTarget(addresses, secret) {
  const candidates = Array.isArray(addresses) ? addresses : [addresses];
  for (const candidate of candidates) {
    const address = extractEmailAddress(candidate);
    const local = address.split('@')[0] || '';
    const match = local.match(/^sms\+(\d{10,15})\.(\d{10,15})\.([a-f0-9]{16})$/i);
    if (!match) continue;
    const [, customerDigits, businessDigits, sig] = match;
    const expected = signatureFor(customerDigits, businessDigits, secret);
    if (sig.toLowerCase() !== expected) continue;
    return {
      to: `+${customerDigits}`,
      from: `+${businessDigits}`,
    };
  }
  return null;
}

function htmlToText(html) {
  return String(html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function extractReplyBody(text, html) {
  const raw = String(text || '').trim() || htmlToText(html);
  const lines = raw.replace(/\r\n/g, '\n').split('\n');
  const fresh = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (/^On .+wrote:$/i.test(trimmed)) break;
    if (/^-{2,}\s*Original Message\s*-{2,}$/i.test(trimmed)) break;
    if (/^From:\s/i.test(trimmed)) break;
    if (/^>/.test(trimmed)) continue;
    fresh.push(line);
  }

  return fresh
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
    .slice(0, 1500);
}

async function fetchReceivedEmail({ emailId, resendApiKey, fetchImpl = fetch }) {
  if (!emailId) throw new Error('Missing Resend email_id');
  if (!resendApiKey) throw new Error('Missing RESEND_API_KEY');
  const response = await fetchImpl(`https://api.resend.com/emails/receiving/${encodeURIComponent(emailId)}`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${resendApiKey}`,
      'Content-Type': 'application/json',
    },
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`Resend received email lookup failed: ${response.status} ${body}`.trim());
  }
  return response.json();
}

async function handleSmsEmailReplyWebhook({
  event,
  pool,
  twilioClient,
  resendApiKey,
  replySecret,
  allowedSenders = [],
  fetchImpl = fetch,
}) {
  if (!event || event.type !== 'email.received') return { handled: false, reason: 'ignored_event' };
  const target = parseSmsReplyTarget(event.data?.to || [], replySecret);
  if (!target) return { handled: false, reason: 'not_sms_reply' };
  const allowed = allowedSenders.map(extractEmailAddress).filter(Boolean);
  const sender = extractEmailAddress(event.data?.from || '');
  if (allowed.length > 0 && !allowed.includes(sender)) {
    return { handled: false, reason: 'sender_not_allowed' };
  }
  if (!twilioClient?.messages?.create) throw new Error('Twilio messaging is not configured');

  const receivedEmail = await fetchReceivedEmail({
    emailId: event.data?.email_id,
    resendApiKey,
    fetchImpl,
  });
  const body = extractReplyBody(receivedEmail.text, receivedEmail.html);
  if (!body) return { handled: false, reason: 'empty_reply' };

  const twilioMessage = await twilioClient.messages.create({
    to: target.to,
    from: target.from,
    body,
  });

  const cleanedPhone = target.to.replace(/\D/g, '').slice(-10);
  const customerResult = await pool.query(`
    SELECT id, name FROM customers
    WHERE RIGHT(REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g'), 10) = $1
       OR RIGHT(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = $1
    LIMIT 1
  `, [cleanedPhone]);

  await pool.query(`
    INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
    VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    ON CONFLICT (twilio_sid) DO NOTHING
  `, [
    twilioMessage.sid,
    target.from,
    target.to,
    body,
    twilioMessage.status || 'queued',
    customerResult.rows[0]?.id || null,
  ]);

  return { handled: true, sid: twilioMessage.sid };
}

module.exports = {
  buildSmsReplyAddress,
  parseSmsReplyTarget,
  extractReplyBody,
  handleSmsEmailReplyWebhook,
  toE164,
};
