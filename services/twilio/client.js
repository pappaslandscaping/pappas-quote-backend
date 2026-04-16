// ═══════════════════════════════════════════════════════════
// Twilio Client
// Centralizes SMS/voice operations and credential handling.
// Returns null if Twilio is not configured — callers should guard.
// ═══════════════════════════════════════════════════════════

const twilio = require('twilio');

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_PHONE_NUMBER = '+14408867318';
const TWILIO_PHONE_NUMBER_SECONDARY = '+12169413737';

let _client = null;
if (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
  try {
    _client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
  } catch (e) {
    console.error('Twilio client init failed:', e.message);
  }
}

function getClient() {
  return _client;
}

function isConfigured() {
  return _client !== null;
}

/**
 * Normalize a phone number to E.164 (+1 prefix for 10-digit US numbers).
 */
function normalizePhone(phone) {
  if (!phone) return null;
  let digits = String(phone).replace(/\D/g, '');
  if (digits.length === 10) return '+1' + digits;
  if (digits.length === 11 && digits.startsWith('1')) return '+' + digits;
  if (String(phone).startsWith('+')) return String(phone);
  return '+' + digits;
}

/**
 * Send an SMS. Returns the Twilio message object on success, or
 * throws a normalized error on failure.
 */
async function sendSms({ to, body, from = TWILIO_PHONE_NUMBER, mediaUrl = null }) {
  if (!_client) throw new Error('Twilio not configured');
  if (!to || !body) throw new Error('to and body are required');
  const normalizedTo = normalizePhone(to);
  const opts = { to: normalizedTo, from, body };
  if (mediaUrl) opts.mediaUrl = Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl];
  return await _client.messages.create(opts);
}

/**
 * Best-effort SMS — logs but doesn't throw. Use for non-critical
 * notifications where the main flow should not fail.
 */
async function trySendSms(opts) {
  if (!_client) return { sent: false, reason: 'twilio_not_configured' };
  try {
    const msg = await sendSms(opts);
    return { sent: true, sid: msg.sid };
  } catch (e) {
    console.error('Twilio sendSms failed:', e.message);
    return { sent: false, reason: e.message };
  }
}

module.exports = {
  TWILIO_PHONE_NUMBER,
  TWILIO_PHONE_NUMBER_SECONDARY,
  getClient,
  isConfigured,
  normalizePhone,
  sendSms,
  trySendSms,
};
