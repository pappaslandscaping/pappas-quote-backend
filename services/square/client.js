// ═══════════════════════════════════════════════════════════
// Square Client
// Centralizes Square SDK initialization and webhook signature
// verification. Returns null SDK if not configured.
// ═══════════════════════════════════════════════════════════

const crypto = require('crypto');

const SQUARE_APP_ID = process.env.SQUARE_APPLICATION_ID || '';
const SQUARE_LOCATION_ID = process.env.SQUARE_LOCATION_ID || '';
const SQUARE_ACCESS_TOKEN = process.env.SQUARE_ACCESS_TOKEN;
const SQUARE_ENVIRONMENT = process.env.SQUARE_ENVIRONMENT || 'sandbox';
const SQUARE_WEBHOOK_SIGNATURE_KEY = process.env.SQUARE_WEBHOOK_SIGNATURE_KEY;

let _client = null;
let _SquareApiError = null;

if (SQUARE_ACCESS_TOKEN) {
  try {
    const square = require('square');
    _SquareApiError = square.SquareError || square.ApiError;
    if (square.SquareClient) {
      // New SDK
      const env = SQUARE_ENVIRONMENT === 'production'
        ? square.SquareEnvironment.Production
        : square.SquareEnvironment.Sandbox;
      _client = new square.SquareClient({ token: SQUARE_ACCESS_TOKEN, environment: env });
    } else if (square.Client) {
      // Old SDK
      _client = new square.Client({
        accessToken: SQUARE_ACCESS_TOKEN,
        environment: SQUARE_ENVIRONMENT === 'production'
          ? square.Environment.Production
          : square.Environment.Sandbox,
      });
    }
    console.log(`✅ Square SDK initialized (${SQUARE_ENVIRONMENT})`);
  } catch (e) {
    console.error('Square SDK init failed:', e.message);
  }
}

function getClient() {
  return _client;
}

function isConfigured() {
  return _client !== null;
}

function getApiErrorClass() {
  return _SquareApiError;
}

/**
 * Normalize a Square SDK error to a human-readable message.
 * Handles the various result.errors[].detail shapes.
 */
function normalizeError(error) {
  if (_SquareApiError && error instanceof _SquareApiError) {
    return error.result?.errors?.[0]?.detail || error.message;
  }
  return error.message;
}

/**
 * Verify a Square webhook HMAC signature.
 * Returns true if the signature matches the configured signing key.
 */
function verifyWebhookSignature(rawBody, notificationUrl, signatureHeader) {
  if (!SQUARE_WEBHOOK_SIGNATURE_KEY || !signatureHeader) return false;
  const stringToSign = notificationUrl + rawBody;
  const expectedSignature = crypto
    .createHmac('sha256', SQUARE_WEBHOOK_SIGNATURE_KEY)
    .update(stringToSign)
    .digest('base64');
  // timing-safe comparison
  try {
    return crypto.timingSafeEqual(
      Buffer.from(expectedSignature),
      Buffer.from(signatureHeader)
    );
  } catch {
    return false;
  }
}

module.exports = {
  SQUARE_APP_ID,
  SQUARE_LOCATION_ID,
  SQUARE_ENVIRONMENT,
  getClient,
  isConfigured,
  getApiErrorClass,
  normalizeError,
  verifyWebhookSignature,
};
