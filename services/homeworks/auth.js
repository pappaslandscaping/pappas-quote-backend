'use strict';

function extractAccessToken(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/(?:^|;\s*)copilotApiAccessToken=([^;]+)/i);
  return decodeURIComponent(match ? match[1] : raw.replace(/^Bearer\s+/i, ''));
}

async function getHomeWorksAccessToken(pool) {
  const configured = process.env.HOMEWORKS_API_TOKEN || process.env.HOMEWORKS_ACCESS_TOKEN;
  if (configured) return extractAccessToken(configured);
  if (!pool) return '';
  // One-way credential compatibility only. No browser session is established or used.
  const result = await pool.query(
    "SELECT key, value FROM copilot_sync_settings WHERE key IN ('homeworks_api_token', 'copilot_token', 'copilot_cookies')",
  );
  const values = Object.fromEntries(result.rows.map((row) => [row.key, row.value]));
  return extractAccessToken(
    values.homeworks_api_token || values.copilot_cookies || values.copilot_token || '',
  );
}

function tokenExpiry(token) {
  try {
    const { exp } = JSON.parse(Buffer.from(extractAccessToken(token).split('.')[1], 'base64url').toString());
    if (!Number.isFinite(exp)) return { expiresAt: null, daysUntilExpiry: null };
    const expiresAt = new Date(exp * 1000);
    return { expiresAt, daysUntilExpiry: (expiresAt.getTime() - Date.now()) / 86400000 };
  } catch {
    return { expiresAt: null, daysUntilExpiry: null };
  }
}

module.exports = { extractAccessToken, getHomeWorksAccessToken, tokenExpiry };
