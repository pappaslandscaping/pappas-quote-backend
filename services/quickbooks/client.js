// ═══════════════════════════════════════════════════════════
// QuickBooks Client
// Centralizes OAuth lifecycle (token storage, auto-refresh) and
// API calls. Routes/sync code use this module instead of
// constructing OAuth + HTTP requests inline.
// ═══════════════════════════════════════════════════════════

const OAuthClient = require('intuit-oauth');

function createOAuthClient() {
  return new OAuthClient({
    clientId: process.env.QB_CLIENT_ID || '',
    clientSecret: process.env.QB_CLIENT_SECRET || '',
    environment: process.env.QB_ENVIRONMENT || 'sandbox',
    redirectUri: process.env.QB_REDIRECT_URI || 'http://localhost:3000/api/quickbooks/callback',
    logging: false,
  });
}

/**
 * Returns an authenticated OAuthClient + realmId.
 * Auto-refreshes the access token if it expires within 5 minutes.
 * Throws on missing/invalid tokens — caller should map to 401 or
 * "QuickBooks not connected" UI message.
 */
async function getQBClient(pool) {
  const tokenRow = await pool.query('SELECT * FROM qb_tokens ORDER BY id DESC LIMIT 1');
  if (tokenRow.rows.length === 0) throw new Error('QuickBooks not connected');

  const t = tokenRow.rows[0];
  const oauthClient = createOAuthClient();
  oauthClient.setToken({
    access_token: t.access_token,
    refresh_token: t.refresh_token,
    token_type: t.token_type,
    expires_in: Math.floor((new Date(t.expires_at) - new Date()) / 1000),
    realmId: t.realm_id,
  });

  // Auto-refresh if expired or expiring within 5 minutes
  if (new Date(t.expires_at) <= new Date(Date.now() + 5 * 60 * 1000)) {
    try {
      const authResponse = await oauthClient.refresh();
      const newToken = authResponse.getJson();
      const expiresAt = new Date(Date.now() + (newToken.expires_in || 3600) * 1000);
      await pool.query(
        `UPDATE qb_tokens SET access_token=$1, refresh_token=$2, expires_at=$3, updated_at=NOW() WHERE id=$4`,
        [newToken.access_token, newToken.refresh_token || t.refresh_token, expiresAt, t.id]
      );
    } catch (e) {
      console.error('QB token refresh failed:', e.message);
      throw new Error('QuickBooks token expired. Please reconnect.');
    }
  }

  return { oauthClient, realmId: t.realm_id };
}

/**
 * GET to a QuickBooks v3 endpoint.
 * Handles the various intuit-oauth response shapes so callers get
 * a clean parsed object back.
 */
async function qbApiGet(pool, endpoint) {
  const { oauthClient, realmId } = await getQBClient(pool);
  const baseUrl = process.env.QB_ENVIRONMENT === 'production'
    ? 'https://quickbooks.api.intuit.com'
    : 'https://sandbox-quickbooks.api.intuit.com';
  const url = `${baseUrl}/v3/company/${realmId}/${endpoint}`;
  const response = await oauthClient.makeApiCall({ url, method: 'GET' });
  if (response.getJson) return response.getJson();
  if (response.json) return typeof response.json === 'function' ? await response.json() : response.json;
  if (response.body) return typeof response.body === 'string' ? JSON.parse(response.body) : response.body;
  if (typeof response.text === 'function') return JSON.parse(response.text());
  if (typeof response === 'string') return JSON.parse(response);
  return response;
}

module.exports = {
  createOAuthClient,
  getQBClient,
  qbApiGet,
};
