// ═══════════════════════════════════════════════════════════
// QuickBooks Client
// Centralizes OAuth lifecycle (token storage, auto-refresh) and
// API calls without a third-party OAuth parser.
// ═══════════════════════════════════════════════════════════

const QB_SCOPES = Object.freeze({
  Accounting: 'com.intuit.quickbooks.accounting',
  OpenId: 'openid',
});

const INTUIT_AUTHORIZE_URL = 'https://appcenter.intuit.com/connect/oauth2';
const INTUIT_TOKEN_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer';

function responseWrapper(json, status = 200, headers = null) {
  return {
    status,
    headers,
    body: JSON.stringify(json),
    json,
    getJson: () => json,
    text: () => JSON.stringify(json),
  };
}

class QuickBooksOAuthClient {
  constructor({
    clientId = '',
    clientSecret = '',
    redirectUri = '',
    environment = 'sandbox',
  } = {}) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.redirectUri = redirectUri;
    this.environment = environment;
    this.token = {};
  }

  setToken(token = {}) {
    this.token = { ...token };
    return this;
  }

  authorizeUri({ scope = [], state = '' } = {}) {
    const params = new URLSearchParams({
      client_id: this.clientId,
      response_type: 'code',
      redirect_uri: this.redirectUri,
      scope: (Array.isArray(scope) ? scope : [scope]).filter(Boolean).join(' '),
    });
    if (state) params.set('state', state);
    return `${INTUIT_AUTHORIZE_URL}?${params.toString()}`;
  }

  async requestToken(params) {
    if (!this.clientId || !this.clientSecret) {
      throw new Error('QuickBooks OAuth credentials are not configured');
    }

    const auth = Buffer.from(`${this.clientId}:${this.clientSecret}`).toString('base64');
    const response = await fetch(INTUIT_TOKEN_URL, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${auth}`,
        Accept: 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams(params).toString(),
    });

    const text = await response.text();
    let json;
    try {
      json = text ? JSON.parse(text) : {};
    } catch (_error) {
      throw new Error(`QuickBooks OAuth returned invalid JSON (HTTP ${response.status})`);
    }

    if (!response.ok) {
      const detail = json.error_description || json.error || `HTTP ${response.status}`;
      throw new Error(`QuickBooks OAuth failed: ${detail}`);
    }

    this.setToken({ ...this.token, ...json });
    return responseWrapper(json, response.status, response.headers);
  }

  async createToken(callbackUrl) {
    const url = new URL(callbackUrl);
    const code = url.searchParams.get('code');
    if (!code) throw new Error('QuickBooks OAuth callback is missing authorization code');

    return this.requestToken({
      grant_type: 'authorization_code',
      code,
      redirect_uri: this.redirectUri,
    });
  }

  async refresh() {
    const refreshToken = this.token?.refresh_token;
    if (!refreshToken) throw new Error('QuickBooks refresh token is missing');

    return this.requestToken({
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
    });
  }

  async makeApiCall({ url, method = 'GET', headers = {}, body } = {}) {
    const accessToken = this.token?.access_token;
    if (!accessToken) throw new Error('QuickBooks access token is missing');
    if (!url) throw new Error('QuickBooks API URL is required');

    const response = await fetch(url, {
      method,
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${accessToken}`,
        ...headers,
      },
      body,
    });

    const text = await response.text();
    let json = {};
    if (text) {
      try {
        json = JSON.parse(text);
      } catch (_error) {
        if (!response.ok) {
          throw new Error(`QuickBooks API failed: HTTP ${response.status}`);
        }
        throw new Error('QuickBooks API returned invalid JSON');
      }
    }

    if (!response.ok) {
      const detail = json?.Fault?.Error?.[0]?.Message
        || json?.fault?.error?.[0]?.message
        || json?.error_description
        || json?.error
        || `HTTP ${response.status}`;
      throw new Error(`QuickBooks API failed: ${detail}`);
    }

    return responseWrapper(json, response.status, response.headers);
  }
}

function createOAuthClient() {
  return new QuickBooksOAuthClient({
    clientId: process.env.QB_CLIENT_ID || '',
    clientSecret: process.env.QB_CLIENT_SECRET || '',
    environment: process.env.QB_ENVIRONMENT || 'sandbox',
    redirectUri: process.env.QB_REDIRECT_URI || 'http://localhost:3000/api/quickbooks/callback',
  });
}

/**
 * Returns an authenticated QuickBooks OAuth client + realmId.
 * Auto-refreshes the access token if it expires within 5 minutes.
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

  // Auto-refresh if expired or expiring within 5 minutes.
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

/** GET to a QuickBooks v3 endpoint. */
async function qbApiGet(pool, endpoint) {
  const { oauthClient, realmId } = await getQBClient(pool);
  const baseUrl = process.env.QB_ENVIRONMENT === 'production'
    ? 'https://quickbooks.api.intuit.com'
    : 'https://sandbox-quickbooks.api.intuit.com';
  const url = `${baseUrl}/v3/company/${encodeURIComponent(realmId)}/${endpoint}`;
  const response = await oauthClient.makeApiCall({ url, method: 'GET' });
  return response.getJson();
}

module.exports = {
  QB_SCOPES,
  QuickBooksOAuthClient,
  createOAuthClient,
  getQBClient,
  qbApiGet,
};
