const {
  QB_SCOPES,
  QuickBooksOAuthClient,
} = require('../services/quickbooks/client');

describe('native QuickBooks OAuth client', () => {
  const originalFetch = global.fetch;

  afterEach(() => {
    global.fetch = originalFetch;
  });

  test('builds the Intuit authorization URL with accounting and OpenID scopes', () => {
    const client = new QuickBooksOAuthClient({
      clientId: 'client-id',
      clientSecret: 'client-secret',
      redirectUri: 'https://example.com/api/quickbooks/callback',
    });

    const authUrl = new URL(client.authorizeUri({
      scope: [QB_SCOPES.Accounting, QB_SCOPES.OpenId],
      state: 'origin:https://example.com',
    }));

    expect(authUrl.origin + authUrl.pathname).toBe('https://appcenter.intuit.com/connect/oauth2');
    expect(authUrl.searchParams.get('client_id')).toBe('client-id');
    expect(authUrl.searchParams.get('response_type')).toBe('code');
    expect(authUrl.searchParams.get('redirect_uri')).toBe('https://example.com/api/quickbooks/callback');
    expect(authUrl.searchParams.get('scope')).toBe('com.intuit.quickbooks.accounting openid');
    expect(authUrl.searchParams.get('state')).toBe('origin:https://example.com');
  });

  test('exchanges an authorization code using Basic auth and the registered redirect URI', async () => {
    let request;
    global.fetch = jest.fn(async (url, options) => {
      request = { url, options };
      return {
        ok: true,
        status: 200,
        headers: new Headers(),
        text: async () => JSON.stringify({
          access_token: 'access-1',
          refresh_token: 'refresh-1',
          expires_in: 3600,
          token_type: 'bearer',
        }),
      };
    });

    const client = new QuickBooksOAuthClient({
      clientId: 'client-id',
      clientSecret: 'client-secret',
      redirectUri: 'https://example.com/api/quickbooks/callback',
    });
    const response = await client.createToken(
      'https://example.com/api/quickbooks/callback?code=abc123&realmId=42'
    );

    expect(request.url).toBe('https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer');
    expect(request.options.method).toBe('POST');
    expect(request.options.headers.Authorization).toBe(
      'Basic ' + Buffer.from('client-id:client-secret').toString('base64')
    );
    const body = new URLSearchParams(request.options.body);
    expect(body.get('grant_type')).toBe('authorization_code');
    expect(body.get('code')).toBe('abc123');
    expect(body.get('redirect_uri')).toBe('https://example.com/api/quickbooks/callback');
    expect(response.getJson().access_token).toBe('access-1');
  });

  test('refreshes an access token and preserves a rotated refresh token', async () => {
    global.fetch = jest.fn(async (_url, options) => {
      const body = new URLSearchParams(options.body);
      expect(body.get('grant_type')).toBe('refresh_token');
      expect(body.get('refresh_token')).toBe('refresh-old');
      return {
        ok: true,
        status: 200,
        headers: new Headers(),
        text: async () => JSON.stringify({
          access_token: 'access-new',
          refresh_token: 'refresh-new',
          expires_in: 3600,
        }),
      };
    });

    const client = new QuickBooksOAuthClient({
      clientId: 'client-id',
      clientSecret: 'client-secret',
      redirectUri: 'https://example.com/callback',
    });
    client.setToken({ access_token: 'access-old', refresh_token: 'refresh-old' });

    const response = await client.refresh();
    expect(response.getJson().refresh_token).toBe('refresh-new');
    expect(client.token.access_token).toBe('access-new');
    expect(client.token.refresh_token).toBe('refresh-new');
  });

  test('uses the current bearer token for QuickBooks API requests', async () => {
    let request;
    global.fetch = jest.fn(async (url, options) => {
      request = { url, options };
      return {
        ok: true,
        status: 200,
        headers: new Headers(),
        text: async () => JSON.stringify({ QueryResponse: { Customer: [] } }),
      };
    });

    const client = new QuickBooksOAuthClient();
    client.setToken({ access_token: 'access-token' });
    const response = await client.makeApiCall({
      url: 'https://sandbox-quickbooks.api.intuit.com/v3/company/42/query?query=SELECT%201',
      method: 'GET',
    });

    expect(request.options.headers.Authorization).toBe('Bearer access-token');
    expect(response.getJson()).toEqual({ QueryResponse: { Customer: [] } });
  });

  test('rejects OAuth callback URLs without an authorization code', async () => {
    const client = new QuickBooksOAuthClient({
      clientId: 'client-id',
      clientSecret: 'client-secret',
      redirectUri: 'https://example.com/callback',
    });
    await expect(client.createToken('https://example.com/callback?realmId=42'))
      .rejects.toThrow('missing authorization code');
  });
});
