const fs = require('fs');
const path = require('path');

describe('QuickBooks OAuth callback hardening', () => {
  test('bounds callback URLs before passing them to intuit-oauth', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'routes', 'quickbooks.js'), 'utf8');
    const routeStart = source.indexOf("router.get('/api/quickbooks/callback'");
    expect(routeStart).toBeGreaterThan(-1);
    const routeEnd = source.indexOf("// GET /api/quickbooks/status", routeStart);
    const callbackSource = source.slice(routeStart, routeEnd);

    expect(callbackSource).toContain('MAX_OAUTH_CALLBACK_URL_LENGTH');
    expect(callbackSource).toContain('res.status(414)');
    expect(callbackSource.indexOf('MAX_OAUTH_CALLBACK_URL_LENGTH')).toBeLessThan(
      callbackSource.indexOf('oauthClient.createToken')
    );
  });
});
