const { getHomeWorksAccessToken } = require('../services/homeworks/auth');

describe('Homeworks server credential selection', () => {
  let original;
  beforeEach(() => {
    original = [process.env.HOMEWORKS_API_TOKEN, process.env.HOMEWORKS_ACCESS_TOKEN];
    delete process.env.HOMEWORKS_API_TOKEN;
    delete process.env.HOMEWORKS_ACCESS_TOKEN;
  });
  afterEach(() => {
    for (const [index, key] of ['HOMEWORKS_API_TOKEN', 'HOMEWORKS_ACCESS_TOKEN'].entries()) {
      if (original[index] === undefined) delete process.env[key];
      else process.env[key] = original[index];
    }
  });
  const pool = values => ({ query: jest.fn().mockResolvedValue({ rows: Object.entries(values).map(([key, value]) => ({ key, value })) }) });

  test('preserves the previously working preference for a refreshed cookie token over an old standalone token', async () => {
    const connection = pool({ copilot_token: 'old-standalone-token', copilot_cookies: 'session=unused; copilotApiAccessToken=current-api-token; other=unused' });
    expect(await getHomeWorksAccessToken(connection)).toBe('current-api-token');
  });
  test('prefers the dedicated official credential when configured', async () => {
    expect(await getHomeWorksAccessToken(pool({ homeworks_api_token: 'official', copilot_cookies: 'copilotApiAccessToken=legacy' }))).toBe('official');
  });
  test('retains the standalone token fallback when no cookie credential exists', async () => {
    expect(await getHomeWorksAccessToken(pool({ copilot_token: 'standalone' }))).toBe('standalone');
  });
  test('uses the explicit deployment credential without reading compatibility settings', async () => {
    process.env.HOMEWORKS_API_TOKEN = 'Bearer deployment-token';
    const connection = pool({});
    expect(await getHomeWorksAccessToken(connection)).toBe('deployment-token');
    expect(connection.query).not.toHaveBeenCalled();
  });
});
