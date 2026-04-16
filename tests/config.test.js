const { buildDatabaseUrl, getConfig } = require('../config');

describe('config', () => {
  test('requires DATABASE_URL and JWT_SECRET', () => {
    expect(() => getConfig({})).toThrow('Missing required environment variable: DATABASE_URL');
    expect(() => getConfig({ DATABASE_URL: 'postgres://example' })).toThrow('Missing required environment variable: JWT_SECRET');
  });

  test('builds DATABASE_URL from Railway PG variables when needed', () => {
    expect(
      buildDatabaseUrl({
        PGHOST: 'switchyard.proxy.rlwy.net',
        PGPORT: '5432',
        PGUSER: 'postgres',
        PGPASSWORD: 'secret:pass',
        PGDATABASE: 'railway',
      })
    ).toBe('postgres://postgres:secret%3Apass@switchyard.proxy.rlwy.net:5432/railway');
  });

  test('does not provide insecure auth defaults', () => {
    const config = getConfig({
      DATABASE_URL: 'postgres://example',
      JWT_SECRET: 'super-secret',
    });

    expect(config.auth.jwtSecret).toBe('super-secret');
    expect(config.auth.bootstrapAdminPassword).toBe('');
  });
});
