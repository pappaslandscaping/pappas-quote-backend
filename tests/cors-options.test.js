const {
  buildAllowedOrigins,
  isCorsOriginAllowed,
  parseAllowedOrigins
} = require('../lib/cors-options');

describe('CORS options', () => {
  test('parses comma-separated ALLOWED_ORIGINS and trims whitespace', () => {
    expect(
      parseAllowedOrigins('https://app.example.com, https://preview.example.com ,,')
    ).toEqual(['https://app.example.com', 'https://preview.example.com']);
  });

  test('allows configured production frontend origins only', () => {
    const env = {
      NODE_ENV: 'production',
      ALLOWED_ORIGINS: 'https://app.example.com,https://preview.example.com'
    };

    expect(isCorsOriginAllowed('https://app.example.com', env)).toBe(true);
    expect(isCorsOriginAllowed('https://preview.example.com', env)).toBe(true);
    expect(isCorsOriginAllowed('https://evil.example.com', env)).toBe(false);
    expect(isCorsOriginAllowed('http://localhost:3001', env)).toBe(false);
  });

  test('does not allow all origins in production when ALLOWED_ORIGINS is missing', () => {
    const env = { NODE_ENV: 'production' };

    expect(buildAllowedOrigins(env)).toEqual([]);
    expect(isCorsOriginAllowed('https://app.example.com', env)).toBe(false);
  });

  test('keeps local Next frontend origins available outside production', () => {
    const env = { NODE_ENV: 'development' };

    expect(isCorsOriginAllowed('http://localhost:3001', env)).toBe(true);
    expect(isCorsOriginAllowed('http://127.0.0.1:3001', env)).toBe(true);
    expect(isCorsOriginAllowed('https://evil.example.com', env)).toBe(false);
  });

  test('allows requests without an Origin header', () => {
    expect(isCorsOriginAllowed(undefined, { NODE_ENV: 'production' })).toBe(true);
  });
});
