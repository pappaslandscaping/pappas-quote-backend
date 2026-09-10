const LOCAL_FRONTEND_ORIGINS = [
  'http://localhost:3001',
  'http://127.0.0.1:3001'
];

const BUSINESS_FRONTEND_ORIGINS = [
  'https://pappaslandscaping.com',
  'https://www.pappaslandscaping.com',
  'https://magenta-gelato-064a50.netlify.app'
];

function parseAllowedOrigins(value) {
  return String(value || '')
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);
}

function buildAllowedOrigins(env = process.env) {
  const configuredOrigins = parseAllowedOrigins(env.ALLOWED_ORIGINS);

  if (env.NODE_ENV === 'production') {
    return [...new Set([...BUSINESS_FRONTEND_ORIGINS, ...configuredOrigins])];
  }

  return [...new Set([...configuredOrigins, ...LOCAL_FRONTEND_ORIGINS])];
}

function isCorsOriginAllowed(origin, env = process.env) {
  if (!origin) return true;
  return buildAllowedOrigins(env).includes(origin);
}

function buildCorsOptions(env = process.env) {
  return {
    origin(origin, callback) {
      callback(null, isCorsOriginAllowed(origin, env) ? origin || true : false);
    },
    credentials: true
  };
}

module.exports = {
  LOCAL_FRONTEND_ORIGINS,
  BUSINESS_FRONTEND_ORIGINS,
  parseAllowedOrigins,
  buildAllowedOrigins,
  isCorsOriginAllowed,
  buildCorsOptions
};
