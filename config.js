const dotenv = require('dotenv');

dotenv.config();

const DEFAULT_BASE_URL = 'https://app.pappaslandscaping.com';
const DEFAULT_NOTIFICATION_EMAIL = 'hello@pappaslandscaping.com';
const DEFAULT_TWILIO_PHONE_NUMBER = '+14408867318';

function required(env, key) {
  const value = env[key];
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

function getConfig(env = process.env) {
  const baseUrl = env.BASE_URL || DEFAULT_BASE_URL;
  const nodeEnv = env.NODE_ENV || 'development';

  return {
    nodeEnv,
    port: Number(env.PORT || 3000),
    databaseUrl: required(env, 'DATABASE_URL'),
    auth: {
      jwtSecret: required(env, 'JWT_SECRET'),
      bootstrapAdminPassword: env.BOOTSTRAP_ADMIN_PASSWORD || '',
    },
    urls: {
      baseUrl,
      emailAssetsUrl: env.EMAIL_ASSETS_URL || baseUrl,
    },
    notifications: {
      email: env.NOTIFICATION_EMAIL || DEFAULT_NOTIFICATION_EMAIL,
    },
    twilio: {
      accountSid: env.TWILIO_ACCOUNT_SID || '',
      authToken: env.TWILIO_AUTH_TOKEN || '',
      phoneNumber: env.TWILIO_PHONE_NUMBER || DEFAULT_TWILIO_PHONE_NUMBER,
    },
  };
}

module.exports = {
  DEFAULT_BASE_URL,
  DEFAULT_NOTIFICATION_EMAIL,
  DEFAULT_TWILIO_PHONE_NUMBER,
  getConfig,
};
