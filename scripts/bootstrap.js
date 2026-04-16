#!/usr/bin/env node

const crypto = require('crypto');
const { Pool } = require('pg');
const { getConfig } = require('../config');
const { createStartupSchemaTools } = require('../lib/startup-schema');

const ADMIN_USERS_TABLE = `CREATE TABLE IF NOT EXISTS admin_users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  role VARCHAR(50) DEFAULT 'admin',
  last_login TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

function hashPassword(password, existingSalt) {
  const salt = existingSalt || crypto.randomBytes(32).toString('hex');
  const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
  return `${salt}:${hash}`;
}

async function main() {
  const config = getConfig();
  const pool = new Pool({
    connectionString: config.databaseUrl,
    ssl: config.nodeEnv === 'production' ? { rejectUnauthorized: false } : false,
  });

  try {
    const { runCoreStartupMigrations } = createStartupSchemaTools({
      pool,
      config,
      hashPassword,
      adminUsersTable: ADMIN_USERS_TABLE,
    });

    console.log('Starting backend bootstrap...');
    await runCoreStartupMigrations();
    console.log('✅ Backend bootstrap complete');
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error('Bootstrap failed:', error);
  process.exitCode = 1;
});
