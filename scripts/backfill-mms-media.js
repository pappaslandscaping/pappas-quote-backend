#!/usr/bin/env node

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const fallbackEnvPath = path.join(__dirname, '..', '..', 'pappas-quote-backend', '.env');
if (!process.env.DATABASE_URL && fs.existsSync(fallbackEnvPath)) {
  require('dotenv').config({ path: fallbackEnvPath });
}

const execute = process.argv.includes('--execute');
const limitArg = process.argv.find((arg) => arg.startsWith('--limit='));
const limit = limitArg ? Math.max(parseInt(limitArg.split('=')[1], 10) || 0, 0) : 500;
const publicBaseUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com').replace(/\/$/, '');

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL is not set.');
  process.exit(1);
}

if (execute && (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN)) {
  console.error('TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required to download old MMS media.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined,
});

function isBackfillableUrl(url) {
  return typeof url === 'string'
    && url.length > 0
    && !url.startsWith(`${publicBaseUrl}/api/mms-image/`)
    && !url.includes('/api/mms-image/');
}

async function copyMediaUrl(mediaUrl) {
  const auth = Buffer.from(`${process.env.TWILIO_ACCOUNT_SID}:${process.env.TWILIO_AUTH_TOKEN}`).toString('base64');
  const response = await fetch(mediaUrl, { headers: { Authorization: `Basic ${auth}` } });
  if (!response.ok) {
    throw new Error(`fetch failed ${response.status}`);
  }

  const mimeType = (response.headers.get('content-type') || 'application/octet-stream').split(';')[0];
  const buffer = Buffer.from(await response.arrayBuffer());
  const result = await pool.query(
    'INSERT INTO mms_uploads (mime_type, data) VALUES ($1, $2) RETURNING id',
    [mimeType, buffer.toString('base64')]
  );
  return `${publicBaseUrl}/api/mms-image/${result.rows[0].id}`;
}

async function main() {
  await pool.query(`CREATE TABLE IF NOT EXISTS mms_uploads (
    id SERIAL PRIMARY KEY,
    mime_type VARCHAR(100) NOT NULL,
    data TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);

  const result = await pool.query(
    `SELECT id, media_urls
     FROM messages
     WHERE media_urls IS NOT NULL
       AND array_length(media_urls, 1) > 0
     ORDER BY created_at ASC
     LIMIT $1`,
    [limit]
  );

  let checked = 0;
  let candidates = 0;
  let updated = 0;
  let failed = 0;

  for (const row of result.rows) {
    checked += 1;
    const originalUrls = Array.isArray(row.media_urls) ? row.media_urls : [];
    const needsBackfill = originalUrls.some(isBackfillableUrl);
    if (!needsBackfill) continue;

    candidates += 1;

    if (!execute) {
      continue;
    }

    const nextUrls = [];
    let rowFailed = false;

    for (const url of originalUrls) {
      if (!isBackfillableUrl(url)) {
        nextUrls.push(url);
        continue;
      }

      try {
        nextUrls.push(await copyMediaUrl(url));
      } catch (error) {
        rowFailed = true;
        failed += 1;
        nextUrls.push(url);
        console.warn(`Message ${row.id}: failed to copy ${url}: ${error.message}`);
      }
    }

    if (!rowFailed) {
      await pool.query('UPDATE messages SET media_urls = $1 WHERE id = $2', [nextUrls, row.id]);
      updated += 1;
      console.log(`Message ${row.id}: updated ${nextUrls.length} media URL(s)`);
    }
  }

  console.log(JSON.stringify({
    mode: execute ? 'execute' : 'dry-run',
    checked,
    candidates,
    updated,
    failed,
    limit,
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end().catch(() => {});
  });
