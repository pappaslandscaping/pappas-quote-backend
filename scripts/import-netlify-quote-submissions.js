#!/usr/bin/env node

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { classifyServiceArea } = require('../services/copilot/website-leads');

const inputPath = process.argv.find(arg => arg.endsWith('.json'));
const apply = process.argv.includes('--apply');
const confirmation = process.argv.find(arg => arg.startsWith('--confirm-count='));

if (!inputPath) {
  console.error('Usage: node scripts/import-netlify-quote-submissions.js submissions.json [--apply]');
  process.exit(1);
}

const submissions = JSON.parse(fs.readFileSync(path.resolve(inputPath), 'utf8'));
if (!Array.isArray(submissions)) throw new Error('Expected a JSON array of Netlify submissions');
if (apply && confirmation !== `--confirm-count=${submissions.length}`) {
  throw new Error(`Refusing to import without --confirm-count=${submissions.length}`);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : undefined,
});
let db = pool;

function booleanValue(value) {
  return value === true || value === 'true' || value === 'yes';
}

function normalizedServices(value) {
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  return String(value || '').split(',').map(item => item.trim()).filter(Boolean);
}

function submissionFingerprint(submission) {
  return crypto.createHash('sha256')
    .update(`netlify:${submission.netlify_submission_id}`)
    .digest('hex');
}

async function importSubmission(submission) {
  const services = normalizedServices(submission.services);
  const serviceArea = classifyServiceArea(submission.address, submission.city);
  const questions = {
    netlifySubmissionId: submission.netlify_submission_id,
    recoveredFromNetlify: true,
    propertyType: submission.propertyType,
    frequency: submission.frequency,
    gate: submission.gate,
    dogs: submission.dogs,
    lawnHeight: submission.lawnHeight,
    contactMethod: submission.contactMethod,
    startTime: submission.startTime,
    backyardAccess: submission.backyardAccess,
    preferredEstimateDate: submission.preferredEstimateDate,
    alternateEstimateDate: submission.alternateEstimateDate,
    landingPage: submission.landingPage,
    referrer: submission.referrer,
    utmSource: submission.utmSource,
    utmMedium: submission.utmMedium,
    utmCampaign: submission.utmCampaign,
    smsConsent: {
      transactional: booleanValue(submission.consentTransactional),
      marketing: booleanValue(submission.consentMarketing),
      termsAccepted: booleanValue(submission.consentTerms),
    },
  };
  const fingerprint = submissionFingerprint(submission);
  const existing = await db.query(
    `SELECT id FROM quotes
     WHERE submission_fingerprint = $1
        OR (LOWER(email) = LOWER($2) AND REGEXP_REPLACE(phone, '[^0-9]', '', 'g') = REGEXP_REPLACE($3, '[^0-9]', '', 'g') AND created_at::date = $4::timestamptz::date)
     LIMIT 1`,
    [fingerprint, submission.email, submission.phone, submission.created_at]
  );
  if (existing.rows.length) return { status: 'duplicate', id: existing.rows[0].id, name: submission.name };
  if (!apply) return { status: 'would-import', name: submission.name, createdAt: submission.created_at };

  const result = await db.query(
    `INSERT INTO quotes (
       name, email, phone, address, package, services, questions, notes, source, status,
       service_area_status, service_area_reason, homeworks_sync_status,
       follow_up_due_at, submission_fingerprint, created_at, updated_at
     ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, 'new', $10, $11, $12,
       $13::timestamptz + INTERVAL '1 day', $14, $13::timestamptz, CURRENT_TIMESTAMP)
     RETURNING id`,
    [
      submission.name, submission.email, submission.phone, submission.address,
      submission.package || null, services, JSON.stringify(questions), submission.notes || null,
      submission.source || 'Website', serviceArea.status, serviceArea.reason,
      serviceArea.status === 'inside' ? 'pending' : serviceArea.status === 'outside' ? 'skipped' : 'review',
      submission.created_at, fingerprint,
    ]
  );
  const quoteId = result.rows[0].id;
  await db.query(
    `INSERT INTO quote_lead_events (quote_id, event_type, description, details)
     VALUES ($1, 'recovered', 'Recovered from Netlify website form storage.', $2::jsonb)`,
    [quoteId, JSON.stringify({ netlifySubmissionId: submission.netlify_submission_id })]
  ).catch(() => {});
  return { status: 'imported', id: quoteId, name: submission.name };
}

(async () => {
  const results = [];
  let client;
  try {
    if (apply) {
      client = await pool.connect();
      db = client;
      await client.query('BEGIN');
    }
    for (const submission of submissions) results.push(await importSubmission(submission));
    if (client) await client.query('COMMIT');
    const counts = results.reduce((summary, result) => {
      summary[result.status] = (summary[result.status] || 0) + 1;
      return summary;
    }, {});
    console.log(JSON.stringify({ apply, counts, results }, null, 2));
  } catch (error) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    if (client) client.release();
    await pool.end();
  }
})().catch(error => {
  console.error(error.message || error);
  process.exit(1);
});
