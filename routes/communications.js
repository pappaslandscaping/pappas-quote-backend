// ═══════════════════════════════════════════════════════════
// Communications Routes — extracted from server.js
// Handles: admin message threads/conversations, SMS webhook,
//          broadcasts (preview/send/filter-options), email log
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const { validate, schemas } = require('../lib/validate');
const {
  renderCompiledCopilotTemplate,
  isCompiledCopilotTemplateSlug
} = require('../lib/compiled-copilot-templates');

function getBroadcastEligibility(customer, prefs, channel) {
  const emailEligible = !!(customer.email && customer.email.trim()) && (!prefs || prefs.email_marketing !== false);
  const smsEligible = !!(customer.mobile && customer.mobile.trim()) && (!prefs || prefs.sms_marketing !== false);

  const emailBlockedReason = !customer.email || !customer.email.trim()
    ? 'Missing email'
    : (prefs && prefs.email_marketing === false ? 'Email opted out' : null);
  const smsBlockedReason = !customer.mobile || !customer.mobile.trim()
    ? 'Missing mobile'
    : (prefs && prefs.sms_marketing === false ? 'SMS opted out' : null);

  let channelEligible = false;
  let channelLabel = 'Needs review';
  let channelBlockedReason = null;

  if (channel === 'email') {
    channelEligible = emailEligible;
    channelLabel = emailEligible ? 'Email ready' : 'Email blocked';
    channelBlockedReason = emailBlockedReason;
  } else if (channel === 'sms') {
    channelEligible = smsEligible;
    channelLabel = smsEligible ? 'SMS ready' : 'SMS blocked';
    channelBlockedReason = smsBlockedReason;
  } else {
    channelEligible = emailEligible || smsEligible;
    if (emailEligible && smsEligible) channelLabel = 'Email + SMS ready';
    else if (emailEligible) channelLabel = 'Email only';
    else if (smsEligible) channelLabel = 'SMS only';
    else channelLabel = 'Needs review';

    if (!channelEligible) {
      channelBlockedReason = [emailBlockedReason, smsBlockedReason].filter(Boolean).join(' · ') || 'No reachable channel';
    }
  }

  return {
    email_eligible: emailEligible,
    sms_eligible: smsEligible,
    email_blocked_reason: emailBlockedReason,
    sms_blocked_reason: smsBlockedReason,
    channel_eligible: channelEligible,
    channel_label: channelLabel,
    channel_blocked_reason: channelBlockedReason
  };
}

function extractFirstName(customer = {}) {
  const explicitFirstName = String(customer.first_name || '').trim();
  if (explicitFirstName) return explicitFirstName;
  const fullName = String(customer.name || '').trim();
  return fullName ? fullName.split(/\s+/)[0] : 'there';
}

function getBroadcastInclusionReasons(customer, filters) {
  const reasons = [];
  const rawTags = (customer.tags || '').split(',').map(t => t.trim()).filter(Boolean);
  const loweredTags = rawTags.map(t => t.toLowerCase());

  if (filters.tags && filters.tags.length > 0) {
    const matchedTags = filters.tags.filter(tag => loweredTags.includes(String(tag).toLowerCase()));
    matchedTags.forEach(tag => reasons.push(`Tag: ${tag}`));
  }
  if (filters.postal_codes && filters.postal_codes.length > 0 && customer.postal_code) {
    reasons.push(`ZIP: ${customer.postal_code}`);
  }
  if (filters.cities && filters.cities.length > 0 && customer.city) {
    reasons.push(`City: ${customer.city}`);
  }
  if (filters.status && customer.status) {
    reasons.push(`Status: ${customer.status}`);
  }
  if (filters.customer_type && customer.customer_type) {
    reasons.push(`Type: ${customer.customer_type}`);
  }
  if (filters.service_type) {
    reasons.push(`Service: ${customer.matched_service_type || filters.service_type}`);
  }
  if (filters.service_frequencies && filters.service_frequencies.length > 0) {
    const frequencyLabel = customer.matched_service_frequency_display
      || customer.matched_service_frequency
      || filters.service_frequencies.join(', ');
    reasons.push(`Frequency: ${frequencyLabel}`);
  }
  if (filters.monthly_plan) {
    reasons.push('Monthly plan customer');
  }
  if (filters.active_since_months) {
    reasons.push(`Active in last ${filters.active_since_months} months`);
  }
  if (filters.job_date) {
    reasons.push(`Scheduled on ${filters.job_date}`);
  }

  return reasons.length ? reasons : ['Matches current audience filters'];
}

function getBroadcastFilterSummary(filters) {
  const summary = [];
  if (filters.tags && filters.tags.length) summary.push(`Tags: ${filters.tags.join(', ')}`);
  if (filters.exclude_tags && filters.exclude_tags.length) summary.push(`Exclude tags: ${filters.exclude_tags.join(', ')}`);
  if (filters.postal_codes && filters.postal_codes.length) summary.push(`ZIPs: ${filters.postal_codes.join(', ')}`);
  if (filters.cities && filters.cities.length) summary.push(`Cities: ${filters.cities.join(', ')}`);
  if (filters.status) summary.push(`Status: ${filters.status}`);
  if (filters.customer_type) summary.push(`Type: ${filters.customer_type}`);
  if (filters.service_type) summary.push(`Service: ${filters.service_type}`);
  if (filters.service_frequencies && filters.service_frequencies.length) summary.push(`Frequency: ${filters.service_frequencies.join(', ')}`);
  if (filters.monthly_plan) summary.push('Monthly plan only');
  if (filters.active_since_months) summary.push(`Active in last ${filters.active_since_months} months`);
  if (filters.job_date) summary.push(`Scheduled on ${filters.job_date}`);
  return summary;
}

function buildBroadcastCustomerActivityCondition({ liveDateClause, scheduledDateClause }) {
  return `(
    EXISTS (
      SELECT 1
      FROM copilot_live_jobs clj
      LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
      LEFT JOIN customers live_customer
        ON live_customer.customer_number IS NOT NULL
       AND clj.source_customer_id IS NOT NULL
       AND live_customer.customer_number = clj.source_customer_id
      WHERE clj.source_deleted_at IS NULL
        AND COALESCE(yjo.customer_link_id, live_customer.id) = c.id
        AND ${liveDateClause}
    )
    OR EXISTS (
      SELECT 1
      FROM scheduled_jobs sj
      WHERE sj.customer_id = c.id
        AND ${scheduledDateClause}
    )
  )`;
}

function normalizeBroadcastServiceFrequency(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z]+/g, '');
}

const BROADCAST_ACTIVE_SERVICE_LOOKBACK_DAYS = 45;

function buildBroadcastNormalizedFrequencyExpression(sourceSql) {
  return `CASE
    WHEN regexp_replace(LOWER(${sourceSql}), '[^a-z]+', '', 'g') LIKE '%biweekly%' THEN 'biweekly'
    WHEN regexp_replace(LOWER(${sourceSql}), '[^a-z]+', '', 'g') LIKE '%weekly%' THEN 'weekly'
    ELSE NULL
  END`;
}

function buildBroadcastServiceProgramCondition({ serviceTypePlaceholder, frequencyPlaceholder }) {
  const liveServiceText = `CONCAT_WS(' ',
    clj.job_title,
    clj.raw_payload->>'job_title',
    clj.raw_payload->>'service_type',
    clj.raw_payload->>'service_title',
    clj.raw_payload->>'service_frequency'
  )`;
  const scheduledServiceText = `CONCAT_WS(' ',
    sj.service_frequency,
    sj.recurring_pattern,
    sj.service_type
  )`;
  const liveNormalizedFrequency = buildBroadcastNormalizedFrequencyExpression(`CONCAT_WS(' ',
    clj.job_title,
    clj.raw_payload->>'job_title',
    clj.raw_payload->>'service_type',
    clj.raw_payload->>'service_title',
    clj.raw_payload->>'service_frequency'
  )`);
  const scheduledNormalizedFrequency = buildBroadcastNormalizedFrequencyExpression(scheduledServiceText);
  const liveCustomerMatch = `(
    COALESCE(yjo.customer_link_id, live_customer.id) = c.id
    OR (
      COALESCE(yjo.customer_link_id, live_customer.id) IS NULL
      AND LOWER(BTRIM(COALESCE(clj.customer_name, ''))) = LOWER(BTRIM(COALESCE(c.name, '')))
    )
  )`;
  const scheduledActiveMowingCondition = `(
    sj.job_date::date >= CURRENT_DATE - INTERVAL '${BROADCAST_ACTIVE_SERVICE_LOOKBACK_DAYS} days'
    OR (
      (
        COALESCE(sj.is_recurring, false) = true
        OR LOWER(COALESCE(sj.recurring_pattern, '')) IN ('weekly', 'biweekly')
        OR LOWER(COALESCE(sj.service_frequency, '')) IN ('weekly', 'biweekly')
      )
      AND (sj.recurring_end_date IS NULL OR sj.recurring_end_date >= CURRENT_DATE)
    )
  )`;

  return `(
    EXISTS (
      SELECT 1
      FROM copilot_live_jobs clj
      LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
      LEFT JOIN customers live_customer
        ON live_customer.customer_number IS NOT NULL
       AND clj.source_customer_id IS NOT NULL
       AND live_customer.customer_number = clj.source_customer_id
      WHERE clj.source_deleted_at IS NULL
        AND ${liveCustomerMatch}
        AND clj.service_date >= CURRENT_DATE - INTERVAL '${BROADCAST_ACTIVE_SERVICE_LOOKBACK_DAYS} days'
        AND (
          ${serviceTypePlaceholder}::text IS NULL
          OR LOWER(${liveServiceText}) LIKE '%' || LOWER(${serviceTypePlaceholder}::text) || '%'
        )
        AND (
          ${frequencyPlaceholder}::text[] IS NULL
          OR COALESCE(array_length(${frequencyPlaceholder}::text[], 1), 0) = 0
          OR ${liveNormalizedFrequency} = ANY(${frequencyPlaceholder}::text[])
        )
    )
    OR EXISTS (
      SELECT 1
      FROM scheduled_jobs sj
      WHERE sj.customer_id = c.id
        AND COALESCE(LOWER(sj.status), '') NOT IN ('cancelled', 'canceled')
        AND ${scheduledActiveMowingCondition}
        AND (
          ${serviceTypePlaceholder}::text IS NULL
          OR LOWER(COALESCE(sj.service_type, '')) LIKE '%' || LOWER(${serviceTypePlaceholder}::text) || '%'
        )
        AND (
          ${frequencyPlaceholder}::text[] IS NULL
          OR COALESCE(array_length(${frequencyPlaceholder}::text[], 1), 0) = 0
          OR ${scheduledNormalizedFrequency} = ANY(${frequencyPlaceholder}::text[])
        )
    )
  )`;
}

function buildBroadcastServiceMatchDetailsQuery() {
  const liveNormalizedFrequency = buildBroadcastNormalizedFrequencyExpression(`CONCAT_WS(' ',
    clj.job_title,
    clj.raw_payload->>'job_title',
    clj.raw_payload->>'service_type',
    clj.raw_payload->>'service_title',
    clj.raw_payload->>'service_frequency'
  )`);
  const scheduledNormalizedFrequency = buildBroadcastNormalizedFrequencyExpression(`CONCAT_WS(' ',
    sj.service_frequency,
    sj.recurring_pattern,
    sj.service_type
  )`);
  const liveResolvedCustomerId = `COALESCE(
    yjo.customer_link_id,
    live_customer.id,
    (
      SELECT fallback_customer.id
      FROM customers fallback_customer
      WHERE fallback_customer.id = ANY($1::int[])
        AND LOWER(BTRIM(COALESCE(fallback_customer.name, ''))) = LOWER(BTRIM(COALESCE(clj.customer_name, '')))
      ORDER BY fallback_customer.id ASC
      LIMIT 1
    )
  )`;
  const liveCustomerMatch = `(
    ${liveResolvedCustomerId} = ANY($1::int[])
    OR (
      COALESCE(yjo.customer_link_id, live_customer.id) IS NULL
      AND EXISTS (
        SELECT 1
        FROM customers fallback_customer
        WHERE fallback_customer.id = ANY($1::int[])
          AND LOWER(BTRIM(COALESCE(fallback_customer.name, ''))) = LOWER(BTRIM(COALESCE(clj.customer_name, '')))
      )
    )
  )`;
  const scheduledActiveMowingCondition = `(
    sj.job_date::date >= CURRENT_DATE - INTERVAL '${BROADCAST_ACTIVE_SERVICE_LOOKBACK_DAYS} days'
    OR (
      (
        COALESCE(sj.is_recurring, false) = true
        OR LOWER(COALESCE(sj.recurring_pattern, '')) IN ('weekly', 'biweekly')
        OR LOWER(COALESCE(sj.service_frequency, '')) IN ('weekly', 'biweekly')
      )
      AND (sj.recurring_end_date IS NULL OR sj.recurring_end_date >= CURRENT_DATE)
    )
  )`;

  return `
    SELECT DISTINCT ON (customer_id)
      customer_id,
      matched_service_type,
      matched_service_frequency,
      CASE
        WHEN matched_service_frequency = 'biweekly' THEN 'Bi-Weekly'
        WHEN matched_service_frequency = 'weekly' THEN 'Weekly'
        ELSE NULL
      END AS matched_service_frequency_display
    FROM (
      SELECT
        ${liveResolvedCustomerId} AS customer_id,
        $2::text AS matched_service_type,
        ${liveNormalizedFrequency} AS matched_service_frequency,
        2 AS source_priority,
        clj.service_date AS sort_date
      FROM copilot_live_jobs clj
      LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
      LEFT JOIN customers live_customer
        ON live_customer.customer_number IS NOT NULL
       AND clj.source_customer_id IS NOT NULL
       AND live_customer.customer_number = clj.source_customer_id
      WHERE clj.source_deleted_at IS NULL
        AND ${liveCustomerMatch}
        AND clj.service_date >= CURRENT_DATE - INTERVAL '${BROADCAST_ACTIVE_SERVICE_LOOKBACK_DAYS} days'
        AND (
          $2::text IS NULL
          OR LOWER(CONCAT_WS(' ',
            clj.job_title,
            clj.raw_payload->>'job_title',
            clj.raw_payload->>'service_type',
            clj.raw_payload->>'service_title',
            clj.raw_payload->>'service_frequency'
          )) LIKE '%' || LOWER($2::text) || '%'
        )
        AND (
          $3::text[] IS NULL
          OR COALESCE(array_length($3::text[], 1), 0) = 0
          OR ${liveNormalizedFrequency} = ANY($3::text[])
        )

      UNION ALL

      SELECT
        sj.customer_id,
        $2::text AS matched_service_type,
        ${scheduledNormalizedFrequency} AS matched_service_frequency,
        1 AS source_priority,
        sj.job_date::date AS sort_date
      FROM scheduled_jobs sj
      WHERE sj.customer_id = ANY($1::int[])
        AND COALESCE(LOWER(sj.status), '') NOT IN ('cancelled', 'canceled')
        AND ${scheduledActiveMowingCondition}
        AND (
          $2::text IS NULL
          OR LOWER(COALESCE(sj.service_type, '')) LIKE '%' || LOWER($2::text) || '%'
        )
        AND (
          $3::text[] IS NULL
          OR COALESCE(array_length($3::text[], 1), 0) = 0
          OR ${scheduledNormalizedFrequency} = ANY($3::text[])
        )
    ) service_matches
    ORDER BY customer_id, source_priority, sort_date ASC
  `;
}

async function lookupBroadcastJobsForCustomerOnDate(pool, customerId, jobDate) {
  if (!customerId || !jobDate) return [];

  const liveResult = await pool.query(
    `SELECT
       COALESCE(
         NULLIF(clj.job_title, ''),
         NULLIF(clj.raw_payload->>'job_title', ''),
         NULLIF(clj.raw_payload->>'service_type', ''),
         'Service'
       ) AS service_type,
       COALESCE(yjo.address_override, clj.address_raw) AS address,
       clj.visit_total AS service_price,
       clj.service_date AS job_date
     FROM copilot_live_jobs clj
     LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
     LEFT JOIN customers live_customer
       ON live_customer.customer_number IS NOT NULL
      AND clj.source_customer_id IS NOT NULL
      AND live_customer.customer_number = clj.source_customer_id
     WHERE clj.source_deleted_at IS NULL
       AND COALESCE(yjo.customer_link_id, live_customer.id) = $1
       AND clj.service_date = $2::date
     ORDER BY clj.source_event_id ASC`,
    [customerId, jobDate]
  );

  if (liveResult.rows.length > 0) {
    return liveResult.rows;
  }

  const scheduledResult = await pool.query(
    `SELECT service_type, address, service_price, job_date
     FROM scheduled_jobs
     WHERE customer_id = $1 AND job_date::date = $2::date
     ORDER BY id ASC`,
    [customerId, jobDate]
  );

  return scheduledResult.rows;
}

function createCommunicationRoutes({ pool, sendEmail, emailTemplate, renderWithBaseLayout, renderManagedEmail, getTemplate, escapeHtml, serverError, twilioClient, TWILIO_PHONE_NUMBER, NOTIFICATION_EMAIL, replaceTemplateVars, sendPushToAllDevices }) {
  const router = express.Router();

  async function getOrCreatePortalTokenForCustomer(customerId, email) {
    const existing = await pool.query(
      `SELECT token
       FROM customer_portal_tokens
       WHERE customer_id = $1 AND expires_at > NOW()
       ORDER BY expires_at DESC, created_at DESC
       LIMIT 1`,
      [customerId]
    );
    if (existing.rows.length > 0) return existing.rows[0].token;

    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);
    await pool.query(
      'INSERT INTO customer_portal_tokens (token, customer_id, email, expires_at) VALUES ($1, $2, $3, $4)',
      [token, customerId, email, expiresAt]
    );
    return token;
  }

  async function buildYardSignResponseVars({ customerId, email, baseUrl }) {
    if (!customerId || !email) {
      return {
        yard_sign_yes_link: '',
        yard_sign_no_link: ''
      };
    }

    const token = await getOrCreatePortalTokenForCustomer(customerId, email);
    return {
      yard_sign_yes_link: `${baseUrl}/yard-sign-response?token=${token}&answer=yes`,
      yard_sign_no_link: `${baseUrl}/yard-sign-response?token=${token}&answer=no`
    };
  }

  async function findCustomerContext({ customerId, phoneNumber }) {
    if (customerId) {
      const result = await pool.query(
        'SELECT id, name, email, phone, mobile FROM customers WHERE id = $1 LIMIT 1',
        [customerId]
      );
      return result.rows[0] || null;
    }

    if (phoneNumber) {
      const cleanedPhone = String(phoneNumber).replace(/\D/g, '').slice(-10);
      if (!cleanedPhone) return null;
      const result = await pool.query(`
        SELECT id, name, email, phone, mobile
        FROM customers
        WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1
           OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1
        ORDER BY id ASC
        LIMIT 1
      `, [`%${cleanedPhone}`]);
      return result.rows[0] || null;
    }

    return null;
  }

  function plainTextToHtml(text) {
    return String(text || '')
      .split(/\r?\n/)
      .map((line) => escapeHtml(line))
      .join('<br>');
  }

  function appendTrackingPixel(html, pixelUrl) {
    const pixel = `<img src="${pixelUrl}" width="1" height="1" style="display:none;" />`;
    if (!html) return pixel;
    if (html.includes('</body>')) {
      return html.replace('</body>', `${pixel}</body>`);
    }
    return `${html}${pixel}`;
  }

  async function loginToCopilotCrm() {
    const username = process.env.COPILOTCRM_USERNAME || process.env.COPILOT_USERNAME;
    const password = process.env.COPILOTCRM_PASSWORD || process.env.COPILOT_PASSWORD;
    if (!username || !password) {
      throw new Error('Copilot credentials are not configured. Set COPILOTCRM_USERNAME/COPILOTCRM_PASSWORD or COPILOT_USERNAME/COPILOT_PASSWORD.');
    }

    const loginRes = await fetch('https://api.copilotcrm.com/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
      body: JSON.stringify({
        username,
        password
      })
    });
    const loginText = await loginRes.text();
    let auth;
    try {
      auth = JSON.parse(loginText);
    } catch (error) {
      throw new Error(`CopilotCRM login returned non-JSON: ${loginText.substring(0, 200)}`);
    }
    if (!auth.accessToken) {
      throw new Error(`CopilotCRM login failed: ${loginText.substring(0, 200)}`);
    }

    const cookie = `copilotApiAccessToken=${auth.accessToken}`;
    return {
      Cookie: cookie,
      Origin: 'https://secure.copilotcrm.com',
      Referer: 'https://secure.copilotcrm.com/',
      'X-Requested-With': 'XMLHttpRequest'
    };
  }

  async function findCopilotCustomer(headers, customer) {
    const emailLower = String(customer.email || '').trim().toLowerCase();
    if (!emailLower) return null;

    const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
      method: 'POST',
      headers: { ...headers, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `query=${encodeURIComponent(customer.email)}`
    });
    const searchText = await searchRes.text();
    let customers;
    try {
      customers = JSON.parse(searchText);
    } catch (error) {
      throw new Error(`CopilotCRM customer search returned non-JSON (status ${searchRes.status}): ${searchText.substring(0, 300)}`);
    }
    if (!Array.isArray(customers) || customers.length === 0) return null;

    return customers.find((c) => String(c.email || '').trim().toLowerCase() === emailLower && c.id) || null;
  }

  async function renderYardSignRequestEmail(customer, baseUrl) {
    const yardSignVars = await buildYardSignResponseVars({
      customerId: customer.id,
      email: customer.email,
      baseUrl
    });
    const customerName = customer.name || ((customer.first_name || '') + (customer.last_name ? ` ${customer.last_name}` : '')).trim() || 'there';
    const firstName = extractFirstName(customer);
    const customerAddress = [customer.street, customer.city, customer.state, customer.postal_code].filter(Boolean).join(', ');
    const tagVars = {
      CUSTOMER_FIRST_NAME: firstName,
      CUSTOMER_ADDRESS: customerAddress,
      COMPANY_NAME: 'Pappas & Co. Landscaping',
      COMPANY_PHONE: '(440) 886-7318',
      COMPANY_EMAIL: 'hello@pappaslandscaping.com',
      YARD_SIGN_YES_LINK: yardSignVars.yard_sign_yes_link,
      YARD_SIGN_NO_LINK: yardSignVars.yard_sign_no_link
    };
    const appVars = {
      customer_first_name: firstName,
      customer_address: customerAddress,
      company_name: tagVars.COMPANY_NAME,
      company_phone: tagVars.COMPANY_PHONE,
      company_email: tagVars.COMPANY_EMAIL,
      yard_sign_yes_link: yardSignVars.yard_sign_yes_link,
      yard_sign_no_link: yardSignVars.yard_sign_no_link,
      unsubscribe_email: customer.email ? encodeURIComponent(customer.email) : ''
    };

    const savedTemplate = typeof getTemplate === 'function'
      ? await getTemplate('yard_sign_request')
      : null;
    return {
      html: renderCompiledCopilotTemplate('yard_sign_request', tagVars),
      subject: replaceTemplateVars(savedTemplate?.subject || 'Quick Question: Would you be open to a yard sign?', appVars),
      tagVars,
      appVars,
      source: 'file'
    };
  }

  function isLocalDevRequest(req) {
    const host = String(req.hostname || '').toLowerCase();
    return process.env.NODE_ENV !== 'production' || host === 'localhost' || host === '127.0.0.1';
  }

  function getYardSignSendTargets(body) {
    const customerIds = [
      ...(Array.isArray(body.customer_ids) ? body.customer_ids : []),
      ...(body.customer_id ? [body.customer_id] : [])
    ].map((id) => parseInt(id, 10)).filter(Boolean);

    const emails = [
      ...(Array.isArray(body.emails) ? body.emails : []),
      ...(body.email ? [body.email] : [])
    ].map((email) => String(email || '').trim().toLowerCase()).filter(Boolean);

    return { customerIds, emails };
  }

  async function findYardSignCustomers({ customerIds, emails }) {
    if (customerIds.length && emails.length) {
      return pool.query(
        `SELECT id, name, first_name, last_name, email, street, city, state, postal_code
         FROM customers
         WHERE id = ANY($1) OR LOWER(COALESCE(email, '')) = ANY($2)`,
        [customerIds, emails]
      );
    }
    if (customerIds.length) {
      return pool.query(
        'SELECT id, name, first_name, last_name, email, street, city, state, postal_code FROM customers WHERE id = ANY($1)',
        [customerIds]
      );
    }
    return pool.query(
      `SELECT id, name, first_name, last_name, email, street, city, state, postal_code
       FROM customers
       WHERE LOWER(COALESCE(email, '')) = ANY($1)`,
      [emails]
    );
  }

  async function buildYardSignSendResults({ customers, dryRun, baseUrl }) {
    const copilotHeaders = await loginToCopilotCrm();
    const results = [];

    for (const customer of customers) {
      const rendered = await renderYardSignRequestEmail(customer, baseUrl);
      const copilotCustomer = await findCopilotCustomer(copilotHeaders, customer);
      const backendEmail = String(customer.email || '').trim().toLowerCase();
      const copilotEmail = String(copilotCustomer?.email || '').trim().toLowerCase();

      if (!copilotCustomer?.id) {
        results.push({
          customer_id: customer.id,
          customer_email: customer.email,
          backend_customer_email: customer.email,
          copilot_customer_email: null,
          copilot_customer_id: null,
          success: false,
          error: 'Copilot customer exact email match not found',
          yes_link: rendered.tagVars.YARD_SIGN_YES_LINK,
          no_link: rendered.tagVars.YARD_SIGN_NO_LINK
        });
        continue;
      }

      if (!backendEmail || backendEmail !== copilotEmail) {
        results.push({
          customer_id: customer.id,
          customer_email: customer.email,
          backend_customer_email: customer.email,
          copilot_customer_email: copilotCustomer.email || null,
          copilot_customer_id: copilotCustomer.id || null,
          success: false,
          error: 'Copilot customer email does not exactly match backend customer email',
          yes_link: rendered.tagVars.YARD_SIGN_YES_LINK,
          no_link: rendered.tagVars.YARD_SIGN_NO_LINK
        });
        continue;
      }

      if (dryRun) {
        results.push({
          customer_id: customer.id,
          customer_email: customer.email,
          backend_customer_email: customer.email,
          copilot_customer_email: copilotCustomer.email || null,
          copilot_customer_id: copilotCustomer.id,
          success: true,
          dry_run: true,
          yes_link: rendered.tagVars.YARD_SIGN_YES_LINK,
          no_link: rendered.tagVars.YARD_SIGN_NO_LINK,
          html_preview: rendered.html
        });
        continue;
      }

      const sendMailBody = new URLSearchParams({
        co_id: '5261',
        'to_customer[]': String(copilotCustomer.id),
        type: 'email',
        subject: rendered.subject,
        content: rendered.html
      });
      const sendMailRes = await fetch('https://secure.copilotcrm.com/emails/sendMail', {
        method: 'POST',
        headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: sendMailBody.toString()
      });
      const sendMailText = await sendMailRes.text();

      try {
        await pool.query(
          `INSERT INTO email_log (
            recipient_email,
            subject,
            email_type,
            customer_id,
            customer_name,
            status,
            error_message,
            html_body
          ) VALUES ($1, $2, 'yard_sign_request', $3, $4, $5, $6, $7)`,
          [
            customer.email || null,
            rendered.subject,
            customer.id,
            customer.name || null,
            sendMailRes.ok ? 'sent' : 'failed',
            sendMailRes.ok ? null : sendMailText.substring(0, 1000),
            rendered.html
          ]
        );
      } catch (logError) {
        console.error('Yard sign email log write error:', logError);
      }

      results.push({
        customer_id: customer.id,
        customer_email: customer.email,
        backend_customer_email: customer.email,
        copilot_customer_email: copilotCustomer.email || null,
        copilot_customer_id: copilotCustomer.id,
        success: sendMailRes.ok,
        status: sendMailRes.status,
        sendmail_response: sendMailRes.ok ? undefined : sendMailText.substring(0, 300),
        yes_link: rendered.tagVars.YARD_SIGN_YES_LINK,
        no_link: rendered.tagVars.YARD_SIGN_NO_LINK
      });
    }

    return results;
  }

router.get('/api/messages/conversations', async (req, res) => {
  try {
    const result = await pool.query(`
      WITH latest_messages AS (
        SELECT DISTINCT ON (
          CASE 
            WHEN direction = 'inbound' THEN from_number 
            ELSE to_number 
          END
        )
        id, twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read, created_at,
        CASE 
          WHEN direction = 'inbound' THEN from_number 
          ELSE to_number 
        END as contact_number
        FROM messages
        ORDER BY contact_number, created_at DESC
      )
      SELECT 
        lm.*,
        c.name as customer_name,
        (SELECT COUNT(*) FROM messages m2 
         WHERE m2.read = false 
         AND m2.direction = 'inbound'
         AND (m2.from_number = lm.contact_number OR m2.to_number = lm.contact_number)
        ) as unread_count,
        (SELECT COUNT(*) FROM messages m3 
         WHERE m3.from_number = lm.contact_number OR m3.to_number = lm.contact_number
        ) as message_count
      FROM latest_messages lm
      LEFT JOIN customers c ON lm.customer_id = c.id
      ORDER BY lm.created_at DESC
      LIMIT 100
    `);

    // Enrich with customer names where missing
    const conversations = await Promise.all(result.rows.map(async (conv) => {
      if (!conv.customer_name) {
        const cleanedPhone = conv.contact_number.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        conv.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: conv.id,
        phoneNumber: conv.contact_number,
        customerName: conv.customer_name,
        lastMessage: conv.body,
        lastMessageTime: conv.created_at,
        direction: conv.direction,
        unreadCount: parseInt(conv.unread_count) || 0,
        messageCount: parseInt(conv.message_count) || 0,
        read: conv.read
      };
    }));

    res.json({ success: true, conversations });
  } catch (error) {
    console.error('Get conversations error:', error);
    res.status(500).json({ success: false, conversations: [] });
  }
});

  // POST /api/copilotcrm/yard-sign/send - Render and send yard sign request emails through Copilot sendMail
  router.post('/api/copilotcrm/yard-sign/send', async (req, res) => {
    try {
      const { customerIds, emails } = getYardSignSendTargets(req.body);
      const dryRun = req.body.dry_run === true;
      if (!customerIds.length && !emails.length) {
        return res.status(400).json({ success: false, error: 'customer_ids, customer_id, email, or emails is required' });
      }

      const customerResult = await findYardSignCustomers({ customerIds, emails });
      if (!customerResult.rows.length) {
        return res.status(404).json({ success: false, error: 'No matching customers found' });
      }

      const baseUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com').replace(/\/$/, '');
      const results = await buildYardSignSendResults({ customers: customerResult.rows, dryRun, baseUrl });

      res.json({
        success: true,
        dry_run: dryRun,
        results
      });
    } catch (error) {
      console.error('Copilot yard sign send error:', error);
      serverError(res, error);
    }
  });

  // POST /dev/copilotcrm/yard-sign/send - Local-only unauthenticated dry-run helper
  router.post('/dev/copilotcrm/yard-sign/send', async (req, res) => {
    try {
      if (!isLocalDevRequest(req)) {
        return res.status(404).send('Not found');
      }

      const { customerIds, emails } = getYardSignSendTargets(req.body || {});
      const dryRun = req.body?.dry_run === true;
      if (!dryRun) {
        return res.status(400).json({ success: false, error: 'Local helper only supports dry_run: true' });
      }
      if (!customerIds.length && !emails.length) {
        return res.status(400).json({ success: false, error: 'customer_ids, customer_id, email, or emails is required' });
      }

      const customerResult = await findYardSignCustomers({ customerIds, emails });
      if (!customerResult.rows.length) {
        return res.status(404).json({ success: false, error: 'No matching customers found' });
      }

      const baseUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com').replace(/\/$/, '');
      const results = await buildYardSignSendResults({ customers: customerResult.rows, dryRun: true, baseUrl });

      res.json({
        success: true,
        dry_run: true,
        results
      });
    } catch (error) {
      console.error('Dev Copilot yard sign send error:', error);
      if (isLocalDevRequest(req)) {
        return res.status(500).json({
          success: false,
          error: error?.message || 'Something went wrong. Please try again.',
          stack: error?.stack || null
        });
      }
      serverError(res, error);
    }
  });

// Get all messages for a specific conversation thread
router.get('/api/messages/thread/:phoneNumber', async (req, res) => {
  const { phoneNumber } = req.params;
  try {
    const result = await pool.query(`
      SELECT m.*, c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      WHERE m.from_number = $1 OR m.to_number = $1
      ORDER BY m.created_at ASC
    `, [phoneNumber]);

    // Mark inbound messages as read
    await pool.query(`
      UPDATE messages SET read = true 
      WHERE (from_number = $1 OR to_number = $1) AND direction = 'inbound' AND read = false
    `, [phoneNumber]);

    const messages = result.rows.map(msg => ({
      id: msg.id,
      sid: msg.twilio_sid,
      direction: msg.direction,
      from: msg.from_number,
      to: msg.to_number,
      body: msg.body,
      mediaUrls: msg.media_urls,
      status: msg.status,
      customerName: msg.customer_name,
      timestamp: msg.created_at,
      read: msg.read
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get thread error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Get all messages for web dashboard (legacy - flat list)
router.get('/api/messages', async (req, res) => {
  const limit = parseInt(req.query.limit) || 200;
  try {
    const result = await pool.query(`
      SELECT 
        m.*,
        c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      ORDER BY m.created_at DESC
      LIMIT $1
    `, [limit]);

    // Enrich with customer names where missing
    const messages = await Promise.all(result.rows.map(async (msg) => {
      if (!msg.customer_name) {
        const phoneToSearch = msg.direction === 'inbound' ? msg.from_number : msg.to_number;
        const cleanedPhone = phoneToSearch.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        msg.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: msg.id,
        sid: msg.twilio_sid,
        direction: msg.direction,
        from: msg.from_number,
        to: msg.to_number,
        body: msg.body,
        status: msg.status,
        customerName: msg.customer_name,
        timestamp: msg.created_at,
        read: msg.read
      };
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get messages error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Send SMS from web dashboard
router.post('/api/messages/send', validate(schemas.sendMessage), async (req, res) => {
  const { to, body } = req.body;

  try {
    let formattedTo = to.replace(/\D/g, '');
    if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
    else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

    const twilioMessage = await twilioClient.messages.create({
      body,
      from: TWILIO_PHONE_NUMBER,
      to: formattedTo
    });

    // Find customer
    const cleanedPhone = formattedTo.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);

    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, body, twilioMessage.status, customerResult.rows[0]?.id || null]);

    res.json({ success: true, sid: twilioMessage.sid });
  } catch (error) {
    console.error('Send SMS error:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post('/api/communications/email/send', validate(schemas.sendOperationalEmail), async (req, res) => {
  try {
    const {
      to,
      customer_id: customerId,
      customer_name: customerName,
      phone_number: phoneNumber,
      subject,
      body,
      html_body: htmlBody,
    } = req.body;

    if (!subject || !String(subject).trim()) {
      return res.status(400).json({ success: false, error: 'subject is required' });
    }

    if ((!body || !String(body).trim()) && (!htmlBody || !String(htmlBody).trim())) {
      return res.status(400).json({ success: false, error: 'body or html_body is required' });
    }

    const customer = await findCustomerContext({ customerId, phoneNumber });
    const recipient = String(to || customer?.email || '').trim();

    if (!recipient) {
      return res.status(400).json({
        success: false,
        error: 'No recipient email available for this contact',
      });
    }

    const innerHtml = htmlBody && String(htmlBody).trim()
      ? String(htmlBody)
      : plainTextToHtml(body);

    await sendEmail(
      recipient,
      String(subject).trim(),
      emailTemplate(innerHtml),
      null,
      {
        type: 'communication',
        customer_id: customer?.id || customerId || null,
        customer_name: customer?.name || customerName || null,
      }
    );

    res.json({
      success: true,
      recipient_email: recipient,
      customer: customer ? {
        id: customer.id,
        name: customer.name,
        email: customer.email,
      } : null,
    });
  } catch (error) {
    console.error('Inbox email send error:', error);
    serverError(res, error);
  }
});


router.post('/api/sms/webhook', async (req, res) => {
  const { MessageSid, From, To, Body, NumMedia } = req.body;
  
  try {
    // Get media URLs if any
    const mediaUrls = [];
    const numMedia = parseInt(NumMedia) || 0;
    for (let i = 0; i < numMedia; i++) {
      if (req.body[`MediaUrl${i}`]) {
        mediaUrls.push(req.body[`MediaUrl${i}`]);
      }
    }

    // Find customer by phone number
    const cleanedPhone = From.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id, name FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);
    
    const customerId = customerResult.rows[0]?.id || null;
    const customerName = customerResult.rows[0]?.name || 'Unknown';

    // Store message
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read)
      VALUES ($1, 'inbound', $2, $3, $4, $5, 'received', $6, false)
      ON CONFLICT (twilio_sid) DO NOTHING
    `, [MessageSid, From, To, Body, mediaUrls, customerId]);

    console.log(`📨 Incoming SMS from ${customerName} (${From}): ${Body?.substring(0, 50)}...`);

    // Send push notification
    await sendPushToAllDevices(`💬 ${customerName}`, Body?.substring(0, 100) || 'New message', { type: 'sms', phoneNumber: cleanedPhone, contactName: customerName });

    // Send email notification (fire-and-forget)
    const smsDisplayName = customerName !== 'Unknown' ? escapeHtml(customerName) : From;
    const smsTimestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
    sendEmail(NOTIFICATION_EMAIL, `💬 New text from ${customerName !== 'Unknown' ? customerName : From}`, emailTemplate(`
      <h2 style="color:#1e293b;margin:0 0 16px;">New Text Message</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr><td style="padding:8px 0;color:#64748b;width:80px;">From</td><td style="padding:8px 0;color:#1e293b;font-weight:500;">${smsDisplayName}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Phone</td><td style="padding:8px 0;color:#1e293b;">${escapeHtml(From)}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Time</td><td style="padding:8px 0;color:#1e293b;">${smsTimestamp}</td></tr>
      </table>
      <div style="margin-top:20px;padding:16px;background:#f8fafc;border-radius:8px;border-left:4px solid #2e403d;">
        <p style="margin:0;color:#1e293b;line-height:1.6;">${escapeHtml(Body || 'No message content')}</p>
      </div>
    `, { showSignature: false })).catch(err => console.error('SMS notification email error:', err));

    // Send TwiML response (empty - don't auto-reply)
    res.type('text/xml').send('<Response></Response>');
  } catch (error) {
    console.error('SMS webhook error:', error);
    res.type('text/xml').send('<Response></Response>');
  }
});

// Get message conversations (grouped by NORMALIZED phone number) - for app
// FIX: Now properly groups by last 10 digits to prevent duplicate conversations

router.get('/api/broadcasts/filter-options', async (req, res) => {
  // Auth already verified by global middleware
  try {
    // Get all unique tags (comma-separated field, need to split and deduplicate)
    const [tagsResult, cities, postalCodes, statuses, customerTypes] = await Promise.all([
      pool.query(`SELECT DISTINCT tags FROM customers WHERE tags IS NOT NULL AND tags != ''`),
      pool.query(`SELECT DISTINCT city FROM customers WHERE city IS NOT NULL AND city != '' ORDER BY city`),
      pool.query(`SELECT DISTINCT postal_code FROM customers WHERE postal_code IS NOT NULL AND postal_code != '' ORDER BY postal_code`),
      pool.query(`SELECT DISTINCT status FROM customers WHERE status IS NOT NULL AND status != '' ORDER BY status`),
      pool.query(`SELECT DISTINCT customer_type FROM customers WHERE customer_type IS NOT NULL AND customer_type != '' ORDER BY customer_type`)
    ]);
    const tagSet = new Set();
    for (const row of tagsResult.rows) {
      (row.tags || '').split(',').forEach(t => { const trimmed = t.trim(); if (trimmed) tagSet.add(trimmed); });
    }

    res.json({
      success: true,
      tags: Array.from(tagSet).sort(),
      cities: cities.rows.map(r => r.city),
      postal_codes: postalCodes.rows.map(r => r.postal_code),
      statuses: statuses.rows.map(r => r.status),
      customer_types: customerTypes.rows.map(r => r.customer_type),
      service_types: ['Mowing'],
      service_frequencies: ['Weekly', 'Bi-Weekly']
    });
  } catch (error) {
    console.error('Broadcast filter-options error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/preview - Preview audience with filters
router.post('/api/broadcasts/preview', async (req, res) => {
  // Auth already verified by global middleware
  try {
    const filters = req.body.filters || {};
    const channel = req.body.channel || 'email';
    const conditions = [];
    const params = [];
    let paramIdx = 1;

    // Tags filter (comma-separated text field, match ANY of the provided tags)
    if (filters.tags && filters.tags.length > 0) {
      params.push(filters.tags.map(tag => String(tag).toLowerCase()));
      conditions.push(`EXISTS (
        SELECT 1
        FROM unnest(string_to_array(lower(COALESCE(c.tags, '')), ',')) AS tag_value
        WHERE btrim(tag_value) = ANY($${paramIdx++})
      )`);
    }

    if (filters.exclude_tags && filters.exclude_tags.length > 0) {
      params.push(filters.exclude_tags.map(tag => String(tag).toLowerCase()));
      conditions.push(`NOT EXISTS (
        SELECT 1
        FROM unnest(string_to_array(lower(COALESCE(c.tags, '')), ',')) AS tag_value
        WHERE btrim(tag_value) = ANY($${paramIdx++})
      )`);
    }

    // Postal codes
    if (filters.postal_codes && filters.postal_codes.length > 0) {
      params.push(filters.postal_codes);
      conditions.push(`c.postal_code = ANY($${paramIdx++})`);
    }

    // Cities (case-insensitive)
    if (filters.cities && filters.cities.length > 0) {
      params.push(filters.cities.map(c => c.toLowerCase()));
      conditions.push(`LOWER(c.city) = ANY($${paramIdx++})`);
    }

    // Status
    if (filters.status) {
      params.push(filters.status);
      conditions.push(`c.status = $${paramIdx++}`);
    }

    // Customer type
    if (filters.customer_type) {
      params.push(filters.customer_type);
      conditions.push(`c.customer_type = $${paramIdx++}`);
    }

    // Active/current service program + frequency filters.
    const normalizedFrequencies = Array.isArray(filters.service_frequencies)
      ? filters.service_frequencies.map(normalizeBroadcastServiceFrequency).filter(Boolean)
      : [];
    if (filters.service_type || normalizedFrequencies.length > 0) {
      params.push(filters.service_type ? String(filters.service_type) : null);
      const serviceTypePlaceholder = `$${paramIdx++}`;
      params.push(normalizedFrequencies.length > 0 ? normalizedFrequencies : null);
      const frequencyPlaceholder = `$${paramIdx++}`;
      conditions.push(buildBroadcastServiceProgramCondition({
        serviceTypePlaceholder,
        frequencyPlaceholder
      }));
    }

    // Has email
    if (filters.has_email) {
      conditions.push(`c.email IS NOT NULL AND c.email != ''`);
    }

    // Has mobile
    if (filters.has_mobile) {
      conditions.push(`c.mobile IS NOT NULL AND c.mobile != ''`);
    }

    // Monthly plan
    if (filters.monthly_plan) {
      conditions.push(`c.monthly_plan_amount > 0`);
    }

    // Active since N months: prefer live Copilot-linked jobs, then fall back to local scheduled rows.
    if (filters.active_since_months) {
      params.push(filters.active_since_months);
      const liveMonthsPlaceholder = `$${paramIdx++}`;
      params.push(filters.active_since_months);
      const scheduledMonthsPlaceholder = `$${paramIdx++}`;
      conditions.push(buildBroadcastCustomerActivityCondition({
        liveDateClause: `clj.service_date >= CURRENT_DATE - (${liveMonthsPlaceholder}::text || ' months')::INTERVAL`,
        scheduledDateClause: `COALESCE(sj.job_date::date, sj.created_at::date) >= CURRENT_DATE - (${scheduledMonthsPlaceholder}::text || ' months')::INTERVAL`
      }));
    }

    // Scheduled on specific date: prefer live Copilot-linked jobs, then fall back to local scheduled rows.
    if (filters.job_date) {
      params.push(filters.job_date);
      const liveJobDatePlaceholder = `$${paramIdx++}`;
      params.push(filters.job_date);
      const scheduledJobDatePlaceholder = `$${paramIdx++}`;
      conditions.push(buildBroadcastCustomerActivityCondition({
        liveDateClause: `clj.service_date = ${liveJobDatePlaceholder}::date`,
        scheduledDateClause: `sj.job_date::date = ${scheduledJobDatePlaceholder}::date`
      }));
    }

    const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
    const query = `SELECT c.id, c.name, c.first_name, c.last_name, c.email, c.mobile, c.city, c.postal_code, c.tags, c.status, c.customer_type, c.monthly_plan_amount
      FROM customers c ${whereClause} ORDER BY c.name`;
    const result = await pool.query(query, params);

    // Normalize names
    const baseCustomers = result.rows.map(c => ({
      id: c.id,
      name: c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim() || 'Unknown',
      email: c.email,
      mobile: c.mobile,
      city: c.city,
      postal_code: c.postal_code,
      tags: c.tags,
      status: c.status,
      customer_type: c.customer_type,
      monthly_plan_amount: c.monthly_plan_amount
    }));

    let customers = baseCustomers;
    const summary = {
      total: customers.length,
      eligible_current_channel: 0,
      with_email: customers.filter(c => c.email && c.email.trim()).length,
      with_mobile: customers.filter(c => c.mobile && c.mobile.trim()).length,
      email_opted_in: 0,
      sms_opted_in: 0,
      blocked_for_channel: 0,
      blocked_missing_contact: 0,
      blocked_opted_out: 0
    };

    if (customers.length > 0) {
      const custIds = customers.map(c => c.id);
      const prefs = await pool.query('SELECT customer_id, email_marketing, sms_marketing FROM customer_communication_prefs WHERE customer_id = ANY($1)', [custIds]);
      const prefsMap = {};
      prefs.rows.forEach(p => { prefsMap[p.customer_id] = p; });
      let emailOptIn = 0, smsOptIn = 0;
      let serviceMatchMap = {};
      const normalizedFrequencies = Array.isArray(filters.service_frequencies)
        ? filters.service_frequencies.map(normalizeBroadcastServiceFrequency).filter(Boolean)
        : [];
      if ((filters.service_type || normalizedFrequencies.length > 0) && custIds.length > 0) {
        const matchResult = await pool.query(
          buildBroadcastServiceMatchDetailsQuery(),
          [custIds, filters.service_type ? String(filters.service_type) : null, normalizedFrequencies.length > 0 ? normalizedFrequencies : null]
        );
        serviceMatchMap = Object.fromEntries(matchResult.rows.map((row) => [row.customer_id, row]));
      }

      customers = customers.map(c => {
        const p = prefsMap[c.id] || null;
        const serviceMatch = serviceMatchMap[c.id] || {};
        if (!p || p.email_marketing !== false) emailOptIn++;
        if (p && p.sms_marketing === true) smsOptIn++;
        const eligibility = getBroadcastEligibility(c, p, channel);
        return {
          ...c,
          ...serviceMatch,
          communication_prefs: {
            email_marketing: p ? p.email_marketing !== false : true,
            sms_marketing: p ? p.sms_marketing === true : false
          },
          inclusion_reasons: getBroadcastInclusionReasons(c, filters),
          ...eligibility
        };
      });
      summary.email_opted_in = emailOptIn;
      summary.sms_opted_in = smsOptIn;
      summary.eligible_current_channel = customers.filter(c => c.channel_eligible).length;
      summary.blocked_for_channel = customers.filter(c => !c.channel_eligible).length;
      summary.blocked_missing_contact = customers.filter(c => !c.channel_eligible && (
        c.email_blocked_reason === 'Missing email' ||
        c.sms_blocked_reason === 'Missing mobile'
      )).length;
      summary.blocked_opted_out = customers.filter(c => !c.channel_eligible && (
        c.email_blocked_reason === 'Email opted out' ||
        c.sms_blocked_reason === 'SMS opted out'
      )).length;
    } else {
      summary.email_opted_in = 0;
      summary.sms_opted_in = 0;
    }

    res.json({
      success: true,
      count: customers.length,
      customers,
      summary,
      filter_summary: getBroadcastFilterSummary(filters),
      channel
    });
  } catch (error) {
    console.error('Broadcast preview error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/send - Send broadcast email and/or SMS
// requireAdmin middleware blocks employees at the /api/broadcasts/send mount
router.post('/api/broadcasts/send', async (req, res) => {
  // Auth already verified by global middleware
  try {
    const { channel, template_id, sms_body, customer_ids, campaign_id, job_date } = req.body;
    if (!channel || !['email', 'sms', 'both'].includes(channel)) {
      return res.status(400).json({ success: false, error: 'channel must be email, sms, or both' });
    }
    if (!customer_ids || customer_ids.length === 0) {
      return res.status(400).json({ success: false, error: 'customer_ids required' });
    }
    if ((channel === 'email' || channel === 'both') && !template_id) {
      return res.status(400).json({ success: false, error: 'template_id required for email' });
    }
    if ((channel === 'sms' || channel === 'both') && !sms_body) {
      return res.status(400).json({ success: false, error: 'sms_body required for SMS' });
    }

    // Load template for email
    let tmpl = null;
    if (template_id) {
      const templateResult = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]);
      if (templateResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
      tmpl = templateResult.rows[0];
    }

    // Load customers
    const custResult = await pool.query('SELECT * FROM customers WHERE id = ANY($1)', [customer_ids]);

    // Load communication preferences for all target customers
    const prefsResult = await pool.query('SELECT * FROM customer_communication_prefs WHERE customer_id = ANY($1)', [customer_ids]);
    const prefsMap = {};
    for (const p of prefsResult.rows) { prefsMap[p.customer_id] = p; }

    const results = { email_sent: 0, email_skipped: 0, email_errors: 0, sms_sent: 0, sms_skipped: 0, sms_errors: 0 };
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';

    for (const cust of custResult.rows) {
      const custName = cust.name || ((cust.first_name || '') + (cust.last_name ? ' ' + cust.last_name : '')).trim() || 'Unknown';
      const yardSignVars = await buildYardSignResponseVars({
        customerId: cust.id,
        email: cust.email,
        baseUrl
      });
      const vars = {
        customer_name: custName,
        customer_first_name: extractFirstName(cust),
        customer_email: cust.email,
        customer_phone: cust.phone || cust.mobile,
        customer_address: [cust.street, cust.city, cust.state, cust.postal_code].filter(Boolean).join(', '),
        company_name: 'Pappas & Co. Landscaping',
        company_phone: '(440) 886-7318',
        company_email: 'hello@pappaslandscaping.com',
        company_website: 'pappaslandscaping.com',
        portal_link: `${baseUrl}/customer-portal.html`,
        yard_sign_yes_link: yardSignVars.yard_sign_yes_link,
        yard_sign_no_link: yardSignVars.yard_sign_no_link,
        unsubscribe_email: encodeURIComponent(cust.email || '')
      };

      // If job_date provided, look up ALL job details for this customer on that date
      if (job_date) {
        try {
          const jobs = await lookupBroadcastJobsForCustomerOnDate(pool, cust.id, job_date);
          if (jobs.length > 0) {
            if (jobs.length === 1) {
              // Single job — keep simple format
              const job = jobs[0];
              vars.service_type = job.service_type || '';
              const fullAddr = job.address || vars.customer_address || '';
              vars.address = fullAddr.split(',')[0].trim();
              vars.service_list = `${vars.service_type} at ${vars.address}`;
              vars.services_list = vars.service_list;
              vars.service_price = job.service_price ? '$' + Number(job.service_price).toFixed(2) : '';
            } else {
              // Multiple jobs — build "Mowing at 123 Main St and Spring Cleanup at 456 Oak Ave"
              const jobParts = jobs.map(j => {
                const svc = j.service_type || '';
                const fa = j.address || vars.customer_address || '';
                const street = fa.split(',')[0].trim();
                return `${svc} at ${street}`;
              });
              vars.service_list = jobParts.join(' and ');
              vars.services_list = vars.service_list;
              vars.service_type = jobs.map(j => j.service_type || '').join(' & ');
              vars.address = jobs.map(j => {
                const fa = j.address || vars.customer_address || '';
                return fa.split(',')[0].trim();
              }).join(' & ');
              const total = jobs.reduce((sum, j) => sum + (j.service_price ? Number(j.service_price) : 0), 0);
              vars.service_price = total > 0 ? '$' + total.toFixed(2) : '';
            }
            const firstJob = jobs[0];
            vars.job_date = firstJob.job_date ? new Date(firstJob.job_date).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' }) : '';
          }
        } catch (e) { console.error('Job lookup error:', e.message); }
      }

      const prefs = prefsMap[cust.id];

      // Send email
      if (channel === 'email' || channel === 'both') {
        // Check prefs: default allow email if no prefs row
        const emailAllowed = prefs ? prefs.email_marketing !== false : true;
        if (!emailAllowed || !cust.email) {
          results.email_skipped++;
        } else {
          try {
            const trackingId = crypto.randomUUID().replace(/-/g, '').slice(0, 24);
            const subject = replaceTemplateVars(tmpl.subject, vars);
            let finalHtml;
            if (tmpl.slug && isCompiledCopilotTemplateSlug(tmpl.slug)) {
              finalHtml = renderCompiledCopilotTemplate(tmpl.slug, vars);
            } else {
              const body = replaceTemplateVars(tmpl.body, vars);
              finalHtml = await renderManagedEmail(body, {
                wrapper: tmpl.options?.wrapper || 'full',
                showFeatures: tmpl.options?.showFeatures || false,
                showSignature: tmpl.options?.showSignature !== false,
                baseUrl,
                unsubscribeEmail: encodeURIComponent(cust.email)
              });
            }
            finalHtml = appendTrackingPixel(finalHtml, `${baseUrl}/api/t/${trackingId}/open.png`);
            await sendEmail(cust.email, subject, finalHtml, null, { type: 'broadcast', customer_id: cust.id, customer_name: custName });
            // Track in campaign_sends if campaign_id provided
            if (campaign_id) {
              await pool.query(
                'INSERT INTO campaign_sends (campaign_id, template_id, customer_id, customer_email, status, tracking_id) VALUES ($1, $2, $3, $4, $5, $6)',
                [campaign_id, template_id, cust.id, cust.email, 'sent', trackingId]
              );
            }
            results.email_sent++;
          } catch (e) {
            console.error(`Broadcast email error for customer ${cust.id}:`, e.message);
            results.email_errors++;
          }
        }
      }

      // Send SMS
      if (channel === 'sms' || channel === 'both') {
        // Check prefs: allow SMS by default if no prefs row exists
        const smsAllowed = prefs ? prefs.sms_marketing !== false : true;
        if (!smsAllowed || !cust.mobile) {
          results.sms_skipped++;
        } else {
          try {
            const smsText = replaceTemplateVars(sms_body, vars);
            let formattedTo = cust.mobile.replace(/\D/g, '');
            if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
            else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

            const twilioMessage = await twilioClient.messages.create({
              body: smsText,
              from: TWILIO_PHONE_NUMBER,
              to: formattedTo
            });

            // Log to messages table
            await pool.query(`
              INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
              VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
            `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, smsText, twilioMessage.status, cust.id]);

            results.sms_sent++;
          } catch (e) {
            console.error(`Broadcast SMS error for customer ${cust.id}:`, e.message);
            results.sms_errors++;
          }
        }
      }
    }

    // Update campaign stats if linked
    if (campaign_id && results.email_sent > 0) {
      await pool.query('UPDATE campaigns SET template_id = COALESCE(template_id, $1), send_count = COALESCE(send_count, 0) + $2 WHERE id = $3', [template_id, results.email_sent, campaign_id]);
    }

    res.json({ success: true, ...results });
  } catch (error) {
    console.error('Broadcast send error:', error);
    serverError(res, error);
  }
});


router.get('/api/email-log', async (req, res) => {
  try {
    const { type, search, days, limit = 100, offset = 0 } = req.query;
    let where = [];
    let params = [];
    let idx = 1;

    if (type && type !== 'all') {
      where.push(`email_type = $${idx++}`);
      params.push(type);
    }
    if (search) {
      where.push(`(recipient_email ILIKE $${idx} OR subject ILIKE $${idx} OR customer_name ILIKE $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }
    if (days) {
      where.push(`sent_at >= NOW() - $${idx}::int * INTERVAL '1 day'`);
      params.push(parseInt(days));
      idx++;
    }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const [countResult, result] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM email_log ${whereClause}`, params),
      pool.query(`SELECT * FROM email_log ${whereClause} ORDER BY sent_at DESC LIMIT $${idx} OFFSET $${idx + 1}`, [...params, parseInt(limit), parseInt(offset)])
    ]);

    res.json({ success: true, emails: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) {
    console.error('Email log error:', error);
    serverError(res, error);
  }
});

// Email log stats
router.get('/api/email-log/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS last_24h,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days') AS last_7d,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
        COUNT(DISTINCT recipient_email) AS unique_recipients
      FROM email_log
    `);
    const byType = await pool.query(`
      SELECT email_type, COUNT(*) AS count
      FROM email_log
      GROUP BY email_type
      ORDER BY count DESC
    `);
    res.json({ success: true, stats: stats.rows[0], by_type: byType.rows });
  } catch (error) {
    serverError(res, error);
  }
});


  return router;
}

createCommunicationRoutes._helpers = {
  getBroadcastEligibility,
  getBroadcastInclusionReasons,
  getBroadcastFilterSummary,
  buildBroadcastCustomerActivityCondition,
  lookupBroadcastJobsForCustomerOnDate
};

module.exports = createCommunicationRoutes;
