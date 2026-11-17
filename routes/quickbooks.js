// ═══════════════════════════════════════════════════════════
// QuickBooks Routes — OAuth flow, sync, sync log
// Vendor client + token lifecycle in services/quickbooks/client.js
// ═══════════════════════════════════════════════════════════

const express = require('express');
const OAuthClient = require('intuit-oauth');
const { createOAuthClient, getQBClient, qbApiGet } = require('../services/quickbooks/client');

module.exports = function createQuickbooksRoutes({ pool, serverError, nextCustomerNumber }) {
  const router = express.Router();
  const MAX_OAUTH_CALLBACK_URL_LENGTH = 2048;

// ═══════════════════════════════════════════════════════════
// QUICKBOOKS INTEGRATION (One-Way Sync: QB → Pappas)
// ═══════════════════════════════════════════════════════════

// QB tables created at startup via runStartupTableInit in lib/startup-schema.js

// --- OAuth Routes ---

// GET /api/quickbooks/debug - Show QB config for debugging redirect_uri issues
router.get('/api/quickbooks/debug', (req, res) => {
  const redirectUri = process.env.QB_REDIRECT_URI || 'http://localhost:3000/api/quickbooks/callback';
  const env = process.env.QB_ENVIRONMENT || 'sandbox';
  res.json({
    redirectUri,
    environment: env,
    hasClientId: !!process.env.QB_CLIENT_ID,
    hasClientSecret: !!process.env.QB_CLIENT_SECRET,
    clientIdPrefix: process.env.QB_CLIENT_ID ? process.env.QB_CLIENT_ID.substring(0, 8) + '...' : null
  });
});

// GET /api/quickbooks/auth - Start OAuth flow
router.get('/api/quickbooks/auth', (req, res) => {
  if (!process.env.QB_CLIENT_ID) {
    return res.status(400).json({ success: false, error: 'QuickBooks credentials not configured. Set QB_CLIENT_ID and QB_CLIENT_SECRET.' });
  }
  // Encode the origin so the callback knows where to redirect back to
  const origin = req.query.origin || (req.protocol + '://' + req.get('host'));
  const oauthClient = createOAuthClient();
  const authUri = oauthClient.authorizeUri({
    scope: [OAuthClient.scopes.Accounting, OAuthClient.scopes.OpenId],
    state: 'origin:' + origin
  });
  console.log('🔑 QB Auth - redirect_uri:', process.env.QB_REDIRECT_URI);
  console.log('🔑 QB Auth - environment:', process.env.QB_ENVIRONMENT);
  console.log('🔑 QB Auth - origin:', origin);
  res.redirect(authUri);
});

// GET /api/quickbooks/callback - Handle OAuth callback
router.get('/api/quickbooks/callback', async (req, res) => {
  try {
    // intuit-oauth currently depends on query-string 6.x, whose URI decoder
    // has an upstream malformed-percent DoS advisory with no CommonJS-safe
    // patched dependency path. Bound the callback before handing it to the
    // vendor parser so attacker-controlled input cannot grow enough to trigger
    // pathological decode work.
    if (String(req.originalUrl || '').length > MAX_OAUTH_CALLBACK_URL_LENGTH) {
      return res.status(414).send('QuickBooks callback URL is too long.');
    }

    const oauthClient = createOAuthClient();
    // Build the full callback URL using the registered redirect URI for token exchange
    const redirectUri = process.env.QB_REDIRECT_URI || (req.protocol + '://' + req.get('host') + '/api/quickbooks/callback');
    const callbackUrl = redirectUri + '?' + new URL(req.protocol + '://' + req.get('host') + req.originalUrl).searchParams.toString();
    const authResponse = await oauthClient.createToken(callbackUrl);
    const token = authResponse.getJson();
    const realmId = req.query.realmId;
    const expiresAt = new Date(Date.now() + (token.expires_in || 3600) * 1000);

    // Clear old tokens and store new ones
    await pool.query('DELETE FROM qb_tokens');
    await pool.query(
      `INSERT INTO qb_tokens (realm_id, access_token, refresh_token, token_type, expires_at) VALUES ($1,$2,$3,$4,$5)`,
      [realmId, token.access_token, token.refresh_token, token.token_type || 'bearer', expiresAt]
    );

    console.log('✅ QuickBooks connected. Realm ID:', realmId);

    // Redirect back to the origin (localhost in dev, production URL in prod)
    const state = req.query.state || '';
    const originMatch = state.match(/^origin:(.+)$/);
    const returnTo = originMatch ? originMatch[1] : '';
    if (returnTo && returnTo.startsWith('http://localhost')) {
      res.redirect(returnTo + '/settings.html?qb=connected');
    } else {
      res.redirect('/settings.html?qb=connected');
    }
  } catch (e) {
    console.error('QB callback error:', e);
    res.redirect('/settings.html?qb=error&msg=' + encodeURIComponent(e.message));
  }
});

// GET /api/quickbooks/status - Check connection
router.get('/api/quickbooks/status', async (req, res) => {
  try {
    const [tokenRow, lastSync] = await Promise.all([
      pool.query('SELECT realm_id, expires_at, updated_at FROM qb_tokens ORDER BY id DESC LIMIT 1'),
      pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 1')
    ]);

    if (tokenRow.rows.length === 0) {
      return res.json({ success: true, connected: false });
    }

    const t = tokenRow.rows[0];
    const isExpired = new Date(t.expires_at) <= new Date();

    res.json({
      success: true,
      connected: !isExpired,
      realmId: t.realm_id,
      tokenExpiresAt: t.expires_at,
      connectedAt: t.updated_at,
      lastSync: lastSync.rows[0] || null
    });
  } catch (e) {
    serverError(res, e);
  }
});

// POST /api/quickbooks/disconnect - Remove tokens
router.post('/api/quickbooks/disconnect', async (req, res) => {
  try {
    await pool.query('DELETE FROM qb_tokens');
    res.json({ success: true, message: 'QuickBooks disconnected' });
  } catch (e) {
    serverError(res, e);
  }
});

// --- Sync Functions ---

async function syncQBCustomers(){return {synced:0,created:0,updated:0,skipped:0,source:'official_homeworks_graphql',status:'managed_in_homeworks',message:'Homeworks supplies YardDesk operational records. QuickBooks import cannot overwrite the live mirror.'};}

async function syncQBInvoices(){return {synced:0,created:0,updated:0,skipped:0,source:'official_homeworks_graphql',status:'managed_in_homeworks',message:'Homeworks supplies YardDesk operational records. QuickBooks import cannot overwrite the live mirror.'};}

async function syncQBPayments(){return {synced:0,created:0,updated:0,skipped:0,source:'official_homeworks_graphql',status:'managed_in_homeworks',message:'Homeworks supplies YardDesk operational records. QuickBooks import cannot overwrite the live mirror.'};}

async function syncQBExpenses(changedSince = null) {
  let count = 0;

  // Expense columns/constraints ensured at startup by ensureQBTables (lib/startup-schema.js)

  // Sync Purchases (Bills, Expenses, Checks)
  for (const entityType of ['Purchase', 'Bill']) {
    let startPos = 1;
    const pageSize = 100;

    while (true) {
      const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
      const query = `SELECT * FROM ${entityType}${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
      const data = await qbApiGet(pool, `query?query=${encodeURIComponent(query)}`);
      const items = data?.QueryResponse?.[entityType] || [];
      if (items.length === 0) break;

      for (const item of items) {
        const qbId = `${entityType}-${item.Id}`;
        const lines = item.Line || [];
        const description = lines.map(l => l.Description).filter(Boolean).join('; ') || `${entityType} #${item.Id}`;
        const amount = parseFloat(item.TotalAmt) || 0;
        const vendor = item.EntityRef?.name || null;
        const category = lines[0]?.AccountBasedExpenseLineDetail?.AccountRef?.name
                       || lines[0]?.ItemBasedExpenseLineDetail?.ItemRef?.name
                       || entityType;
        const expenseDate = item.TxnDate || null;

        const existing = await pool.query('SELECT id FROM expenses WHERE qb_id = $1', [qbId]);
        if (existing.rows.length > 0) {
          await pool.query(
            `UPDATE expenses SET description=$1, amount=$2, category=$3, vendor=$4, expense_date=$5 WHERE qb_id=$6`,
            [description, amount, category, vendor, expenseDate, qbId]
          );
        } else {
          await pool.query(
            `INSERT INTO expenses (description, amount, category, vendor, expense_date, qb_id, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,NOW())`,
            [description, amount, category, vendor, expenseDate, qbId]
          );
        }
        count++;
      }

      if (items.length < pageSize) break;
      startPos += pageSize;
    }
  }
  return count;
}

// Track active sync state in memory
let activeSyncLogId = null;
let activeSyncProgress = null; // { stage, customers, invoices, payments, expenses, errors }

// POST /api/quickbooks/sync - Start background sync (returns immediately)
router.post('/api/quickbooks/sync', async (req, res) => {
  try {
    // Verify connection first
    await getQBClient(pool);

    // If a sync is already running, return its log ID
    if (activeSyncLogId !== null) {
      return res.json({ success: true, logId: activeSyncLogId, status: 'already_running' });
    }

    // Create sync log entry
    const logEntry = await pool.query(
      `INSERT INTO qb_sync_log (sync_type, started_at) VALUES ('full', NOW()) RETURNING id`
    );
    const logId = logEntry.rows[0].id;
    activeSyncLogId = logId;
    activeSyncProgress = { stage: 'customers', customers: 0, invoices: 0, payments: 0, expenses: 0, errors: [] };

    // Return immediately — sync runs in background
    res.json({ success: true, logId, status: 'started' });

    // Run sync in background (no await here)
    (async () => {
      const results = { customers: 0, invoices: 0, payments: 0, expenses: 0, errors: [] };

      // Get last successful sync time so we only fetch records changed since then
      let changedSince = null;
      try {
        const lastSync = await pool.query(
          `SELECT completed_at FROM qb_sync_log WHERE completed_at IS NOT NULL ORDER BY id DESC LIMIT 1`
        );
        if (lastSync.rows.length > 0) {
          changedSince = lastSync.rows[0].completed_at.toISOString().split('T')[0]; // YYYY-MM-DD
          console.log(`QB incremental sync: only fetching records changed since ${changedSince}`);
        } else {
          console.log('QB full sync: no previous sync found, fetching all records');
        }
      } catch (e) {
        console.error('Could not determine last sync time, running full sync:', e.message);
      }

      try {
        activeSyncProgress.stage = 'customers';
        results.customers = await syncQBCustomers(changedSince);
        activeSyncProgress.customers = results.customers;
      } catch (e) {
        results.errors.push('Customers: ' + e.message);
        activeSyncProgress.errors.push('Customers: ' + e.message);
        console.error('QB sync customers error:', e);
      }

      try {
        activeSyncProgress.stage = 'invoices';
        results.invoices = await syncQBInvoices(changedSince);
        activeSyncProgress.invoices = results.invoices;
      } catch (e) {
        results.errors.push('Invoices: ' + e.message);
        activeSyncProgress.errors.push('Invoices: ' + e.message);
        console.error('QB sync invoices error:', e);
      }

      try {
        activeSyncProgress.stage = 'payments';
        results.payments = await syncQBPayments(changedSince);
        activeSyncProgress.payments = results.payments;
      } catch (e) {
        results.errors.push('Payments: ' + e.message);
        activeSyncProgress.errors.push('Payments: ' + e.message);
        console.error('QB sync payments error:', e);
      }

      try {
        activeSyncProgress.stage = 'expenses';
        results.expenses = await syncQBExpenses(changedSince);
        activeSyncProgress.expenses = results.expenses;
      } catch (e) {
        results.errors.push('Expenses: ' + e.message);
        activeSyncProgress.errors.push('Expenses: ' + e.message);
        console.error('QB sync expenses error:', e);
      }

      // Update sync log as completed
      await pool.query(
        `UPDATE qb_sync_log SET customers_synced=$1, invoices_synced=$2, payments_synced=$3,
         expenses_synced=$4, errors=$5, completed_at=NOW() WHERE id=$6`,
        [results.customers, results.invoices, results.payments, results.expenses,
         results.errors.length ? results.errors.join('; ') : null, logId]
      );

      activeSyncProgress.stage = 'done';
      activeSyncLogId = null;
      console.log('✅ QB sync complete:', results);
    })();

  } catch (e) {
    activeSyncLogId = null;
    activeSyncProgress = null;
    console.error('QB sync error:', e);
    serverError(res, e);
  }
});

// GET /api/quickbooks/sync-progress - Poll progress of running sync
router.get('/api/quickbooks/sync-progress', async (req, res) => {
  if (activeSyncLogId === null) {
    // No active sync — return last completed log entry
    const last = await pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 1');
    return res.json({
      running: false,
      lastSync: last.rows[0] || null
    });
  }
  res.json({
    running: true,
    logId: activeSyncLogId,
    progress: activeSyncProgress
  });
});

// GET /api/quickbooks/sync-log - Get sync history
router.get('/api/quickbooks/sync-log', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 20');
    res.json({ success: true, logs: result.rows });
  } catch (e) {
    serverError(res, e);
  }
});
  return router;
};
