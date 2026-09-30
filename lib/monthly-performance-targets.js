const SETTINGS_KEY = 'monthly_performance_customer_targets';

function parseTarget(value) {
  if (value === null || value === '') return null;
  const target = Number(value);
  if (!Number.isFinite(target) || target < 1 || target > 10000 || Math.abs(Math.round(target * 100) - target * 100) > 0.000001) {
    throw new Error('Enter a target from $1 to $10,000 per budgeted labor-hour, with no more than two decimals.');
  }
  return target;
}

function parseCustomerId(value) {
  const customerId = String(value ?? '').trim();
  if (!/^\d+$/.test(customerId)) throw new Error('A HomeWorks customer ID is required.');
  return customerId;
}

async function loadCustomerTargets(pool) {
  const result = await pool.query('SELECT value FROM business_settings WHERE key = $1', [SETTINGS_KEY]);
  const saved = result.rows[0]?.value || {};
  const targets = {};
  for (const [id, value] of Object.entries(saved)) {
    if (/^\d+$/.test(id) && Number.isFinite(Number(value)) && Number(value) > 0) targets[id] = Number(value);
  }
  return targets;
}

async function saveCustomerTarget(pool, customerIdInput, value) {
  const customerId = parseCustomerId(customerIdInput);
  const target = parseTarget(value);
  if (target === null) {
    await pool.query(
      `UPDATE business_settings SET value = COALESCE(value, '{}'::jsonb) - $2::text, updated_at = NOW() WHERE key = $1`,
      [SETTINGS_KEY, customerId]
    );
  } else {
    await pool.query(
      `INSERT INTO business_settings (key, value, updated_at)
       VALUES ($1, jsonb_build_object($2::text, $3::numeric), NOW())
       ON CONFLICT (key) DO UPDATE SET
         value = COALESCE(business_settings.value, '{}'::jsonb) || jsonb_build_object($2::text, $3::numeric),
         updated_at = NOW()`,
      [SETTINGS_KEY, customerId, target]
    );
  }
  return { customerId, targetPerHour: target };
}

module.exports = { parseTarget, parseCustomerId, loadCustomerTargets, saveCustomerTarget };
