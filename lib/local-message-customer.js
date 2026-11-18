'use strict';

// messages.customer_id is a foreign key to the local customers table.
// Homeworks customer IDs belong in Homeworks matching/sync records, not here.
function normalizeUsPhone(phoneNumber) {
  const digits = String(phoneNumber || '').replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return digits.length === 10 ? digits : null;
}

async function findLocalMessageCustomer(pool, phoneNumber) {
  const phone = normalizeUsPhone(phoneNumber);
  if (!phone) return null;

  const result = await pool.query(`
    SELECT id, name
    FROM customers
    WHERE RIGHT(REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g'), 10) = $1
       OR RIGHT(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = $1
    ORDER BY id
    LIMIT 2
  `, [phone]);

  // A shared number is not a verified customer link. Keep the text,
  // but leave it unmatched rather than assigning it to the wrong account.
  return result.rows.length === 1 ? result.rows[0] : null;
}

module.exports = { normalizeUsPhone, findLocalMessageCustomer };
