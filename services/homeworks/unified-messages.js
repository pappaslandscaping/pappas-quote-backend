'use strict';
function mergeMessages(homeworks, local) {
  const seen = new Map();
  for (const row of homeworks) {
    const key = row.sid || `homeworks:${row.id}`;
    seen.set(key, {
      id: `homeworks:${row.id}`,
      providerId: row.sid || null,
      text: row.text,
      direction: row.direction === 'IN' ? 'inbound' : 'outbound',
      status: row.status,
      at: row.sentAt,
      source: 'homeworks',
    });
  }
  for (const row of local) {
    const key = row.twilio_sid || `yarddesk:${row.id}`;
    if (seen.has(key)) {
      seen.get(key).alsoInYardDesk = true;
      continue;
    }
    seen.set(key, {
      id: `yarddesk:${row.id}`,
      providerId: row.twilio_sid || null,
      text: row.body,
      direction: row.direction,
      status: row.status,
      at: row.created_at,
      source: 'yarddesk',
    });
  }
  return [...seen.values()].sort((a, b) => new Date(b.at) - new Date(a.at));
}
async function loadUnifiedMessages({ pool, customer, sms = [], homeworksError }) {
  if (!pool)
    return {
      messages: mergeMessages(sms, []),
      sources: { homeworks: homeworksError ? 'unavailable' : 'verified', yarddesk: 'unavailable' },
      limited: sms.length >= 100,
    };
  try {
    const phone = String(customer.cell || customer.phone || '')
      .replace(/\D/g, '')
      .slice(-10);
    if (phone.length !== 10) throw new Error('This customer has no verified mobile number.');
    const { rows: matches } = await pool.query(
      'SELECT homeworks_customer_id FROM homeworks_phone_matches WHERE normalized_phone=$1',
      [phone],
    );
    // Only combine phone-only office records after an explicit customer match.
    if (matches.length !== 1 || Number(matches[0].homeworks_customer_id) !== customer.id)
      throw new Error('Match this phone to the customer before combining office messages.');
    const result = await pool.query(
      "SELECT id,twilio_sid,body,direction,status,created_at FROM messages WHERE RIGHT(REGEXP_REPLACE(CASE WHEN direction='inbound' THEN from_number ELSE to_number END,'[^0-9]','','g'),10)=$1 ORDER BY created_at DESC LIMIT 200",
      [phone],
    );
    return {
      messages: mergeMessages(sms, result.rows),
      sources: { homeworks: homeworksError ? 'unavailable' : 'verified', yarddesk: 'verified' },
      limited: sms.length >= 100 || result.rows.length >= 200,
    };
  } catch (error) {
    return {
      messages: mergeMessages(sms, []),
      sources: { homeworks: homeworksError ? 'unavailable' : 'verified', yarddesk: 'unavailable' },
      error: error.message,
      limited: sms.length >= 100,
    };
  }
}
module.exports = { mergeMessages, loadUnifiedMessages };
