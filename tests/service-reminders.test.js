const assert = require('assert');
const createServiceReminderRoutes = require('../routes/service-reminders');

const { groupEligibleJobs, REMINDER_BODY, tomorrowInEastern } = createServiceReminderRoutes._helpers;

test('starts with August 20 service and computes tomorrow in Eastern time', () => {
  assert.strictEqual(tomorrowInEastern(new Date('2026-08-18T22:00:00Z')), '2026-08-19');
  assert.strictEqual(tomorrowInEastern(new Date('2026-08-19T02:00:00Z')), '2026-08-19');
});

test('groups eligible Homeworks visits once per customer and excludes canceled work', () => {
  const groups = groupEligibleJobs([
    { status: 'pending', copilot_event_type: 'VISIT', copilot_customer_id: 10, customer_name: 'Marie', copilot_visit_id: 1 },
    { status: 'pending', copilot_event_type: 'VISIT', copilot_customer_id: 10, customer_name: 'Marie', copilot_visit_id: 2 },
    { status: 'cancelled', copilot_event_type: 'VISIT', copilot_customer_id: 11, customer_name: 'No Send' },
    { status: 'pending', copilot_event_type: 'TODO', copilot_customer_id: 12, customer_name: 'Office Todo' },
  ]);
  assert.strictEqual(groups.length, 1);
  assert.strictEqual(groups[0].customerId, '10');
  assert.strictEqual(groups[0].jobs.length, 2);
});

test('approved reminder stays in one GSM SMS segment', () => {
  assert.ok(REMINDER_BODY.length <= 160);
  assert.match(REMINDER_BODY, /^This is Theresa from Pappas & Co\. Landscaping\./);
  assert.doesNotMatch(REMINDER_BODY, /[^\x00-\x7F]/);
});
