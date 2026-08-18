const assert = require('assert');
const createServiceReminderRoutes = require('../routes/service-reminders');

const { buildReminderBody, groupEligibleJobs, tomorrowInEastern } = createServiceReminderRoutes._helpers;

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

test('builds the approved automated reminder with the Homeworks service date', () => {
  const body = buildReminderBody('2026-08-19');
  assert.strictEqual(body, 'Automated reminder from Pappas & Co. Landscaping:\n\nYour property is on our schedule for tomorrow, Aug 19, 2026.\n\nPlease note that weather or other unexpected delays may affect the schedule.\n\nQuestions? Reply to this text or email hello@pappaslandscaping.com.');
  assert.doesNotMatch(body, /[^\x00-\x7F]/);
});
