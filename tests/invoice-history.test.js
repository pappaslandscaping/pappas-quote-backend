const assert = require('assert');
const { buildInvoiceHistoryEvents } = require('../lib/invoice-history');

let failures = 0;
function it(name, fn) {
  try {
    fn();
    console.log(`  \u2713 ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`  \u2717 ${name}\n    ${error.message}`);
  }
}

console.log('invoice-history');

it('prefers email log events over generic sent/reminder timestamps and preserves payment states', () => {
  const invoice = {
    invoice_number: '10448',
    customer_email: 'customer@example.com',
    created_at: '2026-04-01T12:00:00Z',
    sent_at: '2026-04-02T13:00:00Z',
    viewed_at: '2026-04-03T14:00:00Z',
    reminder_sent_at: '2026-04-04T15:00:00Z',
    reminder_count: 2,
    due_date: '2026-04-05',
    status: 'overdue',
    total: 250,
    amount_paid: 100,
  };

  const payments = [
    {
      amount: 75,
      method: 'ach',
      status: 'pending',
      ach_bank_name: 'KeyBank',
      paid_at: '2026-04-06T10:00:00Z',
    },
    {
      amount: 25,
      method: 'card',
      status: 'completed',
      card_last4: '4242',
      paid_at: '2026-04-05T09:00:00Z',
    },
  ];

  const emailLog = [
    {
      email_type: 'invoice',
      status: 'sent',
      recipient_email: 'customer@example.com',
      sent_at: '2026-04-02T13:05:00Z',
    },
    {
      email_type: 'invoice_reminder',
      status: 'sent',
      recipient_email: 'customer@example.com',
      sent_at: '2026-04-04T15:05:00Z',
    },
    {
      email_type: 'payment_receipt',
      status: 'sent',
      recipient_email: 'customer@example.com',
      sent_at: '2026-04-05T09:05:00Z',
    },
  ];

  const events = buildInvoiceHistoryEvents(invoice, {
    payments,
    emailLog,
    now: '2026-04-10T12:00:00Z',
  });

  const badges = events.map(event => event.badge);
  assert.ok(badges.includes('Sent by Email'));
  assert.ok(badges.includes('Reminder Sent'));
  assert.ok(badges.includes('Receipt Sent'));
  assert.ok(badges.includes('Payment Pending'));
  assert.ok(badges.includes('Payment Received'));
  assert.ok(badges.includes('Viewed'));
  assert.ok(badges.includes('Overdue'));

  assert.strictEqual(events.filter(event => event.badge === 'Sent by Email').length, 1);
  assert.strictEqual(events.filter(event => event.badge === 'Reminder Sent').length, 1);

  const pendingPayment = events.find(event => event.badge === 'Payment Pending');
  assert.strictEqual(pendingPayment.type, 'payment-pending');
  assert.ok(pendingPayment.detail.includes('KeyBank'));

  const settledPayment = events.find(event => event.badge === 'Payment Received');
  assert.strictEqual(settledPayment.type, 'paid');
  assert.ok(settledPayment.detail.includes('4242'));
});

it('falls back to paid_at when there is no payment row history', () => {
  const events = buildInvoiceHistoryEvents({
    invoice_number: '9297',
    created_at: '2026-04-01T12:00:00Z',
    paid_at: '2026-04-02T15:00:00Z',
    status: 'paid',
    total: 48.6,
  });

  const paidEvent = events.find(event => event.badge === 'Paid');
  assert.ok(paidEvent);
  assert.strictEqual(paidEvent.type, 'paid');
  assert.ok(paidEvent.title.includes('$48.60'));
});

it('does not add synthetic sent/reminder events when email_log already recorded failed attempts', () => {
  const events = buildInvoiceHistoryEvents({
    invoice_number: '10500',
    customer_email: 'customer@example.com',
    created_at: '2026-04-01T12:00:00Z',
    sent_at: '2026-04-02T13:00:00Z',
    reminder_sent_at: '2026-04-03T14:00:00Z',
    reminder_count: 1,
  }, {
    emailLog: [
      {
        email_type: 'invoice',
        status: 'failed',
        recipient_email: 'customer@example.com',
        sent_at: '2026-04-02T13:00:10Z',
      },
      {
        email_type: 'invoice_reminder',
        status: 'failed',
        recipient_email: 'customer@example.com',
        sent_at: '2026-04-03T14:00:10Z',
      },
    ],
  });

  const sentLikeEvents = events.filter(event => event.badge === 'Sent' || event.badge === 'Sent by Email');
  const reminderLikeEvents = events.filter(event => event.badge === 'Reminder Sent');
  const failedEvents = events.filter(event => event.badge === 'Email Failed' || event.badge === 'Reminder Failed');

  assert.strictEqual(sentLikeEvents.length, 0);
  assert.strictEqual(reminderLikeEvents.length, 0);
  assert.strictEqual(failedEvents.length, 2);
});

if (failures > 0) {
  process.exitCode = 1;
} else {
  console.log('\nAll invoice history tests passed.');
}
