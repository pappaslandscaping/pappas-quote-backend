const assert = require('assert');
const { getCustomer360 } = require('../services/copilot/integration');

function createPool() {
  return {
    async query(sql) {
      if (sql.includes('FROM customers WHERE id')) {
        return {
          rows: [
            {
              id: 7,
              customer_number: 'C-7',
              name: 'Ada Customer',
              email: 'ada@example.com',
              phone: '4405551212',
              status: 'Active',
              customer_type: 'customer',
              notes: 'Prefers text updates.',
              created_at: '2026-05-01T12:00:00.000Z',
              updated_at: '2026-05-13T12:00:00.000Z',
            },
          ],
        };
      }

      if (sql.includes('FROM sent_quotes')) {
        return {
          rows: [
            {
              id: 11,
              quote_number: 'Q-11',
              services: { mowing: 'Weekly mowing' },
              total: '450',
              status: 'sent',
              created_at: '2026-05-12T12:00:00.000Z',
            },
          ],
        };
      }

      if (sql.includes('FROM scheduled_jobs')) {
        return {
          rows: [
            {
              id: 12,
              service_type: 'Mulch refresh',
              service_price: '900',
              status: 'scheduled',
              job_date: '2026-05-15T12:00:00.000Z',
              address: '1 Main St',
              crew_assigned: 'North',
            },
          ],
        };
      }

      if (sql.includes('FROM invoices')) {
        return {
          rows: [
            {
              id: 13,
              invoice_number: 'INV-13',
              total: '300',
              amount_paid: '100',
              status: 'sent',
              created_at: '2026-05-11T12:00:00.000Z',
            },
          ],
        };
      }

      if (sql.includes('FROM payments')) {
        return {
          rows: [
            {
              id: 14,
              invoice_id: 13,
              invoice_number: 'INV-13',
              amount: '100',
              method: 'card',
              status: 'completed',
              paid_at: '2026-05-12T13:00:00.000Z',
            },
          ],
        };
      }

      if (sql.includes('FROM messages')) {
        return {
          rows: [
            {
              id: 15,
              direction: 'inbound',
              body: 'Can we move the job?',
              status: 'received',
              created_at: '2026-05-14T12:00:00.000Z',
            },
          ],
        };
      }

      if (sql.includes('FROM calls')) {
        return { rows: [] };
      }

      if (sql.includes('FROM internal_notes')) {
        return { rows: [] };
      }

      return { rows: [] };
    },
  };
}

test('builds read-only Customer 360 from local data sources', async () => {
  const customer360 = await getCustomer360({ pool: createPool(), customerId: 7 });

  assert.strictEqual(customer360.customer.name, 'Ada Customer');
  assert.strictEqual(customer360.summary.quote_count, 1);
  assert.strictEqual(customer360.summary.job_count, 1);
  assert.strictEqual(customer360.summary.invoice_count, 1);
  assert.strictEqual(customer360.summary.open_invoice_balance, 200);
  assert.strictEqual(customer360.summary.payment_count, 1);
  assert.strictEqual(customer360.ai.mode, 'draft_only');
  assert.ok(customer360.ai.blocked_actions.includes('send_email'));
  assert.ok(customer360.timeline.some((event) => event.type === 'communication'));
  assert.ok(customer360.timeline.some((event) => event.detail.includes('mowing: Weekly mowing')));
});
