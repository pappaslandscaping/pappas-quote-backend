const { upsert } = require('../scripts/import-copilot-invoices');

describe('Copilot invoice upsert duplicate protection', () => {
  test('keeps a legacy external-id row from stealing an invoice number owned by another row', async () => {
    let updateSql = null;
    let updateParams = null;

    const pool = {
      query: jest.fn(async (sql, params) => {
        if (sql.startsWith('SELECT id FROM invoices WHERE external_source')) {
          return { rows: [{ id: 10 }] };
        }
        if (sql.startsWith('SELECT id FROM invoices WHERE invoice_number')) {
          return { rows: [{ id: 20 }] };
        }
        if (sql.startsWith('UPDATE invoices SET')) {
          updateSql = sql;
          updateParams = params;
          return { rows: [] };
        }
        throw new Error('Unexpected SQL in test: ' + sql);
      }),
    };

    const result = await upsert(pool, {
      external_invoice_id: 'copilot-abc',
      invoice_number: '12178',
      customer_id: null,
      customer_name: 'Test Customer',
      customer_email: null,
      customer_address: null,
      status: 'pending',
      subtotal: 100,
      tax_amount: 8,
      total: 108,
      amount_paid: 0,
      due_date: null,
      paid_at: null,
      created_at: '2026-09-01',
      notes: null,
      terms: null,
      sent_status: 'sent',
      line_items: [],
      metadata: { source: 'test' },
    });

    expect(result).toEqual({ id: 20, inserted: false });
    expect(updateSql).toContain('UPDATE invoices SET');
    expect(updateParams[1]).toBeNull();
    expect(updateParams[2]).toBe('12178');
    expect(updateParams[20]).toBe(20);

    const metadata = JSON.parse(updateParams[19]);
    expect(metadata.source).toBe('test');
    expect(metadata.external_invoice_id_conflict).toBe('copilot-abc');
    expect(metadata.external_invoice_id_owner).toBe(10);
  });
});
