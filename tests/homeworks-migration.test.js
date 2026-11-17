const records = require('../services/homeworks/records');
const dispatch = require('../services/homeworks/dispatch');
const { mergeMessages } = require('../services/homeworks/unified-messages');
const { snowReadiness } = require('../services/homeworks/review');
const { mirrorInvoices } = require('../services/homeworks/invoice-cache');
const { mapHomeWorksEventToLiveJob } = require('../services/homeworks/client');

const remote = {
  id: 71,
  number: 123,
  customerId: 9,
  date: '2026-11-01',
  status: 'PARTIALLY_PAID',
  subtotal: '100',
  tax: '8',
  discountAmount: '0',
  total: '113',
  paidAmount: '40',
  lateFee: '3',
  processingFee: '2',
  isSent: true,
  daysPastDue: 16,
  lineItems: [
    {
      id: 1,
      name: 'Mowing',
      price: '100',
      quantity: '1.25',
      subtotal: '100',
      subtotalWithTax: '108',
      taxPrice1: '8',
      taxPrice2: '0',
    },
  ],
};
const owner = { id: 9, fullName: 'Test Customer', outstanding: '73', pastDue: '73', credit: '0' };
describe('Homeworks migration financial fidelity', () => {
  test('uses authoritative totals once, including fees, and keeps fractional quantities', () => {
    const row = records.mapInvoice(remote, owner);
    expect(row.total).toBe(113);
    expect(row.total_due).toBe(73);
    expect(row.subtotal).toBe(100);
    expect(row.line_items[0].quantity).toBe(1.25);
  });
  test('does not age unsent invoices or write-offs as issued balances', () => {
    const issued = { ...records.mapInvoice(remote, owner), homeworks: remote };
    const unsent = { ...issued, homeworks: { ...remote, id: 72, isSent: false } };
    const writtenOff = { ...issued, homeworks: { ...remote, id: 73, status: 'WRITE_OFF' } };
    const result = records.invoiceAging([issued, unsent, writtenOff]);
    expect(result.buckets.within_30.total).toBe(73);
    expect(result.buckets.within_30.count).toBe(1);
  });
  test('rejects incomplete money instead of treating it as zero', () =>
    expect(() => records.mapInvoice({ ...remote, total: null }, owner)).toThrow('invalid amount'));
  test('fails closed on repeated pages', async () => {
    const queryImpl = jest.fn().mockResolvedValue({ customers: [{ id: 1 }] });
    await expect(
      records.listAll(
        { queryImpl },
        { entity: 'customers', filterType: 'CustomerFilter', fields: 'id', pageSize: 1, maxRows: 3 },
      ),
    ).rejects.toThrow('changed while loading');
  });
  test('requires one unique phone match', async () => {
    const queryImpl = jest.fn().mockResolvedValue({
      customers: [
        { id: 1, phone: '4405551212' },
        { id: 2, cell: '(440)555-1212' },
      ],
    });
    await expect(records.uniqueCustomer({ queryImpl }, { phone: '440-555-1212' })).rejects.toThrow(
      'More than one',
    );
  });
  test('mail confirmation uses mark-only and verifies the stored channel', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce({ markInvoicesAsSent: [{ id: 71 }] })
      .mockResolvedValueOnce({ invoices: [{ id: 71, isSent: true, sentSnailMail: false }] });
    const result = await records.markInvoicesMailed({ queryImpl }, [71]);
    expect(result[0].success).toBe(false);
    expect(queryImpl.mock.calls[0][0].query).toContain('markInvoicesAsSent');
    expect(queryImpl.mock.calls[0][0].query).not.toContain('sendInvoices(');
  });
  test('a conflicting mirror identity is not reassigned by invoice number', async () => {
    const pool = {
      query: jest
        .fn()
        .mockResolvedValue({
          rows: [
            { id: 99, invoice_number: '123', external_source: 'copilotcrm', external_invoice_id: '999' },
          ],
        }),
    };
    await expect(mirrorInvoices(pool, [records.mapInvoice(remote, owner)])).rejects.toThrow(
      'conflicting YardDesk identity',
    );
    expect(pool.query).toHaveBeenCalledTimes(1);
  });
  test('collected revenue excludes refunded and expense payments in its official filter', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValue({ paymentReport: [{ isRefund: false, _sum: { totalAmount: '99.99' } }] });
    expect((await records.collectedRevenue({ queryImpl }, '2026-11-01', '2026-11-17')).total).toBe(99.99);
    expect(queryImpl.mock.calls[0][0].variables.where).toMatchObject({
      isRefund: false,
      isExpensePayment: false,
      isDeleted: false,
    });
  });
});
describe('Homeworks migration dispatch', () => {
  const state = {
    crews: [{ id: 3, name: 'Tim' }],
    events: [
      { id: 10, startDate: '2026-11-17', crewId: 3, status: 'OPEN' },
      { id: 11, startDate: '2026-11-17', crewId: 3, status: 'OPEN' },
    ],
    routes: [{ id: 40, crewId: 3, date: '2026-11-17' }],
  };
  test('uses an event route stop over its unrelated table order', () => {
    const mapped = mapHomeWorksEventToLiveJob({
      id: 10,
      startDate: '2026-11-17',
      sortOrder: 98,
      routeStops: [{ routeDate: '2026-11-17', routeId: 40, stopOrder: 2 }],
      isInvoiceable: false,
      location: { name: 'Custom entrance', latitude: 41, longitude: -81 },
      recurringEvent: { frequency: 'WEEKLY', weeklyCount: 2 },
    });
    expect(mapped.stop_order).toBe(2);
    expect(mapped.address).toBe('Custom entrance');
    expect(mapped.raw_data.invoiceable).toBe(false);
    expect(mapped.raw_data.frequency).toBe('Bi-Weekly');
  });
  test('does not mutate a route when the selected stop set is stale', async () => {
    const queryImpl = jest.fn().mockResolvedValue(state);
    await expect(
      dispatch.saveRouteOrder(
        { queryImpl },
        { date: '2026-11-17', crewName: 'Tim', orderedKeys: ['copilot:2026-11-17:10'] },
      ),
    ).rejects.toThrow('visit list changed');
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });
  test('verifies exact saved order rather than trusting the mutation response', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce(state)
      .mockResolvedValueOnce({ updateRoute: { id: 40 } })
      .mockResolvedValueOnce({
        route: {
          id: 40,
          date: '2026-11-17',
          crewId: 3,
          routeStops: [
            { eventId: 10, stopOrder: 1 },
            { eventId: 11, stopOrder: 2 },
          ],
        },
      });
    await expect(
      dispatch.saveRouteOrder(
        { queryImpl },
        {
          date: '2026-11-17',
          crewName: 'Tim',
          orderedKeys: ['copilot:2026-11-17:11', 'copilot:2026-11-17:10'],
        },
      ),
    ).rejects.toThrow('could not be verified');
  });
  test('saves and reads back the whole Homeworks route', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce(state)
      .mockResolvedValueOnce({ updateRoute: { id: 40 } })
      .mockResolvedValueOnce({
        route: {
          id: 40,
          date: '2026-11-17',
          crewId: 3,
          routeStops: [
            { eventId: 11, stopOrder: 1 },
            { eventId: 10, stopOrder: 2 },
          ],
        },
      });
    const result = await dispatch.saveRouteOrder(
      { queryImpl },
      {
        date: '2026-11-17',
        crewName: 'Tim',
        orderedKeys: ['copilot:2026-11-17:11', 'copilot:2026-11-17:10'],
      },
    );
    expect(result.verified).toBe(true);
    expect(queryImpl.mock.calls[1][0].variables.input).toMatchObject({
      eventIds: [11, 10],
      pinnedFirstEventId: 11,
    });
  });
  test('reports partial crew changes without automatically retrying', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce({ crews: [{ id: 3, name: 'Tim' }], events: state.events })
      .mockResolvedValueOnce({ assignEventCrew: { id: 10, crewId: 3 } })
      .mockRejectedValueOnce(new Error('connection lost'));
    try {
      await dispatch.saveAssignments({ queryImpl }, [
        { job_key: 'copilot:2026-11-17:10', crew_assigned: 'Tim' },
        { job_key: 'copilot:2026-11-17:11', crew_assigned: 'Tim' },
      ]);
      throw new Error('Expected failure');
    } catch (error) {
      expect(error.completedEventIds).toEqual([10]);
      expect(error.message).toContain('Some visits were updated');
    }
    expect(queryImpl).toHaveBeenCalledTimes(3);
  });
});
describe('Homeworks migration communications and snow review', () => {
  test('deduplicates only matching provider IDs, retaining repeated legitimate messages', () => {
    const rows = mergeMessages(
      [{ id: 1, sid: 'SM1', text: 'Hello', direction: 'OUT', sentAt: '2026-11-17T12:00:00Z' }],
      [
        { id: 1, twilio_sid: 'SM1', body: 'Hello', created_at: '2026-11-17T12:00:00Z' },
        { id: 2, twilio_sid: 'SM2', body: 'Hello', created_at: '2026-11-17T12:01:00Z' },
      ],
    );
    expect(rows).toHaveLength(2);
    expect(rows.find((row) => row.providerId === 'SM1').alsoInYardDesk).toBe(true);
  });
  test('does not count prior-season lists as current coverage or excluded estimate lines as accepted work', () => {
    const result = snowReadiness(
      [
        {
          id: 1,
          number: 123,
          customerId: 9,
          lineItems: [
            { id: 1, isIncluded: true, propertyId: 77, itemId: 5, name: 'Snow' },
            { id: 2, isIncluded: false, propertyId: 88, itemId: 5, name: 'Snow' },
          ],
        },
      ],
      [{ id: 1, name: '2025-2026 Snow', propertyCount: 1, properties: [{ propertyId: 77 }] }],
      '2026-2027',
    );
    expect(result.acceptedPropertyCount).toBe(1);
    expect(result.currentTemplateCount).toBe(0);
    expect(result.issues).toEqual([
      expect.objectContaining({ type: 'accepted_missing_dispatch', propertyId: 77 }),
    ]);
  });
  test('flags duplicate current dispatch entries for review', () => {
    const result = snowReadiness(
      [{ id: 1, lineItems: [{ id: 1, isIncluded: true, propertyId: 77 }] }],
      [
        {
          id: 1,
          name: '2026-2027 Snow',
          properties: [
            { propertyId: 77, crewId: 3 },
            { propertyId: 77, crewId: 3 },
          ],
        },
      ],
      '2026-2027',
    );
    expect(result.issues.some((row) => row.type === 'multiple_dispatch_entries')).toBe(true);
  });
});
