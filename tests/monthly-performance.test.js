const { monthBounds, summarizeVisits, fetchMonthlyPerformance } = require('../lib/monthly-performance');

describe('HomeWorks monthly performance', () => {
  test('validates calendar months and handles leap years', () => {
    expect(monthBounds('2024-02')).toEqual({ start: '2024-02-01', end: '2024-02-29', prior: '2024-01' });
    expect(monthBounds('2026-01').prior).toBe('2025-12');
    expect(() => monthBounds('2026-13')).toThrow('month must be YYYY-MM');
  });

  test('keeps missing actual time separate from budgeted pricing signals', () => {
    const result = summarizeVisits([
      {
        id: 1, title: 'Mowing', subtotal: '45', budgetedHours: '0.5',
        customer: { id: 100, fullName: 'Customer A' }, property: { id: 10, name: '10 Main St' },
        timeEntryTotals: { pastTimeEntrySeconds: 0 },
      },
      {
        id: 2, title: 'Mowing', subtotal: '55', budgetedHours: '0.5',
        customer: { id: 100, fullName: 'Customer A' }, property: { id: 10, name: '10 Main St' },
        timeEntryTotals: { pastTimeEntrySeconds: 1800 },
      },
      {
        id: 3, title: 'Cleanup', subtotal: '200', budgetedHours: '0',
        customer: { id: 101, fullName: 'Customer B' }, property: { id: 11, name: '11 Main St' },
        timeEntryTotals: { pastTimeEntrySeconds: 0 },
      },
    ]);
    expect(result.coverage).toMatchObject({ completedVisits: 3, budgetedVisits: 2, trackedVisits: 1, trackedHours: 0.5 });
    expect(result.services.find((row) => row.name === 'Mowing').revenuePerBudgetedHour).toBe(100);
    expect(result.services.find((row) => row.name === 'Mowing').customers).toBe(1);
    expect(result.pricingReview).toHaveLength(1);
    expect(result.pricingReview[0]).toMatchObject({ customerId: '100', visits: 2, averagePrice: 50, averageBudgetedHours: 0.5 });
  });

  test('paginates visits and keeps payment dates distinct from invoice dates', async () => {
    const queryImpl = jest.fn(async (request) => {
      let data;
      if (request.operationName === 'YardDeskMonthlyPerformanceVisits') {
        const visit = {
          id: 1, title: 'Mowing', subtotal: '50', budgetedHours: '0.5',
          property: { id: 1, name: 'A' }, timeEntryTotals: { pastTimeEntrySeconds: 0 },
        };
        data = { events: request.variables.skip === 0 ? Array.from({ length: 1000 }, (_, index) => ({ ...visit, id: index + 1 })) : [{ ...visit, id: 1001 }] };
      } else if (request.operationName === 'YardDeskMonthlyPerformancePrior') {
        data = {
          invoiceReport: [{ _count: { id: 1 }, _sum: { total: '100', subtotal: '93', discountAmount: '0', paidAmount: '100' } }],
          paymentReport: [{ _sum: { totalAmount: '80' } }], refundReport: [{ _sum: { totalAmount: '0' } }],
        };
      } else {
        data = {
          invoiceReport: [{ _count: { id: 2 }, _sum: { total: '120', subtotal: '115', discountAmount: '5', paidAmount: '40' } }],
          paymentReport: [{ _sum: { totalAmount: '90' } }], refundReport: [{ _sum: { totalAmount: '10' } }],
        };
      }
      return data;
    });
    const report = await fetchMonthlyPerformance({ month: '2026-09', queryImpl });
    expect(report.financials).toMatchObject({ count: 2, billed: 120, paidOnInvoices: 40, collected: 80 });
    expect(report.previousMonth).toMatchObject({ month: '2026-08', billed: 100, collected: 80 });
    expect(report.coverage).toMatchObject({ completedVisits: 1001, trackedVisits: 0 });
    expect(report.visitValue).toBe(50050);
    expect(queryImpl).toHaveBeenCalledTimes(4);
  });
});
