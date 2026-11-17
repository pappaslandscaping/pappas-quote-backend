'use strict';
const { loadInvoices, loadPayments, cents, SOURCE } = require('./records');
async function taxSnapshot(context, { startDate, endDate, basis = 'collected' }) {
  if (!startDate || !endDate) throw new Error('Choose a tax report date range.');
  if (basis === 'collected') {
    const payments = await loadPayments(context, { date: { gte: startDate, lte: endDate }, isRefund: false });
    // Payment allocations are available, but the official API has no authoritative
    // collected-tax report. Never substitute an estimate for the transfer amount.
    const error = new Error(
      'Homeworks does not expose its collected-tax report through the official API. Review collected tax in Homeworks before creating a tax transfer.',
    );
    error.code = 'HOMEWORKS_COLLECTED_TAX_REVIEW_REQUIRED';
    error.paymentCount = payments.length;
    throw error;
  }
  const invoices = await loadInvoices(context, { date: { gte: startDate, lte: endDate } });
  const issued = invoices.filter(
    (row) => row.homeworks.isSent && !['DRAFT', 'WRITE_OFF'].includes(row.homeworks.status),
  );
  const sum = (field) => issued.reduce((total, row) => total + cents(row.homeworks[field]), 0) / 100;
  return {
    success: true,
    source: SOURCE,
    as_of: new Date().toISOString(),
    basis,
    start_date: startDate,
    end_date: endDate,
    rows: [
      {
        tax_rate: 0,
        raw_tax_rate: 'Mixed rates',
        total_sales: sum('subtotal'),
        taxable_amount: 0,
        discount: sum('discountAmount'),
        tax_amount: sum('tax'),
      },
    ],
    total_sales: sum('subtotal'),
    taxable_amount: null,
    discount: sum('discountAmount'),
    tax_amount: sum('tax'),
    processing_fees: sum('processingFee'),
    tips: 0,
    external_metadata: { source: SOURCE, invoice_count: issued.length, taxable_amount_available: false },
  };
}
module.exports = { taxSnapshot };
