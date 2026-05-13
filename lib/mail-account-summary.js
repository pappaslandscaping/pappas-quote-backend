function resolveMailAccountSummary({
  invoiceTotal,
  amountPaid,
  metadata,
  parseMoney,
  roundMoney,
}) {
  const meta = metadata && typeof metadata === 'object' ? metadata : {};
  const normalizedInvoiceTotal = roundMoney(parseMoney(invoiceTotal) ?? 0);
  const normalizedAmountPaid = roundMoney(Math.max(0, parseMoney(amountPaid) ?? 0));
  const invoiceBalance = roundMoney(Math.max(0, normalizedInvoiceTotal - normalizedAmountPaid));

  const customerOutstandingBalance = parseMoney(meta.customer_outstanding_balance);
  const explicitPriorBalance = parseMoney(
    meta.prior_balance
    ?? meta.previous_balance
    ?? meta.past_due_balance
  );
  const metadataAccountDue = parseMoney(meta.total_due_on_account ?? meta.total_due);
  const metadataOutstanding = parseMoney(meta.outstanding_balance);
  const metadataThisInvoice = parseMoney(meta.this_invoice);

  const storedOutstandingIsPriorBalance = (
    metadataOutstanding !== null
    && metadataOutstanding > 0
    && metadataThisInvoice !== null
    && Math.abs(metadataThisInvoice - normalizedInvoiceTotal) <= 0.02
    && metadataAccountDue !== null
    && Math.abs(metadataAccountDue - roundMoney(metadataThisInvoice + metadataOutstanding)) <= 0.02
  );

  let priorBalance = 0;
  if (
    normalizedAmountPaid > 0
    && customerOutstandingBalance !== null
    && Math.abs(customerOutstandingBalance - invoiceBalance) <= 0.02
  ) {
    priorBalance = 0;
  } else if (customerOutstandingBalance !== null && customerOutstandingBalance > 0) {
    if (Math.abs(customerOutstandingBalance - invoiceBalance) <= 0.02) {
      priorBalance = 0;
    } else if (customerOutstandingBalance > invoiceBalance + 0.009) {
      priorBalance = roundMoney(customerOutstandingBalance - invoiceBalance);
    } else {
      priorBalance = roundMoney(customerOutstandingBalance);
    }
  } else if (explicitPriorBalance !== null && explicitPriorBalance > 0) {
    priorBalance = roundMoney(explicitPriorBalance);
  } else if (storedOutstandingIsPriorBalance) {
    priorBalance = roundMoney(metadataOutstanding);
  } else if (metadataAccountDue !== null && metadataAccountDue > normalizedInvoiceTotal + 0.009) {
    priorBalance = roundMoney(metadataAccountDue - normalizedInvoiceTotal);
  } else if (metadataOutstanding !== null && metadataOutstanding > normalizedInvoiceTotal + 0.009) {
    priorBalance = roundMoney(metadataOutstanding - normalizedInvoiceTotal);
  }

  return {
    priorBalance,
    paymentCredit: normalizedAmountPaid,
    thisInvoice: invoiceBalance,
    invoiceTotal: normalizedInvoiceTotal,
    totalDueOnAccount: roundMoney(invoiceBalance + priorBalance),
  };
}

module.exports = {
  resolveMailAccountSummary,
};
