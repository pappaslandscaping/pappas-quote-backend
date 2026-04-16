function cleanStatusValue(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeCopilotSentStatus(rawSentStatus) {
  const raw = cleanStatusValue(rawSentStatus);
  if (!raw) return null;

  const s = raw.toLowerCase();
  if (s === 'no' || s.includes('not sent') || s.includes('unsent')) return 'not sent';
  if (s === 'yes' || s === 'sent' || s.includes('delivered')) return 'sent';
  if (s.includes('viewed') || s.includes('opened') || s.includes('read')) return 'viewed';

  return raw.toLowerCase();
}

function parseAmount(value) {
  const n = parseFloat(value || 0);
  return Number.isFinite(n) ? n : 0;
}

function hasPastDueDate(dueDate) {
  if (!dueDate) return false;
  const due = new Date(dueDate);
  if (Number.isNaN(due.getTime())) return false;
  due.setHours(0, 0, 0, 0);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return due < today;
}

function normalizeCopilotInvoiceStatus({
  rawStatus,
  sentStatus,
  amountPaid,
  total,
  dueDate,
  defaultUnpaidStatus = null,
} = {}) {
  const paid = parseAmount(amountPaid);
  const grandTotal = parseAmount(total);
  const balance = Math.max(0, grandTotal - paid);
  const normalizedSentStatus = normalizeCopilotSentStatus(sentStatus);
  const raw = cleanStatusValue(rawStatus).toLowerCase();

  if (raw) {
    if (raw.includes('paid in full') || raw === 'paid') return 'paid';
    if (raw.includes('partial')) return balance > 0 ? 'partial' : 'paid';
    if (raw.includes('written off') || raw.includes('write off') || raw.includes('writeoff')) return 'void';
    if (raw.includes('void') || raw.includes('cancel')) return 'void';
    if (raw.includes('draft')) return 'draft';
    if (raw.includes('overdue') || raw.includes('past due')) return balance > 0 ? 'overdue' : 'paid';
    if (raw.includes('pending') || raw.includes('open')) {
      if (grandTotal > 0 && paid >= grandTotal) return 'paid';
      if (paid > 0 && balance > 0) return 'partial';
      return 'pending';
    }
    if (raw.includes('sent')) {
      if (grandTotal > 0 && paid >= grandTotal) return 'paid';
      if (paid > 0 && balance > 0) return 'partial';
      return hasPastDueDate(dueDate) ? 'overdue' : 'sent';
    }
  }

  if (grandTotal > 0 && paid >= grandTotal) return 'paid';
  if (paid > 0 && balance > 0) return 'partial';
  if (normalizedSentStatus === 'not sent' && balance > 0) return 'pending';
  if ((normalizedSentStatus === 'sent' || normalizedSentStatus === 'viewed') && balance > 0) {
    return hasPastDueDate(dueDate) ? 'overdue' : 'sent';
  }
  if (balance > 0 && hasPastDueDate(dueDate)) return 'overdue';

  return defaultUnpaidStatus;
}

function normalizeStoredInvoiceStatus(status, dueDate, total, amountPaid) {
  const normalized = cleanStatusValue(status).toLowerCase();
  if (!normalized) {
    return normalizeCopilotInvoiceStatus({ dueDate, total, amountPaid, defaultUnpaidStatus: 'draft' }) || 'draft';
  }
  if (normalized === 'sent' && hasPastDueDate(dueDate) && parseAmount(total) > parseAmount(amountPaid)) {
    return 'overdue';
  }
  if (normalized === 'pending' && hasPastDueDate(dueDate) && parseAmount(total) > parseAmount(amountPaid)) {
    return 'overdue';
  }
  return normalized;
}

function isOutstandingInvoice(status, total, amountPaid) {
  const normalized = cleanStatusValue(status).toLowerCase();
  const balance = Math.max(0, parseAmount(total) - parseAmount(amountPaid));
  if (balance <= 0) return false;
  return !['paid', 'void', 'draft'].includes(normalized);
}

module.exports = {
  cleanStatusValue,
  normalizeCopilotSentStatus,
  normalizeCopilotInvoiceStatus,
  normalizeStoredInvoiceStatus,
  isOutstandingInvoice,
  hasPastDueDate,
};
