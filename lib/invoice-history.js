function parseEventDate(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatCurrency(amount) {
  const parsed = Number(amount || 0);
  return `$${parsed.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function titleCase(value) {
  return String(value || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function paymentMethodLabel(method) {
  const map = {
    card: 'Credit Card',
    credit_card: 'Credit Card',
    debit_card: 'Debit Card',
    ach: 'ACH Bank Transfer',
    apple_pay: 'Apple Pay',
    cash: 'Cash',
    check: 'Check',
    venmo: 'Venmo',
    zelle: 'Zelle',
    money_order: 'Money Order',
    manual: 'Manual Payment',
  };
  return map[method] || titleCase(method || 'Payment');
}

function buildPaymentEvent(payment) {
  const date = parseEventDate(payment.paid_at || payment.created_at);
  if (!date) return null;

  const status = String(payment.status || '').trim().toLowerCase();
  const amount = formatCurrency(payment.amount);
  const methodLabel = paymentMethodLabel(payment.method);
  const details = [];

  if (payment.card_last4) details.push(`Card ending in ${payment.card_last4}`);
  if (payment.ach_bank_name) details.push(`Bank: ${payment.ach_bank_name}`);
  if (payment.square_receipt_url) details.push('Square receipt available');
  if (payment.notes) details.push(String(payment.notes).trim());

  if (status === 'pending') {
    return {
      type: 'payment-pending',
      date,
      badge: 'Payment Pending',
      title: `${amount} via ${methodLabel}`,
      detail: details.join(' • ') || 'Awaiting settlement',
    };
  }

  if (status === 'failed') {
    return {
      type: 'payment-failed',
      date,
      badge: 'Payment Failed',
      title: `${amount} via ${methodLabel}`,
      detail: details.join(' • '),
    };
  }

  return {
    type: 'paid',
    date,
    badge: 'Payment Received',
    title: `${amount} via ${methodLabel}`,
    detail: details.join(' • '),
  };
}

function buildEmailEvent(email) {
  const date = parseEventDate(email.sent_at);
  if (!date) return null;

  const emailType = String(email.email_type || '').trim().toLowerCase();
  const status = String(email.status || '').trim().toLowerCase();
  const recipient = String(email.recipient_email || '').trim();
  const failed = status && status !== 'sent';

  if (emailType === 'invoice') {
    return {
      type: failed ? 'email-failed' : 'sent',
      date,
      badge: failed ? 'Email Failed' : 'Sent by Email',
      title: failed ? 'Invoice email failed' : 'Invoice emailed to customer',
      detail: recipient ? `Recipient: ${recipient}` : '',
    };
  }

  if (emailType === 'invoice_reminder') {
    return {
      type: failed ? 'email-failed' : 'reminder',
      date,
      badge: failed ? 'Reminder Failed' : 'Reminder Sent',
      title: failed ? 'Payment reminder email failed' : 'Payment reminder emailed',
      detail: recipient ? `Recipient: ${recipient}` : '',
    };
  }

  if (emailType === 'payment_receipt') {
    return {
      type: failed ? 'email-failed' : 'receipt',
      date,
      badge: failed ? 'Receipt Failed' : 'Receipt Sent',
      title: failed ? 'Payment receipt email failed' : 'Payment receipt emailed',
      detail: recipient ? `Recipient: ${recipient}` : '',
    };
  }

  if (emailType === 'late_fee') {
    return {
      type: failed ? 'email-failed' : 'notice',
      date,
      badge: failed ? 'Notice Failed' : 'Late Fee Notice',
      title: failed ? 'Late fee email failed' : 'Late fee notice emailed',
      detail: recipient ? `Recipient: ${recipient}` : '',
    };
  }

  return null;
}

function buildInvoiceHistoryEvents(invoice, options = {}) {
  const now = parseEventDate(options.now) || new Date();
  const payments = Array.isArray(options.payments) ? options.payments : [];
  const emailLog = Array.isArray(options.emailLog) ? options.emailLog : [];
  const events = [];

  const createdAt = parseEventDate(invoice.created_at);
  if (createdAt) {
    events.push({
      type: 'created',
      date: createdAt,
      badge: 'Created',
      title: 'Invoice created',
      detail: invoice.invoice_number ? `#${invoice.invoice_number}` : '',
    });
  }

  const emailEvents = emailLog
    .map(buildEmailEvent)
    .filter(Boolean);
  events.push(...emailEvents);

  const hasInvoiceEmailAttempt = emailLog.some(email => String(email.email_type || '').trim().toLowerCase() === 'invoice');
  const hasReminderEmailAttempt = emailLog.some(email => String(email.email_type || '').trim().toLowerCase() === 'invoice_reminder');

  const sentAt = parseEventDate(invoice.sent_at);
  if (sentAt && !hasInvoiceEmailAttempt) {
    events.push({
      type: 'sent',
      date: sentAt,
      badge: 'Sent',
      title: 'Invoice marked as sent',
      detail: invoice.customer_email ? `Recipient: ${invoice.customer_email}` : '',
    });
  }

  const viewedAt = parseEventDate(invoice.viewed_at);
  if (viewedAt) {
    events.push({
      type: 'viewed',
      date: viewedAt,
      badge: 'Viewed',
      title: 'Viewed by customer',
      detail: '',
    });
  }

  const reminderSentAt = parseEventDate(invoice.reminder_sent_at);
  if (reminderSentAt && !hasReminderEmailAttempt) {
    events.push({
      type: 'reminder',
      date: reminderSentAt,
      badge: 'Reminder Sent',
      title: `Payment reminder sent${invoice.reminder_count > 1 ? ` (${invoice.reminder_count} total)` : ''}`,
      detail: invoice.customer_email ? `Recipient: ${invoice.customer_email}` : '',
    });
  }

  if (invoice.due_date) {
    const dueDate = parseEventDate(invoice.due_date);
    const total = Number(invoice.total || 0);
    const amountPaid = Number(invoice.amount_paid || 0);
    if (dueDate && dueDate < now && amountPaid < total && String(invoice.status || '').toLowerCase() !== 'paid') {
      events.push({
        type: 'overdue',
        date: dueDate,
        badge: 'Overdue',
        title: 'Invoice became overdue',
        detail: '',
      });
    }
  }

  const paymentEvents = payments
    .map(buildPaymentEvent)
    .filter(Boolean);
  events.push(...paymentEvents);

  if (!paymentEvents.length) {
    const paidAt = parseEventDate(invoice.paid_at);
    if (paidAt && String(invoice.status || '').toLowerCase() === 'paid') {
      events.push({
        type: 'paid',
        date: paidAt,
        badge: 'Paid',
        title: `Payment received — ${formatCurrency(invoice.total)}`,
        detail: '',
      });
    }
  }

  events.sort((a, b) => b.date - a.date);
  return events;
}

module.exports = {
  buildInvoiceHistoryEvents,
};
