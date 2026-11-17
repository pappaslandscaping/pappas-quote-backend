const status = value => String(value || '').trim().toUpperCase();
const dateOnly = value => /^\d{4}-\d{2}-\d{2}$/.test(String(value || '')) ? String(value) : null;

function scoreCustomerRelationship({ customer, localType, lastCompletedVisit, nextScheduledVisit,
  completedVisitsLast12Months = 0, estimates = [], today }) {
  const todayDate = dateOnly(today);
  if (!todayDate) throw new Error('A current HomeWorks calendar date is required.');
  const priorCustomer = Boolean(lastCompletedVisit)
    || estimates.some(estimate => ['ACCEPTED', 'INVOICED'].includes(status(estimate.status)))
    || status(localType) === 'CUSTOMER';
  let lead;
  if (priorCustomer) {
    lead = { label: 'Existing customer', score: null, applicable: false,
      factors: ['Lead scoring applies to prospects, not established customers.'] };
  } else {
    let score = 0;
    const factors = [];
    if (customer.email || customer.phone || customer.cell) {
      score += 10; factors.push('Contact method on file (+10)');
    }
    if (customer.address?.street1) {
      score += 10; factors.push('Property address on file (+10)');
    }
    const pending = estimates.find(estimate => status(estimate.status) === 'PENDING');
    if (pending) {
      score += 25; factors.push('Pending estimate (+25)');
      if (pending.isSent) { score += 15; factors.push('Estimate sent through HomeWorks (+15)'); }
      const estimateDate = dateOnly(pending.date);
      const age = estimateDate ? (Date.parse(todayDate) - Date.parse(estimateDate)) / 86400000 : NaN;
      if (age >= 0 && age <= 30) { score += 10; factors.push('Estimate dated within 30 days (+10)'); }
    }
    lead = { label: score >= 50 ? 'Hot' : score >= 25 ? 'Warm' : 'Cold', score,
      applicable: true, factors: factors.length ? factors : ['No qualifying prospect activity found.'] };
  }

  let churn;
  if (!priorCustomer) {
    churn = { label: 'Not applicable', score: null, applicable: false,
      factors: ['No established service relationship to assess.'] };
  } else if (status(customer.status) !== 'ACTIVE') {
    churn = { label: 'Needs review', score: null, applicable: true,
      factors: [`HomeWorks account status: ${status(customer.status) || 'unknown'}.`] };
  } else if (Number(customer.pastDue) > 0) {
    churn = { label: 'Needs review', score: null, applicable: true,
      factors: ['A past-due balance needs account review; it does not establish cancellation.'] };
  } else if (nextScheduledVisit) {
    churn = { label: 'Low', score: 0, applicable: true,
      factors: ['Upcoming HomeWorks visit scheduled.',
        ...(completedVisitsLast12Months > 0 ? [`${completedVisitsLast12Months} completed visit${completedVisitsLast12Months === 1 ? '' : 's'} in the past 12 months.`] : [])] };
  } else {
    churn = { label: 'Undetermined', score: null, applicable: true,
      factors: ['No upcoming HomeWorks visit is scheduled.', 'A seasonal gap alone does not establish churn.'] };
  }
  return { lead, churn };
}

module.exports = { scoreCustomerRelationship };
