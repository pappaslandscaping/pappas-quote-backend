const status = value => String(value || '').trim().toUpperCase();
const dateOnly = value => /^\d{4}-\d{2}-\d{2}$/.test(String(value || '')) ? String(value) : null;
const daysBetween = (later, earlier) => {
  const a = dateOnly(later), b = dateOnly(earlier);
  return a && b ? (Date.parse(a) - Date.parse(b)) / 86400000 : NaN;
};
function offSeasonFor(title, month) {
  const service = String(title || '').toLowerCase();
  if (/fertiliz|lawn treatment/.test(service)) return month < 3 || month > 10;
  if (/mow|lawn care/.test(service)) return month < 4 || month > 11;
  if (/snow|ice/.test(service)) return month >= 4 && month <= 10;
  if (/fall cleanup|leaf cleanup/.test(service)) return month < 9;
  if (/spring cleanup/.test(service)) return month < 3 || month > 6;
  return false;
}

function scoreCustomerRelationship({ customer, localType, lastCompletedVisit, nextScheduledVisit,
  completedVisitsLast12Months = 0, recentVisits = [], estimates = [], today }) {
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
      const age = daysBetween(todayDate, pending.date);
      if (age >= 0 && age <= 30) { score += 10; factors.push('Estimate dated within 30 days (+10)'); }
    }
    lead = { label: score >= 50 ? 'Hot' : score >= 25 ? 'Warm' : 'Cold', score,
      applicable: true, factors: factors.length ? factors : ['No qualifying prospect activity found.'] };
  }

  let churn;
  const pastWaitlisted = recentVisits.find(visit => status(visit.status) === 'WAITLISTED'
    && dateOnly(visit.startDate) && visit.startDate < todayDate);
  const futureWaitlisted = recentVisits.find(visit => status(visit.status) === 'WAITLISTED'
    && dateOnly(visit.startDate) && visit.startDate >= todayDate);
  const pendingRenewal = estimates.find(estimate => status(estimate.status) === 'PENDING'
    && estimate.isSent && daysBetween(todayDate, estimate.date) >= 0
    && daysBetween(todayDate, estimate.date) <= 180);
  const lastServiceAge = daysBetween(todayDate, lastCompletedVisit?.startDate);
  const month = Number(todayDate.slice(5, 7));
  if (!priorCustomer) {
    churn = { label: 'Not applicable', score: null, applicable: false,
      factors: ['No established service relationship to assess.'] };
  } else if (status(customer.status) !== 'ACTIVE') {
    churn = { label: 'Needs review', score: null, applicable: true,
      factors: [`HomeWorks account status: ${status(customer.status) || 'unknown'}.`] };
  } else if (nextScheduledVisit) {
    churn = { label: 'Low', score: 0, applicable: true,
      factors: ['Upcoming HomeWorks visit scheduled.',
        ...(completedVisitsLast12Months > 0 ? [`${completedVisitsLast12Months} completed visit${completedVisitsLast12Months === 1 ? '' : 's'} in the past 12 months.`] : [])] };
  } else if (pastWaitlisted) {
    churn = { label: 'Needs review', score: null, applicable: true,
      factors: [`${pastWaitlisted.title || 'Visit'} dated ${pastWaitlisted.startDate} remains waitlisted in HomeWorks.`,
        'Confirm the service plan before assessing customer retention.'] };
  } else if (futureWaitlisted) {
    churn = { label: 'Planned work', score: null, applicable: true,
      factors: [`${futureWaitlisted.title || 'Visit'} is waitlisted for ${futureWaitlisted.startDate}; its schedule is unconfirmed.`] };
  } else if (pendingRenewal) {
    churn = { label: 'Estimate pending', score: null, applicable: true,
      factors: [`Estimate #${pendingRenewal.number || pendingRenewal.id} remains pending in HomeWorks.`,
        'A pending renewal does not establish customer loss.'] };
  } else if (lastServiceAge >= 0 && lastServiceAge <= 366
    && offSeasonFor(lastCompletedVisit?.title, month)) {
    churn = { label: 'Seasonal pause', score: null, applicable: true,
      factors: [`Last service: ${lastCompletedVisit.title || 'Visit'} on ${lastCompletedVisit.startDate}.`,
        'No upcoming visit is expected solely from an off-season gap.'] };
  } else if (lastServiceAge > 366) {
    churn = { label: 'Needs review', score: null, applicable: true,
      factors: ['No completed visit in more than a year and no upcoming visit is scheduled.',
        'Review service history before contacting the customer.'] };
  } else {
    churn = { label: 'Undetermined', score: null, applicable: true,
      factors: ['No upcoming HomeWorks visit is scheduled.', 'Service timing alone does not establish churn.'] };
  }
  return { lead, churn };
}

module.exports = { scoreCustomerRelationship };
