const {
  fetchMobileCustomerSnapshot,
  fetchMobileCustomers,
  fetchMobileToday,
} = require('./mobile-app');

const operationalQuestion = /\b(customers?|clients?|jobs?|schedules?|routes?|crews?|visits?|invoices?|payments?|balances?|past due|unpaid|overdue|owes?|owed|estimates?|quotes?|service history|today|tomorrow|calls?|texts?|messages?|voicemails?|emails?)\b/i;
const scheduleQuestion = /\b(jobs?|schedules?|routes?|crews?|visits?|today|tomorrow)\b/i;
const customerQuestion = /\b(customers?|clients?|invoices?|payments?|balances?|past due|unpaid|overdue|owes?|owed|estimates?|quotes?|service history)\b/i;

function localDate(now = new Date(), offset = 0) {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const today = `${values.year}-${values.month}-${values.day}`;
  const date = new Date(`${today}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offset);
  return date.toISOString().slice(0, 10);
}

function scheduleDate(question, now = new Date()) {
  if (!scheduleQuestion.test(question)) return null;
  const explicit = String(question).match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (explicit) return explicit[1];
  if (/\b(mon(day)?|tue(s(day)?)?|wed(nesday)?|thu(rs(day)?)?|fri(day)?|sat(urday)?|sun(day)?|next week|last week|this week|january|february|march|april|june|july|august|september|october|november|december)\b/i.test(question)) return null;
  return localDate(now, /\btomorrow\b/i.test(question) ? 1 : 0);
}

function matchCustomer(question, customers) {
  const text = String(question).toLowerCase();
  const matches = customers.filter((customer) => {
    const name = String(customer.name || '').trim().toLowerCase();
    return name.length > 3 && text.includes(name);
  });
  if (matches.length === 1) return { customer: matches[0] };
  if (matches.length > 1) return { ambiguous: matches.map((customer) => customer.name) };
  return {};
}

function compactSnapshot(snapshot) {
  const activeJobs = (snapshot.jobs || []).filter((job) => !['CANCELLED', 'CANCELED'].includes(String(job.status || '').toUpperCase()));
  return {
    customer: snapshot.customer,
    properties: (snapshot.properties || []).slice(0, 15),
    jobs: activeJobs.slice(0, 50),
    jobsLimited: activeJobs.length > 50,
    estimates: (snapshot.estimates || []).slice(0, 25),
    invoices: (snapshot.invoices || []).slice(0, 25),
    invoicesLimited: (snapshot.invoices || []).length >= 25,
    payments: (snapshot.payments || []).slice(0, 25),
  };
}

async function buildAssistantContext({ pool, question, now = new Date(), fetchCustomers = fetchMobileCustomers, fetchSnapshot = fetchMobileCustomerSnapshot, fetchToday = fetchMobileToday }) {
  const checkedAt = now.toISOString();
  const sources = [];
  const context = {};
  const date = scheduleDate(question, now);
  if (scheduleQuestion.test(question) && !date) context.dateNeedsClarification = true;
  if (date) {
    try {
      const today = await fetchToday({ pool, date });
      const activeJobs = (today.jobs || []).filter((job) => !['CANCELLED', 'CANCELED'].includes(String(job.status || '').toUpperCase()));
      context.schedule = { date, jobs: activeJobs.slice(0, 150), totalJobs: activeJobs.length, limited: activeJobs.length > 150 };
      sources.push({ name: 'HomeWorks schedule', status: 'verified', checkedAt });
    } catch {
      sources.push({ name: 'HomeWorks schedule', status: 'unavailable', checkedAt });
    }
  }

  if (customerQuestion.test(question)) {
    try {
      const customers = await fetchCustomers({ pool });
      const match = matchCustomer(question, customers);
      if (match.ambiguous) context.customerAmbiguity = match.ambiguous;
      else if (match.customer) {
        try {
          const snapshot = await fetchSnapshot({ pool, customerId: match.customer.id });
          if (snapshot) {
            context.customer = compactSnapshot(snapshot);
            sources.push({ name: 'HomeWorks customer', status: 'verified', checkedAt });
          } else sources.push({ name: 'HomeWorks customer', status: 'unavailable', checkedAt });
        } catch {
          sources.push({ name: 'HomeWorks customer', status: 'unavailable', checkedAt });
        }
      } else context.customerNotIdentified = true;
    } catch {
      sources.push({ name: 'HomeWorks customer', status: 'unavailable', checkedAt });
    }
  }

  return { checkedAt, context, sources, requiresVerification: operationalQuestion.test(question) };
}

module.exports = { buildAssistantContext, compactSnapshot, localDate, matchCustomer, scheduleDate };
