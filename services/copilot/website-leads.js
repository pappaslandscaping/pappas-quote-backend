const {
  getHomeWorksAccessToken,
  queryHomeWorksGraphql,
} = require('../homeworks/client');
const SUPPORTED_CITIES = new Set([
  'lakewood', 'bay village', 'brook park',
]);
// Keep older city names recognizable in free-form addresses so an out-of-area
// request is not mistaken for one with an unknown city.
const RECOGNIZED_CITIES = new Set([
  ...SUPPORTED_CITIES, 'cleveland', 'rocky river', 'fairview park',
  'parma', 'north olmsted', 'avon', 'avon lake', 'westlake',
  'north royalton', 'strongsville', 'berea', 'middleburg heights',
  'olmsted falls',
]);

function normalizePhone(value) {
  const digits = String(value || '').replace(/\D/g, '');
  return digits.length === 11 && digits.startsWith('1') ? digits.slice(1) : digits;
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeAddress(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b(street)\b/g, 'st')
    .replace(/\b(avenue)\b/g, 'ave')
    .replace(/\b(road)\b/g, 'rd')
    .replace(/\b(drive)\b/g, 'dr')
    .replace(/\b(boulevard)\b/g, 'blvd')
    .replace(/\b(lane)\b/g, 'ln')
    .replace(/[^a-z0-9]/g, '');
}

function parseAddress(value, cityHint) {
  const raw = String(value || '').trim();
  const parts = raw.split(',').map(part => part.trim()).filter(Boolean);
  const result = { street1: parts[0] || raw, city: '', state: 'OH', zip: '', country: 'US' };

  if (parts.length >= 2) result.city = parts[1];
  const stateZip = parts.slice(2).join(' ');
  const stateZipMatch = stateZip.match(/\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b/i);
  if (stateZipMatch) {
    result.state = stateZipMatch[1].toUpperCase();
    result.zip = stateZipMatch[2];
  }

  if (!result.city) {
    const cityNames = [...RECOGNIZED_CITIES].sort((a, b) => b.length - a.length);
    const lowered = raw.toLowerCase();
    const found = cityNames.find(city => new RegExp(`(?:,|\\s)${city.replace(/ /g, '\\s+')}\\s*,?\\s*(?:oh)?\\s*\\d{0,5}$`, 'i').test(lowered));
    if (found) result.city = found.replace(/\b\w/g, char => char.toUpperCase());
  }
  if (!result.city && cityHint) result.city = String(cityHint).trim();
  if (!result.zip) {
    const zipMatch = raw.match(/\b(\d{5})(?:-\d{4})?\b/);
    if (zipMatch) result.zip = zipMatch[1];
  }
  return result;
}

function classifyServiceArea(address, cityHint) {
  const parsed = parseAddress(address, cityHint);
  const city = String(parsed.city || '').trim().toLowerCase();
  if (city === 'cleveland') {
    return { status: 'review', city: parsed.city, reason: 'Only the west side of Cleveland is served; confirm the property address before routing.' };
  }
  if (SUPPORTED_CITIES.has(city)) {
    return { status: 'inside', city: parsed.city, reason: 'Address is in the current service area.' };
  }
  if (city) return { status: 'outside', city: parsed.city, reason: `${parsed.city} is outside the current service area.` };
  return { status: 'review', city: parsed.city, reason: 'City could not be confirmed from the address.' };
}

function splitName(fullName) {
  const parts = String(fullName || '').trim().split(/\s+/).filter(Boolean);
  return { firstName: parts.shift() || '', lastName: parts.join(' ') || '' };
}

function requestSummary(quote) {
  const questions = quote.questions && typeof quote.questions === 'object' ? quote.questions : {};
  const lines = [
    'Website quote request',
    quote.services?.length ? `Services: ${quote.services.join(', ')}` : null,
    quote.package && quote.package !== 'None' ? `Package: ${quote.package}` : null,
    questions.frequency ? `Frequency: ${questions.frequency}` : null,
    questions.contactMethod ? `Preferred contact: ${questions.contactMethod}` : null,
    questions.startTime ? `Start timing: ${questions.startTime}` : null,
    questions.gate ? `Gate: ${questions.gate}` : null,
    questions.dogs ? `Dogs: ${questions.dogs}` : null,
    questions.lawnHeight ? `Lawn over 6 inches: ${questions.lawnHeight}` : null,
    questions.backyardAccess ? `Backyard access: ${questions.backyardAccess}` : null,
    quote.notes ? `Notes: ${quote.notes}` : null,
    quote.source ? `Found us through: ${quote.source}` : null,
  ];
  return lines.filter(Boolean).join('\n');
}

function matchCustomers(customers, quote) {
  const email = normalizeEmail(quote.email);
  const phone = normalizePhone(quote.phone);
  return customers.filter(customer => {
    const emailMatch = email && normalizeEmail(customer.email) === email;
    const phoneMatch = phone && [customer.phone, customer.cell].some(value => normalizePhone(value) === phone);
    return emailMatch || phoneMatch;
  });
}

function hasMatchingProperty(customer, quoteAddress) {
  const wanted = normalizeAddress(quoteAddress);
  if (!wanted) return null;
  return (customer.properties || []).find(property => {
    const address = property.address || {};
    return normalizeAddress([address.street1, address.city, address.state, address.zip].filter(Boolean).join(' ')) === wanted;
  }) || null;
}

async function updateSyncState(pool, quoteId, values) {
  const fields = [];
  const params = [];
  for (const [column, value] of Object.entries(values)) {
    params.push(value);
    fields.push(`${column} = $${params.length}`);
  }
  params.push(quoteId);
  await pool.query(`UPDATE quotes SET ${fields.join(', ')}, updated_at = CURRENT_TIMESTAMP WHERE id = $${params.length}`, params);
}

async function addLeadEvent(pool, quoteId, type, description, details = null) {
  await pool.query(
    'INSERT INTO quote_lead_events (quote_id, event_type, description, details) VALUES ($1, $2, $3, $4)',
    [quoteId, type, description, details ? JSON.stringify(details) : null]
  ).catch(() => {});
}

async function syncWebsiteLead({ pool, quoteId, force = false }) {
  const quoteResult = await pool.query('SELECT * FROM quotes WHERE id = $1', [quoteId]);
  const quote = quoteResult.rows[0];
  if (!quote) throw new Error('Quote request not found');
  if (!force && ['created', 'linked'].includes(quote.homeworks_sync_status)) return quote;

  await updateSyncState(pool, quoteId, { homeworks_sync_status: 'checking', homeworks_sync_error: null });
  const accessToken = await getHomeWorksAccessToken(pool);
  if (!accessToken) {
    await updateSyncState(pool, quoteId, { homeworks_sync_status: 'unavailable', homeworks_sync_error: 'HomeWorks connection is not available.' });
    return null;
  }

  const phoneDigits = normalizePhone(quote.phone);
  const phoneVariants = [...new Set([quote.phone, phoneDigits, phoneDigits ? `1${phoneDigits}` : '', phoneDigits ? `+1${phoneDigits}` : ''].filter(Boolean))];
  const filters = [];
  if (quote.email) {
    const email = String(quote.email).trim();
    filters.push({ email: { equals: email } }, { email: { contains: email } });
  }
  if (phoneVariants.length) {
    filters.push(
      { phone: { in: phoneVariants } },
      { cell: { in: phoneVariants } },
      { phone: { contains: phoneDigits.slice(-7) } },
      { cell: { contains: phoneDigits.slice(-7) } }
    );
  }

  const lookup = await queryHomeWorksGraphql({ pool, accessToken, query: `query WebsiteLeadMatch($filters: [CustomerFilter!]!) {
    customerTypes { id name }
    customers(take: 50, where: { isDeleted: false, OR: $filters }) {
      id fullName firstName lastName email phone cell customerType { id name }
      properties(where: { isActive: true }) { id name address { street1 street2 city state zip country } }
    }
  }`, variables: { filters } });

  const matches = matchCustomers(lookup.customers || [], quote);
  if (matches.length > 1) {
    await updateSyncState(pool, quoteId, {
      homeworks_sync_status: 'review',
      homeworks_match_type: 'ambiguous',
      homeworks_sync_error: 'More than one HomeWorks customer matches this email or phone.',
    });
    await addLeadEvent(pool, quoteId, 'homeworks_review', 'HomeWorks found multiple possible customer matches.');
    return null;
  }

  if (matches.length === 1) {
    const customer = matches[0];
    const property = hasMatchingProperty(customer, quote.address);
    const addressConflict = !property;
    await updateSyncState(pool, quoteId, {
      homeworks_sync_status: addressConflict ? 'review' : 'linked',
      homeworks_customer_id: customer.id,
      homeworks_property_id: property?.id || null,
      homeworks_match_type: property ? 'existing_customer_property' : 'existing_customer',
      homeworks_sync_error: addressConflict ? 'Customer matched, but the request address does not match an existing property.' : null,
      homeworks_synced_at: new Date(),
    });
    await addLeadEvent(pool, quoteId, addressConflict ? 'homeworks_review' : 'homeworks_linked', addressConflict
      ? 'Matched an existing HomeWorks customer. The property address needs review.'
      : 'Linked to an existing HomeWorks customer.');
    return customer;
  }

  const leadType = (lookup.customerTypes || []).find(type => String(type.name).trim().toLowerCase() === 'lead');
  if (!leadType) throw new Error('The HomeWorks Lead customer type was not found.');

  const questions = quote.questions && typeof quote.questions === 'object' ? quote.questions : {};
  const consent = questions.smsConsent || {};
  const name = splitName(quote.name);
  const address = parseAddress(quote.address, questions.city);
  const created = await queryHomeWorksGraphql({ pool, accessToken, query: `mutation CreateWebsiteLead($input: CustomerInput!) {
    createCustomer(input: $input) { id fullName email phone cell }
  }`, variables: { input: {
    fullName: quote.name,
    firstName: name.firstName,
    lastName: name.lastName,
    email: quote.email || undefined,
    phone: quote.phone || undefined,
    cell: quote.phone || undefined,
    address,
    customerTypeId: leadType.id,
    description: requestSummary(quote),
    isReceivePhone: true,
    isReceiveEmail: Boolean(quote.email),
    isReceiveText: consent.transactional === true,
  } } });

  const customer = created.createCustomer;
  let property = null;
  try {
    const propertyData = await queryHomeWorksGraphql({ pool, accessToken, query: `mutation CreateWebsiteLeadProperty($input: PropertyInput!) {
      createProperty(input: $input) { id name }
    }`, variables: { input: {
      customerId: customer.id,
      name: address.street1 || `${quote.name} property`,
      address,
      notes: requestSummary(quote),
      tags: ['Website Lead'],
    } } });
    property = propertyData.createProperty;
  } catch (error) {
    await updateSyncState(pool, quoteId, {
      homeworks_sync_status: 'partial',
      homeworks_customer_id: customer.id,
      homeworks_match_type: 'new_lead',
      homeworks_sync_error: `Lead created, but property creation failed: ${error.message}`,
      homeworks_synced_at: new Date(),
    });
    await addLeadEvent(pool, quoteId, 'homeworks_partial', 'Created the HomeWorks lead, but the property needs attention.');
    return customer;
  }

  await updateSyncState(pool, quoteId, {
    homeworks_sync_status: 'created',
    homeworks_customer_id: customer.id,
    homeworks_property_id: property.id,
    homeworks_match_type: 'new_lead',
    homeworks_sync_error: null,
    homeworks_synced_at: new Date(),
  });
  await addLeadEvent(pool, quoteId, 'homeworks_created', 'Created a new HomeWorks Lead and property.');
  return customer;
}

module.exports = {
  SUPPORTED_CITIES,
  classifyServiceArea,
  hasMatchingProperty,
  matchCustomers,
  normalizeAddress,
  normalizeEmail,
  normalizePhone,
  parseAddress,
  requestSummary,
  syncWebsiteLead,
};
