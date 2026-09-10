const {
  classifyServiceArea,
  matchCustomers,
  normalizeAddress,
  normalizePhone,
  parseAddress,
  requestSummary,
} = require('../services/copilot/website-leads');

describe('website lead integration helpers', () => {
  test('normalizes US phone numbers without changing the final ten digits', () => {
    expect(normalizePhone('+1 (216) 469-9356')).toBe('2164699356');
    expect(normalizePhone('216-469-9356')).toBe('2164699356');
  });

  test('matches customers by normalized email or phone', () => {
    const customers = [
      { id: 1, email: 'person@example.com', phone: '', cell: '(216) 469-9356' },
      { id: 2, email: 'someone@example.com', phone: '4405550000', cell: '' },
    ];
    expect(matchCustomers(customers, { email: '', phone: '216-469-9356' }).map(c => c.id)).toEqual([1]);
    expect(matchCustomers(customers, { email: 'PERSON@example.com', phone: '' }).map(c => c.id)).toEqual([1]);
  });

  test('treats Westlake city as outside without confusing Westlake Avenue in Lakewood', () => {
    expect(classifyServiceArea('123 Main St, Westlake, OH 44145').status).toBe('outside');
    expect(classifyServiceArea('1283 Westlake Avenue, Lakewood, OH 44107').status).toBe('inside');
  });

  test('parses a Google-formatted address for HomeWorks', () => {
    expect(parseAddress('1090 Erie Cliff Drive, Lakewood, OH 44107')).toEqual({
      street1: '1090 Erie Cliff Drive', city: 'Lakewood', state: 'OH', zip: '44107', country: 'US',
    });
  });

  test('normalizes common street suffixes for property matching', () => {
    expect(normalizeAddress('1090 Erie Cliff Drive, Lakewood OH 44107')).toBe(normalizeAddress('1090 Erie Cliff Dr Lakewood, OH 44107'));
  });

  test('builds a useful HomeWorks handoff summary', () => {
    const summary = requestSummary({
      services: ['Lawn Maintenance'],
      package: 'None',
      source: 'Google Search',
      notes: 'Please call first',
      questions: { frequency: 'Every two weeks', contactMethod: 'Text' },
    });
    expect(summary).toContain('Services: Lawn Maintenance');
    expect(summary).toContain('Frequency: Every two weeks');
    expect(summary).toContain('Found us through: Google Search');
  });
});
