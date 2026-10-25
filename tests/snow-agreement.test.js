const agreement = require('../public/js/snow-agreement');
const { generateSnowAgreementPDF } = require('../lib/snow-agreement-pdf');
const { syncSnowAgreementToHomeWorks } = require('../lib/snow-agreement-homeworks');
const { PDFDocument } = require('pdf-lib');

function quote(overrides = {}) {
  return { id: 42, quote_number: '1500', customer_name: 'Preview Client', customer_email: 'preview@example.com', customer_address: '123 Example Street, Lakewood, OH', total: 675,
    services: [{ name: 'Snow Removal Contract', description: '2026-2027 winter: December 1, 2026 through March 31, 2027. Seasonal price $675, due November 1, 2026. Driveway plowing at two inches.', price: 675, property_id: 77 }], ...overrides };
}
const create = q => agreement.create(q, { estimate_id: 888, accepted_at: '2026-10-25T18:00:00Z' });

test('summary removes estimate boilerplate and preserves scope, dates and custom instructions', () => {
  const q = quote();
  q.services[0].description = "What's Included\nSeasonal driveway snow removal.\nAreas Included:\nDriveway and front walkway.\nSeason Coverage:\n2026-2027: December 1, 2026 through March 31, 2027.\nSnow Removal Timing:\nStandard route timing.\nGate instructions:\nClose gate after service.\nSalt & Deicing:\nSalt is included.";
  const doc = agreement.create(q, { estimate_id:888, notes:'Thank You\nThank you for the opportunity.\nNext Step:\nOnce accepted, your service will be scheduled automatically.\nSpecial instructions:\nUse the side entrance.', terms:'Estimate Validity:\nThis estimate is valid for 30 days.\nPayment:\nTwo installments of $337.50.' });
  const html = agreement.render(doc);
  expect(html).not.toContain('Thank you for the opportunity');
  expect(html).not.toContain('valid for 30 days');
  expect(html).not.toContain('Standard route timing');
  for (const phrase of ['Driveway and front walkway','March 31, 2027','Close gate after service','Salt is included','Use the side entrance','Two installments']) expect(html).toContain(phrase);
  expect(doc.services[0].description).toBe(q.services[0].description);
  expect(doc.estimate_terms).toContain('valid for 30 days');
});

test('preserves the accepted property, price and full service description', () => {
  const q = quote(); const doc = create(q);
  expect(doc.services[0]).toMatchObject({ amount: 675, property_id: 77, property_address: q.customer_address, description: q.services[0].description });
  q.services[0].price = 900; q.services[0].description = 'Changed later';
  expect(doc.services[0].amount).toBe(675);
  expect(doc.services[0].description).toContain('December 1, 2026');
});
test('keeps extended commercial dates and seasonal salt in the accepted scope', () => {
  const q = quote({ total: 8500, services: [
    { name: 'Snow Plowing and Shoveling', description: '2026-2027: November 1, 2026 through April 15, 2027. Walkways, sidewalks and entries included. Four installments of $1,250.', price: 5000, property_id: 77 },
    { name: 'Salt Application', description: 'Seasonal salt $3,500, four installments of $875.', price: 3500, property_id: 77 }
  ] });
  const html = agreement.render(create(q));
  expect(html).toContain('April 15, 2027'); expect(html).toContain('$8,500.00'); expect(html).toContain('four installments of $875');
  expect(html).not.toContain('December 1');
});
test.each(['Snow Removal Contract (Monthly)', 'Snow Removal Per Push', 'Salt Application (Per Visit)'])('retains billing basis for %s', name => {
  const q = quote(); q.services[0].name = name;
  expect(agreement.render(create(q))).toContain(name);
  expect(agreement.render(create(q))).toContain('not a promise of a full-season total');
});
test('zero-dollar included walkways remain zero rather than taking a fallback amount', () => {
  const q = quote(); q.services.push({ name: 'Snow Clearing Add-On', description: 'Front walkway included.', price: 0, amount: 0, property_id: 77 });
  expect(create(q).services[1].amount).toBe(0);
});
test('does not apply the snow template to mowing', () => {
  expect(create(quote({ services: [{ name: 'Weekly Mowing', price: 40 }] }))).toBeNull();
});
test('holds mixed-service estimates for review', () => {
  const q = quote(); q.services.push({ name: 'Weekly Mowing', description: 'Weekly', price: 40 });
  expect(() => create(q)).toThrow('Separate winter');
});
test('requires the actual property reference before issuing an official agreement', () => {
  const q = quote(); delete q.services[0].property_id;
  expect(() => create(q)).toThrow('HomeWorks property ID');
});
test.each([
  ['missing description', '', 'accepted description'],
  ['old winter', '2025-2026: December 1, 2025 through March 31, 2026.', '2025-26'],
  ['missing season year', 'December 1 through March 31.', '2026-27 winter'],
  ['missing dates', '2026-2027 snow season.', 'start and end dates'],
])('holds %s rather than guessing', (_label, description, error) => {
  const q = quote(); q.services[0].description = description;
  expect(() => create(q)).toThrow(error);
});
test('preserves each property on a multi-property accepted estimate', () => {
  const q = quote(); q.services[0].property_address = 'Property One';
  q.services.push({ ...q.services[0], property_id: 78, property_address: 'Property Two' });
  const doc = create(q); expect(doc.services.map(s => s.property_address)).toEqual(['Property One', 'Property Two']);
});
test('escapes customer and source text in HTML', () => {
  const q = quote({ customer_name: '<img src=x onerror=alert(1)>' });
  q.services[0].description += '<script>alert(2)</script>';
  const html = agreement.html(create(q), { name: '<svg onload=alert(3)>' });
  expect(html).not.toContain('<script>'); expect(html).not.toContain('<img src=x'); expect(html).not.toContain('<svg');
});
test('contains winter terms and omits mowing fees, skip rules and renewal', () => {
  const html = agreement.render(create(quote()));
  expect(html).toContain('15 days'); expect(html).toContain('two inches');
  expect(html).not.toContain('courtesy skips'); expect(html).not.toContain('Mowing Services'); expect(html).not.toContain('2.9%');
  expect(html).not.toContain('automatic March renewal');
});
test('PDF uses stored text, includes typed signature and paginates long scopes', async () => {
  const doc = create(quote()); doc.sections[0].paragraphs.push('A'.repeat(1400));
  const bytes = await generateSnowAgreementPDF(doc, { data: 'Preview Signer', name: 'Preview Signer', date: 'October 25, 2026' });
  const pdf = await PDFDocument.load(bytes);
  expect(pdf.getTitle()).toBe(doc.title); expect(pdf.getPageCount()).toBeGreaterThan(2);
  expect(pdf.getPages().every(page => page.getHeight() === 792)).toBe(true);
});

function syncFixture(done = []) {
  const doc = create(quote());
  const q = { ...quote(), contract_document: doc, contract_document_hash: 'test-hash', contract_signed_at: '2026-10-25' };
  const pool = { query: jest.fn(async sql => ({ rows: sql.startsWith('SELECT') ? done : [] })) };
  const query = jest.fn(async args => {
    if (args.operationName === 'YardDeskSnowContractEstimate') return { estimate: { id: 888, number: 1500, lineItems: [{ removed: 'NOT_REMOVED', property: { id: 77 } }] } };
    if (args.operationName === 'YardDeskSnowContractUpload') return { createUpload: { key: 'temp-pdf', uploadUrl: 'https://storage.example/upload' } };
    return { addPropertyMedia: { id: 77 } };
  });
  const fetchImpl = jest.fn(async () => ({ ok: true }));
  return { quote: q, pdfBytes: new Uint8Array([1, 2]), pool, query, fetchImpl };
}
test('sync uses the official PDF upload and property attachment operations', async () => {
  const args = syncFixture(); const result = await syncSnowAgreementToHomeWorks(args);
  expect(result.success).toBe(true);
  expect(args.query.mock.calls.map(c => c[0].operationName)).toEqual(['YardDeskSnowContractEstimate', 'YardDeskSnowContractUpload', 'YardDeskSnowContractAttach']);
  expect(args.fetchImpl.mock.calls[0][1].method).toBe('PUT');
  expect(args.pool.query.mock.calls[1][1][1]).toBe('snow_pdf_synced');
});
test('does not upload the same document to an already synced property again', async () => {
  const args = syncFixture([{ details: { document_hash: 'test-hash', property_id: 77 } }]);
  expect((await syncSnowAgreementToHomeWorks(args)).success).toBe(true); expect(args.fetchImpl).not.toHaveBeenCalled();
});
test('does not attach a PDF to an unrelated property', async () => {
  const args = syncFixture(); args.quote.contract_document.services[0].property_id = 99;
  expect((await syncSnowAgreementToHomeWorks(args)).success).toBe(false); expect(args.fetchImpl).not.toHaveBeenCalled();
});
test('failed upload is not recorded as attached', async () => {
  const args = syncFixture(); args.fetchImpl.mockResolvedValue({ ok: false, status: 500 });
  expect((await syncSnowAgreementToHomeWorks(args)).success).toBe(false);
  expect(args.query).toHaveBeenCalledTimes(2); expect(args.pool.query).toHaveBeenCalledTimes(1);
});
