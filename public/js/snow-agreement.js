/* One immutable document is used by the signing page, HTML copy and PDF. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SnowAgreement = factory();
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const VERSION = 'snow-2026-27-v1';
  const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  function plain(value) {
    return String(value || '').replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, '').replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, '')
      .replace(/<br\s*\/?>|<\/(?:p|div|li|h[1-6])>/gi, '\n').replace(/<[^>]+>/g, '')
      .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&lt;/gi, '<').replace(/&gt;/gi, '>').replace(/&quot;/gi, '"').replace(/&#39;|&apos;/gi, "'")
      .replace(/&#(\d+);/g, (_, n) => Number(n) <= 0x10ffff ? String.fromCodePoint(Number(n)) : '').replace(/[ \t]+/g, ' ').replace(/\n\s*\n+/g, '\n').trim();
  }
  function servicesOf(quote) {
    const items = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
    return Array.isArray(items) ? items : [];
  }
  const winterService = name => /\b(?:snow|plow(?:ing)?|salting|salt application|de-?icing|ice management|shoveling|shovelling|walkway|sidewalk)\b/i.test(name || '');
  function isSnowEstimate(quote) { return servicesOf(quote).some(item => winterService(item.name)); }
  function read(quote) {
    const doc = typeof quote.contract_document === 'string' ? JSON.parse(quote.contract_document) : quote.contract_document;
    return doc && doc.kind === 'snow' ? doc : null;
  }
  function create(quote, source = {}) {
    const services = servicesOf(quote);
    if (!services.some(item => winterService(item.name))) return null;
    if (services.some(item => !winterService(item.name))) throw new Error('Separate winter services from other work before creating the snow agreement.');
    if (!services.every(item => plain(item.description))) throw new Error('Each winter service needs its accepted description, including coverage and billing terms.');
    const acceptedText = services.map(item => item.name + ' ' + plain(item.description)).concat(plain(source.terms), plain(source.notes)).join('\n');
    if (/2025\s*[-–/]\s*(?:20)?26\b/.test(acceptedText)) throw new Error('The accepted estimate still refers to the 2025-26 season. Correct the estimate before sending a 2026-27 agreement.');
    if (!/2026\s*[-–/]\s*(?:20)?27\b|2026[\s\S]*2027/.test(acceptedText)) throw new Error('The accepted estimate must explicitly identify the 2026-27 winter season.');
    const dates = acceptedText.match(/\b(?:Nov(?:ember)?|Dec(?:ember)?|Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?)\.?\s+\d{1,2}\b|\b(?:11|12|0?[1-4])\/\d{1,2}(?:\/\d{2,4})?\b/gi) || [];
    if (dates.length < 2) throw new Error('Add the winter start and end dates to the accepted estimate before sending its agreement.');
    if (services.some(item => !item.property_address && !quote.customer_address)) throw new Error('The snow agreement needs the accepted service property address.');
    if (source.estimate_id && services.some(item => !Number.isSafeInteger(Number(item.property_id)) || Number(item.property_id) <= 0)) throw new Error('Every winter service needs its HomeWorks property ID before the agreement can be sent.');
    const amount = Number(quote.total);
    if (!Number.isFinite(amount) || amount < 0) throw new Error('The snow agreement needs a valid accepted estimate total.');
    return {
      kind: 'snow', version: VERSION, title: 'Snow & Ice Service Agreement',
      source: { system: 'HomeWorks', estimate_id: source.estimate_id || null, accepted_at: source.accepted_at || null },
      estimate_notes: plain(source.notes), estimate_terms: plain(source.terms),
      estimate_number: String(quote.quote_number || ''),
      customer: { name: quote.customer_name || '', email: quote.customer_email || '', phone: quote.customer_phone || '' },
      total: amount,
      services: services.map(item => {
        const value = Number(item.amount ?? item.price);
        if (!Number.isFinite(value) || value < 0) throw new Error('A winter service has an invalid accepted amount.');
        return { name: plain(item.name), description: plain(item.description), amount: value, quantity: item.quantity ?? null, property_id: item.property_id || null, property_address: item.property_address || quote.customer_address };
      }),
      sections: [
        { title: '1. Agreement and service period', paragraphs: [
          'This agreement is between T T Pappas Enterprises LLC, doing business as Pappas & Co. Landscaping (Contractor), and the Client named above. It incorporates the accepted HomeWorks estimate identified above, including its property addresses, service descriptions, prices, season dates and payment schedule.',
          'Service is limited to the winter period stated in the accepted estimate. Coverage does not extend outside those dates unless agreed in writing. There is no automatic March renewal under this snow agreement. A future season requires a new accepted estimate and agreement.',
          'Property-specific terms in the accepted estimate control the service area, season dates, snowfall trigger, service level and billing arrangement if they differ from the general winter terms below. Questions or missing details must be resolved in writing before signing.'
        ] },
        { title: '2. Areas included and additional work', paragraphs: [
          'Only the services and areas expressly included in the accepted estimate are covered. Driveway plowing does not include walkways, public sidewalks, steps, decks, salting or other deicing unless those areas or services are included in an accepted line item.',
          'Snow hauling, off-site removal, roof clearing and additional areas are excluded unless expressly quoted. Additional work requires a separately approved written price before it is performed. Snow will be placed in suitable areas on the property, subject to available space and site conditions.'
        ] },
        { title: '3. Snowfall trigger and storm service', paragraphs: [
          'Unless the accepted estimate specifies a different trigger, plowing begins after an accumulation of two inches or more of new snowfall at the property. Ice-only conditions do not by themselves trigger a plowing visit.',
          'Service timing depends on when snow starts and ends, accumulation, road conditions and the route. An exact arrival or completion time is not promised unless expressly included in the accepted estimate.',
          'During extended storms, an initial pass may be made to maintain access, followed by final clearing after the storm, as route and weather conditions permit. For per-push or per-visit service, separately performed visits are billed at the accepted rate unless the estimate expressly includes multiple passes in one event price. Seasonal or monthly coverage follows the accepted scope.',
          'Snow deposited after service by municipal plows, drifting or other sources is handled according to the accepted estimate. A separate return visit outside the agreed scope requires an approved additional price.'
        ] },
        { title: '4. Salt, ice and winter conditions', paragraphs: [
          'Salt or other deicing materials are applied only when included in the accepted estimate or separately authorized. The covered areas, application terms and price are those stated in that estimate.',
          'Plowing and deicing do not guarantee continuously bare or ice-free pavement. Packed snow, residual snow, melting and refreezing can occur between visits. Continuous site monitoring is included only if expressly quoted. The Client should promptly report hazardous conditions or service concerns to 440-886-7318 or hello@pappaslandscaping.com.'
        ] },
        { title: '5. Price and payment', paragraphs: [
          'The accepted estimate supplies the price and billing basis for each service. The estimate total shown above is not a promise of a full-season total for per-push, per-visit or monthly services unless the estimate expressly says so. The number of billable visits or months follows the accepted arrangement.',
          'Seasonal service is a fixed price for the stated winter period and scope, not a purchase of a guaranteed number of visits. Monthly charges and installment payments are due on the schedule stated in the accepted estimate. Per-push or per-visit charges apply to service actually performed at the accepted rate.',
          'Payments are due on the date stated on the invoice. A lighter winter, fewer qualifying snowfalls or a Client-requested skip does not reduce an agreed fixed seasonal price or fixed monthly charge. Fees, surcharges and card authorization must be expressly disclosed and agreed; this agreement does not create a new fee or change the accepted billing arrangement.'
        ] },
        { title: '6. Property access and preparation', paragraphs: [
          'The Client must move vehicles and personal items from the service area, unlock necessary gates and provide safe access. Before snow coverage, the Client must identify pavement edges, curbs, drains, landscape features and other hidden obstacles or existing damage that could affect service.',
          'Areas blocked by vehicles, locked gates or unsafe conditions may be left uncleared. Fixed seasonal or monthly pricing is not reduced for Client-related access restrictions. Any extra return visit outside the accepted scope requires an approved price. Per-visit charges apply only to work performed.',
          'Please report suspected service-related damage or an incomplete visit promptly, with the property address and available photographs, so the Contractor can inspect and address the concern.'
        ] },
        { title: '7. Cancellation', paragraphs: [
          "The Client may cancel service with 15 days' written notice. Services continue through the notice period, and the final invoice includes work completed and amounts due under the accepted payment schedule. No refunds are given for prepaid services or unused portions of seasonal contracts.",
          "Pappas & Co. Landscaping may cancel service with 15 days' written notice. Cancellation does not erase amounts already due. Written notices may be sent to hello@pappaslandscaping.com."
        ] },
        { title: '8. Performance and service concerns', paragraphs: [
          'The Contractor will perform the agreed services with reasonable care and supply the labor and equipment needed for that work. Conditions outside the Contractor\'s reasonable control may affect timing; the Contractor will communicate material interruptions and resume service when conditions allow.',
          'This agreement does not make the Contractor responsible for services, areas or continuous monitoring that were not included in the accepted estimate. It does not excuse either party from responsibility imposed by applicable law. The parties will first attempt to resolve service or billing disputes in good faith. Ohio law governs this agreement.'
        ] },
        { title: '9. Acceptance', paragraphs: [
          'By signing, the Client confirms that the listed property, services, season dates and payment arrangement match the accepted estimate, agrees to this Snow & Ice Service Agreement and consents to using an electronic signature. If signing for a business or property owner, the signer confirms authority to do so. The Client may retain a copy of the signed agreement.'
        ] }
      ]
    };
  }
  const money = n => n == null ? '[From accepted estimate]' : Number(n).toLocaleString('en-US', { style: 'currency', currency: 'USD' });
  function render(doc) {
    const p = text => '<p>' + escape(text).replace(/\n/g, '<br>') + '</p>';
    return '<h1>' + escape(doc.title) + '</h1><p>Estimate #' + escape(doc.estimate_number) + ' | Version ' + escape(doc.version) + '</p>' +
      '<h2>Client</h2>' + p([doc.customer.name, doc.customer.email, doc.customer.phone].filter(Boolean).join('\n')) +
      '<h2>Contractor</h2>' + p('Pappas & Co. Landscaping\nT T Pappas Enterprises LLC\nPO Box 770057, Lakewood, OH 44107\n440-886-7318 | hello@pappaslandscaping.com') +
      '<h2>Accepted services and property details</h2>' + doc.services.map(s => '<h3>' + escape(s.name) + '</h3>' + p('Property: ' + s.property_address) + p('Quoted amount: ' + money(s.amount) + (s.quantity == null ? '' : ' | Quantity: ' + s.quantity)) + p(s.description)).join('') +
      '<p><strong>Accepted estimate total: ' + money(doc.total) + '</strong></p>' +
      (doc.estimate_notes ? '<h2>Accepted estimate notes</h2>' + p(doc.estimate_notes) : '') +
      (doc.estimate_terms ? '<h2>Accepted estimate terms</h2>' + p(doc.estimate_terms) : '') +
      doc.sections.map(s => '<h2>' + escape(s.title) + '</h2>' + s.paragraphs.map(p).join('')).join('');
  }
  function html(doc, signature = {}) {
    const image = /^data:image\/png;base64,[A-Za-z0-9+/=]+$/.test(signature.data || '') ? '<img style="max-width:250px;max-height:80px" alt="Signature" src="' + signature.data + '">' : '';
    return '<!doctype html><html><head><meta charset="utf-8"><title>' + escape(doc.title) + '</title><style>body{font:14px/1.55 Arial,sans-serif;color:#273b36;max-width:760px;margin:36px auto;padding:24px}h1{font-size:27px}h2{font-size:17px;margin-top:25px}h3{font-size:15px}h1,h2,h3{break-after:avoid}p{orphans:3;widows:3}img{display:block}</style></head><body>' + render(doc) +
      '<h2>Client signature</h2>' + image + '<p>' + escape(signature.name || '____________________________') + '</p><p>Date: ' + escape(signature.date || '__________________') + '</p></body></html>';
  }
  return { VERSION, create, read, render, html, plain, isSnowEstimate };
}));
