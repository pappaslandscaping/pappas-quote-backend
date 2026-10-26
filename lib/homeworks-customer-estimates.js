const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { readLogoDataUri, renderPrintDocument } = require('./invoice-mail-html');
const { resolveHomeworksCustomer } = require('./homeworks-customer-billing');
const snowAgreement = require('../public/js/snow-agreement');
const { createLandscapingMailAgreement } = require('./landscaping-mail-agreement');

async function loadCustomerEstimates(options) {
  const { owner, query } = await resolveHomeworksCustomer(options);
  const result = await query('YardDeskProfileEstimates', `query YardDeskProfileEstimates($customerId: SafeInt!) {
    customer(customerId: $customerId) { id fullName email phone cell address { street1 street2 city state zip } }
    estimates(where: { customerId: { equals: $customerId }, isDeleted: false }, take: 1000, orderBy: [{ date: desc }, { id: desc }]) {
      id number customerId date title status subtotal tax total discountAmount notes terms
      lineItems { name description price quantity subtotal subtotalWithTax isIncluded property { id address { street1 street2 city state zip } } }
    }
  }`, { customerId: owner.id });
  if (result.customer?.id !== owner.id || !Array.isArray(result.estimates) || result.estimates.length >= 1000
      || result.estimates.some(estimate => estimate.customerId !== owner.id)) throw new Error('Could not verify this customer’s estimates.');
  return { customer: result.customer, estimates: result.estimates };
}

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
function plain(value) {
  return String(value || '')
    .replace(/<(script|style|iframe|object)\b[^>]*>[\s\S]*?<\/\1>/gi, '')
    .replace(/<!--([\s\S]*?)-->/g, '')
    .replace(/<br\s*\/?\s*>|<\/(?:p|li|tr|div)>/gi, '\n')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&(?:nbsp|amp|lt|gt|quot|apos);/gi, entity => ({'&nbsp;':' ','&amp;':'&','&lt;':'<','&gt;':'>','&quot;':'"','&apos;':"'"}[entity.toLowerCase()]))
    .replace(/&#(x[0-9a-f]+|[0-9]+);/gi, (entity, code) => {
      const value = code[0].toLowerCase() === 'x' ? parseInt(code.slice(1),16) : Number(code);
      return value > 0 && value <= 0x10ffff ? String.fromCodePoint(value) : '';
    })
    .replace(/[ \t]+/g, ' ').replace(/\n\s*\n/g, '\n\n').trim();
}

function amount(value) {
  if (value == null || value === '' || !Number.isFinite(Number(value))) throw new Error('Estimate amount is unavailable.');
  return Number(value).toLocaleString('en-US', { style:'currency', currency:'USD' });
}
function estimateDate(value) {
  const iso = String(value || '').slice(0, 10);
  const date = new Date(`${iso}T12:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso) || !Number.isFinite(date.getTime())) throw new Error('Estimate date is unavailable.');
  return date.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
}
function addressLines(address) {
  return [address?.street1, address?.street2, [address?.city, address?.state, address?.zip].filter(Boolean).join(' ')].filter(Boolean);
}
function renderMailEstimate({ customer, estimate }) {
  if (estimate.customerId !== customer.id) throw new Error('Estimate does not belong to this customer.');
  const lines = estimate.lineItems.filter(item => item.isIncluded);
  const isSnow = snowAgreement.isSnowEstimate({ services: lines });
  const agreementTitle = isSnow ? 'Snow Removal Agreement' : 'Landscaping Services Agreement';
  let agreement = null;
  if (isSnow) {
    try {
      agreement = snowAgreement.create({ customer_name: customer.fullName, customer_email: customer.email,
        customer_phone: customer.cell || customer.phone, quote_number: estimate.number, total: estimate.total,
        services: lines.map(item => ({ name: item.name, description: item.description,
          price: item.subtotalWithTax ?? item.subtotal, quantity: item.quantity, property_id: item.property?.id,
          property_address: addressLines(item.property?.address).join(', ') })) },
      { estimate_id: estimate.id, terms: estimate.terms, notes: estimate.notes });
    } catch (error) {
      const review = new Error(`Cannot print a complete snow agreement: ${error.message}`);
      review.code = 'SNOW_ESTIMATE_NEEDS_REVIEW';
      throw review;
    }
  }
  if (!isSnow) agreement = createLandscapingMailAgreement({ customer, estimate, lines, plain, addressLines });
  const mailing = addressLines(customer.address);
  if (!mailing.length || !customer.address?.street1 || !customer.address?.zip) throw new Error('A complete customer mailing address is required.');
  const logo = readLogoDataUri();
  const maison = fs.readFileSync(path.join(__dirname, '../public/MaisonduSoleil-Sans.otf')).toString('base64');
  const masthead = `<div class="top-rule"></div><div class="masthead">${logo ? `<img src="${logo}" alt="Pappas &amp; Co. Landscaping" class="logo">` : '<div class="wordmark">Pappas &amp; Co. Landscaping</div>'}<div class="company"><strong>Pappas &amp; Co. Landscaping</strong>PO Box 770057<br>Lakewood, OH 44107<br>440-886-7318<br>hello@pappaslandscaping.com</div></div>`;
  const totals = `<div class="totals-box"><div class="totals-row"><span>Subtotal</span><span>${amount(estimate.subtotal)}</span></div>${Number(estimate.discountAmount) ? `<div class="totals-row"><span>Discount</span><span>-${amount(estimate.discountAmount)}</span></div>` : ''}<div class="totals-row"><span>Taxes</span><span>${amount(estimate.tax)}</span></div><div class="totals-row total"><span>Estimate total</span><span>${amount(estimate.total)}</span></div></div>`;
  const reference = agreement ? `${estimate.number}-${crypto.createHash('sha256').update(JSON.stringify({ agreement, date: estimate.date })).digest('hex').slice(0, 16)}` : null;
  const footer = `<div class="estimate-footer">Pappas &amp; Co. Landscaping &nbsp; | &nbsp; Estimate #${esc(estimate.number)}${reference ? ` &nbsp; | &nbsp; Packet ${esc(reference)}` : ''}</div>`;
  const paperSections = agreement ? agreement.sections.slice(0, -1) : [];
  const agreementPages = agreement ? `<section class="page estimate-page agreement-page">${masthead}
    <h1 class="details-heading">${esc(agreement.title)}</h1>
    <p class="subtitle">Estimate #${esc(estimate.number)} dated ${esc(estimateDate(estimate.date))} &nbsp; | &nbsp; ${esc(customer.fullName)}</p>
    <p class="agreement-copy">This agreement accompanies the estimate and its Service Details and Estimate Terms in this packet. Review all pages before signing the final page.</p>
    ${paperSections.map(section => `<section class="agreement-section"><h2 class="contract-bar">${esc(section.title)}</h2>${section.paragraphs.map(paragraph => `<p class="agreement-copy">${esc(paragraph)}</p>`).join('')}</section>`).join('')}
    ${footer}
  </section>
  <section class="page estimate-page agreement-signature-page">${masthead}
    <h1 class="details-heading">Sign and Return</h1>
    <p class="subtitle">${esc(agreement.title)} &nbsp; | &nbsp; Estimate #${esc(estimate.number)}</p>
    <div class="signature-summary">
      <p><strong>Customer:</strong> ${esc(customer.fullName)}<br><strong>Estimate date:</strong> ${esc(estimateDate(estimate.date))}<br><strong>Estimate:</strong> ${esc(estimate.title || 'Proposed services')}</p>
      ${lines.map(item => `<p><strong>${esc(item.name)}</strong><br>${addressLines(item.property?.address).map(esc).join(', ')}<br>Quantity: ${esc(item.quantity)} &nbsp; | &nbsp; Line subtotal: ${amount(item.subtotal)}</p>`).join('')}
      ${agreement.estimate_terms.match(/Winter season:[^\n]*/i) ? `<p>${esc(agreement.estimate_terms.match(/Winter season:[^\n]*/i)[0])}</p>` : ''}
      <p class="signature-totals">Subtotal: ${amount(estimate.subtotal)}${Number(estimate.discountAmount) ? ` &nbsp; | &nbsp; Discount: -${amount(estimate.discountAmount)}` : ''} &nbsp; | &nbsp; Taxes: ${amount(estimate.tax)}<br><strong>Estimate total: ${amount(estimate.total)}</strong></p>
    </div>
    <section class="paper-acceptance" aria-label="Agreement acceptance and signature">
      <h2 class="contract-bar">${esc(agreement.sections[agreement.sections.length - 1].title)}</h2>
      <p>By signing below, I accept Estimate #${esc(estimate.number)} dated ${esc(estimateDate(estimate.date))}, including its services, property details, pricing, season dates, Service Details and Estimate Terms, and I agree to the ${esc(agreement.title)} included in this packet. These documents are incorporated into this acceptance. I acknowledge receiving and reviewing the complete packet identified below.</p>
      <p>I am the customer named above or am authorized to sign on the customer's behalf.</p>
      <div class="paper-signature-fields"><div><div class="paper-signature-line"></div>Customer signature</div><div><div class="paper-signature-line"></div>Date</div></div>
      <div class="paper-printed-name"><div class="paper-signature-line"></div>Printed name</div>
      <p class="packet-reference">Packet reference: ${esc(reference)} &nbsp; | &nbsp; Agreement version: ${esc(agreement.version)}</p>
      <div class="return-callout"><strong class="return-heading">RETURN THIS SIGNED PAGE</strong><p>Mail to Pappas &amp; Co. Landscaping, PO Box 770057, Lakewood, OH 44107, or email a photo to hello@pappaslandscaping.com. Keep the estimate and remaining agreement pages for your records.</p></div>
    </section>${footer}
  </section>` : '';
  const body = `<section class="page estimate-page estimate-cover">${masthead}
    <div class="window-address-slot" aria-hidden="true"></div>
    <div class="window-recipient" aria-label="Mailing address"><strong>${esc(customer.fullName)}</strong>${mailing.map(line=>`<div>${esc(line)}</div>`).join('')}</div>
    <div class="invoice-head"><div><h1>Estimate</h1><p class="subtitle">${esc(estimate.title || 'Proposed services')}</p></div>
      <div class="summary-panel"><table class="summary" aria-label="Estimate summary"><tr><td>Estimate #</td><td>${esc(estimate.number)}</td></tr><tr><td>Date</td><td>${esc(estimateDate(estimate.date))}</td></tr><tr class="total"><td>Estimate total</td><td>${amount(estimate.total)}</td></tr></table></div></div>
    <div class="section-title">Proposed services</div>
    <table class="services estimate-services" aria-label="Proposed services"><thead><tr><th>Service / Property</th><th class="num">Qty</th><th class="num">Rate</th><th class="num">Amount</th></tr></thead><tbody>
      ${lines.map(item=>`<tr><td><div class="service-name">${esc(item.name)}</div><div class="property">${addressLines(item.property?.address).map(esc).join(', ')}</div></td><td class="num">${esc(item.quantity)}</td><td class="num">${amount(item.price)}</td><td class="num amount">${amount(item.subtotal)}</td></tr>`).join('')}
    </tbody></table>
    <div class="bottom"><div class="payment-note return-callout"><strong class="return-heading">${agreement ? 'ACTION REQUIRED: SIGN AND RETURN' : 'NEXT STEP'}</strong><p>${agreement ? `Please review this estimate and the enclosed ${agreementTitle}. To accept, <strong>sign and return the agreement’s signature page</strong>. Keep the estimate and remaining agreement pages for your records.` : 'Please review the service details and terms, then contact our office to discuss acceptance.'}</p>${agreement ? '<p>Mail the signed agreement page to Pappas &amp; Co. Landscaping, PO Box 770057, Lakewood, OH 44107, or email a photo to hello@pappaslandscaping.com.</p>' : ''}<p>Questions or changes? Call 440-886-7318.</p></div>${totals}</div>
    ${footer}
  </section>
  <section class="page estimate-page estimate-details">${masthead}<h1 class="details-heading">Service Details</h1><p class="subtitle">Estimate #${esc(estimate.number)} &nbsp; | &nbsp; ${esc(customer.fullName)}</p>
    ${lines.map(item=>`<section class="service-detail"><h2 class="contract-bar">${esc(item.name)}</h2><div class="property">${addressLines(item.property?.address).map(esc).join(', ')}</div><div class="description">${esc(plain(item.description))}</div></section>`).join('')}
    ${estimate.notes ? `<h2 class="contract-bar">Notes</h2><div class="terms">${esc(plain(estimate.notes))}</div>` : ''}
    ${estimate.terms ? `<h2 class="contract-bar">Estimate Terms</h2><div class="terms">${esc(plain(estimate.terms))}</div>` : ''}
    ${footer}
  </section>${agreementPages}`;
  const styles = `<style>
    @font-face{font-family:Maison;src:url(data:font/otf;base64,${maison}) format('opentype');font-weight:400;font-style:normal;font-display:block}
    .estimate-page{height:auto;min-height:11in;overflow:visible;break-inside:auto;page-break-inside:auto}
    .estimate-page h1,.contract-bar{font-family:Maison,Helvetica,Arial,sans-serif;font-weight:400}
    .estimate-page h1{color:#2e403d;font-size:26pt}
    .estimate-page .logo{width:2.92in;max-height:1.12in;object-fit:contain;object-position:left top}
    .estimate-page .company{font-family:Helvetica,Arial,sans-serif;font-weight:400}
    .estimate-cover .invoice-head{margin-top:.14in}
    .estimate-services th{font-size:8pt;padding:.09in .08in}
    .estimate-services td{padding:.12in .08in;font-size:10pt;line-height:1.45}
    .estimate-services .property{font-size:9pt;color:var(--muted);margin-top:.04in}
    .estimate-page .totals-row{font-size:10pt;padding:.06in 0}
    .estimate-page .totals-row.total{font-size:12pt;color:#2e403d}
    .estimate-page .bottom{break-inside:avoid;margin-top:.2in}
    .estimate-page .payment-note{font-size:10pt;line-height:1.5}
    .return-callout{border:1px solid #a8ba9e;border-left:4px solid #6e7f6d;background:#f4f6ef;padding:.14in .16in;color:#2e403d;break-inside:avoid;print-color-adjust:exact;-webkit-print-color-adjust:exact}
    .return-heading{display:block;font:700 9pt/1.4 Helvetica,Arial,sans-serif;letter-spacing:.04em}
    .return-callout p{font-weight:400;margin:.08in 0 0}
    .agreement-copy,.signature-summary,.paper-acceptance{font:400 9.5pt/1.5 Helvetica,Arial,sans-serif;color:#202a28;overflow-wrap:anywhere}
    .agreement-copy{orphans:3;widows:3}
    .agreement-section{break-inside:auto;page-break-inside:auto}
    .agreement-section .agreement-copy{break-inside:avoid;page-break-inside:avoid}
    .agreement-section .contract-bar{margin:.15in 0 .08in;padding:.08in .12in}
    .signature-summary{margin-top:.18in}
    .signature-summary .totals-box{margin-left:auto}
    .paper-acceptance{break-inside:avoid;page-break-inside:avoid}
    .paper-signature-fields{display:grid;grid-template-columns:1fr 1.4in;gap:.3in;margin-top:.28in;font-size:8pt}
    .paper-signature-line{border-bottom:1px solid #61706b;height:.17in;margin-bottom:.05in}
    .paper-printed-name{width:70%;margin-top:.17in;font-size:8pt}
    .packet-reference{font-size:7.5pt;color:var(--muted)}
    .details-heading{margin-top:.24in}
    .contract-bar{margin:.23in 0 .12in;padding:.1in .14in;background:#2e403d;color:#c9dd80;font-size:13pt;break-after:avoid;page-break-after:avoid;print-color-adjust:exact;-webkit-print-color-adjust:exact}
    .estimate-details .description,.estimate-details .terms{white-space:pre-line;font:400 10pt/1.55 Helvetica,Arial,sans-serif;color:#202a28;overflow-wrap:anywhere}
    .estimate-details .property{font-size:9pt;color:var(--muted);margin-bottom:.12in}
    .estimate-footer{border-top:1px solid var(--line);margin-top:.3in;padding-top:.1in;font-size:8pt;color:var(--muted)}
    @media print{
      @page{size:letter;margin:.42in .48in}
      html,body{width:auto;margin:0;padding:0}
      .document{padding:0}
      .estimate-page{width:auto;margin:0;padding:0;min-height:0;height:auto;overflow:visible;break-inside:auto;page-break-inside:auto}
      .estimate-page .top-rule{height:.05in;margin:0 0 .16in}
      .estimate-cover{break-after:page;page-break-after:always}
      .estimate-details{padding:0}
      .estimate-details,.agreement-page{break-after:page;page-break-after:always}
      .agreement-signature-page{break-before:page;page-break-before:always;break-inside:avoid;page-break-inside:avoid}
      .agreement-signature-page .logo{width:2.3in;max-height:.7in}
      .agreement-signature-page .company{font-size:8.5pt;line-height:1.3}
      .agreement-signature-page .company strong{font-size:9pt}
      .agreement-signature-page .details-heading{font-size:22pt;margin-top:.15in}
      .agreement-signature-page .signature-summary,.agreement-signature-page .paper-acceptance{font-size:9pt;line-height:1.35}
      .agreement-signature-page p{margin:.08in 0}
      .agreement-signature-page .contract-bar{margin:.12in 0 .08in;padding:.08in .12in}
      .agreement-signature-page .paper-signature-fields{margin-top:.22in}
      .agreement-signature-page .return-callout{padding:.1in .13in}
      .estimate-footer{margin-top:.18in;break-before:avoid;page-break-before:avoid}
      .estimate-details .description,.estimate-details .terms{orphans:3;widows:3}
    }
  </style>`;
  return renderPrintDocument(body)
    .replace('<title>Mail Invoice Packet</title>', `<title>Estimate ${esc(estimate.number)} - ${esc(customer.fullName)}</title>`)
    .replace('</head>', styles + '</head>')
    .replace('Print Packet', 'Print Estimate')
    .replace('Use 100% scale on plain letter paper for mailed invoice inserts.', 'Use 100% scale on letter paper for mailing.');
}

module.exports = { loadCustomerEstimates, renderMailEstimate };
