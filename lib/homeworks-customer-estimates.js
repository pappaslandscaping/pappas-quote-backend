const fs = require('node:fs');
const path = require('node:path');
const { readLogoDataUri, renderPrintDocument } = require('./invoice-mail-html');
const { resolveHomeworksCustomer } = require('./homeworks-customer-billing');

async function loadCustomerEstimates(options) {
  const { owner, query } = await resolveHomeworksCustomer(options);
  const result = await query('YardDeskProfileEstimates', `query YardDeskProfileEstimates($customerId: SafeInt!) {
    customer(customerId: $customerId) { id fullName address { street1 street2 city state zip } }
    estimates(where: { customerId: { equals: $customerId }, isDeleted: false }, take: 1000, orderBy: [{ date: desc }, { id: desc }]) {
      id number customerId date title status subtotal tax total discountAmount notes terms
      lineItems { name description price quantity subtotal subtotalWithTax isIncluded property { address { street1 street2 city state zip } } }
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
function addressLines(address) {
  return [address?.street1, address?.street2, [address?.city, address?.state, address?.zip].filter(Boolean).join(' ')].filter(Boolean);
}
function renderMailEstimate({ customer, estimate }) {
  if (estimate.customerId !== customer.id) throw new Error('Estimate does not belong to this customer.');
  const lines = estimate.lineItems.filter(item => item.isIncluded);
  const mailing = addressLines(customer.address);
  if (!mailing.length || !customer.address?.street1 || !customer.address?.zip) throw new Error('A complete customer mailing address is required.');
  const logo = readLogoDataUri();
  const maison = fs.readFileSync(path.join(__dirname, '../public/MaisonduSoleil-Sans.otf')).toString('base64');
  const masthead = `<div class="top-rule"></div><div class="masthead">${logo ? `<img src="${logo}" alt="Pappas &amp; Co. Landscaping" class="logo">` : '<div class="wordmark">Pappas &amp; Co. Landscaping</div>'}<div class="company"><strong>Pappas &amp; Co. Landscaping</strong>PO Box 770057<br>Lakewood, OH 44107<br>440-886-7318<br>hello@pappaslandscaping.com</div></div>`;
  const totals = `<div class="totals-box"><div class="totals-row"><span>Subtotal</span><span>${amount(estimate.subtotal)}</span></div>${Number(estimate.discountAmount) ? `<div class="totals-row"><span>Discount</span><span>-${amount(estimate.discountAmount)}</span></div>` : ''}<div class="totals-row"><span>Taxes</span><span>${amount(estimate.tax)}</span></div><div class="totals-row total"><span>Estimate total</span><span>${amount(estimate.total)}</span></div></div>`;
  const body = `<section class="page estimate-page estimate-cover">${masthead}
    <div class="window-address-slot" aria-hidden="true"></div>
    <div class="window-recipient" aria-label="Mailing address"><strong>${esc(customer.fullName)}</strong>${mailing.map(line=>`<div>${esc(line)}</div>`).join('')}</div>
    <div class="invoice-head"><div><h1>Estimate</h1><p class="subtitle">${esc(estimate.title || 'Proposed services')}</p></div>
      <div class="summary-panel"><table class="summary" aria-label="Estimate summary"><tr><td>Estimate #</td><td>${esc(estimate.number)}</td></tr><tr><td>Date</td><td>${esc(String(estimate.date).slice(0,10))}</td></tr><tr class="total"><td>Estimate total</td><td>${amount(estimate.total)}</td></tr></table></div></div>
    <div class="section-title">Proposed services</div>
    <table class="services estimate-services" aria-label="Proposed services"><thead><tr><th>Service / Property</th><th class="num">Qty</th><th class="num">Rate</th><th class="num">Amount</th></tr></thead><tbody>
      ${lines.map(item=>`<tr><td><div class="service-name">${esc(item.name)}</div><div class="property">${addressLines(item.property?.address).map(esc).join(', ')}</div></td><td class="num">${esc(item.quantity)}</td><td class="num">${amount(item.price)}</td><td class="num amount">${amount(item.subtotal)}</td></tr>`).join('')}
    </tbody></table>
    <div class="bottom"><div class="payment-note"><strong>Next step</strong><p>Review the service details and terms on the following pages, then sign and return this page to accept your estimate.</p><p>Questions or changes? Call 440-886-7318.</p></div>${totals}</div>
    <section class="estimate-acceptance" aria-label="Estimate acceptance and return instructions">
      <h2>Estimate Acceptance</h2>
      <p>By signing, I accept the services and pricing in this estimate. A separate service agreement will be provided for signature before service begins.</p>
      <div class="signature-fields"><div class="signature-field"><div class="signature-line"></div><span>Customer signature</span></div><div class="signature-field"><div class="signature-line"></div><span>Date</span></div></div>
      <div class="printed-name"><div class="signature-line"></div><span>Printed name</span></div>
      <p class="return-instructions"><strong>Return this signed page:</strong> Mail to Pappas &amp; Co. Landscaping, PO Box 770057, Lakewood, OH 44107, or email a photo to hello@pappaslandscaping.com.</p>
    </section>
    <div class="estimate-footer">Pappas &amp; Co. Landscaping &nbsp; | &nbsp; Estimate #${esc(estimate.number)}</div>
  </section>
  <section class="page estimate-page estimate-details">${masthead}<h1 class="details-heading">Service Details</h1><p class="subtitle">Estimate #${esc(estimate.number)} &nbsp; | &nbsp; ${esc(customer.fullName)}</p>
    ${lines.map(item=>`<section class="service-detail"><h2 class="contract-bar">${esc(item.name)}</h2><div class="property">${addressLines(item.property?.address).map(esc).join(', ')}</div><div class="description">${esc(plain(item.description))}</div></section>`).join('')}
    ${estimate.notes ? `<h2 class="contract-bar">Notes</h2><div class="terms">${esc(plain(estimate.notes))}</div>` : ''}
    ${estimate.terms ? `<h2 class="contract-bar">Estimate Terms</h2><div class="terms">${esc(plain(estimate.terms))}</div>` : ''}
    <div class="estimate-footer">Pappas &amp; Co. Landscaping &nbsp; | &nbsp; Estimate #${esc(estimate.number)}</div>
  </section>`;
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
    .estimate-acceptance{margin-top:.25in;padding:.16in .18in;border:1px solid var(--line-strong);break-inside:avoid;page-break-inside:avoid;font:400 9pt/1.45 Helvetica,Arial,sans-serif}
    .estimate-acceptance h2{margin:0 0 .08in;color:#2e403d;font:400 14pt Maison,Helvetica,Arial,sans-serif}
    .estimate-acceptance p{margin:.06in 0 .12in}
    .signature-fields{display:grid;grid-template-columns:1fr 1.5in;gap:.3in;margin-top:.28in}
    .signature-line{border-bottom:1px solid #61706b;height:.15in}
    .signature-field span,.printed-name span{display:block;margin-top:.04in;font-size:8pt;color:var(--muted)}
    .printed-name{width:70%;margin-top:.18in}
    .estimate-acceptance .return-instructions{margin:.14in 0 0;font-size:9pt}
    .details-heading{margin-top:.24in}
    .contract-bar{margin:.23in 0 .12in;padding:.1in .14in;background:#2e403d;color:#c9dd80;font-size:13pt;break-after:avoid;page-break-after:avoid;print-color-adjust:exact;-webkit-print-color-adjust:exact}
    .estimate-details .description,.estimate-details .terms{white-space:pre-line;font:400 10pt/1.55 Helvetica,Arial,sans-serif;color:#202a28;overflow-wrap:anywhere}
    .estimate-details .property{font-size:9pt;color:var(--muted);margin-bottom:.12in}
    .estimate-footer{border-top:1px solid var(--line);margin-top:.3in;padding-top:.1in;font-size:8pt;color:var(--muted)}
    @media print{.estimate-page{margin:0;min-height:11in;height:auto;overflow:visible}.estimate-cover{break-after:page;page-break-after:always}.estimate-details{padding-bottom:.5in}}
  </style>`;
  return renderPrintDocument(body)
    .replace('<title>Mail Invoice Packet</title>', `<title>Estimate ${esc(estimate.number)} - ${esc(customer.fullName)}</title>`)
    .replace('</head>', styles + '</head>')
    .replace('Print Packet', 'Print Estimate')
    .replace('Use 100% scale on plain letter paper for mailed invoice inserts.', 'Use 100% scale on letter paper for mailing.');
}

module.exports = { loadCustomerEstimates, renderMailEstimate };
