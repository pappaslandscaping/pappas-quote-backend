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
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Estimate ${esc(estimate.number)} - ${esc(customer.fullName)}</title>
  <style>@page{size:letter;margin:.6in}*{box-sizing:border-box}body{font:13px Arial,sans-serif;color:#20364b;max-width:7.3in;margin:20px auto;line-height:1.5}header{display:flex;justify-content:space-between;border-bottom:2px solid #20364b;padding-bottom:14px}h1{margin:0;font-size:24px}h2{font-size:17px}.mailing{margin:24px 0 28px;white-space:pre-line}.description{white-space:pre-line;font-size:12px;color:#354454}.service{border-top:1px solid #cbd5df;padding:14px 0} .service-head{display:flex;justify-content:space-between;gap:20px;font-weight:bold}.property{font-size:12px;margin:5px 0}.totals{margin-left:auto;width:260px;break-inside:avoid}.totals div{display:flex;justify-content:space-between;padding:4px}.total{font-weight:bold;border-top:2px solid #20364b;font-size:17px}.terms{white-space:pre-line;font-size:11px}footer{border-top:1px solid #cbd5df;margin-top:24px;padding-top:10px}@media print{body{margin:0;max-width:none}.toolbar{display:none}h2,.service-head{break-after:avoid}}</style></head><body>
  <header><div><h1>Pappas &amp; Co. Landscaping</h1>PO Box 770057<br>Lakewood, OH 44107<br>440-886-7318<br>hello@pappaslandscaping.com</div><div><h1>Estimate #${esc(estimate.number)}</h1>${esc(String(estimate.date).slice(0,10))}</div></header>
  <div class="toolbar"><p>Print this estimate using your browser’s Print command, or save it as a PDF for mailing.</p></div>
  <div class="mailing"><strong>${esc(customer.fullName)}</strong>\n${mailing.map(esc).join('\n')}</div>
  <h2>${esc(estimate.title || 'Proposed services')}</h2>
  ${lines.map(item => `<section class="service"><div class="service-head"><span>${esc(item.name)}</span><span>${amount(item.subtotal)}</span></div><div class="property">${addressLines(item.property?.address).map(esc).join(', ')}</div><div>Quantity: ${esc(item.quantity)} &nbsp; Rate: ${amount(item.price)}</div><div class="description">${esc(plain(item.description))}</div></section>`).join('')}
  <div class="totals"><div><span>Subtotal</span><span>${amount(estimate.subtotal)}</span></div>${Number(estimate.discountAmount) ? `<div><span>Discount</span><span>-${amount(estimate.discountAmount)}</span></div>` : ''}<div><span>Tax</span><span>${amount(estimate.tax)}</span></div><div class="total"><span>Estimate total</span><span>${amount(estimate.total)}</span></div></div>
  ${estimate.notes ? `<h2>Notes</h2><div class="terms">${esc(plain(estimate.notes))}</div>` : ''}${estimate.terms ? `<h2>Estimate terms</h2><div class="terms">${esc(plain(estimate.terms))}</div>` : ''}
  <footer>Questions or ready to proceed? Call 440-886-7318.<br>www.pappaslandscaping.com</footer></body></html>`;
}
module.exports = { loadCustomerEstimates, renderMailEstimate };
