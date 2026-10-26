const test = typeof global.test === 'function' ? global.test : require('node:test');
const assert = require('node:assert/strict');
const { publicInvoiceText, refreshHomeworksInvoiceText, applyHomeworksMailSnapshot } = require('../lib/homeworks-mail-invoices');
const { renderMailInvoiceHtml } = require('../lib/invoice-mail-html');
const { renderMailInvoicePdf } = require('../lib/invoice-mail-pdf');
const { PDFDocument } = require('pdf-lib');
const note='<div><strong>Fall Cleanups &amp; Snow Removal</strong><p>Get your property ready for the changing seasons!</p><p>Contact us to request an estimate.</p><script>PRIVATE()</script></div>';
test('live public invoice notes replace stale notes without reading internal notes or changing money',async()=>{
 const row={id:1,external_invoice_id:'3949969',notes:'Old note',total:'197.76'};
 const live=await refreshHomeworksInvoiceText(row,async(name,query,variables)=>{
  assert.equal(variables.id,3949969);assert(!query.includes('internalNotes'));assert(!query.includes('mutation'));
  return {data:{invoice:{id:3949969,notes:note,terms:'Due upon receipt.'}}};
 });
 assert(live.notes.includes('Fall Cleanups & Snow Removal'));assert(!live.notes.includes('PRIVATE'));
 assert.equal(live.total,row.total);assert.equal(row.notes,'Old note');
 await assert.rejects(refreshHomeworksInvoiceText(row,async()=>({data:{invoice:{id:5}}})),/verified/);
});
test('mail invoice prints the customer note and retains remittance instructions',()=>{
 const invoice={id:1,invoice_number:'13037',customer_name:'Client',customer_address:'123 Main St\nLakewood, OH 44107',subtotal:184,tax_amount:13.76,total:197.76,amount_paid:0,line_items:[],notes:note};
 const html=renderMailInvoiceHtml(invoice);
 assert(html.includes('Fall Cleanups &amp; Snow Removal'));assert(html.includes('Contact us to request an estimate.'));
 assert(html.includes('Please return the remittance stub'));assert(!html.includes('PRIVATE()'));
});
test('a long invoice note gets complete PDF continuation pages instead of being truncated',async()=>{
 const note=Array.from({length:45},(_,i)=>`Public note line ${i + 1}: contact our office for service information.`).join('\n');
 const bytes=await renderMailInvoicePdf({id:1,invoice_number:'13037',customer_name:'Client',customer_address:'123 Main St\nLakewood, OH 44107',subtotal:184,tax_amount:13.76,total:197.76,amount_paid:0,line_items:[],notes:note});
 const pdf=await PDFDocument.load(bytes);assert(pdf.getPageCount()>=3);
});
