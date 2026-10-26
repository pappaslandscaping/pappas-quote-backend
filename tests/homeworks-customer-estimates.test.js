const test = typeof global.test === 'function' ? global.test : require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const filename = path.join(__dirname, '../lib/homeworks-customer-estimates.js');
function subject(resolve) {
  const sandbox = {module:{exports:{}},require:()=>({resolveHomeworksCustomer:resolve})};
  vm.runInNewContext(fs.readFileSync(filename,'utf8'),sandbox,{filename});
  return sandbox.module.exports;
}
const customer = {id:1053343,fullName:'Rose Marie Amato',address:{street1:'1485 West 116th Street',city:'Cleveland',state:'OH',zip:'44102'}};
const estimate = {id:746214,number:1826,customerId:1053343,date:'2026-10-25',title:'Seasonal snow',subtotal:'625',tax:'50',total:'675',discountAmount:'0',notes:'',terms:'Payment is due upon receipt.',lineItems:[{name:'Snow Removal Contract',description:'<p>December 1 through March 31</p><script>alert(1)</script>',price:'625',quantity:'1',subtotal:'625',isIncluded:true,property:{address:customer.address}},{name:'Removed salt',price:'100',subtotal:'100',isIncluded:false}]};
test('blank email is resolved by verified identity; estimates are scoped to the canonical customer ID',async()=>{
 let called;
 const api=subject(async options=>{assert.equal(options.customer.email,'');return {owner:customer,query:async(op,query,variables)=>{called={op,query,variables};return {customer,estimates:[estimate]};}};});
 const result=await api.loadCustomerEstimates({customer:{name:customer.fullName,email:''}});
 assert.equal(result.estimates[0].id,746214);assert.equal(called.variables.customerId,1053343);
 assert(!/mutation|email:/.test(called.query));
});
test('mixed customer results and printing a different customer’s estimate fail closed',async()=>{
 const api=subject(async()=>({owner:customer,query:async()=>({customer,estimates:[{...estimate,customerId:7}]})}));
 await assert.rejects(api.loadCustomerEstimates({customer:{}}),/verify/);
 assert.throws(()=>api.renderMailEstimate({customer,estimate:{...estimate,customerId:7}}),/belong/);
});
test('mailing copy preserves live totals, service dates, recipient and terms while hiding internal and excluded data',()=>{
 const html=subject().renderMailEstimate({customer,estimate:{...estimate,internalNotes:'PRIVATE REVIEW',notes:'<img src=x onerror=alert(2)><p>Call office &amp; ask</p>'}});
 for(const text of ['Rose Marie Amato','1485 West 116th Street','$675.00','$50.00','December 1 through March 31','Payment is due upon receipt.','Call office &amp; ask']) assert(html.includes(text),text);
 assert(!/PRIVATE REVIEW|Removed salt|alert\(|onerror=|<script|<img/.test(html));
});
test('incomplete postal addresses and financial amounts cannot produce a misleading printout',()=>{
 const api=subject();
 assert.throws(()=>api.renderMailEstimate({customer:{...customer,address:null},estimate}),/mailing address/);
 assert.throws(()=>api.renderMailEstimate({customer,estimate:{...estimate,total:null}}),/amount/);
});
