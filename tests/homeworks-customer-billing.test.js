const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { loadHomeworksCustomerBilling, summarizeBilling } = require('../lib/homeworks-customer-billing');
const { renderStatementPdf, _internal } = require('../lib/statement-pdf');
const records = [
  [3718048,12742,'2026-09-03','PENDING',165,13.2,178.2,0,0,false,'2026-10-03'],
  [3518905,12497,'2026-08-27','PENDING',174,13.2,187.2,0,0,true,'2026-09-26'],
  [3324195,12210,'2026-07-23','PAID',116,8.8,124.8,124.8,0,true,'2026-08-29'],
  [3123384,11875,'2026-06-25','PAST_DUE',118,8.8,145.82,0,19.02,true,'2026-07-30'],
  [2916343,11523,'2026-05-04','PAID',277,21.2,298.2,298.2,0,true,'2026-06-27'],
].map(([id,number,date,status,subtotal,tax,total,paidAmount,lateFee,isSent,dueDate]) => ({ id,number,date,status,subtotal,tax,total,paidAmount,lateFee,isSent,dueDate,isArchived:false,processingFee:0,lineItems:[{name:'Mowing (Weekly)',description:''}] }));
const payments = [
  {id:3304891,invoiceId:3324195,date:'2026-08-10',totalAmount:'124.8',isRefund:false},
  {id:2925667,invoiceId:2916343,date:'2026-06-15',totalAmount:'298.2',isRefund:false},
];
async function load(overrides = {}) {
  const calls = [];
  const result = await loadHomeworksCustomerBilling({ customer:{name:'Jackie Singleton',email:''},
    getCopilotToken:async()=>({cookieHeader:'copilotApiAccessToken=test-token'}),
    fetchImpl:async(url,options)=>{
      const request = JSON.parse(options.body); calls.push(request);
      assert.equal(url,'https://api.copilotcrm.com/graphql');
      assert(!request.query.includes('mutation'));
      return {ok:true,json:async()=>({data: request.operationName === 'CustomerBillingIdentity'
        ? {customers:overrides.customers || [{id:2686070,fullName:'Jackie Singleton',outstanding:'333.02'}]}
        : {invoices:records,payments}})};
    }});
  return {result,calls};
}
test('blank email cannot select unrelated accounts; canonical ID scopes all billing records',async()=>{
  const {result,calls} = await load();
  assert.deepEqual(calls[0].variables.where.OR,[{fullName:{equals:'Jackie Singleton'}}]);
  assert.equal(calls[1].variables.customerId,2686070);
  assert.equal(result.invoices.length,5);
  assert.equal(result.balance,333.02);
  assert.equal(result.pastDue,145.82);
  assert.equal(result.current,187.2);
  assert.equal(result.invoices[3].external_metadata.late_fee,19.02);
});
test('ambiguous accounts and unreconciled balances fail closed',async()=>{
  await assert.rejects(load({customers:[{id:1,fullName:'Jackie Singleton'},{id:2,fullName:'Jackie Singleton'}]}),/uniquely/);
  await assert.rejects(load({customers:[{id:1,fullName:'Jackie Singleton',outstanding:559.82}]}),/reconcile/);
});
test('partial payments are deducted; unsent, paid and archived invoices do not inflate balance',()=>{
  assert.equal(summarizeBilling([{total:100,amount_paid:25,is_sent:true,status:'partially_paid'},{total:80,is_sent:false,status:'pending'},{total:90,is_sent:true,status:'paid'},{total:10,is_sent:true,is_archived:true,status:'pending'}]).balance,75);
  assert.equal(_internal.invoiceBalance({total:145.82,amount_paid:0,status:'past_due',external_metadata:{total_due:333.02}}),145.82);
  assert.equal(_internal.invoiceBalance({total:298.2,amount_paid:298.2,status:'paid',external_metadata:{total_due:333.02}}),0);
});
test('Jackie statement reconciles two open invoices and keeps unsent charges off the statement',async()=>{
  const {result} = await load();
  const pdf = await renderStatementPdf({customer:{name:'Jackie Singleton',street:'25825 Eaton Way',city:'Bay Village',state:'OH',postal_code:'44140'},invoices:result.invoices.filter(i=>i.is_sent),payments:result.payments.filter(p=>p.paid_at>='2026-06-19'),statementDate:'2026-09-17',sourceAsOf:'2026-09-17',activityFrom:'2026-06-19',activityTo:'2026-09-17'});
  assert.equal(pdf.summary.balance.toFixed(2),'333.02');
  assert.equal(pdf.summary.pastDue.toFixed(2),'145.82');
  assert.equal(pdf.summary.current.toFixed(2),'187.20');
  if (process.env.STATEMENT_PREVIEW_PATH) fs.writeFileSync(process.env.STATEMENT_PREVIEW_PATH,pdf.bytes);
});
test('customer invoice and statement routes share official records without legacy sync or billing writes',async()=>{
  const express = require('express');
  const createRoutes = require('../routes/customers');
  const originalFetch = global.fetch;
  const queries = [];
  global.fetch = async (url, options) => {
    assert.equal(url,'https://api.copilotcrm.com/graphql');
    const request = JSON.parse(options.body);
    return {ok:true,json:async()=>({data:request.operationName==='CustomerBillingIdentity'
      ? {customers:[{id:2686070,fullName:'Jackie Singleton',outstanding:'333.02'}]}
      : {invoices:records,payments}})};
  };
  const app = express();
  app.use(createRoutes({pool:{query:async(sql)=>{
    queries.push(sql); assert(sql.startsWith('SELECT'));
    return {rows:sql.includes('FROM customers')?[{id:2533,name:'Jackie Singleton',email:''}]:[{id:17170,external_invoice_id:'3123384'}]};
  }},serverError:(res,error)=>res.status(500).json({error:error.message}),authenticateToken:(_req,_res,next)=>next(),
  upload:{single:()=> (_req,_res,next)=>next(),array:()=> (_req,_res,next)=>next()},
  getCopilotToken:async()=>({cookieHeader:'copilotApiAccessToken=test-token'}),generateStatementPDF:async(data)=>{
    assert.equal(data.invoices.length,4); assert(!data.invoices.some(i=>i.invoice_number==='12742'));
    return renderStatementPdf(data);
  }}));
  const server = app.listen(0,'127.0.0.1');
  await new Promise(resolve=>server.once('listening',resolve));
  try {
    const base='http://127.0.0.1:'+server.address().port;
    const response=await originalFetch(base+'/api/customers/2533/invoices');
    assert.equal(response.status,200); const body=await response.json();
    assert.equal(body.balance,333.02); assert.equal(body.invoices[3].id,17170);
    const statement=await originalFetch(base+'/api/customers/2533/statement-pdf');
    assert.equal(statement.status,200); assert.equal(statement.headers.get('content-type'),'application/pdf');
    assert((await statement.arrayBuffer()).byteLength>5000);
    assert(queries.every(sql=>!sql.includes('LOWER(customer_email)')));
  } finally {global.fetch=originalFetch; await new Promise(resolve=>server.close(resolve));}
});
