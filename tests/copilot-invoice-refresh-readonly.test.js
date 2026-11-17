const {discoverInvoicePdf}=require('../lib/copilot-live-invoices');
test('compatibility invoice detail refresh only queries the official API',async()=>{
 const original=global.fetch,calls=[];
 global.fetch=async(url,options)=>{const request=JSON.parse(options.body);calls.push({url,request,headers:options.headers});return {ok:true,json:async()=>({data:request.operationName==='YardDeskInvoiceOwners'?{customers:[{id:9,fullName:'Jane'}]}:{invoices:[{id:101,number:123,customerId:9,date:'2026-11-01',isSent:true,status:'PENDING',subtotal:'100',tax:'8',total:'108',paidAmount:'0',lineItems:[]}]}})};};
 try{const result=await discoverInvoicePdf({accessToken:'fake'},{external_invoice_id:'101'});
 expect(result.source).toBe('official_homeworks_graphql');expect(result.parsedInvoice.total_due).toBe(108);expect(calls).toHaveLength(2);
 for(const call of calls){expect(call.url).toBe('https://api.copilotcrm.com/graphql');expect(call.request.query.trim()).toMatch(/^query /);expect(call.headers.Cookie).toBeUndefined();}
 }finally{global.fetch=original;}
});
