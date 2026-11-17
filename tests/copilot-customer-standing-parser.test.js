const {accountStanding}=require('../services/homeworks/records');
test('account standing keeps unsent drafts out of issued balances and credits separate',async()=>{
 const queryImpl=jest.fn().mockResolvedValue({invoiceReport:[
 {status:'DRAFT',isSent:false,_count:{id:7}},{status:'PENDING',isSent:true,_count:{id:3}},
 {status:'PENDING',isSent:false,_count:{id:4}},{status:'PAST_DUE',isSent:true,_count:{id:2}},
 {status:'PAID',isSent:true,_count:{id:9}}],customers:[{id:1,credit:'10'},{id:2,credit:'0'}]});
 const result=await accountStanding({queryImpl});
 expect(result).toMatchObject({total:25,outstanding:5,past_due:2,paid:9,credit:1,source:'official_homeworks_graphql'});
 expect(queryImpl.mock.calls[0][0].query).toContain('invoiceReport');
});
