'use strict';
const ready=new WeakMap();
async function ensure(pool) {
  if(!ready.has(pool))ready.set(pool,(async()=>{
    await pool.query('CREATE TABLE IF NOT EXISTS homeworks_operations_reviews(action_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,state TEXT NOT NULL,note TEXT,snoozed_until DATE,reviewed_by TEXT,updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())');
    await pool.query('CREATE TABLE IF NOT EXISTS homeworks_operations_drafts(id TEXT PRIMARY KEY,kind TEXT NOT NULL,customer_id BIGINT,source_fingerprint TEXT NOT NULL,payload JSONB NOT NULL,created_by TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())');
  })().catch(error=>{ready.delete(pool);throw error;}));
  await ready.get(pool);
}
async function reviews(pool,items,today) {
  await ensure(pool);
  const rows=(await pool.query('SELECT * FROM homeworks_operations_reviews WHERE action_id=ANY($1::text[])',[items.map(i=>i.id)])).rows;
  const state=new Map(rows.map(r=>[r.action_id,r]));
  return items.map(item=>{
    const row=state.get(item.id),matches=row?.fingerprint===item.fingerprint;
    const until=row?.snoozed_until instanceof Date?row.snoozed_until.toISOString().slice(0,10):String(row?.snoozed_until||'').slice(0,10);
    return {...item,reviewState:matches?(row.state==='snoozed'&&until<today?'open':row.state):'open',reviewNote:matches?row.note:null,reviewedAt:matches?row.updated_at:null};
  });
}
async function saveReview(pool,item,input,user) {
  const {validDate,shift}=require('./operations-source');
  const state=input.state;
  if(!['open','reviewed','snoozed'].includes(state))throw new Error('Choose open, reviewed, or snoozed.');
  if(state==='snoozed'&&(!validDate(input.until)||input.until<require('./records').dateKey()||input.until>shift(require('./records').dateKey(),90)))throw new Error('Snooze for a date within the next 90 days.');
  await ensure(pool);
  await pool.query('INSERT INTO homeworks_operations_reviews(action_id,fingerprint,state,note,snoozed_until,reviewed_by) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(action_id) DO UPDATE SET fingerprint=EXCLUDED.fingerprint,state=EXCLUDED.state,note=EXCLUDED.note,snoozed_until=EXCLUDED.snoozed_until,reviewed_by=EXCLUDED.reviewed_by,updated_at=NOW()',[item.id,item.fingerprint,state,String(input.note||'').slice(0,1500),state==='snoozed'?input.until:null,String(user||'owner')]);
  return {success:true,localOnly:true,state};
}
async function saveDraft(pool,{id,kind,customerId,fingerprint,payload},user) {
  await ensure(pool);
  await pool.query('INSERT INTO homeworks_operations_drafts(id,kind,customer_id,source_fingerprint,payload,created_by) VALUES($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT(id) DO UPDATE SET source_fingerprint=EXCLUDED.source_fingerprint,payload=EXCLUDED.payload,updated_at=NOW()',[id,kind,customerId,fingerprint,JSON.stringify(payload),String(user||'owner')]);
  return {success:true,id,saved:true,localOnly:true,sent:false};
}
async function listDrafts(pool) {
  await ensure(pool);
  return (await pool.query('SELECT id,kind,customer_id,source_fingerprint,payload,created_at,updated_at FROM homeworks_operations_drafts ORDER BY updated_at DESC LIMIT 200')).rows;
}
module.exports={ensure,reviews,saveReview,saveDraft,listDrafts};
