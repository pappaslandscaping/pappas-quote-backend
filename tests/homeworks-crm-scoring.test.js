const test = typeof global.test === 'function' ? global.test : require('node:test');
const assert = require('node:assert/strict');
const { scoreCustomerRelationship } = require('../lib/homeworks-crm-scoring');

const today = '2026-11-17';
const customer = { status: 'ACTIVE', email: '', phone: '2166765169', cell: '',
  address: { street1: '6180 Michael Drive' }, pastDue: '0' };

test('an established customer with current work is not scored as a cold lead or at churn risk', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'customer',
    lastCompletedVisit: { startDate: '2026-11-16' }, nextScheduledVisit: { startDate: '2026-11-23' },
    completedVisitsLast12Months: 38, estimates: [{ status: 'PENDING' }], today });
  assert.equal(result.lead.label, 'Existing customer');
  assert.equal(result.lead.score, null);
  assert.equal(result.churn.label, 'Low');
  assert.equal(result.churn.score, 0);
  assert(result.churn.factors.some(factor => factor.includes('38 completed')));
});

test('a sent recent estimate increases a prospect score using stated factors', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'lead',
    estimates: [{ status: 'PENDING', isSent: true, date: '2026-11-10' }], today });
  assert.equal(result.lead.label, 'Hot');
  assert.equal(result.lead.score, 70);
  assert.equal(result.churn.label, 'Not applicable');
});

test('a seasonal scheduling gap alone cannot produce a churn rating', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'customer',
    lastCompletedVisit: { startDate: '2026-03-20' }, nextScheduledVisit: null, today });
  assert.equal(result.churn.label, 'Undetermined');
  assert.equal(result.churn.score, null);
});

test('inactive status and past-due balances call for review without pretending to predict churn', () => {
  const inactive = scoreCustomerRelationship({ customer: { ...customer, status: 'INACTIVE' }, localType: 'customer', today });
  const overdue = scoreCustomerRelationship({ customer: { ...customer, pastDue: '125.00' }, localType: 'customer',
    nextScheduledVisit: { startDate: '2026-11-23' }, today });
  assert.equal(inactive.churn.label, 'Needs review');
  assert.equal(overdue.churn.label, 'Needs review');
  assert.equal(overdue.churn.score, null);
});
