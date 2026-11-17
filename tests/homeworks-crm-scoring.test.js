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

test('inactive status needs review while a past-due balance remains separate from churn', () => {
  const inactive = scoreCustomerRelationship({ customer: { ...customer, status: 'INACTIVE' }, localType: 'customer', today });
  const overdue = scoreCustomerRelationship({ customer: { ...customer, pastDue: '125.00' }, localType: 'customer',
    nextScheduledVisit: { startDate: '2026-11-23' }, today });
  assert.equal(inactive.churn.label, 'Needs review');
  assert.equal(overdue.churn.label, 'Low');
});

test('Mary’s past waitlisted fertilizing visit prompts a service review, not a generic seasonal churn guess', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'customer',
    lastCompletedVisit: { startDate: '2026-06-30', title: 'Early Summer Fertilizing' },
    completedVisitsLast12Months: 3, recentVisits: [
      { startDate: '2026-09-25', title: 'Fall Fertilizing', status: 'WAITLISTED' },
      { startDate: '2026-09-20', title: 'Late Summer Fertilizing', status: 'SKIPPED' },
    ], today });
  assert.equal(result.churn.label, 'Needs review');
  assert(result.churn.factors.some(factor => factor.includes('Fall Fertilizing')));
});

test('a completed seasonal service without unfinished work is a seasonal pause', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'customer',
    lastCompletedVisit: { startDate: '2026-06-30', title: 'Early Summer Fertilizing' }, today });
  assert.equal(result.churn.label, 'Seasonal pause');
  assert.equal(result.churn.score, null);
});

test('a current sent renewal estimate is pending rather than evidence of churn', () => {
  const result = scoreCustomerRelationship({ customer, localType: 'customer',
    lastCompletedVisit: { startDate: '2026-03-10', title: 'Snow Removal' },
    estimates: [{ id: 7, number: 1782, status: 'PENDING', isSent: true, date: '2026-10-25' }], today });
  assert.equal(result.churn.label, 'Estimate pending');
});
