const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'communications.html'), 'utf8');

function functionBlock(start, end) {
  const from = html.indexOf(start);
  const to = html.indexOf(end, from);
  expect(from).toBeGreaterThan(-1);
  expect(to).toBeGreaterThan(from);
  return html.slice(from, to);
}

function composerContext() {
  const elements = {
    'general-sms-template': { value: 'estimate' },
    'general-sms-body': { value: '' },
    'general-sms-reviewed': { checked: false },
    'general-sms-send-btn': { disabled: true },
  };
  const context = vm.createContext({
    URL,
    document: { getElementById: (id) => elements[id] },
    findSmsTemplate: (id) => id === 'estimate' ? {
      id,
      category: 'estimates',
      sms_body: 'Hi {{CUSTOMER_FIRST_NAME}}, your {{ESTIMATE_NUMBER}} is ready. This estimate: ${{ESTIMATE_TOTAL}} Review: {{ESTIMATE_LINK}}',
    } : null,
    generalTextRecipient: { homeworksId: 91, name: 'Jane Smith', firstName: 'Jane', phone: '216-555-0199' },
    generalTextEstimate: {
      id: 25, number: 1680, total: 286.2, status: 'PENDING', isSent: true,
      portalUrl: 'https://secure.copilotcrm.com/client/estimates/view/25/portal-key',
    },
  });
  vm.runInContext(functionBlock('function selectedGeneralSmsTemplate()', 'async function applyGeneralSmsTemplate('), context);
  vm.runInContext(functionBlock('function updateGeneralTextSendState()', 'async function sendReviewedGeneralText()'), context);
  return { context, elements };
}

describe('HomeWorks estimate text template', () => {
  test('fills number, amount, and review link from the selected sent estimate', () => {
    const { context, elements } = composerContext();
    vm.runInContext('renderGeneralTextTemplate()', context);
    expect(elements['general-sms-body'].value).toBe(
      'Hi Jane, your estimate #1680 is ready. This estimate: $286.20 Review: https://secure.copilotcrm.com/client/estimates/view/25/portal-key'
    );
    elements['general-sms-reviewed'].checked = true;
    vm.runInContext('updateGeneralTextSendState()', context);
    expect(elements['general-sms-send-btn'].disabled).toBe(false);
  });

  test('allows pending estimates and blocks drafts, accepted estimates, and unresolved fields', () => {
    const { context, elements } = composerContext();
    elements['general-sms-reviewed'].checked = true;
    vm.runInContext("generalTextEstimate = { ...generalTextEstimate, isSent: false }; renderGeneralTextTemplate(); updateGeneralTextSendState()", context);
    expect(elements['general-sms-body'].value).toContain('your estimate #1680 is ready');
    expect(elements['general-sms-send-btn'].disabled).toBe(false);

    vm.runInContext("generalTextEstimate = { ...generalTextEstimate, status: 'DRAFT' }; renderGeneralTextTemplate(); updateGeneralTextSendState()", context);
    expect(elements['general-sms-body'].value).toBe('');
    expect(elements['general-sms-send-btn'].disabled).toBe(true);

    vm.runInContext("generalTextEstimate = { ...generalTextEstimate, isSent: true, status: 'ACCEPTED' }; updateGeneralTextSendState()", context);
    expect(elements['general-sms-send-btn'].disabled).toBe(true);

    vm.runInContext("generalTextEstimate = { ...generalTextEstimate, status: 'PENDING' }", context);
    elements['general-sms-body'].value = 'Review {{ESTIMATE_LINK}}';
    vm.runInContext('updateGeneralTextSendState()', context);
    expect(elements['general-sms-send-btn'].disabled).toBe(true);
  });

  test('searches HomeWorks customers and loads their estimates before filling the message', async () => {
    const template = {
      id: 'estimate', category: 'estimates',
      sms_body: 'Hi {{CUSTOMER_FIRST_NAME}}, your {{ESTIMATE_NUMBER}} is ready. ${{ESTIMATE_TOTAL}} {{ESTIMATE_LINK}}',
    };
    const elements = new Map();
    const element = (id) => {
      if (!elements.has(id)) elements.set(id, { value: '', innerHTML: '', textContent: '', style: {}, checked: false, disabled: true });
      return elements.get(id);
    };
    element('general-sms-template').value = 'estimate';
    element('general-sms-search').value = 'Jane Smith';
    const requests = [];
    const context = vm.createContext({
      URL,
      document: { getElementById: element },
      localStorage: { getItem: () => 'test-token' },
      fetch: async (url) => {
        requests.push(url);
        if (url.startsWith('/api/app/homeworks/customers?')) return {
          ok: true, json: async () => ({ success: true, source: 'official_homeworks_graphql',
            customers: [{ id: 91, name: 'Jane Smith', firstName: 'Jane', phone: '216-555-0199' }] }),
        };
        if (url === '/api/app/homeworks/customers/91/text-estimates') return {
          ok: true, json: async () => ({ success: true, source: 'official_homeworks_graphql',
            customer: { id: 91, name: 'Jane Smith', firstName: 'Jane', phone: '216-555-0199' },
            estimates: [{ id: 25, number: 1680, total: 286.2, status: 'PENDING', isSent: true,
              portalUrl: 'https://secure.copilotcrm.com/client/estimates/view/25/portal-key' }] }),
        };
        throw new Error(`Unexpected request: ${url}`);
      },
      findSmsTemplate: (id) => id === 'estimate' ? template : null,
      digitsOnly: (value) => String(value || '').replace(/\D/g, '').slice(-10),
      fmtPhone: (value) => value,
      esc: (value) => String(value),
      generalTextRecipient: null,
      generalTextRecipients: [],
      generalTextEstimate: null,
      generalTextEstimates: [],
      generalEstimateRequestId: 0,
      generalRecipientRequestId: 0,
    });
    vm.runInContext(functionBlock('function selectedGeneralSmsTemplate()', 'async function draftGeneralTextWithAi()'), context);
    vm.runInContext(functionBlock('function updateGeneralTextSendState()', 'async function sendReviewedGeneralText()'), context);

    await vm.runInContext("applyGeneralSmsTemplate('estimate')", context);
    expect(requests).toEqual([
      '/api/app/homeworks/customers?search=Jane%20Smith&summary=1',
      '/api/app/homeworks/customers/91/text-estimates',
    ]);
    expect(element('general-sms-estimate').innerHTML).toContain('#1680');
    expect(element('general-sms-body').value).toBe('');

    vm.runInContext("selectGeneralTextEstimate('25')", context);
    expect(element('general-sms-body').value).toContain('your estimate #1680 is ready. $286.20');
    expect(element('general-sms-body').value).toContain('/client/estimates/view/25/portal-key');
    expect(element('general-sms-send-btn').disabled).toBe(true);
  });
});
