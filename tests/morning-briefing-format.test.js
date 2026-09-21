const {
  formatMorningBriefing,
  normalizeInboxSummary,
  splitSmsMessage,
} = require('../lib/morning-briefing-format');

describe('morning briefing formatting', () => {
  test('keeps important emails and removes routine automated mail', () => {
    const summary = normalizeInboxSummary(
      '📨 GMAIL OVERNIGHT\n─────────────────────\n📨 Report domain: pappaslandscaping.com Submitter: google.com\nFrom: noreply-dmarc-support@google.com\n\n📨 Estimate Accepted: Carol Slembarski\nFrom: Pappas & Co. <noreply@copilotcrm.com>\n"Internal Notification Estimate Accepted"\n\n📨 September newsletter\nFrom: marketing@example.com\n\nTotal: 3 incoming emails'
    );

    expect(summary).toBe('📬 Important emails\n• Estimate accepted — Carol Slembarski');
  });

  test('keeps a message from a person even without a system keyword', () => {
    const summary = normalizeInboxSummary('📨 Question about tomorrow\nFrom: Marilyn Conrad <marilyn@example.com>');
    expect(summary).toBe('📬 Important emails\n• Question about tomorrow — Marilyn Conrad');
  });

  test('omits the inbox section when nothing important arrived', () => {
    expect(normalizeInboxSummary('📨 GMAIL OVERNIGHT\n\n📨 Weekly digest\nFrom: newsletter@example.com')).toBe('');
  });

  test('keeps a normal briefing in one provider message when under the limit', () => {
    const message = formatMorningBriefing({
      dateLabel: 'Sep 21',
      sections: {
        jobs: '📋 Today’s schedule\n74 jobs across 3 crews',
        deposit: '💵 Deposits\n$305.98 total incoming',
        invoices: '🧾 Past due\n32 invoices • $4,270.90 total',
        stripe: '💳 Payments\n✓ No failed payments',
      },
    });
    expect(splitSmsMessage(message)).toHaveLength(1);
  });
});
