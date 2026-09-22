const { guardAppAiReply, SAFE_REVIEW_REPLY } = require('../lib/app-ai-reply-guard');

describe('app AI reply safety', () => {
  test('replaces an unverified estimate deadline', () => {
    expect(guardAppAiReply('Yes I apologize! We will get the estimate to you by end of day tomorrow. What is your email address?'))
      .toEqual({ suggestion: SAFE_REVIEW_REPLY, safetyAdjusted: true });
  });

  test('replaces an unsupported service-area rejection', () => {
    expect(guardAppAiReply("Unfortunately we don't service your property area at this time."))
      .toEqual({ suggestion: SAFE_REVIEW_REPLY, safetyAdjusted: true });
  });

  test('allows a reply that asks for details without making a commitment', () => {
    const suggestion = "I'm sorry for the mix-up. Could you confirm both service addresses so our team can review your request?";
    expect(guardAppAiReply(suggestion)).toEqual({ suggestion, safetyAdjusted: false });
  });
});
