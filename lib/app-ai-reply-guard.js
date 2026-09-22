const SAFE_REVIEW_REPLY = "Thanks for reaching out. I'm passing your request to our team for review, and we'll follow up once we confirm the details.";

function guardAppAiReply(suggestion) {
  const text = String(suggestion || '').trim();
  const hasTimeClaim = /\b(?:today|tomorrow|tonight|this afternoon|end of (?:the )?day|within \d+ (?:business )?days?|by (?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|noon|\d{1,2}(?::\d{2})?))\b/i.test(text);
  const hasCommitment = /\b(?:we|i)\s*(?:['’]?ll|will|can|should|plan to)\b/i.test(text)
    && /\b(?:estimate|quote|proposal|price|schedule|service|visit|arrive|send|provide|deliver|follow up|call back|get (?:it|that|the))\b/i.test(text);
  const hasServiceAreaClaim = /\b(?:we|our company)\s+(?:do not|don't|does not|doesn't|cannot|can't)\s+(?:currently\s+)?(?:service|serve|cover|work in)\b/i.test(text)
    || /\b(?:outside|out of)\s+(?:our|the)\s+service area\b/i.test(text);

  if ((hasTimeClaim && hasCommitment) || hasServiceAreaClaim) {
    return { suggestion: SAFE_REVIEW_REPLY, safetyAdjusted: true };
  }
  return { suggestion: text, safetyAdjusted: false };
}

module.exports = { guardAppAiReply, SAFE_REVIEW_REPLY };
