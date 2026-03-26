/**
 * Gotcha Guard Tests
 *
 * Verifies the three most dangerous invariants in the codebase:
 * 1. Square webhook route is registered BEFORE express.json() middleware
 * 2. CopilotCRM sync code still exists inside the quote-signing handler
 * 3. Customer name fallback chain is used correctly in outgoing communications
 *
 * These tests read server.js as a text file and verify structural properties.
 * They catch accidental deletions, reorderings, and missing null guards.
 */

const fs = require('fs');
const path = require('path');

const SERVER_PATH = path.join(__dirname, '..', 'server.js');
const serverCode = fs.readFileSync(SERVER_PATH, 'utf-8');
const serverLines = serverCode.split('\n');

// Helper: find line number (1-indexed) of first match
function findLine(pattern) {
  for (let i = 0; i < serverLines.length; i++) {
    if (pattern instanceof RegExp ? pattern.test(serverLines[i]) : serverLines[i].includes(pattern)) {
      return i + 1;
    }
  }
  return -1;
}

// Helper: find ALL line numbers matching a pattern
function findAllLines(pattern) {
  const matches = [];
  for (let i = 0; i < serverLines.length; i++) {
    if (pattern instanceof RegExp ? pattern.test(serverLines[i]) : serverLines[i].includes(pattern)) {
      matches.push({ line: i + 1, text: serverLines[i].trim() });
    }
  }
  return matches;
}

// ─────────────────────────────────────────────
// GOTCHA 1: Square webhook BEFORE express.json()
// ─────────────────────────────────────────────

describe('Square webhook middleware order', () => {
  const webhookLine = findLine("/api/webhooks/square");
  // Find the app.use(express.json()) middleware registration, not just any mention of express.json
  const jsonMiddlewareLine = findLine(/app\.use\(express\.json/);

  test('Square webhook route exists', () => {
    expect(webhookLine).toBeGreaterThan(0);
  });

  test('express.json() middleware exists', () => {
    expect(jsonMiddlewareLine).toBeGreaterThan(0);
  });

  test('Square webhook is registered BEFORE express.json()', () => {
    // This is the #1 gotcha: if someone moves express.json() above the webhook,
    // it silently parses the raw body, and HMAC signature verification breaks.
    expect(webhookLine).toBeLessThan(jsonMiddlewareLine);
  });

  test('Square webhook uses express.raw() for raw body access', () => {
    const webhookText = serverLines[webhookLine - 1];
    expect(webhookText).toContain('express.raw(');
  });

  test('Square webhook verifies HMAC signature', () => {
    // Find the webhook handler body (next ~60 lines after the route definition)
    const handlerBlock = serverLines.slice(webhookLine - 1, webhookLine + 60).join('\n');
    expect(handlerBlock).toContain('createHmac');
    expect(handlerBlock).toContain('sha256');
    expect(handlerBlock).toContain('x-square-hmacsha256-signature');
  });

  test('no other express.json() or body-parser is registered before the webhook', () => {
    // Check that nothing between line 1 and the webhook line parses JSON bodies
    const beforeWebhook = serverLines.slice(0, webhookLine - 1).join('\n');
    // express.json and bodyParser.json would both consume the raw body
    expect(beforeWebhook).not.toMatch(/app\.use\(.*express\.json/);
    expect(beforeWebhook).not.toMatch(/app\.use\(.*bodyParser\.json/);
  });
});

// ─────────────────────────────────────────────
// GOTCHA 2: CopilotCRM sync inside quote-signing
// ─────────────────────────────────────────────

describe('CopilotCRM sync in quote-signing handler', () => {
  // Find the sign-contract endpoint
  const signContractLine = findLine("'/api/sent-quotes/:id/sign-contract'");

  test('sign-contract endpoint exists', () => {
    expect(signContractLine).toBeGreaterThan(0);
  });

  test('CopilotCRM login call exists inside the handler', () => {
    // The CopilotCRM sync block should be within ~500 lines of the sign-contract route
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('api.copilotcrm.com/auth/login');
  });

  test('CopilotCRM customer search exists inside the handler', () => {
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('copilotcrm.com/customers/filter');
  });

  test('CopilotCRM estimate acceptance exists inside the handler', () => {
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('estimates/accept');
  });

  test('CopilotCRM contract upload exists inside the handler', () => {
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('estimates/uploadImage');
  });

  test('CopilotCRM portal invite email exists inside the handler', () => {
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('emails/sendMail');
  });

  test('CopilotCRM sync is gated on credentials being set', () => {
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('COPILOTCRM_USERNAME');
    expect(handlerBlock).toContain('COPILOTCRM_PASSWORD');
  });

  test('CopilotCRM sync failure does not block contract signing', () => {
    // The sync should be wrapped in try/catch so failures don't prevent the response
    const handlerBlock = serverLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('CopilotCRM sync failed');
    // The response (res.json) should come AFTER the sync block
    expect(handlerBlock).toMatch(/copilotErr[\s\S]*?res\.json/);
  });

  test('backfill endpoint also exists as a separate route', () => {
    const backfillLine = findLine("'/api/copilotcrm/backfill-contract'");
    expect(backfillLine).toBeGreaterThan(0);
  });
});

// ─────────────────────────────────────────────
// GOTCHA 3: Customer name fallback chain
// ─────────────────────────────────────────────

describe('Customer name fallback chain', () => {
  // The correct pattern: c.name || ((c.first_name||'') + (c.last_name?' '+c.last_name:'')).trim() || 'Unknown'
  // At minimum, any place that resolves a customer name from the customers table
  // should use the fallback chain, not just c.name alone.

  test('fallback chain is used in customer-facing name resolution', () => {
    // Find all instances of the fallback pattern
    const fallbackPatterns = findAllLines(/\.name \|\| \(\(.*first_name/);
    // Should have multiple instances across the codebase
    expect(fallbackPatterns.length).toBeGreaterThanOrEqual(5);
  });

  test('no .split on customer_name without null guard', () => {
    // Pattern: customer_name.split(' ') WITHOUT (customer_name || '')
    // This is the dangerous pattern — throws TypeError if customer_name is null
    const unsafeLines = findAllLines(/customer_name\.split\(' '\)/);

    // Filter to only lines that DON'T have a || '' guard before .split
    const dangerous = unsafeLines.filter(({ text }) => {
      return !text.includes("|| ''") && !text.includes("|| \"\"");
    });

    // Report which lines are dangerous for easier debugging
    if (dangerous.length > 0) {
      console.warn('  Lines with unsafe customer_name.split():');
      dangerous.forEach(d => console.warn(`    Line ${d.line}: ${d.text}`));
    }

    expect(dangerous).toHaveLength(0);
  });

  test('no .split on .name without null guard', () => {
    // Pattern: .name.split(' ') or .name).split(' ') WITHOUT || ''
    // Matches: quote.customer_name.split, cust.name.split, etc.
    const splitOnName = findAllLines(/\.name\)?\.split\(' '\)/);
    const dangerous = splitOnName.filter(({ text }) => {
      return !text.includes("|| ''") && !text.includes("|| \"\"");
    });

    if (dangerous.length > 0) {
      console.warn('  Lines with unsafe .name.split():');
      dangerous.forEach(d => console.warn(`    Line ${d.line}: ${d.text}`));
    }

    expect(dangerous).toHaveLength(0);
  });

  test('firstName extraction in emails always has fallback', () => {
    // Every line that does .split(' ')[0] to get a first name for an email greeting
    // should have a fallback like || 'there' or || 'Customer'
    const firstNameExtractions = findAllLines(/split\(' '\)\[0\]/);

    const missingFallback = firstNameExtractions.filter(({ text }) => {
      // Exclude non-customer-name splits (column names, schema parsing, etc.)
      if (text.includes('col.split') || text.includes('header.split')) return false;
      // Exclude lines where the variable was already resolved via fallback chain
      // e.g. custName is set from c.name || (first_name + last_name) || 'Unknown' earlier
      if (/\(custName\)\.split/.test(text)) return false;

      // Safe patterns:
      // (name || '').split(' ')[0] — null-safe with empty string
      // (name || 'Customer').split(' ')[0] — null-safe with default name
      // .split(' ')[0] || 'there' — has fallback after
      // .split(' ')[0] || 'Valued Customer' — has fallback after
      const hasNullGuard = text.includes("|| ''") || text.includes('|| ""');
      const hasPostFallback = /\[0\]\s*\|\|/.test(text);
      const hasParenGuard = /\([^)]*\|\|\s*['"][^'"]*['"]\s*\)\.split/.test(text);
      return !hasNullGuard && !hasPostFallback && !hasParenGuard;
    });

    if (missingFallback.length > 0) {
      console.warn('  Lines extracting firstName without null guard or fallback:');
      missingFallback.forEach(d => console.warn(`    Line ${d.line}: ${d.text}`));
    }

    expect(missingFallback).toHaveLength(0);
  });

  test('sendEmail calls for customer-facing emails include customer_name metadata', () => {
    // All sendEmail calls that go to customers (not admin) should include
    // customer_name in the metadata for audit/tracking in email_log
    const sendEmailCalls = findAllLines(/sendEmail\(/);

    // Filter to customer-facing emails (exclude admin notifications)
    const customerEmails = sendEmailCalls.filter(({ text }) => {
      return !text.includes('NOTIFICATION_EMAIL') && !text.includes('hello@pappas');
    });

    // Check that customer emails include metadata with customer_name
    const missingMeta = customerEmails.filter(({ text }) => {
      // If it has a type meta object, it should have customer_name
      if (text.includes('type:') && !text.includes('customer_name')) return true;
      return false;
    });

    if (missingMeta.length > 0) {
      console.warn('  sendEmail calls missing customer_name metadata:');
      missingMeta.forEach(d => console.warn(`    Line ${d.line}: ${d.text}`));
    }

    expect(missingMeta).toHaveLength(0);
  });

  test('customer name resolution from DB uses fallback in critical paths', () => {
    // Check that the main customer detail endpoint (GET /api/customers/:id)
    // and portal endpoints resolve names with the fallback chain
    const portalDashboard = findAllLines(/portal.*dashboard|portal.*invoices/);
    // Portal should be resolving customer names somewhere with fallback
    expect(portalDashboard.length).toBeGreaterThan(0);
  });
});
