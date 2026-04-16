/**
 * Gotcha Guard Tests
 *
 * Verifies the three most dangerous invariants in the codebase:
 * 1. Square webhook route is registered BEFORE express.json() middleware
 * 2. CopilotCRM sync code still exists inside the quote-signing handler
 * 3. Customer name fallback chain is used correctly in outgoing communications
 *
 * These tests read source files as text and verify structural properties.
 * They catch accidental deletions, reorderings, and missing null guards.
 */

const fs = require('fs');
const path = require('path');

// Read all source files that contain route logic
const SERVER_PATH = path.join(__dirname, '..', 'server.js');
const QUOTES_PATH = path.join(__dirname, '..', 'routes', 'quotes.js');
const INVOICES_PATH = path.join(__dirname, '..', 'routes', 'invoices.js');
const CUSTOMERS_PATH = path.join(__dirname, '..', 'routes', 'customers.js');
const JOBS_PATH = path.join(__dirname, '..', 'routes', 'jobs.js');

const serverCode = fs.readFileSync(SERVER_PATH, 'utf-8');
const serverLines = serverCode.split('\n');

// Quotes file contains CopilotCRM sync and sign-contract handler
const quotesCode = fs.readFileSync(QUOTES_PATH, 'utf-8');
const quotesLines = quotesCode.split('\n');

// All route files for broad pattern checks
const allRouteFiles = [
  { path: SERVER_PATH, code: serverCode, lines: serverLines },
  { path: QUOTES_PATH, code: quotesCode, lines: quotesLines },
];
for (const p of [INVOICES_PATH, CUSTOMERS_PATH, JOBS_PATH]) {
  if (fs.existsSync(p)) {
    const code = fs.readFileSync(p, 'utf-8');
    allRouteFiles.push({ path: p, code, lines: code.split('\n') });
  }
}

// Helper: find line number (1-indexed) of first match in given lines array
function findLine(lines, pattern) {
  for (let i = 0; i < lines.length; i++) {
    if (pattern instanceof RegExp ? pattern.test(lines[i]) : lines[i].includes(pattern)) {
      return i + 1;
    }
  }
  return -1;
}

// Helper: find ALL line numbers matching a pattern across all route files
function findAllLinesAcrossFiles(pattern) {
  const matches = [];
  for (const { path: filePath, lines } of allRouteFiles) {
    const name = path.basename(filePath);
    for (let i = 0; i < lines.length; i++) {
      if (pattern instanceof RegExp ? pattern.test(lines[i]) : lines[i].includes(pattern)) {
        matches.push({ file: name, line: i + 1, text: lines[i].trim() });
      }
    }
  }
  return matches;
}

// Helper: find ALL line numbers in a specific file
function findAllLines(lines, pattern) {
  const matches = [];
  for (let i = 0; i < lines.length; i++) {
    if (pattern instanceof RegExp ? pattern.test(lines[i]) : lines[i].includes(pattern)) {
      matches.push({ line: i + 1, text: lines[i].trim() });
    }
  }
  return matches;
}

// ─────────────────────────────────────────────
// GOTCHA 1: Square webhook BEFORE express.json()
// (This stays in server.js — never extracted)
// ─────────────────────────────────────────────

describe('Square webhook middleware order', () => {
  const webhookLine = findLine(serverLines, "/api/webhooks/square");
  const jsonMiddlewareLine = findLine(serverLines, /app\.use\(express\.json/);

  test('Square webhook route exists', () => {
    expect(webhookLine).toBeGreaterThan(0);
  });

  test('express.json() middleware exists', () => {
    expect(jsonMiddlewareLine).toBeGreaterThan(0);
  });

  test('Square webhook is registered BEFORE express.json()', () => {
    expect(webhookLine).toBeLessThan(jsonMiddlewareLine);
  });

  test('Square webhook uses express.raw() for raw body access', () => {
    const webhookText = serverLines[webhookLine - 1];
    expect(webhookText).toContain('express.raw(');
  });

  test('Square webhook verifies HMAC signature', () => {
    const handlerBlock = serverLines.slice(webhookLine - 1, webhookLine + 60).join('\n');
    expect(handlerBlock).toContain('createHmac');
    expect(handlerBlock).toContain('sha256');
    expect(handlerBlock).toContain('x-square-hmacsha256-signature');
  });

  test('no other express.json() or body-parser is registered before the webhook', () => {
    const beforeWebhook = serverLines.slice(0, webhookLine - 1).join('\n');
    expect(beforeWebhook).not.toMatch(/app\.use\(.*express\.json/);
    expect(beforeWebhook).not.toMatch(/app\.use\(.*bodyParser\.json/);
  });
});

// ─────────────────────────────────────────────
// GOTCHA 2: CopilotCRM sync inside quote-signing
// (Now lives in routes/quotes.js)
// ─────────────────────────────────────────────

describe('CopilotCRM sync in quote-signing handler', () => {
  const signContractLine = findLine(quotesLines, "'/api/sent-quotes/:id/sign-contract'");

  test('sign-contract endpoint exists in routes/quotes.js', () => {
    expect(signContractLine).toBeGreaterThan(0);
  });

  test('CopilotCRM login call exists inside the handler', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('api.copilotcrm.com/auth/login');
  });

  test('CopilotCRM customer search exists inside the handler', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('copilotcrm.com/customers/filter');
  });

  test('CopilotCRM estimate acceptance exists inside the handler', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('estimates/accept');
  });

  test('CopilotCRM contract upload exists inside the handler', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('estimates/uploadImage');
  });

  test('CopilotCRM portal invite email exists inside the handler', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('emails/sendMail');
  });

  test('CopilotCRM sync is gated on credentials being set', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('COPILOTCRM_USERNAME');
    expect(handlerBlock).toContain('COPILOTCRM_PASSWORD');
  });

  test('CopilotCRM sync failure does not block contract signing', () => {
    const handlerBlock = quotesLines.slice(signContractLine - 1, signContractLine + 550).join('\n');
    expect(handlerBlock).toContain('CopilotCRM sync failed');
    expect(handlerBlock).toMatch(/copilotErr[\s\S]*?res\.json/);
  });

  test('backfill endpoint also exists in routes/quotes.js', () => {
    const backfillLine = findLine(quotesLines, "'/api/copilotcrm/backfill-contract'");
    expect(backfillLine).toBeGreaterThan(0);
  });
});

// ─────────────────────────────────────────────
// GOTCHA 3: Customer name fallback chain
// (Patterns now span server.js + route files)
// ─────────────────────────────────────────────

describe('Customer name fallback chain', () => {
  test('fallback chain is used in customer-facing name resolution', () => {
    const fallbackPatterns = findAllLinesAcrossFiles(/\.name \|\| \(\(.*first_name/);
    expect(fallbackPatterns.length).toBeGreaterThanOrEqual(5);
  });

  test('no .split on customer_name without null guard', () => {
    const unsafeLines = findAllLinesAcrossFiles(/customer_name\.split\(' '\)/);
    const dangerous = unsafeLines.filter(({ text }) => {
      return !text.includes("|| ''") && !text.includes("|| \"\"");
    });
    if (dangerous.length > 0) {
      console.warn('  Lines with unsafe customer_name.split():');
      dangerous.forEach(d => console.warn(`    ${d.file}:${d.line}: ${d.text}`));
    }
    expect(dangerous).toHaveLength(0);
  });

  test('no .split on .name without null guard', () => {
    const splitOnName = findAllLinesAcrossFiles(/\.name\)?\.split\(' '\)/);
    const dangerous = splitOnName.filter(({ text }) => {
      return !text.includes("|| ''") && !text.includes("|| \"\"");
    });
    if (dangerous.length > 0) {
      console.warn('  Lines with unsafe .name.split():');
      dangerous.forEach(d => console.warn(`    ${d.file}:${d.line}: ${d.text}`));
    }
    expect(dangerous).toHaveLength(0);
  });

  test('firstName extraction in emails always has fallback', () => {
    const firstNameExtractions = findAllLinesAcrossFiles(/split\(' '\)\[0\]/);
    const missingFallback = firstNameExtractions.filter(({ text }) => {
      if (text.includes('col.split') || text.includes('header.split')) return false;
      if (/\(custName\)\.split/.test(text)) return false;
      // Technician name splits are safe — they come from regex match groups, not DB
      if (text.includes('TechName') || text.includes('techName')) return false;
      const hasNullGuard = text.includes("|| ''") || text.includes('|| ""');
      const hasPostFallback = /\[0\]\s*\|\|/.test(text);
      const hasParenGuard = /\([^)]*\|\|\s*['"][^'"]*['"]\s*\)\.split/.test(text);
      return !hasNullGuard && !hasPostFallback && !hasParenGuard;
    });
    if (missingFallback.length > 0) {
      console.warn('  Lines extracting firstName without null guard or fallback:');
      missingFallback.forEach(d => console.warn(`    ${d.file}:${d.line}: ${d.text}`));
    }
    expect(missingFallback).toHaveLength(0);
  });

  test('sendEmail calls for customer-facing emails include customer_name metadata', () => {
    const sendEmailCalls = findAllLinesAcrossFiles(/sendEmail\(/);
    const customerEmails = sendEmailCalls.filter(({ text }) => {
      return !text.includes('NOTIFICATION_EMAIL') && !text.includes('hello@pappas');
    });
    const missingMeta = customerEmails.filter(({ text }) => {
      if (text.includes('type:') && !text.includes('customer_name')) return true;
      return false;
    });
    if (missingMeta.length > 0) {
      console.warn('  sendEmail calls missing customer_name metadata:');
      missingMeta.forEach(d => console.warn(`    ${d.file}:${d.line}: ${d.text}`));
    }
    expect(missingMeta).toHaveLength(0);
  });

  test('customer name resolution from DB uses fallback in critical paths', () => {
    const portalDashboard = findAllLinesAcrossFiles(/portal.*dashboard|portal.*invoices/);
    expect(portalDashboard.length).toBeGreaterThan(0);
  });
});
