const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);
const LINE_ITEM_DATE_RE = /^[A-Z][a-z]{2} \d{2}, \d{4}\s+/;

function cleanText(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanLine(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+$/g, '')
    .replace(/^\s+/g, '');
}

function parseMoney(value) {
  if (value === null || value === undefined) return 0;
  const normalized = String(value).replace(/[$,\s]/g, '').trim();
  const amount = Number.parseFloat(normalized);
  return Number.isFinite(amount) ? Number(amount.toFixed(2)) : 0;
}

function parseDateToIso(value) {
  const text = cleanText(value);
  if (!text) return null;
  const parsed = new Date(`${text} 12:00:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function extractMoneyByLabel(text, label) {
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = String(text || '').match(new RegExp(`${escaped}\\s+([0-9,]+\\.\\d{2})`, 'i'));
  return match ? parseMoney(match[1]) : 0;
}

function splitPages(text) {
  return String(text || '')
    .split('\f')
    .map((page) => page.replace(/\r/g, ''))
    .filter((page) => page.trim());
}

function extractCustomerBlock(lines) {
  const balanceIndex = lines.findIndex((line) => cleanText(line).startsWith('Outstanding Balance'));
  if (balanceIndex === -1) return { customer_name: null, customer_address: null };

  let index = balanceIndex + 1;
  while (index < lines.length && !cleanText(lines[index])) index += 1;

  const block = [];
  while (index < lines.length) {
    const line = cleanText(lines[index]);
    if (!line || /^Description\b/i.test(line)) break;
    block.push(line);
    index += 1;
  }

  return {
    customer_name: block[0] || null,
    customer_address: block.slice(1).join('\n') || null,
  };
}

function parseLineItemLead(line) {
  const columns = cleanLine(line).split(/\s{2,}/).map(cleanText).filter(Boolean);
  if (columns.length < 5) return null;

  const [lead, rateRaw, quantityRaw, taxPercentRaw, amountRaw] = columns.slice(-5);
  if (!LINE_ITEM_DATE_RE.test(lead)) return null;

  const leadMatch = lead.match(/^([A-Z][a-z]{2} \d{2}, \d{4})\s+(.+)$/);
  if (!leadMatch) return null;

  return {
    service_date: parseDateToIso(leadMatch[1]),
    service_date_raw: leadMatch[1],
    name: cleanText(leadMatch[2]),
    description_lines: [],
    quantity: Number.parseFloat(quantityRaw) || 0,
    rate: parseMoney(rateRaw),
    tax_percent: Number.parseFloat(taxPercentRaw) || 0,
    amount: parseMoney(amountRaw),
  };
}

function finalizeLineItem(item) {
  if (!item) return null;
  const description = item.description_lines
    .map(cleanLine)
    .filter(Boolean)
    .join('\n')
    .trim();

  return {
    service_date: item.service_date,
    service_date_raw: item.service_date_raw,
    name: item.name,
    description,
    quantity: item.quantity,
    rate: item.rate,
    tax_percent: item.tax_percent,
    amount: item.amount,
    taxable: item.tax_percent > 0,
  };
}

function extractLineItems(firstPageText) {
  const lines = String(firstPageText || '').split('\n');
  const headerIndex = lines.findIndex((line) => /^Description\b/i.test(cleanText(line)));
  const notesIndex = lines.findIndex((line) => /^Notes:/i.test(cleanText(line)));
  if (headerIndex === -1 || notesIndex === -1 || notesIndex <= headerIndex) {
    return { property_address: null, line_items: [] };
  }

  const lineItems = [];
  let propertyAddress = null;
  let currentItem = null;

  for (const rawLine of lines.slice(headerIndex + 1, notesIndex)) {
    const line = cleanLine(rawLine);
    const compact = cleanText(line);
    if (!compact) continue;

    if (/^Property Address:/i.test(compact)) {
      propertyAddress = compact.replace(/^Property Address:\s*/i, '').trim() || null;
      continue;
    }

    const maybeItem = parseLineItemLead(line);
    if (maybeItem) {
      if (currentItem) lineItems.push(finalizeLineItem(currentItem));
      currentItem = maybeItem;
      continue;
    }

    if (currentItem) currentItem.description_lines.push(line);
  }

  if (currentItem) lineItems.push(finalizeLineItem(currentItem));

  return {
    property_address: propertyAddress,
    line_items: lineItems.filter(Boolean),
  };
}

function extractNotes(firstPageText) {
  const lines = String(firstPageText || '').split('\n');
  const notesIndex = lines.findIndex((line) => /^Notes:/i.test(cleanText(line)));
  if (notesIndex === -1) return '';

  const notes = [];
  const firstLine = cleanLine(lines[notesIndex]).replace(/^Notes:\s*/i, '');
  const trimmedFirstLine = firstLine.replace(/\s+Subtotal\s+[0-9,]+\.\d{2}.*$/i, '').trim();
  if (trimmedFirstLine) notes.push(trimmedFirstLine);

  for (const line of lines.slice(notesIndex + 1)) {
    const compact = cleanText(line);
    if (!compact) continue;
    if (/^Subtotal\b/i.test(compact)) break;
    if (/^Invoice Terms\b/i.test(compact)) break;
    notes.push(cleanLine(line));
  }

  return notes.join('\n').trim();
}

function parseCopilotInvoiceText(text) {
  const pages = splitPages(text);
  const firstPageText = pages[0] || '';
  const firstPageLines = firstPageText.split('\n');

  const invoiceNumberMatch = String(text || '').match(/Invoice #\s+([A-Z0-9-]+)/i);
  const invoiceDateMatch = String(text || '').match(/Invoice Date\s+([A-Z][a-z]{2} \d{2}, \d{4})/i);

  const { customer_name, customer_address } = extractCustomerBlock(firstPageLines);
  const { property_address, line_items } = extractLineItems(firstPageText);
  const notes = extractNotes(firstPageText);

  const subtotal = extractMoneyByLabel(text, 'Subtotal');
  const thisInvoice = extractMoneyByLabel(text, 'This Invoice');
  const amountPaid = extractMoneyByLabel(text, 'Amount Paid');
  const creditAvailable = extractMoneyByLabel(text, 'Credit Available');
  const outstandingBalance = extractMoneyByLabel(text, 'Outstanding Balance');
  const totalDueOnAccount = extractMoneyByLabel(text, 'Total Due on Account');
  const taxAmount = Number(Math.max(0, (thisInvoice || 0) - (subtotal || 0)).toFixed(2));
  const total = thisInvoice || Number((subtotal + taxAmount).toFixed(2));

  let status = 'pending';
  if (total <= 0 || total - amountPaid <= 0.009) status = 'paid';
  else if (amountPaid > 0) status = 'partial';

  return {
    external_source: 'copilotcrm',
    external_invoice_id: null,
    invoice_number: invoiceNumberMatch ? invoiceNumberMatch[1] : null,
    created_at: invoiceDateMatch ? parseDateToIso(invoiceDateMatch[1]) : null,
    due_date: invoiceDateMatch ? parseDateToIso(invoiceDateMatch[1])?.slice(0, 10) : null,
    invoice_date_raw: invoiceDateMatch ? invoiceDateMatch[1] : null,
    customer_name,
    customer_email: null,
    customer_address,
    title_description: line_items[0]?.name || null,
    property_name: null,
    property_address,
    crew: null,
    tax_amount: taxAmount,
    subtotal,
    total,
    total_due: total,
    amount_paid: amountPaid,
    credit_available: creditAvailable,
    status,
    sent_status: null,
    notes: notes || null,
    line_items,
    metadata: {
      source_document: 'copilot_invoice_pdf',
      invoice_date_raw: invoiceDateMatch ? invoiceDateMatch[1] : null,
      outstanding_balance: outstandingBalance,
      this_invoice: thisInvoice,
      total_due_on_account: totalDueOnAccount,
      property_address,
      customer_address,
    },
  };
}

async function extractTextFromPdfBuffer(buffer) {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'copilot-invoice-'));
  const pdfPath = path.join(tempDir, 'source.pdf');

  try {
    await fs.promises.writeFile(pdfPath, buffer);
    const { stdout } = await execFileAsync('pdftotext', ['-layout', pdfPath, '-'], {
      maxBuffer: 10 * 1024 * 1024,
    });
    return stdout;
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error('pdftotext is required to import Copilot invoice PDFs on this machine.');
    }
    throw error;
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
}

async function parseCopilotInvoicePdfBuffer(buffer) {
  const text = await extractTextFromPdfBuffer(buffer);
  return parseCopilotInvoiceText(text);
}

module.exports = {
  cleanText,
  extractTextFromPdfBuffer,
  parseCopilotInvoicePdfBuffer,
  parseCopilotInvoiceText,
};
