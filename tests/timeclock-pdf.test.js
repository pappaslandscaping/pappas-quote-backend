const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { PDFDocument } = require('pdf-lib');

// Exercise the actual route with the installed PDF reader, without starting
// the application or connecting to operational data.
async function runAssertions() {
  const source = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const start = source.indexOf("app.post('/api/timeclock/parse-pdf'");
  const end = source.indexOf('// Save crew pay rates', start);
  assert.ok(start >= 0 && end > start);
  let handler;
  let queries = 0;
  const errors = [];
  vm.runInNewContext(source.slice(start, end), {
    require,
    app: { post: (_url, _upload, callback) => { handler = callback; } },
    uploadPdf: { single: () => () => {} },
    pool: { query: async () => {
      queries++;
      return { rows: [{ value: { 'Worker, Sample': 20 } }] };
    } },
    serverError: (res, error) => {
      errors.push(error);
      res.status(500).json({ success: false });
    },
  });
  async function invoke(file) {
    const result = { status: 200 };
    const res = {
      status(code) { result.status = code; return this; },
      json(body) { result.body = JSON.parse(JSON.stringify(body)); return this; },
    };
    await handler({ file }, res);
    return result;
  }

  const pdf = await PDFDocument.create();
  pdf.addPage().drawText('Sep 21, 2026 Worker, Sample , 8:00 am 4:30 pm 8 hrs. 30 min.', { x: 30, y: 700, size: 10 });
  const second = pdf.addPage();
  second.drawText('Sep 22, 2026 Worker, Sample , 8:00 am 4:45 pm 8 hrs. 45 min.', { x: 30, y: 700, size: 10 });
  second.drawText('Total Working time: 17 hrs. 15 mins.', { x: 30, y: 670, size: 10 });
  const parsed = await invoke({ buffer: Buffer.from(await pdf.save()) });
  assert.equal(parsed.status, 200);
  assert.equal(parsed.body.success, true);
  assert.equal(parsed.body.entries.length, 2);
  assert.equal(parsed.body.entries[0].totalHours, 8.5);
  assert.equal(parsed.body.entries[1].totalHours, 8.75);
  const employee = parsed.body.byEmployee['Worker, Sample'];
  assert.equal(employee.totalHours, 17);
  assert.equal(employee.totalMinutes, 15);
  assert.equal(employee.decimalHours, 17.25);
  assert.deepEqual(parsed.body.reportTotal, { hours: 17, minutes: 15 });
  assert.equal(parsed.body.payRates['Worker, Sample'], 20);
  assert.equal(queries, 1);

  assert.equal((await invoke(undefined)).status, 400);
  assert.equal((await invoke({ buffer: Buffer.from('Not a PDF') })).status, 500);
  assert.equal(errors.length, 1);
  assert.equal(queries, 1);
}

if (typeof test === 'function') {
  test('timeclock parses a multi-page PDF and preserves hours, totals and rates', runAssertions, 30000);
} else {
  runAssertions().then(() => console.log('timeclock-pdf.test.js passed')).catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
}
