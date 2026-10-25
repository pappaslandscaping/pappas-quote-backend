const express = require('express');
const createQuoteRoutes = require('../routes/quotes');

async function postQuote({ secret, token, verification }) {
  const pool = { query: jest.fn() };
  const sendEmail = jest.fn();
  const verifyRecaptcha = jest.fn().mockResolvedValue(verification);
  const router = createQuoteRoutes({
    pool, sendEmail, verifyRecaptcha, RECAPTCHA_SECRET_KEY: secret,
    serverError: (_res, error) => { throw error; },
  });
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = app.listen(0);
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/quotes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recaptchaToken: token }),
    });
    return { status: response.status, body: await response.json(), pool, sendEmail, verifyRecaptcha };
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
}

test('public quote endpoint fails closed without its verification secret', async () => {
  const result = await postQuote({ token: 'fake', verification: { success: true, score: 1, action: 'quote_submit' } });
  expect(result.status).toBe(503);
  expect(result.verifyRecaptcha).not.toHaveBeenCalled();
  expect(result.pool.query).not.toHaveBeenCalled();
  expect(result.sendEmail).not.toHaveBeenCalled();
});

test('public quote endpoint rejects missing tokens', async () => {
  const result = await postQuote({ secret: 'configured' });
  expect(result.status).toBe(400);
  expect(result.verifyRecaptcha).not.toHaveBeenCalled();
  expect(result.pool.query).not.toHaveBeenCalled();
});

test.each([
  { success: false, score: 0.9, action: 'quote_submit' },
  { success: true, score: 0.3, action: 'quote_submit' },
  { success: true, score: 0.9, action: 'site_chat' },
  { success: true, action: 'quote_submit' },
])('public quote endpoint rejects failed or mismatched verification: %j', async verification => {
  const result = await postQuote({ secret: 'configured', token: 'fake', verification });
  expect(result.status).toBe(403);
  expect(result.pool.query).not.toHaveBeenCalled();
  expect(result.sendEmail).not.toHaveBeenCalled();
});

test('matching quote token reaches ordinary field validation', async () => {
  const result = await postQuote({ secret: 'configured', token: 'fake', verification: { success: true, score: 0.9, action: 'quote_submit' } });
  expect(result.status).toBe(400);
  expect(result.body.error).toBe('Missing required fields');
  expect(result.verifyRecaptcha).toHaveBeenCalledWith('fake');
  expect(result.pool.query).not.toHaveBeenCalled();
});
