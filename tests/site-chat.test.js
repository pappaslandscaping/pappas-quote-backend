const express = require('express');
const crypto = require('crypto');
const createSiteChatRoutes = require('../routes/site-chat');

function makePool() {
  const state = { chat: null, messages: [], hourlyCount: 0 };
  return {
    state,
    async query(sql, args = []) {
      if (sql.includes('INSERT INTO site_chats')) {
        state.chat = { id: args[0], hash: args[1], name: args[2], contact: args[3], mode: args[4], status: 'open', alerted_at: null };
        return { rows: [{ id: args[0] }] };
      }
      if (sql.includes('COUNT(*) AS count FROM site_chats')) return { rows: [{ count: state.hourlyCount }] };
      if (sql.includes('INSERT INTO site_chat_messages')) {
        const sender = sql.includes("'staff'") ? 'staff' : sql.includes("'visitor'") ? 'visitor' : args[1];
        const row = { id: state.messages.length + 1, sender, body: args[sender === args[1] ? 2 : 1], created_at: new Date() };
        state.messages.push(row);
        return { rows: [row] };
      }
      if (sql.includes('visitor_token_hash')) {
        return { rows: state.chat && args[0] === state.chat.id && args[1] === state.chat.hash ? [{ id: state.chat.id, status: state.chat.status, mode: state.chat.mode, alerted_at: state.chat.alerted_at }] : [] };
      }
      if (sql.includes('FROM site_chat_messages')) return { rows: state.messages };
      if (sql.includes('UPDATE site_chats')) {
        if (sql.includes('alerted_at = NOW()')) state.chat.alerted_at = new Date();
        if (sql.includes("mode = 'human'")) state.chat.mode = 'human';
        if (sql.includes('visitor_name = $2')) { state.chat.name = args[1]; state.chat.contact = args[2]; }
        return { rows: [{ id: state.chat.id }] };
      }
      if (sql.includes('FROM site_chats')) return { rows: state.chat ? [state.chat] : [] };
      throw new Error('Unexpected SQL: ' + sql);
    }
  };
}

async function withServer(router, run, staff = false) {
  const app = express();
  app.use(express.json());
  if (staff) app.use((req, _res, next) => { req.user = { isAdmin: true, isEmployee: false }; next(); });
  app.use(router);
  const server = app.listen(0);
  try { return await run(`http://127.0.0.1:${server.address().port}`); }
  finally { await new Promise((resolve) => server.close(resolve)); }
}

test('alert hours follow the supplied Eastern schedule, including Saturday and Sunday', () => {
  const inNewYork = (date) => new Date(date);
  expect(createSiteChatRoutes.isAlertHours(inNewYork('2026-09-22T21:59:00Z'))).toBe(true); // Tue 5:59 PM
  expect(createSiteChatRoutes.isAlertHours(inNewYork('2026-09-22T22:00:00Z'))).toBe(false); // Tue 6 PM
  expect(createSiteChatRoutes.isAlertHours(inNewYork('2026-09-26T19:59:00Z'))).toBe(true); // Sat 3:59 PM
  expect(createSiteChatRoutes.isAlertHours(inNewYork('2026-09-26T20:00:00Z'))).toBe(false); // Sat 4 PM
  expect(createSiteChatRoutes.isAlertHours(inNewYork('2026-09-27T16:00:00Z'))).toBe(false); // Sun noon
});

test('after-hours chat is saved without texting and visitor token protects messages', async () => {
  const pool = makePool();
  const sms = { messages: { create: jest.fn() } };
  const router = createSiteChatRoutes({ pool, twilioClient: sms, fromNumber: '+14408867318', ownerNumber: '+12165550000', now: () => new Date('2026-09-27T16:00:00Z') });
  await withServer(router, async (base) => {
    const created = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Guest', contact: 'guest@example.com', message: 'Please help' })
    });
    expect(created.status).toBe(201);
    const result = await created.json();
    expect(result.id).toMatch(/^[a-f0-9-]{36}$/);
    expect(result.token).toMatch(/^[a-f0-9]{64}$/);
    expect(pool.state.chat.hash).toBe(crypto.createHash('sha256').update(result.token).digest('hex'));
    expect(result.afterHours).toBe(true);
    expect(sms.messages.create).not.toHaveBeenCalled();
    const blocked = await fetch(base + '/api/site-chat/' + result.id, { headers: { 'x-chat-token': '0'.repeat(64) } });
    expect(blocked.status).toBe(404);
    const allowed = await fetch(base + '/api/site-chat/' + result.id, { headers: { 'x-chat-token': result.token } });
    expect(allowed.status).toBe(200);
    expect((await allowed.json()).messages[0].body).toBe('Please help');
  });
});

test('new chats require a valid spam check when configured', async () => {
  const pool = makePool();
  const router = createSiteChatRoutes({ pool, recaptchaRequired: true, verifyRecaptcha: async () => ({ success: true, score: 0.9, action: 'wrong_action' }) });
  await withServer(router, async (base) => {
    const response = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Guest', contact: 'guest@example.com', message: 'Help', recaptchaToken: 'fake' })
    });
    expect(response.status).toBe(403);
    expect(pool.state.chat).toBeNull();
  });
});

test('business-hours chat sends one owner text with the private inbox link', async () => {
  const pool = makePool();
  const sms = { messages: { create: jest.fn().mockResolvedValue({ sid: 'SMtest' }) } };
  const router = createSiteChatRoutes({
    pool, twilioClient: sms, fromNumber: '+14408867318', ownerNumber: '+12165550000',
    now: () => new Date('2026-09-22T14:00:00Z')
  });
  await withServer(router, async (base) => {
    const response = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Guest', contact: 'guest@example.com', message: 'Help' })
    });
    expect(response.status).toBe(201);
    const data = await response.json();
    expect(data.afterHours).toBe(false);
    expect(data.alerted).toBe(true);
    expect(sms.messages.create).toHaveBeenCalledTimes(1);
    expect(sms.messages.create.mock.calls[0][0].body).toContain('live-chat.html?chat=' + data.id);
    expect(sms.messages.create.mock.calls[0][0].body).not.toContain(data.token);
  });
});

test('global alert cap saves the chat without sending another text', async () => {
  const pool = makePool();
  pool.state.hourlyCount = 13;
  const sms = { messages: { create: jest.fn() } };
  const router = createSiteChatRoutes({
    pool, twilioClient: sms, fromNumber: '+14408867318', ownerNumber: '+12165550000',
    now: () => new Date('2026-09-22T14:00:00Z')
  });
  await withServer(router, async (base) => {
    const response = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Guest', contact: 'guest@example.com', message: 'Help' })
    });
    expect(response.status).toBe(201);
    expect((await response.json()).alerted).toBe(false);
    expect(sms.messages.create).not.toHaveBeenCalled();
    expect(pool.state.chat).not.toBeNull();
  });
});

test('AI conversations are saved, alert once, and can hand off in the same thread', async () => {
  const pool = makePool();
  const sms = { messages: { create: jest.fn().mockResolvedValue({ sid: 'SMtest' }) } };
  const router = createSiteChatRoutes({
    pool, twilioClient: sms, fromNumber: '+14408867318', ownerNumber: '+12165550000',
    now: () => new Date('2026-09-22T14:00:00Z')
  });
  await withServer(router, async (base) => {
    const started = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'assistant', message: 'What does fall cleanup include?' })
    });
    expect(started.status).toBe(201);
    const session = await started.json();
    expect(session.mode).toBe('assistant');
    expect(session.alerted).toBe(true);
    expect(sms.messages.create).toHaveBeenCalledTimes(1);
    const headers = { 'content-type': 'application/json', 'x-chat-token': session.token };
    const aiReply = await fetch(base + '/api/site-chat/' + session.id + '/messages', {
      method: 'POST', headers, body: JSON.stringify({ sender: 'assistant', message: 'We remove leaves and debris.' })
    });
    expect(aiReply.status).toBe(201);
    const handedOff = await fetch(base + '/api/site-chat/' + session.id + '/handoff', {
      method: 'POST', headers,
      body: JSON.stringify({ name: 'Guest', contact: 'guest@example.com', message: 'Please have someone follow up.' })
    });
    expect(handedOff.status).toBe(200);
    expect((await handedOff.json()).alreadyAlerted).toBe(true);
    expect(sms.messages.create).toHaveBeenCalledTimes(1);
    const transcript = await fetch(base + '/api/site-chat/' + session.id, { headers });
    const body = await transcript.json();
    expect(body.mode).toBe('human');
    expect(body.messages.map((item) => item.sender)).toEqual(['visitor', 'assistant', 'visitor']);
    const rejectedAi = await fetch(base + '/api/site-chat/' + session.id + '/messages', {
      method: 'POST', headers, body: JSON.stringify({ sender: 'assistant', message: 'Should not be allowed' })
    });
    expect(rejectedAi.status).toBe(409);
  });
});

test('staff can join an AI chat and future assistant messages stop', async () => {
  const pool = makePool();
  const router = createSiteChatRoutes({ pool, now: () => new Date('2026-09-27T16:00:00Z') });
  await withServer(router, async (base) => {
    const created = await fetch(base + '/api/site-chat', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'assistant', message: 'Can you help?' })
    });
    const session = await created.json();
    const staffReply = await fetch(base + '/api/site-chat-staff/' + session.id + '/messages', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ message: 'Yes, how can I help?' })
    });
    expect(staffReply.status).toBe(201);
    const transcript = await fetch(base + '/api/site-chat/' + session.id, { headers: { 'x-chat-token': session.token } });
    const data = await transcript.json();
    expect(data.mode).toBe('human');
    expect(data.messages.map((item) => item.sender)).toEqual(['visitor', 'staff']);
  }, true);
});
