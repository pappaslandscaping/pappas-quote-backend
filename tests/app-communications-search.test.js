const fs = require('fs');
const path = require('path');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

function routeSource(startMarker, endMarker) {
  const start = serverSource.indexOf(startMarker);
  expect(start).toBeGreaterThan(-1);
  const end = serverSource.indexOf(endMarker, start);
  expect(end).toBeGreaterThan(start);
  return serverSource.slice(start, end);
}

describe('TwilioConnect app communication search and history', () => {
  test('message conversations support server-side search and are not capped at 100', () => {
    const source = routeSource(
      "app.get('/api/app/messages/conversations'",
      "// Get messages for a specific conversation",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim();');
    expect(source).toContain('matching_conversations AS');
    expect(source).toContain('m.body ILIKE');
    expect(source).toContain('c.name ILIKE');
    expect(source).toContain('LIMIT $');
    expect(source).not.toContain('LIMIT 100');
  });

  test('message threads return full conversation history and support search', () => {
    const source = routeSource(
      "app.get('/api/app/messages/thread/:phoneNumber'",
      "// AI reply suggestion",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim();');
    expect(source).toContain('body ILIKE');
    expect(source).toContain('ORDER BY created_at ASC');
    expect(source).not.toContain('LIMIT 100');
  });

  test('voicemail endpoint requests larger history and applies search before returning', () => {
    const source = routeSource(
      "app.get('/api/app/voicemails'",
      "app.post('/api/app/voicemails/:id/play'",
    );

    expect(source).toContain('const searchTerm = String(search || q || \'\').trim().toLowerCase();');
    expect(source).toContain('limit=${voicemailLimit}');
    expect(source).toContain('filteredVoicemails');
    expect(source).toContain('transcription');
    expect(source).not.toContain('limit=100');
  });
});
