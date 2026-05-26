const fs = require('fs');
const path = require('path');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

describe('TwilioConnect app message sending', () => {
  test('keeps general YardDesk SMS guarded while app replies use an unguarded Twilio client', () => {
    expect(serverSource).toContain('disableClientSmsCreate(twilioClient)');
    expect(serverSource).toContain('let twilioAppMessagingClient = null');

    const routeStart = serverSource.indexOf("app.post('/api/app/messages/send'");
    expect(routeStart).toBeGreaterThan(-1);

    const routeEnd = serverSource.indexOf("app.get('/api/app/messages/unread-count'", routeStart);
    expect(routeEnd).toBeGreaterThan(routeStart);

    const routeSource = serverSource.slice(routeStart, routeEnd);
    expect(routeSource).toContain('twilioAppMessagingClient.messages.create(messageOptions)');
    expect(routeSource).not.toContain('twilioClient.messages.create(messageOptions)');
  });
});
