const fs = require('fs');
const path = require('path');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const communicationsSource = fs.readFileSync(path.join(__dirname, '..', 'routes', 'communications.js'), 'utf8');
const appTransformsPath = path.join(__dirname, '..', '..', 'TwilioConnect-main', 'src', 'utils', 'transforms.ts');
const appTransformsSource = fs.existsSync(appTransformsPath)
  ? fs.readFileSync(appTransformsPath, 'utf8')
  : null;
const backfillSource = fs.readFileSync(path.join(__dirname, '..', 'scripts', 'backfill-mms-media.js'), 'utf8');

describe('TwilioConnect MMS media handling', () => {
  test('inbound Twilio MMS media is copied to app-hosted media URLs', () => {
    expect(communicationsSource).toContain('async function storeInboundMedia(mediaUrl, contentTypeHint)');
    expect(communicationsSource).toContain('TWILIO_ACCOUNT_SID');
    expect(communicationsSource).toContain('Authorization = `Basic ${token}`');
    expect(communicationsSource).toContain('CREATE TABLE IF NOT EXISTS mms_uploads');
    expect(communicationsSource).toContain('INSERT INTO mms_uploads (mime_type, data)');
    expect(communicationsSource).toContain('const storedMediaUrl = await storeInboundMedia(req.body[`MediaUrl${i}`], req.body[`MediaContentType${i}`])');
    expect(communicationsSource).toContain('mediaUrls.push(storedMediaUrl)');
  });

  test('app MMS send route safely supports image-only messages', () => {
    expect(serverSource).toContain("const messageBody = String(body || '')");
    expect(serverSource).toContain("if (!to || (!messageBody && (!mediaUrls || mediaUrls.length === 0)))");
    expect(serverSource).toContain('body: messageBody');
    expect(serverSource).toContain('messageOptions.mediaUrl = mediaUrls');
    expect(serverSource).toContain('media_urls: mediaUrls || []');
    expect(serverSource).not.toContain('body.substring(0, 50)');
  });

  (appTransformsSource ? test : test.skip)('mobile conversation previews preserve media metadata', () => {
    expect(appTransformsSource).toContain('media_urls?: string[]');
    expect(appTransformsSource).toContain('const mediaUrls = c.media_urls || c.mediaUrls || []');
    expect(appTransformsSource).toContain("mediaUrls.length > 0 ? 'Photo message' : ''");
    expect(appTransformsSource).toContain('mediaUrls,');
  });

  test('old MMS backfill runs dry by default and only rewrites external media URLs', () => {
    expect(backfillSource).toContain("const execute = process.argv.includes('--execute')");
    expect(backfillSource).toContain("mode: execute ? 'execute' : 'dry-run'");
    expect(backfillSource).toContain('!url.includes(\'/api/mms-image/\')');
    expect(backfillSource).toContain('TWILIO_ACCOUNT_SID');
    expect(backfillSource).toContain('TWILIO_AUTH_TOKEN');
    expect(backfillSource).toContain('UPDATE messages SET media_urls = $1 WHERE id = $2');
  });

  test('production MMS backfill endpoint is admin protected and dry-run capable', () => {
    expect(serverSource).toContain("app.post('/api/app/messages/backfill-mms', authenticateToken, requireAdmin");
    expect(serverSource).toContain('const execute = req.body?.execute === true');
    expect(serverSource).toContain("mode: execute ? 'execute' : 'dry-run'");
    expect(serverSource).toContain('const candidates = result.rows.filter((row) => (row.media_urls || []).some(isBackfillableMmsUrl))');
    expect(serverSource).toContain('copyLegacyMmsMediaUrl(url)');
    expect(serverSource).toContain('UPDATE messages SET media_urls = $1 WHERE id = $2');
  });
});