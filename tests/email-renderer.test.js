const { renderMJML, renderWithBaseLayout, emailTemplate } = require('../lib/email-renderer');

describe('email renderer', () => {
  test('renderMJML compiles a simple MJML document', async () => {
    const html = await renderMJML(`
      <mjml>
        <mj-body>
          <mj-section>
            <mj-column>
              <mj-text>Hello MJML</mj-text>
            </mj-column>
          </mj-section>
        </mj-body>
      </mjml>
    `);

    expect(html).toContain('Hello MJML');
    expect(html).toContain('<html');
  });

  test('renderWithBaseLayout wraps MJML body content in the branded shell', async () => {
    const html = await renderWithBaseLayout('<mj-text>Wrapped Body</mj-text>', {
      wrapper: 'full',
      baseUrl: 'https://app.pappaslandscaping.com',
      unsubscribeEmail: 'jane%40example.com',
    });

    expect(html).toContain('Wrapped Body');
    expect(html).toContain('Unsubscribe');
    expect(html).toContain('Pappas');
  });

  test('emailTemplate wrapper none returns a plain shell without footer chrome', () => {
    const html = emailTemplate('<p>Hello</p>', { wrapper: 'none' });

    expect(html).toContain('<p>Hello</p>');
    expect(html).not.toContain('Unsubscribe');
  });
});
