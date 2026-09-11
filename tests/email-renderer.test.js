const { renderMJML, renderWithBaseLayout, emailTemplate, buildServiceAgreementEmailV4 } = require('../lib/email-renderer');

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

  test('service agreement email matches the HomeWorks Estimate v4 structure', () => {
    const html = buildServiceAgreementEmailV4({
      customerFirstName: 'Theresa',
      estimateNumber: '1676',
      total: 1.08,
      contractUrl: 'https://app.pappaslandscaping.com/sign-contract.html?token=test',
    });

    expect(html).toContain('background-color:#1f2933');
    expect(html).toContain('linear-gradient(135deg,#f7f9f5 0%,#edf3e6 100%)');
    expect(html).toContain('Agreement Ready');
    expect(html).toContain('Your service agreement is ready');
    expect(html).toContain('Agreement Summary');
    expect(html).toContain('.summary-column { display:table-cell !important; }');
    expect(html).not.toContain('.summary-column { display:block !important;');
    expect(html).toContain('border-left:3px solid #c9dd80');
    expect(html).toContain('<strong>Estimate:</strong> #1676');
    expect(html).toContain('<strong>Total:</strong> $1.08');
    expect(html).toContain('Review &amp; Sign Agreement');
    expect(html).toContain('Next Steps');
    expect(html).toContain('Hi Theresa,');
  });

  test('unsigned agreement reminders reuse the same Estimate v4 design', () => {
    const reminder = buildServiceAgreementEmailV4({
      customerFirstName: '{customer_first_name}',
      estimateNumber: '{quote_number}',
      total: '{quote_total}',
      contractUrl: '{contract_link}',
      variant: 'reminder',
    });
    const finalReminder = buildServiceAgreementEmailV4({
      customerFirstName: '{customer_first_name}',
      estimateNumber: '{quote_number}',
      total: '{quote_total}',
      contractUrl: '{contract_link}',
      variant: 'final',
    });

    for (const html of [reminder, finalReminder]) {
      expect(html).toContain('background-color:#1f2933');
      expect(html).toContain('linear-gradient(135deg,#f7f9f5 0%,#edf3e6 100%)');
      expect(html).toContain('/images/email-logo.png');
      expect(html).toContain('<strong>Estimate:</strong> #{quote_number}');
      expect(html).toContain('<strong>Total:</strong> ${quote_total}');
      expect(html).toContain('Review &amp; Sign Agreement');
      expect(html).toContain('href="{contract_link}"');
      expect(html).not.toContain('—');
    }

    expect(reminder).toContain('Signature Reminder');
    expect(reminder).toContain('One quick step left');
    expect(finalReminder).toContain('Final Reminder');
    expect(finalReminder).toContain('Your agreement still needs a signature');
  });
});
