const { Pool } = require('pg');

// Mock the pg Pool to prevent actual database connections during the unit test
jest.mock('pg', () => {
  const mPool = {
    query: jest.fn().mockResolvedValue({ rows: [] }),
    on: jest.fn(),
    end: jest.fn(),
  };
  return { Pool: jest.fn(() => mPool) };
});

// Mock the MJML renderer to easily verify when the MJML path is taken
jest.mock('../lib/email-renderer', () => ({
  renderWithBaseLayout: jest.fn().mockResolvedValue('<html>Mocked MJML Output</html>'),
  renderMJML: jest.fn()
}));

const { renderTemplate } = require('../server');
const { renderWithBaseLayout } = require('../lib/email-renderer');

describe('renderTemplate Function', () => {
  let pool;

  beforeEach(() => {
    // Re-initialize the mocked pool instance
    pool = new Pool();
    jest.clearAllMocks();
  });

  // Helper to safely mock the database query specifically for the template fetch,
  // ignoring any background queries triggered by server.js initialization.
  const mockTemplateDbResponse = (templateRow) => {
    pool.query.mockImplementation((text) => {
      if (text.includes('SELECT * FROM email_templates WHERE slug = $1')) {
        return Promise.resolve({ rows: templateRow ? [templateRow] : [] });
      }
      return Promise.resolve({ rows: [] });
    });
  };

  it('should use the MJML path if options.use_mjml is explicitly true', async () => {
    mockTemplateDbResponse({
      subject: 'MJML Subject {name}',
      body: '<p>Content for {name}</p>',
      options: { use_mjml: true, wrapper: 'full' }
    });

    const vars = { name: 'Jane' };
    const result = await renderTemplate('test-template', vars, 'Fallback Subj', 'Fallback HTML');

    // Verify variable replacement happened before passing the string to the MJML renderer
    expect(renderWithBaseLayout).toHaveBeenCalledWith(
      '<p>Content for Jane</p>',
      expect.objectContaining({ wrapper: 'full' })
    );

    expect(result.subject).toBe('MJML Subject Jane');
    expect(result.html).toBe('<html>Mocked MJML Output</html>');
    expect(result.fromTemplate).toBe(true);
  });

  it('should auto-detect and use the MJML path if body contains <mjml> tags', async () => {
    mockTemplateDbResponse({
      subject: 'Auto-detected MJML',
      body: '<mjml><mj-body>Hello {name}</mj-body></mjml>',
      options: {} // No explicit use_mjml flag
    });

    const result = await renderTemplate('auto-mjml', { name: 'Bob' }, 'Subj', 'HTML');

    expect(renderWithBaseLayout).toHaveBeenCalledWith(
      '<mjml><mj-body>Hello Bob</mj-body></mjml>',
      expect.any(Object)
    );
    expect(result.html).toBe('<html>Mocked MJML Output</html>');
  });

  it('should fallback to the legacy emailTemplate path if no MJML indicators are present', async () => {
    mockTemplateDbResponse({
      subject: 'Legacy Subject',
      body: '<div>Legacy Content</div>',
      options: { wrapper: 'none' } // using 'none' to simplify the asserted output
    });

    const result = await renderTemplate('legacy-template', {}, 'Subj', 'HTML');

    expect(renderWithBaseLayout).not.toHaveBeenCalled();
    // Legacy rendering uses raw string replacements instead of the MJML library
    expect(result.html).toContain('<div>Legacy Content</div>');
    expect(result.fromTemplate).toBe(true);
  });

  it('should return fallback strings if the template is not found in the database', async () => {
    mockTemplateDbResponse(null); // Template not found

    const result = await renderTemplate(
      'missing-template', 
      { name: 'Alice' }, 
      'Fallback Subject {name}', 
      '<div>Fallback HTML {name}</div>'
    );

    expect(renderWithBaseLayout).not.toHaveBeenCalled();
    expect(result.subject).toBe('Fallback Subject {name}');
    expect(result.html).toBe('<div>Fallback HTML {name}</div>');
    expect(result.fromTemplate).toBe(false);
  });
});