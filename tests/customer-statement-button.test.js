const fs = require('fs');
const path = require('path');

test('customer statement button fetches the protected PDF before opening it', () => {
  const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'customer-detail.html'), 'utf8');
  const functionMatch = html.match(/async function openStatement\(\) \{[\s\S]*?\n\}/);

  expect(functionMatch).not.toBeNull();
  expect(functionMatch[0]).toContain("await fetch('/api/customers/'");
  expect(functionMatch[0]).toContain('URL.createObjectURL(await res.blob())');
  expect(functionMatch[0]).not.toContain("window.open('/api/customers/");
});
