const fs = require('fs');
const agreement = require('../public/js/snow-agreement');
const { generateSnowAgreementPDF } = require('../lib/snow-agreement-pdf');

async function main() {
  const doc = agreement.create({
    quote_number: '[Estimate number]', customer_name: '[Client or business name]',
    customer_email: '[Client email]', customer_phone: '[Client phone]',
    customer_address: '[Service property address]', total: 0,
    services: [{ name: 'Snow Removal Contract', price: 0, description: '2026-2027: December 1, 2026 through March 31, 2027.' }]
  });
  doc.title = 'Snow & Ice Service Agreement';
  doc.estimate_number = '[Estimate number] | REVIEW COPY';
  doc.total = null;
  doc.services = [{ name: '[Accepted snow or ice service]', amount: null, property_address: '[Service property address from accepted estimate]', description:
    'Winter season: 2026-27\nCoverage dates: [Start date] through [End date], as accepted for this property\nAreas included: [Driveway / parking area / walkways / sidewalks / steps]\nSnowfall trigger: [Accepted trigger; two inches unless otherwise stated]\nSalt or deicing: [Included scope and price, or not included]\nBilling basis: [Fixed seasonal / fixed monthly / per-push / per-visit]\nPayment schedule: [Amounts, installment count and due dates from accepted estimate]\nSpecial instructions: [Accepted property-specific requirements]\nRepeat this service block for each accepted line item and property.' }];
  const output = process.argv[2] || '/tmp/Pappas_Snow_Agreement_2026-27.pdf';
  fs.writeFileSync(output, await generateSnowAgreementPDF(doc));
  fs.writeFileSync(output.replace(/\.pdf$/, '.html'), agreement.html(doc));
  console.log(output);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
