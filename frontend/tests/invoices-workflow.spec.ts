import { expect, type APIRequestContext, type Page, test } from "@playwright/test";

const adminEmail =
  process.env.PLAYWRIGHT_ADMIN_EMAIL || "tim@pappaslandscaping.com";
const adminPassword = process.env.PLAYWRIGHT_ADMIN_PASSWORD || "changeme";
const apiBaseUrl =
  process.env.PLAYWRIGHT_API_BASE_URL || "http://127.0.0.1:3010";

let adminToken = "";

async function fetchAdminToken(request: APIRequestContext) {
  const response = await request.post(`${apiBaseUrl}/api/auth/login`, {
    data: {
      email: adminEmail,
      password: adminPassword
    }
  });

  expect(response.ok()).toBeTruthy();
  const data = (await response.json()) as { token?: string };
  expect(data.token).toBeTruthy();
  return data.token || "";
}

async function seedSession(page: Page) {
  await page.goto("/login");
  await page.evaluate((token) => {
    window.localStorage.clear();
    window.localStorage.setItem("adminToken", token);
    window.localStorage.setItem("adminName", "Tim Pappas");
    window.localStorage.setItem("adminEmail", "tim@pappaslandscaping.com");
  }, adminToken);
}

async function openInvoices(page: Page) {
  await seedSession(page);
  await page.goto("/invoices");
  await expect(page.getByRole("heading", { name: "Invoices", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Invoices", exact: true })).toBeVisible();
  await expect(page.getByRole("table")).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
}

test.describe("React invoice workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("invoices table loads", async ({ page }) => {
    await openInvoices(page);

    await expect(page.getByRole("columnheader", { name: "Invoice #" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Customer" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Amount" })).toBeVisible();
  });

  test("invoice search filter works", async ({ page }) => {
    await openInvoices(page);

    const firstInvoice = (
      await page.locator("tbody tr").first().locator("td").first().innerText()
    ).trim();

    await page
      .getByPlaceholder("Search by invoice # or customer...")
      .fill(firstInvoice);
    await expect(page.locator("tbody tr").first()).toContainText(firstInvoice);

    await page
      .getByPlaceholder("Search by invoice # or customer...")
      .fill("__no_matching_invoice__");
    await expect(page.getByText("No invoices found")).toBeVisible();
  });

  test("invoice status filter works", async ({ page }) => {
    await openInvoices(page);

    const firstStatus = (
      await page.locator("tbody tr").first().locator("td").nth(4).innerText()
    ).trim();

    await page.getByLabel("Filter by status").selectOption(firstStatus);
    await expect(page.locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("tbody tr").first().locator("td").nth(4)).toHaveText(
      new RegExp(firstStatus, "i")
    );
  });

  test("clicking an invoice row opens /invoices/[id]", async ({ page }) => {
    await openInvoices(page);

    const invoiceLink = page.locator("tbody tr").first().locator("td").first().locator("a");
    const invoiceNumber = (await invoiceLink.innerText()).trim();

    await invoiceLink.click();

    await expect(page).toHaveURL(/\/invoices\/\d+$/);
    await expect(page.getByRole("heading", { name: invoiceNumber })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Invoice Info" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Line Items" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Back to Invoices" })).toBeVisible();
  });
});
