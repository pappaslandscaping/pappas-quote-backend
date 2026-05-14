import { expect, type APIRequestContext, type Page, test } from "@playwright/test";

const adminEmail = process.env.PLAYWRIGHT_ADMIN_EMAIL || "tim@pappaslandscaping.com";
const adminPassword = process.env.PLAYWRIGHT_ADMIN_PASSWORD || "changeme";
const apiBaseUrl = process.env.PLAYWRIGHT_API_BASE_URL || "http://127.0.0.1:3010";

let adminToken = "";

async function fetchAdminToken(request: APIRequestContext) {
  const response = await request.post(`${apiBaseUrl}/api/auth/login`, {
    data: { email: adminEmail, password: adminPassword }
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

async function mockPayments(page: Page) {
  await page.route("**/api/payments?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, payments: [], total: 0, totalReceived: 0, monthly: [] } });
  });
  await page.route("**/api/payment-records?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        total: 1,
        payments: [{
          id: 1,
          invoice_id: 44,
          customer_id: 7,
          customer_name: "Ada Customer",
          invoice_number: "INV-44",
          amount: 222.76,
          paid_at: "2026-05-12T12:00:00Z",
          external_source: "copilotcrm",
          link_status: "linked"
        }]
      }
    });
  });
  await page.route("**/api/copilot/payment-review?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        summary: { total_rows: 1, linked_count: 0, unresolved_count: 1 },
        payments: [{ id: 2, customer_name: "Unmatched Customer", amount: 50, link_status: "unresolved", link_reason: "No invoice match" }]
      }
    });
  });
  await page.route("**/api/reports/tax-sweep?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, summary: { tax_portion_collected: 18 } } });
  });
  await page.route("**/api/reports/tax-transfer-freshness-status", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, status: { ui_state: "current" } } });
  });
  await page.route("**/api/tax-transfer-instructions?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, instructions: [] } });
  });
  await page.route("**/api/invoices/aging", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, buckets: {} } });
  });
}

async function mockCommunications(page: Page, postCalls: string[]) {
  await page.route("**/api/messages?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, messages: [{ id: 1, direction: "inbound", body: "Can you call me?", customerName: "Grace", timestamp: "2026-05-13T12:00:00Z", status: "received" }] } });
  });
  await page.route("**/api/calls?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, calls: [{ id: 2, customer_name: "Ada", status: "voicemail", transcription: "Please call back", created_at: "2026-05-13T11:00:00Z" }] } });
  });
  await page.route("**/api/app/voicemails", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { voicemails: [{ id: "v1", customer_name: "Tim", status: "voicemail", transcript: "Need service", created_at: "2026-05-13T10:00:00Z" }] } });
  });
  await page.route("**/api/ai/**", async (route) => {
    if (route.request().method() === "POST") {
      postCalls.push(route.request().url());
      await route.fulfill({ contentType: "application/json", json: { success: true, draft: "Prepared communication draft" } });
      return;
    }
    await route.fallback();
  });
}

test.describe("Payments and communications workflows", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("Payments page loads list, filters, and true payment dates", async ({ page }) => {
    await mockPayments(page);
    await seedSession(page);
    await page.goto("/payments");

    await expect(page.getByRole("heading", { name: "Payments" })).toBeVisible();
    await expect(page.getByRole("table")).toContainText("Ada Customer");
    await expect(page.getByRole("table")).toContainText("May 12, 2026");
    await page.getByLabel("Search payments").fill("Ada");
    await expect(page.getByRole("region", { name: "Needs Review" })).toContainText("Unmatched Customer");
  });

  test("Communications loads draft-only AI helper and does not send automatically", async ({ page }) => {
    const postCalls: string[] = [];
    await mockCommunications(page, postCalls);
    await seedSession(page);
    await page.goto("/communications");

    await expect(page.getByRole("heading", { name: "Inbox", exact: true })).toBeVisible();
    await expect(page.getByRole("region", { name: "Needs attention first" })).toContainText("Grace");
    await expect(page.getByRole("region", { name: "Needs attention first" })).toContainText("Draft a customer reply");
    await page.getByRole("button", { name: "AI draft reply" }).first().click();
    await expect(page.getByLabel("Draft context")).toHaveValue(/Draft a manual customer reply/);
    await expect(page.getByRole("region", { name: "Unread Messages" })).toContainText("Can you call me?");
    await expect(page.getByRole("region", { name: "Customer Replies" })).toContainText("Can you call me?");
    await expect(page.getByRole("region", { name: "AI Reply Drafts" })).toContainText("Sending stays manual");
    expect(postCalls).toHaveLength(0);

    await page.getByRole("button", { name: "Prepare reply draft" }).click();
    await expect(page.getByText("Prepared communication draft")).toBeVisible();
    await expect(page.getByText("No email or SMS was sent.")).toBeVisible();
    expect(postCalls).toHaveLength(1);
  });
});
