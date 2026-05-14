import { expect, type APIRequestContext, type Page, test } from "@playwright/test";

const adminEmail =
  process.env.PLAYWRIGHT_ADMIN_EMAIL || "tim@pappaslandscaping.com";
const adminPassword = process.env.PLAYWRIGHT_ADMIN_PASSWORD || "changeme";
const apiBaseUrl =
  process.env.PLAYWRIGHT_API_BASE_URL || "http://127.0.0.1:3010";

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

async function mockReports(page: Page) {
  await page.route("**/api/reports/business-summary", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        summary: {
          revenue: 12000,
          outstanding: 2400,
          profit: 7200,
          invoicesTotal: 18,
          quotesSent: 10,
          quotesSigned: 6,
          conversionRate: 60,
          newCustomers: 3,
          jobsTotal: 22,
          jobsCompleted: 16,
          expenses: 4800
        }
      }
    });
  });
  await page.route("**/api/kpi/dashboard", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, metrics: { closeRatio: { value: 60 } } } });
  });
  await page.route("**/api/finance/summary", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, revenueThisMonth: 12000 } });
  });
  await page.route("**/api/finance/cash-flow-forecast", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, total_expected_inflow: 4000, monthly_expense_avg: 1500, forecast: [] } });
  });
  await page.route("**/api/reports/job-costing", async (route) => {
    await route.fulfill({ contentType: "application/json", json: [{ id: 1, customer_name: "Ada", revenue: 500, profit: 300 }] });
  });
  await page.route("**/api/reports/customer-value", async (route) => {
    await route.fulfill({ contentType: "application/json", json: [{ id: 2, name: "Grace", total_invoiced: 900, invoice_count: 3 }] });
  });
  await page.route("**/api/reports/crew-performance", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, crews: [{ crew: "North", jobs_total: 5, jobs_completed: 4, total_revenue: 1200 }] } });
  });
  await page.route("**/api/reports/customer-acquisition", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, months: [{ month: "2026-05", count: 3 }] } });
  });
  await page.route("**/api/reports/sales-tax?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, summary: { taxable_sales: 1000, tax_due: 80 } } });
  });
  await page.route("**/api/reports/tax-sweep?**", async (route) => {
    await route.fulfill({ contentType: "application/json", json: { success: true, summary: { transfer_due: 80 } } });
  });
  await page.route("**/api/invoices/aging", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: { success: true, buckets: { current: { amount: 400, count: 2 } } }
    });
  });
}

async function mockAi(page: Page, postCalls: string[]) {
  await page.route("**/api/work-requests?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        source: "live_copilot",
        as_of: "2026-05-13T18:00:00.000Z",
        mode: "copilot",
        requests: [
          {
            id: "wr-1",
            external_source: "copilotcrm",
            customer_name: "Ada Customer",
            customer_address: "1 Main St",
            work_requested: "Spring cleanup",
            preferred_work_date_raw: "May 20, 2026",
            source: "Website"
          }
        ]
      }
    });
  });
  await page.route("**/api/copilot/live-jobs?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: { success: true, jobs: [{ id: 2, customer_name: "Grace Route", service_type: "Mowing", crew_assigned: "North", status: "scheduled" }] }
    });
  });
  await page.route("**/api/finance/summary", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        thisMonth: {
          revenue: 1234,
          revenue_source: "live_copilot",
          revenue_as_of: "2026-05-13T18:00:00.000Z"
        }
      }
    });
  });
  await page.route("**/api/ai/**", async (route) => {
    if (route.request().method() === "POST") {
      postCalls.push(route.request().url());
      await route.fulfill({ contentType: "application/json", json: { success: true, draft: "Prepared draft" } });
      return;
    }
    await route.fallback();
  });
}

test.describe("React reports and AI workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("Reports page loads each tab independently with quick links", async ({ page }) => {
    await mockReports(page);
    await seedSession(page);
    await page.goto("/reports");

    await expect(page.getByRole("heading", { name: "Reporting Hub" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Money Summary" })).toContainText("$12,000");

    await page.getByRole("button", { name: "Sales Pipeline" }).click();
    await expect(page.getByRole("region", { name: "Pipeline Summary" })).toContainText("60%");
    await page.getByRole("link", { name: "Open quote pipeline" }).click();
    await expect(page).toHaveURL(/\/quotes$/);

    await page.goto("/reports");
    await page.getByRole("button", { name: "Operations" }).click();
    await expect(page.getByRole("region", { name: "Job Costing" })).toContainText("Ada");
    await page.getByRole("button", { name: "Customers" }).click();
    await expect(page.getByRole("region", { name: "Customer Value" })).toContainText("Grace");
    await page.getByRole("button", { name: "Tax/Finance" }).click();
    await expect(page.getByRole("region", { name: "Sales Tax" })).toContainText("taxable sales");
    await expect(page.getByRole("region", { name: "Aging" })).toContainText("current");
  });

  test("AI page loads suggestions and does not send draft actions automatically", async ({ page }) => {
    const postCalls: string[] = [];
    await mockAi(page, postCalls);
    await seedSession(page);
    await page.goto("/ai");

    await expect(page.getByRole("heading", { name: "Assistant", exact: true })).toBeVisible();
    await expect(page.getByRole("region", { name: "Assistant actions" })).toContainText("Write rain delay text");
    await expect(page.getByRole("region", { name: "Work items AI can help with right now" })).toContainText("Ada Customer");
    await expect(page.getByRole("region", { name: "Route context for drafts" })).toContainText("Grace Route");
    await expect(page.getByRole("region", { name: "AI scope" })).toContainText("Drafts require manual review");
    await expect(page.getByRole("region", { name: "AI scope" })).toContainText("No automatic actions");
    await expect(page.locator("body")).not.toContainText("Collected revenue");
    await expect(page.locator("body")).not.toContainText("[object Object]");
    expect(postCalls).toHaveLength(0);

    await page.getByRole("button", { name: "Write rain delay text" }).click();
    await expect(page.getByRole("region", { name: "Prepared Actions" })).toContainText("Rain delay draft");
    await page.getByRole("button", { name: "Prepare Rain delay draft" }).click();
    await expect(page.getByText("Prepared draft", { exact: true })).toBeVisible();
    await expect(page.getByRole("region", { name: "Prepared Actions" }).getByText("Rain delay draft", { exact: true })).toHaveCount(2);
    await expect(page.getByText("No message was sent.")).toBeVisible();
    expect(postCalls).toHaveLength(1);
  });
});
