import { expect, type APIRequestContext, type Page, test } from "@playwright/test";

const adminEmail =
  process.env.PLAYWRIGHT_ADMIN_EMAIL || "tim@pappaslandscaping.com";
const adminPassword = process.env.PLAYWRIGHT_ADMIN_PASSWORD || "changeme";
const apiBaseUrl =
  process.env.PLAYWRIGHT_API_BASE_URL || "http://127.0.0.1:3010";

let adminToken = "";

function todayIso() {
  return new Date().toISOString();
}

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

async function mockDashboardApis(page: Page) {
  await page.route("**/api/dashboard/today-summary", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        jobs_today: 4,
        revenue_today: 1250,
        pending_quotes: 3,
        overdue_invoices: 2,
        unread_messages: 0
      }
    });
  });
  await page.route("**/api/dashboard/activity-feed", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        events: [
          { type: "quote", description: "Quote sent to Ada", timestamp: "2026-05-13" }
        ]
      }
    });
  });
  await page.route("**/api/quotes", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        quotes: [
          { id: 1, name: "Ada", status: "new", created_at: "2026-05-13" },
          { id: 2, name: "Grace", status: "quoted", created_at: "2026-05-13" }
        ]
      }
    });
  });
  await page.route("**/api/invoices/stats", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        stats: {
          total: 10,
          draft: 0,
          pending: 1,
          partial: 0,
          sent: 4,
          paid: 5,
          overdue: 2,
          void: 0,
          outstanding: 900,
          overdueAmount: 300,
          paidThisMonth: 1500,
          totalRevenue: 4500
        }
      }
    });
  });
  await page.route("**/api/jobs/dashboard", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        stats: { today: 4, thisWeek: 12, pending: 3 },
        upcoming: [
          {
            id: 101,
            customer_name: "Ada Lovelace",
            service_type: "Mowing",
            crew_assigned: "North",
            job_date: "2026-05-13",
            status: "scheduled"
          }
        ]
      }
    });
  });
  await page.route("**/api/jobs/completed-uninvoiced", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: { success: true, jobs: [{ id: 201, customer_name: "Grace Hopper" }] }
    });
  });
  await page.route("**/api/payments?**", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: {
        success: true,
        payments: [
          {
            id: 301,
            invoice_id: 401,
            customer_name: "Paid Today Customer",
            invoice_number: "INV-401",
            amount_paid: 250,
            method: "card",
            status: "paid",
            paid_at: todayIso(),
            updated_at: "2026-05-13T12:00:00.000Z"
          },
          {
            id: 302,
            invoice_id: 402,
            customer_name: "Updated Only Customer",
            invoice_number: "INV-402",
            amount_paid: 999,
            method: "card",
            status: "paid",
            paid_at: null,
            updated_at: todayIso()
          }
        ]
      }
    });
  });
  await page.route("**/api/finance/summary", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      json: { revenueThisMonth: 4500 }
    });
  });
}

test.describe("React dashboard workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("Home dashboard shows real daily summary numbers", async ({ page }) => {
    await mockDashboardApis(page);
    await seedSession(page);
    await page.goto("/");

    await expect(page.getByRole("heading", { name: "Daily Brief" })).toBeVisible();
    const today = page.getByRole("region", { name: "Today" });
    await expect(today.locator(".stat-card", { hasText: "Jobs today" }).locator("strong")).toHaveText("4");
    await expect(today.locator(".stat-card", { hasText: "Paid today" }).locator("strong")).toHaveText("$1,250");
    await expect(today.locator(".stat-card", { hasText: "Pending quotes" }).locator("strong")).toHaveText("3");
    await expect(today.locator(".stat-card", { hasText: "Overdue invoices" }).locator("strong")).toHaveText("2");
    await expect(today.getByText("Loading")).toHaveCount(0);
  });

  test("Daily Brief panels render success and API health matches", async ({ page }) => {
    await mockDashboardApis(page);
    await seedSession(page);
    await page.goto("/");

    const attention = page.getByRole("region", { name: "Follow-ups needed" });
    await expect(attention.getByText("New quote requests")).toBeVisible();
    await expect(attention.getByText("Overdue invoices")).toBeVisible();
    await expect(attention.getByText("Completed-uninvoiced jobs")).toBeVisible();
    await expect(attention.getByText("Follow-up draft candidates")).toBeVisible();
    await expect(page.getByRole("region", { name: "Recent Activity" })).toContainText("2026");

    await expect(page.getByRole("region", { name: "True payment activity" })).toContainText("Paid Today Customer");
    await expect(page.getByRole("region", { name: "True payment activity" })).not.toContainText("Updated Only Customer");

    const health = page.getByRole("region", { name: "API Health" });
    await expect(health.getByText("Loaded")).toHaveCount(8);
    await expect(health.locator(".dashboard-health-row", { hasText: "Finance" })).toContainText("Loaded");
    await expect(health.getByText("Loading")).toHaveCount(0);
    await expect(health.getByText("Error")).toHaveCount(0);
  });

  test("Daily Brief shows empty and error states distinctly", async ({ page }) => {
    await page.route("**/api/dashboard/today-summary", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true } });
    });
    await page.route("**/api/dashboard/activity-feed", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, events: [] } });
    });
    await page.route("**/api/quotes", async (route) => {
      await route.fulfill({ status: 500, contentType: "application/json", json: { error: "Quotes offline" } });
    });
    await page.route("**/api/invoices/stats", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, stats: { overdue: 0 } } });
    });
    await page.route("**/api/jobs/dashboard", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, upcoming: [] } });
    });
    await page.route("**/api/jobs/completed-uninvoiced", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, jobs: [] } });
    });
    await page.route("**/api/payments?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, payments: [] } });
    });
    await page.route("**/api/finance/summary", async (route) => {
      await route.fulfill({ contentType: "application/json", json: {} });
    });

    await seedSession(page);
    await page.goto("/");

    await expect(page.getByRole("region", { name: "Follow-ups needed" }).getByText("Some data failed: Quotes offline")).toBeVisible();
    await expect(page.getByRole("region", { name: "Upcoming Work" }).getByText("No upcoming jobs found.")).toBeVisible();
    await expect(page.getByRole("region", { name: "True payment activity" }).getByText("No payments with paid_at recorded today.")).toBeVisible();
    await expect(page.getByRole("region", { name: "API Health" }).getByText("Error")).toBeVisible();
    await expect(page.getByRole("region", { name: "API Health" }).getByText("Empty")).toHaveCount(4);
  });

  test("Daily Brief renders partial results while one source is still loading", async ({ page }) => {
    await page.route("**/api/dashboard/today-summary", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true } });
    });
    await page.route("**/api/dashboard/activity-feed", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, events: [] } });
    });
    await page.route("**/api/quotes", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, quotes: [{ id: 1, status: "new" }] } });
    });
    await page.route("**/api/invoices/stats", async () => {
      await new Promise(() => undefined);
    });
    await page.route("**/api/jobs/dashboard", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, upcoming: [] } });
    });
    await page.route("**/api/jobs/completed-uninvoiced", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, jobs: [] } });
    });
    await page.route("**/api/payments?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, payments: [] } });
    });
    await page.route("**/api/finance/summary", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true } });
    });

    await seedSession(page);
    await page.goto("/");

    const attention = page.getByRole("region", { name: "Follow-ups needed" });
    await expect(attention.getByText("New quote requests")).toBeVisible();
    await expect(attention.getByText("Some data is still loading.")).toBeVisible();
  });

  test("dashboard quick links navigate to converted areas", async ({ page }) => {
    await mockDashboardApis(page);
    await seedSession(page);
    await page.goto("/");

    const quickLinks = page.getByRole("region", { name: "Quick Actions" });

    await quickLinks.getByRole("link", { name: "New quote" }).click();
    await expect(page).toHaveURL(/\/quotes$/);
    await expect(page.getByRole("heading", { name: "Quote Requests" })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "View customers" }).click();
    await expect(page).toHaveURL(/\/customers$/);
    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "View invoices" }).click();
    await expect(page).toHaveURL(/\/invoices$/);
    await expect(page.getByRole("heading", { name: "Invoices", exact: true })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "View jobs" }).click();
    await expect(page).toHaveURL(/\/jobs$/);
    await expect(page.getByRole("heading", { name: "Schedule" })).toBeVisible();
  });
});
