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

async function openDashboard(page: Page) {
  await seedSession(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Business Dashboard" })).toBeVisible();
}

test.describe("React dashboard workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("dashboard loads business snapshots", async ({ page }) => {
    await openDashboard(page);

    const snapshots = page.getByRole("region", { name: "Business snapshots" });
    const health = page.getByRole("region", { name: "Dashboard health" });

    await expect(snapshots).toBeVisible();
    await expect(page.getByRole("link", { name: "Home", exact: true })).toBeVisible();
    await expect(page.getByText("Quotes").first()).toBeVisible();
    await expect(page.getByText("Customers").first()).toBeVisible();
    await expect(page.getByText("Invoices").first()).toBeVisible();
    await expect(page.getByText("Scheduling/Jobs").first()).toBeVisible();
    await expect(page.getByRole("region", { name: "Dashboard quick links" })).toBeVisible();
    await expect(health).toBeVisible();
  });

  test("dashboard cards and API health use matching loaded states", async ({ page }) => {
    await page.route("**/api/quotes", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          quotes: [
            { id: 1, name: "One", status: "new", created_at: "2026-01-01" },
            { id: 2, name: "Two", status: "quoted", created_at: "2026-01-02" }
          ]
        }
      });
    });
    await page.route("**/api/customers/stats", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          stats: { total: 12, active: 10, inactive: 2 }
        }
      });
    });
    await page.route("**/api/customers/pipeline-stats", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          stats: {
            totalLeads: 3,
            totalCustomers: 12,
            newLeadsThisMonth: 1,
            convertedThisMonth: 1,
            conversionRate: 25
          }
        }
      });
    });
    await page.route("**/api/invoices/stats", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          stats: {
            total: 8,
            draft: 0,
            pending: 1,
            partial: 0,
            sent: 2,
            paid: 5,
            overdue: 1,
            void: 0,
            outstanding: 1200,
            overdueAmount: 200,
            paidThisMonth: 900,
            totalRevenue: 5000
          }
        }
      });
    });
    await page.route("**/api/jobs/stats", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          stats: {
            total: 6,
            byStatus: { pending: 2, completed: 4 },
            totalRevenue: 1800,
            byCrew: {}
          }
        }
      });
    });

    await openDashboard(page);

    const snapshots = page.getByRole("region", { name: "Business snapshots" });
    const health = page.getByRole("region", { name: "Dashboard health" });

    await expect(snapshots.getByText("Loading")).toHaveCount(0);
    await expect(snapshots.locator("strong", { hasText: "-" })).toHaveCount(0);
    await expect(snapshots.locator(".stat-card", { hasText: "Quotes" }).locator("strong")).toHaveText("2");
    await expect(snapshots.locator(".stat-card", { hasText: "Customers" }).locator("strong")).toHaveText("12");
    await expect(snapshots.locator(".stat-card", { hasText: "Invoices" }).locator("strong")).toHaveText("8");
    await expect(snapshots.locator(".stat-card", { hasText: "Scheduling/Jobs" }).locator("strong")).toHaveText("6");
    await expect(health.getByText("Loaded")).toHaveCount(4);
    await expect(health.getByText("Loading")).toHaveCount(0);
    await expect(health.getByText("Error")).toHaveCount(0);
  });

  test("dashboard quick links navigate to converted areas", async ({ page }) => {
    await openDashboard(page);

    const quickLinks = page.getByRole("region", { name: "Dashboard quick links" });

    await quickLinks.getByRole("link", { name: "Quotes" }).click();
    await expect(page).toHaveURL(/\/quotes$/);
    await expect(page.getByRole("heading", { name: "Quote Requests" })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "Customers" }).click();
    await expect(page).toHaveURL(/\/customers$/);
    await expect(page.getByRole("heading", { name: "Customers" })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "Invoices" }).click();
    await expect(page).toHaveURL(/\/invoices$/);
    await expect(page.getByRole("heading", { name: "Invoices", exact: true })).toBeVisible();

    await page.goto("/");
    await quickLinks.getByRole("link", { name: "Scheduling/Jobs" }).click();
    await expect(page).toHaveURL(/\/jobs$/);
    await expect(page.getByRole("heading", { name: "Schedule" })).toBeVisible();
  });
});
