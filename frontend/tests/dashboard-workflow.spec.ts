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

    await expect(page.getByRole("region", { name: "Business snapshots" })).toBeVisible();
    await expect(page.getByText("Quotes").first()).toBeVisible();
    await expect(page.getByText("Customers").first()).toBeVisible();
    await expect(page.getByText("Invoices").first()).toBeVisible();
    await expect(page.getByText("Scheduling/Jobs").first()).toBeVisible();
    await expect(page.getByRole("region", { name: "Dashboard quick links" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Dashboard health" })).toBeVisible();
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
