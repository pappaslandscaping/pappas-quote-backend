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

async function clearSession(page: Page) {
  await page.goto("/login");
  await page.evaluate(() => window.localStorage.clear());
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

async function login(page: Page) {
  await page.goto("/login");
  await page.getByLabel(/email/i).fill(adminEmail);
  await page.getByLabel(/password/i).fill(adminPassword);
  await page.getByRole("button", { name: "Sign in" }).click();

  await expect(page).toHaveURL(/\/$/);
  await expect(
    page.getByRole("heading", { name: "Command Center" })
  ).toBeVisible();
}

async function expectQuotesTable(page: Page) {
  await expect(page.getByRole("table")).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
}

test.describe("React quote workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test.beforeEach(async ({ page }) => {
    await clearSession(page);
  });

  test("login page loads", async ({ page }) => {
    await page.goto("/login");

    await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
    await expect(page.getByLabel(/email/i)).toBeVisible();
    await expect(page.getByLabel(/password/i)).toBeVisible();
    await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  });

  test("unauthenticated quotes route redirects to /login", async ({ page }) => {
    await page.goto("/quotes");

    await expect(page).toHaveURL(/\/login$/);
    await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
  });

  test("successful login redirects to /", async ({ page }) => {
    await login(page);
    const primaryNav = page.getByRole("navigation", { name: "Primary navigation" });
    await expect(primaryNav).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Command Center", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Leads & Estimates", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Customers", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Invoices", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Payments", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Crew Schedule", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Inbox", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Reports", exact: true })).toBeVisible();
    await expect(primaryNav.getByRole("link", { name: "Assistant", exact: true })).toBeVisible();
  });

  test("leads and estimates pipeline loads", async ({ page }) => {
    await seedSession(page);
    await page.goto("/quotes");
    await expect(page.getByRole("heading", { name: "Leads & Estimates" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Sales queue" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Needs response" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Estimate needed" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Follow-up due" })).toBeVisible();
    await expect(page.getByRole("region", { name: "Selected lead detail" })).toContainText("Next best action");
    await expect(page.getByRole("link", { name: "AI email draft" }).first()).toBeVisible();
    await expectQuotesTable(page);
  });

  test("search filter works", async ({ page }) => {
    await seedSession(page);
    await page.goto("/quotes");
    await expectQuotesTable(page);

    const firstCustomer = (
      await page.locator("tbody tr").first().locator("td").nth(1).innerText()
    )
      .split("\n")[0]
      .trim();

    await page
      .getByPlaceholder("Search name, phone, email, address...")
      .fill(firstCustomer);
    await expect(page.locator("tbody tr").first()).toContainText(firstCustomer);

    await page
      .getByPlaceholder("Search name, phone, email, address...")
      .fill("__no_matching_quote__");
    await expect(page.getByText("No quote requests found")).toBeVisible();
  });

  test("status filter works", async ({ page }) => {
    await seedSession(page);
    await page.goto("/quotes");
    await expectQuotesTable(page);

    await page.getByLabel("Filter by status").selectOption("new");
    await expect(page.locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("tbody tr").first().locator("td").last()).toHaveText(
      /new/i
    );
  });

  test("clicking a quote row opens /quotes/[id]", async ({ page }) => {
    await seedSession(page);
    await page.goto("/quotes");
    await expectQuotesTable(page);

    const firstRow = page.locator("tbody tr").first();
    const quoteLink = firstRow.locator("td").nth(1).locator("a").first();
    const quoteName = (await quoteLink.innerText()).trim();

    await quoteLink.click();

    await expect(page).toHaveURL(/\/quotes\/\d+$/);
    await expect(page.getByRole("heading", { name: quoteName })).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Contact Information" })
    ).toBeVisible();
    await expect(page.getByRole("link", { name: "Back to Leads & Estimates" })).toBeVisible();
  });

  test("logout clears the session and returns to /login", async ({ page }) => {
    await seedSession(page);
    await page.goto("/quotes");
    await expect(page.getByRole("heading", { name: "Leads & Estimates" })).toBeVisible();
    await expect(page.getByText("Tim Pappas")).toBeVisible();
    await expect(page.getByText("tim@pappaslandscaping.com")).toBeVisible();
    await expect(
      page.evaluate(() => window.localStorage.getItem("adminToken"))
    ).resolves.toBeTruthy();

    await page.getByRole("button", { name: "Logout" }).click();

    await expect(page).toHaveURL(/\/login$/);
    await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
    await expect(
      page.evaluate(() => window.localStorage.getItem("adminToken"))
    ).resolves.toBeNull();
  });
});
