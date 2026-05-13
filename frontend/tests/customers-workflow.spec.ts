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
  await page.evaluate(() => window.localStorage.clear());
  await page.evaluate((token) => {
    window.localStorage.setItem("adminToken", token);
    window.localStorage.setItem("adminName", "Tim Pappas");
    window.localStorage.setItem("adminEmail", "tim@pappaslandscaping.com");
  }, adminToken);
}

async function openCustomers(page: Page) {
  await seedSession(page);
  await page.goto("/customers");
  await expect(page.getByRole("heading", { name: "Clients" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Quotes" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Customers" })).toBeVisible();
  await expect(page.getByRole("table")).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
}

test.describe("React customer workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("customers table loads", async ({ page }) => {
    await openCustomers(page);

    await expect(page.getByRole("columnheader", { name: "Name" })).toBeVisible();
    await expect(page.locator("tbody tr").first()).toContainText(/.+/);
  });

  test("customer search filter works", async ({ page }) => {
    await openCustomers(page);

    const firstCustomer = (
      await page.locator("tbody tr").first().locator("td").nth(1).innerText()
    )
      .split("\n")[0]
      .trim();

    await page
      .getByPlaceholder("Search name, email, address...")
      .fill(firstCustomer);
    await expect(page.locator("tbody tr").first()).toContainText(firstCustomer);

    await page
      .getByPlaceholder("Search name, email, address...")
      .fill("__no_matching_customer__");
    await expect(page.getByText("No customers found")).toBeVisible();
  });

  test("customer status filter works", async ({ page }) => {
    await openCustomers(page);

    const firstStatus = (
      await page.locator("tbody tr").first().locator("td").last().innerText()
    ).trim();

    await page.getByLabel("Filter by status").selectOption(firstStatus);
    await expect(page.locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("tbody tr").first().locator("td").last()).toHaveText(
      new RegExp(firstStatus, "i")
    );
  });

  test("clicking a customer row opens /customers/[id]", async ({ page }) => {
    await openCustomers(page);

    const customerLink = page
      .locator("tbody tr")
      .first()
      .locator("td")
      .nth(1)
      .locator("a")
      .first();
    const customerName = (await customerLink.innerText()).trim();

    await customerLink.click();

    await expect(page).toHaveURL(/\/customers\/\d+$/);
    await expect(page.getByRole("heading", { name: customerName })).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Contact Information" })
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Back to Customers" })
    ).toBeVisible();
  });
});
