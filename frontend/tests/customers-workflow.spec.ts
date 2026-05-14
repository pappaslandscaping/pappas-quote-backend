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
  await expect(page.getByRole("link", { name: "Leads & Estimates" })).toBeVisible();
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
    let draftCalls = 0;
    await page.route("**/api/customers/*/360", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        json: {
          success: true,
          customer360: {
            customer: { id: 7, name: "Ada Customer" },
            summary: {
              quote_count: 1,
              signed_quote_count: 0,
              job_count: 1,
              completed_job_count: 0,
              invoice_count: 1,
              open_invoice_balance: 250,
              payment_count: 1,
              communication_count: 1,
              note_count: 1
            },
            sources: {
              quotes: { status: "live", source: "local_database" },
              jobs: { status: "live", source: "local_database" },
              invoices: { status: "live", source: "local_database" },
              payments: { status: "live", source: "local_database" },
              communications: { status: "live", source: "local_database" },
              notes: { status: "live", source: "local_database" }
            },
            records: {
              quotes: [],
              jobs: [],
              invoices: [],
              payments: [],
              communications: [],
              notes: []
            },
            timeline: [
              {
                id: "quote-1",
                type: "quote",
                title: "Quote #Q-1",
                detail: "mowing: Weekly mowing",
                status: "sent",
                date: "2026-05-12T12:00:00.000Z",
                amount: 250,
                href: "/quotes/1",
                source: "local_database"
              },
              {
                id: "message-1",
                type: "communication",
                title: "Inbound message",
                detail: "Can you send an update?",
                status: "received",
                date: "2026-05-13T12:00:00.000Z",
                source: "local_database"
              }
            ],
            ai: {
              mode: "draft_only",
              allowed_actions: ["prepare_followup_draft"],
              blocked_actions: ["send_email", "send_sms", "collect_payment"]
            }
          }
        }
      });
    });
    await page.route("**/api/ai/generate-followup", async (route) => {
      draftCalls += 1;
      await route.fulfill({
        contentType: "application/json",
        json: { success: true, draft: "Prepared customer follow-up draft" }
      });
    });

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
      page.getByRole("heading", { name: "Contact / Property / Services" })
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Chronological Timeline" })
    ).toBeVisible();
    await expect(page.locator('[aria-label="Customer 360 sources"]')).toContainText("quotes: live");
    await expect(page.getByText("Quote #Q-1")).toBeVisible();
    await expect(page.getByText("[object Object]")).toHaveCount(0);
    await expect(page.getByText(/do not send email, SMS, payments, or job updates/i)).toBeVisible();
    expect(draftCalls).toBe(0);
    await page.getByRole("button", { name: "Prepare follow-up draft" }).click();
    await expect(page.getByText("Prepared customer follow-up draft")).toBeVisible();
    await expect(page.getByText("No email or SMS was sent.")).toBeVisible();
    expect(draftCalls).toBe(1);
    await expect(
      page.getByRole("link", { name: "Back to Customers" })
    ).toBeVisible();
  });
});
