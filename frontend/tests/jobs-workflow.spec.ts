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

async function openJobs(page: Page) {
  await seedSession(page);
  await page.goto("/jobs");
  await expect(page.getByRole("heading", { name: "Crew Schedule", exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Crew Schedule", exact: true })).toBeVisible();
  await expect(page.getByRole("table")).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
}

test.describe("React jobs workflow", () => {
  test.beforeAll(async ({ request }) => {
    adminToken = await fetchAdminToken(request);
  });

  test("jobs table loads", async ({ page }) => {
    await openJobs(page);

    await expect(page.getByRole("columnheader", { name: "Customer" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Service" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Status" })).toBeVisible();
  });

  test("jobs search filter works", async ({ page }) => {
    await openJobs(page);

    const firstCustomer = (
      await page.locator("tbody tr").first().locator("td").nth(1).innerText()
    )
      .split("\n")[0]
      .trim();

    await page.getByPlaceholder("Search jobs...").fill(firstCustomer);
    await expect(page.locator("tbody tr").first()).toContainText(firstCustomer);

    await page.getByPlaceholder("Search jobs...").fill("__no_matching_job__");
    await expect(page.getByText("No jobs found")).toBeVisible();
  });

  test("jobs status filter works", async ({ page }) => {
    await openJobs(page);

    const firstStatus = (
      await page.locator("tbody tr").first().locator("td").nth(5).innerText()
    ).trim();

    await page.getByLabel("Filter by status").selectOption({ label: firstStatus });
    await expect(page.locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("tbody tr").first().locator("td").nth(5)).toHaveText(
      new RegExp(firstStatus, "i")
    );
  });

  test("clicking a job row opens /jobs/[id]", async ({ page }) => {
    await openJobs(page);

    const jobLink = page.locator("tbody tr").first().locator("td").nth(1).locator("a");
    const customerName = (await jobLink.innerText()).trim();

    await jobLink.click();

    await expect(page).toHaveURL(/\/jobs\/\d+$/);
    await expect(page.getByRole("heading", { name: "Job Information" })).toBeVisible();
    await expect(page.getByText(customerName).first()).toBeVisible();
    await expect(page.getByRole("link", { name: "Back to Jobs" })).toBeVisible();
  });

  test("operations panels load crew availability and completed-uninvoiced work", async ({ page }) => {
    await page.route("**/api/jobs?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, jobs: [{ id: 10, customer_name: "Ada", service_type: "Mowing", status: "pending", job_date: "2026-05-13" }] } });
    });
    await page.route("**/api/jobs/stats?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, stats: { total: 1, byStatus: { pending: 1 }, byCrew: { North: 1 }, totalRevenue: 50 } } });
    });
    await page.route("**/api/jobs/completed-uninvoiced", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, jobs: [{ id: 11, customer_name: "Grace", service_type: "Cleanup", service_price: 200, job_date: "2026-05-12" }] } });
    });
    await page.route("**/api/dispatch/crew-availability?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, crews: [{ crew_name: "North", job_count: 3, total_hours: 4.5 }] } });
    });
    await page.route("**/api/jobs/pipeline", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, stages: { scheduled: [{ id: 10 }] } } });
    });
    await page.route("**/api/copilot/live-jobs?**", async (route) => {
      await route.fulfill({ contentType: "application/json", json: { success: true, jobs: [{ id: 12, customer_name: "Live Customer", service_type: "Mowing", crew_assigned: "North" }] } });
    });

    await seedSession(page);
    await page.goto("/jobs");

    await expect(page.getByRole("region", { name: "Today by Crew" })).toContainText("Ada");
    await expect(page.getByRole("region", { name: "Crew Readiness" })).toContainText("North");
    await expect(page.getByRole("region", { name: "Completed Not Invoiced" })).toContainText("Grace");
    await expect(page.getByRole("region", { name: "Missing info and blockers" })).toContainText("Ada");
    await expect(page.getByRole("region", { name: "Missing info and blockers" })).toContainText("missing address");
    await expect(page.getByRole("region", { name: "Copilot Live Jobs" })).toContainText("Live Customer");
  });
});
