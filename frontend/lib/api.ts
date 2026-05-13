import type {
  LoginPayload,
  LoginResponse,
  QuoteCreatePayload,
  QuoteCreateResponse,
  QuoteDeleteResponse,
  QuoteDetailResponse,
  QuoteListResponse
} from "../types/quotes";
import type {
  Customer,
  CustomerDetailResponse,
  CustomerInvoice,
  CustomerJob,
  CustomerListParams,
  CustomerListResponse,
  CustomerPipelineStatsResponse,
  CustomerProperty,
  CustomerQuote,
  CustomerRelatedResponse,
  CustomerStatsResponse
} from "../types/customers";
import type {
  InvoiceDetailResponse,
  InvoiceListParams,
  InvoiceListResponse,
  InvoiceStatsResponse
} from "../types/invoices";
import type {
  JobDetailResponse,
  JobListParams,
  JobListResponse,
  JobStatsResponse
} from "../types/jobs";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "") ||
  "http://localhost:3000";

function getAdminToken() {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem("adminToken");
}

async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const token = getAdminToken();

  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers
  });

  const data = (await response.json().catch(() => ({}))) as T & {
    error?: string;
  };

  if (!response.ok) {
    throw new Error(data.error || `Request failed with ${response.status}`);
  }

  return data;
}

export async function fetchQuoteRequests() {
  const data = await apiFetch<QuoteListResponse>("/api/quotes");
  if (!data.success) {
    throw new Error(data.error || "Failed to load quotes");
  }
  return data.quotes || [];
}

export async function fetchQuoteRequest(id: string | number) {
  const data = await apiFetch<QuoteDetailResponse>(`/api/quotes/${id}`);
  if (!data.success || !data.quote) {
    throw new Error(data.error || "Failed to load quote");
  }
  return data.quote;
}

export async function createQuoteRequest(payload: QuoteCreatePayload) {
  const data = await apiFetch<QuoteCreateResponse>("/api/quotes/admin", {
    method: "POST",
    body: JSON.stringify(payload)
  });

  if (!data.success) {
    throw new Error(data.error || "Failed to create quote request");
  }

  return data.quote;
}

export async function updateQuoteStatus(id: string | number, status: string) {
  const data = await apiFetch<QuoteDetailResponse>(`/api/quotes/${id}`, {
    method: "PATCH",
    body: JSON.stringify({ status })
  });

  if (!data.success || !data.quote) {
    throw new Error(data.error || "Failed to update quote");
  }

  return data.quote;
}

export async function deleteQuoteRequest(id: string | number) {
  const data = await apiFetch<QuoteDeleteResponse>(`/api/quotes/${id}`, {
    method: "DELETE"
  });

  if (!data.success) {
    throw new Error(data.error || "Failed to delete quote");
  }

  return data.deleted;
}

export async function login(payload: LoginPayload) {
  const data = await apiFetch<LoginResponse>("/api/auth/login", {
    method: "POST",
    body: JSON.stringify(payload)
  });

  if (!data.success || !data.token) {
    throw new Error(data.error || "Login failed");
  }

  return data;
}

function customerQuery(params: CustomerListParams = {}) {
  const query = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });

  return query.toString();
}

function searchQuery(params: Record<string, string | number | undefined> = {}) {
  const query = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });

  return query.toString();
}

export async function fetchCustomers(params: CustomerListParams = {}) {
  const query = customerQuery(params);
  const data = await apiFetch<CustomerListResponse>(
    `/api/customers${query ? `?${query}` : ""}`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load customers");
  }

  return {
    customers: data.customers || [],
    total: data.total || 0
  };
}

export async function fetchCustomerStats() {
  const data = await apiFetch<CustomerStatsResponse>("/api/customers/stats");

  if (!data.success || !data.stats) {
    throw new Error(data.error || "Failed to load customer stats");
  }

  return data.stats;
}

export async function fetchCustomerPipelineStats() {
  const data = await apiFetch<CustomerPipelineStatsResponse>(
    "/api/customers/pipeline-stats"
  );

  if (!data.success || !data.stats) {
    throw new Error(data.error || "Failed to load customer pipeline stats");
  }

  return data.stats;
}

export async function fetchCustomer(id: string | number) {
  const data = await apiFetch<CustomerDetailResponse>(`/api/customers/${id}`);

  if (!data.success || !data.customer) {
    throw new Error(data.error || "Failed to load customer");
  }

  return data.customer;
}

export async function fetchCustomerProperties(id: string | number) {
  const data = await apiFetch<CustomerRelatedResponse<CustomerProperty>>(
    `/api/customers/${id}/properties`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load customer properties");
  }

  return data.properties || [];
}

export async function fetchCustomerJobs(id: string | number) {
  const data = await apiFetch<CustomerRelatedResponse<CustomerJob>>(
    `/api/customers/${id}/jobs`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load customer jobs");
  }

  return data.jobs || [];
}

export async function fetchCustomerQuotes(id: string | number) {
  const data = await apiFetch<CustomerRelatedResponse<CustomerQuote>>(
    `/api/customers/${id}/quotes`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load customer quotes");
  }

  return data.quotes || [];
}

export async function fetchCustomerInvoices(id: string | number) {
  const data = await apiFetch<CustomerRelatedResponse<CustomerInvoice>>(
    `/api/customers/${id}/invoices`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load customer invoices");
  }

  return data.invoices || [];
}

export type { Customer };

export async function fetchInvoices(params: InvoiceListParams = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<InvoiceListResponse>(
    `/api/invoices${query ? `?${query}` : ""}`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load invoices");
  }

  return data.invoices || [];
}

export async function fetchInvoiceStats() {
  const data = await apiFetch<InvoiceStatsResponse>("/api/invoices/stats");

  if (!data.success || !data.stats) {
    throw new Error(data.error || "Failed to load invoice stats");
  }

  return data.stats;
}

export async function fetchInvoice(id: string | number) {
  const data = await apiFetch<InvoiceDetailResponse>(`/api/invoices/${id}`);

  if (!data.success || !data.invoice) {
    throw new Error(data.error || "Failed to load invoice");
  }

  return data.invoice;
}

export async function fetchJobs(params: JobListParams = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<JobListResponse>(
    `/api/jobs${query ? `?${query}` : ""}`
  );

  if (!data.success) {
    throw new Error(data.error || "Failed to load jobs");
  }

  return data.jobs || [];
}

export async function fetchJobStats(params: Pick<JobListParams, "date"> = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<JobStatsResponse>(
    `/api/jobs/stats${query ? `?${query}` : ""}`
  );

  if (!data.success || !data.stats) {
    throw new Error(data.error || "Failed to load job stats");
  }

  return data.stats;
}

export async function fetchJob(id: string | number) {
  const data = await apiFetch<JobDetailResponse>(`/api/jobs/${id}`);

  if (!data.success || !data.job) {
    throw new Error(data.error || "Failed to load job");
  }

  return data.job;
}
