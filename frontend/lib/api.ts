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
  CrewAvailabilityResponse,
  JobDetailResponse,
  JobListParams,
  JobListResponse,
  JobsPipelineResponse,
  JobStatsResponse,
  LiveJobsResponse
} from "../types/jobs";
import type {
  ActivityFeedResponse,
  CompletedUninvoicedJobsResponse,
  FinanceSummaryResponse,
  JobsDashboardResponse,
  TodaySummaryResponse
} from "../types/dashboard";
import type {
  BusinessSummaryResponse,
  CashFlowForecastResponse,
  CrewPerformanceResponse,
  CustomerAcquisitionResponse,
  CustomerValueRow,
  InvoiceAgingResponse,
  JobCostingRow,
  KpiDashboardResponse,
  SalesTaxReportResponse,
  TaxSweepReportResponse
} from "../types/reports";
import type {
  AiCampaignSegmentsResponse,
  AiChurnRiskResponse,
  AiDraftResponse,
  AiLeadScoresResponse,
  AiRevenueForecastResponse,
  AiScheduleSuggestionsResponse
} from "../types/ai";
import type {
  PaymentRecordsResponse,
  PaymentReviewResponse,
  PaymentsResponse,
  TaxTransferFreshnessResponse,
  TaxTransferInstructionResponse
} from "../types/payments";
import type {
  CallsResponse,
  MessagesResponse,
  VoicemailsResponse
} from "../types/communications";

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

function searchQuery(
  params: Record<string, string | number | null | undefined> = {}
) {
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

export async function fetchPayments(params: Record<string, string | number | undefined> = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<PaymentsResponse>(`/api/payments${query ? `?${query}` : ""}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load payments");
  }
  return data;
}

export async function fetchPaymentRecords(params: Record<string, string | number | undefined> = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<PaymentRecordsResponse>(
    `/api/payment-records${query ? `?${query}` : ""}`
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load payment records");
  }
  return data;
}

export async function fetchPaymentReview(params: {
  start_date: string;
  end_date: string;
  unresolved_only?: string;
}) {
  const query = searchQuery(params);
  const data = await apiFetch<PaymentReviewResponse>(`/api/copilot/payment-review?${query}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load payment review");
  }
  return data;
}

export async function fetchTaxTransferFreshnessStatus() {
  const data = await apiFetch<TaxTransferFreshnessResponse>(
    "/api/reports/tax-transfer-freshness-status"
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load tax transfer freshness");
  }
  return data;
}

export async function fetchTaxTransferInstructions(params: Record<string, string | number | undefined> = {}) {
  const query = searchQuery(params);
  const data = await apiFetch<TaxTransferInstructionResponse>(
    `/api/tax-transfer-instructions${query ? `?${query}` : ""}`
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load tax transfer instructions");
  }
  return data;
}

export async function fetchMessages(limit = 100) {
  const data = await apiFetch<MessagesResponse>(`/api/messages?limit=${limit}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load messages");
  }
  return data.messages || [];
}

export async function fetchCalls(limit = 100) {
  const data = await apiFetch<CallsResponse>(`/api/calls?limit=${limit}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load calls");
  }
  return data.calls || [];
}

export async function fetchVoicemails() {
  const data = await apiFetch<VoicemailsResponse>("/api/app/voicemails");
  if (data.success === false) {
    throw new Error(data.error || "Failed to load voicemails");
  }
  return data.voicemails || [];
}

export async function fetchCrewAvailability(date: string) {
  const query = searchQuery({ date });
  const data = await apiFetch<CrewAvailabilityResponse>(`/api/dispatch/crew-availability?${query}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load crew availability");
  }
  return data.crews || [];
}

export async function fetchJobsPipeline() {
  const data = await apiFetch<JobsPipelineResponse>("/api/jobs/pipeline");
  if (!data.success) {
    throw new Error(data.error || "Failed to load jobs pipeline");
  }
  return data;
}

export async function fetchCopilotLiveJobs(date: string) {
  const query = searchQuery({ date });
  const data = await apiFetch<LiveJobsResponse>(`/api/copilot/live-jobs?${query}`);
  if (!data.success) {
    throw new Error(data.error || "Failed to load live jobs");
  }
  return data.jobs || [];
}

export async function fetchTodaySummary() {
  const data = await apiFetch<TodaySummaryResponse>("/api/dashboard/today-summary");
  if (!data.success) {
    throw new Error(data.error || "Failed to load today's summary");
  }
  return data;
}

export async function fetchActivityFeed() {
  const data = await apiFetch<ActivityFeedResponse>("/api/dashboard/activity-feed");
  if (!data.success) {
    throw new Error(data.error || "Failed to load activity feed");
  }
  return data.events || [];
}

export async function fetchJobsDashboard() {
  const data = await apiFetch<JobsDashboardResponse>("/api/jobs/dashboard");
  if (!data.success) {
    throw new Error(data.error || "Failed to load jobs dashboard");
  }

  return data.dashboard || {
    stats: data.stats,
    upcoming: data.upcoming || []
  };
}

export async function fetchCompletedUninvoicedJobs() {
  const data = await apiFetch<CompletedUninvoicedJobsResponse>(
    "/api/jobs/completed-uninvoiced"
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load completed uninvoiced jobs");
  }
  return data.jobs || [];
}

export async function fetchFinanceSummary() {
  const data = await apiFetch<FinanceSummaryResponse>("/api/finance/summary");
  if (!data.success) {
    throw new Error(data.error || "Failed to load finance summary");
  }
  return data.summary || data;
}

export async function fetchBusinessSummary() {
  const data = await apiFetch<BusinessSummaryResponse>("/api/reports/business-summary");
  if (!data.success || !data.summary) {
    throw new Error(data.error || "Failed to load business summary");
  }
  return data.summary;
}

export async function fetchKpiDashboard() {
  const data = await apiFetch<KpiDashboardResponse>("/api/kpi/dashboard");
  if (!data.success) {
    throw new Error(data.error || "Failed to load KPI dashboard");
  }
  return data;
}

export async function fetchCashFlowForecast() {
  const data = await apiFetch<CashFlowForecastResponse>(
    "/api/finance/cash-flow-forecast"
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load cash-flow forecast");
  }
  return data;
}

export async function fetchJobCostingReport() {
  const data = await apiFetch<JobCostingRow[] | { success?: boolean; rows?: JobCostingRow[]; error?: string }>(
    "/api/reports/job-costing"
  );
  if (Array.isArray(data)) return data;
  if (data.success === false) {
    throw new Error(data.error || "Failed to load job costing");
  }
  return data.rows || [];
}

export async function fetchCustomerValueReport() {
  const data = await apiFetch<CustomerValueRow[] | { success?: boolean; rows?: CustomerValueRow[]; error?: string }>(
    "/api/reports/customer-value"
  );
  if (Array.isArray(data)) return data;
  if (data.success === false) {
    throw new Error(data.error || "Failed to load customer value");
  }
  return data.rows || [];
}

export async function fetchCrewPerformanceReport() {
  const data = await apiFetch<CrewPerformanceResponse>("/api/reports/crew-performance");
  if (!data.success) {
    throw new Error(data.error || "Failed to load crew performance");
  }
  return data.crews || [];
}

export async function fetchCustomerAcquisitionReport() {
  const data = await apiFetch<CustomerAcquisitionResponse>(
    "/api/reports/customer-acquisition"
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load customer acquisition");
  }
  return data.months || [];
}

export async function fetchSalesTaxReport(params: {
  start_date: string;
  end_date: string;
  type?: string;
}) {
  const query = searchQuery({ type: "all", ...params });
  const data = await apiFetch<SalesTaxReportResponse>(
    `/api/reports/sales-tax?${query}`
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load sales tax report");
  }
  return data;
}

export async function fetchTaxSweepReport(params: {
  start_date: string;
  end_date: string;
}) {
  const query = searchQuery(params);
  const data = await apiFetch<TaxSweepReportResponse>(
    `/api/reports/tax-sweep?${query}`
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load tax sweep report");
  }
  return data;
}

export async function fetchInvoiceAging() {
  const data = await apiFetch<InvoiceAgingResponse>("/api/invoices/aging");
  if (!data.success) {
    throw new Error(data.error || "Failed to load invoice aging");
  }
  return data;
}

export async function fetchAiLeadScores() {
  const data = await apiFetch<AiLeadScoresResponse>("/api/ai/lead-scores");
  if (!data.success) {
    throw new Error(data.error || "Failed to load lead scores");
  }
  return data.customers || [];
}

export async function fetchAiChurnRisk() {
  const data = await apiFetch<AiChurnRiskResponse>("/api/ai/churn-risk");
  if (!data.success) {
    throw new Error(data.error || "Failed to load churn risk");
  }
  return data.customers || [];
}

export async function fetchAiRevenueForecast() {
  const data = await apiFetch<AiRevenueForecastResponse>("/api/ai/revenue-forecast");
  if (!data.success) {
    throw new Error(data.error || "Failed to load revenue forecast");
  }
  return data.forecast || [];
}

export async function fetchAiCampaignSegments() {
  const data = await apiFetch<AiCampaignSegmentsResponse>("/api/ai/campaign-segments");
  if (!data.success) {
    throw new Error(data.error || "Failed to load campaign segments");
  }
  return data.segments || [];
}

export async function fetchAiScheduleSuggestions(date: string) {
  const query = searchQuery({ date });
  const data = await apiFetch<AiScheduleSuggestionsResponse>(
    `/api/ai/schedule-suggestions?${query}`
  );
  if (!data.success) {
    throw new Error(data.error || "Failed to load schedule suggestions");
  }
  return data.suggestions || [];
}

export async function generateAiQuote(payload: Record<string, unknown>) {
  return apiFetch<AiDraftResponse>("/api/ai/generate-quote", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function generateAiFollowup(payload: Record<string, unknown>) {
  return apiFetch<AiDraftResponse>("/api/ai/generate-followup", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function generateAiTemplate(payload: Record<string, unknown>) {
  return apiFetch<AiDraftResponse>("/api/ai/generate-template", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function chatWithAi(payload: Record<string, unknown>) {
  return apiFetch<AiDraftResponse>("/api/ai/chat", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function suggestAiService(payload: Record<string, unknown>) {
  return apiFetch<AiDraftResponse>("/api/ai/suggest-service", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}
