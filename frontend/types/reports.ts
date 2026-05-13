export type BusinessSummary = {
  period?: string;
  start?: string;
  quotesSent?: number;
  quotesSigned?: number;
  conversionRate?: number | string;
  invoicesTotal?: number | string;
  revenue?: number | string;
  outstanding?: number | string;
  jobsTotal?: number;
  jobsCompleted?: number;
  expenses?: number | string;
  profit?: number | string;
  newCustomers?: number;
};

export type BusinessSummaryResponse = {
  success: boolean;
  summary?: BusinessSummary;
  error?: string;
};

export type KpiDashboardResponse = {
  success: boolean;
  metrics?: Record<string, unknown>;
  suggestions?: unknown[];
  error?: string;
} & Record<string, unknown>;

export type CashFlowForecastResponse = {
  success: boolean;
  forecast?: Array<Record<string, unknown>>;
  total_expected_inflow?: number | string;
  monthly_expense_avg?: number | string;
  expense_categories?: Array<Record<string, unknown>>;
  error?: string;
};

export type JobCostingRow = {
  id?: number;
  customer_name?: string | null;
  service_type?: string | null;
  revenue?: number | string | null;
  expenses?: number | string | null;
  profit?: number | string | null;
};

export type CustomerValueRow = {
  id?: number;
  name?: string | null;
  email?: string | null;
  total_invoiced?: number | string | null;
  invoice_count?: number | string | null;
  last_invoice_date?: string | null;
};

export type CrewPerformanceRow = {
  crew?: string | null;
  jobs_total?: number | string | null;
  jobs_completed?: number | string | null;
  total_revenue?: number | string | null;
};

export type CrewPerformanceResponse = {
  success: boolean;
  crews?: CrewPerformanceRow[];
  error?: string;
};

export type CustomerAcquisitionResponse = {
  success: boolean;
  months?: Array<Record<string, unknown>>;
  error?: string;
};

export type SalesTaxReportResponse = {
  success: boolean;
  summary?: Record<string, unknown>;
  rows?: Array<Record<string, unknown>>;
  report?: Record<string, unknown>;
  error?: string;
} & Record<string, unknown>;

export type TaxSweepReportResponse = SalesTaxReportResponse;

export type InvoiceAgingResponse = {
  success: boolean;
  source?: string;
  as_of?: string;
  buckets?: Array<Record<string, unknown>> | Record<string, unknown>;
  error?: string;
};
