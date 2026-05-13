import type { Job } from "./jobs";

export type TodaySummary = {
  jobs_today?: number;
  revenue_today?: number | string;
  pending_quotes?: number;
  overdue_invoices?: number;
  unread_messages?: number;
};

export type TodaySummaryResponse = {
  success: boolean;
  error?: string;
} & TodaySummary;

export type ActivityEvent = {
  id?: string | number;
  type?: string | null;
  description?: string | null;
  timestamp?: string | null;
  link?: string | null;
};

export type ActivityFeedResponse = {
  success: boolean;
  events?: ActivityEvent[];
  error?: string;
};

export type JobsDashboard = {
  stats?: {
    today?: number;
    thisWeek?: number;
    pending?: number;
  };
  upcoming?: Job[];
};

export type JobsDashboardResponse = {
  success: boolean;
  dashboard?: JobsDashboard;
  stats?: JobsDashboard["stats"];
  upcoming?: Job[];
  error?: string;
};

export type CompletedUninvoicedJobsResponse = {
  success: boolean;
  jobs?: Job[];
  error?: string;
};

export type FinanceSummary = Record<string, unknown>;

export type FinanceSummaryResponse = {
  success: boolean;
  summary?: FinanceSummary;
  error?: string;
} & FinanceSummary;
