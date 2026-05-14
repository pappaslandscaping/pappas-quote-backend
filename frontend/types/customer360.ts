import type { Customer, CustomerInvoice, CustomerJob, CustomerQuote } from "./customers";

export type Customer360SourceStatus = "live" | "empty" | "loading" | "error";

export type Customer360Source = {
  status: Customer360SourceStatus;
  source: string;
  error?: string | null;
};

export type Customer360TimelineItem = {
  id: string;
  record_id?: string | number | null;
  type: "quote" | "job" | "invoice" | "payment" | "communication" | "note";
  title: string;
  detail?: string | null;
  status?: string | null;
  date?: string | null;
  amount?: number | null;
  href?: string | null;
  source?: string | null;
};

export type Customer360Payment = {
  id: number;
  invoice_id?: number | null;
  invoice_number?: string | null;
  amount?: string | number | null;
  method?: string | null;
  status?: string | null;
  paid_at?: string | null;
  created_at?: string | null;
};

export type Customer360Communication = {
  id: string | number;
  record_type?: "message" | "call" | string | null;
  direction?: string | null;
  body?: string | null;
  transcription?: string | null;
  status?: string | null;
  read?: boolean | null;
  created_at?: string | null;
};

export type Customer360Note = {
  id: string | number;
  author_name?: string | null;
  content?: string | null;
  pinned?: boolean | null;
  created_at?: string | null;
};

export type Customer360Summary = {
  quote_count: number;
  signed_quote_count: number;
  job_count: number;
  completed_job_count: number;
  invoice_count: number;
  open_invoice_balance: number;
  payment_count: number;
  communication_count: number;
  note_count: number;
};

export type Customer360 = {
  customer: Customer;
  summary: Customer360Summary;
  sources: Record<string, Customer360Source>;
  records: {
    quotes: CustomerQuote[];
    jobs: CustomerJob[];
    invoices: CustomerInvoice[];
    payments: Customer360Payment[];
    communications: Customer360Communication[];
    notes: Customer360Note[];
  };
  timeline: Customer360TimelineItem[];
  ai: {
    mode: "draft_only";
    allowed_actions: string[];
    blocked_actions: string[];
  };
};

export type Customer360Response = {
  success: boolean;
  customer360?: Customer360;
  error?: string;
};
