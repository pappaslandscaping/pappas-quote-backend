export type InvoiceLineItem = {
  name?: string | null;
  description?: string | null;
  service_date?: string | null;
  quantity?: string | number | null;
  qty?: string | number | null;
  rate?: string | number | null;
  unit_price?: string | number | null;
  amount?: string | number | null;
  line_total?: string | number | null;
  property_name?: string | null;
};

export type InvoicePayment = {
  id: number;
  amount?: string | number | null;
  method?: string | null;
  status?: string | null;
  card_brand?: string | null;
  card_last4?: string | null;
  square_receipt_url?: string | null;
  notes?: string | null;
  paid_at?: string | null;
  created_at?: string | null;
};

export type InvoiceHistoryEvent = {
  type?: string | null;
  badge?: string | null;
  title?: string | null;
  detail?: string | null;
  date?: string | null;
};

export type Invoice = {
  id: number;
  invoice_number?: string | null;
  display_invoice_number?: string | null;
  customer_id?: number | null;
  customer_name?: string | null;
  customer_email?: string | null;
  customer_address?: string | null;
  status?: string | null;
  sent_status?: string | null;
  subtotal?: string | number | null;
  tax_rate?: string | number | null;
  tax_amount?: string | number | null;
  total?: string | number | null;
  amount_paid?: string | number | null;
  late_fee_total?: string | number | null;
  due_date?: string | null;
  paid_at?: string | null;
  sent_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  notes?: string | null;
  terms?: string | null;
  line_items?: InvoiceLineItem[] | string | null;
  payment_token?: string | null;
  mail_state?: string | null;
  mailed_at?: string | null;
  qb_invoice_id?: string | null;
  external_source?: string | null;
  external_metadata?: Record<string, unknown> | null;
  payment_history?: InvoicePayment[];
  history_events?: InvoiceHistoryEvent[];
};

export type InvoiceStats = {
  total: number;
  draft: number;
  pending: number;
  partial: number;
  sent: number;
  paid: number;
  overdue: number;
  void: number;
  outstanding: number;
  overdueAmount: number;
  paidThisMonth: number;
  totalRevenue: number;
  account_standing?: {
    past_due?: string | number;
    outstanding?: string | number;
    credit?: string | number;
    paid?: string | number;
    source?: string;
  };
};

export type InvoiceListParams = {
  status?: string;
  search?: string;
  mailed?: string;
  limit?: number;
  offset?: number;
};

export type InvoiceListResponse = {
  success: boolean;
  invoices?: Invoice[];
  error?: string;
};

export type InvoiceStatsResponse = {
  success: boolean;
  stats?: InvoiceStats;
  error?: string;
};

export type InvoiceDetailResponse = {
  success: boolean;
  invoice?: Invoice;
  error?: string;
};
