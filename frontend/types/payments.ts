export type PaymentRow = {
  id?: number;
  invoice_id?: number | null;
  customer_id?: number | null;
  customer_name?: string | null;
  customer_email?: string | null;
  invoice_number?: string | null;
  display_invoice_number?: string | null;
  amount?: number | string | null;
  amount_paid?: number | string | null;
  tip_amount?: number | string | null;
  method?: string | null;
  status?: string | null;
  external_source?: string | null;
  source_date_raw?: string | null;
  paid_at?: string | null;
  payment_date?: string | null;
  created_at?: string | null;
  imported_at?: string | null;
  link_status?: string | null;
  link_reason?: string | null;
  current_invoice_match_id?: number | null;
  current_invoice_match_number?: string | null;
};

export type PaymentsResponse = {
  success: boolean;
  payments?: PaymentRow[];
  total?: number;
  totalReceived?: number | string;
  monthly?: Array<Record<string, unknown>>;
  error?: string;
};

export type PaymentRecordsResponse = {
  success: boolean;
  payments?: PaymentRow[];
  total?: number;
  error?: string;
};

export type PaymentReviewResponse = {
  success: boolean;
  summary?: {
    total_rows?: number;
    linked_count?: number;
    unresolved_count?: number;
  };
  payments?: PaymentRow[];
  total?: number;
  error?: string;
};

export type TaxTransferFreshnessResponse = {
  success: boolean;
  status?: Record<string, unknown>;
  error?: string;
} & Record<string, unknown>;

export type TaxTransferInstructionResponse = {
  success: boolean;
  instructions?: Array<Record<string, unknown>>;
  transfers?: Array<Record<string, unknown>>;
  error?: string;
} & Record<string, unknown>;
