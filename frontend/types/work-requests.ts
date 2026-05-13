export type WorkRequest = {
  id?: string | number;
  external_source?: string | null;
  customer_name?: string | null;
  customer_phone?: string | null;
  customer_email?: string | null;
  customer_address?: string | null;
  preferred_work_date?: string | null;
  preferred_work_date_raw?: string | null;
  work_requested?: string | null;
  source?: string | null;
  customer_path?: string | null;
};

export type WorkRequestsResponse = {
  success: boolean;
  source?: string | null;
  as_of?: string | null;
  mode?: string | null;
  requests?: WorkRequest[];
  total?: number;
  stats?: Record<string, unknown>;
  error?: string;
};
