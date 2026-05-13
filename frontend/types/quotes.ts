export type QuoteStatus =
  | "new"
  | "contacted"
  | "quoted"
  | "scheduled"
  | "completed"
  | "cancelled"
  | string;

export type QuoteRequest = {
  id: number;
  name: string | null;
  email: string | null;
  phone: string | null;
  address: string | null;
  package: string | null;
  services: string[] | string | null;
  questions: Record<string, string> | string | null;
  notes: string | null;
  source: string | null;
  status: QuoteStatus;
  created_at: string;
  updated_at?: string | null;
};

export type QuoteListResponse = {
  success: boolean;
  quotes?: QuoteRequest[];
  error?: string;
};

export type QuoteDetailResponse = {
  success: boolean;
  quote?: QuoteRequest;
  error?: string;
};

export type QuoteCreatePayload = {
  name: string;
  phone: string;
  email?: string;
  address?: string;
  package?: string;
  source?: string;
  services?: string;
  notes?: string;
};

export type QuoteCreateResponse = {
  success: boolean;
  quote?: QuoteRequest;
  error?: string;
};

export type QuoteDeleteResponse = {
  success: boolean;
  deleted?: QuoteRequest;
  error?: string;
};

export type LoginPayload = {
  email: string;
  password: string;
};

export type LoginResponse = {
  success: boolean;
  token?: string;
  name?: string;
  email?: string;
  role?: string;
  isAdmin?: boolean;
  isEmployee?: boolean;
  permissions?: unknown;
  error?: string;
};
