export type Customer = {
  id: number;
  customer_number?: string | null;
  name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  email?: string | null;
  phone?: string | null;
  mobile?: string | null;
  street?: string | null;
  street2?: string | null;
  city?: string | null;
  state?: string | null;
  postal_code?: string | null;
  country?: string | null;
  status?: string | null;
  customer_type?: string | null;
  customer_company_name?: string | null;
  tags?: string | null;
  notes?: string | null;
  tax_exempt?: boolean | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type CustomerStats = {
  total: number;
  active: number;
  inactive: number;
  topCities?: Array<{ city: string | null; count: string | number }>;
  trend?: {
    recent: number;
    previous: number;
    pct: number;
  };
};

export type CustomerPipelineStats = {
  totalLeads: number;
  totalCustomers: number;
  newLeadsThisMonth: number;
  convertedThisMonth: number;
  conversionRate: number;
};

export type CustomerListParams = {
  status?: string;
  city?: string;
  search?: string;
  sort?: string;
  type?: string;
  limit?: number;
  offset?: number;
};

export type CustomerListResponse = {
  success: boolean;
  customers?: Customer[];
  total?: number;
  error?: string;
};

export type CustomerStatsResponse = {
  success: boolean;
  stats?: CustomerStats;
  error?: string;
};

export type CustomerPipelineStatsResponse = {
  success: boolean;
  stats?: CustomerPipelineStats;
  error?: string;
};

export type CustomerDetailResponse = {
  success: boolean;
  customer?: Customer;
  error?: string;
};

export type CustomerProperty = {
  id: number;
  property_name?: string | null;
  street?: string | null;
  city?: string | null;
  state?: string | null;
  zip?: string | null;
  status?: string | null;
  lot_size?: string | number | null;
};

export type CustomerJob = {
  id: number;
  job_date?: string | null;
  scheduled_date?: string | null;
  service_type?: string | null;
  service_price?: string | number | null;
  address?: string | null;
  status?: string | null;
  crew_assigned?: string | null;
};

export type CustomerQuote = {
  id: number;
  quote_number?: string | null;
  services?: string[] | string | null;
  total?: string | number | null;
  status?: string | null;
  created_at?: string | null;
};

export type CustomerInvoice = {
  id: number;
  invoice_number?: string | null;
  total?: string | number | null;
  status?: string | null;
  due_date?: string | null;
  created_at?: string | null;
};

export type CustomerRelatedResponse<T> = {
  success: boolean;
  properties?: T[];
  jobs?: T[];
  quotes?: T[];
  invoices?: T[];
  error?: string;
};
