export type Job = {
  id: number;
  job_date?: string | null;
  customer_name?: string | null;
  customer_id?: number | null;
  service_type?: string | null;
  service_frequency?: string | null;
  service_price?: string | number | null;
  address?: string | null;
  phone?: string | null;
  special_notes?: string | null;
  property_notes?: string | null;
  status?: string | null;
  route_order?: number | null;
  estimated_duration?: number | string | null;
  crew_assigned?: string | null;
  completed_at?: string | null;
  completed_by?: string | null;
  pipeline_stage?: string | null;
  is_recurring?: boolean | null;
  recurring_pattern?: string | null;
  recurring_day_of_week?: string | number | null;
  recurring_start_date?: string | null;
  recurring_end_date?: string | null;
  invoice_id?: number | null;
  property_id?: number | null;
  latitude?: number | string | null;
  longitude?: number | string | null;
  has_street_address?: boolean;
  geocode_address?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

export type JobStats = {
  total: number;
  byStatus: Record<string, number>;
  totalRevenue: number;
  byCrew: Record<string, number>;
};

export type JobListParams = {
  date?: string;
  status?: string;
  crew?: string;
  start_date?: string;
  end_date?: string;
  search?: string;
  limit?: number;
};

export type JobListResponse = {
  success: boolean;
  jobs?: Job[];
  error?: string;
};

export type JobStatsResponse = {
  success: boolean;
  stats?: JobStats;
  error?: string;
};

export type JobDetailResponse = {
  success: boolean;
  job?: Job;
  error?: string;
};

export type CrewAvailabilityResponse = {
  success: boolean;
  date?: string;
  crews?: Array<{
    crew_name?: string | null;
    job_count?: string | number | null;
    total_hours?: string | number | null;
  }>;
  error?: string;
};

export type JobsPipelineResponse = {
  success: boolean;
  stages?: Record<string, Job[]>;
  jobs?: Job[];
  error?: string;
} & Record<string, unknown>;

export type LiveJobsResponse = {
  success: boolean;
  jobs?: Job[];
  error?: string;
} & Record<string, unknown>;
