export type AiLeadScore = {
  id?: number;
  name?: string | null;
  score?: number | string | null;
  grade?: string | null;
  factors?: string[] | Record<string, unknown> | null;
};

export type AiLeadScoresResponse = {
  success: boolean;
  customers?: AiLeadScore[];
  error?: string;
};

export type AiChurnRisk = {
  id?: number;
  name?: string | null;
  risk_level?: string | null;
  risk_score?: number | string | null;
  factors?: string[] | Record<string, unknown> | null;
};

export type AiChurnRiskResponse = {
  success: boolean;
  customers?: AiChurnRisk[];
  error?: string;
};

export type AiRevenueForecastResponse = {
  success: boolean;
  forecast?: Array<Record<string, unknown>>;
  error?: string;
};

export type AiCampaignSegmentsResponse = {
  success: boolean;
  segments?: Array<Record<string, unknown>> | Record<string, unknown>;
  error?: string;
};

export type AiScheduleSuggestionsResponse = {
  success: boolean;
  suggestions?: Array<Record<string, unknown>>;
  error?: string;
};

export type AiDraftResponse = {
  success: boolean;
  draft?: string;
  text?: string;
  message?: string;
  response?: string;
  error?: string;
} & Record<string, unknown>;
