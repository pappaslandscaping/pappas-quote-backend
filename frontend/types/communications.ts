export type MessageRow = {
  id?: number;
  sid?: string | null;
  direction?: string | null;
  from?: string | null;
  to?: string | null;
  body?: string | null;
  status?: string | null;
  customerName?: string | null;
  timestamp?: string | null;
  read?: boolean | null;
};

export type MessagesResponse = {
  success: boolean;
  messages?: MessageRow[];
  error?: string;
};

export type CallRow = {
  id?: number;
  from_number?: string | null;
  to_number?: string | null;
  customer_name?: string | null;
  status?: string | null;
  option_selected?: string | null;
  recording_url?: string | null;
  transcription?: string | null;
  summary?: string | null;
  created_at?: string | null;
};

export type CallsResponse = {
  success: boolean;
  calls?: CallRow[];
  error?: string;
};

export type VoicemailRow = {
  id?: string | number;
  from?: string | null;
  from_number?: string | null;
  customer_name?: string | null;
  status?: string | null;
  transcript?: string | null;
  transcription?: string | null;
  recording_url?: string | null;
  created_at?: string | null;
  timestamp?: string | null;
};

export type VoicemailsResponse = {
  success?: boolean;
  voicemails?: VoicemailRow[];
  error?: string;
};
