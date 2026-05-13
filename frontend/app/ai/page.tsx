"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchCopilotLiveJobs,
  fetchFinanceSummary,
  fetchWorkRequests,
  generateAiFollowup
} from "../../lib/api";
import type { Job } from "../../types/jobs";
import type { WorkRequest, WorkRequestsResponse } from "../../types/work-requests";

type LoadStatus = "loading" | "success" | "empty" | "error";
type LoadState<T> = { status: LoadStatus; data?: T; error?: string };

const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function today() {
  return new Date().toISOString().slice(0, 10);
}

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

export default function AiPage() {
  const [workRequests, setWorkRequests] = useState<LoadState<WorkRequestsResponse>>(loading());
  const [liveJobs, setLiveJobs] = useState<LoadState<Job[]>>(loading());
  const [finance, setFinance] = useState<LoadState<Record<string, unknown>>>(loading());
  const [draftPrompt, setDraftPrompt] = useState("");
  const [draft, setDraft] = useState<LoadState<string> | null>(null);

  useEffect(() => {
    let active = true;
    const date = today();

    async function load<T>(
      request: () => Promise<T>,
      setter: (state: LoadState<T>) => void,
      empty: (data: T) => boolean = () => false
    ) {
      try {
        const data = await request();
        if (!active) return;
        setter(empty(data) ? { status: "empty", data } : { status: "success", data });
      } catch (error) {
        if (!active) return;
        setter({ status: "error", error: shortError(error) });
      }
    }

    void load(() => fetchWorkRequests({ limit: 25 }), setWorkRequests, (data) => (data.requests || []).length === 0);
    void load(() => fetchCopilotLiveJobs(date), setLiveJobs, (data) => data.length === 0);
    void load(fetchFinanceSummary, setFinance, () => false);

    return () => {
      active = false;
    };
  }, []);

  const requests = workRequests.data?.requests || [];
  const requestSources = useMemo(() => buildSourceSegments(requests), [requests]);

  async function prepareFollowup() {
    setDraft({ status: "loading" });
    try {
      const response = await generateAiFollowup({
        context: draftPrompt || "Draft a polite follow-up for a CopilotCRM work request."
      });
      const text =
        response.draft ||
        response.text ||
        response.message ||
        response.response ||
        "Draft prepared. Review before sending.";
      setDraft({ status: "success", data: text });
    } catch (error) {
      setDraft({ status: "error", error: shortError(error) });
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">AI</p>
          <h1>CopilotCRM Assistant</h1>
          <p className="muted">
            Live CopilotCRM signals and prepared drafts only. Nothing sends automatically.
          </p>
        </div>
      </header>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel
          title="Live Copilot work requests"
          helper={sourceHelper(workRequests.data)}
          state={workRequests}
        >
          {(data) => <WorkRequestCards rows={(data.requests || []).slice(0, 8)} />}
        </SuggestionPanel>

        <SuggestionPanel
          title="Live Copilot route"
          helper="Pulled from CopilotCRM live jobs for today."
          state={liveJobs}
        >
          {(rows) => <LiveJobCards rows={rows.slice(0, 8)} />}
        </SuggestionPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel
          title="Collected revenue"
          helper="Uses CopilotCRM collected revenue when the backend has a live or persisted Copilot snapshot."
          state={finance}
        >
          {(data) => <RevenueSignal data={data} />}
        </SuggestionPanel>

        <SuggestionPanel
          title="Copilot request sources"
          helper="Live audience signals from CopilotCRM work request sources."
          state={workRequests}
        >
          {() => <SimpleCards rows={requestSources} />}
        </SuggestionPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Not live yet">
          <div className="table-toolbar">
            <div>
              <h2>Not live yet</h2>
              <p>These old local AI stats are hidden until we have real CopilotCRM endpoints for them.</p>
            </div>
          </div>
          <div className="compact-list">
            <div className="compact-row">
              <div>
                <strong>Churn risk</strong>
                <span>Needs live Copilot service history and customer activity before it should be trusted.</span>
              </div>
            </div>
            <div className="compact-row">
              <div>
                <strong>Revenue forecast</strong>
                <span>Use collected revenue and live route/work request data for now. Forecasts should not be shown as hard stats.</span>
              </div>
            </div>
          </div>
        </section>

        <section className="table-card dashboard-panel" aria-label="Prepared Actions">
          <div className="table-toolbar">
            <div>
              <h2>Prepare Actions</h2>
              <p>Draft text for review. Sending stays manual.</p>
            </div>
          </div>
          <div className="form-panel">
            <label>
              Draft context
              <textarea
                placeholder="Example: Follow up with a CopilotCRM work request about a patio quote."
                value={draftPrompt}
                onChange={(event) => setDraftPrompt(event.target.value)}
              />
            </label>
            <button className="btn btn-primary" type="button" onClick={prepareFollowup}>
              Prepare follow-up draft
            </button>
            {draft?.status === "loading" ? <div className="state-block">Loading</div> : null}
            {draft?.status === "error" ? (
              <div className="state-block error">API failed: {draft.error}</div>
            ) : null}
            {draft?.status === "success" ? (
              <div className="draft-preview">
                <strong>Draft preview</strong>
                <p>{draft.data}</p>
                <small>No message was sent.</small>
              </div>
            ) : null}
          </div>
        </section>
      </section>
    </main>
  );
}

function SuggestionPanel<T>({
  title,
  helper,
  state,
  children
}: {
  title: string;
  helper: string;
  state: LoadState<T>;
  children: (data: T) => ReactNode;
}) {
  return (
    <section className="table-card dashboard-panel" aria-label={title}>
      <div className="table-toolbar">
        <div>
          <h2>{title}</h2>
          <p>{helper}</p>
        </div>
      </div>
      {state.status === "loading" ? <div className="state-block">Loading</div> : null}
      {state.status === "error" ? (
        <div className="state-block error">API failed: {state.error}</div>
      ) : null}
      {state.status === "empty" ? <div className="empty-state">Nothing to show.</div> : null}
      {state.status === "success" ? children(state.data as T) : null}
    </section>
  );
}

function WorkRequestCards({ rows }: { rows: WorkRequest[] }) {
  if (!rows.length) return <div className="empty-state">Nothing to show.</div>;
  return (
    <div className="compact-list">
      {rows.map((row) => (
        <div className="compact-row" key={row.id || `${row.customer_name}-${row.customer_phone}`}>
          <div>
            <strong>{row.customer_name || "Work request"}</strong>
            <span>{[row.work_requested, row.customer_address, row.preferred_work_date_raw || row.preferred_work_date].filter(Boolean).join(" - ")}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function LiveJobCards({ rows }: { rows: Job[] }) {
  if (!rows.length) return <div className="empty-state">Nothing to show.</div>;
  return (
    <div className="compact-list">
      {rows.map((row) => (
        <div className="compact-row" key={row.id}>
          <div>
            <strong>{row.customer_name || "Copilot job"}</strong>
            <span>{[row.service_type, row.crew_assigned, row.status].filter(Boolean).join(" - ")}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function RevenueSignal({ data }: { data: Record<string, unknown> }) {
  const month = asRecord(data.thisMonth);
  const source = String(month?.revenue_source || data.revenue_source || "unknown");
  const revenue = month?.revenue ?? data.revenue;
  const asOf = month?.revenue_as_of || data.revenue_as_of;
  return (
    <div className="compact-list">
      <div className="compact-row">
        <div>
          <strong>{money(revenue)}</strong>
          <span>Source: {sourceLabel(source)}{asOf ? ` - as of ${formatDate(asOf)}` : ""}</span>
        </div>
      </div>
    </div>
  );
}

function SimpleCards({ rows }: { rows: Array<Record<string, unknown>> }) {
  if (!rows.length) return <div className="empty-state">Nothing to show.</div>;

  return (
    <div className="compact-list">
      {rows.map((row, index) => (
        <div className="compact-row" key={index}>
          <div>
            <strong>{String(row.name || "Source")}</strong>
            <span>{String(row.count || 0)} work request{Number(row.count || 0) === 1 ? "" : "s"}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function buildSourceSegments(rows: WorkRequest[]) {
  const counts = new Map<string, number>();
  rows.forEach((row) => {
    const source = String(row.source || "Unknown").trim() || "Unknown";
    counts.set(source, (counts.get(source) || 0) + 1);
  });
  return Array.from(counts.entries())
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
}

function sourceHelper(data?: WorkRequestsResponse) {
  if (!data) return "Pulled from CopilotCRM work requests.";
  const source = sourceLabel(data.source || data.mode || "");
  const asOf = data.as_of ? ` - as of ${formatDate(data.as_of)}` : "";
  return `${source}${asOf}.`;
}

function sourceLabel(source: unknown) {
  const value = String(source || "").toLowerCase();
  if (value.includes("live_copilot")) return "Live CopilotCRM";
  if (value.includes("persisted_copilot")) return "Recent CopilotCRM snapshot";
  if (value.includes("copilot")) return "CopilotCRM";
  if (value.includes("database")) return "Database fallback";
  return "Unknown source";
}

function money(value: unknown) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  });
}

function asRecord(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function formatDate(value: unknown) {
  const date = new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) return String(value || "");
  return date.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}
