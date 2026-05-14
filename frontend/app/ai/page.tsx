"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchCopilotLiveJobs,
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

    return () => {
      active = false;
    };
  }, []);

  const requests = workRequests.data?.requests || [];
  const draftCandidates = useMemo(
    () => requests.filter((request) => request.customer_name || request.work_requested).slice(0, 6),
    [requests]
  );

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
          <p className="eyebrow">Assistant</p>
          <h1>Assistant</h1>
          <p className="muted">
            Draft and recommendation workspace for landscaping follow-ups. It prepares text only; it never sends, charges, deletes, or updates records automatically.
          </p>
        </div>
      </header>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel
          title="Follow-up draft candidates"
          helper="Live work requests that may need a response. Use one as context for a manual draft."
          state={workRequests}
        >
          {() => <WorkRequestCards rows={draftCandidates} />}
        </SuggestionPanel>

        <SuggestionPanel
          title="Schedule recommendations"
          helper="Live route context for possible customer updates, delay notices, or crew questions."
          state={liveJobs}
        >
          {(rows) => <LiveJobCards rows={rows.slice(0, 8)} />}
        </SuggestionPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="AI scope">
          <div className="table-toolbar">
            <div>
              <h2>Assistant Guardrails</h2>
              <p>Recommendations are read-only. Drafts require manual review before any customer action.</p>
            </div>
          </div>
          <div className="compact-list">
            <div className="compact-row">
              <div>
                <strong>Generative AI drafts</strong>
                <span>Creates follow-up wording from the context you provide. You review before anything is sent.</span>
              </div>
            </div>
            <div className="compact-row">
              <div>
                <strong>Live context panels</strong>
                <span>Work requests and route activity come from existing backend APIs and are shown as context, not automated decisions.</span>
              </div>
            </div>
            <div className="compact-row">
              <div>
                <strong>No automatic actions</strong>
                <span>No emails, texts, payment actions, tax transfers, or job changes run without a manual click and review.</span>
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

function formatDate(value: unknown) {
  const date = new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) return String(value || "");
  return date.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}
