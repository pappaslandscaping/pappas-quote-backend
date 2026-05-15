"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchCopilotLiveJobs,
  fetchWorkRequests,
  generateAiTemplate
} from "../../lib/api";
import type { Job } from "../../types/jobs";
import type { WorkRequest, WorkRequestsResponse } from "../../types/work-requests";

type LoadStatus = "loading" | "success" | "empty" | "error";
type LoadState<T> = { status: LoadStatus; data?: T; error?: string };

type AssistantAction = {
  draftTitle: string;
  queryKeys: string[];
  title: string;
  prompt: string;
};

const assistantActions: AssistantAction[] = [
  {
    draftTitle: "Lead follow-up draft",
    queryKeys: ["lead", "lead-followup", "email", "text"],
    title: "Draft follow-up for this lead",
    prompt: "Draft a concise follow-up for a landscaping lead who requested service but has not responded yet. Ask one clear question and offer the next step."
  },
  {
    draftTitle: "Customer summary draft",
    queryKeys: ["customer", "summary", "customer-summary"],
    title: "Summarize this customer",
    prompt: "Summarize this customer history in plain language: what they asked for, what happened next, what is open, and what I should do before contacting them."
  },
  {
    draftTitle: "Rain delay draft",
    queryKeys: ["rain-delay", "weather"],
    title: "Write rain delay text",
    prompt: "Write a short rain delay text for today's landscaping route. Be specific, apologetic, and include that we will reschedule as soon as conditions allow."
  },
  {
    draftTitle: "Payment reminder draft",
    queryKeys: ["payment", "payment-reminder", "invoice-reminder"],
    title: "Write payment reminder",
    prompt: "Write a polite payment reminder for an overdue landscaping invoice. Keep it firm, helpful, and manual-send only."
  },
  {
    draftTitle: "Spring cleanup campaign draft",
    queryKeys: ["spring-cleanup", "campaign"],
    title: "Create spring cleanup campaign",
    prompt: "Draft spring cleanup campaign copy for existing landscaping customers. Include mulch, bed cleanup, pruning, and weed control."
  },
  {
    draftTitle: "Today’s priorities summary",
    queryKeys: ["priorities", "daily-brief"],
    title: "Explain today's priorities",
    prompt: "Explain today's priorities for a landscaping admin: leads needing response, crew route issues, work completed not billed, and overdue money."
  },
  {
    draftTitle: "Mulch / weed control candidate summary",
    queryKeys: ["mulch", "weed-control", "candidates"],
    title: "Find mulch / weed control candidates",
    prompt: "Describe how to identify customers likely to need mulch or weed control based on recent jobs, season, and customer history. Do not change data."
  },
  {
    draftTitle: "Voicemail task draft",
    queryKeys: ["voicemail", "voicemail-task"],
    title: "Turn voicemail into task",
    prompt: "Turn this voicemail into a clear office task with customer, request, urgency, and suggested reply."
  }
];

const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function today() {
  return new Date().toISOString().slice(0, 10);
}

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

function textFromBlocks(blocks: unknown) {
  if (!Array.isArray(blocks)) return "";
  return blocks
    .map((block) => {
      if (!block || typeof block !== "object") return "";
      const item = block as Record<string, unknown>;
      if (Array.isArray(item.content)) return item.content.join("\n");
      return [item.content, item.url].filter(Boolean).join(" ");
    })
    .filter(Boolean)
    .join("\n\n");
}

function readableDraft(response: Record<string, unknown>) {
  const result = response.result;
  const payload =
    result && typeof result === "object" && !Array.isArray(result)
      ? (result as Record<string, unknown>)
      : response;
  const campaign =
    payload.campaign && typeof payload.campaign === "object" && !Array.isArray(payload.campaign)
      ? (payload.campaign as Record<string, unknown>)
      : null;
  const source = campaign || payload;
  const subject = typeof source.subject === "string" ? `Subject: ${source.subject}` : "";
  const sms = typeof source.sms === "string" ? `SMS: ${source.sms}` : "";
  const message = typeof source.message === "string" ? source.message : "";
  const blocks = textFromBlocks(source.blocks);
  const name = campaign && typeof campaign.name === "string" ? campaign.name : "";
  const description = campaign && typeof campaign.description === "string" ? campaign.description : "";
  const text =
    typeof payload.draft === "string" ? payload.draft :
    typeof payload.text === "string" ? payload.text :
    typeof payload.response === "string" ? payload.response :
    "";

  return [name, description, subject, blocks, sms, text, message]
    .filter(Boolean)
    .join("\n\n") || "Draft prepared. Review before sending.";
}

export default function AiPage() {
  const [workRequests, setWorkRequests] = useState<LoadState<WorkRequestsResponse>>(loading());
  const [liveJobs, setLiveJobs] = useState<LoadState<Job[]>>(loading());
  const [draftPrompt, setDraftPrompt] = useState("");
  const [selectedDraftTitle, setSelectedDraftTitle] = useState("Lead follow-up draft");
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

  useEffect(() => {
    const requestedDraft = new URLSearchParams(window.location.search).get("draft");
    if (!requestedDraft) return;
    const action = assistantActions.find((item) => item.queryKeys.includes(requestedDraft));
    if (!action) return;
    setSelectedDraftTitle(action.draftTitle);
    setDraftPrompt(action.prompt);
    setDraft(null);
  }, []);

  const requests = workRequests.data?.requests || [];
  const draftCandidates = useMemo(
    () => requests.filter((request) => request.customer_name || request.work_requested).slice(0, 6),
    [requests]
  );

  async function prepareFollowup() {
    const prompt = draftPrompt || "Draft a polite follow-up for a CopilotCRM work request.";
    setDraft({ status: "loading" });
    try {
      const response = await generateAiTemplate({
        type: "assistant_draft",
        action: selectedDraftTitle,
        prompt: [
          `Prepare this ${selectedDraftTitle.toLowerCase()} for Pappas & Co. Landscaping.`,
          "Return clear draft text for manual review only.",
          "Do not say anything was sent, scheduled, charged, deleted, or updated.",
          "",
          prompt
        ].join("\n")
      });
      setDraft({ status: "success", data: readableDraft(response) });
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

      <section className="assistant-workspace" aria-label="Assistant workspace">
        <section className="assistant-action-rail" aria-label="Assistant actions">
          <div className="rail-heading">
            <h2>What can I draft?</h2>
            <p>Pick the work item. YardDesk prepares text for manual review only.</p>
          </div>
          {assistantActions.map((action) => (
            <button
              className={selectedDraftTitle === action.draftTitle ? "assistant-action-card active" : "assistant-action-card"}
              type="button"
              key={action.title}
              onClick={() => {
                setSelectedDraftTitle(action.draftTitle);
                setDraftPrompt(action.prompt);
                setDraft(null);
              }}
            >
              <strong>{action.title}</strong>
              <span>{action.prompt}</span>
            </button>
          ))}
        </section>

        <section className="table-card dashboard-panel" aria-label="Prepared Actions">
          <div className="table-toolbar">
            <div>
              <h2>{selectedDraftTitle}</h2>
              <p>Draft text for review. No email, text, payment, transfer, delete, or job update is sent automatically.</p>
            </div>
          </div>
          <div className="form-panel">
            <label>
              Context for {selectedDraftTitle}
              <textarea
                placeholder="Example: Follow up with a CopilotCRM work request about a patio quote."
                value={draftPrompt}
                onChange={(event) => setDraftPrompt(event.target.value)}
              />
            </label>
            <button className="btn btn-primary" type="button" onClick={prepareFollowup}>
              Prepare {selectedDraftTitle}
            </button>
            {draft?.status === "loading" ? <div className="state-block">Loading</div> : null}
            {draft?.status === "error" ? (
              <div className="state-block error">API failed: {draft.error}</div>
            ) : null}
            {draft?.status === "success" ? (
              <div className="draft-preview">
                <strong>{selectedDraftTitle}</strong>
                <p>{draft.data}</p>
                <small>No message was sent.</small>
              </div>
            ) : null}
          </div>
        </section>
      </section>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel
          title="Work items AI can help with right now"
          helper="Real work requests from existing APIs. Select one, then draft manually."
          state={workRequests}
        >
          {() => <WorkRequestCards rows={draftCandidates} />}
        </SuggestionPanel>

        <SuggestionPanel
          title="Route context for drafts"
          helper="Use this for rain delay, prep notes, and customer schedule updates."
          state={liveJobs}
        >
          {(rows) => <LiveJobCards rows={rows.slice(0, 8)} />}
        </SuggestionPanel>
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
