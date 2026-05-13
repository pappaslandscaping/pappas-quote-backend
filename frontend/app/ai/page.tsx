"use client";

import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import {
  fetchAiCampaignSegments,
  fetchAiChurnRisk,
  fetchAiLeadScores,
  fetchAiRevenueForecast,
  fetchAiScheduleSuggestions,
  generateAiFollowup
} from "../../lib/api";
import type { AiChurnRisk, AiLeadScore } from "../../types/ai";

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
  const [leadScores, setLeadScores] = useState<LoadState<AiLeadScore[]>>(loading());
  const [churnRisk, setChurnRisk] = useState<LoadState<AiChurnRisk[]>>(loading());
  const [forecast, setForecast] = useState<LoadState<Array<Record<string, unknown>>>>(loading());
  const [segments, setSegments] = useState<LoadState<unknown>>(loading());
  const [schedule, setSchedule] = useState<LoadState<Array<Record<string, unknown>>>>(loading());
  const [draftPrompt, setDraftPrompt] = useState("");
  const [draft, setDraft] = useState<LoadState<string> | null>(null);

  useEffect(() => {
    let active = true;

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

    void load(fetchAiLeadScores, setLeadScores, (data) => data.length === 0);
    void load(fetchAiChurnRisk, setChurnRisk, (data) => data.length === 0);
    void load(fetchAiRevenueForecast, setForecast, (data) => data.length === 0);
    void load(fetchAiCampaignSegments, setSegments, (data) => Array.isArray(data) && data.length === 0);
    void load(() => fetchAiScheduleSuggestions(today()), setSchedule, (data) => data.length === 0);

    return () => {
      active = false;
    };
  }, []);

  async function prepareFollowup() {
    setDraft({ status: "loading" });
    try {
      const response = await generateAiFollowup({
        context: draftPrompt || "Draft a polite follow-up for an open landscaping lead."
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
          <h1>AI Assistant</h1>
          <p className="muted">
            Suggestions and prepared drafts only. Nothing sends automatically.
          </p>
        </div>
      </header>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel title="Lead follow-up candidates" state={leadScores}>
          {(rows) => (
            <div className="compact-list">
              {rows.slice(0, 6).map((row) => (
                <div className="compact-row" key={row.id || row.name}>
                  <div>
                    <strong>{row.name || "Lead"}</strong>
                    <span>Grade {row.grade || "-"} - Score {row.score || 0}</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </SuggestionPanel>

        <SuggestionPanel title="Churn risk" state={churnRisk}>
          {(rows) => (
            <div className="compact-list">
              {rows.slice(0, 6).map((row) => (
                <div className="compact-row" key={row.id || row.name}>
                  <div>
                    <strong>{row.name || "Customer"}</strong>
                    <span>{row.risk_level || "Unknown"} risk - {row.risk_score || 0}</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </SuggestionPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel title="Revenue forecast" state={forecast}>
          {(rows) => <SimpleCards rows={rows.slice(0, 4)} />}
        </SuggestionPanel>

        <SuggestionPanel title="Campaign segments" state={segments}>
          {(data) => <SimpleCards rows={Array.isArray(data) ? data : Object.values(data as Record<string, unknown>)} />}
        </SuggestionPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <SuggestionPanel title="Schedule opportunities" state={schedule}>
          {(rows) => <SimpleCards rows={rows.slice(0, 5)} />}
        </SuggestionPanel>

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
                placeholder="Example: Follow up with a new lead about a patio quote."
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
  state,
  children
}: {
  title: string;
  state: LoadState<T>;
  children: (data: T) => ReactNode;
}) {
  return (
    <section className="table-card dashboard-panel" aria-label={title}>
      <div className="table-toolbar">
        <div>
          <h2>{title}</h2>
          <p>Suggest only.</p>
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

function SimpleCards({ rows }: { rows: unknown[] }) {
  if (!rows.length) return <div className="empty-state">Nothing to show.</div>;

  return (
    <div className="compact-list">
      {rows.map((row, index) => (
        <div className="compact-row" key={index}>
          <div>
            <strong>{titleFor(row)}</strong>
            <span>{detailFor(row)}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function titleFor(row: unknown) {
  if (!row || typeof row !== "object") return String(row || "Suggestion");
  const record = row as Record<string, unknown>;
  return String(record.name || record.month || record.title || "Suggestion");
}

function detailFor(row: unknown) {
  if (!row || typeof row !== "object") return "";
  const record = row as Record<string, unknown>;
  const parts = Object.entries(record)
    .filter(([key]) => !["name", "month", "title", "customer_ids"].includes(key))
    .slice(0, 2)
    .map(([key, value]) => `${key.replaceAll("_", " ")}: ${String(value)}`);
  return parts.join(" - ");
}
