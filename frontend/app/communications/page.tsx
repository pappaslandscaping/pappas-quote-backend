"use client";

import { useEffect, useMemo, useState } from "react";
import {
  fetchCalls,
  fetchMessages,
  fetchVoicemails,
  generateAiFollowup
} from "../../lib/api";
import type { CallRow, MessageRow, VoicemailRow } from "../../types/communications";

type LoadState<T> = { status: "loading" | "success" | "empty" | "error"; data?: T; error?: string };
type TimelineItem = {
  id: string;
  kind: "message" | "call" | "voicemail";
  title: string;
  detail: string;
  date: string | null;
  status: string;
};

const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

function formatDate(value?: string | null) {
  if (!value) return "No date";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "No date";
  return date.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

export default function CommunicationsPage() {
  const [messages, setMessages] = useState<LoadState<MessageRow[]>>(loading());
  const [calls, setCalls] = useState<LoadState<CallRow[]>>(loading());
  const [voicemails, setVoicemails] = useState<LoadState<VoicemailRow[]>>(loading());
  const [draftPrompt, setDraftPrompt] = useState("");
  const [draft, setDraft] = useState<LoadState<string> | null>(null);

  useEffect(() => {
    let active = true;

    async function load<T>(
      request: () => Promise<T>,
      setter: (state: LoadState<T>) => void,
      empty: (data: T) => boolean
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

    void load(() => fetchMessages(100), setMessages, (data) => data.length === 0);
    void load(() => fetchCalls(50), setCalls, (data) => data.length === 0);
    void load(fetchVoicemails, setVoicemails, (data) => data.length === 0);

    return () => {
      active = false;
    };
  }, []);

  const timeline = useMemo(() => {
    const items: TimelineItem[] = [];
    (messages.data || []).forEach((message) => {
      items.push({
        id: `message-${message.id || message.sid}`,
        kind: "message",
        title: `${message.direction === "inbound" ? "Inbound" : "Outbound"} message${message.customerName ? ` with ${message.customerName}` : ""}`,
        detail: message.body || "No message body",
        date: message.timestamp || null,
        status: message.status || (message.read ? "read" : "unread")
      });
    });
    (calls.data || []).forEach((call) => {
      items.push({
        id: `call-${call.id}`,
        kind: "call",
        title: `Call${call.customer_name ? ` with ${call.customer_name}` : ""}`,
        detail: call.transcription || call.summary || call.option_selected || call.from_number || "No call detail",
        date: call.created_at || null,
        status: call.status || "call"
      });
    });
    (voicemails.data || []).forEach((voicemail) => {
      items.push({
        id: `voicemail-${voicemail.id}`,
        kind: "voicemail",
        title: `Voicemail${voicemail.customer_name ? ` from ${voicemail.customer_name}` : ""}`,
        detail: voicemail.transcript || voicemail.transcription || voicemail.from || voicemail.from_number || "No voicemail transcript",
        date: voicemail.created_at || voicemail.timestamp || null,
        status: voicemail.status || "voicemail"
      });
    });
    return items.sort((a, b) => Date.parse(b.date || "") - Date.parse(a.date || "")).slice(0, 40);
  }, [calls.data, messages.data, voicemails.data]);

  async function prepareDraft() {
    setDraft({ status: "loading" });
    try {
      const response = await generateAiFollowup({
        context: draftPrompt || "Draft a helpful customer follow-up based on recent landscaping communication."
      });
      setDraft({
        status: "success",
        data: response.draft || response.text || response.message || response.response || "Draft prepared. Review before sending."
      });
    } catch (error) {
      setDraft({ status: "error", error: shortError(error) });
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Communications</p>
          <h1>Communication Center</h1>
          <p className="muted">Recent email/SMS/call activity with AI drafts only. Nothing sends automatically.</p>
        </div>
      </header>

      <section className="stats-grid" aria-label="Communication stats">
        <StatCard label="Messages" state={messages} />
        <StatCard label="Calls" state={calls} />
        <StatCard label="Voicemails" state={voicemails} />
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Communication Timeline">
          <PanelHeader title="Communication Timeline" subtitle="Latest customer touches across available channels." />
          {messages.status === "loading" && calls.status === "loading" && voicemails.status === "loading" ? <div className="state-block">Loading</div> : null}
          {timeline.length ? (
            <div className="compact-list">
              {timeline.map((item) => (
                <div className="compact-row" key={item.id}>
                  <div>
                    <strong>{item.title}</strong>
                    <span>{item.detail}</span>
                  </div>
                  <div className="compact-row-meta">
                    <small>{formatDate(item.date)}</small>
                    <span className={`status-pill status-${item.kind}`}>{item.status}</span>
                  </div>
                </div>
              ))}
            </div>
          ) : messages.status !== "loading" || calls.status !== "loading" || voicemails.status !== "loading" ? (
            <div className="empty-state">No communications found.</div>
          ) : null}
        </section>

        <section className="table-card dashboard-panel" aria-label="AI Communication Drafts">
          <PanelHeader title="AI Communication Drafts" subtitle="Prepare only. Sending stays manual." />
          <div className="form-panel">
            <label>
              Draft context
              <textarea value={draftPrompt} onChange={(event) => setDraftPrompt(event.target.value)} placeholder="Example: Reply to a customer asking about their overdue invoice." />
            </label>
            <button className="btn btn-primary" type="button" onClick={prepareDraft}>Prepare reply draft</button>
            {draft?.status === "loading" ? <div className="state-block">Loading</div> : null}
            {draft?.status === "error" ? <div className="state-block error">API failed: {draft.error}</div> : null}
            {draft?.status === "success" ? <div className="draft-preview"><strong>Draft preview</strong><p>{draft.data}</p><small>No email or SMS was sent.</small></div> : null}
          </div>
        </section>
      </section>
    </main>
  );
}

function PanelHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return <div className="table-toolbar"><div><h2>{title}</h2><p>{subtitle}</p></div></div>;
}

function StatCard<T extends unknown[]>({ label, state }: { label: string; state: LoadState<T> }) {
  const value = state.status === "success" || state.status === "empty" ? state.data?.length || 0 : state.status === "error" ? "Error" : "-";
  const meta = state.status === "success" ? "Loaded" : state.status === "empty" ? "Empty" : state.status === "error" ? state.error || "Error" : "Loading";
  return <div className={`stat-card dashboard-stat ${state.status === "error" ? "has-error" : ""}`}><span>{label}</span><strong>{value}</strong><p>{meta}</p><i className="stat-dot green" aria-hidden="true" /></div>;
}
