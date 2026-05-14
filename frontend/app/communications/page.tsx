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

  const inboxQueues = useMemo(() => {
    const allMessages = messages.data || [];
    const allCalls = calls.data || [];
    const allVoicemails = voicemails.data || [];
    return {
      missedCalls: allCalls.filter((call) =>
        ["missed", "no-answer", "no_answer", "voicemail"].includes(
          String(call.status || "").toLowerCase()
        )
      ),
      voicemails: allVoicemails,
      unreadMessages: allMessages.filter(
        (message) => message.direction === "inbound" && message.read !== true
      ),
      customerReplies: allMessages.filter((message) => message.direction === "inbound")
    };
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
          <p className="eyebrow">Inbox</p>
          <h1>Inbox</h1>
          <p className="muted">Missed calls, voicemails, unread messages, and customer replies. AI drafts only; nothing sends automatically.</p>
        </div>
      </header>

      <section className="workflow-summary" aria-label="Inbox queues">
        <QueueCard label="Missed Calls" count={inboxQueues.missedCalls.length} state={calls.status} />
        <QueueCard label="Voicemails" count={inboxQueues.voicemails.length} state={voicemails.status} />
        <QueueCard label="Unread Messages" count={inboxQueues.unreadMessages.length} state={messages.status} />
        <QueueCard label="Customer Replies" count={inboxQueues.customerReplies.length} state={messages.status} />
      </section>

      <section className="dashboard-grid command-grid">
        <InboxQueue title="Missed Calls" state={calls} rows={inboxQueues.missedCalls} emptyText="No missed calls found." />
        <InboxQueue title="Voicemails" state={voicemails} rows={inboxQueues.voicemails} emptyText="No voicemails found." />
      </section>

      <section className="dashboard-grid command-grid">
        <InboxQueue title="Unread Messages" state={messages} rows={inboxQueues.unreadMessages} emptyText="No unread messages." />
        <InboxQueue title="Customer Replies" state={messages} rows={inboxQueues.customerReplies.slice(0, 8)} emptyText="No customer replies." />
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Inbox Timeline">
          <PanelHeader title="Inbox Timeline" subtitle="Latest customer touches across available channels." />
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

        <section className="table-card dashboard-panel" aria-label="AI Reply Drafts">
          <PanelHeader title="AI Reply Drafts" subtitle="Prepare only. Sending stays manual." />
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

function QueueCard({ label, count, state }: { label: string; count: number; state: LoadState<unknown>["status"] }) {
  const value = state === "loading" ? "-" : state === "error" ? "Error" : count;
  return (
    <div className={`stat-card dashboard-stat ${state === "error" ? "has-error" : ""}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <p>{state === "success" ? "Live" : state === "empty" ? "Empty" : state === "error" ? "API failed" : "Loading"}</p>
      <i className="stat-dot green" aria-hidden="true" />
    </div>
  );
}

function InboxQueue<T extends CallRow | MessageRow | VoicemailRow>({
  title,
  state,
  rows,
  emptyText
}: {
  title: string;
  state: LoadState<T[]>;
  rows: T[];
  emptyText: string;
}) {
  return (
    <section className="table-card dashboard-panel" aria-label={title}>
      <PanelHeader title={title} subtitle="Action queue from existing communication records." />
      {state.status === "loading" ? <div className="state-block">Loading</div> : null}
      {state.status === "error" ? <div className="state-block error">API failed: {state.error}</div> : null}
      {state.status !== "loading" && state.status !== "error" && !rows.length ? (
        <div className="empty-state">{emptyText}</div>
      ) : null}
      {rows.length ? (
        <div className="compact-list">
          {rows.slice(0, 6).map((row, index) => (
            <div className="compact-row" key={String(row.id || index)}>
              <div>
                <strong>{queueTitle(row)}</strong>
                <span>{queueDetail(row)}</span>
              </div>
              <small>{formatDate(queueDate(row))}</small>
            </div>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function queueTitle(row: CallRow | MessageRow | VoicemailRow) {
  if ("customerName" in row && row.customerName) return row.customerName;
  if ("customer_name" in row && row.customer_name) return row.customer_name;
  if ("direction" in row && row.direction) return `${row.direction} message`;
  if ("transcript" in row || "transcription" in row) return "Voicemail";
  return "Call";
}

function queueDetail(row: CallRow | MessageRow | VoicemailRow) {
  if ("body" in row && row.body) return row.body;
  if ("transcript" in row && row.transcript) return row.transcript;
  if ("transcription" in row && row.transcription) return row.transcription;
  if ("summary" in row && row.summary) return row.summary;
  if ("status" in row && row.status) return row.status;
  return "No detail";
}

function queueDate(row: CallRow | MessageRow | VoicemailRow) {
  if ("created_at" in row && row.created_at) return row.created_at;
  if ("timestamp" in row && row.timestamp) return row.timestamp;
  return null;
}
