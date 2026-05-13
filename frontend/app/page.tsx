"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchActivityFeed,
  fetchCompletedUninvoicedJobs,
  fetchFinanceSummary,
  fetchInvoiceStats,
  fetchJobsDashboard,
  fetchQuoteRequests,
  fetchTodaySummary
} from "../lib/api";
import type { ActivityEvent, JobsDashboard, TodaySummary } from "../types/dashboard";
import type { InvoiceStats } from "../types/invoices";
import type { Job } from "../types/jobs";
import type { QuoteRequest } from "../types/quotes";

type LoadStatus = "loading" | "success" | "empty" | "error";

type LoadState<T> = {
  status: LoadStatus;
  data?: T;
  error?: string;
};

type NeedItem = {
  label: string;
  value: number;
  href: string;
  detail: string;
  tone: "blue" | "amber" | "red" | "green";
};

const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function money(value?: string | number | null) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  });
}

function formatDate(value?: string | null) {
  if (!value) return "Unscheduled";
  return new Date(value).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric"
  });
}

function formatFullDate(value?: string | null) {
  if (!value) return "No date";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "No date";
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  });
}

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

function isNewQuote(quote: QuoteRequest) {
  return ["new", "pending", "requested"].includes(
    String(quote.status || "").toLowerCase()
  );
}

export default function HomePage() {
  const [today, setToday] = useState<LoadState<TodaySummary>>(loading());
  const [quotes, setQuotes] = useState<LoadState<QuoteRequest[]>>(loading());
  const [invoices, setInvoices] = useState<LoadState<InvoiceStats>>(loading());
  const [jobs, setJobs] = useState<LoadState<JobsDashboard>>(loading());
  const [uninvoiced, setUninvoiced] = useState<LoadState<Job[]>>(loading());
  const [activity, setActivity] = useState<LoadState<ActivityEvent[]>>(loading());
  const [finance, setFinance] = useState<LoadState<Record<string, unknown>>>(loading());

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

    void load(fetchTodaySummary, setToday, () => false);
    void load(fetchQuoteRequests, setQuotes, (data) => data.length === 0);
    void load(fetchInvoiceStats, setInvoices, () => false);
    void load(fetchJobsDashboard, setJobs, (data) => (data.upcoming || []).length === 0);
    void load(fetchCompletedUninvoicedJobs, setUninvoiced, (data) => data.length === 0);
    void load(fetchActivityFeed, setActivity, (data) => data.length === 0);
    void load(fetchFinanceSummary, setFinance, () => false);

    return () => {
      active = false;
    };
  }, []);

  const needsAttention = useMemo<NeedItem[]>(() => {
    const newQuotes =
      quotes.status === "success" || quotes.status === "empty"
        ? (quotes.data || []).filter(isNewQuote).length
        : 0;
    const overdueInvoices =
      invoices.status === "success" || invoices.status === "empty"
        ? Number(invoices.data?.overdue || 0)
        : 0;
    const uninvoicedJobs =
      uninvoiced.status === "success" || uninvoiced.status === "empty"
        ? (uninvoiced.data || []).length
        : 0;
    const followUps = newQuotes + overdueInvoices;

    return [
      {
        label: "New quote requests",
        value: newQuotes,
        href: "/quotes",
        detail: "Review and respond before the day gets away.",
        tone: "blue"
      },
      {
        label: "Overdue invoices",
        value: overdueInvoices,
        href: "/invoices",
        detail: "Send reminders or check payment status.",
        tone: "red"
      },
      {
        label: "Completed-uninvoiced jobs",
        value: uninvoicedJobs,
        href: "/jobs",
        detail: "Turn completed work into billable invoices.",
        tone: "amber"
      },
      {
        label: "High-priority follow-ups",
        value: followUps,
        href: "/ai",
        detail: "Use AI drafts for quote or invoice follow-up.",
        tone: "green"
      }
    ];
  }, [invoices, quotes, uninvoiced]);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Daily Command Center</h1>
          <p className="muted">
            Today&apos;s work, money, and follow-ups from the existing backend APIs.
          </p>
        </div>
        <div className="topbar-actions">
          <Link className="btn btn-secondary" href="/reports">
            View Reports
          </Link>
          <Link className="btn btn-primary" href="/quotes">
            New Quote
          </Link>
        </div>
      </header>

      <section className="stats-grid stats-grid-five" aria-label="Today">
        <TodayCard label="Jobs today" state={today} value={(data) => data.jobs_today || 0} />
        <TodayCard
          label="Revenue today"
          state={today}
          value={(data) => money(data.revenue_today)}
        />
        <TodayCard
          label="Pending quotes"
          state={today}
          value={(data) => data.pending_quotes || 0}
        />
        <TodayCard
          label="Overdue invoices"
          state={today}
          value={(data) => data.overdue_invoices || 0}
        />
        <TodayCard
          label="Unread messages"
          state={today}
          value={(data) => data.unread_messages || 0}
        />
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Needs Attention">
          <PanelHeader title="Needs Attention" subtitle="Work that can change today's outcome." />
          <Stateful state={[quotes, invoices, uninvoiced]} emptyText="Nothing urgent right now.">
            <div className="attention-list">
              {needsAttention.map((item) => (
                <Link className={`attention-item tone-${item.tone}`} href={item.href} key={item.label}>
                  <strong>{item.value}</strong>
                  <span>{item.label}</span>
                  <small>{item.value > 0 ? item.detail : "No current items."}</small>
                </Link>
              ))}
            </div>
          </Stateful>
        </section>

        <section className="table-card dashboard-panel" aria-label="Quick Actions">
          <PanelHeader title="Quick Actions" subtitle="Jump straight into common admin work." />
          <div className="quick-links-grid quick-links-vertical">
            <Link className="quick-action-btn primary" href="/quotes">
              New quote
            </Link>
            <Link className="quick-action-btn" href="/invoices">
              View invoices
            </Link>
            <Link className="quick-action-btn" href="/jobs">
              View jobs
            </Link>
            <Link className="quick-action-btn" href="/customers">
              View customers
            </Link>
            <Link className="quick-action-btn" href="/ai">
              Start AI assistant
            </Link>
          </div>
        </section>
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Upcoming Work">
          <PanelHeader title="Upcoming Work" subtitle="Next scheduled jobs." />
          <Panel state={jobs} emptyText="No upcoming jobs found.">
            {(data) => (
              <div className="compact-list">
                {(data.upcoming || []).slice(0, 5).map((job) => (
                  <Link className="compact-row" href={`/jobs/${job.id}`} key={job.id}>
                    <div>
                      <strong>{job.customer_name || "Unknown customer"}</strong>
                      <span>{job.service_type || "Service"} - {job.crew_assigned || "Crew TBD"}</span>
                    </div>
                    <div className="compact-row-meta">
                      <span>{formatDate(job.job_date)}</span>
                      <span className={`status-pill status-${String(job.status || "pending").toLowerCase()}`}>
                        {job.status || "pending"}
                      </span>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </Panel>
        </section>

        <section className="table-card dashboard-panel" aria-label="Recent Activity">
          <PanelHeader title="Recent Activity" subtitle="Latest business activity, not limited to today." />
          <Panel state={activity} emptyText="No recent activity yet.">
            {(events) => (
              <div className="compact-list">
                {events.slice(0, 8).map((event, index) => (
                  <div className="compact-row" key={`${event.type || "event"}-${index}`}>
                    <div>
                      <strong>{event.description || "Activity recorded"}</strong>
                      <span>{event.type || "Update"}</span>
                    </div>
                    <small>{formatFullDate(event.timestamp)}</small>
                  </div>
                ))}
              </div>
            )}
          </Panel>
        </section>
      </section>

      <section className="table-card dashboard-panel" aria-label="API Health">
        <PanelHeader title="API Health" subtitle="Dashboard panels load independently." />
        <div className="dashboard-health-list health-grid">
          <HealthRow label="Today" state={today} />
          <HealthRow label="Quotes" state={quotes} />
          <HealthRow label="Invoices" state={invoices} />
          <HealthRow label="Scheduling/Jobs" state={jobs} />
          <HealthRow label="Completed-uninvoiced" state={uninvoiced} />
          <HealthRow label="Finance" state={finance} />
          <HealthRow label="Activity" state={activity} />
        </div>
      </section>
    </main>
  );
}

function TodayCard({
  label,
  state,
  value
}: {
  label: string;
  state: LoadState<TodaySummary>;
  value: (data: TodaySummary) => string | number;
}) {
  const cardValue =
    state.status === "loading"
      ? "-"
      : state.status === "error"
        ? "Error"
        : value(state.data || {});
  const meta =
    state.status === "loading"
      ? "Loading"
      : state.status === "error"
        ? state.error || "API failed"
        : Number(cardValue) === 0
          ? "Nothing to show"
          : "Loaded";

  return (
    <div className={`stat-card dashboard-stat ${state.status === "error" ? "has-error" : ""}`}>
      <span>{label}</span>
      <strong>{cardValue}</strong>
      <p>{meta}</p>
      <i className="stat-dot green" aria-hidden="true" />
    </div>
  );
}

function PanelHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="table-toolbar">
      <div>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </div>
    </div>
  );
}

function Panel<T>({
  state,
  emptyText,
  children
}: {
  state: LoadState<T>;
  emptyText: string;
  children: (data: T) => ReactNode;
}) {
  if (state.status === "loading") return <div className="state-block">Loading</div>;
  if (state.status === "error") {
    return <div className="state-block error">API failed: {state.error}</div>;
  }
  if (state.status === "empty") return <div className="empty-state">{emptyText}</div>;
  return <>{children(state.data as T)}</>;
}

function Stateful({
  state,
  emptyText,
  children
}: {
  state: Array<LoadState<unknown>>;
  emptyText: string;
  children: ReactNode;
}) {
  const hasResolved = state.some((item) => item.status === "success" || item.status === "empty");
  if (!hasResolved && state.some((item) => item.status === "loading")) {
    return <div className="state-block">Loading</div>;
  }
  const failed = state.find((item) => item.status === "error");
  if (!hasResolved && failed) return <div className="state-block error">API failed: {failed.error}</div>;
  const allEmpty = state.every((item) => item.status === "empty");
  if (allEmpty) return <div className="empty-state">{emptyText}</div>;
  return (
    <>
      {failed ? <div className="state-inline-error">Some data failed: {failed.error}</div> : null}
      {state.some((item) => item.status === "loading") ? (
        <div className="state-inline-note">Some data is still loading.</div>
      ) : null}
      {children}
    </>
  );
}

function HealthRow({ label, state }: { label: string; state: LoadState<unknown> }) {
  const text =
    state.status === "success"
      ? "Loaded"
      : state.status === "empty"
        ? "Empty"
        : state.status === "error"
          ? "Error"
          : "Loading";
  const className =
    state.status === "success"
      ? "status-pill status-completed"
      : state.status === "empty"
        ? "status-pill status-draft"
        : state.status === "error"
          ? "status-pill status-cancelled"
          : "status-pill status-pending";

  return (
    <div className="dashboard-health-row">
      <span>{label}</span>
      <strong className={className}>{text}</strong>
    </div>
  );
}
