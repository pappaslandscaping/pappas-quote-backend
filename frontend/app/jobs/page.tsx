"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  backendUrl,
  fetchCompletedUninvoicedJobs,
  fetchCopilotLiveJobs,
  fetchCrewAvailability,
  fetchJobs,
  fetchJobStats
} from "../../lib/api";
import type { Job, JobStats } from "../../types/jobs";

type LoadState<T> = { status: "loading" | "success" | "empty" | "error"; data?: T; error?: string };
const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function money(value?: string | number | null) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD"
  });
}

function formatDate(value?: string | null) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric"
  });
}

function statusLabel(value?: string | null) {
  return String(value || "pending")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function statusClass(value?: string | null) {
  return String(value || "pending").toLowerCase().replace(/_/g, "-");
}

function jobTitle(job: Job) {
  return job.service_type || `Job #${job.id}`;
}

function dateInputValue(date: string) {
  return date ? date.slice(0, 10) : "";
}

function dateKey(value?: string | null) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
}

function isSkipped(job: Job) {
  return ["skipped", "cancelled", "canceled"].includes(String(job.status || "").toLowerCase());
}

export default function JobsPage() {
  const router = useRouter();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [stats, setStats] = useState<JobStats | null>(null);
  const [date, setDate] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [crewFilter, setCrewFilter] = useState("");
  const [search, setSearch] = useState("");
  const [isLoadingJobs, setIsLoadingJobs] = useState(true);
  const [error, setError] = useState("");
  const [completedUninvoiced, setCompletedUninvoiced] = useState<LoadState<Job[]>>(loading());
  const [crewAvailability, setCrewAvailability] = useState<LoadState<Array<Record<string, unknown>>>>(loading());
  const [liveJobs, setLiveJobs] = useState<LoadState<Job[]>>(loading());

  async function loadJobs() {
    setIsLoadingJobs(true);
    setError("");
    try {
      const nextJobs = await fetchJobs({
        date: date || undefined,
        status: statusFilter || undefined,
        crew: crewFilter || undefined,
        search: search || undefined,
        limit: 500
      });
      setJobs(nextJobs);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load jobs");
    } finally {
      setIsLoadingJobs(false);
    }
  }

  async function loadStats() {
    try {
      setStats(await fetchJobStats({ date: date || undefined }));
    } catch {
      setStats(null);
    }
  }

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      void loadJobs();
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [crewFilter, date, search, statusFilter]);

  useEffect(() => {
    void loadStats();
  }, [date]);

  useEffect(() => {
    let active = true;
    const targetDate = date || new Date().toISOString().slice(0, 10);

    async function load<T>(
      request: () => Promise<T>,
      setter: (state: LoadState<T>) => void,
      empty: (data: T) => boolean = () => false
    ) {
      try {
        const data = await request();
        if (!active) return;
        setter(empty(data) ? { status: "empty", data } : { status: "success", data });
      } catch (err) {
        if (!active) return;
        setter({ status: "error", error: err instanceof Error ? err.message : "API request failed" });
      }
    }

    void load(fetchCompletedUninvoicedJobs, setCompletedUninvoiced, (data) => data.length === 0);
    void load(
      () => fetchCrewAvailability(targetDate) as Promise<Array<Record<string, unknown>>>,
      setCrewAvailability,
      (data) => data.length === 0
    );
    void load(() => fetchCopilotLiveJobs(targetDate), setLiveJobs, (data) => data.length === 0);

    return () => {
      active = false;
    };
  }, [date]);

  const crews = useMemo(() => {
    const names = new Set<string>();
    jobs.forEach((job) => {
      if (job.crew_assigned) names.add(job.crew_assigned);
    });
    return [...names].sort((a, b) => a.localeCompare(b));
  }, [jobs]);

  const statuses = useMemo(() => {
    const values = new Set(["pending", "in_progress", "in-progress", "completed", "skipped", "cancelled"]);
    jobs.forEach((job) => {
      if (job.status) values.add(job.status);
    });
    return [...values];
  }, [jobs]);

  const visibleSummary = useMemo(() => {
    const byStatus = jobs.reduce<Record<string, number>>((acc, job) => {
      const status = String(job.status || "pending");
      acc[status] = (acc[status] || 0) + 1;
      return acc;
    }, {});

    return {
      total: jobs.length,
      pending: byStatus.pending || 0,
      completed: (byStatus.completed || 0) + (byStatus.done || 0),
      skipped: jobs.filter(isSkipped).length,
      revenue: jobs.reduce((sum, job) => sum + Number(job.service_price || 0), 0)
    };
  }, [jobs]);

  const summary = {
    total: stats?.total ?? visibleSummary.total,
    pending: stats?.byStatus?.pending ?? visibleSummary.pending,
    completed:
      (stats?.byStatus?.completed ?? 0) ||
      (stats?.byStatus?.done ?? 0) ||
      visibleSummary.completed,
    skipped:
      (stats?.byStatus?.skipped ?? 0) ||
      (stats?.byStatus?.cancelled ?? 0) ||
      visibleSummary.skipped,
    revenue: stats?.totalRevenue ?? visibleSummary.revenue
  };

  const targetDate = date || new Date().toISOString().slice(0, 10);
  const todaysJobs = useMemo(
    () => jobs.filter((job) => dateKey(job.job_date) === targetDate || (!date && jobs.length <= 25)),
    [date, jobs, targetDate]
  );
  const jobsByCrew = useMemo(() => {
    const grouped = new Map<string, Job[]>();
    todaysJobs.forEach((job) => {
      const crew = job.crew_assigned || "Unassigned";
      grouped.set(crew, [...(grouped.get(crew) || []), job]);
    });
    return Array.from(grouped.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [todaysJobs]);
  const blockers = useMemo(
    () =>
      jobs.filter(
        (job) =>
          !job.address ||
          !job.crew_assigned ||
          !job.service_type ||
          String(job.status || "").toLowerCase().includes("blocked")
      ),
    [jobs]
  );

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Crew Schedule</h1>
          <p className="muted">Today’s landscaping route work, blockers, and completed jobs that still need invoicing.</p>
        </div>
        <div className="topbar-actions">
          <a className="btn btn-secondary" href={backendUrl("/dispatch.html")}>
            Dispatch Board
          </a>
          <a className="btn btn-secondary" href={backendUrl("/import-scheduling.html")}>
            Legacy Import
          </a>
          <a className="btn btn-primary" href={backendUrl("/new-job.html")}>
            New Job
          </a>
        </div>
      </header>

      <section className="workflow-summary" aria-label="Crew schedule summary">
        <StatCard label="Route Jobs" value={summary.total} tone="blue" />
        <StatCard label="Pending" value={summary.pending} tone="amber" />
        <StatCard label="Completed" value={summary.completed} tone="green" />
        <StatCard label="Skipped" value={summary.skipped} tone="purple" />
      </section>

      <section className="table-card" aria-label="Today by Crew">
        <div className="table-toolbar">
          <div>
            <h2>Today by Crew</h2>
            <p>{targetDate} route cards grouped by crew.</p>
          </div>
          <div className="workflow-actions">
            <button className="quick-action-btn" type="button" disabled>
              Rain delay placeholder
            </button>
            <button className="quick-action-btn" type="button" disabled>
              Notify crew placeholder
            </button>
          </div>
        </div>
        {isLoadingJobs ? (
          <div className="state-block">Loading route cards...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : jobsByCrew.length ? (
          <div className="crew-board">
            {jobsByCrew.map(([crew, crewJobs]) => (
              <section className="crew-lane" key={crew} aria-label={`${crew} jobs`}>
                <header>
                  <h3>{crew}</h3>
                  <span>{crewJobs.length} jobs</span>
                </header>
                <div className="workflow-stack">
                  {crewJobs.map((job) => (
                    <Link className="route-card" href={`/jobs/${job.id}`} key={job.id}>
                      <div>
                        <strong>{job.customer_name || "Unknown customer"}</strong>
                        <span>{jobTitle(job)}</span>
                      </div>
                      <p>{job.address || "Missing address"}</p>
                      <div className="timeline-row-meta">
                        <span>{job.estimated_duration || 30} min</span>
                        <span>{money(job.service_price)}</span>
                        <span className={`status-pill status-${statusClass(job.status)}`}>
                          {statusLabel(job.status)}
                        </span>
                      </div>
                    </Link>
                  ))}
                </div>
              </section>
            ))}
          </div>
        ) : (
          <div className="empty-state">No jobs found for this route date.</div>
        )}
      </section>

      <section className="dashboard-grid command-grid">
        <OperationsPanel title="Crew Readiness" state={crewAvailability} emptyText="No crew workload found for this date.">
          {(rows) => (
            <div className="compact-list">
              {rows.slice(0, 6).map((crew, index) => (
                <div className="compact-row" key={String(crew.crew_name || index)}>
                  <div>
                    <strong>{String(crew.crew_name || "Unassigned")}</strong>
                    <span>{String(crew.job_count || 0)} jobs - {String(crew.total_hours || 0)} hours</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </OperationsPanel>

        <OperationsPanel title="Completed Not Invoiced" state={completedUninvoiced} emptyText="No completed-uninvoiced jobs.">
          {(rows) => (
            <div className="compact-list">
              {rows.slice(0, 6).map((job) => (
                <Link className="compact-row" href={`/jobs/${job.id}`} key={job.id}>
                  <div>
                    <strong>{job.customer_name || "Unknown customer"}</strong>
                    <span>{job.service_type || "Service"} - {formatDate(job.job_date)}</span>
                  </div>
                  <small>{money(job.service_price)}</small>
                </Link>
              ))}
            </div>
          )}
        </OperationsPanel>
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Missing info and blockers">
          <div className="table-toolbar">
            <div>
              <h2>Missing Info / Blockers</h2>
              <p>Jobs needing cleanup before a crew can execute confidently.</p>
            </div>
          </div>
          {blockers.length ? (
            <div className="compact-list">
              {blockers.slice(0, 8).map((job) => (
                <Link className="compact-row" href={`/jobs/${job.id}`} key={job.id}>
                  <div>
                    <strong>{job.customer_name || "Unknown customer"}</strong>
                    <span>
                      {[
                        !job.address ? "missing address" : null,
                        !job.crew_assigned ? "missing crew" : null,
                        !job.service_type ? "missing service" : null,
                        job.status
                      ].filter(Boolean).join(" - ")}
                    </span>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <div className="empty-state">No obvious blockers found.</div>
          )}
        </section>

        <OperationsPanel title="Copilot Live Jobs" state={liveJobs} emptyText="No live Copilot jobs available.">
          {(rows) => (
            <div className="compact-list">
              {rows.slice(0, 6).map((job) => (
                <div className="compact-row" key={job.id || `${job.customer_name}-${job.job_date}`}>
                  <div>
                    <strong>{job.customer_name || "Live job"}</strong>
                    <span>{job.service_type || job.address || "Copilot job"}</span>
                  </div>
                  <small>{job.crew_assigned || job.status || "Live"}</small>
                </div>
              ))}
            </div>
          )}
        </OperationsPanel>
      </section>

      <section className="table-card">
        <div className="table-toolbar">
          <div>
            <h2>Jobs</h2>
            <p>{jobs.length} visible</p>
          </div>
          <div className="filters">
            <input
              aria-label="Filter by date"
              type="date"
              value={dateInputValue(date)}
              onChange={(event) => setDate(event.target.value)}
            />
            <select
              aria-label="Filter by status"
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
            >
              <option value="">All Statuses</option>
              {statuses.map((status) => (
                <option key={status} value={status}>
                  {statusLabel(status)}
                </option>
              ))}
            </select>
            <select
              aria-label="Filter by crew"
              value={crewFilter}
              onChange={(event) => setCrewFilter(event.target.value)}
            >
              <option value="">All Crews</option>
              {crews.map((crew) => (
                <option key={crew} value={crew}>
                  {crew}
                </option>
              ))}
            </select>
            <input
              aria-label="Search jobs"
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search jobs..."
            />
          </div>
        </div>

        {isLoadingJobs ? (
          <div className="state-block">Loading jobs...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Customer</th>
                  <th>Service</th>
                  <th>Crew</th>
                  <th>Price</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {jobs.length ? (
                  jobs.map((job) => (
                    <tr
                      className="clickable-row"
                      key={job.id}
                      onClick={() => router.push(`/jobs/${job.id}`)}
                    >
                      <td>
                        <div>{formatDate(job.job_date) || "-"}</div>
                        <div className="subtle">{job.estimated_duration || 30} min</div>
                      </td>
                      <td>
                        <Link
                          className="row-link"
                          href={`/jobs/${job.id}`}
                          onClick={(event) => event.stopPropagation()}
                        >
                          {job.customer_name || "Unknown"}
                        </Link>
                        <div className="subtle truncate">{job.address || ""}</div>
                      </td>
                      <td>{jobTitle(job)}</td>
                      <td>{job.crew_assigned || "Unassigned"}</td>
                      <td>{money(job.service_price)}</td>
                      <td>
                        <span className={`status-pill status-${statusClass(job.status)}`}>
                          {statusLabel(job.status)}
                        </span>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={6}>
                      <div className="empty-state">No jobs found</div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}

function StatCard({
  label,
  value,
  tone
}: {
  label: string;
  value: number | string;
  tone: "blue" | "amber" | "green" | "purple";
}) {
  return (
    <div className="stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <i className={`stat-dot ${tone}`} aria-hidden="true" />
    </div>
  );
}

function OperationsPanel<T>({
  title,
  state,
  emptyText,
  children
}: {
  title: string;
  state: LoadState<T>;
  emptyText: string;
  children: (data: T) => ReactNode;
}) {
  return (
    <section className="table-card dashboard-panel" aria-label={title}>
      <div className="table-toolbar">
        <div>
          <h2>{title}</h2>
          <p>Read-only operations visibility.</p>
        </div>
      </div>
      {state.status === "loading" ? <div className="state-block">Loading</div> : null}
      {state.status === "error" ? <div className="state-block error">API failed: {state.error}</div> : null}
      {state.status === "empty" ? <div className="empty-state">{emptyText}</div> : null}
      {state.status === "success" ? children(state.data as T) : null}
    </section>
  );
}
