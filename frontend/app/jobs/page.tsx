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
  fetchJobs
} from "../../lib/api";
import type { Job } from "../../types/jobs";

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

function todayKey() {
  return new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });
}

function jobDateValue(job: Job) {
  return job.job_date || job.service_date || null;
}

function dateKey(value?: string | null) {
  if (!value) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
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

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      void loadJobs();
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [crewFilter, date, search, statusFilter]);

  useEffect(() => {
    let active = true;
    const targetDate = date || todayKey();

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

  const targetDate = date || todayKey();
  const liveRouteJobs = liveJobs.status === "success" && liveJobs.data?.length ? liveJobs.data : [];
  const scheduledRouteJobs = useMemo(
    () => jobs.filter((job) => dateKey(jobDateValue(job)) === targetDate),
    [jobs, targetDate]
  );
  const routeJobs = liveRouteJobs.length ? liveRouteJobs : scheduledRouteJobs;
  const routeSource = liveRouteJobs.length ? "Live CopilotCRM route" : "Scheduled jobs";
  const routeSummary = {
    total: routeJobs.length,
    pending: routeJobs.filter((job) => {
      const status = String(job.status || "pending").toLowerCase();
      return !["completed", "done", "skipped", "cancelled", "canceled"].includes(status);
    }).length,
    completed: routeJobs.filter((job) => ["completed", "done"].includes(String(job.status || "").toLowerCase())).length,
    unassigned: routeJobs.filter((job) => !job.crew_assigned).length,
    skipped: routeJobs.filter(isSkipped).length
  };
  const jobsByCrew = useMemo(() => {
    const grouped = new Map<string, Job[]>();
    routeJobs.forEach((job) => {
      const crew = job.crew_assigned || "Unassigned";
      grouped.set(crew, [...(grouped.get(crew) || []), job]);
    });
    return Array.from(grouped.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [routeJobs]);
  const routeWorkload = useMemo(
    () =>
      jobsByCrew.map(([crew, crewJobs]) => ({
        crew,
        count: crewJobs.length,
        completed: crewJobs.filter((job) => ["completed", "done"].includes(String(job.status || "").toLowerCase())).length,
        pending: crewJobs.filter((job) => {
          const status = String(job.status || "pending").toLowerCase();
          return !["completed", "done", "skipped", "cancelled", "canceled"].includes(status);
        }).length,
        minutes: crewJobs.reduce((sum, job) => sum + Number(job.estimated_duration || 30), 0)
      })),
    [jobsByCrew]
  );
  const blockers = useMemo(
    () =>
      routeJobs.filter(
        (job) =>
          !job.address ||
          !job.service_type ||
          job.hold_from_dispatch ||
          String(job.status || "").toLowerCase().includes("blocked")
      ),
    [routeJobs]
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
        <StatCard label="Today's route" value={routeSummary.total} tone="blue" />
        <StatCard label="Still pending" value={routeSummary.pending} tone="amber" />
        <StatCard label="Completed" value={routeSummary.completed} tone="green" />
        <StatCard label="Unassigned" value={routeSummary.unassigned} tone="purple" />
      </section>

      <section className="table-card" aria-label="Today by Crew">
        <div className="table-toolbar">
          <div>
            <h2>Today by Crew</h2>
            <p>{targetDate} route cards grouped by crew. Source: {routeSource}.</p>
          </div>
          <div className="workflow-actions">
            <input
              aria-label="Route date"
              type="date"
              value={dateInputValue(targetDate)}
              onChange={(event) => setDate(event.target.value)}
            />
            <button className="quick-action-btn" type="button" disabled>
              Rain delay draft
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
                  {crewJobs.map((job) => {
                    const isReadOnlyLiveJob = job.is_read_only || typeof job.id === "string";
                    const card = (
                      <div>
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
                      </div>
                    );

                    return isReadOnlyLiveJob ? (
                      <div className="route-card" key={job.id}>
                        {card}
                      </div>
                    ) : (
                      <Link className="route-card" href={`/jobs/${job.id}`} key={job.id}>
                        {card}
                      </Link>
                    );
                  })}
                </div>
              </section>
            ))}
          </div>
        ) : (
          <div className="empty-state">No jobs found for this route date.</div>
        )}
      </section>

      <section className="dashboard-grid command-grid">
        <section className="table-card dashboard-panel" aria-label="Crew Readiness">
          <div className="table-toolbar">
            <div>
              <h2>Crew Readiness</h2>
              <p>Built from today&apos;s visible route.</p>
            </div>
          </div>
          {routeWorkload.length ? (
            <div className="compact-list">
              {routeWorkload.map((crew) => (
                <div className="compact-row" key={crew.crew}>
                  <div>
                    <strong>{crew.crew}</strong>
                    <span>
                      {crew.count} jobs - {crew.completed} completed - {crew.pending} pending
                    </span>
                  </div>
                  <small>{Math.round(crew.minutes / 60 * 10) / 10} hrs</small>
                </div>
              ))}
            </div>
          ) : crewAvailability.status === "loading" ? (
            <div className="state-block">Loading</div>
          ) : crewAvailability.status === "error" ? (
            <div className="state-block error">API failed: {crewAvailability.error}</div>
          ) : (
            <div className="empty-state">No crew workload found for this date.</div>
          )}
        </section>

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
              <p>Only flags issues on today&apos;s visible route.</p>
            </div>
          </div>
          {blockers.length ? (
            <div className="compact-list">
              {blockers.slice(0, 8).map((job) => (
                <div className="compact-row" key={job.id}>
                  <div>
                    <strong>{job.customer_name || "Unknown customer"}</strong>
                    <span>
                      {[
                        !job.address ? "missing address" : null,
                        !job.service_type ? "missing service" : null,
                        job.hold_from_dispatch ? "held from dispatch" : null,
                        job.status
                      ].filter(Boolean).join(" - ")}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="empty-state">No obvious blockers found.</div>
          )}
        </section>

        <section className="table-card dashboard-panel" aria-label="Route source">
          <div className="table-toolbar">
            <div>
              <h2>Route Source</h2>
              <p>Where today&apos;s board is coming from.</p>
            </div>
          </div>
          <div className="compact-list">
            <div className="compact-row">
              <div>
                <strong>{routeSource}</strong>
                <span>{targetDate} - {routeSummary.total} visible route jobs</span>
              </div>
              <small>{liveJobs.status === "error" ? "Error" : liveRouteJobs.length ? "Live" : "Fallback"}</small>
            </div>
          </div>
        </section>
      </section>

      <details className="table-card disclosure-card">
        <summary>
          <span>
            <strong>All Scheduled Jobs</strong>
            <small>{jobs.length} visible from the local schedule table</small>
          </span>
          <span>Open</span>
        </summary>
        <div className="table-toolbar">
          <div>
            <h2>Scheduled Job Archive</h2>
            <p>Use this for search and cleanup. Today&apos;s route board above uses {routeSource}.</p>
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
      </details>
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
