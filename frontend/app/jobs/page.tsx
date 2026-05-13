"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { fetchJobs, fetchJobStats } from "../../lib/api";
import type { Job, JobStats } from "../../types/jobs";

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

export default function JobsPage() {
  const router = useRouter();
  const [jobs, setJobs] = useState<Job[]>([]);
  const [stats, setStats] = useState<JobStats | null>(null);
  const [date, setDate] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [crewFilter, setCrewFilter] = useState("");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function loadJobs() {
    setLoading(true);
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
      setLoading(false);
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
    revenue: stats?.totalRevenue ?? visibleSummary.revenue
  };

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Schedule</h1>
          <p className="muted">Jobs, route work, and crew assignments.</p>
        </div>
        <div className="topbar-actions">
          <a className="btn btn-secondary" href="http://localhost:3000/dispatch.html">
            Dispatch
          </a>
          <a className="btn btn-secondary" href="http://localhost:3000/import-scheduling.html">
            Legacy Import
          </a>
          <a className="btn btn-primary" href="http://localhost:3000/new-job.html">
            Local Job
          </a>
        </div>
      </header>

      <section className="stats-grid" aria-label="Job stats">
        <StatCard label="Total Jobs" value={summary.total} tone="blue" />
        <StatCard label="Pending" value={summary.pending} tone="amber" />
        <StatCard label="Completed" value={summary.completed} tone="green" />
        <StatCard label="Revenue" value={money(summary.revenue)} tone="purple" />
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

        {loading ? (
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
