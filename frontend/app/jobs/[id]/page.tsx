"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import { backendUrl, fetchJob } from "../../../lib/api";
import type { Job } from "../../../types/jobs";

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
  return date.toLocaleString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit"
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

export default function JobDetailPage() {
  const params = useParams<{ id: string }>();
  const jobId = params.id;
  const [job, setJob] = useState<Job | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadJob() {
      setLoading(true);
      setError("");
      try {
        setJob(await fetchJob(jobId));
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load job");
      } finally {
        setLoading(false);
      }
    }

    void loadJob();
  }, [jobId]);

  if (loading) {
    return (
      <main className="app-shell">
        <div className="state-block">Loading job...</div>
      </main>
    );
  }

  if (error || !job) {
    return (
      <main className="app-shell">
        <div className="error-panel">
          <h1>Job not found</h1>
          <p>{error || "The job could not be loaded."}</p>
          <Link className="btn btn-primary" href="/jobs">
            Back to Jobs
          </Link>
        </div>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <Link className="back-link" href="/jobs">
            Back to Jobs
          </Link>
          <p className="eyebrow">Job Details</p>
          <h1>{jobTitle(job)}</h1>
          <p className="muted">
            {job.customer_name || "Unknown Customer"} · {formatDate(job.job_date) || "No date"}
          </p>
        </div>
        <div className="topbar-actions">
          {job.customer_id ? (
            <Link className="btn btn-secondary" href={`/customers/${job.customer_id}`}>
              View Customer
            </Link>
          ) : null}
          {job.invoice_id ? (
            <Link className="btn btn-secondary" href={`/invoices/${job.invoice_id}`}>
              View Invoice
            </Link>
          ) : null}
          <a className="btn btn-secondary" href={backendUrl(`/job-detail.html?id=${job.id}`)}>
            Legacy View
          </a>
        </div>
      </header>

      <div className="customer-profile-card">
        <div className="profile-avatar" aria-hidden="true">
          {String(job.customer_name || jobTitle(job)).charAt(0).toUpperCase()}
        </div>
        <div>
          <div className="profile-name">{job.customer_name || "Unknown Customer"}</div>
          <div className="profile-meta">
            <span className={`status-pill status-${statusClass(job.status)}`}>
              {statusLabel(job.status)}
            </span>
            {job.crew_assigned ? <span className="tag">{job.crew_assigned}</span> : null}
            {job.is_recurring ? <span className="tag">Recurring</span> : null}
          </div>
        </div>
      </div>

      <div className="detail-grid">
        <section className="detail-main">
          <DetailCard title="Job Information">
            <InfoRow label="Customer">
              {job.customer_id ? (
                <Link href={`/customers/${job.customer_id}`}>{job.customer_name || "Customer"}</Link>
              ) : (
                job.customer_name || "-"
              )}
            </InfoRow>
            <InfoRow label="Service">{job.service_type || "-"}</InfoRow>
            <InfoRow label="Address">
              {job.address ? (
                <a
                  href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(job.address)}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  {job.address}
                </a>
              ) : (
                "-"
              )}
            </InfoRow>
            <InfoRow label="Phone">
              {job.phone ? <a href={`tel:${job.phone}`}>{job.phone}</a> : "-"}
            </InfoRow>
          </DetailCard>

          <DetailCard title="Schedule">
            <InfoRow label="Date">{formatDate(job.job_date) || "-"}</InfoRow>
            <InfoRow label="Crew">{job.crew_assigned || "Unassigned"}</InfoRow>
            <InfoRow label="Duration">{job.estimated_duration || 30} min</InfoRow>
            <InfoRow label="Frequency">{job.service_frequency || "-"}</InfoRow>
          </DetailCard>

          <DetailCard title="Notes">
            <div className={job.special_notes || job.property_notes ? "notes-content" : "notes-content notes-empty"}>
              {job.special_notes || job.property_notes ? (
                <>
                  {job.special_notes ? <p>{job.special_notes}</p> : null}
                  {job.property_notes ? <p>{job.property_notes}</p> : null}
                </>
              ) : (
                "No notes on this job."
              )}
            </div>
          </DetailCard>
        </section>

        <aside className="detail-side">
          <DetailCard title="Job Status">
            <InfoRow label="Status">
              <span className={`status-pill status-${statusClass(job.status)}`}>
                {statusLabel(job.status)}
              </span>
            </InfoRow>
            <InfoRow label="Pipeline">{statusLabel(job.pipeline_stage || job.status)}</InfoRow>
            <InfoRow label="Completed">{formatDate(job.completed_at) || "-"}</InfoRow>
            <InfoRow label="Completed By">{job.completed_by || "-"}</InfoRow>
          </DetailCard>

          <DetailCard title="Billing">
            <InfoRow label="Price">{money(job.service_price)}</InfoRow>
            <InfoRow label="Invoice">
              {job.invoice_id ? (
                <Link href={`/invoices/${job.invoice_id}`}>Invoice #{job.invoice_id}</Link>
              ) : (
                "-"
              )}
            </InfoRow>
          </DetailCard>

          <DetailCard title="Safe Actions">
            <div className="quick-actions-list">
              <a className="quick-action-btn primary" href={backendUrl(`/new-job.html?job_id=${job.id}`)}>
                Open Legacy Editor
              </a>
              {job.customer_id ? (
                <Link className="quick-action-btn" href={`/customers/${job.customer_id}`}>
                  View Customer
                </Link>
              ) : null}
              <a className="quick-action-btn" href={backendUrl("/dispatch.html")}>
                Open Dispatch
              </a>
            </div>
          </DetailCard>
        </aside>
      </div>
    </main>
  );
}

function DetailCard({
  title,
  children
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="detail-card">
      <header>
        <h2>{title}</h2>
      </header>
      <div className="detail-card-body">{children}</div>
    </section>
  );
}

function InfoRow({
  label,
  children
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="info-row">
      <span>{label}</span>
      <strong>{children}</strong>
    </div>
  );
}
