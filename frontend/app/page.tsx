"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  fetchCustomerPipelineStats,
  fetchCustomerStats,
  fetchInvoiceStats,
  fetchJobStats,
  fetchQuoteRequests
} from "../lib/api";

type Summary = {
  total: number | string;
  meta: string;
  detail: string;
  error?: string;
};

type DashboardState = {
  quotes: Summary;
  customers: Summary;
  invoices: Summary;
  jobs: Summary;
};

const emptyDashboard: DashboardState = {
  quotes: {
    total: "-",
    meta: "Loading",
    detail: "Quote request summary is loading."
  },
  customers: {
    total: "-",
    meta: "Loading",
    detail: "Customer summary is loading."
  },
  invoices: {
    total: "-",
    meta: "Loading",
    detail: "Invoice summary is loading."
  },
  jobs: {
    total: "-",
    meta: "Loading",
    detail: "Scheduling summary is loading."
  }
};

function money(value?: string | number | null) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  });
}

function countByStatus<T extends { status?: string | null }>(items: T[]) {
  return items.reduce<Record<string, number>>((acc, item) => {
    const status = String(item.status || "unknown").toLowerCase();
    acc[status] = (acc[status] || 0) + 1;
    return acc;
  }, {});
}

function unavailable(label: string): Summary {
  return {
    total: "-",
    meta: "Unavailable",
    detail: `${label} could not be loaded.`,
    error: "Unavailable"
  };
}

export default function HomePage() {
  const [dashboard, setDashboard] = useState<DashboardState>(emptyDashboard);

  useEffect(() => {
    let isMounted = true;

    async function loadDashboard() {
      const [quotesResult, customerStatsResult, customerPipelineResult, invoiceStatsResult, jobStatsResult] =
        await Promise.allSettled([
          fetchQuoteRequests(),
          fetchCustomerStats(),
          fetchCustomerPipelineStats(),
          fetchInvoiceStats(),
          fetchJobStats()
        ]);

      if (!isMounted) return;

      const quotes =
        quotesResult.status === "fulfilled"
          ? (() => {
              const byStatus = countByStatus(quotesResult.value);
              return {
                total: quotesResult.value.length,
                meta: `${byStatus.new || 0} new - ${byStatus.quoted || 0} quoted`,
                detail: `${byStatus.contacted || 0} contacted, ${byStatus.scheduled || 0} scheduled`
              };
            })()
          : unavailable("Quotes");

      const customers =
        customerStatsResult.status === "fulfilled" || customerPipelineResult.status === "fulfilled"
          ? {
              total:
                customerPipelineResult.status === "fulfilled"
                  ? customerPipelineResult.value.totalCustomers
                  : customerStatsResult.status === "fulfilled"
                    ? customerStatsResult.value.total
                    : "-",
              meta:
                customerPipelineResult.status === "fulfilled"
                  ? `${customerPipelineResult.value.totalLeads} leads`
                  : `${customerStatsResult.status === "fulfilled" ? customerStatsResult.value.active : 0} active`,
              detail:
                customerPipelineResult.status === "fulfilled"
                  ? `${customerPipelineResult.value.conversionRate}% conversion, ${customerPipelineResult.value.newLeadsThisMonth} new leads this month`
                  : `${customerStatsResult.status === "fulfilled" ? customerStatsResult.value.inactive : 0} inactive customers`
            }
          : unavailable("Customers");

      const invoices =
        invoiceStatsResult.status === "fulfilled"
          ? {
              total: invoiceStatsResult.value.total,
              meta: `${money(invoiceStatsResult.value.outstanding)} outstanding`,
              detail: `${invoiceStatsResult.value.overdue} overdue, ${money(invoiceStatsResult.value.paidThisMonth)} paid this month`
            }
          : unavailable("Invoices");

      const jobs =
        jobStatsResult.status === "fulfilled"
          ? {
              total: jobStatsResult.value.total,
              meta: `${jobStatsResult.value.byStatus.pending || 0} pending - ${jobStatsResult.value.byStatus.completed || 0} completed`,
              detail: `${money(jobStatsResult.value.totalRevenue)} scheduled revenue`
            }
          : unavailable("Jobs");

      setDashboard({
        quotes,
        customers,
        invoices,
        jobs
      });
    }

    void loadDashboard();

    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Business Dashboard</h1>
          <p className="muted">High-level snapshots from the existing backend APIs.</p>
        </div>
      </header>

      <section className="stats-grid" aria-label="Business snapshots">
        <DashboardCard label="Quotes" summary={dashboard.quotes} tone="blue" />
        <DashboardCard label="Customers" summary={dashboard.customers} tone="green" />
        <DashboardCard label="Invoices" summary={dashboard.invoices} tone="amber" />
        <DashboardCard label="Scheduling/Jobs" summary={dashboard.jobs} tone="purple" />
      </section>

      <section className="dashboard-grid">
        <section className="table-card dashboard-panel" aria-label="Dashboard quick links">
          <div className="table-toolbar">
            <div>
              <h2>Quick Links</h2>
              <p>Open the converted React work areas.</p>
            </div>
          </div>
          <div className="quick-links-grid">
            <Link className="quick-action-btn primary" href="/quotes">
              Quotes
            </Link>
            <Link className="quick-action-btn" href="/customers">
              Customers
            </Link>
            <Link className="quick-action-btn" href="/invoices">
              Invoices
            </Link>
            <Link className="quick-action-btn" href="/jobs">
              Scheduling/Jobs
            </Link>
          </div>
        </section>

        <section className="table-card dashboard-panel" aria-label="Dashboard health">
          <div className="table-toolbar">
            <div>
              <h2>API Health</h2>
              <p>Each summary loads independently.</p>
            </div>
          </div>
          <div className="dashboard-health-list">
            <HealthRow label="Quotes" summary={dashboard.quotes} />
            <HealthRow label="Customers" summary={dashboard.customers} />
            <HealthRow label="Invoices" summary={dashboard.invoices} />
            <HealthRow label="Scheduling/Jobs" summary={dashboard.jobs} />
          </div>
        </section>
      </section>
    </main>
  );
}

function DashboardCard({
  label,
  summary,
  tone
}: {
  label: string;
  summary: Summary;
  tone: "blue" | "amber" | "green" | "purple";
}) {
  return (
    <div className={`stat-card dashboard-stat ${summary.error ? "has-error" : ""}`}>
      <span>{label}</span>
      <strong>{summary.total}</strong>
      <p>{summary.meta}</p>
      <small>{summary.detail}</small>
      <i className={`stat-dot ${tone}`} aria-hidden="true" />
    </div>
  );
}

function HealthRow({ label, summary }: { label: string; summary: Summary }) {
  return (
    <div className="dashboard-health-row">
      <span>{label}</span>
      <strong className={summary.error ? "status-pill status-cancelled" : "status-pill status-completed"}>
        {summary.error ? "Unavailable" : "Loaded"}
      </strong>
    </div>
  );
}
