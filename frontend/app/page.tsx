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
  status: "loading" | "success" | "error";
  total: number | string;
  meta: string;
  detail: string;
};

type DashboardState = {
  quotes: Summary;
  customers: Summary;
  invoices: Summary;
  jobs: Summary;
};

const emptyDashboard: DashboardState = {
  quotes: {
    status: "loading",
    total: "-",
    meta: "Loading",
    detail: "Quote request summary is loading."
  },
  customers: {
    status: "loading",
    total: "-",
    meta: "Loading",
    detail: "Customer summary is loading."
  },
  invoices: {
    status: "loading",
    total: "-",
    meta: "Loading",
    detail: "Invoice summary is loading."
  },
  jobs: {
    status: "loading",
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
    status: "error",
    total: "-",
    meta: "Error",
    detail: `${label} could not be loaded.`,
  };
}

export default function HomePage() {
  const [dashboard, setDashboard] = useState<DashboardState>(emptyDashboard);

  useEffect(() => {
    let isMounted = true;

    function updateSummary(key: keyof DashboardState, summary: Summary) {
      if (!isMounted) return;
      setDashboard((current) => ({
        ...current,
        [key]: summary
      }));
    }

    async function loadQuotes() {
      try {
        const quotes = await fetchQuoteRequests();
        const byStatus = countByStatus(quotes);
        updateSummary("quotes", {
          status: "success",
          total: quotes.length,
          meta: `${byStatus.new || 0} new - ${byStatus.quoted || 0} quoted`,
          detail: `${byStatus.contacted || 0} contacted, ${byStatus.scheduled || 0} scheduled`
        });
      } catch {
        updateSummary("quotes", unavailable("Quotes"));
      }
    }

    async function loadCustomers() {
      const [customerStatsResult, customerPipelineResult] = await Promise.allSettled([
        fetchCustomerStats(),
        fetchCustomerPipelineStats()
      ]);

      if (customerStatsResult.status !== "fulfilled" && customerPipelineResult.status !== "fulfilled") {
        updateSummary("customers", unavailable("Customers"));
        return;
      }

      updateSummary("customers", {
        status: "success",
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
      });
    }

    async function loadInvoices() {
      try {
        const stats = await fetchInvoiceStats();
        updateSummary("invoices", {
          status: "success",
          total: stats.total,
          meta: `${money(stats.outstanding)} outstanding`,
          detail: `${stats.overdue} overdue, ${money(stats.paidThisMonth)} paid this month`
        });
      } catch {
        updateSummary("invoices", unavailable("Invoices"));
      }
    }

    async function loadJobs() {
      try {
        const stats = await fetchJobStats();
        updateSummary("jobs", {
          status: "success",
          total: stats.total,
          meta: `${stats.byStatus.pending || 0} pending - ${stats.byStatus.completed || 0} completed`,
          detail: `${money(stats.totalRevenue)} scheduled revenue`
        });
      } catch {
        updateSummary("jobs", unavailable("Jobs"));
      }
    }

    void loadQuotes();
    void loadCustomers();
    void loadInvoices();
    void loadJobs();

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
    <div className={`stat-card dashboard-stat ${summary.status === "error" ? "has-error" : ""}`}>
      <span>{label}</span>
      <strong>{summary.total}</strong>
      <p>{summary.meta}</p>
      <small>{summary.detail}</small>
      <i className={`stat-dot ${tone}`} aria-hidden="true" />
    </div>
  );
}

function HealthRow({ label, summary }: { label: string; summary: Summary }) {
  const className =
    summary.status === "success"
      ? "status-pill status-completed"
      : summary.status === "error"
        ? "status-pill status-cancelled"
        : "status-pill status-pending";
  const text =
    summary.status === "success"
      ? "Loaded"
      : summary.status === "error"
        ? "Error"
        : "Loading";

  return (
    <div className="dashboard-health-row">
      <span>{label}</span>
      <strong className={className}>{text}</strong>
    </div>
  );
}
