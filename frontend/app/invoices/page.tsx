"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { fetchInvoices, fetchInvoiceStats } from "../../lib/api";
import type { Invoice, InvoiceStats } from "../../types/invoices";

const STATUS_OPTIONS = [
  "draft",
  "pending",
  "partial",
  "sent",
  "paid",
  "overdue",
  "void"
];

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
    month: "short",
    day: "numeric",
    year: "numeric"
  });
}

function invoiceNumber(invoice: Invoice) {
  return (
    invoice.display_invoice_number ||
    invoice.invoice_number ||
    `INV-${invoice.id}`
  );
}

function balance(invoice: Invoice) {
  return Math.max(Number(invoice.total || 0) - Number(invoice.amount_paid || 0), 0);
}

function effectiveStatus(invoice: Invoice) {
  const status = String(invoice.status || "draft").toLowerCase();
  if (
    status !== "paid" &&
    status !== "void" &&
    invoice.due_date &&
    new Date(invoice.due_date) < new Date() &&
    balance(invoice) > 0
  ) {
    return "overdue";
  }
  return status;
}

export default function InvoicesPage() {
  const router = useRouter();
  const [invoices, setInvoices] = useState<Invoice[]>([]);
  const [stats, setStats] = useState<InvoiceStats | null>(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function loadInvoices() {
    setLoading(true);
    setError("");
    try {
      const nextInvoices = await fetchInvoices({
        limit: 500,
        status: statusFilter,
        search
      });
      setInvoices(nextInvoices);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load invoices");
    } finally {
      setLoading(false);
    }
  }

  async function loadStats() {
    try {
      setStats(await fetchInvoiceStats());
    } catch {
      setStats(null);
    }
  }

  useEffect(() => {
    void loadStats();
  }, []);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      void loadInvoices();
    }, 250);

    return () => window.clearTimeout(timeout);
  }, [search, statusFilter]);

  const summary = useMemo(() => {
    const accountStanding = stats?.account_standing;
    return {
      pastDue: accountStanding?.past_due ?? stats?.overdue ?? 0,
      outstanding: accountStanding?.outstanding ?? stats?.outstanding ?? 0,
      paid: accountStanding?.paid ?? stats?.paid ?? 0,
      credit: accountStanding?.credit ?? 0
    };
  }, [stats]);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Invoices</h1>
          <p className="muted">Receivables, payment status, and collections workflow.</p>
        </div>
        <div className="topbar-actions">
          <a className="btn btn-secondary" href="http://localhost:3000/payments.html">
            Payments
          </a>
          <a className="btn btn-secondary" href="http://localhost:3000/reports.html">
            Reports
          </a>
          <a className="btn btn-primary" href="http://localhost:3000/new-invoice.html">
            New Invoice
          </a>
        </div>
      </header>

      <section className="stats-grid" aria-label="Invoice stats">
        <StatCard label="Past Due" value={String(summary.pastDue)} tone="amber" />
        <StatCard label="Outstanding" value={money(summary.outstanding)} tone="blue" />
        <StatCard label="Credit" value={String(summary.credit)} tone="purple" />
        <StatCard label="Paid" value={String(summary.paid)} tone="green" />
      </section>

      <section className="table-card">
        <div className="table-toolbar">
          <div>
            <h2>All Invoices</h2>
            <p>{invoices.length} visible</p>
          </div>
          <div className="filters">
            <select
              aria-label="Filter by status"
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
            >
              <option value="">All Statuses</option>
              {STATUS_OPTIONS.map((status) => (
                <option key={status} value={status}>
                  {status.charAt(0).toUpperCase() + status.slice(1)}
                </option>
              ))}
            </select>
            <input
              aria-label="Search invoices"
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search by invoice # or customer..."
            />
          </div>
        </div>

        {loading ? (
          <div className="state-block">Loading invoices...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Invoice #</th>
                  <th>Customer</th>
                  <th>Amount</th>
                  <th>Balance</th>
                  <th>Status</th>
                  <th>Due / Created</th>
                </tr>
              </thead>
              <tbody>
                {invoices.length ? (
                  invoices.map((invoice) => (
                    <tr
                      className="clickable-row"
                      key={invoice.id}
                      onClick={() => router.push(`/invoices/${invoice.id}`)}
                    >
                      <td>
                        <Link
                          className="row-link"
                          href={`/invoices/${invoice.id}`}
                          onClick={(event) => event.stopPropagation()}
                        >
                          {invoiceNumber(invoice)}
                        </Link>
                      </td>
                      <td>
                        <div className="strong">{invoice.customer_name || "Unknown"}</div>
                        <div className="subtle">{invoice.customer_email || ""}</div>
                      </td>
                      <td>{money(invoice.total)}</td>
                      <td>{money(balance(invoice))}</td>
                      <td>
                        <span className={`status-pill status-${effectiveStatus(invoice)}`}>
                          {effectiveStatus(invoice)}
                        </span>
                      </td>
                      <td>
                        <div>{invoice.due_date ? `Due ${formatDate(invoice.due_date)}` : "Due upon receipt"}</div>
                        <div className="subtle">{formatDate(invoice.created_at)}</div>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={6}>
                      <div className="empty-state">No invoices found</div>
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
