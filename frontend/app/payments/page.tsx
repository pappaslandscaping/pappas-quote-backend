"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchInvoiceAging,
  fetchPaymentRecords,
  fetchPaymentReview,
  fetchPayments,
  fetchTaxSweepReport,
  fetchTaxTransferFreshnessStatus,
  fetchTaxTransferInstructions
} from "../../lib/api";
import type { PaymentRow } from "../../types/payments";

type LoadState<T> = { status: "loading" | "success" | "empty" | "error"; data?: T; error?: string };

const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function todayRange() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
  const end = now.toISOString().slice(0, 10);
  return { start_date: start, end_date: end };
}

function money(value?: string | number | null) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function monthValue() {
  return String(new Date().getMonth() + 1);
}

function yearValue() {
  return String(new Date().getFullYear());
}

function formatDate(value?: string | null) {
  if (!value) return "No payment date";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "No payment date";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function amountFor(payment: PaymentRow) {
  return Number(payment.amount ?? payment.amount_paid ?? 0);
}

function paymentDate(payment: PaymentRow) {
  return payment.paid_at || payment.payment_date || payment.created_at || null;
}

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

export default function PaymentsPage() {
  const [search, setSearch] = useState("");
  const [month, setMonth] = useState(monthValue());
  const [year, setYear] = useState(yearValue());
  const [payments, setPayments] = useState<LoadState<PaymentRow[]>>(loading());
  const [records, setRecords] = useState<LoadState<PaymentRow[]>>(loading());
  const [review, setReview] = useState<LoadState<PaymentRow[]>>(loading());
  const [taxSweep, setTaxSweep] = useState<LoadState<Record<string, unknown>>>(loading());
  const [freshness, setFreshness] = useState<LoadState<Record<string, unknown>>>(loading());
  const [instructions, setInstructions] = useState<LoadState<Array<Record<string, unknown>>>>(loading());
  const [aging, setAging] = useState<LoadState<Record<string, unknown>>>(loading());

  useEffect(() => {
    let active = true;
    const range = todayRange();

    async function load<T>(
      request: () => Promise<T>,
      setter: (state: LoadState<T>) => void,
      empty: (data: T) => boolean = () => false
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

    void load(
      async () => (await fetchPayments({ search, month, year, limit: 200 })).payments || [],
      setPayments,
      (data) => data.length === 0
    );
    void load(
      async () => (await fetchPaymentRecords({ search, month, year, limit: 200 })).payments || [],
      setRecords,
      (data) => data.length === 0
    );
    void load(
      async () => (await fetchPaymentReview({ ...range, unresolved_only: "true" })).payments || [],
      setReview,
      (data) => data.length === 0
    );
    void load(async () => await fetchTaxSweepReport(range), setTaxSweep);
    void load(fetchTaxTransferFreshnessStatus, setFreshness);
    void load(
      async () => {
        const data = await fetchTaxTransferInstructions(range);
        return data.instructions || data.transfers || [];
      },
      setInstructions,
      (data) => data.length === 0
    );
    void load(fetchInvoiceAging, setAging);

    return () => {
      active = false;
    };
  }, [month, search, year]);

  const visiblePayments = records.status === "success" || records.status === "empty"
    ? records.data || []
    : payments.data || [];

  const summary = useMemo(() => {
    const collected = visiblePayments.reduce((sum, payment) => sum + amountFor(payment), 0);
    const linked = visiblePayments.filter((payment) => payment.invoice_id).length;
    const unresolved = review.data?.length || 0;
    const taxSummary = asRecord(taxSweep.data?.summary) || asRecord(taxSweep.data);
    const taxPortion = Number(taxSummary?.tax_portion_collected || taxSummary?.tax_due || 0);
    return { collected, linked, unresolved, taxPortion, total: visiblePayments.length };
  }, [review.data, taxSweep.data, visiblePayments]);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Money</p>
          <h1>Payments</h1>
          <p className="muted">Received payments, Copilot review, and tax transfer visibility.</p>
        </div>
        <div className="topbar-actions">
          <Link className="btn btn-secondary" href="/reports">
            Reports
          </Link>
          <Link className="btn btn-primary" href="/invoices">
            Invoices
          </Link>
        </div>
      </header>

      <section className="stats-grid" aria-label="Payment stats">
        <StatCard label="Collected" value={money(summary.collected)} tone="green" />
        <StatCard label="Payments" value={summary.total} tone="blue" />
        <StatCard label="Linked" value={summary.linked} tone="purple" />
        <StatCard label="Needs Review" value={summary.unresolved} tone="amber" />
        <StatCard label="Tax Portion" value={money(summary.taxPortion)} tone="amber" />
      </section>

      <section className="table-card">
        <div className="table-toolbar">
          <div>
            <h2>Payment List</h2>
            <p>Filtered by true payment records when available.</p>
          </div>
          <div className="filters">
            <input aria-label="Search payments" placeholder="Search customer or invoice..." value={search} onChange={(event) => setSearch(event.target.value)} />
            <select aria-label="Filter by month" value={month} onChange={(event) => setMonth(event.target.value)}>
              {Array.from({ length: 12 }, (_, index) => (
                <option value={String(index + 1)} key={index + 1}>{new Date(2026, index, 1).toLocaleString("en-US", { month: "long" })}</option>
              ))}
            </select>
            <input aria-label="Filter by year" value={year} onChange={(event) => setYear(event.target.value)} />
          </div>
        </div>
        <PaymentTable state={records.status === "error" ? payments : records} fallbackRows={visiblePayments} />
      </section>

      <section className="dashboard-grid command-grid">
        <Panel title="Needs Review" state={review} emptyText="No unresolved Copilot payments in this range.">
          {(rows) => <CompactPaymentList rows={rows.slice(0, 6)} />}
        </Panel>
        <section className="table-card dashboard-panel">
          <PanelHeader title="Tax Transfer" subtitle="Visibility only. No transfers are submitted here." />
          <div className="compact-list">
            <InfoRow label="Freshness" state={freshness} />
            <InfoRow label="Tax sweep" state={taxSweep} />
            <InfoRow label="Instructions" state={instructions} />
            <InfoRow label="Aging" state={aging} />
          </div>
        </section>
      </section>
    </main>
  );
}

function PaymentTable({ state, fallbackRows }: { state: LoadState<PaymentRow[]>; fallbackRows: PaymentRow[] }) {
  if (state.status === "loading") return <div className="state-block">Loading payments...</div>;
  if (state.status === "error") return <div className="state-block error">{state.error}</div>;
  const rows = state.data || fallbackRows;
  if (!rows.length) return <div className="empty-state">No payments found.</div>;
  return (
    <div className="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>Customer</th><th>Invoice</th><th>Source</th><th>Amount</th><th>Status</th></tr></thead>
        <tbody>
          {rows.map((payment, index) => (
            <tr key={payment.id || index}>
              <td>{formatDate(paymentDate(payment))}</td>
              <td>{payment.customer_id ? <Link className="row-link" href={`/customers/${payment.customer_id}`}>{payment.customer_name || "Unknown"}</Link> : payment.customer_name || "Unknown"}</td>
              <td>{payment.invoice_id ? <Link className="row-link" href={`/invoices/${payment.invoice_id}`}>{payment.display_invoice_number || payment.invoice_number || payment.invoice_id}</Link> : payment.display_invoice_number || payment.invoice_number || "-"}</td>
              <td>{payment.external_source || payment.method || "database"}</td>
              <td>{money(amountFor(payment))}</td>
              <td><span className={`status-pill status-${String(payment.link_status || payment.status || "paid").toLowerCase()}`}>{payment.link_status || payment.status || "paid"}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CompactPaymentList({ rows }: { rows: PaymentRow[] }) {
  return <div className="compact-list">{rows.map((payment, index) => (
    <div className="compact-row" key={payment.id || index}>
      <div>
        <strong>{payment.customer_name || payment.display_invoice_number || "Unresolved payment"}</strong>
        <span>{payment.current_invoice_match_number ? `Possible invoice ${payment.current_invoice_match_number}` : payment.link_reason || "Needs invoice match"}</span>
      </div>
      <small>{money(amountFor(payment))}</small>
    </div>
  ))}</div>;
}

function Panel<T>({ title, state, emptyText, children }: { title: string; state: LoadState<T>; emptyText: string; children: (data: T) => ReactNode }) {
  return <section className="table-card dashboard-panel" aria-label={title}>
    <PanelHeader title={title} subtitle="Review queue. No automatic changes." />
    {state.status === "loading" ? <div className="state-block">Loading</div> : null}
    {state.status === "error" ? <div className="state-block error">API failed: {state.error}</div> : null}
    {state.status === "empty" ? <div className="empty-state">{emptyText}</div> : null}
    {state.status === "success" ? children(state.data as T) : null}
  </section>;
}

function PanelHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return <div className="table-toolbar"><div><h2>{title}</h2><p>{subtitle}</p></div></div>;
}

function InfoRow({ label, state }: { label: string; state: LoadState<unknown> }) {
  return <div className="compact-row"><strong>{label}</strong><span className={`status-pill status-${state.status === "success" ? "completed" : state.status === "error" ? "cancelled" : state.status}`}>{state.status}</span></div>;
}

function StatCard({ label, value, tone }: { label: string; value: string | number; tone: "blue" | "amber" | "green" | "purple" }) {
  return <div className="stat-card"><span>{label}</span><strong>{value}</strong><i className={`stat-dot ${tone}`} aria-hidden="true" /></div>;
}

function asRecord(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}
