"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import {
  fetchBusinessSummary,
  fetchCashFlowForecast,
  fetchCrewPerformanceReport,
  fetchCustomerAcquisitionReport,
  fetchCustomerValueReport,
  fetchFinanceSummary,
  fetchInvoiceAging,
  fetchJobCostingReport,
  fetchKpiDashboard,
  fetchSalesTaxReport,
  fetchTaxSweepReport
} from "../../lib/api";
import type {
  BusinessSummary,
  CashFlowForecastResponse,
  CrewPerformanceRow,
  CustomerValueRow,
  InvoiceAgingResponse,
  JobCostingRow
} from "../../types/reports";

type Tab = "Money" | "Sales Pipeline" | "Operations" | "Customers" | "Tax/Finance";
type LoadStatus = "loading" | "success" | "empty" | "error";
type LoadState<T> = { status: LoadStatus; data?: T; error?: string };

const tabs: Tab[] = ["Money", "Sales Pipeline", "Operations", "Customers", "Tax/Finance"];
const loading = <T,>(): LoadState<T> => ({ status: "loading" });

function money(value?: string | number | null) {
  return Number(value || 0).toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  });
}

function shortError(error: unknown) {
  return error instanceof Error ? error.message : "API request failed";
}

function monthRange() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);
  const fmt = (date: Date) => date.toISOString().slice(0, 10);
  return { start_date: fmt(start), end_date: fmt(now) };
}

export default function ReportsPage() {
  const [activeTab, setActiveTab] = useState<Tab>("Money");
  const [business, setBusiness] = useState<LoadState<BusinessSummary>>(loading());
  const [kpi, setKpi] = useState<LoadState<Record<string, unknown>>>(loading());
  const [finance, setFinance] = useState<LoadState<Record<string, unknown>>>(loading());
  const [forecast, setForecast] = useState<LoadState<CashFlowForecastResponse>>(loading());
  const [jobCosting, setJobCosting] = useState<LoadState<JobCostingRow[]>>(loading());
  const [customerValue, setCustomerValue] = useState<LoadState<CustomerValueRow[]>>(loading());
  const [crew, setCrew] = useState<LoadState<CrewPerformanceRow[]>>(loading());
  const [acquisition, setAcquisition] = useState<LoadState<Array<Record<string, unknown>>>>(loading());
  const [salesTax, setSalesTax] = useState<LoadState<Record<string, unknown>>>(loading());
  const [taxSweep, setTaxSweep] = useState<LoadState<Record<string, unknown>>>(loading());
  const [aging, setAging] = useState<LoadState<InvoiceAgingResponse>>(loading());

  useEffect(() => {
    let active = true;
    const range = monthRange();

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

    void load(fetchBusinessSummary, setBusiness);
    void load(fetchKpiDashboard, setKpi);
    void load(fetchFinanceSummary, setFinance);
    void load(fetchCashFlowForecast, setForecast);
    void load(fetchJobCostingReport, setJobCosting, (data) => data.length === 0);
    void load(fetchCustomerValueReport, setCustomerValue, (data) => data.length === 0);
    void load(fetchCrewPerformanceReport, setCrew, (data) => data.length === 0);
    void load(fetchCustomerAcquisitionReport, setAcquisition, (data) => data.length === 0);
    void load(() => fetchSalesTaxReport(range), setSalesTax);
    void load(() => fetchTaxSweepReport(range), setTaxSweep);
    void load(fetchInvoiceAging, setAging);

    return () => {
      active = false;
    };
  }, []);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Reports</p>
          <h1>Reporting Hub</h1>
          <p className="muted">Business reports load independently from existing APIs.</p>
        </div>
        <div className="topbar-actions">
          <Link className="btn btn-secondary" href="/invoices">
            View Invoices
          </Link>
          <Link className="btn btn-primary" href="/jobs">
            View Jobs
          </Link>
        </div>
      </header>

      <nav className="segmented-tabs report-tabs" aria-label="Report tabs">
        {tabs.map((tab) => (
          <button
            className={activeTab === tab ? "active" : ""}
            key={tab}
            type="button"
            onClick={() => setActiveTab(tab)}
          >
            {tab}
          </button>
        ))}
      </nav>

      {activeTab === "Money" ? (
        <ReportGrid>
          <MetricPanel title="Money Summary" state={business}>
            {(data) => (
              <div className="stats-grid report-metrics">
                <MiniMetric label="Revenue" value={money(data.revenue)} />
                <MiniMetric label="Outstanding" value={money(data.outstanding)} />
                <MiniMetric label="Profit" value={money(data.profit)} />
                <MiniMetric label="Invoices" value={data.invoicesTotal || 0} />
              </div>
            )}
          </MetricPanel>
          <MetricPanel title="Cash Forecast" state={forecast}>
            {(data) => (
              <div className="compact-list">
                <MiniMetric label="Expected inflow" value={money(data.total_expected_inflow)} />
                <MiniMetric label="Monthly expenses" value={money(data.monthly_expense_avg)} />
              </div>
            )}
          </MetricPanel>
          <MetricPanel title="Aging" state={aging}>
            {(data) => <SimpleObjectTable rows={data.buckets || []} />}
          </MetricPanel>
        </ReportGrid>
      ) : null}

      {activeTab === "Sales Pipeline" ? (
        <ReportGrid>
          <MetricPanel title="Pipeline Summary" state={business}>
            {(data) => (
              <div className="stats-grid report-metrics">
                <MiniMetric label="Quotes sent" value={data.quotesSent || 0} />
                <MiniMetric label="Quotes signed" value={data.quotesSigned || 0} />
                <MiniMetric label="Close rate" value={`${data.conversionRate || 0}%`} />
                <MiniMetric label="New customers" value={data.newCustomers || 0} />
              </div>
            )}
          </MetricPanel>
          <MetricPanel title="KPI Dashboard" state={kpi}>
            {(data) => <SimpleObjectTable rows={[asRecord(data.metrics) || data]} />}
          </MetricPanel>
          <QuickPanel title="Pipeline Links">
            <Link className="quick-action-btn" href="/quotes">
              Open quote pipeline
            </Link>
            <Link className="quick-action-btn" href="/customers">
              Open customers
            </Link>
          </QuickPanel>
        </ReportGrid>
      ) : null}

      {activeTab === "Operations" ? (
        <ReportGrid>
          <MetricPanel title="Operations Summary" state={business}>
            {(data) => (
              <div className="stats-grid report-metrics">
                <MiniMetric label="Jobs total" value={data.jobsTotal || 0} />
                <MiniMetric label="Completed" value={data.jobsCompleted || 0} />
                <MiniMetric label="Job revenue" value={money(data.revenue)} />
                <MiniMetric label="Expenses" value={money(data.expenses)} />
              </div>
            )}
          </MetricPanel>
          <MetricPanel title="Job Costing" state={jobCosting}>
            {(rows) => <SimpleObjectTable rows={rows.slice(0, 8)} />}
          </MetricPanel>
          <MetricPanel title="Crew Performance" state={crew}>
            {(rows) => <SimpleObjectTable rows={rows.slice(0, 8)} />}
          </MetricPanel>
        </ReportGrid>
      ) : null}

      {activeTab === "Customers" ? (
        <ReportGrid>
          <MetricPanel title="Customer Value" state={customerValue}>
            {(rows) => <SimpleObjectTable rows={rows.slice(0, 8)} />}
          </MetricPanel>
          <MetricPanel title="Customer Acquisition" state={acquisition}>
            {(rows) => <SimpleObjectTable rows={rows.slice(0, 8)} />}
          </MetricPanel>
          <QuickPanel title="Customer Links">
            <Link className="quick-action-btn" href="/customers">
              Customer list
            </Link>
            <Link className="quick-action-btn" href="/ai">
              Churn risk suggestions
            </Link>
          </QuickPanel>
        </ReportGrid>
      ) : null}

      {activeTab === "Tax/Finance" ? (
        <ReportGrid>
          <MetricPanel title="Sales Tax" state={salesTax}>
            {(data) => <SimpleObjectTable rows={[asRecord(data.summary) || data]} />}
          </MetricPanel>
          <MetricPanel title="Tax Sweep" state={taxSweep}>
            {(data) => <SimpleObjectTable rows={[asRecord(data.summary) || data]} />}
          </MetricPanel>
          <MetricPanel title="Finance Summary" state={finance}>
            {(data) => <SimpleObjectTable rows={[data]} />}
          </MetricPanel>
        </ReportGrid>
      ) : null}
    </main>
  );
}

function ReportGrid({ children }: { children: ReactNode }) {
  return <section className="report-grid">{children}</section>;
}

function QuickPanel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="table-card dashboard-panel">
      <PanelHeader title={title} />
      <div className="quick-links-grid quick-links-vertical">{children}</div>
    </section>
  );
}

function MetricPanel<T>({
  title,
  state,
  children
}: {
  title: string;
  state: LoadState<T>;
  children: (data: T) => ReactNode;
}) {
  return (
    <section className="table-card dashboard-panel" aria-label={title}>
      <PanelHeader title={title} />
      {state.status === "loading" ? <div className="state-block">Loading</div> : null}
      {state.status === "error" ? (
        <div className="state-block error">API failed: {state.error}</div>
      ) : null}
      {state.status === "empty" ? <div className="empty-state">Nothing to show.</div> : null}
      {state.status === "success" ? children(state.data as T) : null}
    </section>
  );
}

function PanelHeader({ title }: { title: string }) {
  return (
    <div className="table-toolbar">
      <div>
        <h2>{title}</h2>
        <p>Loaded independently.</p>
      </div>
    </div>
  );
}

function MiniMetric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="stat-card mini-metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function SimpleObjectTable({ rows }: { rows: Array<Record<string, unknown>> }) {
  const safeRows = rows.filter((row) => row && typeof row === "object");
  if (!safeRows.length) return <div className="empty-state">Nothing to show.</div>;
  const columns = Object.keys(safeRows[0]).slice(0, 5);

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column}>{column.replaceAll("_", " ")}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {safeRows.map((row, index) => (
            <tr key={index}>
              {columns.map((column) => (
                <td key={column}>{formatCell(row[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(value: unknown) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function asRecord(value: unknown) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}
