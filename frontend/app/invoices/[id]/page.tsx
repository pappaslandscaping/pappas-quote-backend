"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { backendUrl, fetchInvoice } from "../../../lib/api";
import type { Invoice, InvoiceLineItem } from "../../../types/invoices";

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

function parseLineItems(value: Invoice["line_items"]): InvoiceLineItem[] {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function lineAmount(item: InvoiceLineItem) {
  const quantity = Number(item.quantity || item.qty || 1);
  const rate = Number(item.rate || item.unit_price || item.amount || 0);
  return Number(item.line_total || item.amount || quantity * rate || 0);
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

export default function InvoiceDetailPage() {
  const params = useParams<{ id: string }>();
  const invoiceId = params.id;
  const [invoice, setInvoice] = useState<Invoice | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");

  useEffect(() => {
    async function loadInvoice() {
      setLoading(true);
      setError("");
      try {
        setInvoice(await fetchInvoice(invoiceId));
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load invoice");
      } finally {
        setLoading(false);
      }
    }

    void loadInvoice();
  }, [invoiceId]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(""), 2200);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const lineItems = useMemo(
    () => parseLineItems(invoice?.line_items || []),
    [invoice]
  );

  async function copyPayLink() {
    if (!invoice?.payment_token) return;
    await navigator.clipboard.writeText(
      backendUrl(`/pay-invoice.html?token=${invoice.payment_token}`)
    );
    setToast("Pay link copied");
  }

  if (loading) {
    return (
      <main className="app-shell">
        <div className="state-block">Loading invoice...</div>
      </main>
    );
  }

  if (error || !invoice) {
    return (
      <main className="app-shell">
        <div className="error-panel">
          <h1>Invoice not found</h1>
          <p>{error || "The invoice could not be loaded."}</p>
          <Link className="btn btn-primary" href="/invoices">
            Back to Invoices
          </Link>
        </div>
      </main>
    );
  }

  const status = effectiveStatus(invoice);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <Link className="back-link" href="/invoices">
            Back to Invoices
          </Link>
          <p className="eyebrow">Invoice Details</p>
          <h1>{invoiceNumber(invoice)}</h1>
          <p className="muted">
            {invoice.customer_name || "Unknown Customer"} · {money(invoice.total)}
          </p>
        </div>
        <div className="topbar-actions">
          <a className="btn btn-secondary" href={backendUrl(`/api/invoices/${invoice.id}/pdf`)}>
            Download PDF
          </a>
          <a className="btn btn-secondary" href={backendUrl(`/invoice-detail.html?id=${invoice.id}`)}>
            Legacy View
          </a>
        </div>
      </header>

      {status === "paid" ? (
        <div className="payment-banner success">
          <strong>Payment received</strong>
          <span>{money(invoice.amount_paid || invoice.total)} paid{invoice.paid_at ? ` on ${formatDate(invoice.paid_at)}` : ""}</span>
        </div>
      ) : status === "overdue" ? (
        <div className="payment-banner danger">
          <strong>Invoice overdue</strong>
          <span>{money(balance(invoice))} remaining{invoice.due_date ? ` since ${formatDate(invoice.due_date)}` : ""}</span>
        </div>
      ) : Number(invoice.amount_paid || 0) > 0 ? (
        <div className="payment-banner warning">
          <strong>Partial payment</strong>
          <span>{money(invoice.amount_paid)} paid, {money(balance(invoice))} remaining</span>
        </div>
      ) : null}

      <div className="detail-grid">
        <section className="detail-main">
          <DetailCard title="Customer">
            <InfoRow label="Name">
              {invoice.customer_id ? (
                <Link href={`/customers/${invoice.customer_id}`}>
                  {invoice.customer_name || "Unknown"}
                </Link>
              ) : (
                invoice.customer_name || "Unknown"
              )}
            </InfoRow>
            <InfoRow label="Email">
              {invoice.customer_email ? (
                <a href={`mailto:${invoice.customer_email}`}>{invoice.customer_email}</a>
              ) : (
                "-"
              )}
            </InfoRow>
            <InfoRow label="Address">{invoice.customer_address || "-"}</InfoRow>
          </DetailCard>

          <DetailCard title="Line Items">
            {lineItems.length ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Service</th>
                      <th>Date</th>
                      <th>Qty</th>
                      <th>Rate</th>
                      <th>Amount</th>
                    </tr>
                  </thead>
                  <tbody>
                    {lineItems.map((item, index) => (
                      <tr key={`${item.description || item.name || "item"}-${index}`}>
                        <td>
                          <div className="strong">{item.name || item.description || "Service"}</div>
                          {item.property_name ? <div className="subtle">{item.property_name}</div> : null}
                        </td>
                        <td>{formatDate(item.service_date)}</td>
                        <td>{item.quantity || item.qty || 1}</td>
                        <td>{money(item.rate || item.unit_price || item.amount)}</td>
                        <td>{money(lineAmount(item))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="empty-list">No line items available.</div>
            )}
          </DetailCard>

          {invoice.notes ? (
            <DetailCard title="Notes">
              <div className="notes-content">{invoice.notes}</div>
            </DetailCard>
          ) : null}
        </section>

        <aside className="detail-side">
          <DetailCard title="Invoice Info">
            <InfoRow label="Status">
              <span className={`status-pill status-${status}`}>{status}</span>
            </InfoRow>
            <InfoRow label="Invoice #">{invoiceNumber(invoice)}</InfoRow>
            <InfoRow label="Created">{formatDate(invoice.created_at) || "-"}</InfoRow>
            <InfoRow label="Due">{formatDate(invoice.due_date) || "Upon receipt"}</InfoRow>
            <InfoRow label="Sent">{formatDate(invoice.sent_at) || "-"}</InfoRow>
          </DetailCard>

          <DetailCard title="Payment Summary">
            <InfoRow label="Subtotal">{money(invoice.subtotal)}</InfoRow>
            <InfoRow label="Tax">{money(invoice.tax_amount)}</InfoRow>
            <InfoRow label="Total">{money(invoice.total)}</InfoRow>
            <InfoRow label="Paid">{money(invoice.amount_paid)}</InfoRow>
            <InfoRow label="Balance">{money(balance(invoice))}</InfoRow>
          </DetailCard>

          <DetailCard title="Safe Actions">
            <div className="quick-actions-list">
              <a className="quick-action-btn primary" href={backendUrl(`/api/invoices/${invoice.id}/pdf`)}>
                Download PDF
              </a>
              {invoice.payment_token ? (
                <button className="quick-action-btn" type="button" onClick={copyPayLink}>
                  Copy Pay Link
                </button>
              ) : null}
              <a className="quick-action-btn" href={backendUrl(`/pay-invoice.html${invoice.payment_token ? `?token=${invoice.payment_token}` : ""}`)}>
                Customer Pay Page
              </a>
            </div>
          </DetailCard>

          {invoice.payment_history?.length ? (
            <DetailCard title="Payment History">
              <div className="related-list">
                {invoice.payment_history.slice(0, 5).map((payment) => (
                  <div className="related-item" key={payment.id}>
                    <div>
                      <div className="strong">{payment.method || "Payment"}</div>
                      <div className="subtle">{formatDate(payment.paid_at || payment.created_at)}</div>
                    </div>
                    <span>{money(payment.amount)}</span>
                  </div>
                ))}
              </div>
            </DetailCard>
          ) : null}
        </aside>
      </div>

      {toast ? <div className="toast">{toast}</div> : null}
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
