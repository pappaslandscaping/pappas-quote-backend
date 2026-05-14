"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  backendUrl,
  fetchCustomer,
  fetchCustomer360,
  fetchCustomerInvoices,
  fetchCustomerJobs,
  fetchCustomerProperties,
  fetchCustomerQuotes,
  generateAiFollowup
} from "../../../lib/api";
import type { Customer360, Customer360TimelineItem } from "../../../types/customer360";
import type {
  Customer,
  CustomerInvoice,
  CustomerJob,
  CustomerProperty,
  CustomerQuote
} from "../../../types/customers";

function customerName(customer: Customer) {
  return (
    customer.name ||
    [customer.first_name, customer.last_name].filter(Boolean).join(" ") ||
    "Unknown"
  );
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

function currency(value?: string | number | null) {
  const amount = Number(value || 0);
  return amount.toLocaleString("en-US", {
    style: "currency",
    currency: "USD"
  });
}

function address(customer: Customer) {
  return [customer.street, customer.street2, customer.city, customer.state, customer.postal_code]
    .filter(Boolean)
    .join(", ");
}

function phoneHref(phone?: string | null) {
  const cleaned = String(phone || "").replace(/\D/g, "");
  return cleaned ? `tel:${cleaned}` : undefined;
}

export default function CustomerDetailPage() {
  const params = useParams<{ id: string }>();
  const customerId = params.id;
  const [customer, setCustomer] = useState<Customer | null>(null);
  const [properties, setProperties] = useState<CustomerProperty[]>([]);
  const [jobs, setJobs] = useState<CustomerJob[]>([]);
  const [quotes, setQuotes] = useState<CustomerQuote[]>([]);
  const [invoices, setInvoices] = useState<CustomerInvoice[]>([]);
  const [customer360, setCustomer360] = useState<Customer360 | null>(null);
  const [customer360Loading, setCustomer360Loading] = useState(true);
  const [customer360Error, setCustomer360Error] = useState("");
  const [draftLoading, setDraftLoading] = useState(false);
  const [draftError, setDraftError] = useState("");
  const [draftText, setDraftText] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadCustomer() {
      setLoading(true);
      setError("");
      try {
        const nextCustomer = await fetchCustomer(customerId);
        setCustomer(nextCustomer);

        const [nextProperties, nextJobs, nextQuotes, nextInvoices] =
          await Promise.all([
            fetchCustomerProperties(customerId),
            fetchCustomerJobs(customerId),
            fetchCustomerQuotes(customerId),
            fetchCustomerInvoices(customerId)
          ]);

        setProperties(nextProperties);
        setJobs(nextJobs);
        setQuotes(nextQuotes);
        setInvoices(nextInvoices);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load customer");
      } finally {
        setLoading(false);
      }
    }

    async function loadCustomer360() {
      setCustomer360Loading(true);
      setCustomer360Error("");
      try {
        const nextCustomer360 = await fetchCustomer360(customerId);
        setCustomer360(nextCustomer360);
      } catch (err) {
        setCustomer360Error(
          err instanceof Error ? err.message : "Failed to load Customer 360"
        );
      } finally {
        setCustomer360Loading(false);
      }
    }

    void loadCustomer();
    void loadCustomer360();
  }, [customerId]);

  const balance = useMemo(
    () =>
      invoices
        .filter((invoice) =>
          ["sent", "viewed", "overdue"].includes(
            String(invoice.status || "").toLowerCase()
          )
        )
        .reduce((sum, invoice) => sum + Number(invoice.total || 0), 0),
    [invoices]
  );

  if (loading) {
    return (
      <main className="app-shell">
        <div className="state-block">Loading customer...</div>
      </main>
    );
  }

  if (error || !customer) {
    return (
      <main className="app-shell">
        <div className="error-panel">
          <h1>Customer not found</h1>
          <p>{error || "The customer record could not be loaded."}</p>
          <Link className="btn btn-primary" href="/customers">
            Back to Customers
          </Link>
        </div>
      </main>
    );
  }

  const name = customerName(customer);
  const phone = customer.phone || customer.mobile || "";

  async function prepareCustomerFollowup() {
    if (!customer) return;

    setDraftLoading(true);
    setDraftError("");
    setDraftText("");

    try {
      const response = await generateAiFollowup({
        type: "customer_followup",
        instructions:
          "Draft a concise, helpful customer follow-up for a landscaping admin. Do not send anything.",
        customer: {
          id: customer.id,
          name,
          email: customer.email,
          phone
        },
        customer360: {
          summary: customer360?.summary,
          recent_timeline: customer360?.timeline.slice(0, 8)
        }
      });
      const text =
        response.draft ||
        response.text ||
        response.message ||
        response.response ||
        "Draft prepared. Review before sending.";
      setDraftText(String(text));
    } catch (err) {
      setDraftError(err instanceof Error ? err.message : "Failed to prepare draft");
    } finally {
      setDraftLoading(false);
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <Link className="back-link" href="/customers">
            Back to Customers
          </Link>
          <p className="eyebrow">Customer 360</p>
          <h1>{name}</h1>
          <p className="muted">
            {customer.customer_number ? `#${customer.customer_number}` : "Complete customer record"}
            {customer.customer_company_name ? ` · ${customer.customer_company_name}` : ""}
          </p>
        </div>
        <div className="topbar-actions">
          {customer.email ? (
            <a className="btn btn-secondary" href={`mailto:${customer.email}`}>
              Email
            </a>
          ) : null}
          {phoneHref(phone) ? (
            <a className="btn btn-primary" href={phoneHref(phone)}>
              Call
            </a>
          ) : null}
        </div>
      </header>

      <section className="customer-summary-band" aria-label="Customer summary">
        <div className="profile-avatar" aria-hidden="true">
          {name.charAt(0).toUpperCase()}
        </div>
        <div>
          <div className="profile-name">{name}</div>
          <div className="profile-meta">
            <span className={`status-pill status-${(customer.status || "inactive").toLowerCase()}`}>
              {customer.status || "-"}
            </span>
            <span className={`type-pill type-${customer.customer_type || "customer"}`}>
              {customer.customer_type === "lead" ? "Lead" : "Customer"}
            </span>
            {customer.tax_exempt ? <span className="tag">Tax Exempt</span> : null}
          </div>
        </div>
        <div className="summary-metrics compact">
          <Metric label="Open Balance" value={currency(customer360?.summary.open_invoice_balance ?? balance)} />
          <Metric label="Active Jobs" value={jobs.filter((job) => !["completed", "done", "cancelled"].includes(String(job.status || "").toLowerCase())).length} />
          <Metric label="Estimates" value={customer360?.summary.quote_count ?? quotes.length} />
          <Metric label="Messages" value={customer360?.summary.communication_count ?? 0} />
        </div>
      </section>

      <section className="table-card customer-story" aria-label="What to know before contacting this customer">
        <div>
          <h2>What to know before contacting this customer</h2>
          <p>
            {customer360?.timeline.length
              ? buildCustomerStory(name, customer360)
              : `${name} has limited activity in YardDesk so far. Confirm contact details, property needs, and whether there is any open estimate or scheduled work before reaching out.`}
          </p>
        </div>
        <div className="sticky-action-bar">
          {phoneHref(phone) ? <a className="quick-action-btn primary" href={phoneHref(phone)}>Call</a> : null}
          {customer.email ? <a className="quick-action-btn" href={`mailto:${customer.email}`}>Email</a> : null}
          <button className="quick-action-btn" type="button" onClick={prepareCustomerFollowup} disabled={draftLoading}>
            AI draft follow-up
          </button>
        </div>
      </section>

      <div className="detail-grid">
        <section className="detail-main">
          <DetailCard title="Chronological Timeline">
            <Customer360Panel
              data={customer360}
              loading={customer360Loading}
              error={customer360Error}
            />
          </DetailCard>

          <DetailCard title="Contact / Property / Services">
            <InfoRow label="Phone">
              {phone && phoneHref(phone) ? <a href={phoneHref(phone)}>{phone}</a> : "-"}
            </InfoRow>
            <InfoRow label="Email">
              {customer.email ? <a href={`mailto:${customer.email}`}>{customer.email}</a> : "-"}
            </InfoRow>
            <InfoRow label="Address">
              {address(customer) ? (
                <a
                  href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(address(customer))}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  {address(customer)}
                </a>
              ) : (
                "-"
              )}
            </InfoRow>
            <InfoRow label="Created">{formatDate(customer.created_at) || "-"}</InfoRow>
            <InfoRow label="Services">
              {[
                ...new Set([
                  ...jobs.map((job) => job.service_type).filter(Boolean),
                  ...quotes.flatMap((quote) =>
                    Array.isArray(quote.services) ? quote.services : quote.services ? [quote.services] : []
                  )
                ])
              ].slice(0, 3).join(", ") || "-"}
            </InfoRow>
          </DetailCard>

          <DetailCard title="Properties">
            <RelatedList
              empty="No properties found."
              items={properties.slice(0, 5).map((property) => ({
                title: property.street || property.property_name || `Property #${property.id}`,
                subtitle: [property.city, property.state, property.zip].filter(Boolean).join(", "),
                meta: property.status || ""
              }))}
            />
          </DetailCard>

          <DetailCard title="Active Jobs">
            <RelatedList
              empty="No active jobs."
              items={jobs.filter((job) => !["completed", "done", "cancelled"].includes(String(job.status || "").toLowerCase())).slice(0, 5).map((job) => ({
                title: job.service_type || `Job #${job.id}`,
                subtitle: [formatDate(job.job_date || job.scheduled_date), job.address]
                  .filter(Boolean)
                  .join(" · "),
                meta: job.status || ""
              }))}
            />
          </DetailCard>

          <DetailCard title="Communications">
            <RelatedList
              empty="No messages, calls, or voicemails found."
              items={(customer360?.records.communications || []).slice(0, 5).map((item) => ({
                title: item.record_type === "call" ? "Call" : "Message",
                subtitle: item.body || item.transcription || formatDate(item.created_at),
                meta: item.status || item.direction || ""
              }))}
            />
          </DetailCard>

          <DetailCard title="Notes">
            <div className={customer.notes ? "notes-content" : "notes-content notes-empty"}>
              {customer.notes || "No notes on this customer."}
            </div>
          </DetailCard>
        </section>

        <aside className="detail-side">
          <DetailCard title="AI Customer Summary">
            <div className="ai-customer-summary">
              <p>
                This summary uses live Customer 360 records. Draft actions are
                manual review only and do not send email, SMS, payments, or job updates.
              </p>
              {customer360 ? (
                <div className="summary-metrics compact">
                  <Metric label="Quotes" value={customer360.summary.quote_count} />
                  <Metric label="Jobs" value={customer360.summary.job_count} />
                  <Metric label="Invoices" value={customer360.summary.invoice_count} />
                  <Metric label="Open Balance" value={currency(customer360.summary.open_invoice_balance)} />
                </div>
              ) : null}
              <button
                className="quick-action-btn primary"
                type="button"
                onClick={prepareCustomerFollowup}
                disabled={draftLoading || customer360Loading}
              >
                {draftLoading ? "Preparing draft..." : "Prepare follow-up draft"}
              </button>
              {draftError ? <div className="inline-error">{draftError}</div> : null}
              {draftText ? (
                <div className="draft-preview">
                  <strong>Draft preview</strong>
                  <p>{draftText}</p>
                  <span>No email or SMS was sent.</span>
                </div>
              ) : null}
            </div>
          </DetailCard>

          <DetailCard title="Billing">
            <InfoRow label="Open Balance">{currency(balance)}</InfoRow>
            <InfoRow label="Invoices">{invoices.length}</InfoRow>
            <InfoRow label="Quotes">{quotes.length}</InfoRow>
          </DetailCard>

          <DetailCard title="Recent Invoices">
            <RelatedList
              empty="No invoices yet."
              items={invoices.slice(0, 4).map((invoice) => ({
                title: `Invoice #${invoice.invoice_number || invoice.id}`,
                subtitle: formatDate(invoice.created_at),
                meta: `${currency(invoice.total)} · ${invoice.status || "draft"}`
              }))}
            />
          </DetailCard>

          <DetailCard title="Quotes / Estimates">
            <RelatedList
              empty="No estimates yet."
              items={quotes.slice(0, 4).map((quote) => ({
                title: `Quote #${quote.quote_number || quote.id}`,
                subtitle: formatDate(quote.created_at),
                meta: `${currency(quote.total)} · ${quote.status || "draft"}`
              }))}
            />
          </DetailCard>

          <DetailCard title="Quick Actions">
            <div className="quick-actions-list">
              <a className="quick-action-btn primary" href={backendUrl(`/quote-generator.html?customer_id=${customer.id}`)}>
                Create Quote
              </a>
              <a className="quick-action-btn" href={backendUrl(`/new-job.html?customer_id=${customer.id}`)}>
                Create Job
              </a>
              <a className="quick-action-btn" href={backendUrl(`/new-invoice.html?customer_id=${customer.id}`)}>
                Create Invoice
              </a>
            </div>
          </DetailCard>
        </aside>
      </div>
    </main>
  );
}

function Customer360Panel({
  data,
  loading,
  error
}: {
  data: Customer360 | null;
  loading: boolean;
  error: string;
}) {
  if (loading) {
    return <div className="empty-list">Loading Customer 360...</div>;
  }

  if (error) {
    return <div className="inline-error">Customer 360 failed: {error}</div>;
  }

  if (!data) {
    return <div className="empty-list">No Customer 360 data found.</div>;
  }

  return (
    <div className="customer-360">
      <div className="source-strip" aria-label="Customer 360 sources">
        {Object.entries(data.sources).map(([key, source]) => (
          <span className={`source-pill source-${source.status}`} key={key}>
            {key}: {source.status}
          </span>
        ))}
      </div>

      <div className="summary-metrics">
        <Metric label="Quotes" value={data.summary.quote_count} />
        <Metric label="Jobs" value={data.summary.job_count} />
        <Metric label="Invoices" value={data.summary.invoice_count} />
        <Metric label="Payments" value={data.summary.payment_count} />
        <Metric label="Messages" value={data.summary.communication_count} />
        <Metric label="Notes" value={data.summary.note_count} />
      </div>

      {data.timeline.length ? (
        <div className="customer-timeline">
          {data.timeline.slice(0, 20).map((item) => (
            <TimelineRow item={item} key={item.id} />
          ))}
        </div>
      ) : (
        <div className="empty-list">No customer activity found yet.</div>
      )}
    </div>
  );
}

function buildCustomerStory(name: string, data: Customer360) {
  const summary = data.summary;
  const latest = data.timeline[0];
  const balance = summary.open_invoice_balance > 0
    ? `Open balance is ${currency(summary.open_invoice_balance)}.`
    : "No open balance is visible.";
  return `${name} has ${summary.quote_count} estimate record${summary.quote_count === 1 ? "" : "s"}, ${summary.job_count} job${summary.job_count === 1 ? "" : "s"}, and ${summary.communication_count} communication touch${summary.communication_count === 1 ? "" : "es"}. Latest activity: ${latest?.title || "none recorded"}. ${balance} Suggested next step: review the timeline, then call or prepare a manual follow-up draft.`;
}

function Metric({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function TimelineRow({ item }: { item: Customer360TimelineItem }) {
  const content = (
    <>
      <div className="timeline-row-main">
        <span className={`type-chip type-${item.type}`}>{item.type}</span>
        <strong>{item.title}</strong>
      </div>
      {item.detail ? <p>{item.detail}</p> : null}
      <div className="timeline-row-meta">
        <span>{formatDate(item.date)}</span>
        {item.status ? <span>{item.status}</span> : null}
        {item.amount ? <span>{currency(item.amount)}</span> : null}
      </div>
    </>
  );

  return (
    <div className="timeline-row">
      <div className="timeline-dot" aria-hidden="true" />
      <div className="timeline-row-body">
        {item.href ? (
          <Link href={item.href} className="timeline-link">
            {content}
          </Link>
        ) : (
          content
        )}
      </div>
    </div>
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

function RelatedList({
  empty,
  items
}: {
  empty: string;
  items: Array<{ title: string; subtitle?: string; meta?: string }>;
}) {
  if (!items.length) {
    return <div className="empty-list">{empty}</div>;
  }

  return (
    <div className="related-list">
      {items.map((item) => (
        <div className="related-item" key={`${item.title}-${item.subtitle}`}>
          <div>
            <div className="strong">{item.title}</div>
            {item.subtitle ? <div className="subtle">{item.subtitle}</div> : null}
          </div>
          {item.meta ? <span>{item.meta}</span> : null}
        </div>
      ))}
    </div>
  );
}
