"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  fetchCustomer,
  fetchCustomerInvoices,
  fetchCustomerJobs,
  fetchCustomerProperties,
  fetchCustomerQuotes
} from "../../../lib/api";
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

    void loadCustomer();
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

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <Link className="back-link" href="/customers">
            Back to Customers
          </Link>
          <p className="eyebrow">Client Details</p>
          <h1>{name}</h1>
          <p className="muted">
            {customer.customer_number ? `#${customer.customer_number}` : "Customer record"}
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

      <div className="customer-profile-card">
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
      </div>

      <div className="detail-grid">
        <section className="detail-main">
          <DetailCard title="Contact Information">
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

          <DetailCard title="Recent Jobs">
            <RelatedList
              empty="No jobs scheduled."
              items={jobs.slice(0, 5).map((job) => ({
                title: job.service_type || `Job #${job.id}`,
                subtitle: [formatDate(job.job_date || job.scheduled_date), job.address]
                  .filter(Boolean)
                  .join(" · "),
                meta: job.status || ""
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

          <DetailCard title="Recent Quotes">
            <RelatedList
              empty="No quotes yet."
              items={quotes.slice(0, 4).map((quote) => ({
                title: `Quote #${quote.quote_number || quote.id}`,
                subtitle: formatDate(quote.created_at),
                meta: `${currency(quote.total)} · ${quote.status || "draft"}`
              }))}
            />
          </DetailCard>

          <DetailCard title="Quick Actions">
            <div className="quick-actions-list">
              <a className="quick-action-btn primary" href={`http://localhost:3000/quote-generator.html?customer_id=${customer.id}`}>
                Create Quote
              </a>
              <a className="quick-action-btn" href={`http://localhost:3000/new-job.html?customer_id=${customer.id}`}>
                Create Job
              </a>
              <a className="quick-action-btn" href={`http://localhost:3000/new-invoice.html?customer_id=${customer.id}`}>
                Create Invoice
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
