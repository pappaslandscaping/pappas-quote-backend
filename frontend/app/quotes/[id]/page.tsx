"use client";

import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  deleteQuoteRequest,
  fetchQuoteRequest,
  updateQuoteStatus
} from "../../../lib/api";
import type { QuoteRequest } from "../../../types/quotes";

const STATUS_OPTIONS = [
  "new",
  "contacted",
  "quoted",
  "scheduled",
  "completed",
  "cancelled"
];

const PACKAGE_LABELS: Record<string, string> = {
  essential: "Essential Care",
  complete: "Complete Care",
  premium: "Premium Care"
};

function normalizeQuestions(quote: QuoteRequest) {
  if (!quote.questions) return {};
  if (typeof quote.questions === "string") {
    try {
      return JSON.parse(quote.questions) as Record<string, string>;
    } catch {
      return {};
    }
  }
  return quote.questions;
}

function normalizeServices(quote: QuoteRequest) {
  if (!quote.services) return [];
  if (Array.isArray(quote.services)) return quote.services;
  try {
    const parsed = JSON.parse(quote.services);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return quote.services
      .split(",")
      .map((service) => service.trim())
      .filter(Boolean);
  }
}

function capitalize(value?: string | null) {
  if (!value) return "";
  return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
}

function formatDateTime(value?: string | null) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit"
  });
}

function phoneHref(phone?: string | null) {
  const cleaned = String(phone || "").replace(/\D/g, "");
  return cleaned ? `tel:${cleaned}` : undefined;
}

export default function QuoteDetailPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const quoteId = params.id;
  const [quote, setQuote] = useState<QuoteRequest | null>(null);
  const [selectedStatus, setSelectedStatus] = useState("new");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");

  async function loadQuote() {
    setLoading(true);
    setError("");
    try {
      const nextQuote = await fetchQuoteRequest(quoteId);
      setQuote(nextQuote);
      setSelectedStatus(nextQuote.status || "new");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load quote");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadQuote();
  }, [quoteId]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(""), 2500);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const details = useMemo(() => {
    if (!quote) return null;
    const questions = normalizeQuestions(quote);
    return {
      questions,
      gate: questions.gate || "",
      dogs: questions.dogs || "",
      overgrown: questions.lawnHeight || questions.overgrown || "",
      backyardAccess: questions.backyardAccess || questions.backyard_access || "",
      contactMethod: questions.contactMethod || questions.contact_method || "",
      startTiming: questions.startTime || questions.start_timing || "",
      services: normalizeServices(quote),
      packageName: quote.package
        ? PACKAGE_LABELS[quote.package] || quote.package
        : "Individual Services"
    };
  }, [quote]);

  async function handleStatusUpdate() {
    setSaving(true);
    try {
      const updated = await updateQuoteStatus(quoteId, selectedStatus);
      setQuote(updated);
      setToast("Status updated");
    } catch (err) {
      setToast(err instanceof Error ? err.message : "Failed to update status");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm("Are you sure you want to delete this quote request? This cannot be undone.")) {
      return;
    }

    setSaving(true);
    try {
      await deleteQuoteRequest(quoteId);
      setToast("Quote deleted");
      window.setTimeout(() => router.push("/quotes"), 700);
    } catch (err) {
      setToast(err instanceof Error ? err.message : "Failed to delete quote");
      setSaving(false);
    }
  }

  if (loading) {
    return (
      <main className="app-shell">
        <div className="state-block">Loading quote details...</div>
      </main>
    );
  }

  if (error || !quote || !details) {
    return (
      <main className="app-shell">
        <div className="error-panel">
          <h1>Quote not found</h1>
          <p>{error || "The quote request could not be loaded."}</p>
          <Link className="btn btn-primary" href="/quotes">
            Back to Quotes
          </Link>
        </div>
      </main>
    );
  }

  const generatorUrl =
    `http://localhost:3000/quote-generator.html?from_request=${quote.id}` +
    `&name=${encodeURIComponent(quote.name || "")}` +
    `&email=${encodeURIComponent(quote.email || "")}` +
    `&phone=${encodeURIComponent(quote.phone || "")}` +
    `&address=${encodeURIComponent(quote.address || "")}` +
    `&package=${encodeURIComponent(quote.package || "")}` +
    `&services=${encodeURIComponent(JSON.stringify(details.services))}`;
  const calculatorUrl =
    `http://localhost:3000/quote-calculator.html?name=${encodeURIComponent(quote.name || "")}` +
    `&address=${encodeURIComponent(quote.address || "")}`;

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <Link className="back-link" href="/quotes">
            Back to Quotes
          </Link>
          <p className="eyebrow">Quote Details</p>
          <h1>{quote.name || "Unknown Customer"}</h1>
          <p className="muted">Submitted {formatDateTime(quote.created_at)}</p>
        </div>
        <div className="topbar-actions">
          {quote.email ? (
            <a className="btn btn-secondary" href={`mailto:${quote.email}`}>
              Email
            </a>
          ) : null}
          {phoneHref(quote.phone) ? (
            <a className="btn btn-primary" href={phoneHref(quote.phone)}>
              Call
            </a>
          ) : null}
        </div>
      </header>

      <div className="detail-grid">
        <section className="detail-main">
          <DetailCard title="Contact Information">
            <InfoRow label="Status">
              <span className={`status-pill status-${quote.status || "new"}`}>
                {quote.status || "new"}
              </span>
            </InfoRow>
            <InfoRow label="Phone">
              {quote.phone && phoneHref(quote.phone) ? (
                <a href={phoneHref(quote.phone)}>{quote.phone}</a>
              ) : (
                "—"
              )}
            </InfoRow>
            <InfoRow label="Email">
              {quote.email ? <a href={`mailto:${quote.email}`}>{quote.email}</a> : "—"}
            </InfoRow>
            <InfoRow label="Address">
              {quote.address ? (
                <a
                  href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(quote.address)}`}
                  target="_blank"
                  rel="noreferrer"
                >
                  {quote.address}
                </a>
              ) : (
                "—"
              )}
            </InfoRow>
            <InfoRow label="Source">{quote.source || "—"}</InfoRow>
          </DetailCard>

          <DetailCard title="Services Requested">
            <div className="service-list">
              <div className="service-item">
                <span>{details.packageName}</span>
                {quote.package ? <strong>Package</strong> : null}
              </div>
              {details.services.map((service) => (
                <div className="service-item" key={service}>
                  <span>{service}</span>
                </div>
              ))}
            </div>
          </DetailCard>

          <DetailCard title="Property Details">
            <div className="property-tags">
              <PropertyTag label="Gate" value={details.gate} />
              <PropertyTag label="Dogs" value={details.dogs} />
              <PropertyTag label={'Overgrown (6"+)'} value={details.overgrown} />
              <PropertyTag label="Backyard Access" value={details.backyardAccess} />
            </div>
          </DetailCard>

          <DetailCard title="Preferences">
            <InfoRow label="Contact Via">{capitalize(details.contactMethod) || "—"}</InfoRow>
            <InfoRow label="Start Timing">{capitalize(details.startTiming) || "—"}</InfoRow>
          </DetailCard>

          <DetailCard title="Notes">
            <div className={quote.notes ? "notes-content" : "notes-content notes-empty"}>
              {quote.notes || "No additional notes provided"}
            </div>
          </DetailCard>
        </section>

        <aside className="detail-side">
          <DetailCard title="Update Status">
            <div className="status-update">
              <select
                value={selectedStatus}
                onChange={(event) => setSelectedStatus(event.target.value)}
                aria-label="Quote status"
              >
                {STATUS_OPTIONS.map((status) => (
                  <option key={status} value={status}>
                    {capitalize(status)}
                  </option>
                ))}
              </select>
              <button className="btn btn-primary" type="button" onClick={handleStatusUpdate} disabled={saving}>
                {saving ? "Saving..." : "Update"}
              </button>
            </div>
          </DetailCard>

          <DetailCard title="Quick Actions">
            <div className="quick-actions-list">
              <a className="quick-action-btn primary" href={generatorUrl}>
                Create Quote
              </a>
              <a className="quick-action-btn" href={calculatorUrl}>
                Use Calculator
              </a>
              <button className="quick-action-btn danger" type="button" onClick={handleDelete} disabled={saving}>
                Delete Quote
              </button>
            </div>
          </DetailCard>

          <DetailCard title="Activity">
            <div className="timeline-item">
              <span className="timeline-dot" />
              <div>
                <div className="strong">Quote request submitted</div>
                <div className="subtle">{formatDateTime(quote.created_at)}</div>
              </div>
            </div>
            {quote.updated_at && quote.updated_at !== quote.created_at ? (
              <div className="timeline-item">
                <span className="timeline-dot" />
                <div>
                  <div className="strong">Status updated to {quote.status}</div>
                  <div className="subtle">{formatDateTime(quote.updated_at)}</div>
                </div>
              </div>
            ) : null}
          </DetailCard>
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

function PropertyTag({ label, value }: { label: string; value?: string }) {
  const normalized = String(value || "").toLowerCase();
  const className =
    normalized === "yes"
      ? "property-tag yes"
      : normalized === "no"
        ? "property-tag no"
        : "property-tag unknown";
  const text =
    normalized === "yes"
      ? label
      : normalized === "no"
        ? `No ${label}`
        : `${label}: ${value || "—"}`;

  return <span className={className}>{text}</span>;
}
