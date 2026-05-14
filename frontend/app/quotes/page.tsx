"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { backendUrl, createQuoteRequest, fetchQuoteRequests } from "../../lib/api";
import type { QuoteCreatePayload, QuoteRequest } from "../../types/quotes";

const STATUS_OPTIONS = [
  "new",
  "contacted",
  "quoted",
  "scheduled",
  "completed",
  "cancelled"
];

const PIPELINE_STAGES = [
  {
    key: "new_request",
    label: "New request",
    helper: "Fresh intake that needs triage."
  },
  {
    key: "needs_response",
    label: "Needs response",
    helper: "No customer contact has been logged yet."
  },
  {
    key: "estimate_needed",
    label: "Estimate needed",
    helper: "Customer has been contacted; price or scope is next."
  },
  {
    key: "estimate_sent",
    label: "Estimate sent",
    helper: "Estimate is out for review."
  },
  {
    key: "follow_up_due",
    label: "Follow-up due",
    helper: "Sent estimates older than three days."
  },
  {
    key: "won",
    label: "Won",
    helper: "Approved, scheduled, or completed work."
  },
  {
    key: "lost",
    label: "Lost",
    helper: "Cancelled or declined requests."
  }
];

const PACKAGE_LABELS: Record<string, string> = {
  essential: "Essential",
  complete: "Complete",
  premium: "Premium"
};

function formatDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function csvCell(value: unknown) {
  return `"${String(value ?? "").replace(/"/g, '""')}"`;
}

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

function servicesText(quote: QuoteRequest) {
  if (Array.isArray(quote.services)) return quote.services.filter(Boolean).join(", ");
  return quote.services || "Service not specified";
}

function stageForQuote(quote: QuoteRequest) {
  const status = String(quote.status || "new").toLowerCase();
  const created = new Date(quote.created_at);
  const daysOld = Number.isNaN(created.getTime())
    ? 0
    : (Date.now() - created.getTime()) / 86400000;

  if (["cancelled", "lost", "declined"].includes(status)) return "lost";
  if (["completed", "scheduled", "won", "approved", "accepted"].includes(status)) return "won";
  if (["quoted", "sent", "estimate_sent"].includes(status)) {
    return daysOld >= 3 ? "follow_up_due" : "estimate_sent";
  }
  if (["contacted", "needs_estimate", "estimate_needed"].includes(status)) return "estimate_needed";
  if (quote.phone || quote.email) return status === "new" ? "needs_response" : "estimate_needed";
  return "new_request";
}

export default function QuotesPage() {
  const router = useRouter();
  const [quotes, setQuotes] = useState<QuoteRequest[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [selectedQuoteId, setSelectedQuoteId] = useState<number | null>(null);

  async function loadQuotes() {
    setLoading(true);
    setError("");
    try {
      setQuotes(await fetchQuoteRequests());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load quotes");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadQuotes();
  }, []);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(""), 2500);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  const visibleQuotes = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();
    return quotes.filter((quote) => {
      const matchesStatus = !statusFilter || quote.status === statusFilter;
      const matchesSearch =
        !term ||
        [quote.name, quote.email, quote.address, quote.phone]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(term));

      return matchesStatus && matchesSearch;
    });
  }, [quotes, searchTerm, statusFilter]);

  const pipeline = useMemo(() => {
    const grouped = new Map<string, QuoteRequest[]>();
    PIPELINE_STAGES.forEach((stage) => grouped.set(stage.key, []));
    visibleQuotes.forEach((quote) => {
      const stage = stageForQuote(quote);
      grouped.get(stage)?.push(quote);
    });
    return grouped;
  }, [visibleQuotes]);

  const urgencyGroups = useMemo(() => {
    const response = visibleQuotes.filter((quote) => stageForQuote(quote) === "needs_response" || stageForQuote(quote) === "new_request");
    const estimate = visibleQuotes.filter((quote) => stageForQuote(quote) === "estimate_needed");
    const followUp = visibleQuotes.filter((quote) => stageForQuote(quote) === "follow_up_due" || stageForQuote(quote) === "estimate_sent");
    const closed = visibleQuotes.filter((quote) => ["won", "lost"].includes(stageForQuote(quote)));
    return [
      { key: "response", label: "Needs response", helper: "Call or draft a reply first.", rows: response },
      { key: "estimate", label: "Estimate needed", helper: "Scope and price the work.", rows: estimate },
      { key: "follow-up", label: "Follow-up due", helper: "Sent estimates that need a nudge.", rows: followUp },
      { key: "closed", label: "Won / Lost", helper: "Recently closed sales work.", rows: closed }
    ];
  }, [visibleQuotes]);

  const selectedQuote = useMemo(() => {
    const fallback = urgencyGroups.find((group) => group.rows.length)?.rows[0] || visibleQuotes[0] || null;
    return visibleQuotes.find((quote) => quote.id === selectedQuoteId) || fallback;
  }, [selectedQuoteId, urgencyGroups, visibleQuotes]);

  function showToast(message: string) {
    setToast(message);
  }

  function exportToCSV() {
    if (!visibleQuotes.length) {
      showToast("No quotes to export");
      return;
    }

    const headers = [
      "Date",
      "Name",
      "Phone",
      "Email",
      "Address",
      "Package",
      "Services",
      "Status",
      "Source",
      "Gate",
      "Dogs",
      "Overgrown",
      "Contact Method",
      "Start Timing",
      "Backyard Access",
      "Notes"
    ];

    const rows = visibleQuotes.map((quote) => {
      const questions = normalizeQuestions(quote);
      return [
        new Date(quote.created_at).toLocaleDateString(),
        quote.name,
        quote.phone,
        quote.email,
        quote.address,
        quote.package || "Individual",
        Array.isArray(quote.services) ? quote.services.join("; ") : "",
        quote.status,
        quote.source || "",
        questions.gate || "",
        questions.dogs || "",
        questions.lawnHeight || questions.overgrown || "",
        questions.contactMethod || questions.contact_method || "",
        questions.startTime || questions.start_timing || "",
        questions.backyardAccess || questions.backyard_access || "",
        quote.notes || ""
      ];
    });

    const csv = [headers, ...rows]
      .map((row) => row.map(csvCell).join(","))
      .join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `pappas-quotes-${new Date().toISOString().split("T")[0]}.csv`;
    anchor.click();
    URL.revokeObjectURL(url);
    showToast("CSV exported");
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const payload: QuoteCreatePayload = {
      name: String(formData.get("name") || ""),
      phone: String(formData.get("phone") || ""),
      email: String(formData.get("email") || "") || undefined,
      address: String(formData.get("address") || "") || undefined,
      package: String(formData.get("package") || "") || undefined,
      source: String(formData.get("source") || "") || undefined,
      services: String(formData.get("services") || "") || undefined,
      notes: String(formData.get("notes") || "") || undefined
    };

    setIsSubmitting(true);
    try {
      await createQuoteRequest(payload);
      form.reset();
      setIsModalOpen(false);
      showToast("Quote request created");
      await loadQuotes();
    } catch (err) {
      showToast(
        err instanceof Error ? err.message : "Failed to create request"
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Leads & Estimates</h1>
          <p className="muted">Turn new landscaping requests into estimated, won, or intentionally closed work.</p>
        </div>
        <div className="topbar-actions">
          <button className="icon-button" type="button" onClick={loadQuotes} aria-label="Refresh quotes">
            <RefreshIcon />
          </button>
          <button className="icon-button" type="button" onClick={exportToCSV} aria-label="Export CSV">
            <DownloadIcon />
          </button>
          <a className="btn btn-secondary" href={backendUrl("/sent-quotes.html")}>
            Legacy Estimates
          </a>
          <a className="btn btn-secondary" href={backendUrl("/quote-generator.html")}>
            Estimate Builder
          </a>
          <button className="btn btn-primary" type="button" onClick={() => setIsModalOpen(true)}>
            New Request
          </button>
        </div>
      </header>

      <section className="table-card">
        <div className="table-toolbar">
          <div>
            <h2>Pipeline Controls</h2>
            <p>{visibleQuotes.length} visible</p>
          </div>
          <div className="filters">
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)} aria-label="Filter by status">
              <option value="">All Statuses</option>
              {STATUS_OPTIONS.map((status) => (
                <option key={status} value={status}>
                  {status.charAt(0).toUpperCase() + status.slice(1)}
                </option>
              ))}
            </select>
            <input
              type="search"
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              placeholder="Search name, phone, email, address..."
            />
          </div>
        </div>
      </section>

      <section className="split-workspace" aria-label="Sales queue">
        {loading ? (
          <div className="state-block">Loading leads and estimates...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : (
          <>
            <aside className="sales-queue">
              {urgencyGroups.map((group) => (
                <section className="queue-group" key={group.key} aria-label={group.label}>
                  <header>
                    <div>
                      <h2>{group.label}</h2>
                      <p>{group.helper}</p>
                    </div>
                    <strong>{group.rows.length}</strong>
                  </header>
                  <div className="queue-list">
                    {group.rows.length ? group.rows.slice(0, 12).map((quote) => (
                      <button
                        className={selectedQuote?.id === quote.id ? "queue-item active" : "queue-item"}
                        type="button"
                        key={quote.id}
                        onClick={() => setSelectedQuoteId(quote.id)}
                      >
                        <strong>{quote.name || "Unknown lead"}</strong>
                        <span>{servicesText(quote)}</span>
                        <small>{[quote.address, quote.phone || quote.email].filter(Boolean).join(" - ") || "No contact details"}</small>
                      </button>
                    )) : <div className="empty-state">Clear</div>}
                  </div>
                </section>
              ))}
            </aside>
            <section className="selected-detail" aria-label="Selected lead detail">
              {selectedQuote ? (
                <SelectedLeadDetail quote={selectedQuote} onOpen={() => router.push(`/quotes/${selectedQuote.id}`)} />
              ) : (
                <div className="empty-state">No lead selected.</div>
              )}
            </section>
          </>
        )}
      </section>

      <section className="table-card secondary-index">
        <div className="table-toolbar">
          <div>
            <h2>Lead Index</h2>
            <p>CSV export and fast scanning.</p>
          </div>
        </div>

        {loading ? (
          <div className="state-block">Loading quotes...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Customer</th>
                  <th>Phone</th>
                  <th>Address</th>
                  <th>Services</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {visibleQuotes.length ? (
                  visibleQuotes.map((quote) => (
                    <tr
                      key={quote.id}
                      className="clickable-row"
                      onClick={() => router.push(`/quotes/${quote.id}`)}
                    >
                      <td className="date-cell">{formatDate(quote.created_at)}</td>
                      <td>
                        <Link
                          className="row-link"
                          href={`/quotes/${quote.id}`}
                          onClick={(event) => event.stopPropagation()}
                        >
                          {quote.name || "Unknown"}
                        </Link>
                        <div className="subtle">{quote.email || ""}</div>
                      </td>
                      <td>
                        {quote.phone ? (
                          <a href={`tel:${quote.phone}`} onClick={(event) => event.stopPropagation()}>
                            {quote.phone}
                          </a>
                        ) : (
                          ""
                        )}
                      </td>
                      <td className="truncate">{quote.address || ""}</td>
                      <td>{servicesText(quote)}</td>
                      <td>
                        <span className={`status-pill status-${quote.status || "new"}`}>
                          {quote.status || "new"}
                        </span>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={6}>
                      <div className="empty-state">No quote requests found</div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {isModalOpen ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={(event) => {
          if (event.target === event.currentTarget) setIsModalOpen(false);
        }}>
          <div className="modal" role="dialog" aria-modal="true" aria-labelledby="new-request-title">
            <div className="modal-header">
              <h2 id="new-request-title">New Quote Request</h2>
              <button className="icon-button" type="button" onClick={() => setIsModalOpen(false)} aria-label="Close modal">
                <CloseIcon />
              </button>
            </div>
            <form onSubmit={handleSubmit} className="request-form">
              <label className="full">
                Name *
                <input name="name" required placeholder="Customer name" />
              </label>
              <label>
                Phone *
                <input name="phone" required placeholder="(216) 555-0000" />
              </label>
              <label>
                Email
                <input name="email" type="email" placeholder="email@example.com" />
              </label>
              <label className="full">
                Address
                <input name="address" placeholder="123 Main St, Cleveland, OH" />
              </label>
              <label>
                Package
                <select name="package" defaultValue="">
                  <option value="">None</option>
                  <option value="essential">Essential</option>
                  <option value="complete">Complete</option>
                  <option value="premium">Premium</option>
                </select>
              </label>
              <label>
                Source
                <select name="source" defaultValue="phone_call">
                  <option value="phone_call">Phone Call</option>
                  <option value="walk_in">Walk-in</option>
                  <option value="referral">Referral</option>
                  <option value="email">Email</option>
                  <option value="other">Other</option>
                </select>
              </label>
              <label className="full">
                Services Requested
                <input name="services" placeholder="Mowing, Leaf Cleanup, etc. (comma-separated)" />
              </label>
              <label className="full">
                Notes
                <textarea name="notes" rows={3} placeholder="Any details from the call..." />
              </label>
              <div className="modal-actions">
                <button className="btn btn-secondary" type="button" onClick={() => setIsModalOpen(false)}>
                  Cancel
                </button>
                <button className="btn btn-primary" type="submit" disabled={isSubmitting}>
                  {isSubmitting ? "Creating..." : "Create Request"}
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}

      {toast ? <div className="toast">{toast}</div> : null}
    </main>
  );
}

function QuotePipelineCard({
  quote,
  onOpen
}: {
  quote: QuoteRequest;
  onOpen: () => void;
}) {
  const questions = normalizeQuestions(quote);
  return (
    <article className="workflow-card" onClick={onOpen}>
      <div className="workflow-card-top">
        <strong>{quote.name || "Unknown lead"}</strong>
        <span className={`status-pill status-${quote.status || "new"}`}>
          {quote.status || "new"}
        </span>
      </div>
      <p>{servicesText(quote)}</p>
      <div className="workflow-fields">
        <span>{quote.address || "No address"}</span>
        <span>{[quote.phone, quote.email].filter(Boolean).join(" · ") || "No contact info"}</span>
        <span>Source: {quote.source || "Unknown"}</span>
        <span>Package: {quote.package ? PACKAGE_LABELS[quote.package] || quote.package : "Individual"}</span>
        {quote.notes ? <span>Notes: {quote.notes}</span> : null}
        {questions.contactMethod ? <span>Prefers: {questions.contactMethod}</span> : null}
      </div>
      <div className="workflow-actions" onClick={(event) => event.stopPropagation()}>
        <Link className="quick-action-btn" href={`/quotes/${quote.id}`}>
          Open
        </Link>
        {quote.phone ? (
          <a className="quick-action-btn" href={`tel:${quote.phone}`}>
            Call
          </a>
        ) : null}
        {quote.email ? (
          <a className="quick-action-btn" href={`mailto:${quote.email}`}>
            Email
          </a>
        ) : null}
        <a className="quick-action-btn primary" href={backendUrl(`/quote-generator.html?quote_id=${quote.id}`)}>
          Build Estimate
        </a>
      </div>
    </article>
  );
}

function SelectedLeadDetail({
  quote,
  onOpen
}: {
  quote: QuoteRequest;
  onOpen: () => void;
}) {
  const questions = normalizeQuestions(quote);
  const stage = PIPELINE_STAGES.find((item) => item.key === stageForQuote(quote));
  return (
    <article className="sales-detail-card">
      <div className="sales-detail-header">
        <div>
          <p className="eyebrow">Selected Lead</p>
          <h2>{quote.name || "Unknown lead"}</h2>
          <span className={`status-pill status-${quote.status || "new"}`}>{stage?.label || quote.status || "new"}</span>
        </div>
        <button className="btn btn-secondary" type="button" onClick={onOpen}>Open record</button>
      </div>
      <div className="sales-detail-grid">
        <InfoTile label="Services" value={servicesText(quote)} />
        <InfoTile label="Address" value={quote.address || "No address"} />
        <InfoTile label="Phone / Email" value={[quote.phone, quote.email].filter(Boolean).join(" / ") || "No contact info"} />
        <InfoTile label="Source" value={quote.source || "Unknown"} />
        <InfoTile label="Submitted" value={formatDate(quote.created_at)} />
        <InfoTile label="Notes" value={quote.notes || "No notes yet."} />
      </div>
      <div className="next-action-callout">
        <span>Next best action</span>
        <strong>{stageForQuote(quote) === "estimate_needed" ? "Build the estimate while the scope is fresh." : stageForQuote(quote) === "follow_up_due" ? "Send a follow-up draft before this estimate goes cold." : "Respond to the customer and confirm the requested work."}</strong>
      </div>
      <div className="sticky-action-bar">
        {quote.phone ? <a className="quick-action-btn primary" href={`tel:${quote.phone}`}>Call</a> : null}
        {quote.email ? <a className="quick-action-btn" href={`mailto:${quote.email}`}>Email</a> : null}
        <Link className="quick-action-btn" href={`/ai?draft=lead&quote_id=${quote.id}`}>AI email draft</Link>
        <Link className="quick-action-btn" href={`/ai?draft=text&quote_id=${quote.id}`}>AI text draft</Link>
        <a className="quick-action-btn primary" href={backendUrl(`/quote-generator.html?quote_id=${quote.id}`)}>Build estimate</a>
      </div>
      {questions.contactMethod || questions.lawnHeight ? (
        <p className="subtle">Context: {[questions.contactMethod, questions.lawnHeight].filter(Boolean).join(" - ")}</p>
      ) : null}
    </article>
  );
}

function InfoTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function RefreshIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <polyline points="23 4 23 10 17 10" />
      <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
    </svg>
  );
}

function DownloadIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
      <polyline points="7 10 12 15 17 10" />
      <line x1="12" y1="15" x2="12" y2="3" />
    </svg>
  );
}

function CloseIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <line x1="18" y1="6" x2="6" y2="18" />
      <line x1="6" y1="6" x2="18" y2="18" />
    </svg>
  );
}
