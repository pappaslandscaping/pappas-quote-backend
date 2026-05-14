"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import {
  backendUrl,
  fetchCustomerPipelineStats,
  fetchCustomers,
  fetchCustomerStats
} from "../../lib/api";
import type {
  Customer,
  CustomerPipelineStats,
  CustomerStats
} from "../../types/customers";

const PAGE_SIZE = 50;

const SORT_OPTIONS = [
  { value: "name_asc", label: "Name (A-Z)" },
  { value: "name_desc", label: "Name (Z-A)" },
  { value: "city_asc", label: "City (A-Z)" },
  { value: "newest", label: "Newest First" },
  { value: "oldest", label: "Oldest First" }
];

function customerName(customer: Customer) {
  return (
    customer.name ||
    [customer.first_name, customer.last_name].filter(Boolean).join(" ") ||
    "Unknown"
  );
}

function customerAddress(customer: Customer) {
  return [customer.street, customer.city, customer.state, customer.postal_code]
    .filter(Boolean)
    .join(", ");
}

function displayType(customer: Customer) {
  return customer.customer_type === "lead" ? "Lead" : "Customer";
}

function tagList(tags?: string | null) {
  return String(tags || "")
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean)
    .slice(0, 3);
}

export default function CustomersPage() {
  const router = useRouter();
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [stats, setStats] = useState<CustomerStats | null>(null);
  const [pipelineStats, setPipelineStats] =
    useState<CustomerPipelineStats | null>(null);
  const [pipelineType, setPipelineType] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [cityFilter, setCityFilter] = useState("");
  const [sort, setSort] = useState("name_asc");
  const [search, setSearch] = useState("");
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  async function loadStats() {
    const [nextStats, nextPipelineStats] = await Promise.all([
      fetchCustomerStats(),
      fetchCustomerPipelineStats()
    ]);
    setStats(nextStats);
    setPipelineStats(nextPipelineStats);
  }

  async function loadCustomers() {
    setLoading(true);
    setError("");
    try {
      const data = await fetchCustomers({
        limit: PAGE_SIZE,
        offset,
        search,
        status: statusFilter,
        city: cityFilter,
        type: pipelineType,
        sort
      });
      setCustomers(data.customers);
      setTotal(data.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load customers");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadStats().catch((err) => {
      setError(err instanceof Error ? err.message : "Failed to load stats");
    });
  }, []);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      void loadCustomers();
    }, 250);

    return () => window.clearTimeout(timeout);
  }, [cityFilter, offset, pipelineType, search, sort, statusFilter]);

  const cities = useMemo(() => stats?.topCities || [], [stats]);
  const pageEnd = Math.min(offset + PAGE_SIZE, total);

  function updatePipeline(type: string) {
    setPipelineType(type);
    setOffset(0);
  }

  function updateFilter(setter: (value: string) => void, value: string) {
    setter(value);
    setOffset(0);
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Clients</h1>
          <p className="muted">Customer records, leads, and pipeline.</p>
        </div>
        <div className="topbar-actions">
          <Link className="btn btn-secondary" href={backendUrl("/properties.html")}>
            Properties
          </Link>
          <Link className="btn btn-primary" href={backendUrl("/new-customer.html")}>
            Add Client
          </Link>
        </div>
      </header>

      <div className="segmented-tabs" aria-label="Customer pipeline filter">
        <button
          className={pipelineType === "" ? "active" : ""}
          type="button"
          onClick={() => updatePipeline("")}
        >
          All
        </button>
        <button
          className={pipelineType === "customer" ? "active" : ""}
          type="button"
          onClick={() => updatePipeline("customer")}
        >
          Customers
          <span>{pipelineStats?.totalCustomers ?? 0}</span>
        </button>
        <button
          className={pipelineType === "lead" ? "active" : ""}
          type="button"
          onClick={() => updatePipeline("lead")}
        >
          Leads
          <span>{pipelineStats?.totalLeads ?? 0}</span>
        </button>
      </div>

      <section className="stats-grid stats-grid-five" aria-label="Customer stats">
        <StatCard label="Total" value={stats?.total ?? 0} tone="blue" />
        <StatCard label="Customers" value={pipelineStats?.totalCustomers ?? stats?.active ?? 0} tone="green" />
        <StatCard label="Leads" value={pipelineStats?.totalLeads ?? 0} tone="purple" />
        <StatCard label="Conversion Rate" value={`${pipelineStats?.conversionRate ?? 0}%`} tone="blue" />
        <StatCard label="New This Month" value={pipelineStats?.newLeadsThisMonth ?? 0} tone="green" />
      </section>

      <section className="table-card">
        <div className="table-toolbar">
          <div>
            <h2>All Customers</h2>
            <p>
              {total ? `${offset + 1}-${pageEnd} of ${total}` : "0 customers"}
            </p>
          </div>
          <div className="filters">
            <select
              aria-label="Filter by status"
              value={statusFilter}
              onChange={(event) => updateFilter(setStatusFilter, event.target.value)}
            >
              <option value="">All Statuses</option>
              <option value="Active">Active</option>
              <option value="Inactive">Inactive</option>
            </select>
            <select
              aria-label="Filter by city"
              value={cityFilter}
              onChange={(event) => updateFilter(setCityFilter, event.target.value)}
            >
              <option value="">All Cities</option>
              {cities.map((city) =>
                city.city ? (
                  <option key={city.city} value={city.city}>
                    {city.city} ({city.count})
                  </option>
                ) : null
              )}
            </select>
            <select
              aria-label="Sort customers"
              value={sort}
              onChange={(event) => updateFilter(setSort, event.target.value)}
            >
              {SORT_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <input
              aria-label="Search customers"
              type="search"
              value={search}
              onChange={(event) => updateFilter(setSearch, event.target.value)}
              placeholder="Search name, email, address..."
            />
          </div>
        </div>

        {loading ? (
          <div className="state-block">Loading customers...</div>
        ) : error ? (
          <div className="state-block error">{error}</div>
        ) : (
          <>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Phone</th>
                    <th>City</th>
                    <th>Type</th>
                    <th>Tags</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {customers.length ? (
                    customers.map((customer) => (
                      <tr
                        className="clickable-row"
                        key={customer.id}
                        onClick={() => router.push(`/customers/${customer.id}`)}
                      >
                        <td className="date-cell">{customer.customer_number || "-"}</td>
                        <td>
                          <Link
                            className="row-link"
                            href={`/customers/${customer.id}`}
                            onClick={(event) => event.stopPropagation()}
                          >
                            {customerName(customer)}
                          </Link>
                          <div className="subtle">{customerAddress(customer)}</div>
                        </td>
                        <td>{customer.email || "-"}</td>
                        <td>
                          {customer.phone || customer.mobile ? (
                            <a
                              href={`tel:${customer.phone || customer.mobile}`}
                              onClick={(event) => event.stopPropagation()}
                            >
                              {customer.phone || customer.mobile}
                            </a>
                          ) : (
                            "-"
                          )}
                        </td>
                        <td>{customer.city || "-"}</td>
                        <td>
                          <span className={`type-pill type-${customer.customer_type || "customer"}`}>
                            {displayType(customer)}
                          </span>
                        </td>
                        <td>
                          <div className="tag-list">
                            {tagList(customer.tags).length
                              ? tagList(customer.tags).map((tag) => (
                                  <span className="tag" key={tag}>
                                    {tag}
                                  </span>
                                ))
                              : "-"}
                          </div>
                        </td>
                        <td>
                          <span
                            className={`status-pill status-${(customer.status || "inactive").toLowerCase()}`}
                          >
                            {customer.status || "-"}
                          </span>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={8}>
                        <div className="empty-state">No customers found</div>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            {total > PAGE_SIZE ? (
              <div className="pagination">
                <span>
                  Showing {offset + 1}-{pageEnd} of {total}
                </span>
                <div>
                  <button
                    type="button"
                    disabled={offset === 0}
                    onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  >
                    Previous
                  </button>
                  <button
                    type="button"
                    disabled={offset + PAGE_SIZE >= total}
                    onClick={() => setOffset(offset + PAGE_SIZE)}
                  >
                    Next
                  </button>
                </div>
              </div>
            ) : null}
          </>
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
  tone: "blue" | "green" | "purple";
}) {
  return (
    <div className="stat-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <i className={`stat-dot ${tone}`} aria-hidden="true" />
    </div>
  );
}
