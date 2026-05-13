"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";

type NavItem = {
  href: string;
  label: string;
  disabled?: boolean;
};

const NAV_ITEMS: NavItem[] = [
  { href: "/", label: "Home" },
  { href: "/quotes", label: "Quotes" },
  { href: "/customers", label: "Customers" },
  { href: "/invoices", label: "Invoices" },
  { href: "/jobs", label: "Scheduling/Jobs" }
];

function clearAuthStorage() {
  window.localStorage.removeItem("adminToken");
  window.localStorage.removeItem("adminName");
  window.localStorage.removeItem("adminEmail");
  window.localStorage.removeItem("isEmployee");
  window.localStorage.removeItem("employeePermissions");
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [isCheckingAuth, setIsCheckingAuth] = useState(true);
  const [adminName, setAdminName] = useState("");
  const [adminEmail, setAdminEmail] = useState("");
  const isLoginRoute = pathname === "/login";

  useEffect(() => {
    if (isLoginRoute) {
      setIsCheckingAuth(false);
      return;
    }

    const token = window.localStorage.getItem("adminToken");
    if (!token) {
      router.replace("/login");
      return;
    }

    setAdminName(window.localStorage.getItem("adminName") || "");
    setAdminEmail(window.localStorage.getItem("adminEmail") || "");
    setIsCheckingAuth(false);
  }, [isLoginRoute, router]);

  const initials = useMemo(() => {
    const source = adminName || adminEmail || "Admin";
    return source
      .split(/\s|@/)
      .filter(Boolean)
      .slice(0, 2)
      .map((part) => part.charAt(0).toUpperCase())
      .join("");
  }, [adminEmail, adminName]);

  function handleLogout() {
    clearAuthStorage();
    router.push("/login");
  }

  if (isLoginRoute) {
    return children;
  }

  if (isCheckingAuth) {
    return (
      <main className="app-shell">
        <div className="state-block">Loading YardDesk...</div>
      </main>
    );
  }

  return (
    <div className="authenticated-shell">
      <aside className="app-sidebar">
        <Link className="sidebar-brand" href="/">
          <span>YD</span>
          <strong>YardDesk</strong>
        </Link>

        <nav className="sidebar-nav" aria-label="Primary navigation">
          {NAV_ITEMS.map((item) =>
            item.disabled ? (
              <span className="nav-item disabled" key={item.label} aria-disabled="true">
                {item.label}
                <small>Soon</small>
              </span>
            ) : (
              <Link
                className={
                  pathname === item.href || pathname.startsWith(`${item.href}/`)
                    ? "nav-item active"
                    : "nav-item"
                }
                href={item.href}
                key={item.href}
              >
                {item.label}
              </Link>
            )
          )}
        </nav>

        <div className="sidebar-account">
          <div className="account-avatar" aria-hidden="true">
            {initials || "A"}
          </div>
          <div className="account-copy">
            <strong>{adminName || "Admin"}</strong>
            {adminEmail ? <span>{adminEmail}</span> : null}
          </div>
          <button className="btn btn-secondary" type="button" onClick={handleLogout}>
            Logout
          </button>
        </div>
      </aside>

      <div className="authenticated-content">{children}</div>
    </div>
  );
}
