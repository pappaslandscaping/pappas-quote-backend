"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { login } from "../../lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (window.localStorage.getItem("adminToken")) {
      router.replace("/quotes");
    }
  }, [router]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");

    if (!email.trim() || !password) {
      setError("Please enter both email and password.");
      return;
    }

    setIsSubmitting(true);
    try {
      const data = await login({ email: email.trim(), password });

      window.localStorage.setItem("adminToken", data.token || "");
      if (data.name) window.localStorage.setItem("adminName", data.name);
      if (data.email) window.localStorage.setItem("adminEmail", data.email);

      if (data.isEmployee && data.permissions) {
        window.localStorage.setItem("isEmployee", "true");
        window.localStorage.setItem(
          "employeePermissions",
          JSON.stringify(data.permissions)
        );
      } else {
        window.localStorage.removeItem("isEmployee");
        window.localStorage.removeItem("employeePermissions");
      }

      router.push("/quotes");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-panel">
        <div>
          <p className="eyebrow">YardDesk</p>
          <h1>Sign in</h1>
          <p className="muted">Use your existing admin or employee account.</p>
        </div>

        <form className="login-form" onSubmit={handleSubmit}>
          <label>
            Email
            <input
              autoComplete="email"
              autoFocus
              name="email"
              onChange={(event) => setEmail(event.target.value)}
              placeholder="you@example.com"
              type="email"
              value={email}
            />
          </label>

          <label>
            Password
            <input
              autoComplete="current-password"
              name="password"
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Password"
              type="password"
              value={password}
            />
          </label>

          {error ? <div className="form-error">{error}</div> : null}

          <button className="btn btn-primary" disabled={isSubmitting} type="submit">
            {isSubmitting ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </section>
    </main>
  );
}
