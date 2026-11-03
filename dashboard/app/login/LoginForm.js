"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

const DEMO_USERS = [
  ["admin", "admin123", "full control: can crash nodes and partition the network"],
  ["operator", "operator123", "can read and write keys"],
  ["viewer", "viewer123", "read-only"],
];

export default function LoginForm() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(event) {
    event.preventDefault();
    setError("");

    if (!username.trim() || !password) {
      setError("Enter both a username and a password.");
      return;
    }

    setBusy(true);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: username.trim(), password }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(body.error || "Sign-in failed.");
        setBusy(false);
        return;
      }
      router.push("/dashboard");
      router.refresh();
    } catch {
      setError("Could not reach the server. Is MySQL running?");
      setBusy(false);
    }
  }

  function fill(user, pass) {
    setUsername(user);
    setPassword(pass);
    setError("");
  }

  return (
    <div className="card login-card">
      <h1>RaftKV</h1>
      <p className="muted small" style={{ margin: 0 }}>
        Fault-tolerant distributed key-value store
      </p>

      <form onSubmit={submit}>
        <label htmlFor="username">Username</label>
        <input
          id="username"
          value={username}
          autoComplete="username"
          onChange={(e) => {
            setUsername(e.target.value);
            setError("");
          }}
        />

        <label htmlFor="password">Password</label>
        <input
          id="password"
          type="password"
          value={password}
          autoComplete="current-password"
          onChange={(e) => {
            setPassword(e.target.value);
            setError("");
          }}
        />

        {error && (
          <div className="notice error" style={{ marginTop: 14 }}>
            {error}
          </div>
        )}

        <button className="primary" type="submit" disabled={busy}>
          {busy ? "Signing in..." : "Sign in"}
        </button>
      </form>

      <div className="demo-users">
        <div className="muted small" style={{ marginBottom: 6 }}>
          Demo accounts - click to fill
        </div>
        <table>
          <tbody>
            {DEMO_USERS.map(([user, pass, note]) => (
              <tr key={user}>
                <td>
                  <button
                    type="button"
                    className="small"
                    onClick={() => fill(user, pass)}
                  >
                    {user}
                  </button>
                </td>
                <td className="muted">{note}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
