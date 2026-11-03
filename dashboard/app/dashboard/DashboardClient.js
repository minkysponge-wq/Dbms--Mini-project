"use client";

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

const POLL_MS = 2000;

function can(user, permission) {
  return user.permissions.includes(permission);
}

export default function DashboardClient({ user }) {
  const router = useRouter();
  const [tab, setTab] = useState("cluster");
  const [cluster, setCluster] = useState(null);
  const [error, setError] = useState("");

  const loadCluster = useCallback(async () => {
    try {
      const res = await fetch("/api/cluster", { cache: "no-store" });
      if (res.status === 401) {
        router.push("/login");
        return;
      }
      const body = await res.json();
      if (!res.ok) {
        setError(body.error || "Could not load cluster status.");
        return;
      }
      setCluster(body);
      setError("");
    } catch {
      setError("Could not reach the dashboard API.");
    }
  }, [router]);

  useEffect(() => {
    loadCluster();
    const id = setInterval(loadCluster, POLL_MS);
    return () => clearInterval(id);
  }, [loadCluster]);

  async function signOut() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  }

  return (
    <>
      <header className="top">
        <div>
          <h1>RaftKV</h1>
          <div className="sub">
            Fault-tolerant distributed key-value store &middot; Raft consensus
          </div>
        </div>
        <div className="row">
          <span className="small muted">{user.username}</span>
          {user.roles.map((r) => (
            <span key={r} className="badge role">
              {r}
            </span>
          ))}
          <button className="small" onClick={signOut}>
            Sign out
          </button>
        </div>
      </header>

      <div className="wrap">
        {error && (
          <div className="notice error" style={{ marginBottom: 16 }}>
            {error}
          </div>
        )}

        {cluster && !cluster.clusterOnline && (
          <div className="notice" style={{ marginBottom: 16 }}>
            Showing state from MySQL only &mdash; the cluster process is not
            responding. Start it with <span className="mono">python server.py</span>.
          </div>
        )}

        <div className="tabs" role="tablist">
          {[
            ["cluster", "Cluster"],
            ["kv", "Key-value store"],
            ["events", "Event log & vector search"],
          ].map(([id, label]) => (
            <button
              key={id}
              role="tab"
              aria-selected={tab === id}
              onClick={() => setTab(id)}
            >
              {label}
            </button>
          ))}
        </div>

        {tab === "cluster" && (
          <ClusterTab user={user} cluster={cluster} reload={loadCluster} />
        )}
        {tab === "kv" && <KvTab user={user} />}
        {tab === "events" && <EventsTab user={user} />}
      </div>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Cluster                                                             */
/* ------------------------------------------------------------------ */

function ClusterTab({ user, cluster, reload }) {
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState(null);

  if (!cluster) return <div className="card">Loading cluster state...</div>;

  const admin = can(user, "node.kill");

  async function act(label, path, options) {
    setBusy(label);
    setMessage(null);
    try {
      const res = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      const body = await res.json().catch(() => ({}));
      setMessage(
        res.ok
          ? { ok: true, text: `${label} succeeded.` }
          : { ok: false, text: body.detail || body.error || `${label} failed.` }
      );
      await reload();
    } catch {
      setMessage({ ok: false, text: `${label} failed: server unreachable.` });
    } finally {
      setBusy("");
    }
  }

  const alive = cluster.nodes.filter((n) => n.is_alive).length;
  const healthy = alive >= cluster.majority;

  return (
    <div className="grid">
      <div className="card">
        <h2>Cluster health</h2>
        <div className="row">
          <span className={`badge ${healthy ? "leader" : "offline"}`}>
            {healthy ? "quorum available" : "no quorum"}
          </span>
          <span className="small muted">
            {alive} of {cluster.nodes.length} nodes alive &middot; {cluster.majority}{" "}
            needed for a majority
          </span>
          {cluster.messages && (
            <span className="small muted">
              &middot; {cluster.messages.sent} messages sent,{" "}
              {cluster.messages.lost} lost
            </span>
          )}
        </div>
        {!healthy && (
          <div className="notice error" style={{ marginTop: 10 }}>
            Fewer than a majority of nodes are alive, so the cluster cannot
            commit writes. That is correct behaviour &mdash; serving writes here
            would risk split-brain.
          </div>
        )}
      </div>

      {message && (
        <div className={`notice ${message.ok ? "ok" : "error"}`}>
          {message.text}
        </div>
      )}

      <div className="grid nodes">
        {cluster.nodes.map((node) => {
          const offline = !node.is_alive;
          const role = offline ? "offline" : node.role;
          return (
            <div
              key={node.node_id}
              className={`card node-card ${
                node.role === "leader" && !offline ? "is-leader" : ""
              } ${offline ? "is-offline" : ""}`}
            >
              <div className="row" style={{ justifyContent: "space-between" }}>
                <span className="nid">node {node.node_id}</span>
                <span className={`badge ${role}`}>{role}</span>
              </div>
              <dl>
                <dt>term</dt>
                <dd>{node.current_term}</dd>
                <dt>log length</dt>
                <dd>{node.log_length}</dd>
                <dt>commit index</dt>
                <dd>{node.commit_index}</dd>
                <dt>last applied</dt>
                <dd>{node.last_applied}</dd>
                <dt>behind by</dt>
                <dd>{node.entries_behind}</dd>
              </dl>
              {admin && (
                <div className="row tight" style={{ marginTop: 11 }}>
                  {node.is_alive ? (
                    <button
                      className="small danger"
                      disabled={busy !== ""}
                      onClick={() =>
                        act(`Crash node ${node.node_id}`, "/api/node/kill", {
                          method: "POST",
                          body: JSON.stringify({ node_id: node.node_id }),
                        })
                      }
                    >
                      Crash
                    </button>
                  ) : (
                    <button
                      className="small"
                      disabled={busy !== ""}
                      onClick={() =>
                        act(`Restart node ${node.node_id}`, "/api/node/restart", {
                          method: "POST",
                          body: JSON.stringify({ node_id: node.node_id }),
                        })
                      }
                    >
                      Restart
                    </button>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {can(user, "cluster.admin") && (
        <div className="card">
          <h2>Network partition</h2>
          <p className="small muted" style={{ marginTop: 0 }}>
            Isolate the leader with one follower. The remaining three are a
            majority, so they elect a new leader and keep serving &mdash; while
            the isolated side stops committing.
          </p>
          <div className="row">
            <button
              disabled={busy !== "" || cluster.leader < 0}
              onClick={() => {
                const leader = cluster.leader;
                const other = cluster.nodes.find(
                  (n) => n.node_id !== leader && n.is_alive
                );
                act("Partition", "/api/partition", {
                  method: "POST",
                  body: JSON.stringify({
                    minority: [leader, other ? other.node_id : leader],
                  }),
                });
              }}
            >
              Isolate the leader
            </button>
            <button
              disabled={busy !== ""}
              onClick={() => act("Heal", "/api/partition", { method: "DELETE" })}
            >
              Heal the network
            </button>
          </div>
        </div>
      )}

      <div className="grid two">
        <div className="card">
          <h2>Replication progress</h2>
          <p className="small muted" style={{ marginTop: 0 }}>
            From <span className="mono">v_replication_lag</span>, ranked with a
            window function.
          </p>
          {cluster.lag.length === 0 ? (
            <div className="muted small">No leader is tracking followers yet.</div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>follower</th>
                  <th className="num">next</th>
                  <th className="num">match</th>
                  <th className="num">behind</th>
                  <th className="num">rank</th>
                </tr>
              </thead>
              <tbody>
                {cluster.lag.map((r) => (
                  <tr key={`${r.leader_id}-${r.follower_id}`}>
                    <td>node {r.follower_id}</td>
                    <td className="num">{r.next_index}</td>
                    <td className="num">{r.match_index}</td>
                    <td className="num">{r.entries_behind}</td>
                    <td className="num">{r.catch_up_rank}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="card">
          <h2>Commit quorum</h2>
          <p className="small muted" style={{ marginTop: 0 }}>
            From <span className="mono">v_commit_quorum</span>, a CTE applying
            Raft&apos;s majority rule to each log index.
          </p>
          {cluster.quorum.length === 0 ? (
            <div className="muted small">No entries yet.</div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th className="num">index</th>
                  <th className="num">term</th>
                  <th className="num">replicas</th>
                  <th className="num">needed</th>
                  <th>committed</th>
                </tr>
              </thead>
              <tbody>
                {cluster.quorum.map((r) => (
                  <tr key={r.log_index}>
                    <td className="num">{r.log_index}</td>
                    <td className="num">{r.term_number}</td>
                    <td className="num">{r.replica_count}</td>
                    <td className="num">{r.majority_needed}</td>
                    <td>
                      <span
                        className={`badge ${
                          r.is_majority_replicated ? "leader" : "candidate"
                        }`}
                      >
                        {r.is_majority_replicated ? "yes" : "pending"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      <div className="card">
        <h2>Leadership history</h2>
        <p className="small muted" style={{ marginTop: 0 }}>
          From <span className="mono">v_leader_timeline</span>, which uses LAG
          and LEAD to see each term&apos;s neighbours.
        </p>
        {cluster.timeline.length === 0 ? (
          <div className="muted small">No terms recorded yet.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th className="num">term</th>
                <th>leader</th>
                <th>previous</th>
                <th>transition</th>
              </tr>
            </thead>
            <tbody>
              {cluster.timeline.map((t) => (
                <tr key={t.term_number}>
                  <td className="num">{t.term_number}</td>
                  <td>{t.leader_id === null ? "none" : `node ${t.leader_id}`}</td>
                  <td className="muted">
                    {t.previous_leader === null ? "-" : `node ${t.previous_leader}`}
                  </td>
                  <td className="small muted">{t.transition}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Key-value store                                                     */
/* ------------------------------------------------------------------ */

function KvTab({ user }) {
  const [data, setData] = useState(null);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [op, setOp] = useState("PUT");
  const [search, setSearch] = useState("");
  const [message, setMessage] = useState(null);
  const [busy, setBusy] = useState(false);

  const writable = can(user, "kv.write");

  const load = useCallback(async (term) => {
    const url = term
      ? `/api/kv?search=${encodeURIComponent(term)}`
      : "/api/kv";
    const res = await fetch(url, { cache: "no-store" });
    const body = await res.json().catch(() => ({}));
    if (res.ok) setData(body);
    else setMessage({ ok: false, text: body.error || "Could not load keys." });
  }, []);

  useEffect(() => {
    load("");
  }, [load]);

  async function write(event) {
    event.preventDefault();
    setMessage(null);

    if (!key.trim()) {
      setMessage({ ok: false, text: "Enter a key first." });
      return;
    }
    if (op !== "DELETE" && !value.trim()) {
      setMessage({ ok: false, text: "Enter a value first." });
      return;
    }

    setBusy(true);
    try {
      const res = await fetch("/api/kv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key: key.trim(), value, op }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok) {
        setMessage({
          ok: true,
          text: `${op} ${key.trim()} committed through consensus.`,
        });
        setKey("");
        setValue("");
        setTimeout(() => load(search), 600);
      } else {
        setMessage({
          ok: false,
          text: body.detail || body.error || "The write was rejected.",
        });
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid">
      <div className="card">
        <h2>Write a key</h2>
        {!writable && (
          <div className="notice" style={{ marginBottom: 12 }}>
            Your role is read-only. Writing needs the{" "}
            <span className="mono">kv.write</span> permission, which the
            operator and admin roles hold.
          </div>
        )}
        <form onSubmit={write}>
          <div className="row">
            <select value={op} onChange={(e) => setOp(e.target.value)} disabled={!writable}>
              <option>PUT</option>
              <option>APPEND</option>
              <option>DELETE</option>
            </select>
            <input
              className="grow"
              placeholder="key"
              value={key}
              onChange={(e) => setKey(e.target.value)}
              disabled={!writable}
            />
            <input
              className="grow"
              placeholder={op === "DELETE" ? "(not used)" : "value"}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              disabled={!writable || op === "DELETE"}
            />
            <button className="primary" type="submit" disabled={!writable || busy}>
              {busy ? "Committing..." : "Commit"}
            </button>
          </div>
        </form>
        {message && (
          <div className={`notice ${message.ok ? "ok" : "error"}`} style={{ marginTop: 12 }}>
            {message.text}
          </div>
        )}
      </div>

      <div className="card">
        <h2>Stored keys</h2>
        <div className="row" style={{ marginBottom: 12 }}>
          <input
            className="grow"
            placeholder="Full-text search over stored values"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <button onClick={() => load(search)}>Search</button>
          <button
            onClick={() => {
              setSearch("");
              load("");
            }}
          >
            Clear
          </button>
        </div>
        <p className="small muted" style={{ marginTop: 0 }}>
          Read from <span className="mono">kv_state</span> on node{" "}
          {data ? data.nodeId : "?"}. Search uses the MySQL FULLTEXT index.
        </p>
        {!data ? (
          <div className="muted small">Loading...</div>
        ) : data.pairs.length === 0 ? (
          <div className="muted small">
            {search ? "No values matched." : "No keys stored yet."}
          </div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>key</th>
                <th>value</th>
                <th className="num">applied at</th>
                {!search && <th className="num">replicas</th>}
                {search && <th className="num">relevance</th>}
              </tr>
            </thead>
            <tbody>
              {data.pairs.map((p) => (
                <tr key={p.key_name}>
                  <td className="mono">{p.key_name}</td>
                  <td>{p.value_text}</td>
                  <td className="num">{p.last_applied_index}</td>
                  {!search && <td className="num">{p.replica_count}</td>}
                  {search && (
                    <td className="num">{Number(p.relevance).toFixed(3)}</td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Events + vector search                                              */
/* ------------------------------------------------------------------ */

function EventsTab({ user }) {
  const [data, setData] = useState(null);
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState("vector");
  const [results, setResults] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const admin = can(user, "cluster.admin");

  const load = useCallback(async () => {
    const res = await fetch("/api/events?limit=60", { cache: "no-store" });
    const body = await res.json().catch(() => ({}));
    if (res.ok) {
      setData(body);
      setError("");
    } else {
      setError(body.detail || body.error || "Could not load events.");
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 4000);
    return () => clearInterval(id);
  }, [load]);

  async function runSearch(event) {
    event.preventDefault();
    if (!query.trim()) {
      setError("Enter something to search for.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const res = await fetch(
        `/api/events/search?q=${encodeURIComponent(query.trim())}&mode=${mode}`,
        { cache: "no-store" }
      );
      const body = await res.json().catch(() => ({}));
      if (res.ok) setResults(body);
      else setError(body.detail || body.error || "Search failed.");
    } finally {
      setBusy(false);
    }
  }

  async function annotate(eventId) {
    const note = window.prompt("Add a note to this event:");
    if (note === null || !note.trim()) return;
    await fetch("/api/events", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ event_id: eventId, note: note.trim() }),
    });
    load();
  }

  async function remove(eventId) {
    await fetch(`/api/events?event_id=${encodeURIComponent(eventId)}`, {
      method: "DELETE",
    });
    load();
    setResults(null);
  }

  return (
    <div className="grid">
      <div className="card">
        <h2>Search the event stream</h2>
        <p className="small muted" style={{ marginTop: 0 }}>
          Vector search embeds your query and ranks events by cosine similarity,
          computed inside MongoDB with a <span className="mono">$zip</span> /{" "}
          <span className="mono">$reduce</span> pipeline. Keyword search uses
          MongoDB&apos;s text index. Try{" "}
          <span className="mono">which node crashed</span>.
        </p>
        <form onSubmit={runSearch}>
          <div className="row">
            <select value={mode} onChange={(e) => setMode(e.target.value)}>
              <option value="vector">vector</option>
              <option value="text">keyword</option>
            </select>
            <input
              className="grow"
              placeholder="e.g. leadership changed, node crashed, partition"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setError("");
              }}
            />
            <button className="primary" type="submit" disabled={busy}>
              {busy ? "Searching..." : "Search"}
            </button>
            {results && (
              <button type="button" onClick={() => setResults(null)}>
                Clear
              </button>
            )}
          </div>
        </form>

        {error && (
          <div className="notice error" style={{ marginTop: 12 }}>
            {error}
          </div>
        )}

        {results && (
          <>
            <h3>
              {results.results.length} result
              {results.results.length === 1 ? "" : "s"} for &ldquo;{results.query}
              &rdquo; ({results.mode})
            </h3>
            {results.results.length === 0 ? (
              <div className="muted small">Nothing matched.</div>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th className="num">score</th>
                    <th>event</th>
                    <th>description</th>
                  </tr>
                </thead>
                <tbody>
                  {results.results.map((r) => (
                    <tr key={r.event_id}>
                      <td className="num" style={{ width: 86 }}>
                        {r.score === undefined ? "-" : Number(r.score).toFixed(3)}
                        {r.score !== undefined && (
                          <div className="score-bar">
                            <div
                              style={{
                                width: `${Math.max(0, Math.min(1, r.score)) * 100}%`,
                              }}
                            />
                          </div>
                        )}
                      </td>
                      <td className="small">{r.event_type}</td>
                      <td className="small">{r.description}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>

      <div className="card">
        <h2>Recent events</h2>
        {data?.summary?.length > 0 && (
          <div className="row" style={{ marginBottom: 12 }}>
            {data.summary.map((s) => (
              <span key={s.event_type} className="badge follower">
                {s.event_type} &middot; {s.count}
              </span>
            ))}
          </div>
        )}
        <p className="small muted" style={{ marginTop: 0 }}>
          Stored as documents in MongoDB. Each event type carries its own
          fields &mdash; a partition event has minority and majority arrays
          that no other event has.
        </p>
        {!data ? (
          <div className="muted small">Loading...</div>
        ) : data.events.length === 0 ? (
          <div className="muted small">No events recorded yet.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>when</th>
                <th>node</th>
                <th>type</th>
                <th>description</th>
                {admin && <th />}
              </tr>
            </thead>
            <tbody>
              {data.events.map((e) => (
                <tr key={e.event_id} className="event-row">
                  <td className="small muted" style={{ whiteSpace: "nowrap" }}>
                    {new Date(e.occurred_at).toLocaleTimeString()}
                  </td>
                  <td className="small">{e.node_id}</td>
                  <td className="small">{e.event_type}</td>
                  <td className="small">
                    <span className="desc">{e.description}</span>
                    {e.note && (
                      <span className="note">
                        note ({e.note_author}): {e.note}
                      </span>
                    )}
                  </td>
                  {admin && (
                    <td>
                      <div className="row tight">
                        <button
                          className="small"
                          onClick={() => annotate(e.event_id)}
                        >
                          Note
                        </button>
                        <button
                          className="small danger"
                          onClick={() => remove(e.event_id)}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
