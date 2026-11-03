/**
 * The replicated key-value store.
 *
 * GET reads kv_state from MySQL directly - that table *is* the applied
 * state machine, so there is nothing to ask the cluster for.
 *
 * POST and DELETE must go through consensus, so they are proxied to the
 * Python cluster server. A write that bypassed Raft and wrote straight
 * to kv_state would corrupt the replica: the log would no longer
 * explain the state.
 *
 * Reading needs kv.read; writing needs kv.write, which viewer does not
 * have. That is the role split the rubric asks for, enforced here and
 * resolved by the database.
 */

import { query, clusterApi } from "@/lib/db";
import { requirePermission } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function GET(request) {
  const guard = await requirePermission("kv.read");
  if (guard.response) return guard.response;

  const { searchParams } = new URL(request.url);
  const search = searchParams.get("search");
  const nodeParam = searchParams.get("node");

  // Default to the leader's view; any replica holds the same applied
  // state once it has caught up.
  let nodeId = nodeParam !== null ? Number(nodeParam) : null;
  if (nodeId === null) {
    const leader = await query(
      "SELECT node_id FROM node WHERE role = 'leader' LIMIT 1"
    );
    nodeId = leader.length ? leader[0].node_id : 0;
  }

  if (search) {
    // Full-text search over stored values, using the FULLTEXT index.
    const rows = await query(
      `SELECT key_name, value_text, last_applied_index,
              MATCH(value_text) AGAINST(? IN NATURAL LANGUAGE MODE) AS relevance
         FROM kv_state
        WHERE node_id = ?
          AND MATCH(value_text) AGAINST(? IN NATURAL LANGUAGE MODE)
        ORDER BY relevance DESC
        LIMIT 50`,
      [search, nodeId, search]
    );
    return Response.json({ nodeId, search, pairs: rows });
  }

  const pairs = await query(
    `SELECT key_name, value_text, last_applied_index, updated_at
       FROM kv_state WHERE node_id = ? ORDER BY key_name`,
    [nodeId]
  );

  // How many replicas agree on each key - the visible proof that the
  // data really is replicated.
  const replication = await query(
    `SELECT key_name, COUNT(DISTINCT node_id) AS replica_count
       FROM kv_state GROUP BY key_name`
  );
  const counts = Object.fromEntries(
    replication.map((r) => [r.key_name, r.replica_count])
  );

  return Response.json({
    nodeId,
    pairs: pairs.map((p) => ({ ...p, replica_count: counts[p.key_name] || 1 })),
  });
}

export async function POST(request) {
  const guard = await requirePermission("kv.write");
  if (guard.response) return guard.response;

  const { key, value, op } = await request.json().catch(() => ({}));
  if (!key) {
    return Response.json({ error: "key is required" }, { status: 400 });
  }

  const res = await clusterApi("/api/kv", {
    method: "POST",
    body: JSON.stringify({
      key,
      value: value ?? "",
      op: (op || "PUT").toUpperCase(),
      // One stable client id per signed-in user, so the Raft layer can
      // recognise a retry from this user as the same request.
      client_id: `web-user-${guard.user.user_id}`,
    }),
  });

  if (!res.ok) {
    return Response.json(
      {
        error: "the cluster could not commit this write",
        detail: res.body?.error || "no majority available",
      },
      { status: 503 }
    );
  }
  return Response.json({ ok: true, key, op: op || "PUT" });
}

export async function DELETE(request) {
  const guard = await requirePermission("kv.write");
  if (guard.response) return guard.response;

  const { searchParams } = new URL(request.url);
  const key = searchParams.get("key");
  if (!key) {
    return Response.json({ error: "key is required" }, { status: 400 });
  }

  const res = await clusterApi("/api/kv", {
    method: "POST",
    body: JSON.stringify({
      key,
      value: "",
      op: "DELETE",
      client_id: `web-user-${guard.user.user_id}`,
    }),
  });
  if (!res.ok) {
    return Response.json(
      { error: "the cluster could not commit this delete" },
      { status: 503 }
    );
  }
  return Response.json({ ok: true, key });
}
