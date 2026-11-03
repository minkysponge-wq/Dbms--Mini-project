/**
 * Partition the network, or heal it. Admin only.
 *
 * This is the control that makes the demo worth watching: isolate the
 * leader with one follower and the remaining three elect a new leader
 * and keep serving, while the minority side stops committing.
 */

import { clusterApi } from "@/lib/db";
import { requirePermission } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function POST(request) {
  const guard = await requirePermission("cluster.admin");
  if (guard.response) return guard.response;

  const { minority } = await request.json().catch(() => ({}));
  if (!Array.isArray(minority) || minority.length === 0) {
    return Response.json(
      { error: "minority must be a non-empty array of node ids" },
      { status: 400 }
    );
  }

  const res = await clusterApi("/api/partition", {
    method: "POST",
    body: JSON.stringify({ minority: minority.map(Number) }),
  });
  if (!res.ok) {
    return Response.json(
      { error: "the cluster server is not reachable" },
      { status: 503 }
    );
  }
  return Response.json({ ok: true, minority });
}

export async function DELETE() {
  const guard = await requirePermission("cluster.admin");
  if (guard.response) return guard.response;

  const res = await clusterApi("/api/heal", { method: "POST" });
  if (!res.ok) {
    return Response.json(
      { error: "the cluster server is not reachable" },
      { status: 503 }
    );
  }
  return Response.json({ ok: true, healed: true });
}
