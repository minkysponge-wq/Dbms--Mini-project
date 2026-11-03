/**
 * Crash / restart a node. Admin only.
 *
 * Proxied to the Python cluster server because it acts on the live
 * process. The permission is resolved by the database through
 * v_user_permission, so an operator gets a 403 here while an admin
 * does not - and neither is hardcoded in this file.
 */

import { clusterApi } from "@/lib/db";
import { requirePermission } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function POST(request) {
  const guard = await requirePermission("node.kill");
  if (guard.response) return guard.response;

  const { node_id } = await request.json().catch(() => ({}));
  if (node_id === undefined || node_id === null) {
    return Response.json({ error: "node_id is required" }, { status: 400 });
  }

  const res = await clusterApi("/api/node/kill", {
    method: "POST",
    body: JSON.stringify({ node_id: Number(node_id) }),
  });
  if (!res.ok) {
    return Response.json(
      { error: "the cluster server is not reachable", detail: res.body?.error },
      { status: 503 }
    );
  }
  return Response.json({ ok: true, node_id: Number(node_id), action: "kill" });
}
