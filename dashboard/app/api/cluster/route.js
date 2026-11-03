/**
 * Live cluster status.
 *
 * Read straight from MySQL through v_cluster_status and
 * v_replication_lag, which is the point: the dashboard is not told the
 * state by the Python process, it reads the same durable state the
 * nodes themselves recover from after a crash. The window functions in
 * those views do the ranking and lag arithmetic server-side.
 *
 * Requires cluster.view, which viewer, operator and admin all hold -
 * operator and admin by inheritance through the role hierarchy.
 */

import { query, clusterApi } from "@/lib/db";
import { requirePermission } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function GET() {
  const guard = await requirePermission("cluster.view");
  if (guard.response) return guard.response;

  const nodes = await query(
    `SELECT node_id, host_address, port, role, current_term, voted_for,
            commit_index, last_applied, is_alive, log_length,
            last_log_index, last_log_term, cluster_max_index,
            entries_behind, freshness_rank
       FROM v_cluster_status
      ORDER BY node_id`
  );

  const lag = await query(
    `SELECT leader_id, follower_id, next_index, match_index,
            leader_last_index, entries_behind, catch_up_rank
       FROM v_replication_lag
      ORDER BY follower_id`
  );

  const quorum = await query(
    `SELECT log_index, term_number, replica_count, cluster_size,
            majority_needed, is_majority_replicated
       FROM v_commit_quorum
      ORDER BY log_index DESC
      LIMIT 12`
  );

  const timeline = await query(
    `SELECT term_number, leader_id, previous_leader, transition
       FROM v_leader_timeline
      ORDER BY term_number DESC
      LIMIT 10`
  );

  // Only the live process knows the in-flight message counters.
  let live = null;
  try {
    const res = await clusterApi("/api/cluster");
    if (res.ok) live = res.body;
  } catch {
    live = null;
  }

  return Response.json({
    nodes,
    lag,
    quorum,
    timeline,
    leader: live?.leader ?? nodes.find((n) => n.role === "leader")?.node_id ?? -1,
    majority: live?.majority ?? Math.floor(nodes.length / 2) + 1,
    messages: live
      ? { sent: live.messages_sent, lost: live.messages_lost }
      : null,
    clusterOnline: live !== null,
  });
}
