/**
 * The MongoDB event stream: Read and Delete.
 *
 * Read goes straight to MongoDB with the official driver. Documents
 * carry type-specific fields - a partition event has minority/majority
 * arrays, an election has a vote tally - which is exactly why this
 * stream lives in a document store rather than a wide nullable table.
 */

import { eventsCollection } from "@/lib/db";
import { requirePermission } from "@/lib/auth";

export const dynamic = "force-dynamic";

export async function GET(request) {
  const guard = await requirePermission("event.read");
  if (guard.response) return guard.response;

  const { searchParams } = new URL(request.url);
  const limit = Math.min(Number(searchParams.get("limit") || 50), 200);
  const nodeId = searchParams.get("node");
  const eventType = searchParams.get("type");

  const filter = {};
  if (nodeId !== null && nodeId !== "") filter.node_id = Number(nodeId);
  if (eventType) filter.event_type = eventType;

  try {
    const coll = await eventsCollection();
    const events = await coll
      .find(filter, { projection: { _id: 0, embedding: 0 } })
      .sort({ occurred_at: -1 })
      .limit(limit)
      .toArray();

    // Aggregation pipeline: counts per event type, for the summary row.
    const summary = await coll
      .aggregate([
        { $group: { _id: "$event_type", count: { $sum: 1 } } },
        { $project: { _id: 0, event_type: "$_id", count: 1 } },
        { $sort: { count: -1 } },
      ])
      .toArray();

    return Response.json({ events, summary, total: await coll.countDocuments() });
  } catch (err) {
    return Response.json(
      { error: "MongoDB is not reachable", detail: String(err.message || err) },
      { status: 503 }
    );
  }
}

export async function DELETE(request) {
  const guard = await requirePermission("cluster.admin");
  if (guard.response) return guard.response;

  const { searchParams } = new URL(request.url);
  const eventId = searchParams.get("event_id");
  if (!eventId) {
    return Response.json({ error: "event_id is required" }, { status: 400 });
  }

  try {
    const coll = await eventsCollection();
    const result = await coll.deleteOne({ event_id: eventId });
    return Response.json({ ok: result.deletedCount > 0 });
  } catch (err) {
    return Response.json({ error: String(err.message || err) }, { status: 503 });
  }
}

/** Update: attach an operator note to an event. */
export async function PATCH(request) {
  const guard = await requirePermission("cluster.admin");
  if (guard.response) return guard.response;

  const { event_id, note, severity } = await request.json().catch(() => ({}));
  if (!event_id) {
    return Response.json({ error: "event_id is required" }, { status: 400 });
  }

  const update = {};
  if (note !== undefined) {
    update.note = note;
    update.note_author = guard.user.username;
    update.note_added_at = new Date();
  }
  // severity is a field no event was created with: a document store
  // takes it without a migration.
  if (severity !== undefined) update.severity = severity;

  if (!Object.keys(update).length) {
    return Response.json({ error: "nothing to update" }, { status: 400 });
  }

  try {
    const coll = await eventsCollection();
    const result = await coll.updateOne({ event_id }, { $set: update });
    return Response.json({ ok: result.modifiedCount > 0 });
  } catch (err) {
    return Response.json({ error: String(err.message || err) }, { status: 503 });
  }
}
