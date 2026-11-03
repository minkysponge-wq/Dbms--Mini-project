/**
 * Vector and keyword search over the event stream.
 *
 * Vector mode embeds the query with the same hashed n-gram embedder the
 * Python side uses, then computes cosine similarity inside MongoDB with
 * an aggregation pipeline: $zip pairs the stored embedding with the
 * query vector elementwise and $reduce sums the products. Both vectors
 * are L2-normalised, so that dot product is the cosine similarity.
 *
 * MongoDB Community has no native vector index - $vectorSearch is an
 * Atlas feature - so this is an exact search rather than an approximate
 * one. At this scale that is the right trade: exact results, no index
 * to build. A production corpus would want HNSW.
 */

import { eventsCollection } from "@/lib/db";
import { requirePermission } from "@/lib/auth";
import { embed } from "@/lib/embedding";

export const dynamic = "force-dynamic";

export async function GET(request) {
  const guard = await requirePermission("event.read");
  if (guard.response) return guard.response;

  const { searchParams } = new URL(request.url);
  const q = searchParams.get("q") || "";
  const mode = searchParams.get("mode") || "vector";
  const limit = Math.min(Number(searchParams.get("limit") || 10), 50);

  if (!q.trim()) {
    return Response.json({ error: "q is required" }, { status: 400 });
  }

  try {
    const coll = await eventsCollection();

    if (mode === "text") {
      let results;
      try {
        results = await coll
          .find(
            { $text: { $search: q } },
            { projection: { _id: 0, embedding: 0, score: { $meta: "textScore" } } }
          )
          .sort({ score: { $meta: "textScore" } })
          .limit(limit)
          .toArray();
      } catch {
        // No text index: fall back to a case-insensitive regex scan.
        results = await coll
          .find(
            { description: { $regex: q, $options: "i" } },
            { projection: { _id: 0, embedding: 0 } }
          )
          .limit(limit)
          .toArray();
      }
      return Response.json({ mode: "text", query: q, results });
    }

    const queryVector = embed(q);
    const results = await coll
      .aggregate([
        { $match: { embedding: { $exists: true } } },
        {
          $addFields: {
            score: {
              $reduce: {
                input: { $zip: { inputs: ["$embedding", queryVector] } },
                initialValue: 0,
                in: {
                  $add: [
                    "$$value",
                    {
                      $multiply: [
                        { $arrayElemAt: ["$$this", 0] },
                        { $arrayElemAt: ["$$this", 1] },
                      ],
                    },
                  ],
                },
              },
            },
          },
        },
        { $sort: { score: -1 } },
        { $limit: limit },
        { $project: { _id: 0, embedding: 0 } },
      ])
      .toArray();

    return Response.json({ mode: "vector", query: q, results });
  } catch (err) {
    return Response.json(
      { error: "search failed", detail: String(err.message || err) },
      { status: 503 }
    );
  }
}
