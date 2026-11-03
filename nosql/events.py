"""MongoDB event store with vector search.

Why a document store
--------------------
The cluster emits events whose shape genuinely differs by type. An
election carries a term, a candidate and a vote tally; a partition
carries two groups of node ids; a commit advance carries two indexes; a
client request carries a key and a sequence number. Forcing these into
one relational table means a dozen mostly-null columns and a new
migration every time a new event type appears. Documents fit the data,
and operators query these by "what happened around 14:32" rather than
by join.

The relational side keeps what needs constraints and transactions - the
log, the votes, the state machine. The document side keeps the
append-only observability stream. Each database holds what it is good
at, which is the whole argument for using both.

Vector search
-------------
Each event's description is embedded on write. Similarity search lets
an operator ask "show me things like this" without knowing the exact
wording - useful because event text varies by type.

MongoDB Community has no native vector index; ``$vectorSearch`` is an
Atlas feature. Cosine similarity is therefore computed in an
aggregation pipeline using ``$zip`` and ``$reduce``, so the arithmetic
runs server-side. That is exactly what a vector database does
internally, minus the approximate-nearest-neighbour index that makes it
fast at millions of documents. ``PYTHON_FALLBACK`` covers deployments
whose server cannot run the pipeline.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from .embedding import DIMENSION, cosine_similarity, embed

COLLECTION = "cluster_events"
DEFAULT_URI = "mongodb://127.0.0.1:27017"
DEFAULT_DB = "raftkv_events"


class EventStore:
    """Create, read, update and delete cluster events in MongoDB."""

    def __init__(self, client: Any = None, db_name: str | None = None,
                 uri: str | None = None, connect_timeout_ms: int = 2000) -> None:
        self.db_name = db_name or os.environ.get("RAFTKV_MONGO_DB", DEFAULT_DB)
        self._owns_client = client is None

        if client is None:
            from pymongo import MongoClient
            uri = uri or os.environ.get("RAFTKV_MONGO_URI", DEFAULT_URI)
            client = MongoClient(uri, serverSelectionTimeoutMS=connect_timeout_ms)

        self.client = client
        self.db = client[self.db_name]
        self.events = self.db[COLLECTION]

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def create_indexes(self) -> list[str]:
        """Create the collection's indexes. Idempotent."""
        created = []
        try:
            created.append(self.events.create_index(
                [("node_id", 1), ("occurred_at", -1)], name="idx_node_time"))
            created.append(self.events.create_index(
                [("event_type", 1)], name="idx_event_type"))
            created.append(self.events.create_index(
                [("term", 1)], name="idx_term"))
            created.append(self.events.create_index(
                [("event_id", 1)], name="uq_event_id", unique=True))
        except Exception:
            pass
        try:
            # Text index for keyword search alongside the vector search.
            created.append(self.events.create_index(
                [("description", "text")], name="txt_description"))
        except Exception:
            pass
        return [c for c in created if c]

    def ping(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except Exception:
            return False

    def close(self) -> None:
        if self._owns_client:
            try:
                self.client.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------

    def record(self, event: dict[str, Any]) -> str:
        """Insert one event, embedding its description.

        Returns the event_id. Unknown keys are kept as-is: that is the
        point of a document store, and it is why a new event type needs
        no migration.
        """
        doc = dict(event)
        doc.setdefault("event_id", str(uuid.uuid4()))
        doc.setdefault("occurred_at", datetime.now(timezone.utc))
        doc.setdefault("description", "")
        if isinstance(doc.get("ts"), (int, float)):
            doc["occurred_at"] = datetime.fromtimestamp(
                doc.pop("ts"), tz=timezone.utc)
        doc["embedding"] = embed(doc["description"])
        doc["embedding_dim"] = DIMENSION
        self.events.insert_one(doc)
        return doc["event_id"]

    def record_many(self, events: Iterable[dict[str, Any]]) -> int:
        docs = []
        for event in events:
            doc = dict(event)
            doc.setdefault("event_id", str(uuid.uuid4()))
            doc.setdefault("occurred_at", datetime.now(timezone.utc))
            doc.setdefault("description", "")
            if isinstance(doc.get("ts"), (int, float)):
                doc["occurred_at"] = datetime.fromtimestamp(
                    doc.pop("ts"), tz=timezone.utc)
            doc["embedding"] = embed(doc["description"])
            doc["embedding_dim"] = DIMENSION
            docs.append(doc)
        if not docs:
            return 0
        self.events.insert_many(docs)
        return len(docs)

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    def get(self, event_id: str) -> dict[str, Any] | None:
        return self.events.find_one({"event_id": event_id},
                                    {"_id": 0, "embedding": 0})

    def find(self, node_id: int | None = None, event_type: str | None = None,
             term: int | None = None, limit: int = 50) -> list[dict[str, Any]]:
        query: dict[str, Any] = {}
        if node_id is not None:
            query["node_id"] = node_id
        if event_type is not None:
            query["event_type"] = event_type
        if term is not None:
            query["term"] = term
        cursor = (self.events.find(query, {"_id": 0, "embedding": 0})
                  .sort("occurred_at", -1).limit(limit))
        return list(cursor)

    def count(self) -> int:
        return self.events.count_documents({})

    def summary_by_type(self) -> list[dict[str, Any]]:
        """Aggregation pipeline: how many events of each type, per node."""
        pipeline = [
            {"$group": {
                "_id": {"event_type": "$event_type", "node_id": "$node_id"},
                "count": {"$sum": 1},
            }},
            {"$project": {
                "_id": 0,
                "event_type": "$_id.event_type",
                "node_id": "$_id.node_id",
                "count": 1,
            }},
            {"$sort": {"count": -1}},
        ]
        return list(self.events.aggregate(pipeline))

    def text_search(self, text: str, limit: int = 10) -> list[dict[str, Any]]:
        """Keyword search using MongoDB's text index."""
        try:
            cursor = (self.events
                      .find({"$text": {"$search": text}},
                            {"_id": 0, "embedding": 0})
                      .limit(limit))
            return list(cursor)
        except Exception:
            # No text index available: fall back to a regex scan.
            cursor = (self.events
                      .find({"description": {"$regex": text, "$options": "i"}},
                            {"_id": 0, "embedding": 0})
                      .limit(limit))
            return list(cursor)

    # ------------------------------------------------------------------
    # VECTOR SEARCH
    # ------------------------------------------------------------------

    def vector_search(self, text: str, limit: int = 5,
                      min_score: float = 0.0,
                      prefer_pipeline: bool = True) -> list[dict[str, Any]]:
        """Find the events most similar to ``text``.

        Tries the server-side aggregation pipeline first and falls back
        to computing in Python if the server cannot run it.
        """
        query_vec = embed(text)
        if not any(query_vec):
            return []

        if prefer_pipeline:
            try:
                results = self._vector_search_pipeline(query_vec, limit, min_score)
                if results is not None:
                    return results
            except Exception:
                pass
        return self._vector_search_python(query_vec, limit, min_score)

    def _vector_search_pipeline(self, query_vec: list[float], limit: int,
                                min_score: float) -> list[dict[str, Any]] | None:
        """Cosine similarity computed inside MongoDB.

        $zip pairs the stored embedding with the query vector elementwise
        and $reduce sums the products. Both vectors are already
        L2-normalised, so that dot product *is* the cosine similarity.
        """
        pipeline = [
            {"$match": {"embedding": {"$exists": True}}},
            {"$addFields": {
                "score": {
                    "$reduce": {
                        "input": {"$zip": {"inputs": ["$embedding", query_vec]}},
                        "initialValue": 0.0,
                        "in": {
                            "$add": [
                                "$$value",
                                {"$multiply": [
                                    {"$arrayElemAt": ["$$this", 0]},
                                    {"$arrayElemAt": ["$$this", 1]},
                                ]},
                            ]
                        },
                    }
                }
            }},
            {"$match": {"score": {"$gte": min_score}}},
            {"$sort": {"score": -1}},
            {"$limit": limit},
            {"$project": {"_id": 0, "embedding": 0}},
        ]
        results = list(self.events.aggregate(pipeline))
        # mongomock and some older servers silently return no score.
        if results and "score" not in results[0]:
            return None
        return results

    def _vector_search_python(self, query_vec: list[float], limit: int,
                              min_score: float) -> list[dict[str, Any]]:
        scored = []
        for doc in self.events.find({"embedding": {"$exists": True}}):
            score = cosine_similarity(doc.get("embedding", []), query_vec)
            if score < min_score:
                continue
            doc.pop("_id", None)
            doc.pop("embedding", None)
            doc["score"] = score
            scored.append(doc)
        scored.sort(key=lambda d: d["score"], reverse=True)
        return scored[:limit]

    def similar_to(self, event_id: str, limit: int = 5) -> list[dict[str, Any]]:
        """Events resembling a given one, excluding itself."""
        source = self.events.find_one({"event_id": event_id})
        if not source:
            return []
        results = self.vector_search(source.get("description", ""),
                                     limit=limit + 1)
        return [r for r in results if r.get("event_id") != event_id][:limit]

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    def annotate(self, event_id: str, note: str, author: str = "operator") -> bool:
        """Attach an operator note. Re-embeds so the note is searchable
        along with the original description."""
        source = self.events.find_one({"event_id": event_id})
        if not source:
            return False
        combined = f"{source.get('description', '')} {note}".strip()
        result = self.events.update_one(
            {"event_id": event_id},
            {"$set": {
                "note": note,
                "note_author": author,
                "note_added_at": datetime.now(timezone.utc),
                "embedding": embed(combined),
            }},
        )
        return result.modified_count > 0

    def set_severity(self, event_id: str, severity: str) -> bool:
        """Add or change a field that was never part of the schema.

        A document store needs no migration for this, which is a large
        part of why the event stream lives here rather than in MySQL.
        """
        result = self.events.update_one(
            {"event_id": event_id}, {"$set": {"severity": severity}})
        return result.modified_count > 0

    def tag_many(self, event_type: str, tag: str) -> int:
        """Add a tag to every event of a type, using $addToSet so
        repeating the call does not duplicate the tag."""
        result = self.events.update_many(
            {"event_type": event_type}, {"$addToSet": {"tags": tag}})
        return result.modified_count

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    def delete(self, event_id: str) -> bool:
        return self.events.delete_one({"event_id": event_id}).deleted_count > 0

    def purge_before(self, cutoff: datetime) -> int:
        """Retention: drop events older than a cutoff."""
        return self.events.delete_many(
            {"occurred_at": {"$lt": cutoff}}).deleted_count

    def drop_all(self) -> None:
        self.events.delete_many({})


# ----------------------------------------------------------------------
# Connection helper
# ----------------------------------------------------------------------

def open_store(fallback_to_mock: bool = False) -> tuple[EventStore | None, str]:
    """Open an EventStore, reporting what happened.

    Returns (store, message). When MongoDB is unreachable and
    ``fallback_to_mock`` is set, an in-memory mongomock store is
    returned instead so the rest of the system still runs - useful in
    tests and when demonstrating without a server.
    """
    try:
        store = EventStore()
        if store.ping():
            store.create_indexes()
            return store, f"connected to MongoDB at {store.db_name}"
        store.close()
    except Exception as exc:  # pragma: no cover - depends on environment
        last_error = str(exc)
    else:
        last_error = "server did not respond to ping"

    if fallback_to_mock:
        try:
            import mongomock
            store = EventStore(client=mongomock.MongoClient())
            store.create_indexes()
            return store, "MongoDB unreachable - using in-memory mongomock"
        except ImportError:
            pass
    return None, f"MongoDB unavailable: {last_error}"
