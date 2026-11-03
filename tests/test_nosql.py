"""Tests for the MongoDB event store and vector search.

Run with:   python -m unittest discover -s tests -v

These run against a real MongoDB when one is reachable, and against
mongomock otherwise, so the suite is green on a machine with no server
installed. ``RAFTKV_MONGO_FORCE_MOCK=1`` forces the in-memory path.
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nosql.embedding import cosine_similarity, embed      # noqa: E402
from nosql.events import EventStore                        # noqa: E402

SAMPLE_EVENTS = [
    {"node_id": 0, "event_type": "election_started", "term": 1,
     "description": "node 0 timed out and began an election for term 1"},
    {"node_id": 0, "event_type": "became_leader", "term": 1,
     "description": "node 0 won the election for term 1 with a majority",
     "votes_received": 3, "majority_needed": 3},
    {"node_id": 2, "event_type": "crash", "term": 1,
     "description": "node 2 crashed and left the cluster"},
    {"node_id": 2, "event_type": "restart", "term": 1,
     "description": "node 2 restarted and recovered state from stable storage"},
    {"node_id": 1, "event_type": "partition", "term": 2,
     "description": "network partition isolated nodes 1 and 3 from the majority",
     "minority": [1, 3], "majority": [0, 2, 4]},
    {"node_id": 0, "event_type": "commit_advance", "term": 2,
     "description": "node 0 advanced commit index from 4 to 7",
     "old_index": 4, "new_index": 7},
]


def make_store() -> EventStore:
    """A real store when MongoDB answers, mongomock otherwise."""
    if os.environ.get("RAFTKV_MONGO_FORCE_MOCK") != "1":
        try:
            store = EventStore(db_name="raftkv_events_test",
                               connect_timeout_ms=800)
            if store.ping():
                store.drop_all()
                store.create_indexes()
                return store
            store.close()
        except Exception:
            pass
    import mongomock
    store = EventStore(client=mongomock.MongoClient(),
                       db_name="raftkv_events_test")
    store.create_indexes()
    return store


class TestEmbedding(unittest.TestCase):

    def test_embedding_is_normalised(self) -> None:
        vec = embed("node 0 won the election for term 1")
        norm = sum(v * v for v in vec) ** 0.5
        self.assertAlmostEqual(norm, 1.0, places=6)

    def test_identical_text_scores_one(self) -> None:
        text = "node 2 crashed and left the cluster"
        self.assertAlmostEqual(
            cosine_similarity(embed(text), embed(text)), 1.0, places=6)

    def test_related_text_beats_unrelated(self) -> None:
        query = embed("node crashed")
        related = cosine_similarity(query, embed("node 4 crashed and left"))
        unrelated = cosine_similarity(
            query, embed("the quick brown fox jumps over the lazy dog"))
        self.assertGreater(related, unrelated)

    def test_empty_text_is_safe(self) -> None:
        self.assertEqual(sum(embed("")), 0.0)
        self.assertEqual(cosine_similarity(embed(""), embed("anything")), 0.0)


class TestEventStoreCRUD(unittest.TestCase):

    def setUp(self) -> None:
        self.store = make_store()
        self.store.record_many(SAMPLE_EVENTS)

    def tearDown(self) -> None:
        self.store.drop_all()
        self.store.close()

    # -- CREATE ---------------------------------------------------------

    def test_create_stores_every_event(self) -> None:
        self.assertEqual(self.store.count(), len(SAMPLE_EVENTS))

    def test_create_keeps_type_specific_fields(self) -> None:
        """The reason for using a document store: a partition event
        carries fields no other event type has, with no schema change."""
        partition = self.store.find(event_type="partition")[0]
        self.assertEqual(partition["minority"], [1, 3])
        self.assertEqual(partition["majority"], [0, 2, 4])

        leader = self.store.find(event_type="became_leader")[0]
        self.assertEqual(leader["votes_received"], 3)
        self.assertNotIn("minority", leader)

    # -- READ -----------------------------------------------------------

    def test_read_filters_by_node_and_type(self) -> None:
        self.assertEqual(len(self.store.find(node_id=2)), 2)
        self.assertEqual(len(self.store.find(event_type="crash")), 1)
        self.assertEqual(len(self.store.find(term=2)), 2)

    def test_read_single_event_by_id(self) -> None:
        event_id = self.store.find(event_type="crash")[0]["event_id"]
        fetched = self.store.get(event_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["event_type"], "crash")
        self.assertNotIn("embedding", fetched)

    def test_aggregation_summary(self) -> None:
        summary = self.store.summary_by_type()
        self.assertTrue(summary)
        total = sum(row["count"] for row in summary)
        self.assertEqual(total, len(SAMPLE_EVENTS))

    # -- UPDATE ---------------------------------------------------------

    def test_update_annotates_and_reembeds(self) -> None:
        event_id = self.store.find(event_type="crash")[0]["event_id"]
        self.assertTrue(self.store.annotate(event_id, "disk failure suspected"))

        updated = self.store.get(event_id)
        self.assertEqual(updated["note"], "disk failure suspected")

        # The note is now searchable, which only works because the
        # document was re-embedded on update.
        hits = self.store.vector_search("disk failure", limit=3)
        self.assertIn(event_id, [h["event_id"] for h in hits])

    def test_update_adds_a_field_that_never_existed(self) -> None:
        event_id = self.store.find(event_type="partition")[0]["event_id"]
        self.assertTrue(self.store.set_severity(event_id, "critical"))
        self.assertEqual(self.store.get(event_id)["severity"], "critical")

    def test_update_many_tags_a_whole_type(self) -> None:
        modified = self.store.tag_many("crash", "needs-review")
        self.assertEqual(modified, 1)
        self.assertIn("needs-review", self.store.find(event_type="crash")[0]["tags"])

    # -- DELETE ---------------------------------------------------------

    def test_delete_one(self) -> None:
        event_id = self.store.find(event_type="crash")[0]["event_id"]
        self.assertTrue(self.store.delete(event_id))
        self.assertIsNone(self.store.get(event_id))
        self.assertEqual(self.store.count(), len(SAMPLE_EVENTS) - 1)

    def test_delete_by_retention_cutoff(self) -> None:
        self.store.record({
            "node_id": 9, "event_type": "crash", "term": 0,
            "description": "an ancient event",
            "occurred_at": datetime.now(timezone.utc) - timedelta(days=30),
        })
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        self.assertEqual(self.store.purge_before(cutoff), 1)
        self.assertEqual(self.store.count(), len(SAMPLE_EVENTS))


class TestVectorSearch(unittest.TestCase):

    def setUp(self) -> None:
        self.store = make_store()
        self.store.record_many(SAMPLE_EVENTS)

    def tearDown(self) -> None:
        self.store.drop_all()
        self.store.close()

    def test_finds_the_relevant_event(self) -> None:
        hits = self.store.vector_search("which node crashed", limit=3)
        self.assertTrue(hits)
        self.assertIn("crash", hits[0]["description"])

    def test_results_are_ordered_by_score(self) -> None:
        hits = self.store.vector_search("election leader term", limit=5)
        scores = [h["score"] for h in hits]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertLessEqual(scores[0], 1.0001)

    def test_pipeline_and_python_paths_agree(self) -> None:
        """The server-side pipeline and the Python fallback must rank
        results the same way, or the fallback is not a fallback."""
        pipeline = self.store.vector_search("node crashed", limit=4,
                                            prefer_pipeline=True)
        python = self.store.vector_search("node crashed", limit=4,
                                          prefer_pipeline=False)
        self.assertEqual([h["event_id"] for h in pipeline],
                         [h["event_id"] for h in python])

    def test_similar_to_excludes_itself(self) -> None:
        crash_id = self.store.find(event_type="crash")[0]["event_id"]
        similar = self.store.similar_to(crash_id, limit=3)
        self.assertNotIn(crash_id, [s["event_id"] for s in similar])

    def test_min_score_filters(self) -> None:
        strict = self.store.vector_search("node crashed", limit=10,
                                          min_score=0.9)
        loose = self.store.vector_search("node crashed", limit=10,
                                         min_score=0.0)
        self.assertLessEqual(len(strict), len(loose))


if __name__ == "__main__":
    unittest.main(verbosity=2)
