"""Correctness tests for the Raft layer.

Run with:   python -m unittest discover -s tests -v

Uses unittest rather than pytest so there is nothing extra to install.
Each test builds its own five-node cluster against the real MySQL
schema, so these are integration tests: they exercise the triggers,
constraints and transactions as well as the Python.
"""

from __future__ import annotations

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raft import db                      # noqa: E402
from raft.cluster import Cluster         # noqa: E402
from raft.node import Command            # noqa: E402


class RaftTestCase(unittest.TestCase):
    """Shared setup: skip the whole suite if MySQL is not reachable."""

    cluster: Cluster | None = None

    @classmethod
    def setUpClass(cls) -> None:
        if not db.ping():
            raise unittest.SkipTest(
                "MySQL not reachable or schema missing - run setup.py first")

    def tearDown(self) -> None:
        if self.cluster is not None:
            self.cluster.shutdown()
            self.cluster = None
        time.sleep(0.2)

    def assert_single_leader_per_term(self, cluster: Cluster) -> None:
        for term, count in cluster.leaders_by_term().items():
            self.assertLessEqual(
                count, 1, f"safety violation: {count} leaders in term {term}")


class TestElection(RaftTestCase):

    def test_elects_exactly_one_leader(self) -> None:
        self.cluster = Cluster(size=5, seed=101)
        leader = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(leader, 0, "no leader was elected")
        self.assert_single_leader_per_term(self.cluster)

    def test_reelection_after_leader_crash(self) -> None:
        self.cluster = Cluster(size=5, seed=102)
        first = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(first, 0)

        self.cluster.crash(first)
        second = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(second, 0, "no re-election after leader crash")
        self.assertNotEqual(second, first, "crashed node still reports as leader")
        self.assert_single_leader_per_term(self.cluster)

    def test_minority_cannot_elect_or_commit(self) -> None:
        """Three of five down, including the old leader: the surviving two
        must never elect a leader or commit anything.

        An isolated node that was *already* leader keeps believing it
        leads until it hears a higher term - that is correct Raft, not a
        safety bug, because it can no longer reach a majority. So the old
        leader is crashed first and the assertion is about no *new*
        leader appearing.
        """
        self.cluster = Cluster(size=5, seed=103)
        leader = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(leader, 0)

        crashed = {leader}
        self.cluster.crash(leader)
        for i in range(5):
            if len(crashed) >= 3:
                break
            if i not in crashed:
                crashed.add(i)
                self.cluster.crash(i)

        time.sleep(6)
        self.assertEqual(self.cluster.leader(), -1,
                         "a minority of 2/5 elected a leader: split-brain is possible")

        clerk = self.cluster.clerk()
        self.assertFalse(clerk.put("should", "fail", timeout=4),
                         "a minority committed a write")


class TestReplication(RaftTestCase):

    def test_writes_commit_and_read_back(self) -> None:
        self.cluster = Cluster(size=5, seed=201)
        self.assertGreaterEqual(self.cluster.wait_for_leader(20), 0)

        clerk = self.cluster.clerk()
        for i in range(5):
            self.assertTrue(clerk.put(f"key{i}", f"value{i}"),
                            f"put key{i} failed")
        for i in range(5):
            self.assertEqual(clerk.get(f"key{i}"), f"value{i}")

    def test_acknowledged_write_survives_leader_crash(self) -> None:
        """The core durability claim of the whole system."""
        self.cluster = Cluster(size=5, seed=202)
        leader = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(leader, 0)

        clerk = self.cluster.clerk()
        self.assertTrue(clerk.put("durable", "yes"))

        self.cluster.crash(leader)
        self.assertGreaterEqual(self.cluster.wait_for_leader(20), 0,
                                "no new leader after crash")
        self.assertEqual(clerk.get("durable"), "yes",
                         "a committed write was lost when its leader died")

    def test_replicas_converge(self) -> None:
        self.cluster = Cluster(size=5, seed=203)
        self.assertGreaterEqual(self.cluster.wait_for_leader(20), 0)

        clerk = self.cluster.clerk()
        for i in range(4):
            clerk.put(f"conv{i}", f"v{i}")
        time.sleep(3)

        snapshots = []
        for node_id in range(5):
            replica = self.cluster.replica(node_id)
            if replica is not None:
                snapshots.append((node_id, replica.store.snapshot()))

        reference_id, reference = snapshots[0]
        for node_id, snap in snapshots[1:]:
            for key, value in reference.items():
                if key in snap:
                    self.assertEqual(
                        snap[key], value,
                        f"nodes {reference_id} and {node_id} disagree on {key}")


class TestPersistence(RaftTestCase):

    def test_data_survives_full_cluster_restart(self) -> None:
        """Every node is killed and rebuilt from MySQL alone."""
        self.cluster = Cluster(size=5, seed=301)
        self.assertGreaterEqual(self.cluster.wait_for_leader(20), 0)

        clerk = self.cluster.clerk()
        for i in range(3):
            self.assertTrue(clerk.put(f"persist{i}", f"value{i}"))
        time.sleep(2)

        for node_id in range(5):
            self.cluster.crash(node_id)
        time.sleep(1)
        for node_id in range(5):
            self.cluster.restart(node_id)

        self.assertGreaterEqual(self.cluster.wait_for_leader(25), 0,
                                "no leader after a full cluster restart")
        clerk2 = self.cluster.clerk()
        for i in range(3):
            self.assertEqual(clerk2.get(f"persist{i}"), f"value{i}",
                             f"persist{i} did not survive the restart")

    def test_vote_is_durable(self) -> None:
        """A node's vote must still be in MySQL after it votes, or it
        could come back from a crash and vote twice in the same term."""
        self.cluster = Cluster(size=5, seed=302)
        leader = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(leader, 0)

        rows = db.query(
            "SELECT term_number, voter_id, candidate_id FROM vote "
            "WHERE granted = TRUE ORDER BY term_number, voter_id")
        self.assertGreater(len(rows), 0, "no votes were persisted")

        seen: set[tuple[int, int]] = set()
        for row in rows:
            key = (int(row["term_number"]), int(row["voter_id"]))
            self.assertNotIn(
                key, seen,
                f"node {row['voter_id']} voted twice in term {row['term_number']}")
            seen.add(key)


class TestPartition(RaftTestCase):

    def test_majority_side_keeps_serving(self) -> None:
        self.cluster = Cluster(size=5, seed=401)
        old_leader = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(old_leader, 0)

        clerk = self.cluster.clerk()
        self.assertTrue(clerk.put("before", "partition"))

        # Isolate the leader with one follower; three form the majority.
        minority = {old_leader}
        for i in range(5):
            if len(minority) >= 2:
                break
            if i != old_leader:
                minority.add(i)
        self.cluster.net.set_partition(
            {i: (1 if i in minority else 2) for i in range(5)})

        time.sleep(8)
        majority_leader = -1
        for i in range(5):
            if i in minority:
                continue
            replica = self.cluster.replica(i)
            if replica is not None and replica.raft.is_leader():
                majority_leader = i
        self.assertGreaterEqual(
            majority_leader, 0, "the majority partition failed to elect a leader")

        self.cluster.net.heal()
        time.sleep(4)
        self.assert_single_leader_per_term(self.cluster)
        self.assertEqual(clerk.get("before"), "partition",
                         "a pre-partition write was lost")


class TestExactlyOnce(RaftTestCase):

    def test_retried_append_is_applied_once(self) -> None:
        """Without client sessions this is at-least-once and the value
        would be wrong."""
        self.cluster = Cluster(size=5, seed=501)
        leader_id = self.cluster.wait_for_leader(20)
        self.assertGreaterEqual(leader_id, 0)

        replica = self.cluster.replica(leader_id)
        self.assertIsNotNone(replica)

        # The same (client_id, seq_no) submitted three times, as a
        # retrying client would after timeouts.
        cmd = Command(op="APPEND", key="counter", value="x",
                      client_id="dedup-client", seq_no=1)
        for _ in range(3):
            replica.submit(cmd, timeout=5)
        time.sleep(2)

        clerk = self.cluster.clerk()
        self.assertEqual(clerk.get("counter"), "x",
                         "a duplicate request was applied more than once")


if __name__ == "__main__":
    unittest.main(verbosity=2)
