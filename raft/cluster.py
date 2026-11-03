"""Cluster harness and client.

``Cluster`` owns a set of replicas and the network between them, and is
what the tests, the chaos runner, the demo and the web API all drive.
``Clerk`` is a client that retries across replicas, carrying a stable
(client_id, seq_no) so the servers can recognise a retry.
"""

from __future__ import annotations

import itertools
import threading
import time
from typing import Any, Callable

from . import db
from .node import ApplyMsg, Command, RaftNode
from .store import KVStore
from .transport import Network


class Replica:
    """A Raft node plus the state machine it drives."""

    def __init__(self, node_id: int, peers: list[int], net: Network,
                 event_sink: Callable[[dict], None] | None = None) -> None:
        self.node_id = node_id
        self.store = KVStore(node_id)
        self._waiters: dict[int, threading.Event] = {}
        self._results: dict[int, dict] = {}
        self._waiters_lock = threading.Lock()

        self.raft = RaftNode(
            node_id, peers, net,
            apply_fn=self._on_apply,
            event_sink=event_sink,
        )
        # A restarted node may hold committed entries it never applied.
        try:
            self.store.catch_up()
        except Exception:
            pass

    def _on_apply(self, msg: ApplyMsg) -> None:
        result = self.store.apply(msg)
        with self._waiters_lock:
            ev = self._waiters.get(msg.index)
            if ev is not None:
                self._results[msg.index] = result
                ev.set()

    def submit(self, cmd: Command, timeout: float = 5.0) -> dict[str, Any]:
        """Replicate one command and wait for it to be applied here."""
        index, _, is_leader = self.raft.start(cmd)
        if not is_leader:
            return {"ok": False, "err": "not_leader"}

        ev = threading.Event()
        with self._waiters_lock:
            self._waiters[index] = ev
        try:
            if not ev.wait(timeout):
                return {"ok": False, "err": "timeout"}
            with self._waiters_lock:
                return self._results.get(index, {"ok": True})
        finally:
            with self._waiters_lock:
                self._waiters.pop(index, None)
                self._results.pop(index, None)

    def kill(self) -> None:
        self.raft.kill()

    def status(self) -> dict[str, Any]:
        return self.raft.status()


class Cluster:
    """A set of replicas on one simulated network."""

    def __init__(self, size: int = 5, seed: int = 1,
                 event_sink: Callable[[dict], None] | None = None,
                 reset: bool = True) -> None:
        self.size = size
        self.net = Network(seed)
        self.peers = list(range(size))
        self._event_sink = event_sink
        self._replicas: dict[int, Replica | None] = {}
        self._lock = threading.RLock()
        self._client_counter = itertools.count(1)

        if reset:
            db.reset_cluster_state(size)

        for i in self.peers:
            self._replicas[i] = Replica(i, self.peers, self.net, event_sink)

    # -- membership -----------------------------------------------------

    def replica(self, node_id: int) -> Replica | None:
        with self._lock:
            return self._replicas.get(node_id)

    def is_running(self, node_id: int) -> bool:
        with self._lock:
            return self._replicas.get(node_id) is not None

    def crash(self, node_id: int) -> None:
        """Stop a replica without touching its persistent state."""
        with self._lock:
            replica = self._replicas.get(node_id)
            if replica is None:
                return
            self._replicas[node_id] = None
        self.net.set_alive(node_id, False)
        replica.kill()
        try:
            # Reset the role as well as the liveness flag. A crashed node
            # always comes back as a follower, so leaving 'leader' in the
            # table would show two leaders in any view that reads it -
            # misleading, even though the live process is gone.
            db.execute("UPDATE node SET is_alive = FALSE, role = 'follower' "
                       "WHERE node_id = %s", (node_id,))
        except Exception:
            pass

    def restart(self, node_id: int) -> None:
        """Rebuild a crashed replica from what MySQL still holds."""
        with self._lock:
            if self._replicas.get(node_id) is not None:
                return
        try:
            db.execute("UPDATE node SET is_alive = TRUE, role = 'follower' "
                       "WHERE node_id = %s", (node_id,))
        except Exception:
            pass
        self.net.set_alive(node_id, True)
        replica = Replica(node_id, self.peers, self.net, self._event_sink)
        with self._lock:
            self._replicas[node_id] = replica

    def shutdown(self) -> None:
        with self._lock:
            replicas = [r for r in self._replicas.values() if r is not None]
            for i in self._replicas:
                self._replicas[i] = None
        for r in replicas:
            r.kill()

    # -- leadership -----------------------------------------------------

    def leader(self) -> int:
        """Index of the current leader, or -1. When several claim it,
        the highest term wins: only that one can still make progress."""
        best, best_term = -1, -1
        with self._lock:
            replicas = list(self._replicas.items())
        for node_id, replica in replicas:
            if replica is None:
                continue
            term, is_leader = replica.raft.state()
            if is_leader and term > best_term:
                best, best_term = node_id, term
        return best

    def wait_for_leader(self, timeout: float = 10.0) -> int:
        """Poll until a leader emerges and holds. Returns -1 on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            first = self.leader()
            if first >= 0:
                time.sleep(0.25)
                if self.leader() >= 0:
                    return self.leader()
                continue
            time.sleep(0.1)
        return -1

    def leaders_by_term(self) -> dict[int, int]:
        """How many running replicas claim leadership in each term. More
        than one in a single term would be a safety violation."""
        counts: dict[int, int] = {}
        with self._lock:
            replicas = [r for r in self._replicas.values() if r is not None]
        for replica in replicas:
            term, is_leader = replica.raft.state()
            if is_leader:
                counts[term] = counts.get(term, 0) + 1
        return counts

    def status(self) -> list[dict[str, Any]]:
        out = []
        for node_id in self.peers:
            replica = self.replica(node_id)
            if replica is None:
                out.append({"node_id": node_id, "role": "offline",
                            "alive": False, "term": None,
                            "commit_index": None, "log_length": None})
            else:
                out.append(replica.status())
        return out

    # -- clients --------------------------------------------------------

    def clerk(self, client_id: str | None = None) -> "Clerk":
        if client_id is None:
            client_id = f"client-{next(self._client_counter)}"
        return Clerk(self, client_id)


class Clerk:
    """A retrying client.

    The sequence number is allocated once per logical request, outside
    the retry loop, so every retry is recognisably the same request and
    the servers can deduplicate it.
    """

    def __init__(self, cluster: Cluster, client_id: str) -> None:
        self.cluster = cluster
        self.client_id = client_id
        self._seq = 0
        self._last_leader = 0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _call(self, op: str, key: str, value: str,
              timeout: float = 8.0) -> dict[str, Any]:
        seq = self._next_seq()
        deadline = time.time() + timeout
        cmd = Command(op=op, key=key, value=value,
                      client_id=self.client_id, seq_no=seq)

        while time.time() < deadline:
            order = [self._last_leader] + [
                i for i in range(self.cluster.size) if i != self._last_leader]
            for node_id in order:
                replica = self.cluster.replica(node_id)
                if replica is None:
                    continue
                result = replica.submit(cmd, timeout=2.0)
                if result.get("ok"):
                    self._last_leader = node_id
                    return result
            time.sleep(0.1)
        return {"ok": False, "err": "timeout"}

    def put(self, key: str, value: str, timeout: float = 8.0) -> bool:
        return self._call("PUT", key, value, timeout).get("ok", False)

    def append(self, key: str, value: str, timeout: float = 8.0) -> bool:
        return self._call("APPEND", key, value, timeout).get("ok", False)

    def delete(self, key: str, timeout: float = 8.0) -> bool:
        return self._call("DELETE", key, "", timeout).get("ok", False)

    def get(self, key: str, timeout: float = 8.0) -> str | None:
        result = self._call("GET", key, "", timeout)
        if not result.get("ok"):
            return None
        return result.get("value", "")
