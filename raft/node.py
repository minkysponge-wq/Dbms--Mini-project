"""The Raft consensus algorithm, with MySQL as the stable store.

Follows Figure 2 of "In Search of an Understandable Consensus Algorithm"
(Ongaro & Ousterhout, 2014). Where a rule from that figure is enforced,
the comment says so, because nearly every bug in a Raft implementation
is a Figure 2 rule that was skipped.

Storage model
-------------
The durable state - current term, the vote cast in that term, and the
log - lives in MySQL. It is written there *before* the node replies to
any RPC, which is the requirement crash recovery rests on: a node that
answers "I voted for you" and then dies before the write lands could
come back, forget the vote, vote again in the same term, and elect two
leaders.

The log is also mirrored in memory. MySQL is the authority and the
mirror is a read cache, which is how a real storage engine works: a
durable write-ahead log plus an in-memory index. Reads come from the
mirror, every mutation goes to MySQL first, and on restart the mirror is
rebuilt from the table.

Log indexing
------------
``log[0]`` is a sentinel at index 0, term 0, so a list position is
always equal to the log index. There is no snapshotting, which is what
keeps that identity true.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from . import db
from .transport import Network

# Timings. These are an order of magnitude larger than an in-memory Raft
# would need, because every state change here is a SQL round trip. The
# invariant that matters is heartbeat << election timeout, so a healthy
# leader is never timed out by its followers.
HEARTBEAT_INTERVAL = 0.30      # seconds
ELECTION_TIMEOUT_MIN = 1.20
ELECTION_TIMEOUT_MAX = 2.40
TICK_INTERVAL = 0.05

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


@dataclass
class Command:
    """One state-machine operation carried in the log.

    ``client_id`` and ``seq_no`` are what give the system exactly-once
    semantics: a retried request carries the same pair, so the state
    machine recognises and skips the duplicate.
    """
    op: str                      # PUT | APPEND | DELETE | GET | NOOP
    key: str = ""
    value: str = ""
    client_id: str = ""
    seq_no: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": self.op, "key": self.key, "value": self.value,
            "client_id": self.client_id, "seq_no": self.seq_no,
        }

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Command":
        return Command(
            op=d.get("op", "NOOP"), key=d.get("key", ""),
            value=d.get("value", ""), client_id=d.get("client_id", ""),
            seq_no=int(d.get("seq_no", 0)),
        )


@dataclass
class LogEntry:
    term: int
    index: int
    command: Command = field(default_factory=lambda: Command("NOOP"))

    def to_dict(self) -> dict[str, Any]:
        return {"term": self.term, "index": self.index,
                "command": self.command.to_dict()}

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "LogEntry":
        return LogEntry(term=int(d["term"]), index=int(d["index"]),
                        command=Command.from_dict(d.get("command", {})))


@dataclass
class ApplyMsg:
    command: Command
    index: int
    term: int


class RaftNode:
    """One server in the cluster."""

    def __init__(
        self,
        node_id: int,
        peers: list[int],
        net: Network,
        apply_fn: Callable[[ApplyMsg], None] | None = None,
        event_sink: Callable[[dict], None] | None = None,
    ) -> None:
        self.me = node_id
        self.peers = list(peers)
        self.net = net
        self._apply_fn = apply_fn
        self._event_sink = event_sink

        self.mu = threading.RLock()
        self._apply_cv = threading.Condition(self.mu)

        # Persistent state, loaded from MySQL below.
        self.current_term = 0
        self.voted_for: int | None = None
        self.log: list[LogEntry] = [LogEntry(term=0, index=0)]

        # Volatile state.
        self.role = FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id: int | None = None

        # Volatile leader state, rebuilt after every election.
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        self._rng = random.Random(node_id * 7919 + int(time.time() * 1000))
        self._election_deadline = 0.0
        self._dead = threading.Event()
        self._threads: list[threading.Thread] = []

        self._load_persistent_state()
        self._reset_election_deadline()

        net.register(node_id, self._handle_rpc)

        self._threads.append(threading.Thread(
            target=self._ticker, name=f"raft-{node_id}-ticker", daemon=True))
        self._threads.append(threading.Thread(
            target=self._applier, name=f"raft-{node_id}-applier", daemon=True))
        for t in self._threads:
            t.start()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load_persistent_state(self) -> None:
        """Rebuild durable state from MySQL. Called on start and restart."""
        row = db.query_one(
            "SELECT current_term, voted_for, commit_index, last_applied "
            "FROM node WHERE node_id = %s", (self.me,))
        if row:
            self.current_term = int(row["current_term"])
            self.voted_for = row["voted_for"]
            self.commit_index = int(row["commit_index"])
            self.last_applied = int(row["last_applied"])

        rows = db.query(
            "SELECT log_index, term_number, command_type, key_name, "
            "value_text, client_id, seq_no FROM log_entry "
            "WHERE node_id = %s ORDER BY log_index", (self.me,))
        self.log = [LogEntry(term=0, index=0)]
        for r in rows:
            self.log.append(LogEntry(
                term=int(r["term_number"]),
                index=int(r["log_index"]),
                command=Command(
                    op=r["command_type"], key=r["key_name"] or "",
                    value=r["value_text"] or "",
                    client_id=r["client_id"] or "",
                    seq_no=int(r["seq_no"] or 0),
                ),
            ))

    def _persist_term_and_vote(self) -> None:
        """Write term and vote to stable storage. Caller holds self.mu.

        Must complete before the node replies to any RPC.
        """
        db.execute(
            "UPDATE node SET current_term = %s, voted_for = %s WHERE node_id = %s",
            (self.current_term, self.voted_for, self.me))
        db.execute(
            "INSERT IGNORE INTO term (term_number, leader_id) VALUES (%s, NULL)",
            (self.current_term,))

    def _persist_append(self, entries: list[LogEntry]) -> None:
        """Append entries to the log table. Caller holds self.mu."""
        if not entries:
            return
        db.executemany(
            "INSERT INTO log_entry (node_id, log_index, term_number, "
            "command_type, key_name, value_text, client_id, seq_no, committed) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE)",
            [(self.me, e.index, e.term, e.command.op,
              e.command.key or None, e.command.value or None,
              e.command.client_id or None, e.command.seq_no or None)
             for e in entries])

    def _persist_truncate(self, from_index: int) -> None:
        """Delete entries at or after from_index. Caller holds self.mu."""
        db.execute(
            "DELETE FROM log_entry WHERE node_id = %s AND log_index >= %s",
            (self.me, from_index))

    def _persist_commit_index(self) -> None:
        db.execute("UPDATE node SET commit_index = %s WHERE node_id = %s",
                   (self.commit_index, self.me))

    def _persist_role(self) -> None:
        """Record the role. The audit trigger turns this into an event row."""
        db.execute("UPDATE node SET role = %s WHERE node_id = %s",
                   (self.role, self.me))

    def _emit(self, event_type: str, description: str, **extra: Any) -> None:
        """Hand an event to the sink, if one is attached.

        Never allowed to break the protocol: the sink is observability,
        so a failure there is swallowed rather than propagated.
        """
        if self._event_sink is None:
            return
        try:
            self._event_sink({
                "node_id": self.me, "event_type": event_type,
                "term": self.current_term, "role": self.role,
                "description": description, "ts": time.time(), **extra,
            })
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def last_index(self) -> int:
        return len(self.log) - 1

    def last_term(self) -> int:
        return self.log[-1].term

    def state(self) -> tuple[int, bool]:
        with self.mu:
            return self.current_term, self.role == LEADER

    def is_leader(self) -> bool:
        with self.mu:
            return self.role == LEADER

    def status(self) -> dict[str, Any]:
        with self.mu:
            return {
                "node_id": self.me, "role": self.role,
                "term": self.current_term, "voted_for": self.voted_for,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": self.last_index(),
                "leader_id": self.leader_id,
                "alive": not self._dead.is_set(),
            }

    def killed(self) -> bool:
        return self._dead.is_set()

    def kill(self) -> None:
        """Stop this node's threads. Its MySQL state is untouched, so the
        same node id can be restarted from exactly what it had."""
        self._dead.set()
        with self.mu:
            self._apply_cv.notify_all()
        self.net.unregister(self.me)

    # ------------------------------------------------------------------
    # Term changes
    # ------------------------------------------------------------------

    def _reset_election_deadline(self) -> None:
        """Arm a fresh randomised timeout.

        Randomisation breaks split votes. With a fixed timeout every node
        would become a candidate at the same instant, split the vote, and
        repeat indefinitely.
        """
        self._election_deadline = time.time() + self._rng.uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _become_follower(self, term: int) -> None:
        """Step down to a higher term. Caller holds self.mu and must
        persist afterwards.

        Figure 2, Rules for Servers: any RPC request or response carrying
        a term higher than ours converts us to a follower. This is what
        makes a stale leader harmless the moment it reconnects.
        """
        changed_role = self.role != FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.role = FOLLOWER
        self.leader_id = None
        if changed_role:
            self._persist_role()

    # ------------------------------------------------------------------
    # RPC dispatch
    # ------------------------------------------------------------------

    def _handle_rpc(self, method: str, args: dict) -> dict | None:
        if self._dead.is_set():
            return None
        if method == "RequestVote":
            return self._request_vote(args)
        if method == "AppendEntries":
            return self._append_entries(args)
        return None

    # ------------------------------------------------------------------
    # Elections
    # ------------------------------------------------------------------

    def _request_vote(self, args: dict) -> dict:
        with self.mu:
            term = int(args["term"])
            candidate = int(args["candidate_id"])
            cand_last_index = int(args["last_log_index"])
            cand_last_term = int(args["last_log_term"])

            reply = {"term": self.current_term, "vote_granted": False}

            # Reject anything from an older term outright.
            if term < self.current_term:
                return reply

            if term > self.current_term:
                self._become_follower(term)
                self._persist_term_and_vote()
                reply["term"] = self.current_term

            # Vote at most once per term. This single rule is what makes
            # two leaders in one term impossible, and it is why voted_for
            # must be durable.
            if self.voted_for is not None and self.voted_for != candidate:
                return reply

            # Election restriction (paper 5.4.1): only vote for a candidate
            # whose log is at least as up to date as ours - last term
            # first, then last index. Without this, a candidate missing
            # committed entries could win and overwrite them.
            up_to_date = (
                cand_last_term > self.last_term()
                or (cand_last_term == self.last_term()
                    and cand_last_index >= self.last_index())
            )
            if not up_to_date:
                return reply

            self.voted_for = candidate
            self._persist_term_and_vote()
            self._record_vote_row(candidate, cand_last_index, cand_last_term)

            # Only restart the clock for a vote actually granted, so a
            # stream of requests from an unelectable candidate cannot keep
            # us from timing out.
            self._reset_election_deadline()
            reply["vote_granted"] = True
            return reply

    def _record_vote_row(self, candidate: int, cand_index: int,
                         cand_term: int) -> None:
        """Insert the vote. The database rejects a second vote in the same
        term through the composite primary key, and a stale or
        insufficiently-current vote through trg_vote_bi."""
        try:
            db.execute(
                "INSERT INTO vote (term_number, voter_id, candidate_id, "
                "granted, candidate_last_index, candidate_last_term) "
                "VALUES (%s, %s, %s, TRUE, %s, %s)",
                (self.current_term, self.me, candidate, cand_index, cand_term))
        except Exception:
            # A duplicate or rejected vote is a normal protocol outcome,
            # not a crash: the database has simply enforced a rule we
            # also enforce above.
            pass

    def _start_election(self) -> None:
        with self.mu:
            if self._dead.is_set() or self.role == LEADER:
                return
            self.current_term += 1
            self.role = CANDIDATE
            self.voted_for = self.me
            self.leader_id = None
            self._reset_election_deadline()
            self._persist_term_and_vote()
            self._persist_role()
            self._record_vote_row(self.me, self.last_index(), self.last_term())
            self._emit("election_started",
                       f"node {self.me} timed out and began an election for "
                       f"term {self.current_term}")

            term = self.current_term
            args = {
                "term": term, "candidate_id": self.me,
                "last_log_index": self.last_index(),
                "last_log_term": self.last_term(),
            }
            peers = [p for p in self.peers if p != self.me]

        votes = {"count": 1, "won": False}
        votes_lock = threading.Lock()
        needed = len(self.peers) // 2 + 1

        def solicit(peer: int) -> None:
            # Never hold self.mu across a network call: the peer's handler
            # takes its own lock and two nodes calling each other would
            # deadlock immediately.
            reply = self.net.call(self.me, peer, "RequestVote", args)
            if reply is None:
                return
            with self.mu:
                if int(reply["term"]) > self.current_term:
                    self._become_follower(int(reply["term"]))
                    self._persist_term_and_vote()
                    return
                # Discard a reply that arrived after we moved on. A stale
                # vote must not elect a node that already lost.
                if self.role != CANDIDATE or self.current_term != term:
                    return
                if not reply["vote_granted"]:
                    return
                with votes_lock:
                    votes["count"] += 1
                    if votes["count"] >= needed and not votes["won"]:
                        votes["won"] = True
                        self._become_leader(term)

        for peer in peers:
            threading.Thread(target=solicit, args=(peer,), daemon=True).start()

    def _become_leader(self, term: int) -> None:
        """Promote a winning candidate. Caller holds self.mu."""
        if self.role != CANDIDATE or self.current_term != term:
            return
        self.role = LEADER
        self.leader_id = self.me
        self._persist_role()

        # Reinitialise per-follower state. A new leader knows nothing
        # about its followers' progress, so next_index starts past our own
        # log and is walked back by the conflict optimisation.
        for peer in self.peers:
            self.next_index[peer] = self.last_index() + 1
            self.match_index[peer] = 0
        self.match_index[self.me] = self.last_index()

        db.execute("UPDATE term SET leader_id = %s WHERE term_number = %s",
                   (self.me, term))
        db.execute("DELETE FROM replication_state WHERE leader_id = %s",
                   (self.me,))
        try:
            db.executemany(
                "INSERT INTO replication_state (leader_id, follower_id, "
                "next_index, match_index) VALUES (%s, %s, %s, 0)",
                [(self.me, p, self.last_index() + 1)
                 for p in self.peers if p != self.me])
        except Exception:
            pass

        self._emit("became_leader",
                   f"node {self.me} won the election for term {term} with a majority")

        # Append a no-op in our own term (paper section 8).
        #
        # A leader may only commit an entry by counting replicas when
        # that entry is from its current term - the Figure 8 rule
        # enforced in _advance_commit_index. So entries inherited from a
        # previous leader's term can sit replicated-but-uncommitted
        # indefinitely, and a client whose write was the last thing the
        # old leader accepted would never see it commit. Appending one
        # no-op immediately commits the whole backlog with it.
        index = self.last_index() + 1
        noop = LogEntry(term=self.current_term, index=index,
                        command=Command(op="NOOP"))
        self.log.append(noop)
        self._persist_append([noop])
        self.match_index[self.me] = index
        self.next_index[self.me] = index + 1

        threading.Thread(target=self._broadcast_append, daemon=True).start()

    # ------------------------------------------------------------------
    # Log replication
    # ------------------------------------------------------------------

    def start(self, command: Command) -> tuple[int, int, bool]:
        """Append a command and begin replicating it.

        Returns (index, term, is_leader). The command is not committed
        yet; the caller learns the outcome by watching for the apply.
        """
        with self.mu:
            if self._dead.is_set() or self.role != LEADER:
                return -1, self.current_term, False
            index = self.last_index() + 1
            entry = LogEntry(term=self.current_term, index=index,
                             command=command)
            self.log.append(entry)
            self._persist_append([entry])
            self.match_index[self.me] = index
            self.next_index[self.me] = index + 1
            term = self.current_term

        threading.Thread(target=self._broadcast_append, daemon=True).start()
        return index, term, True

    def _append_entries(self, args: dict) -> dict:
        with self.mu:
            term = int(args["term"])
            leader_id = int(args["leader_id"])
            prev_index = int(args["prev_log_index"])
            prev_term = int(args["prev_log_term"])
            entries = [LogEntry.from_dict(e) for e in args.get("entries", [])]
            leader_commit = int(args["leader_commit"])

            reply = {"term": self.current_term, "success": False,
                     "xterm": -1, "xindex": -1, "xlen": self.last_index() + 1}

            if term < self.current_term:
                return reply

            if term > self.current_term:
                self._become_follower(term)
                reply["term"] = self.current_term

            # A valid leader exists for this term, so stand down and
            # restart the clock. A candidate that lost learns it here.
            if self.role != FOLLOWER:
                self.role = FOLLOWER
                self._persist_role()
            self.leader_id = leader_id
            self._reset_election_deadline()
            self._persist_term_and_vote()

            # Consistency check: our log must already hold prev_index at
            # prev_term. If not, reject and return enough for the leader
            # to skip a whole term rather than back off one entry.
            if prev_index > self.last_index():
                reply["xterm"] = -1
                reply["xlen"] = self.last_index() + 1
                return reply

            if self.log[prev_index].term != prev_term:
                conflicting = self.log[prev_index].term
                i = prev_index
                while i > 1 and self.log[i - 1].term == conflicting:
                    i -= 1
                reply["xterm"] = conflicting
                reply["xindex"] = i
                reply["xlen"] = self.last_index() + 1
                return reply

            # Append, truncating only on a genuine conflict. Truncating
            # unconditionally is a real bug: a delayed duplicate
            # AppendEntries would chop off entries the leader has already
            # committed.
            to_persist: list[LogEntry] = []
            truncate_from = -1
            for offset, entry in enumerate(entries):
                idx = prev_index + 1 + offset
                if idx <= self.last_index():
                    if self.log[idx].term != entry.term:
                        truncate_from = idx
                        self.log = self.log[:idx]
                        self.log.extend(entries[offset:])
                        to_persist = entries[offset:]
                        break
                    continue
                self.log.extend(entries[offset:])
                to_persist = entries[offset:]
                break

            if truncate_from >= 0:
                self._persist_truncate(truncate_from)
            if to_persist:
                self._persist_append(to_persist)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.last_index())
                self._persist_commit_index()
                self._apply_cv.notify_all()

            reply["success"] = True
            reply["xlen"] = self.last_index() + 1
            return reply

    def _broadcast_append(self) -> None:
        with self.mu:
            if self._dead.is_set() or self.role != LEADER:
                return
            term = self.current_term
            peers = [p for p in self.peers if p != self.me]
        for peer in peers:
            threading.Thread(target=self._replicate_to, args=(peer, term),
                             daemon=True).start()

    def _replicate_to(self, peer: int, term: int) -> None:
        with self.mu:
            if self._dead.is_set() or self.role != LEADER or self.current_term != term:
                return
            next_idx = max(1, min(self.next_index.get(peer, 1),
                                  self.last_index() + 1))
            prev_index = next_idx - 1
            args = {
                "term": term, "leader_id": self.me,
                "prev_log_index": prev_index,
                "prev_log_term": self.log[prev_index].term,
                "entries": [e.to_dict() for e in self.log[next_idx:]],
                "leader_commit": self.commit_index,
            }

        reply = self.net.call(self.me, peer, "AppendEntries", args)
        if reply is None:
            return

        with self.mu:
            if int(reply["term"]) > self.current_term:
                self._become_follower(int(reply["term"]))
                self._persist_term_and_vote()
                return
            if self.role != LEADER or self.current_term != term:
                return

            if reply["success"]:
                matched = prev_index + len(args["entries"])
                if matched > self.match_index.get(peer, 0):
                    self.match_index[peer] = matched
                self.next_index[peer] = self.match_index[peer] + 1
                self._persist_replication_state(peer)
                self._advance_commit_index()
                return

            # Failed consistency check: use the conflict hint to jump back
            # by a whole term rather than one entry per round trip.
            xterm = int(reply["xterm"])
            if xterm == -1:
                self.next_index[peer] = max(1, int(reply["xlen"]))
            else:
                last_of_term = -1
                for i in range(self.last_index(), 0, -1):
                    if self.log[i].term == xterm:
                        last_of_term = i
                        break
                    if self.log[i].term < xterm:
                        break
                self.next_index[peer] = (
                    last_of_term + 1 if last_of_term >= 0
                    else max(1, int(reply["xindex"])))
            self._persist_replication_state(peer)

    def _persist_replication_state(self, peer: int) -> None:
        try:
            db.execute(
                "INSERT INTO replication_state (leader_id, follower_id, "
                "next_index, match_index, last_contact) "
                "VALUES (%s, %s, %s, %s, NOW(6)) "
                "ON DUPLICATE KEY UPDATE next_index = VALUES(next_index), "
                "match_index = GREATEST(match_index, VALUES(match_index)), "
                "last_contact = VALUES(last_contact)",
                (self.me, peer, self.next_index.get(peer, 1),
                 self.match_index.get(peer, 0)))
        except Exception:
            pass

    def _advance_commit_index(self) -> None:
        """Move the commit point forward on majority replication.
        Caller holds self.mu.

        The term check is the subtle part, and the rule most
        implementations get wrong. A leader may only commit an entry from
        its OWN term by counting replicas; entries left over from a
        previous term commit indirectly once a later same-term entry
        commits. Figure 8 of the paper shows the case where counting
        replicas on an old entry loses committed data.
        """
        for n in range(self.last_index(), self.commit_index, -1):
            if self.log[n].term != self.current_term:
                break
            count = sum(1 for p in self.peers
                        if self.match_index.get(p, 0) >= n)
            if count > len(self.peers) // 2:
                self.commit_index = n
                self._persist_commit_index()
                self._apply_cv.notify_all()
                return

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    def _ticker(self) -> None:
        last_beat = 0.0
        while not self._dead.is_set():
            time.sleep(TICK_INTERVAL)
            if self._dead.is_set():
                break
            with self.mu:
                role = self.role
                deadline = self._election_deadline
            if role == LEADER:
                if time.time() - last_beat >= HEARTBEAT_INTERVAL:
                    last_beat = time.time()
                    self._broadcast_append()
                continue
            if time.time() > deadline:
                self._start_election()
        db.close_connection()

    def _applier(self) -> None:
        """Deliver committed entries to the state machine in index order,
        exactly once each."""
        while not self._dead.is_set():
            with self._apply_cv:
                while (not self._dead.is_set()
                       and self.last_applied >= self.commit_index):
                    self._apply_cv.wait(timeout=0.2)
                if self._dead.is_set():
                    break
                self.last_applied += 1
                entry = self.log[self.last_applied]
                msg = ApplyMsg(command=entry.command, index=entry.index,
                               term=entry.term)
            try:
                db.execute(
                    "UPDATE node SET last_applied = %s WHERE node_id = %s",
                    (msg.index, self.me))
                if self._apply_fn is not None:
                    self._apply_fn(msg)
            except Exception:
                pass
        db.close_connection()
