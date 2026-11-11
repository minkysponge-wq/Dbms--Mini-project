# Design Notes

How the implementation works, the invariants it depends on, and the bugs
that testing found. Read alongside Figure 2 of the Raft paper.

---

## 1. Layers

```
  Clerk               retries with a stable (client_id, seq_no)
    │
  Replica             state machine + request waiters
    │  apply_fn
  RaftNode            elections, replication, commit rules
    │  RPC            │  SQL
  Network             MySQL (durable state)
```

Each layer knows only the one below. The key-value store never reasons
about terms or elections; it consumes an ordered, gap-free stream of
committed commands and trusts every replica consumes the same stream.

---

## 2. Storage model

Durable state — `current_term`, `voted_for`, the log — lives in MySQL and
is written **before** the node replies to any RPC. A node that answers "I
voted for you" and then dies before the write lands could come back,
forget the vote, vote again in the same term, and elect two leaders. The
whole safety argument rests on that ordering.

The log is also mirrored in memory. MySQL is the authority; the mirror is
a read cache. That is how a real storage engine works — a durable
write-ahead log plus an in-memory index. Reads come from the mirror,
every mutation goes to MySQL first, and on restart `_load_persistent_state`
rebuilds the mirror from the table.

### Log indexing

`log[0]` is a sentinel at index 0, term 0, so a list position always
equals the log index:

```python
rf.log[i].index == i
rf.last_index() == len(rf.log) - 1
```

The single biggest simplification in the codebase. Snapshotting would
break it and is scoped out for exactly that reason.

---

## 3. Elections

**Randomised timeouts (1.2–2.4 s).** With a fixed timeout every node
becomes a candidate at the same instant, splits the vote, and repeats
forever. Randomisation makes one node almost certainly time out first.

**One vote per term**, checked and persisted before replying — and
enforced independently by the database through the composite primary key
on `vote` and by `trg_vote_bi`.

**Election restriction (§5.4.1).** A voter grants only if the candidate's
log is at least as up to date as its own — last term first, then last
index. Without this a candidate missing committed entries could win and
overwrite them. Enforced in Python *and* in the trigger.

**Stale replies are discarded.** After every network call the code
re-checks that it is still a candidate in the same term:

```python
if self.role != CANDIDATE or self.current_term != term:
    return   # we moved on; this reply is history
```

**A new leader appends a no-op.** See §7 — this was a bug before it was
a feature.

---

## 4. Replication

Follower side, in order: reject an older term; step down to a higher one;
reset the election clock; run the consistency check; append, **truncating
only on a genuine term conflict**.

That last point matters. Truncating unconditionally is a real bug: a
delayed duplicate `AppendEntries` would chop off entries the leader has
already committed. The code compares terms entry by entry.

### Conflict backtracking

On rejection the follower returns `XTerm`, `XIndex` and `XLen`. The
leader either jumps past its own last entry for `XTerm` or skips the
follower's whole conflicting term. Without this, `next_index` walks back
one entry per round trip, and with a SQL round trip per step a follower
returning from a long partition would take minutes.

### The commit rule

```python
for n in range(self.last_index(), self.commit_index, -1):
    if self.log[n].term != self.current_term:
        break            # <- the subtle part
    ...count match_index >= n; commit on a majority
```

**A leader may only commit entries from its own term by counting
replicas.** Figure 8 of the paper shows the case where an entry from a
previous term is replicated to a majority and still gets overwritten.
Older entries commit indirectly, once a same-term entry does.

The same rule is implemented again in `sp_advance_commit_index`, and
`v_commit_quorum` expresses the majority test as a CTE.

---

## 5. Concurrency

**Never hold `self.mu` across a network call.** The peer's handler takes
its own lock; two nodes calling each other while holding theirs deadlock
immediately. Every `net.call` is made with the lock released, and state
is re-validated afterwards.

**Each thread gets its own MySQL connection.** PyMySQL connections are
not thread-safe, and a Raft node runs a ticker, an applier and one thread
per outbound RPC. `raft/db.py` keeps them in `threading.local()`.

**Row locks in the database.** `sp_cast_vote` takes
`SELECT ... FOR UPDATE` on the voter's row before deciding anything, so
two candidates soliciting the same voter serialise rather than both
reading stale state. `python demos.py locking` shows the second blocking.

---

## 6. Exactly-once clients

Without client sessions the system is at-least-once, and the failure is
concrete: a leader commits a command, crashes before replying, the client
retries, and the command applies twice. For `APPEND` the stored value is
then wrong.

Every request carries `(client_id, seq_no)`. The sequence number is
allocated once by the clerk, **outside** the retry loop, so every retry
is recognisably the same request.

Deduplication happens on **every replica**, not only the leader, because
a retry can reach a different replica and be replicated again.

---

## 7. Bugs found by testing

These are worth reading; each one was invisible until a test caught it.

### `client_session` keyed on `client_id` alone

The first version gave `client_session` a single-column primary key. All
five replicas therefore shared one deduplication row: node A applied a
command and wrote `last_seq_seen`, and nodes B–E then saw the same
command as a duplicate and **skipped it**. The replicas diverged
silently, and reads after a failover came back empty.

The session table is part of the replicated state machine, so each
replica must maintain its own copy by applying the same log — exactly
like `kv_state`. The key became `(node_id, client_id)`, with a separate
`client` relation for cluster-wide client identity.

Caught by `test_acknowledged_write_survives_leader_crash`. The ER diagram
was wrong too and was regenerated.

### No no-op append on election

A write accepted by the old leader sat replicated-but-uncommitted
indefinitely after a failover. Not a bug in the commit rule — the Figure 8
check was doing precisely what it should, refusing to commit a previous
term's entry by counting replicas. But nothing ever created an entry in
the *new* term, so the backlog never committed.

The paper's answer (§8) is for a new leader to append a no-op in its own
term immediately, which commits everything before it. Added in
`_become_leader`.

### Clerks rebuilt per HTTP request

`server.py` created a fresh `Clerk` for every request, which restarted
its sequence counter at 1. The second write from the dashboard therefore
looked like a retry of the first and was correctly — and uselessly —
deduplicated away. One key would save; the next would silently vanish.

Fixed with a clerk cache keyed by client id. The deduplication machinery
worked perfectly; it was being fed bad input by my own code.

### A fourth, in SQL

In `sp_apply_committed`, a `SELECT ... INTO` matching no rows raises
`NOT FOUND` — the *same* condition a cursor uses to signal exhaustion.
With a `CONTINUE HANDLER FOR NOT FOUND` in scope, the first lookup for an
unknown client set `v_done = 1` and ended the loop after one entry.
Symptom: three entries committed, one applied. Fixed with a scalar
subquery, which yields NULL and raises nothing.

### Deadlocked applies were skipped

Every replica applies each committed entry at nearly the same moment,
and those five transactions touch shared rows - the `client` row in
particular. InnoDB detects the resulting deadlocks and rolls one back
(error 1213). The applier wrote `last_applied` *before* applying and
swallowed the exception, so the victim replica recorded the entry as
applied when it never was. That replica silently lacked the key, and a
read it served after a restart came back empty.

Fixed in `RaftNode._applier`: `last_applied` is recorded only after the
apply succeeds, and a failed apply is retried with backoff rather than
skipped - the same lesson as the deadlock demo in `demos.py`.

A related fix in `Replica.submit`: a client now accepts the result at an
index only if the entry applied there is from the term it submitted in.
Otherwise a deposed leader could hand its client the result of whatever
the new leader wrote at that index.

Caught intermittently by `test_data_survives_full_cluster_restart` and
`test_majority_side_keeps_serving`. `test_replicas_converge` used to skip
keys a replica was missing; it now requires every replica's map to equal
the committed writes, and fails on the old code.

---

## 8. The network simulator

`raft/transport.py` routes RPCs in-process and injects faults: per-message
drop probability, random latency, occasional long delays, partition
groups, per-node crash state. Every message round-trips through JSON, so
it crosses a real serialisation boundary — a bug where two nodes share a
mutable object shows up here as it would over TCP.

Seeded, so a run replays.

---

## 9. Known limitations

| Limitation | Consequence | Why |
|---|---|---|
| No log compaction | The log grows without bound | Breaks the index-equals-position invariant |
| Fixed membership | No live add or remove | Joint consensus is a project of its own |
| In-process transport | Not deployable as-is | Real sockets add ops work, not insight |
| Reads go through the log | A round trip per read | Leader leases need clock assumptions |
| ~1.5 s elections | Slower than in-memory Raft | Every state change is a SQL round trip |
| Exact vector search | Scans all embeddings | MongoDB Community has no HNSW index |
