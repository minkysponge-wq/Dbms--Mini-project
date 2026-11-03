# Fault-Tolerant Distributed Key-Value Store using the Raft Consensus Algorithm

**Sampreeth** · PES1UG24AM246 · UE23CS351A Database Management Systems

A replicated key-value store built on a from-scratch implementation of the
Raft consensus algorithm, with MySQL as the storage engine, MongoDB as the
event store, and a Next.js dashboard with role-based access control.

The claim the system makes is narrow and testable: **as long as a majority
of nodes survive, no acknowledged write is ever lost and no two nodes ever
lead the same term.**

---

## Quick start

```bash
# 1. Dependencies
pip install -r requirements.txt

# 2. Create the schema and seed it
python setup.py

# 3. Check it works
python -m unittest discover -s tests

# 4. Start the cluster
python server.py

# 5. In a second terminal, start the dashboard
cd dashboard
npm install
npm run dev          # http://localhost:3000
```

Sign in as `admin` / `admin123`, go to the Cluster tab, and press **Crash**
on whichever node is the leader. A new one is elected in a few seconds and
the data is still there.

`SETUP.md` has the full install guide, including XAMPP on Windows.

---

## What it is

A single database server is simple but fragile: it dies, the data is gone.
Replicating across several machines fixes availability but introduces a
worse problem — the machines can disagree. Two nodes both believing they
are in charge will accept conflicting writes, and the corruption is silent.

Raft prevents that. Three mechanisms, all implemented here:

| Mechanism | Guarantee | Where |
|---|---|---|
| Leader election | At most one leader per term, because each node votes at most once per term | `raft/node.py`, `sql/03_triggers.sql` |
| Log replication | An entry commits only once a majority has stored it | `raft/node.py`, `sql/04_procedures.sql` |
| Persistent state | A crashed node cannot forget its vote and vote twice | MySQL `node`, `vote`, `log_entry` |

Two majorities of a five-node cluster must overlap in at least one node.
That overlap is why a committed entry can never be lost.

---

## Architecture

```
  Browser
     │
  Next.js dashboard ──────────────┬─────────────── MySQL
  (auth, RBAC, views, KV, events) │                (protocol state,
     │                            │                 state machine, RBAC)
     │ control actions            └─────────────── MongoDB
     ▼                                             (event log, vectors)
  server.py  ── Raft cluster (5 nodes, Python)
```

The dashboard reads MySQL and MongoDB **directly** — that is the database
integration. It proxies only *control* actions (write a key through
consensus, crash a node, partition the network) to `server.py`, because
those need the live cluster.

A write that bypassed Raft and went straight to `kv_state` would corrupt
the replica: the log would no longer explain the state.

---

## Layout

```
raftkv-mysql/
├── sql/
│   ├── 01_schema.sql       17 tables, keys, FKs, checks, indexes
│   ├── 02_views.sql        7 views: recursive CTE, window functions
│   ├── 03_triggers.sql     6 triggers enforcing Raft's safety rules
│   ├── 04_procedures.sql   10 procedures and functions, transactional
│   ├── 05_seed.sql         cluster nodes, roles, permissions, users
│   └── 06_queries.sql      ~30 showcase queries
├── raft/
│   ├── db.py               connection handling, transactions
│   ├── transport.py        network simulator: drops, delays, partitions
│   ├── node.py             the algorithm, following Figure 2 of the paper
│   ├── store.py            state machine + exactly-once client sessions
│   └── cluster.py          harness and the retrying client
├── nosql/
│   ├── embedding.py        hashed n-gram embeddings
│   └── events.py           MongoDB event store with vector search
├── dashboard/              Next.js app: auth, RBAC, live cluster view
├── tests/                  30 tests
├── docs/                   diagrams, design notes, schema docs
├── setup.py                one-command database setup
├── server.py               cluster control API
└── demos.py                8 runnable database demonstrations
```

---

## The database work

### Schema

17 tables in four groups: Raft protocol state (`node`, `term`, `vote`,
`log_entry`, `replication_state`, `snapshot`), the applied state machine
(`kv_state`, `client`, `client_session`, `client_request`), access control
(`app_user`, `app_role`, `app_permission`, two bridge relations,
`auth_session`), and auditing (`node_event`).

Five of these are weak entities owned by `node`, because every replica
keeps its own independent copy of the log, the state machine and the
client session table.

### Triggers that earn their place

`trg_vote_bi` enforces Raft's **election restriction** — a node may not
grant a vote to a candidate whose log is behind its own. `trg_vote_bu`
makes a cast vote **immutable**. `trg_log_bi` enforces **log continuity**.
`trg_node_au` writes the audit trail with no application help.

The composite primary key `(term_number, voter_id)` on `vote` does the
heaviest lifting: it makes a second vote in the same term *structurally
impossible*. Election safety is a database constraint here, not an
if-statement.

### Recursive CTE

`app_role.parent_role_id` makes the roles a hierarchy — admin inherits
operator inherits viewer. `v_role_effective_permission` walks it with a
recursive CTE, so admin resolves to 8 permissions: 4 granted directly and
4 inherited across two levels, with none duplicated in the data.

### Concurrency

`sp_cast_vote` takes `SELECT ... FOR UPDATE` on the voter's row before
deciding anything. `python demos.py locking` runs two candidates against
the same voter and shows the second blocking for ~1.4 s, then being
refused.

---

## MongoDB and vector search

The cluster emits events whose shape genuinely differs by type: an
election carries a vote tally, a partition carries two arrays of node ids,
a commit advance carries two indexes. Forcing these into one relational
table means a dozen mostly-null columns and a migration for every new
event type. Documents fit the data.

Each event's description is embedded on write, and similarity search runs
as a MongoDB aggregation pipeline — `$zip` pairs the stored embedding with
the query vector elementwise, `$reduce` sums the products. Both vectors
are L2-normalised, so that dot product is the cosine similarity.

The embedder is hashed word tokens plus character trigrams, written twice:
once in Python (`nosql/embedding.py`) and once in JavaScript
(`dashboard/lib/embedding.js`), because documents are embedded by the
cluster and queries by the dashboard. `tests/test_embedding_parity.py`
checks the two produce bit-identical vectors.

**Honest limitation:** this is lexical similarity, not a trained model.
"node crashed" matches "node 4 crashed and left" strongly; it will not
match a paraphrase that shares no characters. A sentence-transformer would
do better at the cost of a multi-hundred-megabyte dependency. MongoDB
Community also has no native vector index — `$vectorSearch` is Atlas-only
— so this is an exact search rather than an approximate one.

---

## Authentication and access control

Three roles in a hierarchy, with the permission set resolved by the
database rather than hardcoded in the app:

| Role | Direct grants | Inherits | Total |
|---|---|---|---|
| viewer | kv.read, cluster.view, event.read | — | 3 |
| operator | kv.write | viewer | 4 |
| admin | cluster.admin, node.kill, node.restart, user.manage | operator | 8 |

Sessions are signed JWTs in httpOnly cookies, with only the SHA-256 hash
stored in `auth_session` — so a leaked row cannot be replayed. Every
privileged API route calls `requirePermission(code)`, which reads
`v_user_permission`. Adding a permission to a role in SQL changes what the
app allows with no code change.

Verified behaviour: viewer gets 403 on writes, operator gets 403 on node
control, unauthenticated gets 401.

---

## Testing

```bash
python -m unittest discover -s tests -v
```

30 tests: 10 Raft integration tests against the real MySQL schema, 19
MongoDB and vector-search tests, and the cross-language embedding parity
check. The MongoDB tests fall back to `mongomock` when no server is
running, so the suite is green on a fresh machine.

```
TestElection       one leader; re-election after crash; a minority
                   cannot elect or commit
TestReplication    writes commit and read back; an acknowledged write
                   outlives its leader; replicas converge
TestPersistence    whole cluster power-cycled, data intact; votes durable
TestPartition      majority side keeps serving; single leader after heal
TestExactlyOnce    a retried request is not applied twice
```

One thing deliberately **not** asserted: an isolated node that was already
leader keeps believing it leads until it hears a higher term. That is
correct Raft — it can no longer reach a majority, so it can no longer
commit. The property that matters is that no *new* leader is elected
without a majority.

---

## Demos

```bash
python demos.py              # all eight
python demos.py locking      # one
```

| Demo | Shows |
|---|---|
| `transaction` | A deliberately failing third write rolls the first two back |
| `locking` | Two candidates, one row lock, one vote granted |
| `isolation` | REPEATABLE READ vs READ COMMITTED on the same data |
| `optimization` | `EXPLAIN` before and after: `ALL` → `ref`, 20× fewer rows |
| `cte` | The recursive CTE resolving the role hierarchy |
| `window` | `MAX() OVER ()`, `RANK()`, `LAG()` over cluster state |
| `trigger` | Every trigger rejecting the thing it exists to reject |
| `deadlock` | Opposite lock orders; InnoDB picks a victim |

---

## Known limitations

| Limitation | Why |
|---|---|
| No log compaction | Would break the index-equals-position invariant the code relies on |
| Fixed membership | Joint consensus is a project of its own |
| In-process transport | Real sockets add operational work, not insight |
| Reads go through the log | Leader leases need clock assumptions |
| Exact vector search | MongoDB Community has no HNSW index |
| Slower than an in-memory Raft | Every state change is a SQL round trip; election timeouts are 1.2–2.4 s rather than 300 ms |

---

## References

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*
  (extended version). Figure 2 is the specification `raft/node.py` follows
  rule by rule; Figure 8 is why `_advance_commit_index` has its term check.
- `docs/DESIGN.md` — internals and invariants
- `docs/SCHEMA.md` — ER model, relational schema, normalization
- `docs/RUBRIC.md` — where each marking criterion is satisfied
- `SETUP.md` — installation, including XAMPP on Windows
