# Technology Justification

Why each piece of the stack was chosen for *this* problem, and what the
alternatives would have cost.

---

## MySQL / MariaDB — the relational layer

**What it holds:** the Raft protocol state (`node`, `term`, `vote`,
`log_entry`, `replication_state`), the applied state machine
(`kv_state`, `client_session`), and access control.

**Why relational.** Every one of these has a hard integrity rule that
must never be violated, and relational constraints enforce rules rather
than merely describing them:

- A node must vote **at most once per term**. The composite primary key
  `(term_number, voter_id)` on `vote` makes a second vote structurally
  impossible. This is Raft's election-safety property expressed as a key
  constraint — not an `if` statement that a bug could skip.
- A node can never have applied an entry it has not committed:
  `CHECK (last_applied <= commit_index)`.
- Applying an entry touches three relations atomically. That needs real
  ACID transactions, which is the core thing a document store gives up.

**Why not PostgreSQL**, which would have given native arrays and a real
vector extension in `pgvector`: MySQL is the course's database, it is
what the marking environment has, and XAMPP puts MariaDB on a student
machine in one click. Choosing the database the examiner already has
running is worth more here than `pgvector` would be.

**Why not SQLite**, the obvious "no server" option: it has no stored
procedures, which costs an entire category of the work, and its locking
is file-level, so the row-locking demonstration would be impossible.

**MariaDB compatibility** was treated as a requirement rather than a
nice-to-have, because XAMPP ships MariaDB and that is how this will most
likely be run. One incompatibility surfaced and is worked around: MariaDB
rejects `CHECK` constraints on foreign-key columns that cascade, so the
"a leader does not replicate to itself" rule lives in a trigger instead.
The schema loads unchanged on both.

---

## MongoDB — the document layer

**What it holds:** the cluster event stream — elections, role changes,
crashes, partitions, commit advances.

**Why a document store, specifically.** These events genuinely have
different shapes:

```json
{ "event_type": "became_leader",  "term": 4, "votes_received": 3, "majority_needed": 3 }
{ "event_type": "partition",      "minority": [1,3], "majority": [0,2,4] }
{ "event_type": "commit_advance", "old_index": 4, "new_index": 7 }
```

Forcing these into one relational table means a dozen mostly-null columns
and an `ALTER TABLE` every time a new event type is added. The
alternative — one table per event type — turns "show me everything that
happened around 14:32" into a seven-way `UNION`.

The access pattern fits too. Events are written once, never updated in
the normal path, read in time order, and aged out wholesale. There are no
joins and no integrity rules between events. That is a document
workload, and none of what MySQL is good at would be used.

**The split is the argument.** Data with invariants goes in the
relational store; the append-only observability stream goes in the
document store. Each database holds what it is actually good at. Using
one for both would mean either nullable sprawl in MySQL or giving up the
constraints that make the Raft implementation safe.

**Why not Redis**, the other natural fit for a key-value project: Redis
has no durable query layer for this, its Windows support is unofficial,
and the vector features need the Redis Stack distribution. MongoDB
installs from an MSI, runs as a service, and ships Compass for inspecting
documents.

**Why not Neo4j.** The syllabus covers graph databases, but there is no
honest graph in a Raft cluster. Adding one would be visible
marks-chasing. The role hierarchy is the one tree-shaped thing here, and
a recursive CTE handles it natively in MySQL — which is also a
demonstration the course asks for.

---

## Vector search — inside MongoDB

Each event description is embedded into 128 dimensions on write, and
similarity search runs as an aggregation pipeline: `$zip` pairs the
stored embedding with the query vector elementwise, `$reduce` sums the
products. Both vectors are L2-normalised, so that dot product *is* the
cosine similarity.

**Why embedded in MongoDB rather than a dedicated vector database.**
Adding Pinecone, Qdrant or Chroma would mean a third system to install
and keep in sync with the event stream, for a corpus of a few thousand
short strings. Storing the vector next to the document it describes keeps
them atomic — one insert, no sync problem, no orphans.

**The embedder is hand-written**, hashed word tokens plus character
trigrams. A sentence-transformer would give better semantics but costs a
multi-hundred-megabyte PyTorch install and a model download, and cannot
run offline on a fresh machine. The trade is stated plainly rather than
hidden: this captures lexical and sub-word similarity — "node crashed"
ranks "node 4 crashed and left" first — but it will not match a
paraphrase sharing no characters. `embed()` is the single swap point.

**Two honest limitations:**

1. MongoDB Community has no vector index. `$vectorSearch` is Atlas-only,
   so this is an exact scan. Correct, and fine at this scale; a
   production corpus would want HNSW.
2. The embedder exists twice — Python embeds documents, JavaScript embeds
   queries typed into the dashboard. Both had to use SHA-256 rather than
   BLAKE2b, because BLAKE2b folds output length into its IV and Node
   cannot reproduce Python's 8-byte variant.
   `tests/test_embedding_parity.py` asserts the two produce bit-identical
   vectors, because if they ever diverged the scores would become
   meaningless silently.

---

## Python — the cluster

**Why Python.** Threads and `threading.Condition` map directly onto
Raft's state machine; `pymysql` is a pure-Python driver that installs
without a compiler on Windows; and the standard library's `http.server`
gives a control API with no web framework to install.

**Why not Go**, which is where consensus implementations usually live and
which would be an order of magnitude faster: it is outside the course,
and a language the student cannot defend in a viva is a liability
regardless of how well it performs.

**The cost, stated:** every state change is a SQL round trip, so election
timeouts are 1.2–2.4 seconds rather than the 150–300 ms an in-memory
implementation would use. The system is correct but slow. For a five-node
demonstration that is irrelevant; for a real deployment it would not be.

---

## Next.js — the interface

Named in the syllabus, and genuinely the right shape here. App Router
server components resolve the session **before** any markup is sent, so
an unauthenticated visitor never receives the dashboard at all. API
routes run server-side, which means the database credentials and the
`mysql2` and `mongodb` drivers stay out of the browser bundle — a
client-only framework would have needed a separate backend for exactly
that reason.

**Auth choices.** Sessions are signed JWTs in httpOnly cookies, so the
browser cannot read or forge them, and only the SHA-256 hash is stored in
`auth_session`, so a leaked database row cannot be replayed. A revoke
takes effect on the next request rather than whenever the token happens
to expire.

**RBAC lives in the database, not the code.** Every route calls
`requirePermission(code)`, which reads the recursive view
`v_user_permission`. Granting a role a new permission is an `INSERT`; no
file changes. That is what "role-based access control in the database"
should mean.

---

## Summary

| Layer | Choice | Decided by |
|---|---|---|
| Relational | MySQL / MariaDB | Constraints enforce Raft's safety rules; transactions make applying an entry atomic |
| Document | MongoDB | Events have genuinely heterogeneous shapes and no integrity rules |
| Vector | Embedded in MongoDB | Keeps the vector atomic with its document; no third system |
| Cluster | Python | In the course, installs without a compiler, threads fit the algorithm |
| Web | Next.js | Named in the syllabus; server components keep credentials server-side |
| Not used | Neo4j | No honest graph exists in this system |
| Not used | Redis | Poor Windows story; MongoDB covers the NoSQL requirement better |
| Not used | pgvector | Would mean abandoning MySQL, which is the course database |
