# Evaluation Criteria — where each mark is satisfied

UE23CS351A Experiential Learning Level 3, 50 marks.

Every row names the file to open and the command to run, so nothing has
to be taken on trust.

---

## 1. ER diagram — 2 marks

`docs/er_diagram.png`

17 entities. Weak entities are shaded and labelled: `LOG_ENTRY`,
`KV_STATE`, `CLIENT_SESSION`, `REPLICATION_STATE`, `VOTE`, `USER_ROLE`,
`ROLE_PERMISSION`. Crow's-foot cardinality on every relationship.

Two recursive relationships, both real rather than decorative:

- `NODE` → `NODE` (`voted_for`, and `tracks` resolved into
  `REPLICATION_STATE` where `leader_id` and `follower_id` both reference
  `node`)
- `APP_ROLE` → `APP_ROLE` (`parent_role_id`, the role hierarchy)

Regenerate with `python docs/make_diagrams.py` — both diagrams are built
from one declaration, so they cannot drift apart.

## 2. Relational schema with keys, relationships, constraints — 3 marks

`docs/relational_schema.png` and `docs/SCHEMA.md`

Every relation as a row of attributes, primary keys underlined, foreign
keys italicised, arrows from each foreign key to the key it references.
`docs/SCHEMA.md` carries the textual schema, the functional dependencies
and the normalization argument to BCNF.

## 3. SQL DDL with constraints — 4 marks

`sql/01_schema.sql`

| Constraint type | Count | Examples |
|---|---|---|
| Primary keys | 17 | 6 composite, e.g. `(term_number, voter_id)` |
| Foreign keys | 23 | including 3 self-referencing |
| Unique | 7 | `uq_node_endpoint`, `uq_client_request_seq` |
| Check | 6 | `chk_node_applied_le_commit`, `chk_repl_match_lt_next` |
| Indexes | 24 total | plus 2 FULLTEXT |

Verify: `python setup.py` then
`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='raftkv';`

## 4. NoSQL / Vector database creation — 4 marks

`nosql/events.py`, created by `python setup.py`

MongoDB database `raftkv_events`, collection `cluster_events`, five
indexes: compound `(node_id, occurred_at)`, `event_type`, `term`, unique
`event_id`, and a text index on `description`.

Each document carries a 128-dimension embedding of its description,
generated on write.

## 5. Technology justification — 1 mark

`docs/TECH_JUSTIFICATION.md`

## 6. DML operations — 4 marks

`sql/06_queries.sql` sections 1–8, plus every write path in
`raft/store.py` and `sql/04_procedures.sql`.

INSERT (including `INSERT ... ON DUPLICATE KEY UPDATE` and
`INSERT IGNORE`), UPDATE (including multi-table `UPDATE ... JOIN`),
DELETE (including cascade-driven), SELECT with aggregates and grouping.

Run: `mysql raftkv < sql/06_queries.sql`

## 7. Nested / correlated / recursive / join queries — 4 marks

`sql/06_queries.sql` sections 1–5.

| Kind | Example | Location |
|---|---|---|
| Inner join | which node leads each term | §1.1 |
| Left join | nodes with no vote this term | §1.2 |
| Self join | nodes disagreeing at the same log index | §1.3 |
| Three-table join | user → role → permission | §1.4 |
| Nested subquery | nodes whose log is short of the longest | §2.1 |
| Correlated subquery | entries each follower is missing | §3.1 |
| Correlated `NOT EXISTS` | nodes that voted in every term | §3.2 |
| Recursive CTE | the role hierarchy | §4.1 |
| Recursive CTE | leadership succession chain | §4.2 |
| Set operations | UNION, difference, intersection | §5 |

Demo: `python demos.py cte`

## 8. Advanced database concepts — 6 marks

**Procedures and functions** — `sql/04_procedures.sql`, 7 procedures and
3 functions. Each mutating procedure runs in an explicit transaction with
`DECLARE EXIT HANDLER FOR SQLEXCEPTION ... ROLLBACK`.

- `sp_start_election`, `sp_cast_vote`, `sp_append_entry`,
  `sp_advance_commit_index`, `sp_apply_committed`, `sp_kill_node`,
  `sp_restart_node`
- `fn_majority_size`, `fn_last_log_term`, `fn_user_has_permission`

**Triggers** — `sql/03_triggers.sql`, 6 triggers, each enforcing a rule
Raft's correctness depends on:

| Trigger | Enforces |
|---|---|
| `trg_vote_bi` | no stale-term vote; the election restriction |
| `trg_vote_bu` | a cast vote is immutable |
| `trg_log_bi` | log continuity — no gaps |
| `trg_node_au` | automatic audit of role, term, commit and liveness changes |
| `trg_repl_bi` / `trg_repl_bu` | no self-replication; `match_index` monotonic |

Demo: `python demos.py trigger` — every trigger refusing a violation.

**CTEs** — `v_commit_quorum` (Raft's majority rule as a query),
`v_role_effective_permission` (recursive).

Also: transactions, `SELECT ... FOR UPDATE` row locking, isolation levels
and deadlock detection — `python demos.py`.

## 9. NoSQL / Vector feature, RUD — 7 marks

`nosql/events.py`, `dashboard/app/api/events/`, 19 tests in
`tests/test_nosql.py`.

| Operation | Method | Test |
|---|---|---|
| Create | `record`, `record_many` | `test_create_stores_every_event` |
| Read | `get`, `find`, `summary_by_type`, `text_search` | `test_read_filters_by_node_and_type` |
| Update | `annotate` (re-embeds), `set_severity`, `tag_many` | `test_update_annotates_and_reembeds` |
| Delete | `delete`, `purge_before` | `test_delete_by_retention_cutoff` |

**Vector feature.** Cosine similarity computed inside MongoDB with a
`$zip` / `$reduce` aggregation pipeline, with a Python fallback. A test
asserts the two paths rank results identically.

The embedder exists twice — Python for documents, JavaScript for queries
typed into the dashboard — and `tests/test_embedding_parity.py` asserts
the two produce bit-identical vectors across seven inputs including
Unicode and empty strings.

Live: Event log tab → search `which node crashed` in vector mode.

## 10. Next.js interface — 3 marks

`dashboard/`, Next.js 14 App Router.

Three tabs: Cluster (live node cards, crash/restart, partition controls,
replication lag, commit quorum, leadership timeline), Key-value store
(write form, full-text search, replica counts), Event log (vector and
keyword search, annotate, delete).

Polls every 2 seconds, so a crash and the election that follows are
visible as they happen. Build: `cd dashboard && npm run build`.

## 11. Backend / API and database integration — 2 marks

`dashboard/lib/db.js`, `dashboard/app/api/*`

Eleven API routes. They connect to **MySQL directly** with `mysql2`
(pooled, parameterised) and to **MongoDB directly** with the official
driver. Control actions proxy to `server.py`, because writing a key must
go through consensus — writing straight to `kv_state` would leave the log
unable to explain the state.

The cluster view is read from `v_cluster_status`, `v_replication_lag`,
`v_commit_quorum` and `v_leader_timeline`, so the window functions and
CTEs run server-side.

## 12. Authentication, two roles, RBAC in the database — 4 marks

`dashboard/lib/auth.js`, `sql/01_schema.sql`, `sql/05_seed.sql`

Three roles, not two, in a hierarchy: `viewer` ← `operator` ← `admin`.
Permissions resolve through the recursive view, so admin holds 8
permissions of which 4 are inherited.

Passwords are bcrypt. Sessions are signed JWTs in httpOnly cookies, with
only the SHA-256 hash in `auth_session`, so a logout or revoke takes
effect immediately rather than when the token expires.

Every privileged route calls `requirePermission(code)`, which reads
`v_user_permission`. Granting a role a permission in SQL changes what the
app allows with no code change.

Verified:

| | GET /api/cluster | POST /api/kv | POST /api/node/kill |
|---|---|---|---|
| unauthenticated | 401 | 401 | 401 |
| viewer | 200 | **403** | **403** |
| operator | 200 | 200 | **403** |
| admin | 200 | 200 | 200 |

## 13. Documentation, Git/GitHub — 2 marks

`README.md`, `SETUP.md`, `docs/DESIGN.md`, `docs/SCHEMA.md`,
`docs/TECH_JUSTIFICATION.md`, this file.

Git history is yours to create — commit as you work rather than in one
dump.

## 14. Originality, individual contribution, viva — 4 marks

Yours to earn. What helps:

- The topic is not a CRUD application. A consensus algorithm with MySQL
  as its storage engine is unusual, and the integration is the
  interesting part.
- Three bugs were found by testing and are documented in
  `docs/DESIGN.md`: the `client_session` key error that made replicas
  diverge, the missing no-op append that left entries uncommitted after
  failover, and the per-request clerk that tripped the deduplication.
- Be able to explain, from understanding: why the `vote` primary key is
  composite, why `_advance_commit_index` has a term check (Figure 8), and
  why reads go through the log.

---

## Self-assessment

| # | Criterion | Max | Evidence |
|---|---|---|---|
| 1 | ER diagram | 2 | 17 entities, weak entities marked, 2 recursive relationships |
| 2 | Relational schema | 3 | Schema diagram + FD and BCNF analysis |
| 3 | DDL with constraints | 4 | 17 tables, 23 FKs, 6 checks, 7 unique, 24 indexes |
| 4 | NoSQL/Vector creation | 4 | MongoDB collection, 5 indexes, 128-d embeddings |
| 5 | Technology justification | 1 | `docs/TECH_JUSTIFICATION.md` |
| 6 | DML operations | 4 | `sql/06_queries.sql` §1–8 |
| 7 | Nested/correlated/recursive/join | 4 | 10 distinct query forms |
| 8 | Procedure/function/trigger/CTE | 6 | 10 routines, 6 triggers, 2 CTE views |
| 9 | NoSQL/Vector feature + RUD | 7 | Full CRUD, pipeline vector search, 19 tests |
| 10 | Next.js interface | 3 | 3 tabs, live polling, build passes |
| 11 | Backend/API integration | 2 | 11 routes, direct MySQL + MongoDB |
| 12 | Auth + RBAC | 4 | 3 roles, bcrypt, JWT, DB-resolved permissions |
| 13 | Documentation, Git | 2 | 6 documents |
| 14 | Originality, viva | 4 | — |
| | **Total** | **50** | |
