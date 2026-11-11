# ER Model, Relational Schema and Normalization

![ER diagram](er_diagram.png)

*`er_diagram.drawio` opens in [draw.io](https://app.diagrams.net) if you
want to rearrange it or export at a different size. Both it and the PNG
are generated from one model by `make_er_diagram.py`, so they cannot
drift apart.*

![Relational schema](relational_schema.png)

---

## 1. Entities

**Twelve entities.** Weak entities are marked and are drawn with a
double border in the diagram.

### Raft protocol state

| Entity | Weak? | Description |
|---|---|---|
| `NODE` | no | A server in the cluster: address, role, current term, vote, commit and apply progress |
| `TERM` | no | A logical clock interval. Terms increase monotonically; each has at most one leader |
| `LOG_ENTRY` | **yes** | One entry in a node's replicated log |
| `SNAPSHOT` | no | A compacted checkpoint up to a log index |

### Applied state machine

| Entity | Weak? | Description |
|---|---|---|
| `KV_STATE` | **yes** | The key-value pairs produced by applying committed entries |
| `CLIENT` | no | Client identity, cluster-wide |
| `CLIENT_REQUEST` | no | One client command and its outcome |

### Access control

| Entity | Weak? | Description |
|---|---|---|
| `APP_USER` | no | A dashboard user |
| `APP_ROLE` | no | A role, with `parent_role_id` forming a hierarchy |
| `APP_PERMISSION` | no | A named capability |
| `AUTH_SESSION` | no | An issued login session |

### Auditing

| Entity | Weak? | Description |
|---|---|---|
| `NODE_EVENT` | no | Written by triggers on every role, term, commit or liveness change |

### Why `LOG_ENTRY` and `KV_STATE` are weak

Both are owned by a `NODE`, because **every replica keeps its own
independent copy**. The same `log_index` on two nodes may legitimately
hold different entries until the logs converge, so the index alone
cannot identify an entry. Their keys are `(node_id, log_index)` and
`(node_id, key_name)`.

This is not a modelling nicety. An early version of the schema keyed the
client session table on `client_id` alone; all five replicas then shared
one deduplication row, and nodes that had not yet applied a command
treated it as a duplicate and skipped it. The replicas diverged
silently. See `DESIGN.md` §7.

### Non-simple attributes

The relational side is deliberately all atomic — first normal form.
Two attributes would naturally be composite or multi-valued and are
handled rather than stored as lists:

- A node's **address** is `(host_address, port)`, kept as two atomic
  attributes with a `UNIQUE` constraint over the pair.
- A node's **set of followers with their progress** is multi-valued, so
  it is extracted into `REPLICATION_STATE` instead of being a repeating
  group inside `NODE`.
- A role's **effective permissions** are derived, not stored — computed
  by the recursive view, so no transitive duplication exists.

The MongoDB side is where genuinely non-simple data lives: each event
document carries a 128-element embedding array and type-specific nested
fields.

---

## 2. Relationships

### Simple relationships (1:N)

Drawn as labelled connectors carrying their cardinality.

| Relationship | Cardinality | Notes |
|---|---|---|
| TERM elects NODE | 1:0..1 | a split vote elects nobody |
| NODE stores LOG_ENTRY | 1:N | identifying |
| TERM stamps LOG_ENTRY | 1:N | |
| NODE applies KV_STATE | 1:N | identifying |
| NODE captures SNAPSHOT | 1:N | |
| NODE emits NODE_EVENT | 1:N | |
| CLIENT issues CLIENT_REQUEST | 1:N | |
| APP_USER owns CLIENT | 1:N | |
| APP_USER opens AUTH_SESSION | 1:N | |
| APP_USER owns KV_STATE key | 1:N | optional |
| **APP_ROLE inherits APP_ROLE** | N:1 | **recursive**, the role hierarchy |
| **NODE voted_for NODE** | N:1 | **recursive**, self-referencing |

### Relationships that carry their own attributes

Drawn as diamonds, because each one maps to a relation of its own.

| Relationship | Degree | Participants | Attributes |
|---|---|---|---|
| **VOTE** | ternary, M:N | TERM, NODE (voter), NODE (candidate) | granted, candidate_last_index, candidate_last_term, voted_at |
| **REPLICATION_STATE** | M:N, recursive | NODE (leader), NODE (follower) | next_index, match_index, last_contact |
| **CLIENT_SESSION** | M:N | NODE, CLIENT | last_seq_seen, last_result, updated_at |
| **USER_ROLE** | M:N | APP_USER, APP_ROLE | granted_at |
| **ROLE_PERMISSION** | M:N | APP_ROLE, APP_PERMISSION | — |

Three recursive relationships, all load-bearing: a node votes for a
node, a leader tracks followers, and a role inherits from a role. In
`REPLICATION_STATE` the two participating entity sets are both `NODE`,
so `leader_id` and `follower_id` become distinct foreign keys into the
same relation.

`VOTE` is genuinely **ternary**: a vote is meaningless without all three
of the term it was cast in, the node casting it and the node receiving
it. Modelling it as a binary relationship between two of the three would
lose the constraint that a node votes once *per term*.

---

## 2a. Relations that are not entities in the ER model

Mapping the ER model to the relational model produces five relations
that were **relationships**, not entities. Each arises because a
many-to-many (or ternary) relationship cannot be represented by a
foreign key on either participant.

| Relation | Came from | Primary key | Why it is a relation |
|---|---|---|---|
| `VOTE` | the ternary VOTE relationship | `(term_number, voter_id)` | A ternary relationship always maps to a relation. The key is narrower than all three participants on purpose: it is what limits a node to one vote per term |
| `REPLICATION_STATE` | NODE tracks NODE | `(leader_id, follower_id)` | Recursive M:N; neither node can hold a single foreign key to the other |
| `CLIENT_SESSION` | NODE : CLIENT | `(node_id, client_id)` | M:N, because every replica tracks every client independently |
| `USER_ROLE` | APP_USER holds APP_ROLE | `(user_id, role_id)` | M:N bridge |
| `ROLE_PERMISSION` | APP_ROLE grants APP_PERMISSION | `(role_id, permission_id)` | M:N bridge |

The relational model therefore has **17 relations**: the 12 entities
plus these 5.

Note what `VOTE`'s key does. The relationship is ternary, so the
mechanical mapping would key it on all three participants
`(term_number, voter_id, candidate_id)`. Keying it on
`(term_number, voter_id)` instead is a deliberate tightening: it makes a
second vote by the same node in the same term violate the primary key,
which is precisely Raft's election-safety property. The constraint is
stronger than the ER model alone requires, and that is the point.

---

## 3. Relational schema

```sql
NODE(node_id PK, host_address, port, role, current_term -> TERM,
     voted_for -> NODE, commit_index, last_applied, is_alive)

TERM(term_number PK, leader_id -> NODE, started_at, ended_at)

VOTE(term_number -> TERM, voter_id -> NODE, candidate_id -> NODE,
     granted, candidate_last_index, candidate_last_term, voted_at)
     PRIMARY KEY (term_number, voter_id)

LOG_ENTRY(node_id -> NODE, log_index, term_number -> TERM,
          command_type, key_name, value_text, client_id, seq_no, committed)
          PRIMARY KEY (node_id, log_index)

KV_STATE(node_id -> NODE, key_name, value_text, last_applied_index,
         owner_user_id -> APP_USER, updated_at)
         PRIMARY KEY (node_id, key_name)

REPLICATION_STATE(leader_id -> NODE, follower_id -> NODE,
                  next_index, match_index, last_contact)
                  PRIMARY KEY (leader_id, follower_id)

SNAPSHOT(snapshot_id PK, node_id -> NODE, last_included_index,
         last_included_term, state_blob)

CLIENT(client_id PK, user_id -> APP_USER, opened_at)

CLIENT_SESSION(node_id -> NODE, client_id -> CLIENT,
               last_seq_seen, last_result, updated_at)
               PRIMARY KEY (node_id, client_id)

CLIENT_REQUEST(request_id PK, client_id -> CLIENT, seq_no, command_type,
               key_name, value_text, assigned_index, status)

APP_USER(user_id PK, username, email, password_hash, is_active)
APP_ROLE(role_id PK, role_name, description, parent_role_id -> APP_ROLE)
APP_PERMISSION(permission_id PK, permission_code, description)
USER_ROLE(user_id -> APP_USER, role_id -> APP_ROLE, granted_at)
          PRIMARY KEY (user_id, role_id)
ROLE_PERMISSION(role_id -> APP_ROLE, permission_id -> APP_PERMISSION)
                PRIMARY KEY (role_id, permission_id)
AUTH_SESSION(token_hash PK, user_id -> APP_USER, issued_at,
             expires_at, revoked)

NODE_EVENT(event_id PK, node_id -> NODE, event_type, old_value,
           new_value, term_number, description, occurred_at)
```

---

## 4. Functional dependencies

```
NODE              node_id → host_address, port, role, current_term,
                            voted_for, commit_index, last_applied, is_alive
                  {host_address, port} → node_id        (candidate key)

TERM              term_number → leader_id, started_at, ended_at

VOTE              {term_number, voter_id} → candidate_id, granted,
                            candidate_last_index, candidate_last_term, voted_at

LOG_ENTRY         {node_id, log_index} → term_number, command_type,
                            key_name, value_text, client_id, seq_no, committed

KV_STATE          {node_id, key_name} → value_text, last_applied_index,
                            owner_user_id, updated_at

REPLICATION_STATE {leader_id, follower_id} → next_index, match_index,
                            last_contact

CLIENT_SESSION    {node_id, client_id} → last_seq_seen, last_result

CLIENT_REQUEST    request_id → client_id, seq_no, command_type, status
                  {client_id, seq_no} → request_id      (candidate key)

APP_USER          user_id → username, email, password_hash, is_active
                  username → user_id                    (candidate key)
                  email    → user_id                    (candidate key)

APP_ROLE          role_id → role_name, description, parent_role_id
                  role_name → role_id                   (candidate key)

APP_PERMISSION    permission_id → permission_code, description
                  permission_code → permission_id       (candidate key)

AUTH_SESSION      token_hash → user_id, issued_at, expires_at, revoked
```

---

## 5. Normalization

### 1NF
Every attribute is atomic. The one multi-valued fact — a leader's set of
followers and their progress — is extracted into `REPLICATION_STATE`
rather than held as a repeating group.

### 2NF
No partial dependency on part of a composite key.

- `VOTE`: `granted` and `voted_at` depend on both `term_number` and
  `voter_id`. Neither alone determines them — the same voter votes in
  many terms, and many voters vote in one term.
- `LOG_ENTRY`: `command_type`, `key_name` and `value_text` depend on the
  full key. The same `log_index` on a different node may hold a
  different entry, which is precisely what makes this weak.
- `KV_STATE`: `value_text` depends on both node and key.
- `REPLICATION_STATE`: `next_index` and `match_index` are properties of
  the leader–follower *pair*.

### 3NF
No transitive dependency.

- `CLIENT_SESSION` holds no client attributes; those live in `CLIENT`.
  Keeping `opened_at` here would make it transitively dependent on
  `client_id` rather than on the whole key.
- `KV_STATE` stores the applied value, not the command that produced it;
  the command stays in `LOG_ENTRY`. Storing both would duplicate derived
  state.
- `USER_ROLE` and `ROLE_PERMISSION` hold no role or permission
  attributes.
- Effective permissions are **derived**, not stored — the recursive view
  computes them, so an inherited permission exists in exactly one place.

### BCNF
Every determinant is a candidate key.

Checked relation by relation. The only ones with more than one candidate
key are `NODE` (`node_id`, and `{host_address, port}`), `APP_USER`
(`user_id`, `username`, `email`), `APP_ROLE` (`role_id`, `role_name`),
`APP_PERMISSION` (`permission_id`, `permission_code`) and
`CLIENT_REQUEST` (`request_id`, `{client_id, seq_no}`). In each case
every determinant listed in §4 is one of those candidate keys, so **all
17 relations are in BCNF**.

No decomposition was required to reach BCNF, and none was performed —
decomposing further would split keys that the integrity constraints
depend on.

---

## 6. Key integrity constraints

**Election safety.** `PRIMARY KEY (term_number, voter_id)` on `VOTE`
makes a second vote in the same term structurally impossible. Raft's
election-safety property — at most one leader per term — is enforced as a
key constraint, not as application logic.

**Log matching.** `(node_id, log_index)` is unique and `term_number` is
recorded, so the log-matching property is checkable: if two nodes hold an
entry with the same index and term, every preceding entry is identical.

**Exactly-once clients.** `CLIENT_SESSION.last_seq_seen` together with
`CLIENT_REQUEST.seq_no` deduplicates retries. Without it the system is
at-least-once and a retried `APPEND` corrupts its value.

**Recursive integrity.** `app_role.parent_role_id` is a self-referencing
foreign key; the recursive CTE that walks it caps depth at 16 so a cyclic
hierarchy cannot produce infinite recursion.

**Cross-relation invariants** enforced by CHECK:
`last_applied <= commit_index`, `match_index < next_index`,
`expires_at > issued_at`, a write command must name a key, and a port
must be in range.

---

## 7. Mapping to the implementation

| Relation | Where it is used |
|---|---|
| `NODE` | `raft/node.py` — term, vote, role, commit progress |
| `TERM` | incremented by `_start_election` and `sp_start_election` |
| `VOTE` | written by `_record_vote_row`; guarded by `trg_vote_bi` |
| `LOG_ENTRY` | `_persist_append`, `_persist_truncate` |
| `KV_STATE` | `raft/store.py` — the applied state machine |
| `REPLICATION_STATE` | `_persist_replication_state`; read by `v_replication_lag` |
| `CLIENT_SESSION` | deduplication in `store.apply` and `sp_apply_committed` |
| `APP_*`, `USER_ROLE`, `ROLE_PERMISSION` | `dashboard/lib/auth.js` via `v_user_permission` |
| `NODE_EVENT` | written only by `trg_node_au` |
