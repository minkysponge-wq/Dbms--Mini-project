# Problem Statement

**Title:** Fault-Tolerant Distributed Key-Value Store using the Raft
Consensus Algorithm

**Name:** Sampreeth
**SRN:** PES1UG24AM246
**Course:** UE23CS351A — Database Management Systems

---

## Problem statement

A single database server is simple to reason about but fragile: when it
fails, its data becomes unavailable. Replicating that data across several
servers restores availability, but introduces a harder problem — the
replicas can disagree. If two servers both believe they are in charge,
each will accept writes the other never sees, and the resulting
corruption is silent: no error is raised, and the divergence is only
discovered later, if at all. This is the split-brain problem, and it is
the central obstacle to building a database that is both available and
correct.

This project builds a fault-tolerant distributed key-value store that
solves it, using the Raft consensus algorithm to make five replicas agree
on a single ordered sequence of writes. Rather than treating the
database as an external dependency, the system uses **MySQL as its
storage engine**: each node's protocol state — its current term, the vote
it has cast, and its replicated log — is persisted as relational data,
and the integrity rules the consensus algorithm depends on are enforced
as database constraints rather than as application logic. Most notably,
Raft's election-safety property, that at most one leader may be elected
per term, is guaranteed by a composite primary key on the vote relation,
making a double vote structurally impossible rather than merely
discouraged.

Alongside the relational store, the cluster emits a stream of operational
events — elections, crashes, network partitions, commit advances — whose
structure differs by event type. These are stored in **MongoDB**, where
heterogeneous documents are a natural fit, and each event description is
embedded as a vector so that operators can search the history by meaning
rather than by exact keyword. A **Next.js** web application provides the
interface, with authenticated access and a three-level role hierarchy
whose effective permissions are resolved by the database itself through a
recursive query.

The system is validated by a test suite that crashes nodes, partitions
the network and restarts the entire cluster, verifying in each case that
no acknowledged write is ever lost and no two nodes ever lead the same
term.

## Short description

A replicated key-value database spread across five nodes, which uses the
Raft consensus algorithm to agree on the order of writes so the system
keeps serving correctly even when two nodes fail. MySQL stores the
protocol state and enforces the algorithm's safety rules as constraints,
triggers and transactions; MongoDB stores the cluster event log with
vector search over event descriptions; and a Next.js dashboard provides
authenticated, role-based access to the cluster.

## Objectives

1. Design a normalised relational schema for the durable state of a
   distributed consensus protocol, with constraints that enforce the
   protocol's safety properties.
2. Implement Raft leader election, log replication and crash recovery
   with MySQL as the stable store, using transactions and row-level
   locking for correctness under concurrency.
3. Express the protocol's operations as stored procedures and its
   invariants as triggers, so that application bugs cannot violate them.
4. Store the cluster event stream in MongoDB and implement vector
   similarity search over event descriptions.
5. Build a Next.js interface with authentication and role-based access
   control resolved from the database.
6. Demonstrate correctness under node failure, network partition and
   full cluster restart.

## Scope

**In scope:** leader election, log replication with conflict
backtracking, crash recovery, exactly-once client semantics, the
relational schema and its constraints, triggers, stored procedures,
views, the MongoDB event store with vector search, and the authenticated
web interface.

**Out of scope, with reasons:** log compaction and snapshotting (would
break the index-equals-position invariant the log relies on), dynamic
cluster membership changes (joint consensus is a substantial project in
itself), and a real TCP transport (the in-process simulator allows fault
injection that real sockets would make far harder to control).

## Assumptions

- Cluster membership is fixed at startup, at five nodes.
- The system tolerates the failure of fewer than half the nodes; five
  nodes tolerate two.
- Nodes fail by crashing, not by behaving maliciously. Byzantine faults
  are outside Raft's model.
- Persistent state reaches stable storage before any RPC response is
  sent, which is what makes crash recovery safe.
- Commands are PUT, APPEND, DELETE and GET over string keys and values.
