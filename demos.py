#!/usr/bin/env python3
"""Runnable demonstrations of the database concepts the project uses.

Each one prints what it is about to do, does it against the real
database, and prints the evidence. Intended to be run in front of an
examiner, or captured for the report.

    python demos.py              # run every demo
    python demos.py locking      # run one

Demos
    transaction   atomicity and rollback
    locking       two concurrent voters, row locks, and what happens without
    isolation     READ COMMITTED vs REPEATABLE READ on the same data
    optimization  EXPLAIN before and after adding an index
    cte           the recursive CTE resolving the role hierarchy
    window        window functions over replication progress
    trigger       every trigger rejecting the thing it exists to reject
    deadlock      two transactions taking locks in opposite orders
"""

from __future__ import annotations

import sys
import threading
import time

import pymysql

from raft import db

RULE = "=" * 72


def header(title: str, explanation: str) -> None:
    print(f"\n{RULE}\n{title}\n{RULE}")
    print(explanation.strip() + "\n")


def table(rows: list[dict], columns: list[str] | None = None) -> None:
    if not rows:
        print("  (no rows)")
        return
    columns = columns or list(rows[0].keys())
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows))
              for c in columns}
    print("  " + "  ".join(c.ljust(widths[c]) for c in columns))
    print("  " + "  ".join("-" * widths[c] for c in columns))
    for row in rows:
        print("  " + "  ".join(str(row.get(c, "")).ljust(widths[c])
                               for c in columns))


def fresh_connection() -> pymysql.connections.Connection:
    """An independent connection, so two can contend for the same row."""
    cfg = db.db_config()
    cfg["autocommit"] = False
    return pymysql.connect(**cfg)


# ---------------------------------------------------------------------
# 1. Transactions
# ---------------------------------------------------------------------

def demo_transaction() -> None:
    header(
        "1. TRANSACTIONS - atomicity and rollback",
        """
Applying one committed Raft entry touches three relations: kv_state gets
the value, node advances last_applied, and client_session records the
sequence number for deduplication. If the process died between the first
write and the third, the node would re-apply the command on restart and
a retried APPEND would land twice.

So all three commit together, or none do. Here the third write is made
to fail deliberately, and we check that the first two were undone.
""")

    db.execute("DELETE FROM kv_state WHERE node_id = 0 AND key_name = 'demo_txn'")
    before = db.query_one(
        "SELECT last_applied FROM node WHERE node_id = 0")["last_applied"]
    print(f"  before: node 0 last_applied = {before}, key 'demo_txn' absent\n")

    conn = fresh_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("START TRANSACTION")
            cur.execute(
                "INSERT INTO kv_state (node_id, key_name, value_text, "
                "last_applied_index) VALUES (0, 'demo_txn', 'written', 9999)")
            cur.execute(
                "UPDATE node SET last_applied = 9999 WHERE node_id = 0")
            print("  wrote kv_state and advanced last_applied inside the"
                  " transaction")
            # Violates chk_node_applied_le_commit: applied must not exceed
            # committed.
            cur.execute(
                "UPDATE node SET last_applied = 99999, commit_index = 1 "
                "WHERE node_id = 0")
            conn.commit()
            print("  (unexpected) the third statement succeeded")
    except Exception as exc:
        conn.rollback()
        print(f"  third statement failed: {str(exc)[:88]}")
        print("  -> ROLLBACK\n")
    finally:
        conn.close()

    after = db.query_one(
        "SELECT last_applied FROM node WHERE node_id = 0")["last_applied"]
    key = db.query_one(
        "SELECT key_name FROM kv_state WHERE node_id = 0 AND key_name = 'demo_txn'")

    print(f"  after: node 0 last_applied = {after}, "
          f"key 'demo_txn' {'present' if key else 'absent'}")
    ok = after == before and key is None
    print(f"\n  {'PASS' if ok else 'FAIL'}: the partial writes were undone."
          " Nothing observed a half-applied entry.")


# ---------------------------------------------------------------------
# 2. Locking
# ---------------------------------------------------------------------

def demo_locking() -> None:
    header(
        "2. CONCURRENCY CONTROL - row locking during a vote",
        """
Two candidates solicit the same voter at the same instant. Without a
lock both could read the old term, both could be granted a vote, and two
leaders could be elected in one term - which is exactly the split-brain
Raft exists to prevent.

sp_cast_vote takes SELECT ... FOR UPDATE on the voter's row before
deciding anything. The second transaction blocks until the first
commits, then sees the updated state and refuses.
""")

    db.execute("DELETE FROM vote WHERE term_number >= 900")
    db.execute("DELETE FROM term WHERE term_number >= 900")
    db.execute("INSERT INTO term (term_number) VALUES (900)")
    db.execute("UPDATE node SET current_term = 900, voted_for = NULL "
               "WHERE node_id = 2")

    results: dict[str, object] = {}
    started = threading.Event()

    def candidate_one() -> None:
        conn = fresh_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("START TRANSACTION")
                t0 = time.time()
                cur.execute("SELECT current_term, voted_for FROM node "
                            "WHERE node_id = 2 FOR UPDATE")
                results["c1_lock_wait"] = time.time() - t0
                started.set()
                # Hold the lock, so the second candidate has to wait.
                time.sleep(1.5)
                cur.execute(
                    "INSERT INTO vote (term_number, voter_id, candidate_id, "
                    "granted, candidate_last_index, candidate_last_term) "
                    "VALUES (900, 2, 0, TRUE, 0, 0)")
                cur.execute("UPDATE node SET voted_for = 0 WHERE node_id = 2")
                conn.commit()
                results["c1"] = "granted"
        except Exception as exc:
            conn.rollback()
            results["c1"] = f"refused ({str(exc)[:40]})"
        finally:
            conn.close()

    def candidate_two() -> None:
        started.wait(5)
        time.sleep(0.1)
        conn = fresh_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("START TRANSACTION")
                t0 = time.time()
                cur.execute("SELECT current_term, voted_for FROM node "
                            "WHERE node_id = 2 FOR UPDATE")
                waited = time.time() - t0
                results["c2_lock_wait"] = waited
                cur.execute(
                    "INSERT INTO vote (term_number, voter_id, candidate_id, "
                    "granted, candidate_last_index, candidate_last_term) "
                    "VALUES (900, 2, 1, TRUE, 0, 0)")
                cur.execute("UPDATE node SET voted_for = 1 WHERE node_id = 2")
                conn.commit()
                results["c2"] = "granted"
        except Exception as exc:
            conn.rollback()
            results["c2"] = f"refused ({str(exc)[:46]})"
        finally:
            conn.close()

    t1 = threading.Thread(target=candidate_one)
    t2 = threading.Thread(target=candidate_two)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    print(f"  candidate 0: acquired the lock immediately "
          f"({results.get('c1_lock_wait', 0):.2f}s) -> {results.get('c1')}")
    print(f"  candidate 1: blocked on the lock for "
          f"{results.get('c2_lock_wait', 0):.2f}s -> {results.get('c2')}")

    votes = db.query(
        "SELECT term_number, voter_id, candidate_id, granted FROM vote "
        "WHERE term_number = 900")
    print()
    table(votes)

    print(f"\n  Node 2 cast {len(votes)} vote in term 900.")
    print("  The second candidate waited for the lock rather than reading"
          " stale state,")
    print("  and the composite primary key (term_number, voter_id) then"
          " refused the")
    print("  duplicate outright. Two independent defences, both in the"
          " database.")

    db.execute("DELETE FROM vote WHERE term_number >= 900")
    db.execute("DELETE FROM term WHERE term_number >= 900")


# ---------------------------------------------------------------------
# 3. Isolation levels
# ---------------------------------------------------------------------

def demo_isolation() -> None:
    header(
        "3. ISOLATION LEVELS - READ COMMITTED vs REPEATABLE READ",
        """
The same reader, the same two queries, a writer committing in between.
Under REPEATABLE READ (InnoDB's default) the reader sees the value it
started with. Under READ COMMITTED it sees the new one - a non-repeatable
read.

This is why Raft reads go through the log rather than reading kv_state
directly: the isolation level alone does not tell you whether the value
you read is still the committed truth cluster-wide.
""")

    db.execute(
        "INSERT INTO kv_state (node_id, key_name, value_text, last_applied_index) "
        "VALUES (0, 'iso_demo', 'original', 1) "
        "ON DUPLICATE KEY UPDATE value_text = 'original'")

    for level in ("REPEATABLE READ", "READ COMMITTED"):
        reader = fresh_connection()
        writer = fresh_connection()
        try:
            with reader.cursor() as rcur, writer.cursor() as wcur:
                rcur.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
                rcur.execute("START TRANSACTION")
                rcur.execute("SELECT value_text FROM kv_state "
                             "WHERE node_id = 0 AND key_name = 'iso_demo'")
                first = rcur.fetchone()["value_text"]

                wcur.execute("START TRANSACTION")
                wcur.execute(
                    "UPDATE kv_state SET value_text = %s "
                    "WHERE node_id = 0 AND key_name = 'iso_demo'",
                    (f"changed-under-{level.replace(' ', '-').lower()}",))
                writer.commit()

                rcur.execute("SELECT value_text FROM kv_state "
                             "WHERE node_id = 0 AND key_name = 'iso_demo'")
                second = rcur.fetchone()["value_text"]
                reader.commit()

            verdict = ("stable - the reader kept its snapshot"
                       if first == second
                       else "CHANGED - a non-repeatable read")
            print(f"  {level:<18} first read: {first}")
            print(f"  {'':<18} second read: {second}")
            print(f"  {'':<18} -> {verdict}\n")
        finally:
            reader.close()
            writer.close()
            db.execute("UPDATE kv_state SET value_text = 'original' "
                       "WHERE node_id = 0 AND key_name = 'iso_demo'")

    db.execute("DELETE FROM kv_state WHERE node_id = 0 AND key_name = 'iso_demo'")


# ---------------------------------------------------------------------
# 4. Query optimization
# ---------------------------------------------------------------------

def demo_optimization() -> None:
    header(
        "4. QUERY OPTIMIZATION - EXPLAIN before and after an index",
        """
The log table is the one that grows without bound, and the query a
leader runs most often filters it by term and commit status. The base
schema deliberately leaves that index out so the difference is
measurable rather than asserted.
""")

    # The predicate uses command_type and committed. Neither carries an
    # index in the base schema, and neither is a foreign key, so the
    # "before" case is a genuine full scan.
    #
    # term_number would have been the obvious column, but InnoDB keeps a
    # mandatory index on it for the foreign key, so the baseline would
    # already be indexed and the comparison would show nothing.
    TERMS = 40
    query = ("SELECT node_id, log_index, key_name FROM log_entry "
             "WHERE command_type = 'APPEND' AND committed = TRUE")

    db.execute("DROP INDEX IF EXISTS idx_log_cmd_committed ON log_entry")

    existing = int(db.query_one("SELECT COUNT(*) AS c FROM log_entry")["c"])
    if existing < 4000:
        print(f"  log_entry holds {existing} rows; inserting filler across"
              f" {TERMS} terms so the predicate is selective...")
        db.executemany("INSERT IGNORE INTO term (term_number) VALUES (%s)",
                       [(t,) for t in range(1, TERMS + 1)])
        # MySQL returns MAX() over an unsigned column as a Decimal, which
        # range() will not accept.
        start = int(db.query_one(
            "SELECT COALESCE(MAX(log_index), 0) AS m FROM log_entry "
            "WHERE node_id = 0")["m"])
        # Only one row in twenty is an APPEND, so the index on
        # (command_type, committed) has real selectivity to exploit.
        rows = [
            (0, i, (i % TERMS) + 1,
             "APPEND" if i % 20 == 0 else "PUT",
             f"filler{i}", "x", i % 2 == 0)
            for i in range(start + 1, start + 4001)
        ]
        db.executemany(
            "INSERT IGNORE INTO log_entry (node_id, log_index, term_number, "
            "command_type, key_name, value_text, committed) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)", rows)
        print(f"  inserted {len(rows)} rows\n")

    def explain(label: str) -> dict:
        plan = db.query(f"EXPLAIN {query}")[0]
        t0 = time.perf_counter()
        for _ in range(20):
            db.query(query)
        elapsed = (time.perf_counter() - t0) / 20 * 1000
        print(f"  {label}")
        print(f"    access type : {plan.get('type')}")
        print(f"    key used    : {plan.get('key') or 'NONE'}")
        print(f"    rows scanned: {plan.get('rows')}")
        print(f"    mean time   : {elapsed:.2f} ms over 20 runs\n")
        return {"plan": plan, "ms": elapsed}

    before = explain("WITHOUT the index:")
    db.execute("CREATE INDEX idx_log_cmd_committed "
               "ON log_entry (command_type, committed)")
    after = explain("WITH idx_log_cmd_committed (command_type, committed):")

    # EXPLAIN reports 'rows' as a string on some servers.
    rows_before = int(before["plan"].get("rows") or 1)
    rows_after = int(after["plan"].get("rows") or 1)
    print(f"  rows examined fell from {rows_before} to {rows_after} "
          f"({rows_before / max(rows_after, 1):.1f}x fewer)")
    print(f"  access type went from '{before['plan'].get('type')}' "
          f"to '{after['plan'].get('type')}'")
    print("\n  A full scan reads every row and discards most. The composite")
    print("  index lets InnoDB seek straight to the matching range.")


# ---------------------------------------------------------------------
# 5. Recursive CTE
# ---------------------------------------------------------------------

def demo_cte() -> None:
    header(
        "5. RECURSIVE CTE - resolving the role hierarchy",
        """
app_role.parent_role_id makes the roles a tree: admin inherits operator,
operator inherits viewer. A role's real permission set is its own grants
plus every ancestor's, so resolving it means walking up the tree - which
is what a recursive CTE is for.

The anchor seeds each role as its own ancestor at depth 0; the recursive
member climbs one parent per iteration.
""")

    print("  The hierarchy:")
    table(db.query(
        "SELECT r.role_name, COALESCE(p.role_name, '(root)') AS inherits_from "
        "FROM app_role r LEFT JOIN app_role p ON p.role_id = r.parent_role_id "
        "ORDER BY r.role_id"))

    print("\n  Effective permissions, resolved by v_role_effective_permission:")
    table(db.query(
        "SELECT role_name, permission_code, inherited_from_depth AS depth, "
        "granted_by_role FROM v_role_effective_permission "
        "ORDER BY role_name, inherited_from_depth, permission_code"),
        ["role_name", "permission_code", "depth", "granted_by_role"])

    counts = db.query(
        "SELECT role_name, COUNT(*) AS total, "
        "SUM(inherited_from_depth = 0) AS direct, "
        "SUM(inherited_from_depth > 0) AS inherited "
        "FROM v_role_effective_permission GROUP BY role_name ORDER BY total DESC")
    print()
    table(counts)
    print("\n  admin holds four permissions directly and inherits four more")
    print("  across two levels, without any of them being listed twice in"
          " the data.")


# ---------------------------------------------------------------------
# 6. Window functions
# ---------------------------------------------------------------------

def demo_window() -> None:
    header(
        "6. WINDOW FUNCTIONS - cluster state without collapsing rows",
        """
A window function computes across a set of rows while keeping every row.
That is exactly what a cluster view needs: each node's own numbers, plus
where it stands relative to the others.
""")

    print("  v_cluster_status - MAX() OVER () and RANK() OVER ():")
    table(db.query(
        "SELECT node_id, role, current_term, log_length, last_log_index, "
        "cluster_max_index, entries_behind, freshness_rank "
        "FROM v_cluster_status ORDER BY node_id"))

    print("\n  Running totals and term boundaries with COUNT() OVER and LAG():")
    table(db.query(
        "SELECT log_index, term_number, key_name, "
        "COUNT(*) OVER (ORDER BY log_index) AS running_total, "
        "LAG(term_number) OVER (ORDER BY log_index) AS prev_term, "
        "CASE WHEN LAG(term_number) OVER (ORDER BY log_index) <> term_number "
        "THEN 'term boundary' ELSE '' END AS note "
        "FROM log_entry WHERE node_id = 0 ORDER BY log_index LIMIT 12"))


# ---------------------------------------------------------------------
# 7. Triggers
# ---------------------------------------------------------------------

def demo_trigger() -> None:
    header(
        "7. TRIGGERS - safety rules the application cannot bypass",
        """
Each trigger enforces a rule the Raft correctness argument depends on.
Every statement below is one an application bug could plausibly issue;
the database refuses all of them.
""")

    db.execute("DELETE FROM vote WHERE term_number >= 800")
    db.execute("DELETE FROM term WHERE term_number >= 800")
    db.execute("INSERT INTO term (term_number) VALUES (800), (801)")
    db.execute("UPDATE node SET current_term = 801 WHERE node_id = 3")

    attempts = [
        ("vote in a term below the voter's current term",
         "INSERT INTO vote (term_number, voter_id, candidate_id, granted, "
         "candidate_last_index, candidate_last_term) "
         "VALUES (800, 3, 0, TRUE, 0, 0)"),
        ("vote for a candidate whose log is behind",
         "INSERT INTO vote (term_number, voter_id, candidate_id, granted, "
         "candidate_last_index, candidate_last_term) "
         "VALUES (801, 0, 3, TRUE, 0, 0)"),
        ("append a log entry leaving a gap",
         "INSERT INTO log_entry (node_id, log_index, term_number, "
         "command_type, key_name) VALUES (1, 99999, 1, 'PUT', 'gap')"),
        ("make a leader replicate to itself",
         "INSERT INTO replication_state (leader_id, follower_id) VALUES (1, 1)"),
        ("move match_index backwards",
         "UPDATE replication_state SET match_index = 0 "
         "WHERE match_index > 0 LIMIT 1"),
    ]

    for description, statement in attempts:
        try:
            db.execute(statement)
            print(f"  ACCEPTED  {description}")
            print("            ^ this should not have been allowed")
        except Exception as exc:
            message = str(exc)
            if "raft:" in message:
                message = message[message.index("raft:"):].rstrip("\")'")
            print(f"  REJECTED  {description}")
            print(f"            {message[:84]}")

    print("\n  A cast vote is also immutable:")
    db.execute("INSERT IGNORE INTO vote (term_number, voter_id, candidate_id, "
               "granted, candidate_last_index, candidate_last_term) "
               "VALUES (801, 3, 1, TRUE, 0, 0)")
    try:
        db.execute("UPDATE vote SET candidate_id = 2 "
                   "WHERE term_number = 801 AND voter_id = 3")
        print("  ACCEPTED  changing a cast vote - this should not happen")
    except Exception as exc:
        message = str(exc)
        if "raft:" in message:
            message = message[message.index("raft:"):].rstrip("\")'")
        print(f"  REJECTED  changing a cast vote")
        print(f"            {message[:84]}")

    print("\n  And the audit trigger records changes with no application help:")
    db.execute("UPDATE node SET role = 'candidate' WHERE node_id = 3")
    db.execute("UPDATE node SET role = 'follower' WHERE node_id = 3")
    table(db.query(
        "SELECT event_type, description FROM node_event "
        "WHERE node_id = 3 ORDER BY event_id DESC LIMIT 4"))

    db.execute("DELETE FROM vote WHERE term_number >= 800")
    db.execute("DELETE FROM term WHERE term_number >= 800")


# ---------------------------------------------------------------------
# 8. Deadlock
# ---------------------------------------------------------------------

def demo_deadlock() -> None:
    header(
        "8. DEADLOCK DETECTION",
        """
Two transactions lock the same two rows in opposite orders. Neither can
proceed. InnoDB detects the cycle and kills one, rather than letting
both hang forever - the victim's application is expected to retry.
""")

    outcomes: dict[str, str] = {}
    both_ready = threading.Barrier(2, timeout=10)

    def worker(name: str, first: int, second: int) -> None:
        conn = fresh_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("START TRANSACTION")
                cur.execute("SELECT node_id FROM node WHERE node_id = %s "
                            "FOR UPDATE", (first,))
                both_ready.wait()
                time.sleep(0.2)
                cur.execute("SELECT node_id FROM node WHERE node_id = %s "
                            "FOR UPDATE", (second,))
                conn.commit()
                outcomes[name] = "committed"
        except Exception as exc:
            conn.rollback()
            message = str(exc)
            outcomes[name] = ("chosen as the deadlock victim and rolled back"
                              if "eadlock" in message else f"failed: {message[:50]}")
        finally:
            conn.close()

    a = threading.Thread(target=worker, args=("transaction A", 0, 1))
    b = threading.Thread(target=worker, args=("transaction B", 1, 0))
    a.start()
    b.start()
    a.join()
    b.join()

    for name, outcome in sorted(outcomes.items()):
        print(f"  {name}: {outcome}")

    if any("victim" in o for o in outcomes.values()):
        print("\n  InnoDB broke the cycle by rolling one transaction back.")
        print("  The survivor committed. This is why application code that")
        print("  takes multiple row locks must be prepared to retry.")
    else:
        print("\n  No deadlock this run - the threads did not interleave")
        print("  closely enough. Run again; it is timing-dependent.")


DEMOS = {
    "transaction": demo_transaction,
    "locking": demo_locking,
    "isolation": demo_isolation,
    "optimization": demo_optimization,
    "cte": demo_cte,
    "window": demo_window,
    "trigger": demo_trigger,
    "deadlock": demo_deadlock,
}


def main() -> None:
    if not db.ping():
        print("MySQL is not reachable or the schema is missing.")
        print("Run:  python setup.py")
        sys.exit(1)

    requested = sys.argv[1:]
    if requested:
        unknown = [name for name in requested if name not in DEMOS]
        if unknown:
            print(f"Unknown demo(s): {', '.join(unknown)}")
            print(f"Available: {', '.join(DEMOS)}")
            sys.exit(1)
        selected = requested
    else:
        selected = list(DEMOS)

    for name in selected:
        try:
            DEMOS[name]()
        except Exception as exc:
            print(f"\n  demo '{name}' failed: {exc}")

    print(f"\n{RULE}\nDone.\n{RULE}")


if __name__ == "__main__":
    main()
