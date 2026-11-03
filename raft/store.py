"""The replicated key-value state machine.

Raft guarantees every node applies the same commands in the same order.
This module turns that ordered stream into an actual key-value map held
in ``kv_state``, and adds the client-session bookkeeping that upgrades
the system from at-least-once to exactly-once.

Applying one entry touches three relations - ``kv_state`` gets the new
value, ``node`` advances ``last_applied``, and ``client_session``
records the sequence number - so it runs inside one transaction. If the
process died between the first write and the third, the node would
re-apply the command on restart and a retried APPEND would land twice.
"""

from __future__ import annotations

from typing import Any

from . import db
from .node import ApplyMsg, Command


class KVStore:
    """One node's view of the applied state machine."""

    def __init__(self, node_id: int) -> None:
        self.node_id = node_id

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def apply(self, msg: ApplyMsg) -> dict[str, Any]:
        """Apply one committed entry, atomically.

        Returns the result the client should see. A duplicate is detected
        and skipped rather than applied twice.
        """
        cmd = msg.command
        if cmd.op in ("GET", "NOOP"):
            # A GET goes through the log so that a partitioned leader
            # cannot serve a stale read from its local map, but applying
            # it mutates nothing.
            if cmd.op == "GET":
                return self.get(cmd.key)
            return {"ok": True}

        with db.transaction() as conn:
            with conn.cursor() as cur:
                # Duplicate detection, on every replica rather than only
                # the leader. A retry can reach a different replica and
                # be replicated again; if only the leader deduplicated,
                # the replicas would diverge.
                last_seen = None
                if cmd.client_id:
                    cur.execute(
                        "SELECT last_seq_seen FROM client_session "
                        "WHERE node_id = %s AND client_id = %s FOR UPDATE",
                        (self.node_id, cmd.client_id))
                    row = cur.fetchone()
                    if row:
                        last_seen = int(row["last_seq_seen"])

                if (cmd.client_id and last_seen is not None
                        and cmd.seq_no <= last_seen):
                    cur.execute(
                        "UPDATE node SET last_applied = %s WHERE node_id = %s",
                        (msg.index, self.node_id))
                    return {"ok": True, "duplicate": True}

                if cmd.op == "PUT":
                    cur.execute(
                        "INSERT INTO kv_state (node_id, key_name, value_text, "
                        "last_applied_index) VALUES (%s, %s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE value_text = VALUES(value_text), "
                        "last_applied_index = VALUES(last_applied_index)",
                        (self.node_id, cmd.key, cmd.value, msg.index))
                elif cmd.op == "APPEND":
                    cur.execute(
                        "INSERT INTO kv_state (node_id, key_name, value_text, "
                        "last_applied_index) VALUES (%s, %s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE "
                        "value_text = CONCAT(kv_state.value_text, VALUES(value_text)), "
                        "last_applied_index = VALUES(last_applied_index)",
                        (self.node_id, cmd.key, cmd.value, msg.index))
                elif cmd.op == "DELETE":
                    cur.execute(
                        "DELETE FROM kv_state WHERE node_id = %s AND key_name = %s",
                        (self.node_id, cmd.key))

                if cmd.client_id:
                    cur.execute(
                        "INSERT IGNORE INTO client (client_id) VALUES (%s)",
                        (cmd.client_id,))
                    cur.execute(
                        "INSERT INTO client_session (node_id, client_id, "
                        "last_seq_seen) VALUES (%s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE "
                        "last_seq_seen = VALUES(last_seq_seen)",
                        (self.node_id, cmd.client_id, cmd.seq_no))

                cur.execute(
                    "UPDATE node SET last_applied = %s WHERE node_id = %s",
                    (msg.index, self.node_id))

        return {"ok": True}

    def catch_up(self) -> int:
        """Apply every committed-but-unapplied entry using the stored
        procedure.

        Used on restart, when a node may come back with entries already
        committed in its log but not yet reflected in ``kv_state``. The
        procedure does the whole batch in one transaction server-side.
        """
        result = db.call_proc("sp_apply_committed", (self.node_id, 0))
        return int(result[-1]) if result else 0

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get(self, key: str) -> dict[str, Any]:
        row = db.query_one(
            "SELECT value_text FROM kv_state WHERE node_id = %s AND key_name = %s",
            (self.node_id, key))
        if row is None:
            return {"ok": True, "found": False, "value": ""}
        return {"ok": True, "found": True, "value": row["value_text"]}

    def snapshot(self) -> dict[str, str]:
        rows = db.query(
            "SELECT key_name, value_text FROM kv_state WHERE node_id = %s",
            (self.node_id,))
        return {r["key_name"]: r["value_text"] for r in rows}

    def search(self, text: str, limit: int = 20) -> list[dict[str, Any]]:
        """Full-text search over stored values."""
        return db.query(
            "SELECT key_name, value_text, "
            "MATCH(value_text) AGAINST(%s IN NATURAL LANGUAGE MODE) AS relevance "
            "FROM kv_state WHERE node_id = %s "
            "AND MATCH(value_text) AGAINST(%s IN NATURAL LANGUAGE MODE) "
            "ORDER BY relevance DESC LIMIT %s",
            (text, self.node_id, text, limit))
