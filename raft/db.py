"""Database access for the Raft cluster.

Every node persists its protocol state to MySQL. Connections are held
per-thread because a PyMySQL connection is not safe to share between
threads, and each Raft node runs several threads (a ticker, an applier,
and one per outbound RPC).

Configuration comes from environment variables so the same code runs
against a local MySQL, XAMPP's MariaDB, or a container:

    RAFTKV_DB_HOST      default 127.0.0.1
    RAFTKV_DB_PORT      default 3306
    RAFTKV_DB_USER      default root
    RAFTKV_DB_PASSWORD  default empty
    RAFTKV_DB_NAME      default raftkv
    RAFTKV_DB_SOCKET    optional unix socket path
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Any, Iterable

import pymysql
from pymysql.cursors import DictCursor


def db_config() -> dict[str, Any]:
    """Connection parameters, read fresh so tests can override them."""
    cfg: dict[str, Any] = {
        "host": os.environ.get("RAFTKV_DB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("RAFTKV_DB_PORT", "3306")),
        "user": os.environ.get("RAFTKV_DB_USER", "root"),
        "password": os.environ.get("RAFTKV_DB_PASSWORD", ""),
        "database": os.environ.get("RAFTKV_DB_NAME", "raftkv"),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": True,
    }
    socket_path = os.environ.get("RAFTKV_DB_SOCKET")
    if socket_path:
        cfg["unix_socket"] = socket_path
    return cfg


_local = threading.local()


def connection() -> pymysql.connections.Connection:
    """The calling thread's connection, opened on first use.

    ``ping(reconnect=True)`` covers the case where MySQL has dropped an
    idle connection, which happens readily during a long chaos run.
    """
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = pymysql.connect(**db_config())
        _local.conn = conn
        return conn

    # Verify the connection is still usable. PyMySQL deprecated
    # ping(reconnect=True), so reconnect explicitly instead.
    try:
        conn.ping(False)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        conn = pymysql.connect(**db_config())
        _local.conn = conn
    return conn


def close_connection() -> None:
    """Close this thread's connection, if it has one."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


def query(sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    """Run a SELECT and return every row."""
    with connection().cursor() as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def query_one(sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
    """Run a SELECT and return the first row, or None."""
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: Iterable[Any] = ()) -> int:
    """Run an INSERT, UPDATE or DELETE and return the affected row count."""
    with connection().cursor() as cur:
        cur.execute(sql, tuple(params))
        return cur.rowcount


def executemany(sql: str, rows: list[tuple]) -> int:
    """Run one statement against many parameter sets."""
    if not rows:
        return 0
    with connection().cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount


def call_proc(name: str, args: tuple = ()) -> tuple:
    """Call a stored procedure and return its OUT parameters."""
    conn = connection()
    with conn.cursor() as cur:
        cur.callproc(name, args)
        placeholders = ", ".join(f"@_{name}_{i}" for i in range(len(args)))
        if not placeholders:
            return ()
        cur.execute(f"SELECT {placeholders}")
        row = cur.fetchone()
        return tuple(row.values()) if row else ()


@contextmanager
def transaction():
    """Run a block inside an explicit transaction.

    Commits on success, rolls back on any exception. Autocommit is
    disabled for the duration and restored afterwards, so callers that do
    not need a transaction are unaffected.
    """
    conn = connection()
    conn.autocommit(False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit(True)


def reset_cluster_state(node_count: int = 5) -> None:
    """Clear all protocol state and re-register the nodes.

    Access-control data is deliberately left alone: users and roles are
    reference data, not cluster state.
    """
    with connection().cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in (
            "node_event", "client_request", "client_session", "client",
            "kv_state", "replication_state", "snapshot", "log_entry",
            "vote", "term", "node",
        ):
            cur.execute(f"TRUNCATE TABLE {table}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")

        cur.executemany(
            "INSERT INTO node (node_id, host_address, port, role, "
            "current_term, commit_index, last_applied, is_alive) "
            "VALUES (%s, '127.0.0.1', %s, 'follower', 0, 0, 0, TRUE)",
            [(i, 9000 + i) for i in range(node_count)],
        )
        cur.execute("INSERT INTO term (term_number, leader_id) VALUES (0, NULL)")


def ping() -> bool:
    """True when the database is reachable and the schema is present."""
    try:
        row = query_one(
            "SELECT COUNT(*) AS c FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'node'",
            (db_config()["database"],),
        )
        return bool(row and row["c"] == 1)
    except Exception:
        return False
