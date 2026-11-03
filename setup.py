#!/usr/bin/env python3
"""One-command setup: create the MySQL schema and the MongoDB collection.

    python setup.py                 # create everything
    python setup.py --check         # report what is reachable, change nothing
    python setup.py --mysql-only

Connection settings come from environment variables; defaults suit a
stock local MySQL or XAMPP:

    RAFTKV_DB_HOST  RAFTKV_DB_PORT  RAFTKV_DB_USER  RAFTKV_DB_PASSWORD
    RAFTKV_MONGO_URI
"""

from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(ROOT, "sql")

SQL_FILES = [
    ("01_schema.sql", "tables, keys and constraints"),
    ("02_views.sql", "views, CTEs and window functions"),
    ("03_triggers.sql", "triggers"),
    ("04_procedures.sql", "stored procedures and functions"),
    ("05_seed.sql", "cluster nodes, roles, permissions and users"),
]


def mysql_params() -> dict:
    params = {
        "host": os.environ.get("RAFTKV_DB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("RAFTKV_DB_PORT", "3306")),
        "user": os.environ.get("RAFTKV_DB_USER", "root"),
        "password": os.environ.get("RAFTKV_DB_PASSWORD", ""),
        "charset": "utf8mb4",
        "autocommit": True,
    }
    socket_path = os.environ.get("RAFTKV_DB_SOCKET")
    if socket_path:
        params["unix_socket"] = socket_path
    return params


def split_statements(sql: str) -> list[str]:
    """Split a script into statements, honouring DELIMITER.

    Triggers and procedures contain semicolons inside their bodies, so a
    naive split on ';' tears them in half. MySQL's own client solves
    this with DELIMITER; this reproduces that behaviour.
    """
    statements: list[str] = []
    buffer: list[str] = []
    delimiter = ";"

    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("DELIMITER"):
            parts = stripped.split()
            if len(parts) > 1:
                # Flush anything pending before switching delimiter.
                pending = "\n".join(buffer).strip()
                if pending:
                    statements.append(pending)
                buffer = []
                delimiter = parts[1]
            continue

        buffer.append(line)
        if stripped.endswith(delimiter):
            joined = "\n".join(buffer).strip()
            if joined.endswith(delimiter):
                joined = joined[: -len(delimiter)].strip()
            if joined:
                statements.append(joined)
            buffer = []

    tail = "\n".join(buffer).strip()
    if tail:
        statements.append(tail)

    return [s for s in statements if s and not _only_comments(s)]


def _only_comments(statement: str) -> bool:
    body = re.sub(r"--[^\n]*", "", statement)
    return not body.strip()


def setup_mysql(check_only: bool = False) -> bool:
    try:
        import pymysql
    except ImportError:
        print("  ERROR: pymysql is not installed. Run: pip install -r requirements.txt")
        return False

    params = mysql_params()
    where = params.get("unix_socket") or f"{params['host']}:{params['port']}"
    try:
        conn = pymysql.connect(**params)
    except Exception as exc:
        print(f"  ERROR: cannot reach MySQL at {where}")
        print(f"         {exc}")
        print("         Is the server running? For XAMPP, start MySQL in the"
              " control panel.")
        return False

    print(f"  connected to MySQL at {where}")

    if check_only:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s",
                (os.environ.get("RAFTKV_DB_NAME", "raftkv"),))
            tables = cur.fetchone()[0]
        print(f"  server version {version}")
        print(f"  schema 'raftkv' has {tables} tables")
        conn.close()
        return True

    for filename, description in SQL_FILES:
        path = os.path.join(SQL_DIR, filename)
        if not os.path.exists(path):
            print(f"  ERROR: missing {path}")
            conn.close()
            return False

        with open(path, encoding="utf-8") as fh:
            statements = split_statements(fh.read())

        with conn.cursor() as cur:
            for statement in statements:
                try:
                    cur.execute(statement)
                except Exception as exc:
                    head = " ".join(statement.split())[:90]
                    print(f"  ERROR in {filename}: {exc}")
                    print(f"         while running: {head}...")
                    conn.close()
                    return False
        print(f"  {filename:<20} {description} ({len(statements)} statements)")

    with conn.cursor() as cur:
        db_name = os.environ.get("RAFTKV_DB_NAME", "raftkv")
        # BASE TABLE, because information_schema.TABLES counts views too.
        cur.execute("SELECT COUNT(*) FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
                    (db_name,))
        tables = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM information_schema.VIEWS "
                    "WHERE TABLE_SCHEMA = %s", (db_name,))
        views = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = %s", (db_name,))
        triggers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM information_schema.ROUTINES "
                    "WHERE ROUTINE_SCHEMA = %s", (db_name,))
        routines = cur.fetchone()[0]

    print(f"  created {tables} tables, {views} views, {triggers} triggers, "
          f"{routines} procedures and functions")
    conn.close()
    return True


def setup_mongo(check_only: bool = False) -> bool:
    try:
        from pymongo import MongoClient
    except ImportError:
        print("  ERROR: pymongo is not installed. Run: pip install -r requirements.txt")
        return False

    uri = os.environ.get("RAFTKV_MONGO_URI", "mongodb://127.0.0.1:27017")
    db_name = os.environ.get("RAFTKV_MONGO_DB", "raftkv_events")

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2500)
        client.admin.command("ping")
    except Exception as exc:
        print(f"  WARNING: cannot reach MongoDB at {uri}")
        print(f"           {exc}")
        print("           The cluster still runs; the event log and vector")
        print("           search will be unavailable until MongoDB is up.")
        return False

    print(f"  connected to MongoDB at {uri}")

    if check_only:
        count = client[db_name]["cluster_events"].estimated_document_count()
        print(f"  database '{db_name}' holds {count} events")
        client.close()
        return True

    sys.path.insert(0, ROOT)
    from nosql.events import EventStore

    store = EventStore(client=client, db_name=db_name)
    created = store.create_indexes()
    print(f"  collection 'cluster_events' ready with {len(created)} indexes")
    client.close()
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up RaftKV databases")
    parser.add_argument("--check", action="store_true",
                        help="report status without changing anything")
    parser.add_argument("--mysql-only", action="store_true")
    parser.add_argument("--mongo-only", action="store_true")
    args = parser.parse_args()

    action = "Checking" if args.check else "Setting up"
    print(f"{action} RaftKV\n")

    mysql_ok = True
    mongo_ok = True

    if not args.mongo_only:
        print("MySQL")
        mysql_ok = setup_mysql(args.check)
        print()

    if not args.mysql_only:
        print("MongoDB")
        mongo_ok = setup_mongo(args.check)
        print()

    if mysql_ok and mongo_ok:
        print("Everything is ready.")
    elif mysql_ok:
        print("MySQL is ready. MongoDB is not - the cluster will run without"
              " the event log.")
    else:
        print("MySQL setup failed. Nothing else will work until it is fixed.")
        sys.exit(1)

    if not args.check:
        print("\nNext:")
        print("  python -m unittest discover -s tests    run the test suite")
        print("  python demo.py                          narrated demo")
        print("  python server.py                        start the cluster API")
        print("  cd dashboard && npm install && npm run dev")


if __name__ == "__main__":
    main()
