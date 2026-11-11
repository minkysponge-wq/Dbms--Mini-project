#!/usr/bin/env python3
"""Control API for the Raft cluster.

Runs the five-node cluster in this process and exposes it over HTTP so
the Next.js dashboard can show live state and act on it. Uses only the
standard library's http.server, so there is no web framework to install.

The dashboard reads MySQL and MongoDB directly for display; this server
handles the things that need the live cluster - writing a key through
consensus, crashing a node, healing a partition.

    python server.py                 # port 8080
    python server.py --port 9999

Endpoints
    GET  /api/health
    GET  /api/cluster
    GET  /api/kv?node=0
    POST /api/kv          {"key","value","op"}
    GET  /api/kv/get?key=x
    POST /api/node/kill   {"node_id"}
    POST /api/node/restart{"node_id"}
    POST /api/partition   {"minority":[1,3]}
    POST /api/heal
    GET  /api/events?limit=50
    GET  /api/events/search?q=text&mode=vector|text
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from raft import db
from raft.cluster import Cluster
from nosql.events import open_store

CLUSTER: Cluster | None = None
EVENTS: Any = None
STATE_LOCK = threading.Lock()

# Clerks must outlive a single request. A Clerk allocates one sequence
# number per logical request, and the servers use (client_id, seq_no) to
# recognise retries. Building a fresh Clerk per HTTP call restarts that
# counter at 1, so the second write from a client looks like a replay of
# the first and is correctly - and uselessly - deduplicated away.
_CLERKS: dict[str, Any] = {}


def get_clerk(client_id: str) -> Any:
    """The long-lived Clerk for this client id, created on first use."""
    with STATE_LOCK:
        clerk = _CLERKS.get(client_id)
        if clerk is None:
            clerk = CLUSTER.clerk(client_id)
            _CLERKS[client_id] = clerk
        return clerk


def json_default(obj: Any) -> str:
    return str(obj)


class Handler(BaseHTTPRequestHandler):
    server_version = "RaftKV/1.0"

    # -- plumbing -------------------------------------------------------

    def log_message(self, fmt: str, *args: Any) -> None:
        # Quieter than the default, which logs every request.
        if "--verbose" in sys.argv:
            super().log_message(fmt, *args)

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, default=json_default).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # The dashboard runs on a different port in development.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            return {}

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send(204, {})

    # -- routing --------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        route = parsed.path

        try:
            if route == "/api/health":
                return self._send(200, {
                    "ok": True,
                    "mysql": db.ping(),
                    "mongo": EVENTS is not None,
                    "cluster_size": CLUSTER.size if CLUSTER else 0,
                })

            if route == "/api/cluster":
                return self._send(200, self._cluster_payload())

            if route == "/api/kv":
                node_id = int(params.get("node", ["-1"])[0])
                if node_id < 0:
                    node_id = CLUSTER.leader() if CLUSTER else 0
                replica = CLUSTER.replica(node_id) if CLUSTER else None
                if replica is None:
                    return self._send(404, {"error": f"node {node_id} is offline"})
                return self._send(200, {
                    "node_id": node_id,
                    "pairs": replica.store.snapshot(),
                })

            if route == "/api/kv/get":
                key = params.get("key", [""])[0]
                if not key:
                    return self._send(400, {"error": "key is required"})
                clerk = get_clerk("dashboard")
                value = clerk.get(key, timeout=6)
                return self._send(200, {"key": key, "value": value,
                                        "found": bool(value)})

            if route == "/api/events":
                if EVENTS is None:
                    return self._send(503, {"error": "MongoDB is not connected"})
                limit = int(params.get("limit", ["50"])[0])
                node = params.get("node", [None])[0]
                etype = params.get("type", [None])[0]
                return self._send(200, {"events": EVENTS.find(
                    node_id=int(node) if node else None,
                    event_type=etype, limit=limit)})

            if route == "/api/events/search":
                if EVENTS is None:
                    return self._send(503, {"error": "MongoDB is not connected"})
                q = params.get("q", [""])[0]
                mode = params.get("mode", ["vector"])[0]
                if not q:
                    return self._send(400, {"error": "q is required"})
                results = (EVENTS.vector_search(q, limit=10) if mode == "vector"
                           else EVENTS.text_search(q, limit=10))
                return self._send(200, {"mode": mode, "query": q,
                                        "results": results})

            if route == "/api/events/summary":
                if EVENTS is None:
                    return self._send(503, {"error": "MongoDB is not connected"})
                return self._send(200, {"summary": EVENTS.summary_by_type()})

            return self._send(404, {"error": f"no route {route}"})

        except Exception as exc:
            return self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        route = parsed.path
        body = self._body()

        try:
            if route == "/api/kv":
                key = body.get("key", "")
                value = body.get("value", "")
                op = body.get("op", "PUT").upper()
                if not key:
                    return self._send(400, {"error": "key is required"})
                clerk = get_clerk(body.get("client_id") or "dashboard")
                if op == "PUT":
                    ok = clerk.put(key, value, timeout=8)
                elif op == "APPEND":
                    ok = clerk.append(key, value, timeout=8)
                elif op == "DELETE":
                    ok = clerk.delete(key, timeout=8)
                else:
                    return self._send(400, {"error": f"unknown op {op}"})
                return self._send(200 if ok else 503,
                                  {"ok": ok, "key": key, "op": op})

            if route in ("/api/node/kill", "/api/node/restart"):
                node_id = int(body.get("node_id", -1))
                if not 0 <= node_id < CLUSTER.size:
                    return self._send(400, {
                        "error": f"no node {node_id}; nodes are 0 to "
                                 f"{CLUSTER.size - 1}"})

            if route == "/api/node/kill":
                CLUSTER.crash(node_id)
                self._emit("crash", node_id,
                           f"node {node_id} was crashed from the dashboard")
                return self._send(200, {"ok": True, "node_id": node_id})

            if route == "/api/node/restart":
                CLUSTER.restart(node_id)
                self._emit("restart", node_id,
                           f"node {node_id} was restarted from the dashboard "
                           f"and recovered its state from MySQL")
                return self._send(200, {"ok": True, "node_id": node_id})

            if route == "/api/partition":
                minority = [int(n) for n in body.get("minority", [])]
                groups = {i: (1 if i in minority else 2)
                          for i in range(CLUSTER.size)}
                CLUSTER.net.set_partition(groups)
                majority = [i for i in range(CLUSTER.size) if i not in minority]
                self._emit("partition", minority[0] if minority else 0,
                           f"network partition isolated nodes {minority} "
                           f"from the majority {majority}",
                           minority=minority, majority=majority)
                return self._send(200, {"ok": True, "minority": minority})

            if route == "/api/heal":
                CLUSTER.net.heal()
                self._emit("heal", 0, "network partition healed; "
                                      "all nodes can communicate again")
                return self._send(200, {"ok": True})

            if route == "/api/events/annotate":
                if EVENTS is None:
                    return self._send(503, {"error": "MongoDB is not connected"})
                ok = EVENTS.annotate(body.get("event_id", ""),
                                     body.get("note", ""),
                                     body.get("author", "operator"))
                return self._send(200 if ok else 404, {"ok": ok})

            if route == "/api/events/delete":
                if EVENTS is None:
                    return self._send(503, {"error": "MongoDB is not connected"})
                ok = EVENTS.delete(body.get("event_id", ""))
                return self._send(200 if ok else 404, {"ok": ok})

            return self._send(404, {"error": f"no route {route}"})

        except Exception as exc:
            return self._send(500, {"error": str(exc)})

    # -- helpers --------------------------------------------------------

    def _emit(self, event_type: str, node_id: int, description: str,
              **extra: Any) -> None:
        if EVENTS is None:
            return
        try:
            EVENTS.record({"node_id": node_id, "event_type": event_type,
                           "description": description, **extra})
        except Exception:
            pass

    def _cluster_payload(self) -> dict[str, Any]:
        status = CLUSTER.status() if CLUSTER else []
        leader = CLUSTER.leader() if CLUSTER else -1
        sent, lost = CLUSTER.net.stats() if CLUSTER else (0, 0)
        try:
            quorum = db.query(
                "SELECT log_index, replica_count, majority_needed, "
                "is_majority_replicated FROM v_commit_quorum "
                "ORDER BY log_index DESC LIMIT 10")
        except Exception:
            quorum = []
        return {
            "nodes": status,
            "leader": leader,
            "majority": (CLUSTER.size // 2 + 1) if CLUSTER else 0,
            "messages_sent": sent,
            "messages_lost": lost,
            "quorum": quorum,
        }


def main() -> None:
    global CLUSTER, EVENTS

    parser = argparse.ArgumentParser(description="Raft cluster control API")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--no-reset", action="store_true",
                        help="keep existing cluster state in MySQL")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not db.ping():
        print("ERROR: MySQL is not reachable or the schema is missing.")
        print("Run:  python setup.py")
        sys.exit(1)

    EVENTS, message = open_store(fallback_to_mock=True)
    print(f"events: {message}")

    sink = (lambda event: EVENTS.record(event)) if EVENTS else None
    CLUSTER = Cluster(size=args.nodes, seed=args.seed, event_sink=sink,
                      reset=not args.no_reset)

    print(f"starting {args.nodes}-node cluster, waiting for a leader...")
    leader = CLUSTER.wait_for_leader(25)
    print(f"leader: node {leader}" if leader >= 0 else "no leader yet")

    httpd = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"control API listening on http://127.0.0.1:{args.port}")
    print("press Ctrl+C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        CLUSTER.shutdown()
        if EVENTS:
            EVENTS.close()


if __name__ == "__main__":
    main()
