"""In-process message transport with fault injection.

Stands in for real sockets so the whole cluster runs in one process and
tests can inject faults deterministically: dropped messages, latency,
network partitions and node crashes.

Every message is serialised with ``json`` on the way out and parsed on
the way in, so it crosses a real encoding boundary. A bug where two
nodes accidentally share a mutable object surfaces here exactly as it
would over a network.
"""

from __future__ import annotations

import json
import random
import threading
import time
from typing import Any, Callable


class Network:
    """Routes RPCs between nodes and injects faults.

    Seeded, so a run can be replayed. ``seed`` fixes the sequence of
    drops and delays; given the same seed and the same calls, the same
    messages are lost.
    """

    def __init__(self, seed: int = 0) -> None:
        self._lock = threading.RLock()
        self._handlers: dict[int, Callable[[str, dict], dict | None]] = {}
        self._alive: dict[int, bool] = {}
        self._partition: dict[int, int] = {}
        self._drop_rate = 0.0
        self._max_delay = 0.004          # seconds
        self._long_delays = False
        self._rng = random.Random(seed)
        self._rng_lock = threading.Lock()
        self.sent = 0
        self.lost = 0

    # -- registration ---------------------------------------------------

    def register(self, node_id: int, handler: Callable[[str, dict], dict | None]) -> None:
        with self._lock:
            self._handlers[node_id] = handler
            self._alive[node_id] = True
            self._partition[node_id] = 0

    def unregister(self, node_id: int) -> None:
        with self._lock:
            self._handlers.pop(node_id, None)

    # -- fault injection ------------------------------------------------

    def set_alive(self, node_id: int, up: bool) -> None:
        with self._lock:
            self._alive[node_id] = up

    def is_alive(self, node_id: int) -> bool:
        with self._lock:
            return self._alive.get(node_id, False)

    def set_partition(self, groups: dict[int, int]) -> None:
        """Assign nodes to groups. Only same-group nodes can talk."""
        with self._lock:
            self._partition.update(groups)

    def heal(self) -> None:
        with self._lock:
            for node_id in self._partition:
                self._partition[node_id] = 0

    def set_drop_rate(self, rate: float) -> None:
        with self._lock:
            self._drop_rate = rate

    def set_max_delay(self, seconds: float) -> None:
        with self._lock:
            self._max_delay = seconds

    def set_long_delays(self, on: bool) -> None:
        with self._lock:
            self._long_delays = on

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return self.sent, self.lost

    # -- internals ------------------------------------------------------

    def _rand(self) -> float:
        with self._rng_lock:
            return self._rng.random()

    # -- the call path --------------------------------------------------

    def call(self, src: int, dst: int, method: str, args: dict) -> dict | None:
        """Deliver an RPC. Returns None when the message was lost.

        None means "no information", exactly as a timed-out socket does.
        Callers must never treat it as a negative reply.
        """
        with self._lock:
            handler = self._handlers.get(dst)
            src_up = self._alive.get(src, False)
            dst_up = self._alive.get(dst, False)
            same_side = self._partition.get(src) == self._partition.get(dst)
            drop_rate = self._drop_rate
            max_delay = self._max_delay
            long_delays = self._long_delays
            self.sent += 1

        def lost() -> None:
            with self._lock:
                self.lost += 1
            return None

        if handler is None or not src_up or not dst_up or not same_side:
            return lost()
        if drop_rate > 0 and self._rand() < drop_rate:
            return lost()

        if max_delay > 0:
            time.sleep(self._rand() * max_delay)
        if long_delays and self._rand() < 0.05:
            time.sleep(self._rand() * 0.25)

        # Round-trip through JSON so nothing mutable is shared between
        # the caller and the handler.
        try:
            wire_args = json.loads(json.dumps(args))
        except (TypeError, ValueError):
            return lost()

        try:
            reply = handler(method, wire_args)
        except Exception:
            return lost()
        if reply is None:
            return lost()

        # The peer may have crashed or been partitioned while we were in
        # its handler; a real network would simply never deliver this.
        with self._lock:
            still_up = (
                self._alive.get(src, False)
                and self._alive.get(dst, False)
                and self._partition.get(src) == self._partition.get(dst)
            )
        if not still_up:
            return lost()

        if max_delay > 0:
            time.sleep(self._rand() * max_delay)
        if drop_rate > 0 and self._rand() < drop_rate:
            return lost()

        try:
            return json.loads(json.dumps(reply))
        except (TypeError, ValueError):
            return lost()
