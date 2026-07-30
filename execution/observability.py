"""
execution/observability.py — a minimal, dependency-free health + metrics surface.

WHY THIS EXISTS (and why it's deliberately small). For a paper shakeout the
operator needs to answer three questions at a glance: is it alive, is it stuck,
did that exit escalate? Before this, the only window into the executor was log
tailing. This gives a scrapeable surface for those three questions and nothing
more — no Prometheus client, no web framework, no new runtime dependency. The
codebase pulls in asyncpg/aiokafka/httpx/websockets and nothing for metrics, so
introducing a metrics framework here would be scope the shakeout doesn't need.
If you later want Prometheus, Metrics.snapshot() is a plain dict that serializes
straight into any exposition format — swap the surface, keep the counters.

TWO PIECES, INTENTIONALLY SPLIT:

  * Metrics — a pure in-memory counter/gauge store. No I/O, no sockets. The
    executor holds one and increments it inline (a no-op if it holds None), so
    the hot paths stay synchronous and the executor is testable without a
    server. Counters only grow; gauges are set to the current value.

  * HealthServer — a stdlib http.server run on a daemon thread that serves a
    JSON snapshot at /health (liveness + readiness verdict) and /metrics (raw
    counters/gauges). It reads a snapshot callback; it never touches executor
    internals directly. Threaded rather than async so a wedged event loop — the
    exact failure you want to detect — can't also wedge the health check.

LIVENESS vs READINESS. /health returns 200 only when the executor reports booted
AND the escalation loop's last tick is recent (within a staleness bound). A
wedged loop (the loop that advances unfilled exits) is precisely the silent
failure a shakeout must catch, so a stale tick flips the verdict to 503 — a
health checker or a human watching curl sees it immediately instead of noticing
hours later that no exit escalated.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Optional

log = logging.getLogger("executor.observability")

# A tick older than this means the escalation loop is not running as expected.
# Sized generously above the default 1s tick so an occasional slow pass doesn't
# flap the verdict; tune per deployment if the tick interval changes.
DEFAULT_TICK_STALENESS_SECONDS = 10.0


class Metrics:
    """In-memory counters and gauges. Thread-safe so the HTTP thread can read a
    snapshot while the event-loop thread increments — the increments themselves
    are called from async code on one loop, but snapshot() crosses threads.

    Counters are monotonic (increment only); gauges hold a current value. Keep
    names stable — they're the operator's vocabulary during the shakeout."""

    def __init__(self, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self.started_at = now()

    def incr(self, name: str, by: int = 1) -> None:
        """Add to a counter (created at zero on first use)."""
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + by

    def gauge(self, name: str, value: float) -> None:
        """Set a gauge to the current value."""
        with self._lock:
            self._gauges[name] = value

    def snapshot(self) -> dict:
        """A consistent point-in-time copy for serving. Copies under the lock so
        a reader never sees a half-updated map."""
        with self._lock:
            return {
                "uptime_seconds": round(self._now() - self.started_at, 3),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }


class HealthServer:
    """Serves /health and /metrics as JSON on a daemon thread.

    `snapshot_fn` returns the health document (the executor supplies it, folding
    in its own liveness view). The server is dumb: it renders whatever the
    callback returns and maps the document's `healthy` flag to 200 vs 503. Any
    exception in the callback becomes a 500 with the error text, so a broken
    snapshot surfaces as an unhealthy check rather than a silent hang."""

    def __init__(self, snapshot_fn: Callable[[], dict],
                 metrics: "Metrics", *, host: str = "0.0.0.0",
                 port: int = 8080,
                 extra_routes: Optional[Dict[str, Callable[[], dict]]] = None) -> None:
        self._snapshot_fn = snapshot_fn
        self._metrics = metrics
        # Optional path -> callback map, e.g. {"/watermarks": registry.snapshot}.
        # Each callback must return a JSON-serialisable dict and must NOT touch
        # asyncio: it runs on the HTTP thread, deliberately isolated from the
        # event loop so a wedged loop can still be reported. Always 200 — these
        # are informational surfaces; only /health carries a verdict.
        self._extra_routes = dict(extra_routes or {})
        self._host = host
        self._port = port
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Bind and serve on a background daemon thread. Returns immediately."""
        handler = self._make_handler()
        self._httpd = ThreadingHTTPServer((self._host, self._port), handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="health-server", daemon=True)
        self._thread.start()
        log.info("health server listening on %s:%d (%s)", self._host, self._port,
                 ", ".join(["/health", "/metrics", *sorted(self._extra_routes)]))

    def stop(self) -> None:
        """Stop serving and join the thread. Idempotent."""
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        log.info("health server stopped")

    def _make_handler(self):
        snapshot_fn = self._snapshot_fn
        metrics = self._metrics
        extra_routes = self._extra_routes

        class _Handler(BaseHTTPRequestHandler):
            # Silence the default per-request stderr logging; the app logs what
            # it cares about, and request spam would drown the shakeout logs.
            def log_message(self, *args) -> None:  # noqa: D401
                return

            def _write(self, code: int, body: dict) -> None:
                payload = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
                if self.path.rstrip("/") in ("/health", ""):
                    try:
                        doc = snapshot_fn()
                    except Exception as e:  # broken snapshot => unhealthy, not a hang
                        self._write(500, {"healthy": False, "error": repr(e)})
                        return
                    self._write(200 if doc.get("healthy") else 503, doc)
                elif self.path.rstrip("/") == "/metrics":
                    self._write(200, metrics.snapshot())
                elif self.path.rstrip("/") in extra_routes:
                    try:
                        self._write(200, extra_routes[self.path.rstrip("/")]())
                    except Exception as e:
                        self._write(500, {"error": repr(e)})
                else:
                    self._write(404, {"error": "not found",
                                      "paths": ["/health", "/metrics",
                                                *sorted(extra_routes)]})

        return _Handler


def build_health_document(*, booted: bool, last_tick_at: Optional[float],
                          open_trades: int, closing_trades: int,
                          halted_trades: int, metrics: Metrics,
                          now: Callable[[], float] = time.time,
                          tick_staleness_seconds: float = DEFAULT_TICK_STALENESS_SECONDS
                          ) -> dict:
    """Assemble the /health document from the executor's live view.

    Healthy iff: booted AND the escalation loop has ticked within the staleness
    bound. A None last_tick means the loop hasn't ticked yet — unhealthy until it
    does, which correctly reports 'not ready' during the gap between start() and
    the loop's first pass. Pure and side-effect-free so it's unit-testable with a
    fake clock; the HTTP layer just serves what this returns."""
    tick_age = None if last_tick_at is None else round(now() - last_tick_at, 3)
    tick_fresh = last_tick_at is not None and tick_age <= tick_staleness_seconds
    healthy = booted and tick_fresh

    reasons = []
    if not booted:
        reasons.append("not booted")
    if last_tick_at is None:
        reasons.append("escalation loop has not ticked yet")
    elif not tick_fresh:
        reasons.append(f"escalation tick stale ({tick_age}s > "
                       f"{tick_staleness_seconds}s)")

    return {
        "healthy": healthy,
        "booted": booted,
        "escalation_tick_age_seconds": tick_age,
        "open_trades": open_trades,
        "closing_trades": closing_trades,
        "halted_trades": halted_trades,
        "reasons": reasons,   # empty when healthy
        "metrics": metrics.snapshot(),
    }