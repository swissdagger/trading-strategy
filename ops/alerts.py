"""
ops/alerts.py — Slack alerts and dead-man's-switch heartbeats.

ALERTS ARE LOG RECORDS. Code raises an alert by logging with an `alert` key:

    log.error("trade %s HALTED ...", tid, extra=alert("halt:" + tid, every=0))

Every process that calls install_alerting() forwards such records to Slack
(an incoming webhook, SLACK_WEBHOOK_URL); everything else in the log is
untouched. One mechanism, no new plumbing through the call sites, and the
alert text is the log line, so the log and Slack never disagree. Without
SLACK_WEBHOOK_URL nothing is sent and the log is unchanged.

DELIVERY NEVER BLOCKS TRADING. emit() only enqueues; a daemon thread posts
(sync httpx, 10 s timeout, one retry on 429/5xx honouring Retry-After). It
works with no event loop at all, so a refused boot (after asyncio.run has
returned) still alerts. close() — run by logging.shutdown at exit — drains
the queue for up to 5 s.

RATE LIMIT. One alert per `key` per `every` seconds (default 600): a stall
logged every minute alerts once per ten. `every=0` is for one-off events
(a trade opened, a HALT) whose key is already unique.

HEARTBEATS. A dead process cannot alert. Each long-running process pings an
external dead-man's switch (healthchecks.io or any URL that alerts when pings
stop; it can post to the same Slack channel) every HEARTBEAT_INTERVAL_S,
only while its `healthy()` check passes — so a wedged event loop or a stalled
pipeline stops the pings as surely as a crash.
"""
from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
import time
from typing import Callable, Dict, Optional

import httpx

log = logging.getLogger("ops.alerts")

DEFAULT_EVERY_S = 600.0
LEVEL_PREFIX = {logging.CRITICAL: ":rotating_light:", logging.ERROR: ":red_circle:",
                logging.WARNING: ":warning:", logging.INFO: ":large_blue_circle:"}


def alert(key: str, every: Optional[float] = None) -> dict:
    """The `extra=` for a log call that should also go to Slack."""
    return {"alert": key, "alert_every": DEFAULT_EVERY_S if every is None else every}


class SlackAlertHandler(logging.Handler):
    def __init__(self, webhook_url: str, source: str, *,
                 client: Optional[httpx.Client] = None, clock=time.monotonic):
        super().__init__(level=logging.INFO)
        # Filters run BEFORE Handler.handle takes the handler lock, so ordinary
        # records never touch it. That matters: logging.shutdown holds the lock
        # while flush() waits for the delivery thread, and the delivery thread
        # logs its own failures — through this handler, had it no filter.
        self.addFilter(lambda record: bool(getattr(record, "alert", None)))
        self._url = webhook_url
        self._source = source
        self._client = client or httpx.Client(timeout=10.0)
        self._clock = clock
        self._last: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._queue: "queue.Queue[Optional[str]]" = queue.Queue(maxsize=1000)
        self._thread = threading.Thread(target=self._run, name="slack-alerts", daemon=True)
        self._thread.start()

    # ---- logging side (any thread, never blocks) -------------------------
    def emit(self, record: logging.LogRecord) -> None:
        key = getattr(record, "alert", None)
        if not key:
            return
        every = float(getattr(record, "alert_every", DEFAULT_EVERY_S))
        now = self._clock()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < every:
                return
            self._last[key] = now
        try:
            text = f"{LEVEL_PREFIX.get(record.levelno, '')} *[{self._source}]* {record.getMessage()}"
        except Exception:
            self.handleError(record)
            return
        try:
            self._queue.put_nowait(text)
        except queue.Full:
            pass   # Slack down for a long time: drop rather than grow without bound

    # ---- delivery thread -------------------------------------------------
    def _run(self) -> None:
        while True:
            text = self._queue.get()
            try:
                if text is None:
                    return
                self._post(text)
            finally:
                self._queue.task_done()

    def _post(self, text: str) -> None:
        for attempt in range(2):
            try:
                resp = self._client.post(self._url, json={"text": text})
                if resp.status_code < 300:
                    return
                if resp.status_code == 429 or resp.status_code >= 500:
                    time.sleep(min(float(resp.headers.get("Retry-After", 2)), 30.0))
                    continue
                # Not the logger this module writes through for alerts: a
                # failure here must never re-enter the handler.
                log.warning("slack webhook HTTP %d: %s", resp.status_code, resp.text[:200])
                return
            except Exception as e:
                log.warning("slack webhook post failed (attempt %d): %r", attempt + 1, e)
                time.sleep(2)

    def flush(self, timeout: float = 5.0) -> None:
        """Wait (bounded) until everything queued has been posted."""
        deadline = time.monotonic() + timeout
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.05)

    def close(self) -> None:
        self.flush()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        super().close()


def install_alerting(source: str) -> Optional[SlackAlertHandler]:
    """Forward alert-tagged log records from this process to Slack, if
    SLACK_WEBHOOK_URL is set. Call once, after logging is configured."""
    url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        log.info("SLACK_WEBHOOK_URL not set: alerts go to the log only")
        return None
    handler = SlackAlertHandler(url, source)
    root = logging.getLogger()
    root.addHandler(handler)
    # Alert records are logged at INFO and above; make sure the root logger
    # lets INFO through even if LOG_LEVEL is stricter (the handler filters).
    if root.level > logging.INFO:
        for h in root.handlers:
            if h is not handler and h.level == logging.NOTSET:
                h.setLevel(root.level)
        root.setLevel(logging.INFO)
    # Children with their own stricter level (e.g. a chatty library set to
    # WARNING) keep it; alert call sites log at INFO+ through repo loggers.
    log.info("alerts: forwarding to Slack as %r", source)
    return handler


async def heartbeat_loop(url: str, interval_s: float, healthy: Callable[[], bool],
                         *, client: Optional[httpx.AsyncClient] = None) -> None:
    """Ping `url` every interval_s while healthy() is true. Runs on the event
    loop on purpose: a wedged loop must stop the pings. Never raises."""
    own = client is None
    client = client or httpx.AsyncClient(timeout=10.0)
    try:
        while True:
            try:
                if healthy():
                    await client.get(url)
                else:
                    log.warning("heartbeat withheld: health check failing")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("heartbeat ping failed: %r", e)
            await asyncio.sleep(interval_s)
    finally:
        if own:
            await client.aclose()


def start_heartbeat(env_var: str, healthy: Callable[[], bool]) -> Optional[asyncio.Task]:
    """Start the heartbeat task if env_var (a ping URL) is set."""
    url = os.environ.get(env_var, "").strip()
    if not url:
        log.info("%s not set: no heartbeat", env_var)
        return None
    interval = float(os.environ.get("HEARTBEAT_INTERVAL_S", "60"))
    log.info("heartbeat every %.0fs (%s)", interval, env_var)
    return asyncio.create_task(heartbeat_loop(url, interval, healthy), name="heartbeat")
