"""ops/alerts.py: alert-tagged log records reach Slack (rate-limited, off the
caller's thread); everything else is ignored; heartbeats ping only while
healthy."""
import asyncio
import logging

import httpx
import pytest

from ops.alerts import SlackAlertHandler, alert, heartbeat_loop


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _handler(posts, clock=None, status=200):
    def handle(request):
        posts.append(request.read().decode())
        return httpx.Response(status)
    client = httpx.Client(transport=httpx.MockTransport(handle))
    return SlackAlertHandler("https://hooks.slack.test/x", "executor", client=client,
                             clock=clock or Clock())


def _logger(handler, name="test.alerts"):
    lg = logging.getLogger(name)
    lg.handlers = [handler]
    lg.setLevel(logging.INFO)
    lg.propagate = False
    return lg


def test_only_alert_tagged_records_are_posted_with_source_and_text():
    posts = []
    h = _handler(posts)
    lg = _logger(h)
    lg.error("plain error, no alert")
    lg.error("trade %s HALTED", "t1", extra=alert("halt:t1", every=0))
    h.flush()
    assert len(posts) == 1
    assert "[executor]" in posts[0] and "trade t1 HALTED" in posts[0]


def test_a_repeated_key_is_rate_limited_and_every_zero_is_not():
    posts, clock = [], Clock()
    h = _handler(posts, clock)
    lg = _logger(h, "test.alerts.rl")
    for _ in range(3):
        lg.warning("BTC-USD bars held", extra=alert("hold:BTC-USD"))
    clock.t += 601
    lg.warning("BTC-USD bars held", extra=alert("hold:BTC-USD"))
    lg.info("opened a", extra=alert("open:a", every=0))
    lg.info("opened b", extra=alert("open:b", every=0))
    h.flush()
    assert sum("held" in p for p in posts) == 2          # first, then after 10 min
    assert sum("opened" in p for p in posts) == 2


def test_a_failing_webhook_never_raises_into_the_caller():
    posts = []
    h = _handler(posts, status=404)
    lg = _logger(h, "test.alerts.fail")
    lg.error("boom", extra=alert("k", every=0))           # must not raise
    h.flush()
    assert len(posts) == 1


@pytest.mark.asyncio
async def test_heartbeat_pings_only_while_healthy(monkeypatch):
    pings = []

    def handle(request):
        pings.append(str(request.url))
        return httpx.Response(200)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    state = {"ok": True}
    task = asyncio.create_task(heartbeat_loop("https://hc.test/p", 0.01,
                                              lambda: state["ok"], client=client))
    await asyncio.sleep(0.05)
    state["ok"] = False
    n = len(pings)
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert n >= 2 and len(pings) == n                     # stopped while unhealthy


def test_flush_under_the_handler_lock_does_not_deadlock_on_the_senders_own_log(monkeypatch):
    # logging.shutdown holds the handler lock while flushing; the delivery
    # thread logs its failed first attempt through the same handler (as the
    # root logger would route it). Without the filter that log blocks on the
    # lock until the flush times out and the alert is lost.
    import time
    posts, calls = [], {"n": 0}

    def handle(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("reset")
        posts.append(request.read().decode())
        return httpx.Response(200)
    client = httpx.Client(transport=httpx.MockTransport(handle))
    h = SlackAlertHandler("https://hooks.slack.test/x", "executor", client=client)
    own = logging.getLogger("ops.alerts")
    own.addHandler(h)
    try:
        lg = _logger(h, "test.alerts.lock")
        import ops.alerts as oa
        real_sleep = time.sleep
        # Skip the 2 s retry back-off; keep flush's short polling sleep.
        monkeypatch.setattr(oa.time, "sleep", lambda s: real_sleep(min(s, 0.01)))
        lg.error("HALT", extra=alert("halt:z", every=0))
        h.acquire()
        try:
            t0 = time.monotonic()
            h.flush(timeout=3.0)
            waited = time.monotonic() - t0
        finally:
            h.release()
    finally:
        own.removeHandler(h)
    assert posts and "HALT" in posts[0]
    assert waited < 2.0
