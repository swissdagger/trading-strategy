"""
Tests for feeds/bar_aggregator.py.

Two things must hold:

1. EQUIVALENCE: for any set of source bars, the streaming aggregator's
   emitted windows equal what the batch resampler
   (feeds.resample._resample_to_timeframe) produces for the closed
   windows — same OHLC, same window starts. The streaming path must not
   invent a second, subtly-different bucketing rule.

2. STREAMING SEMANTICS: close-on-later-window, one-open-window memory,
   trailing flush, out-of-order drop, and synthetic gap-fill matching
   count_synthetic_for_gap.

We reimplement the batch rule locally (identical arithmetic) as the oracle
rather than importing feeds.resample. The rule is four lines; duplicating it in the test as an independent
oracle is the point — if the aggregator and this oracle agree, the rule is
right.
"""
import random
from datetime import datetime, timezone

from feeds.bar_aggregator import BarAggregator, _StreamAggregator
from feeds.base import timeframe_to_ms


def datetime_utc(y, mo, d, h, mi):
    """Epoch ms for a UTC wall-clock time — readable session-boundary tests."""
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000


def batch_oracle(src_bars, timeframe):
    """Independent reimplementation of _resample_to_timeframe's rule.
    src_bars: list of (t, o, h, l, c). Returns {window_start: (o,h,l,c)}.
    """
    tf_ms = timeframe_to_ms(timeframe)
    by_window = {}
    first_t = {}
    last_t = {}
    for (t, o, h, l, c) in src_bars:
        ws = (t // tf_ms) * tf_ms
        if ws not in by_window:
            by_window[ws] = [o, h, l, c]
            first_t[ws] = t
            last_t[ws] = t
        else:
            b = by_window[ws]
            b[1] = max(b[1], h)
            b[2] = min(b[2], l)
            if t < first_t[ws]:
                b[0] = o
                first_t[ws] = t
            if t > last_t[ws]:
                b[3] = c
                last_t[ws] = t
    return {ws: tuple(v) for ws, v in by_window.items()}


def drain_streaming(src_bars, timeframe, asset_class="equity", fill_gaps=False):
    """Run all src_bars through a single-stream aggregator, then force a
    final flush far in the future so the last open window emits. Returns
    {window_start: (o,h,l,c)} for the real (non-synthetic) windows only.
    fill_gaps=False so this compares apples-to-apples with the oracle,
    which never fabricates.
    """
    agg = _StreamAggregator("SYM", timeframe, asset_class, fill_gaps=fill_gaps)
    emitted = []
    for (t, o, h, l, c) in src_bars:
        emitted.extend(agg.add(t, o, h, l, c))
    # Flush the trailing open window.
    far_future = (src_bars[-1][0] if src_bars else 0) + timeframe_to_ms(timeframe) * 10
    emitted.extend(agg.flush_elapsed(far_future))
    return {p.timestamp: (p.open, p.high, p.low, p.close) for p in emitted}


# --------------------------------------------------------------------
# 1. EQUIVALENCE with the batch rule
# --------------------------------------------------------------------

def test_matches_batch_single_window():
    # Three per-second bars all inside one 5s window.
    src = [
        (0, 100.0, 101.0, 99.0, 100.5),
        (1000, 100.5, 102.0, 100.0, 101.0),
        (2000, 101.0, 101.5, 98.0, 99.0),
    ]
    assert drain_streaming(src, "5s") == batch_oracle(src, "5s")


def test_matches_batch_multiple_windows():
    # Bars spanning several 5s windows.
    src = [
        (0, 10, 12, 9, 11),
        (2000, 11, 13, 10, 12),
        (5000, 12, 12, 11, 11),   # new 5s window
        (7000, 11, 15, 11, 14),
        (10000, 14, 14, 8, 9),    # third window
    ]
    assert drain_streaming(src, "5s") == batch_oracle(src, "5s")


def test_matches_batch_randomized():
    # Fuzz: random per-second bars over an hour, several timeframes.
    rng = random.Random(1234)
    for timeframe in ["5s", "10s", "30s", "1m", "2m"]:
        src = []
        price = 100.0
        # one bar per second for 600s, but randomly skip ~30% (no-trade secs)
        for sec in range(600):
            if rng.random() < 0.3:
                continue
            t = sec * 1000
            o = price
            h = price + rng.random() * 2
            l = price - rng.random() * 2
            c = l + rng.random() * (h - l)
            price = c
            src.append((t, o, h, l, c))
        assert drain_streaming(src, timeframe) == batch_oracle(src, timeframe), (
            f"mismatch at {timeframe}"
        )


# --------------------------------------------------------------------
# 2. STREAMING SEMANTICS
# --------------------------------------------------------------------

def test_emits_on_later_window_not_before():
    agg = _StreamAggregator("SYM", "5s", "equity", fill_gaps=False)
    # Two bars in window [0,5000): nothing emitted yet.
    assert agg.add(0, 10, 10, 10, 10) == []
    assert agg.add(2000, 10, 11, 9, 10) == []
    # A bar in the NEXT window closes the first.
    out = agg.add(5000, 10, 10, 10, 10)
    assert len(out) == 1
    assert out[0].timestamp == 0
    assert out[0].high == 11 and out[0].low == 9


def test_only_one_open_window_in_memory():
    # After processing many windows, exactly one stream window remains open.
    agg = _StreamAggregator("SYM", "5s", "equity", fill_gaps=False)
    for sec in range(0, 100, 1):
        agg.add(sec * 1000, 10, 10, 10, 10)
    # _open holds the current window only; nothing else is retained.
    assert agg._open is not None
    # Its start is the last window we touched.
    assert agg._open.window_start == (99 * 1000 // 5000) * 5000


def test_trailing_flush_idempotent():
    agg = _StreamAggregator("SYM", "5s", "equity", fill_gaps=False)
    agg.add(0, 10, 10, 10, 10)
    # Before the window ends, flush does nothing.
    assert agg.flush_elapsed(4999) == []
    # After it ends, flush emits once.
    out = agg.flush_elapsed(5000)
    assert len(out) == 1 and out[0].timestamp == 0
    # Second flush at the same/later time emits nothing (already cleared).
    assert agg.flush_elapsed(9999) == []


def test_out_of_order_bar_dropped():
    agg = _StreamAggregator("SYM", "5s", "equity", fill_gaps=False)
    agg.add(5000, 10, 10, 10, 10)   # open window [5000,10000)
    agg.add(7000, 10, 12, 10, 11)   # fold in
    # A late bar for the earlier window [0,5000) must not resurrect it.
    out = agg.add(1000, 99, 99, 99, 99)
    assert out == []
    # The open window is untouched by the stale bar.
    assert agg._open.window_start == 5000
    assert agg._open.high == 12


def test_gap_fill_flat_bars_crypto():
    # Crypto has no session boundaries, so count_synthetic_for_gap fills
    # every missing step. Two 1m bars 4 minutes apart -> 3 synthetic flats.
    agg = _StreamAggregator("BTCUSDT", "1m", "crypto", fill_gaps=True)
    agg.add(0, 100, 100, 100, 100)          # window [0, 60000)
    out = agg.add(240_000, 105, 105, 105, 105)  # window 4 min later
    # Emitted: the closed [0,60000) real bar, then synthetic 60k,120k,180k.
    timestamps = [p.timestamp for p in out]
    assert timestamps == [0, 60_000, 120_000, 180_000]
    # Synthetics are flat at the closed window's close (100).
    for p in out[1:]:
        assert p.open == p.high == p.low == p.close == 100


def test_no_gap_fill_when_disabled():
    agg = _StreamAggregator("BTCUSDT", "1m", "crypto", fill_gaps=False)
    agg.add(0, 100, 100, 100, 100)
    out = agg.add(240_000, 105, 105, 105, 105)
    assert [p.timestamp for p in out] == [0]   # only the real closed bar


def test_fx_intra_week_gap_is_filled():
    # fx's session is the WEEKLY window (base.py), so a gap that stays
    # inside the trading week IS filled with synthetic flats — the same
    # "quiet pair gets flat-fill" behaviour base.py documents for sparse
    # pairs like USDKRW. This defers entirely to count_synthetic_for_gap;
    # the aggregator adds no fx-specific logic of its own.
    agg = _StreamAggregator("EURUSD", "1m", "fx", fill_gaps=True)
    base = 1_700_000_000_000  # a Tuesday, mid-week, in-session
    w0 = (base // 60_000) * 60_000
    agg.add(base, 1.10, 1.10, 1.10, 1.10)
    out = agg.add(base + 240_000, 1.11, 1.11, 1.11, 1.11)
    # Real closed bar at w0, then 3 synthetic flats at w0+1m, +2m, +3m.
    assert [p.timestamp for p in out] == [w0, w0 + 60_000, w0 + 120_000, w0 + 180_000]
    for p in out[1:]:
        assert p.open == p.high == p.low == p.close == 1.10


def test_fx_weekend_gap_not_filled():
    # A gap that crosses the Fri-22:00 -> Sun-22:00 weekend boundary must
    # NOT be filled: session_bounds_for caps the fill at the session end,
    # so the inter-session (weekend) portion passes through unfilled. This
    # is the "genuine session gap must not be fabricated" invariant.
    agg = _StreamAggregator("EURUSD", "1m", "fx", fill_gaps=True)
    # Friday 21:58 UTC (in-session, near close). 2023-11-17 is a Friday.
    fri_close_area = int(datetime_utc(2023, 11, 17, 21, 58))
    # Monday 00:00 UTC (next session, well after Sunday 22:00 open).
    mon = int(datetime_utc(2023, 11, 20, 0, 0))
    w0 = (fri_close_area // 60_000) * 60_000
    agg.add(fri_close_area, 1.10, 1.10, 1.10, 1.10)
    out = agg.add(mon, 1.11, 1.11, 1.11, 1.11)
    # Fill runs only up to Fri 22:00 (session end): 21:58->21:59 is the one
    # step before the boundary. The weekend gap is NOT fabricated.
    assert out[0].timestamp == w0
    # No emitted synthetic bar may land at or after Fri 22:00.
    fri_2200 = int(datetime_utc(2023, 11, 17, 22, 0))
    assert all(p.timestamp < fri_2200 for p in out)


# --------------------------------------------------------------------
# 3. MULTI-TIMEFRAME FANOUT (BarAggregator)
# --------------------------------------------------------------------

def test_fanout_emits_per_timeframe():
    agg = BarAggregator("equity", ["5s", "10s"], fill_gaps=False)
    # Feed 12s of per-second bars for one symbol.
    for sec in range(13):
        agg.add_source_bar("AAPL", sec * 1000, 10, 10, 10, 10)
    # Force final flush.
    out = agg.flush_elapsed(20_000)
    # We should have closed 5s windows at 0,5000,10000 and 10s at 0,10000.
    # (exact set depends on flush; assert the windows are correctly labelled)
    per_tf = {}
    # Re-run cleanly to collect ALL emissions deterministically.
    agg2 = BarAggregator("equity", ["5s", "10s"], fill_gaps=False)
    collected = []
    for sec in range(13):
        collected.extend(agg2.add_source_bar("AAPL", sec * 1000, 10, 10, 10, 10))
    collected.extend(agg2.flush_elapsed(20_000))
    for p in collected:
        per_tf.setdefault(p.timeframe, []).append(p.timestamp)
    assert per_tf["5s"] == [0, 5000, 10000]
    assert per_tf["10s"] == [0, 10000]


def test_fanout_output_sorted():
    agg = BarAggregator("equity", ["5s", "10s"], fill_gaps=False)
    collected = []
    for sec in range(25):
        collected.extend(agg.add_source_bar("AAPL", sec * 1000, 10, 10, 10, 10))
    # Close-time order, the shorter rung first on a tie — within a call and,
    # for one symbol, across calls.
    from feeds.close_order import order_key
    keys = [order_key(p) for p in collected]
    assert keys == sorted(keys)
    # The 5s and 10s windows closing at 10 s: the 5s one first.
    tie = [p.timeframe for p in collected if order_key(p)[0] == 10_000]
    assert tie == ["5s", "10s"]


def test_symbols_isolated():
    agg = BarAggregator("equity", ["5s"], fill_gaps=False)
    agg.add_source_bar("AAPL", 0, 10, 10, 10, 10)
    agg.add_source_bar("MSFT", 0, 20, 20, 20, 20)
    out_aapl = agg.add_source_bar("AAPL", 5000, 10, 10, 10, 10)
    # Closing AAPL's window must not emit anything for MSFT.
    assert len(out_aapl) == 1
    assert out_aapl[0].symbol == "AAPL" and out_aapl[0].open == 10
