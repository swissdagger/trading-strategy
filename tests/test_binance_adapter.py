"""
Tests for execution/binance_adapter.py — the pure/mapping logic and the signed
request construction, without hitting Binance (httpx is mocked where a call is
unavoidable).
"""
import hashlib
import hmac
from decimal import Decimal
from urllib.parse import urlencode, parse_qs

import httpx
import pytest

from execution.binance_adapter import (
    BinanceAdapter, _is_buy, _fmt, _to_status, _MAX_CLIENT_ID_LEN,
)
from execution.types import (
    Fill, OrderIntent, OrderRequest, OrderStatus, TradeSide,
)
from execution import client_order_id as coid


def _adapter(**kw):
    return BinanceAdapter(api_key="k", api_secret="s", **kw)


def _order(intent=OrderIntent.ENTRY, side=TradeSide.LONG, symbol="BTCUSDT"):
    tid = "11111111111111111111111111111111"
    return OrderRequest(
        client_order_id=coid.build(tid, intent, 0),
        trade_id=tid, symbol=symbol, asset_class="crypto",
        intent=intent, side=side,
        qty=Decimal("0.01"), limit_price=Decimal("50000.5"),
    )


# --------------------------------------------------------------------
# Signing
# --------------------------------------------------------------------

def test_sign_matches_hmac_sha256():
    a = _adapter()
    params = {"symbol": "BTCUSDT", "side": "BUY", "timestamp": 1}
    sig = a._sign(params)
    expected = hmac.new(b"s", urlencode(params).encode(), hashlib.sha256).hexdigest()
    assert sig == expected


# --------------------------------------------------------------------
# Client-id compression (the venue quirk)
# --------------------------------------------------------------------

def test_venue_id_within_binance_limit():
    a = _adapter()
    canonical = coid.build("22222222222222222222222222222222", OrderIntent.EXIT, 3)
    vid = a._venue_client_id(canonical)
    assert len(vid) <= _MAX_CLIENT_ID_LEN
    # Binance charset: alnum, dash, underscore, dot, colon, slash. Ours is
    # prefix 'q' + hex, so trivially within it.
    assert all(c.isalnum() for c in vid)


def test_venue_id_deterministic():
    a = _adapter()
    canonical = coid.build("33333333333333333333333333333333", OrderIntent.ENTRY, 0)
    v1 = a._venue_client_id(canonical)
    v2 = a._venue_client_id(canonical)
    assert v1 == v2   # same canonical -> same venue id (dedupe still works)


def test_venue_id_reverses():
    a = _adapter()
    canonical = coid.build("44444444444444444444444444444444", OrderIntent.EXIT, 1)
    vid = a._venue_client_id(canonical)
    assert a._canonical_from_venue_id(vid) == canonical


def test_venue_id_distinct_per_canonical():
    a = _adapter()
    c1 = coid.build("55555555555555555555555555555555", OrderIntent.ENTRY, 0)
    c2 = coid.build("66666666666666666666666666666666", OrderIntent.ENTRY, 0)
    assert a._venue_client_id(c1) != a._venue_client_id(c2)


def test_foreign_venue_id_not_ours():
    a = _adapter()
    assert a._canonical_from_venue_id("someManualOrderId") is None


# --------------------------------------------------------------------
# Order direction / formatting / status
# --------------------------------------------------------------------

def test_order_direction():
    assert _is_buy(_order(OrderIntent.ENTRY, TradeSide.LONG)) is True
    assert _is_buy(_order(OrderIntent.ENTRY, TradeSide.SHORT)) is False
    assert _is_buy(_order(OrderIntent.EXIT, TradeSide.LONG)) is False   # sell to close long
    assert _is_buy(_order(OrderIntent.EXIT, TradeSide.SHORT)) is True   # buy to close short


def test_fmt_no_scientific_notation():
    assert _fmt(Decimal("0.00010000")) == "0.0001"
    assert _fmt(Decimal("50000.50")) == "50000.5"
    assert "E" not in _fmt(Decimal("0.00000001"))


def test_status_mapping():
    assert _to_status("FILLED") == OrderStatus.FILLED
    assert _to_status("PARTIALLY_FILLED") == OrderStatus.PARTIALLY_FILLED
    assert _to_status("NEW") == OrderStatus.WORKING
    assert _to_status("CANCELED") == OrderStatus.CANCELLED
    assert _to_status("REJECTED") == OrderStatus.REJECTED
    assert _to_status("SOMETHING_NEW") == OrderStatus.WORKING   # never guess terminal


# --------------------------------------------------------------------
# submit_order — success and reject, via a mocked transport
# --------------------------------------------------------------------

class MockTransport(httpx.AsyncBaseTransport):
    def __init__(self, handler):
        self._handler = handler
    async def handle_async_request(self, request):
        return self._handler(request)


def _mock_adapter(handler):
    a = _adapter()
    a._client = httpx.AsyncClient(transport=MockTransport(handler), timeout=5.0)
    return a


@pytest.mark.asyncio
async def test_submit_order_filled():
    def handler(request):
        assert request.method == "POST"
        assert "/fapi/v1/order" in str(request.url)
        q = parse_qs(request.url.query.decode())
        # It sent a signed limit order with our compressed client id.
        assert q["type"] == ["LIMIT"]
        assert q["side"] == ["BUY"]
        assert "signature" in q
        assert q["newClientOrderId"][0].startswith("q")
        return httpx.Response(200, json={
            "orderId": 123, "status": "FILLED", "executedQty": "0.01",
            "updateTime": 1000,
            "fills": [{"tradeId": 9, "qty": "0.01", "price": "50000.5",
                       "commission": "0.02", "commissionAsset": "USDT"}],
        })
    a = _mock_adapter(handler)
    res = await a.submit_order(_order())
    assert res.status == OrderStatus.FILLED
    assert res.filled_qty == Decimal("0.01")
    assert res.venue_order_id == "123"
    assert len(res.fills) == 1
    assert res.fills[0].venue_exec_id == "9"
    await a.aclose()


@pytest.mark.asyncio
async def test_submit_order_rejected():
    def handler(request):
        return httpx.Response(400, json={"code": -2019, "msg": "Margin is insufficient."})
    a = _mock_adapter(handler)
    res = await a.submit_order(_order())
    assert res.status == OrderStatus.REJECTED
    assert "insufficient" in res.reject_reason.lower()
    await a.aclose()


@pytest.mark.asyncio
async def test_connect_confirms_account():
    calls = []
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, json=[{"asset": "USDT", "balance": "1000"}])
    a = _mock_adapter(handler)
    await a.connect()   # should not raise
    assert any("/fapi/v2/balance" in c for c in calls)
    await a.aclose()


@pytest.mark.asyncio
async def test_connect_raises_on_auth_failure():
    def handler(request):
        return httpx.Response(401, json={"code": -2015, "msg": "Invalid API-key."})
    a = _mock_adapter(handler)
    with pytest.raises(ConnectionError):
        await a.connect()
    await a.aclose()


@pytest.mark.asyncio
async def test_get_positions_signed_and_filtered():
    def handler(request):
        assert "/fapi/v3/positionRisk" in str(request.url)
        return httpx.Response(200, json=[
            {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "49000"},
            {"symbol": "ETHUSDT", "positionAmt": "0", "entryPrice": "0"},   # flat, filtered
        ])
    a = _mock_adapter(handler)
    pos = await a.get_positions()
    assert len(pos) == 1
    assert pos[0].symbol == "BTCUSDT"
    assert pos[0].qty == Decimal("0.5")
    await a.aclose()


@pytest.mark.asyncio
async def test_open_orders_only_ours():
    a = _adapter()
    # Register one of our orders' venue id.
    canonical = coid.build("77777777777777777777777777777777", OrderIntent.ENTRY, 0)
    vid = a._venue_client_id(canonical)
    def handler(request):
        return httpx.Response(200, json=[
            {"clientOrderId": vid, "orderId": 1, "symbol": "BTCUSDT",
             "side": "BUY", "origQty": "0.01", "executedQty": "0",
             "status": "NEW"},
            {"clientOrderId": "someManualOrder", "orderId": 2, "symbol": "BTCUSDT",
             "side": "SELL", "origQty": "1", "executedQty": "0", "status": "NEW"},
        ])
    a._client = httpx.AsyncClient(transport=MockTransport(handler), timeout=5.0)
    orders = await a._open_orders()
    # Only our order is returned; the manual one is skipped.
    assert len(orders) == 1
    assert orders[0].client_order_id == canonical
    await a.aclose()