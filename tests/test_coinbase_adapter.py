"""
Tests for execution/coinbase_client.py, coinbase_adapter.py, read_only_adapter.py
and the factory, against a fake Coinbase Advanced Trade brokerage (httpx
MockTransport) whose request/response shapes follow Coinbase's published
OpenAPI spec. The fake verifies every request's ES256 JWT (signature, claims,
uri) and keeps order state, so the adapter is exercised end to end — including
under the real SignalExecutor.

These check the adapter's logic against the documented API. They are not a
substitute for the read-only verification against the real portfolio
(verify_coinbase_readonly.py), which needs the owner's key.
"""
import asyncio
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519

from execution import client_order_id as coid
from execution.adapter_factory import build_coinbase_adapter
from execution.coinbase_adapter import CoinbaseAdapter, CoinbaseConnectError
from execution.coinbase_client import CoinbaseAuthError, CoinbaseClient, CoinbaseHTTPError
from execution.read_only_adapter import READ_ONLY_REASON, ReadOnlyAdapter
from execution.trade_enable import TradeMode
from execution.types import (
    OrderIntent, OrderRequest, OrderStatus, TradeSide,
)

KEY_NAME = "organizations/org-1/apiKeys/key-1"
PORTFOLIO = "11111111-1111-1111-1111-111111111111"
NOW_S = 1_791_000_000


def ec_pem(key=None):
    key = key or ec.generate_private_key(ec.SECP256R1())
    return key, key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.TraditionalOpenSSL,
                                  serialization.NoEncryption()).decode()


def iso(s):
    return datetime.fromtimestamp(s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FakeBrokerage:
    """Minimal stateful Advanced Trade fake."""

    def __init__(self, public_key, *, perms=None, balances=None, foreign_accounts=False):
        self.public_key = public_key
        self.perms = perms or {"can_view": True, "can_trade": True, "can_transfer": False,
                               "portfolio_uuid": PORTFOLIO, "portfolio_type": "CONSUMER"}
        self.balances = balances if balances is not None else {
            "USD": ("1000", "50"), "BTC": ("0.5", "0"), "ETH": ("0", "0"), "SOL": ("0", "2")}
        self.foreign_accounts = foreign_accounts
        self.products = {
            "BTC-USD": {"base_increment": "0.00000001", "price_increment": "0.01",
                        "quote_increment": "0.01", "base_min_size": "0.00000001",
                        "quote_min_size": "1", "status": "online", "trading_disabled": False},
            "SOL-USD": {"base_increment": "0.001", "price_increment": "0.01",
                        "quote_increment": "0.01", "base_min_size": "0.001",
                        "quote_min_size": "1", "status": "online", "trading_disabled": False},
            "DEAD-USD": {"base_increment": "1", "price_increment": "0.01",
                         "base_min_size": "1", "quote_min_size": "1",
                         "status": "delisted", "trading_disabled": True},
        }
        self.books = {"BTC-USD": ("100000.00", "100010.00"), "SOL-USD": ("150.00", "150.10")}
        self.orders = {}           # order_id -> order dict
        self.by_cid = {}           # client_order_id -> order_id
        self.fills = []
        self.requests = []
        self.reject_next = None    # error_response dict
        self.http_status_next = None
        self.cancel_queued_polls = 2
        self.nonces = set()

    # --- auth -------------------------------------------------------------
    def _auth(self, request):
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        hdr = jwt.get_unverified_header(token)
        claims = jwt.decode(token, self.public_key, algorithms=["ES256"],
                            options={"verify_exp": False, "verify_nbf": False})
        expect_uri = f"{request.method} {request.url.host}{request.url.path}"
        assert claims["uri"] == expect_uri, (claims["uri"], expect_uri)
        assert claims["sub"] == KEY_NAME and claims["iss"] == "cdp"
        assert claims["exp"] - claims["nbf"] == 120
        assert hdr["kid"] == KEY_NAME and hdr["alg"] == "ES256"
        assert hdr["nonce"] not in self.nonces
        self.nonces.add(hdr["nonce"])

    def add_fill(self, order_id, size, price, *, t, size_in_quote=False, side=None,
                 commission="0.12"):
        o = self.orders.get(order_id, {})
        self.fills.append({
            "entry_id": str(uuid.uuid4()), "trade_id": str(uuid.uuid4()),
            "order_id": order_id, "trade_time": iso(t), "price": str(price),
            "size": str(size), "commission": commission,
            "product_id": o.get("product_id", "BTC-USD"),
            "size_in_quote": size_in_quote, "side": side or o.get("side", "BUY"),
        })
        if o:
            filled = Decimal(o["filled_size"]) + (Decimal(str(size)) / Decimal(str(price))
                                                   if size_in_quote else Decimal(str(size)))
            o["filled_size"] = str(filled)
            base = Decimal(o["order_configuration"]["limit_limit_gtc"]["base_size"])
            if filled >= base:
                o["status"] = "FILLED"

    # --- routing ----------------------------------------------------------
    def handler(self, request: httpx.Request) -> httpx.Response:
        self._auth(request)
        path = request.url.path.removeprefix("/api/v3/brokerage")
        q = request.url.params
        self.requests.append((request.method, path, dict(q),
                              json.loads(request.content) if request.content else None))
        if self.http_status_next:
            st, self.http_status_next = self.http_status_next, None
            return httpx.Response(st, json={"error": "INVALID_ARGUMENT", "message": "bad"})
        m = request.method
        if m == "GET" and path == "/key_permissions":
            return httpx.Response(200, json=self.perms)
        if m == "GET" and path == "/accounts":
            accts = [{"uuid": str(uuid.uuid4()), "currency": c,
                      "available_balance": {"value": a, "currency": c},
                      "hold": {"value": h, "currency": c},
                      "retail_portfolio_id": PORTFOLIO}
                     for c, (a, h) in self.balances.items()]
            if self.foreign_accounts:
                accts.append({"currency": "BTC", "available_balance": {"value": "9"},
                              "hold": {"value": "0"}, "retail_portfolio_id": "other"})
            page = int(q.get("cursor") or 0)
            chunk = accts[page * 2:(page + 1) * 2]          # 2 per page: forces paging
            more = (page + 1) * 2 < len(accts)
            return httpx.Response(200, json={"accounts": chunk, "has_next": more,
                                             "cursor": str(page + 1) if more else ""})
        if m == "GET" and path.startswith("/products/"):
            pid = path.split("/")[-1]
            return httpx.Response(200, json={"product_id": pid, **self.products[pid]})
        if m == "GET" and path == "/best_bid_ask":
            books = [{"product_id": p, "bids": [{"price": self.books[p][0], "size": "1"}],
                      "asks": [{"price": self.books[p][1], "size": "1"}]}
                     for p in q.get_list("product_ids") if p in self.books]
            return httpx.Response(200, json={"pricebooks": books})
        if m == "POST" and path == "/orders":
            body = json.loads(request.content)
            if self.reject_next:
                err, self.reject_next = self.reject_next, None
                return httpx.Response(200, json={"success": False, "error_response": err})
            cid = body["client_order_id"]
            if cid in self.by_cid:      # documented: repeated id returns the existing order
                oid = self.by_cid[cid]
            else:
                oid = str(uuid.uuid4())
                self.by_cid[cid] = oid
                self.orders[oid] = {"order_id": oid, "client_order_id": cid,
                                    "product_id": body["product_id"], "side": body["side"],
                                    "status": "OPEN", "filled_size": "0",
                                    "order_configuration": body["order_configuration"]}
            return httpx.Response(200, json={"success": True, "success_response": {
                "order_id": oid, "product_id": body["product_id"], "side": body["side"],
                "client_order_id": cid}})
        if m == "GET" and path.startswith("/orders/historical/") and path.count("/") == 3 \
                and not path.endswith(("batch", "fills")):
            oid = path.split("/")[-1]
            o = self.orders[oid]
            if o["status"] == "CANCEL_QUEUED":
                o["_polls"] = o.get("_polls", 0) + 1
                if o["_polls"] >= self.cancel_queued_polls:
                    o["status"] = "CANCELLED"
            return httpx.Response(200, json={"order": o})
        if m == "GET" and path == "/orders/historical/batch":
            sts = q.get_list("order_status")
            os_ = [o for o in self.orders.values() if not sts or o["status"] in sts]
            return httpx.Response(200, json={"orders": os_, "has_next": False, "cursor": ""})
        if m == "POST" and path == "/orders/batch_cancel":
            res = []
            for oid in json.loads(request.content)["order_ids"]:
                self.orders[oid]["status"] = "CANCEL_QUEUED"
                res.append({"success": True, "order_id": oid})
            return httpx.Response(200, json={"results": res})
        if m == "POST" and path == "/orders/edit":
            body = json.loads(request.content)
            o = self.orders[body["order_id"]]
            o["order_configuration"]["limit_limit_gtc"]["limit_price"] = body["price"]
            return httpx.Response(200, json={"success": True})
        if m == "GET" and path == "/orders/historical/fills":
            start = q.get("start_sequence_timestamp")
            fs = [f for f in self.fills if not start or f["trade_time"] >= start]
            page = int(q.get("cursor") or 0)
            chunk = fs[page * 2:(page + 1) * 2]
            more = (page + 1) * 2 < len(fs)
            return httpx.Response(200, json={"fills": chunk, "cursor": str(page + 1) if more else ""})
        return httpx.Response(404, json={"error": "NOT_FOUND", "path": path})


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    real = asyncio.sleep
    async def s(t, *a, **k):
        await real(0)
    monkeypatch.setattr("execution.coinbase_client.asyncio.sleep", s)
    monkeypatch.setattr("execution.coinbase_adapter.asyncio.sleep", s)


def setup(**fake_kw):
    key, pem = ec_pem()
    fake = FakeBrokerage(key.public_key(), **fake_kw)
    client = CoinbaseClient(KEY_NAME, pem.replace("\n", "\\n"),
                            transport=httpx.MockTransport(fake.handler),
                            now_s=lambda: NOW_S)
    adapter = CoinbaseAdapter(client, portfolio_uuid=PORTFOLIO, quote_currency="USD",
                              now_ms=lambda: NOW_S * 1000)
    return fake, adapter


def order(cid=None, side=TradeSide.LONG, intent=OrderIntent.ENTRY, qty="0.012345678912",
          limit="100020.017", symbol="BTC-USD"):
    tid = str(uuid.uuid4())
    return OrderRequest(client_order_id=cid or coid.build(tid, intent, 0), trade_id=tid,
                        symbol=symbol, asset_class="crypto", intent=intent, side=side,
                        qty=Decimal(qty), limit_price=Decimal(limit))


# --------------------------------------------------------------------
# Client / JWT
# --------------------------------------------------------------------

def test_jwt_claims_and_signature():
    key, pem = ec_pem()
    c = CoinbaseClient(KEY_NAME, pem, now_s=lambda: 1000)
    t = c.build_jwt("get", "/accounts")
    claims = jwt.decode(t, key.public_key(), algorithms=["ES256"],
                        options={"verify_exp": False, "verify_nbf": False})
    assert claims == {"sub": KEY_NAME, "iss": "cdp", "nbf": 1000, "exp": 1120,
                      "uri": "GET api.coinbase.com/api/v3/brokerage/accounts"}
    h = jwt.get_unverified_header(t)
    assert h["kid"] == KEY_NAME and h["alg"] == "ES256" and len(h["nonce"]) == 64


def test_ed25519_key_is_rejected_with_a_clear_message():
    k = ed25519.Ed25519PrivateKey.generate()
    pem = k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                          serialization.NoEncryption()).decode()
    with pytest.raises(CoinbaseAuthError, match="ECDSA"):
        CoinbaseClient(KEY_NAME, pem)


@pytest.mark.parametrize("name,secret,match", [("", "x", "KEY_NAME"), (KEY_NAME, "", "empty"),
                                               (KEY_NAME, "not a pem", "PEM")])
def test_bad_credentials_fail_fast(name, secret, match):
    with pytest.raises(CoinbaseAuthError, match=match):
        CoinbaseClient(name, secret)


@pytest.mark.asyncio
async def test_every_request_is_signed_with_a_fresh_token():
    fake, a = setup()
    await a.connect()
    await a.get_touch("BTC-USD", "crypto")
    assert len(fake.nonces) == len(fake.requests) >= 3   # one token per request


# --------------------------------------------------------------------
# connect() safety
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_accepts_a_scoped_trade_key():
    fake, a = setup()
    await a.connect()
    assert a.permissions["portfolio_uuid"] == PORTFOLIO


@pytest.mark.asyncio
@pytest.mark.parametrize("perm,value,match", [
    ("portfolio_uuid", "22222222-2222-2222-2222-222222222222", "bound to portfolio"),
    ("can_transfer", True, "TRANSFER"),
    ("can_trade", False, "cannot trade"),
    ("can_view", False, "cannot view"),
])
async def test_connect_refuses_wrong_keys(perm, value, match):
    fake, a = setup()
    fake.perms[perm] = value
    with pytest.raises(CoinbaseConnectError, match=match):
        await a.connect()


@pytest.mark.asyncio
async def test_read_only_mode_accepts_a_view_only_key():
    fake, a = setup()
    fake.perms["can_trade"] = False
    a.require_trade = False
    await a.connect()


@pytest.mark.asyncio
async def test_connect_refuses_a_key_that_sees_other_portfolios():
    fake, a = setup(foreign_accounts=True)
    with pytest.raises(CoinbaseConnectError, match="outside the bot portfolio"):
        await a.connect()


@pytest.mark.asyncio
async def test_connect_requires_a_portfolio_uuid():
    fake, a = setup()
    a.portfolio_uuid = ""
    with pytest.raises(CoinbaseConnectError, match="PORTFOLIO_UUID"):
        await a.connect()


# --------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_positions_are_nonzero_base_balances_including_hold():
    fake, a = setup()
    pos = {p.symbol: p.qty for p in await a.get_positions()}
    assert pos == {"BTC-USD": Decimal("0.5"), "SOL-USD": Decimal("2")}   # ETH 0, USD is quote


@pytest.mark.asyncio
async def test_equity_available_is_free_quote_total_marks_holdings_at_mid():
    fake, a = setup()
    eq = await a.get_account_equity()
    assert eq.currency == "USD"
    assert eq.available == Decimal("1000")
    # quote 1000 + 50 hold, BTC 0.5 x 100005, SOL 2 x 150.05
    assert eq.total == Decimal("1050") + Decimal("0.5") * Decimal("100005") + 2 * Decimal("150.05")


@pytest.mark.asyncio
async def test_touch_and_product_constraints():
    fake, a = setup()
    t = await a.get_touch("BTC-USD", "crypto")
    assert (t.bid, t.ask) == (Decimal("100000.00"), Decimal("100010.00"))
    assert await a.get_touch("NOPE-USD", "crypto") is None
    assert await a.get_step_size("SOL-USD", "crypto") == Decimal("0.001")
    assert await a.get_price_increment("SOL-USD", "crypto") == Decimal("0.01")
    m = await a.get_min_order_size("SOL-USD", "crypto")
    assert (m.base, m.quote) == (Decimal("0.001"), Decimal("1"))
    await a.get_step_size("SOL-USD", "crypto")
    assert sum(1 for r in fake.requests if r[1] == "/products/SOL-USD") == 1   # cached


# --------------------------------------------------------------------
# Orders
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_sends_a_limit_gtc_with_quantized_size_and_price():
    fake, a = setup()
    o = order()
    r = await a.submit_order(o)
    assert r.status == OrderStatus.WORKING and r.venue_order_id in fake.orders
    body = [x[3] for x in fake.requests if x[1] == "/orders"][0]
    assert body["client_order_id"] == o.client_order_id
    assert body["side"] == "BUY" and body["product_id"] == "BTC-USD"
    cfg = body["order_configuration"]["limit_limit_gtc"]
    assert cfg == {"base_size": "0.01234567", "limit_price": "100020.01", "post_only": False}


@pytest.mark.asyncio
async def test_exit_of_a_long_is_a_sell_rounded_up_to_the_tick():
    fake, a = setup()
    await a.submit_order(order(intent=OrderIntent.EXIT, limit="99990.011"))
    cfg = [x[3] for x in fake.requests if x[1] == "/orders"][0]
    assert cfg["side"] == "SELL"
    assert cfg["order_configuration"]["limit_limit_gtc"]["limit_price"] == "99990.02"


@pytest.mark.asyncio
async def test_resubmitting_the_same_client_id_returns_the_same_order():
    fake, a = setup()
    o = order()
    r1, r2 = await a.submit_order(o), await a.submit_order(o)
    assert r1.venue_order_id == r2.venue_order_id and len(fake.orders) == 1


@pytest.mark.asyncio
async def test_venue_rejection_maps_to_rejected_with_reason():
    fake, a = setup()
    fake.reject_next = {"error": "INSUFFICIENT_FUND", "message": "Insufficient balance",
                        "new_order_failure_reason": "INSUFFICIENT_FUNDS"}
    r = await a.submit_order(order())
    assert r.status == OrderStatus.REJECTED
    assert "INSUFFICIENT_FUND" in r.reject_reason and "Insufficient balance" in r.reject_reason


@pytest.mark.asyncio
async def test_http_4xx_is_a_rejection_but_5xx_exhaustion_raises():
    fake, a = setup()
    await a.product("BTC-USD")                   # so the 400 hits the order POST
    fake.http_status_next = 400
    assert (await a.submit_order(order())).status == OrderStatus.REJECTED
    fake.http_status_next = None
    a.client._client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(503)))
    with pytest.raises(CoinbaseHTTPError):        # outcome unknown: reconcile resolves it
        await a.submit_order(order())


@pytest.mark.asyncio
async def test_short_and_untradable_are_refused_without_a_request():
    fake, a = setup()
    r = await a.submit_order(order(side=TradeSide.SHORT))
    assert r.status == OrderStatus.REJECTED and "short" in r.reject_reason
    r = await a.submit_order(order(symbol="DEAD-USD", qty="5", limit="1"))
    assert r.status == OrderStatus.REJECTED and "not tradable" in r.reject_reason
    assert not any(x[1] == "/orders" for x in fake.requests)


@pytest.mark.asyncio
async def test_size_below_increment_is_refused():
    fake, a = setup()
    r = await a.submit_order(order(symbol="SOL-USD", qty="0.0004", limit="150.20"))
    assert r.status == OrderStatus.REJECTED and "rounds to 0" in r.reject_reason


@pytest.mark.asyncio
async def test_cancel_waits_for_a_terminal_status_and_reports_filled_qty():
    fake, a = setup()
    o = order()
    r = await a.submit_order(o)
    fake.add_fill(r.venue_order_id, "0.004", "100009", t=NOW_S - 5)
    c = await a.cancel_order(o.client_order_id)
    assert c.status == OrderStatus.CANCELLED
    assert c.filled_qty == Decimal("0.004")
    assert fake.orders[r.venue_order_id]["status"] == "CANCELLED"


@pytest.mark.asyncio
async def test_cancel_of_a_filled_order_sends_no_cancel():
    fake, a = setup()
    o = order(qty="0.01")
    r = await a.submit_order(o)
    fake.add_fill(r.venue_order_id, "0.01", "100009", t=NOW_S - 5)
    c = await a.cancel_order(o.client_order_id)
    assert c.status == OrderStatus.FILLED and c.filled_qty == Decimal("0.01")
    assert not any(x[1] == "/orders/batch_cancel" for x in fake.requests)


@pytest.mark.asyncio
async def test_cancel_finds_an_order_placed_before_a_restart():
    fake, a = setup()
    o = order()
    await a.submit_order(o)
    _, a2 = setup()
    a2.client = a.client                        # same venue, fresh adapter (no id map)
    c = await a2.cancel_order(o.client_order_id)
    assert c.status == OrderStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_of_an_unknown_order_is_a_noop():
    fake, a = setup()
    c = await a.cancel_order("cv1." + "0" * 32 + ".E.0")
    assert c.status == OrderStatus.CANCELLED
    assert not any(x[1] == "/orders/batch_cancel" for x in fake.requests)


@pytest.mark.asyncio
async def test_amend_edits_price_keeping_size():
    fake, a = setup()
    o = order()
    r = await a.submit_order(o)
    res = await a.amend_order(o.client_order_id, Decimal("100030.019"))
    body = [x[3] for x in fake.requests if x[1] == "/orders/edit"][0]
    assert body == {"order_id": r.venue_order_id, "price": "100030.01", "size": "0.01234567"}
    assert res.status == OrderStatus.WORKING


# --------------------------------------------------------------------
# Fills
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_poll_attributes_via_order_lookup_and_dedupes():
    fake, a = setup()
    o = order()
    r = await a.submit_order(o)
    _, fresh = setup()                           # restarted adapter: no id maps
    fresh.client = a.client
    fake.add_fill(r.venue_order_id, "0.006", "100009", t=NOW_S - 30)
    fake.add_fill(r.venue_order_id, "0.006", "100010", t=NOW_S - 20)
    fake.add_fill(r.venue_order_id, "0.00034567", "100010", t=NOW_S - 10)
    got = []
    async def cb(f):
        got.append(f)
    newest = await fresh.poll_fills_once((NOW_S - 300) * 1000, cb)
    assert len(got) == 3 and newest == (NOW_S - 10) * 1000
    f = got[0]
    assert f.client_order_id == o.client_order_id and f.venue_order_id == r.venue_order_id
    assert f.intent == OrderIntent.ENTRY and f.qty == Decimal("0.006")
    assert f.fee == Decimal("0.12") and f.fee_currency == "USD"
    await fresh.poll_fills_once((NOW_S - 300) * 1000, cb)   # overlapping window
    assert len(got) == 3
    lookups = [x for x in fake.requests if x[1] == f"/orders/historical/{r.venue_order_id}"]
    assert len(lookups) == 1                      # order looked up once, then cached


@pytest.mark.asyncio
async def test_size_in_quote_fill_is_converted_to_base():
    fake, a = setup()
    r = await a.submit_order(order())
    fake.add_fill(r.venue_order_id, "500", "100000", t=NOW_S - 5, size_in_quote=True)
    got = []
    async def cb(f): got.append(f)
    await a.poll_fills_once((NOW_S - 60) * 1000, cb)
    assert got[0].qty == Decimal("0.005")


@pytest.mark.asyncio
async def test_failed_callback_is_redelivered_next_poll():
    fake, a = setup()
    r = await a.submit_order(order())
    fake.add_fill(r.venue_order_id, "0.001", "100000", t=NOW_S - 5)
    calls = []
    async def flaky(f):
        calls.append(f)
        if len(calls) == 1:
            raise RuntimeError("db down")
    await a.poll_fills_once((NOW_S - 60) * 1000, flaky)
    await a.poll_fills_once((NOW_S - 60) * 1000, flaky)
    assert len(calls) == 2 and calls[0].venue_exec_id == calls[1].venue_exec_id


@pytest.mark.asyncio
async def test_exit_fill_intent_comes_from_the_cv1_id():
    fake, a = setup()
    o = order(intent=OrderIntent.EXIT, limit="99990")
    r = await a.submit_order(o)
    fake.add_fill(r.venue_order_id, "0.001", "99995", t=NOW_S - 5)
    got = []
    async def cb(f): got.append(f)
    await a.poll_fills_once((NOW_S - 60) * 1000, cb)
    assert got[0].intent == OrderIntent.EXIT


# --------------------------------------------------------------------
# Reconcile
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_reports_positions_open_orders_and_recent_fills():
    fake, a = setup()
    o = order()
    r = await a.submit_order(o)
    fake.add_fill(r.venue_order_id, "0.002", "100000", t=NOW_S - 3600)
    snap = await a.reconcile()
    assert {p.symbol for p in snap.positions} == {"BTC-USD", "SOL-USD"}
    assert len(snap.open_orders) == 1
    vo = snap.open_orders[0]
    assert vo.client_order_id == o.client_order_id and vo.side == TradeSide.LONG
    assert vo.qty == Decimal("0.01234567") and vo.filled_qty == Decimal("0.002")
    assert vo.status == OrderStatus.PARTIALLY_FILLED
    assert len(snap.recent_fills) == 1 and snap.recent_fills[0].client_order_id == o.client_order_id


# --------------------------------------------------------------------
# Read-only wrapper and factory
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_read_only_adapter_refuses_writes_and_passes_reads():
    fake, a = setup()
    ro = ReadOnlyAdapter(a)
    assert (await ro.get_touch("BTC-USD", "crypto")).bid == Decimal("100000.00")
    o = order()
    for res in (await ro.submit_order(o), await ro.cancel_order(o.client_order_id),
                await ro.amend_order(o.client_order_id, Decimal("1"))):
        assert res.status == OrderStatus.REJECTED and res.reject_reason == READ_ONLY_REASON
    assert not any(x[0] == "POST" for x in fake.requests)
    assert ro.supports_short is False and ro.venue == "coinbase"


def test_factory_mode_mapping(monkeypatch):
    key, pem = ec_pem()
    cfg = {"api_key_name": KEY_NAME, "api_private_key": pem, "portfolio_uuid": PORTFOLIO,
           "quote_currency": "USD"}
    off, kind = build_coinbase_adapter(cfg, TradeMode.OFF)
    assert isinstance(off, ReadOnlyAdapter) and kind == "paper"
    assert off.inner.require_trade is False
    live, kind = build_coinbase_adapter(cfg, TradeMode.LIVE)
    assert isinstance(live, CoinbaseAdapter) and kind == "live" and live.require_trade
    with pytest.raises(NotImplementedError, match="Phase 5"):
        build_coinbase_adapter(cfg, TradeMode.PAPER)


# --------------------------------------------------------------------
# End to end under the real executor
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_executor_long_round_trip_and_short_to_shadow():
    from test_signal_executor import enter_row, make_hooks
    from execution.signal_executor import SignalExecutor
    from execution.shadow_book import ShadowBook
    from test_executor_hardening import MemShadowStore

    fake, a = setup(balances={"USD": ("1000", "0")})
    hooks, state = make_hooks(size=Decimal("0.0012345"))
    hooks.get_touch = a.get_touch
    store = MemShadowStore()
    book = ShadowBook(store.hooks(), taker_fee_bps=Decimal("120"))
    ex = SignalExecutor(a, hooks, shadow_book=book)
    a.start_fill_stream = lambda cb: _noop()     # drive fills by hand below
    await ex.start()

    row = enter_row(symbol="BTC-USD", direction="LONG")
    row["asset_class"] = "crypto"
    await ex.handle_signal(row)
    (oid, o), = fake.orders.items()
    assert o["side"] == "BUY" and o["client_order_id"].startswith("cv1.")
    cfg = o["order_configuration"]["limit_limit_gtc"]
    assert cfg["base_size"] == "0.0012345"
    assert Decimal(cfg["limit_price"]) % Decimal("0.01") == 0
    trade = next(iter(ex._open.values()))
    assert trade.state == "SUBMITTED"

    fake.add_fill(oid, "0.0012345", "100010", t=NOW_S - 1)
    await a.poll_fills_once((NOW_S - 60) * 1000, ex._on_fill)
    assert trade.state == "OPEN" and trade.filled_qty == Decimal("0.0012345")

    short = enter_row(chain="chainS", symbol="BTC-USD", direction="SHORT")
    short["asset_class"] = "crypto"
    await ex.handle_signal(short)
    assert len(fake.orders) == 1                  # nothing sent for the short
    assert ("two_ladder", "chainS:S") in store.rows


async def _noop():
    return None
