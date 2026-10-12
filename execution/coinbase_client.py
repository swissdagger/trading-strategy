"""
execution/coinbase_client.py — authenticated Coinbase Advanced Trade REST client.

Thin and venue-generic: it signs and sends requests and returns parsed JSON.
All trading semantics live in execution/coinbase_adapter.py.

## Authentication (CDP API keys)

Every request carries its own short-lived JWT (Authorization: Bearer <jwt>),
signed ES256 with the key's EC private key:

    header  {"alg": "ES256", "kid": <key name>, "nonce": <random hex>}
    payload {"sub": <key name>, "iss": "cdp", "nbf": now, "exp": now + 120,
             "uri": "<METHOD> api.coinbase.com<path>"}

`uri` is the method, host and path WITHOUT the query string. Coinbase accepts
only ECDSA keys here: an Ed25519 key fails authentication, so the key must be
created with the ECDSA signature algorithm. The key name has the form
organizations/{org_id}/apiKeys/{key_id}; the secret is a PEM EC private key,
which env files usually carry on one line with literal "\\n" escapes.

## Errors

Retries 429 and 5xx with exponential backoff. Any other non-2xx raises
CoinbaseHTTPError carrying the status and body, so the adapter decides what a
4xx means (an order rejection is NOT an HTTP error: Coinbase answers 200 with
success=false).
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlsplit

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

log = logging.getLogger("executor.coinbase_client")

DEFAULT_BASE_URL = "https://api.coinbase.com/api/v3/brokerage"
JWT_TTL_S = 120


class CoinbaseAuthError(ValueError):
    """The configured credentials can't be used (missing, or not an EC key)."""


class CoinbaseHTTPError(RuntimeError):
    def __init__(self, method: str, path: str, status: int, body: str):
        super().__init__(f"{method} {path} -> HTTP {status}: {body[:300]}")
        self.status = status
        self.body = body


def load_ec_private_key(secret: str) -> ec.EllipticCurvePrivateKey:
    """Parse the key secret: a PEM EC private key, accepting literal '\\n'
    escapes (the one-line .env form). Raises CoinbaseAuthError for anything
    else — notably an Ed25519 key, which Coinbase rejects for this API."""
    if not secret:
        raise CoinbaseAuthError("COINBASE_API_PRIVATE_KEY is empty")
    pem = secret.replace("\\n", "\n").strip().encode()
    try:
        key = serialization.load_pem_private_key(pem, password=None)
    except Exception as e:
        raise CoinbaseAuthError(
            "COINBASE_API_PRIVATE_KEY is not a PEM private key "
            "(-----BEGIN EC PRIVATE KEY----- ...)") from e
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise CoinbaseAuthError(
            f"COINBASE_API_PRIVATE_KEY is a {type(key).__name__}, not an EC key. "
            "Coinbase Advanced Trade accepts only ECDSA (ES256) keys: create the "
            "CDP key with the ECDSA signature algorithm, not Ed25519.")
    return key


class CoinbaseClient:
    def __init__(
        self,
        key_name: str,
        private_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        timeout: float = 10.0,
        now_s: Optional[Callable[[], int]] = None,
    ):
        if not key_name:
            raise CoinbaseAuthError("COINBASE_API_KEY_NAME is empty")
        self.key_name = key_name
        self._key = load_ec_private_key(private_key)
        parts = urlsplit(base_url)
        self._host = parts.netloc            # e.g. api.coinbase.com
        self._base_path = parts.path.rstrip("/")   # e.g. /api/v3/brokerage
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(transport=transport, timeout=timeout)
        self._now_s = now_s or (lambda: int(time.time()))

    def build_jwt(self, method: str, path: str) -> str:
        """The bearer token for one request. path is relative to the base URL
        (e.g. '/accounts'); the signed uri uses the full path, no query."""
        now = self._now_s()
        uri = f"{method.upper()} {self._host}{self._base_path}{path}"
        return jwt.encode(
            {"sub": self.key_name, "iss": "cdp", "nbf": now,
             "exp": now + JWT_TTL_S, "uri": uri},
            self._key, algorithm="ES256",
            headers={"kid": self.key_name, "nonce": secrets.token_hex()},
        )

    async def request(self, method: str, path: str, *,
                      params: Optional[Dict[str, Any]] = None,
                      json: Optional[dict] = None) -> dict:
        url = f"{self._base_url}{path}"
        for attempt in range(4):
            headers = {"Authorization": f"Bearer {self.build_jwt(method, path)}"}
            try:
                resp = await self._client.request(method, url, params=params,
                                                  json=json, headers=headers)
            except httpx.TransportError as e:
                log.warning("%s %s transport error (attempt %d): %r",
                            method, path, attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                log.warning("%s %s -> HTTP %d (attempt %d), retrying",
                            method, path, resp.status_code, attempt + 1)
                await asyncio.sleep(2 ** attempt)
                continue
            if resp.status_code >= 400:
                raise CoinbaseHTTPError(method, path, resp.status_code, resp.text)
            return resp.json() if resp.content else {}
        raise CoinbaseHTTPError(method, path, 0, "retries exhausted")

    async def get(self, path: str, **params) -> dict:
        return await self.request("GET", path, params={k: v for k, v in params.items()
                                                       if v is not None})

    async def post(self, path: str, body: dict) -> dict:
        return await self.request("POST", path, json=body)

    async def aclose(self) -> None:
        await self._client.aclose()
