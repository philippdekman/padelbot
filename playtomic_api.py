"""Shared Playtomic API client.

Playtomic closed anonymous access to its consumer API in mid-2026 (documented:
https://helpmanager.playtomic.com/hc/en-gb/articles/38836515997073). Every
endpoint now needs a Bearer JWT obtained by logging in with a Playtomic
account's email + password (POST /v3/auth/login). The JWT lives ~1 hour and
must be refreshed.

Separately, Playtomic's CloudFront WAF blocks requests whose TLS fingerprint
doesn't look like a real browser AND whose source IP looks like a datacenter.
Plain `requests`/`httpx`/`urllib` — even routed through a residential proxy —
still get a raw CloudFront 403 HTML page. `curl_cffi` with `impersonate=` set
produces a genuine Chrome TLS ClientHello, and combined with the residential
proxy this reaches the real application (verified empirically: without the
proxy we still get 403 even with TLS impersonation; without impersonation we
still get 403 even through the proxy — both layers are required together).

This module is the single place all Playtomic HTTP calls should go through.
Callers get real exceptions on failure (`PlaytomicError` / `PlaytomicAuthError`)
instead of a silently swallowed `None`/`[]` — a bug that made the bot report
"Ничего не найдено" on genuine outages, indistinguishable from an honestly
empty result. Catch `PlaytomicError` at the call site and tell the user the
service is unavailable; do not treat it as "no matches."
"""
from __future__ import annotations

import os
import time
import logging
import threading

log = logging.getLogger(__name__)

try:
    from curl_cffi import requests as _cffi
except ImportError:  # pragma: no cover - dependency missing is a hard failure
    _cffi = None

BASE = "https://api.app.playtomic.io"
_IMPERSONATE = "chrome124"

_PROXY_HOST = os.environ.get("PLAYTOMIC_PROXY_HOST", "").strip()
_PROXY_PORT = os.environ.get("PLAYTOMIC_PROXY_PORT", "").strip()
_PROXY_USER = os.environ.get("PLAYTOMIC_PROXY_USER", "").strip()
_PROXY_PASS = os.environ.get("PLAYTOMIC_PROXY_PASS", "").strip()
_EMAIL = os.environ.get("PLAYTOMIC_EMAIL", "").strip()
_PASSWORD = os.environ.get("PLAYTOMIC_PASSWORD", "").strip()

_ROLES = ["ROLE_CUSTOMER"]
_DEFAULT_TTL = 55 * 60  # refresh a bit before the documented ~1h expiry


class PlaytomicError(Exception):
    """A real API failure: transport error, HTTP>=400, or bad payload.
    Never treat this as "no results" — surface it to the user instead."""


class PlaytomicAuthError(PlaytomicError):
    """Login/refresh failed — usually PLAYTOMIC_EMAIL/PLAYTOMIC_PASSWORD
    missing or wrong, or the account got logged out elsewhere."""


_lock = threading.Lock()
_access_token: str | None = None
_refresh_token: str | None = None
_expires_at: float = 0.0


def _proxies() -> dict | None:
    if not (_PROXY_HOST and _PROXY_PORT):
        return None
    auth = f"{_PROXY_USER}:{_PROXY_PASS}@" if _PROXY_USER else ""
    url = f"http://{auth}{_PROXY_HOST}:{_PROXY_PORT}"
    return {"http": url, "https": url}


def _raw_request(method: str, path: str, params=None, json_body=None, headers=None, timeout=30):
    if _cffi is None:
        raise PlaytomicError("curl_cffi is not installed — add it to requirements.txt")
    url = f"{BASE}{path}"
    hdrs = {"Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    try:
        r = _cffi.request(
            method, url, params=params, json=json_body, headers=hdrs,
            proxies=_proxies(), impersonate=_IMPERSONATE, timeout=timeout,
        )
    except Exception as e:
        raise PlaytomicError(f"network error calling {path}: {e}") from e
    return r


def _login() -> None:
    global _access_token, _refresh_token, _expires_at
    if not (_EMAIL and _PASSWORD):
        raise PlaytomicAuthError(
            "PLAYTOMIC_EMAIL / PLAYTOMIC_PASSWORD not set — add them in Railway → Variables"
        )
    body = {"email": _EMAIL, "password": _PASSWORD, "requested_user_roles": _ROLES}
    r = _raw_request("POST", "/v3/auth/login", json_body=body, timeout=30)
    if r.status_code == 401:
        raise PlaytomicAuthError("Playtomic отверг email/пароль (401) — проверь PLAYTOMIC_EMAIL/PLAYTOMIC_PASSWORD")
    if r.status_code >= 400:
        raise PlaytomicAuthError(f"login failed: HTTP {r.status_code}: {r.text[:200]}")
    try:
        data = r.json()
    except Exception as e:
        raise PlaytomicAuthError(f"login returned non-JSON body: {e}") from e
    token = data.get("access_token")
    if not token:
        raise PlaytomicAuthError(f"login response missing access_token: {data}")
    _access_token = token
    _refresh_token = data.get("refresh_token")
    ttl = data.get("expires_in")
    _expires_at = time.time() + (int(ttl) - 60 if ttl else _DEFAULT_TTL)


def _refresh() -> None:
    global _access_token, _refresh_token, _expires_at
    if not _refresh_token:
        return _login()
    body = {"refresh_token": _refresh_token, "requested_user_roles": _ROLES}
    try:
        r = _raw_request("POST", "/v3/auth/token", json_body=body, timeout=30)
        if r.status_code >= 400:
            raise PlaytomicError(f"refresh HTTP {r.status_code}")
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise PlaytomicError("refresh response missing access_token")
    except PlaytomicError:
        # Refresh token expired/revoked — fall back to a full login.
        return _login()
    _access_token = token
    _refresh_token = data.get("refresh_token", _refresh_token)
    ttl = data.get("expires_in")
    _expires_at = time.time() + (int(ttl) - 60 if ttl else _DEFAULT_TTL)


def _ensure_token() -> str:
    with _lock:
        if _access_token and time.time() < _expires_at:
            return _access_token
        if _access_token:
            _refresh()
        else:
            _login()
        assert _access_token
        return _access_token


def _invalidate_token() -> None:
    global _access_token, _expires_at
    with _lock:
        _access_token = None
        _expires_at = 0.0


def request_json(method: str, path: str, params=None, json_body=None, timeout=30, _retried=False):
    """Authenticated request against the Playtomic API. Raises `PlaytomicError`
    on any failure. Returns the parsed JSON body (list/dict), or None for a
    2xx response with an empty body."""
    token = _ensure_token()
    r = _raw_request(method, path, params=params, json_body=json_body,
                      headers={"Authorization": f"Bearer {token}"}, timeout=timeout)
    if r.status_code == 401 and not _retried:
        # Token rejected mid-flight (revoked/expired early) — force one retry.
        _invalidate_token()
        return request_json(method, path, params=params, json_body=json_body,
                             timeout=timeout, _retried=True)
    if r.status_code >= 400:
        raise PlaytomicError(f"HTTP {r.status_code} on {method} {path}: {r.text[:200]}")
    if not r.content:
        return None
    try:
        return r.json()
    except Exception as e:
        raise PlaytomicError(f"invalid JSON from {path}: {e}") from e


def get(path: str, params=None, timeout=30):
    return request_json("GET", path, params=params, timeout=timeout)


def is_configured() -> bool:
    """True if login credentials are present. Callers can use this to
    fail fast with a clear message instead of a generic exception."""
    return bool(_EMAIL and _PASSWORD)
