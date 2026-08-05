#!/usr/bin/env python3
"""
Script:   zonneplan.py

Abstract:
    Zonneplan API client for electricity tariff data. Handles the email +
    verification-link login flow, token storage/refresh, and fetching
    quarter-hourly electricity prices for the Charging Plan feature.
    Reverse-engineered against the same app-api.zonneplan.nl endpoints as
    https://github.com/fsaris/home-assistant-zonneplan-one.

Features:
    - request_login()/check_login(): email verification-link auth flow.
    - get_current_tariff(): fetches the electricity-quarter-hourly
      consumer-prices chart (falling back to the legacy hourly summary
      endpoint if unavailable) and stores the forecast via cs_db.py.
    - Standalone CLI mode: periodic tariff download for cron/systemd-timer
      use outside the CSMS web process.

Usage:
    As a library: imported by csms.py (API endpoints under /api/zonneplan).
    As a CLI: python zonneplan.py [--debug]

History:
    1.0.0  karl@lovink.net  Initial version

Copyright (C) 2026 Karl Lovink

License:  GNU General Public License v3.0 or later (GPL-3.0-or-later)
          This program is free software: you may redistribute it and/or
          modify it under the terms of the GNU General Public License, as
          published by the Free Software Foundation, either version 3 of
          the License, or (at your option) any later version. You should
          have received a copy of the License along with this program (see
          COPYING); if not, see <https://www.gnu.org/licenses/gpl-3.0.html>
"""

__version__ = "1.0.0"
__author__  = "Karl Lovink"
__email__   = "karl@lovink.net"
__license__ = "GPL-3.0-or-later"

import logging
import time
import threading
import requests

import cs_db

logger = logging.getLogger(__name__)

API_BASE = "https://app-api.zonneplan.nl"
LOGIN_REQUEST_URI = f"{API_BASE}/auth/request"
OAUTH2_TOKEN_URI = f"{API_BASE}/oauth/token"
APP_VERSION = "5.10.1"
PRICE_VALUE_FACTOR = 0.0000001

_REQUEST_HEADERS = {
    "content-type": "application/json;charset=utf-8",
    "x-app-version": APP_VERSION,
    "x-app-environment": "production",
}

_lock = threading.Lock()


def _raise_for_status(resp):
    """Like resp.raise_for_status(), but includes the response body in the
    exception message — Zonneplan's error responses carry the actual reason
    (e.g. which field failed validation) that the bare requests.HTTPError
    string discards, which makes 4xx/5xx failures otherwise undiagnosable.
    """
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = (resp.text or "").strip()
        if body:
            raise requests.exceptions.HTTPError(f"{e} — response body: {body[:2000]}", response=resp) from e
        raise


def _get_config() -> dict:
    return cs_db.get_zonneplan_config()


def _save_config(key: str, value: str):
    cs_db.set_zonneplan_config_value(key, value)


# ── Authentication ──────────────────────────────────────────────

def request_login(email: str) -> str:
    """Step 1: Request a login email. Returns the auth UUID."""
    resp = requests.post(
        LOGIN_REQUEST_URI,
        json={"email": email},
        headers=_REQUEST_HEADERS,
        timeout=10,
    )
    _raise_for_status(resp)
    data = resp.json()
    uuid = data["data"]["uuid"]
    _save_config("email", email)
    _save_config("auth_uuid", uuid)
    _save_config("auth_status", "pending")
    return uuid


def check_login(uuid: str = None) -> dict:
    """Step 2: Check if the user has verified the login email.
    Returns token dict on success, or None if still pending.
    """
    if not uuid:
        cfg = _get_config()
        uuid = cfg.get("auth_uuid", "")
    if not uuid:
        raise ValueError("No auth UUID — call request_login first")

    resp = requests.get(
        f"{LOGIN_REQUEST_URI}/{uuid}",
        headers=_REQUEST_HEADERS,
        timeout=10,
    )
    _raise_for_status(resp)
    data = resp.json().get("data", {})

    if not data.get("is_activated") or not data.get("password"):
        return None  # still pending

    cfg = _get_config()
    email = cfg.get("email", "")
    token = _request_token({
        "grant_type": "one_time_password",
        "email": email,
        "password": data["password"],
    })
    _store_token(token)
    _save_config("auth_status", "authenticated")
    _save_config("auth_uuid", "")
    return token


def _request_token(grant_params: dict) -> dict:
    resp = requests.post(
        OAUTH2_TOKEN_URI,
        json=grant_params,
        headers=_REQUEST_HEADERS,
        timeout=30,
    )
    _raise_for_status(resp)
    return resp.json()


def _store_token(token: dict):
    _save_config("access_token", token.get("access_token", ""))
    _save_config("refresh_token", token.get("refresh_token", ""))
    expires_in = token.get("expires_in", 3600)
    _save_config("token_expires_at", str(int(time.time()) + int(expires_in)))


def _refresh_token_if_needed() -> str:
    """Ensure the access token is valid, refresh if expired. Returns access_token."""
    with _lock:
        cfg = _get_config()
        access_token = cfg.get("access_token", "")
        refresh_token = cfg.get("refresh_token", "")
        expires_at = int(cfg.get("token_expires_at", "0"))

        if not access_token or not refresh_token:
            raise RuntimeError("Not authenticated with Zonneplan")

        if time.time() < expires_at - 60:
            return access_token

        logger.info("Refreshing Zonneplan token")
        token = _request_token({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        })
        _store_token(token)
        return token["access_token"]


def _authed_headers() -> dict:
    token = _refresh_token_if_needed()
    headers = dict(_REQUEST_HEADERS)
    headers["Authorization"] = f"Bearer {token}"
    return headers


# ── API calls ───────────────────────────────────────────────────

def get_user_accounts() -> dict:
    """Fetch the user's account data (addresses, connections, contracts)."""
    resp = requests.get(
        f"{API_BASE}/user-accounts/me",
        headers=_authed_headers(),
        timeout=15,
    )
    _raise_for_status(resp)
    return resp.json().get("data", {})


def get_electricity_data(connection_uuid: str) -> dict:
    """Fetch electricity delivered data for a connection."""
    resp = requests.get(
        f"{API_BASE}/connections/{connection_uuid}/electricity-delivered",
        headers=_authed_headers(),
        timeout=15,
    )
    _raise_for_status(resp)
    return resp.json().get("data", {})


def get_summary_data(connection_uuid: str) -> dict:
    """Fetch summary data for a connection.

    NOTE: this endpoint's "price_per_hour" field is Zonneplan's LEGACY,
    hourly-only price list — it does not carry the newer quarter-hour prices.
    Only used here as a fallback; see get_consumer_prices_chart() for the
    current quarter-hourly source.
    """
    resp = requests.get(
        f"{API_BASE}/connections/{connection_uuid}/summary",
        headers=_authed_headers(),
        timeout=15,
    )
    _raise_for_status(resp)
    return resp.json().get("data", {})


def get_consumer_prices_chart(chart_name: str) -> dict:
    """Fetch a consumer-prices chart, e.g. 'electricity-quarter-hourly' or
    'electricity-hourly'. Response shape: {"chart": {"series": {"prices": [
    {"start_date", "end_date", "price_tax_included": {"amount": ...},
    "price_tax_excluded": {"amount": ...}, "tariff_group", ...}, ... ]}}}.
    No connection/address id is needed — the account's own contract is
    resolved from the auth token.
    """
    resp = requests.get(
        f"{API_BASE}/api/consumer-prices/charts/{chart_name}",
        headers=_authed_headers(),
        timeout=15,
    )
    _raise_for_status(resp)
    return resp.json().get("data", {})


def _floor_to_quarter(dt):
    """Floor a datetime down to the start of its 15-minute slot."""
    return dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)


def get_current_tariff() -> dict:
    """Get the current electricity tariff from Zonneplan.
    Returns dict with 'tariff_eur_per_kwh', 'tariff_group', 'forecast' keys.

    Prices are fetched per 15-minute slot from the 'electricity-quarter-hourly'
    consumer-prices chart. The account/connection "summary" endpoint also
    returns a "price_per_hour" field, but that one is Zonneplan's legacy,
    hourly-only list (kept for older clients) — it does NOT contain
    quarter-hour prices, despite the similar-sounding name. That field is
    only used here as a fallback if the quarter-hourly chart is unavailable.
    """
    cfg = _get_config()
    connection_uuid = cfg.get("connection_uuid", "")
    if not connection_uuid:
        raise RuntimeError("No Zonneplan connection configured")

    from datetime import datetime, timezone
    import dateutil.parser

    now = datetime.now(timezone.utc)
    now_key = _floor_to_quarter(now).strftime("%Y-%m-%d %H:%M")

    price_data = {}

    def _ingest(entries, dt_field, price_getter):
        for entry in entries:
            dt_str = entry.get(dt_field)
            if not dt_str:
                continue
            raw = price_getter(entry)
            if raw is None:
                continue
            try:
                dt = dateutil.parser.parse(dt_str)
                slot_key = _floor_to_quarter(dt.astimezone(timezone.utc)).strftime("%Y-%m-%d %H:%M")
                price_data[slot_key] = {"electricity_price": raw, "tariff_group": entry.get("tariff_group")}
            except (ValueError, TypeError):
                continue

    try:
        chart = get_consumer_prices_chart("electricity-quarter-hourly")
        prices = (chart or {}).get("chart", {}).get("series", {}).get("prices", [])
        _ingest(prices, "start_date", lambda e: (e.get("price_tax_included") or {}).get("amount"))
    except Exception:
        logger.exception("Failed to fetch electricity-quarter-hourly consumer-prices chart")

    if not price_data:
        # Fall back to the legacy hourly summary data (e.g. older contract
        # type without quarter-hourly pricing, or the chart call failed).
        try:
            data = get_summary_data(connection_uuid)
        except Exception:
            logger.exception("Failed to fetch Zonneplan summary data")
            data = {}
        _ingest(data.get("price_per_hour", []), "datetime", lambda e: e.get("electricity_price"))

    current = price_data.get(now_key, {})

    raw_price = current.get("electricity_price")
    tariff = round(raw_price * PRICE_VALUE_FACTOR, 6) if raw_price is not None else None

    # Build forecast list — one entry per 15-minute slot. The "hour" key name
    # is kept for API/DB compatibility; its value is a quarter-hour
    # timestamp ("YYYY-MM-DD HH:MM"), not a whole hour.
    forecast = []
    for slot_key in sorted(price_data.keys()):
        entry = price_data[slot_key]
        raw = entry.get("electricity_price")
        if raw is not None:
            forecast.append({
                "hour": slot_key,
                "tariff_eur_per_kwh": round(raw * PRICE_VALUE_FACTOR, 6),
                "tariff_group": entry.get("tariff_group"),
            })

    result = {
        "tariff_eur_per_kwh": tariff,
        "tariff_group": current.get("tariff_group"),
        "forecast": forecast,
    }

    # Persist tariff data to database
    if forecast:
        try:
            cs_db.store_zonneplan_tariffs(forecast)
        except Exception:
            logger.exception("Failed to store Zonneplan tariffs in DB")

    return result


def calculate_cost(energy_wh: float, tariff_eur_per_kwh: float) -> float:
    """Calculate charging cost in EUR given energy in Wh and tariff in EUR/kWh."""
    if tariff_eur_per_kwh is None or energy_wh is None:
        return None
    return round((energy_wh / 1000.0) * tariff_eur_per_kwh, 4)


def is_authenticated() -> bool:
    cfg = _get_config()
    return cfg.get("auth_status") == "authenticated" and bool(cfg.get("access_token"))


def setup_connection() -> dict:
    """Discover connections and store the first electricity connection UUID.
    Returns account summary.
    """
    accounts = get_user_accounts()
    address_groups = accounts.get("address_groups", [])

    for group in address_groups:
        for connection in group.get("connections", []):
            contracts = connection.get("contracts", [])
            for contract in contracts:
                if contract.get("type") == "electricity":
                    _save_config("connection_uuid", connection["uuid"])
                    _save_config("address_uuid", group.get("uuid", ""))
                    logger.info("Zonneplan connection configured: %s", connection["uuid"])
                    return {
                        "connection_uuid": connection["uuid"],
                        "address_uuid": group.get("uuid", ""),
                        "contracts": [c.get("type") for c in contracts],
                    }

    raise RuntimeError("No electricity connection found in Zonneplan account")


def disconnect():
    """Clear all Zonneplan credentials."""
    for key in ["email", "auth_uuid", "auth_status", "access_token",
                "refresh_token", "token_expires_at", "connection_uuid", "address_uuid"]:
        _save_config(key, "")


if __name__ == "__main__":
    import sys
    import os
    import argparse
    import json as _json
    from datetime import datetime as _dt, timezone as _tz

    # Ensure we can find cs_db when run standalone
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    parser = argparse.ArgumentParser(description="Zonneplan tariff downloader")
    parser.add_argument("--debug", action="store_true", help="Enable verbose JSON debug logging")
    args = parser.parse_args()

    PROCESS_NAME = "zonneplan.py"

    def _json_log(severity: str, message: str, **extra):
        entry = {
            "tstamp": _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "severity": severity,
            "process": PROCESS_NAME,
            "message": message,
        }
        entry.update(extra)
        print(_json.dumps(entry))

    def _debug(message: str, **extra):
        if args.debug:
            _json_log("DEBUG", message, **extra)

    # Suppress default logging, use JSON output only
    logging.basicConfig(level=logging.CRITICAL)

    cs_db.init_db()

    _json_log("INFO", "Starting tariff download")

    if not is_authenticated():
        _json_log("ERROR", "Zonneplan is not authenticated. Configure via the web UI first.")
        sys.exit(1)

    _debug("Authentication verified")

    # Fetch tariffs
    _debug("Fetching tariff data from Zonneplan API")
    try:
        tariff_data = get_current_tariff()
    except Exception as exc:
        _json_log("ERROR", "Failed to fetch tariffs from Zonneplan", error=str(exc))
        sys.exit(1)

    forecast = tariff_data.get("forecast", [])
    current = tariff_data.get("tariff_eur_per_kwh")
    group = tariff_data.get("tariff_group")

    _debug("Tariff data downloaded", current_tariff=current, tariff_group=group, forecast_count=len(forecast))

    if args.debug:
        for entry in forecast:
            _debug("Tariff received",
                   hour=entry["hour"],
                   tariff_eur_per_kwh=entry["tariff_eur_per_kwh"],
                   tariff_group=entry.get("tariff_group"))

    # Store tariffs
    if forecast:
        result = cs_db.store_zonneplan_tariffs(forecast)

        if args.debug:
            for d in result["details"]:
                _debug("Tariff DB operation",
                       hour=d["hour"],
                       tariff_eur_per_kwh=d["tariff"],
                       tariff_group=d["group"],
                       action=d["action"])

        _json_log("INFO", "Processing completed",
                  forecast_entries=len(forecast),
                  stored=result["stored"],
                  skipped=result["skipped"])
    else:
        _json_log("WARNING", "No forecast data received from Zonneplan")
