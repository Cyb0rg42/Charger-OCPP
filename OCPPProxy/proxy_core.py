"""
Script:   proxy_core.py

Abstract:
    OCPP 1.6-J WebSocket man-in-the-middle proxy. Accepts charge point
    connections, opens a matching connection to each enabled CSMS backend,
    and forwards CALL/CALLRESULT/CALLERROR messages both ways — logging
    every message, applying per-command/per-charge-point/per-backend
    blocking rules, buffering messages while a backend is offline, and
    replaying operator-configured connector-state overrides after a
    (re)connect. Started as a background thread by ocppproxy.py; the web
    dashboard (web_app.py) drives it and receives live updates from it.

Features:
    - N:M charge-point-to-backend fan-out, one upstream connection per
      enabled backend per charge point.
    - Command blocking (block_command/is_command_blocked) with live
      CallError injection for blocked calls.
    - Backend reachability health checks with exponential reconnect
      backoff and jitter.
    - restart_listener(): rebind the OCPP listen port without restarting
      the process or the event loop that hosts it.

Usage:
    Imported by ocppproxy.py / web_app.py; not intended to be run directly.

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

import asyncio
import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import websockets

import proxy_db

logger = logging.getLogger("ocpp_proxy")

# Defaults (overridden by DB config at runtime)
LISTEN_HOST = os.environ.get("PROXY_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("PROXY_LISTEN_PORT", "9100"))

# Backend reconnect backoff (seconds). The delay grows exponentially with
# random jitter so that many charge points do not reconnect in lockstep and
# we avoid hammering a CSMS that is rate-limiting us (HTTP 429).
RECONNECT_BACKOFF_INITIAL = 5
RECONNECT_BACKOFF_MAX = 300  # 5 minutes
RECONNECT_BACKOFF_FACTOR = 2.0
RECONNECT_BACKOFF_JITTER = 0.3  # ±30%

# Active connections: cp_id → { "cp_ws": ws, "upstream": [ ws, … ],
#                               "connected_at": iso, "backends": [name, …] }
connections: dict[str, dict] = {}

# Backend (CSMS) reachability cache: backend url → { "online": bool|None,
#                                    "checked_at": iso, "last_change": iso }
backend_health: dict[str, dict] = {}

# How often (seconds) to probe configured backends for reachability
HEALTHCHECK_INTERVAL = 15

# Server handle — needed for restart
_ws_server = None
_proxy_loop: asyncio.AbstractEventLoop | None = None

# Callback for emitting messages to the web UI (set by web_app at startup)
_emit_callback = None


def set_emit_callback(fn):
    global _emit_callback
    _emit_callback = fn


def _emit(event, data):
    if _emit_callback:
        try:
            _emit_callback(event, data)
        except Exception:
            pass


# ── OCPP message parsing ───────────────────────────────────────

# OCPP 1.6 JSON message types
CALL = 2
CALL_RESULT = 3
CALL_ERROR = 4

OCPP_ACTIONS = [
    "Authorize", "BootNotification", "ChangeAvailability",
    "ChangeConfiguration", "ClearCache", "DataTransfer",
    "DiagnosticsStatusNotification", "FirmwareStatusNotification",
    "GetConfiguration", "Heartbeat", "MeterValues",
    "RemoteStartTransaction", "RemoteStopTransaction", "Reset",
    "StartTransaction", "StatusNotification", "StopTransaction",
    "TriggerMessage", "UnlockConnector",
]


def parse_ocpp_message(raw):
    """Parse a raw OCPP JSON message. Returns (msg_type, unique_id, action_or_none, payload, parsed_list)."""
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None, None, None, None, None

    if not isinstance(msg, list) or len(msg) < 3:
        return None, None, None, None, None

    msg_type = msg[0]
    unique_id = msg[1]

    if msg_type == CALL:
        action = msg[2]
        payload = msg[3] if len(msg) > 3 else {}
        return msg_type, unique_id, action, payload, msg
    elif msg_type == CALL_RESULT:
        payload = msg[2] if len(msg) > 2 else {}
        return msg_type, unique_id, None, payload, msg
    elif msg_type == CALL_ERROR:
        return msg_type, unique_id, None, msg[2:], msg
    return None, None, None, None, None


def _type_label(msg_type):
    return {CALL: "Call", CALL_RESULT: "CallResult", CALL_ERROR: "CallError"}.get(msg_type, "Unknown")


# ── Pending call tracker (maps unique_id → action for CallResult matching) ──

_pending_calls: dict[str, str] = {}

# Maps unique_id → event_log row id for pairing OCPP request with response
_pending_event_ids: dict[str, int] = {}

# UIDs injected by the proxy (e.g. via send_to_cp) whose CallResult
# must NOT be forwarded upstream to the backend.
_proxy_injected_uids: set[str] = set()

# UIDs of messages the proxy injected toward a backend (e.g. a
# StatusNotification from Connector Control). The backend's CallResult for
# these must NOT be forwarded to the charge point.
_proxy_injected_backend_uids: set[str] = set()


# ── BootNotification modifier ──────────────────────────────────

def _apply_boot_override(cp_id, payload, backend_name=None):
    """Modify BootNotification payload fields if an override exists for this cp_id."""
    override = proxy_db.get_boot_override(cp_id)
    if not override:
        return payload, False

    # Honour per-CSMS targeting: only rewrite for the configured backend(s).
    if not proxy_db.backend_targeted(override.get("backends"), backend_name):
        return payload, False

    modified = False
    field_map = {
        "charge_point_vendor": ["chargePointVendor", "charge_point_vendor"],
        "charge_point_model": ["chargePointModel", "charge_point_model"],
        "charge_point_serial_number": ["chargePointSerialNumber", "charge_point_serial_number", "serialNumber"],
        "firmware_version": ["firmwareVersion", "firmware_version"],
        "meter_type": ["meterType", "meter_type"],
    }

    for db_field, ocpp_keys in field_map.items():
        override_val = override.get(db_field, "")
        if not override_val:
            continue
        for key in ocpp_keys:
            if key in payload:
                if payload[key] != override_val:
                    payload[key] = override_val
                    modified = True
                break
        else:
            # Field not in payload yet — use the first OCPP key variant
            payload[ocpp_keys[0]] = override_val
            modified = True

    return payload, modified


def _apply_revert_override(cp_id, payload, backend_name=None, already_modified=False):
    """Restore a persisted Reserved/Unavailable connector state after a session.

    When an operator has configured a connector as Reserved or Unavailable and a
    charging session later runs on it, the charge point reports its real progress
    (Preparing → Charging → Finishing → Available). Once the session ends the CP
    reports ``Available``, which would wrongly clear the out-of-service state.

    For connectors persisted as Reserved/Unavailable, this rewrites an incoming
    ``Available`` (or ``Finishing``) status back to the configured state so the
    CSMS keeps showing it as out of service. Mid-session statuses are left
    untouched so the live session remains visible.

    The revert is only applied for the backend(s) the connector state targets
    (``backend_name`` identifies the backend this forwarding task serves).

    Returns (payload, modified).
    """
    connector_id = payload.get("connectorId")
    if connector_id is None:
        return payload, already_modified

    # Check specific connector first, then fall back to connector 0 (whole CP).
    configured = proxy_db.get_connector_status(cp_id, connector_id)
    target_cid = connector_id
    if not configured and connector_id != 0:
        configured = proxy_db.get_connector_status(cp_id, 0)
        target_cid = 0
    if configured not in ("Reserved", "Unavailable"):
        return payload, already_modified

    # Respect per-CSMS targeting: only revert toward targeted backends.
    targets = proxy_db.get_connector_backends(cp_id, target_cid)
    if not proxy_db.backend_targeted(targets, backend_name):
        return payload, already_modified

    current_status = payload.get("status", "")
    # Only restore once the session has wound down (connector idle again).
    if current_status in ("Available", "Finishing") and current_status != configured:
        payload["status"] = configured
        return payload, True

    return payload, already_modified


# ── Build a CallError response ──────────────────────────────────

def _make_call_error(unique_id, code="SecurityError", description="Command blocked by proxy"):
    return json.dumps([CALL_ERROR, unique_id, code, description, {}])


# ── Main proxy logic per charge point ──────────────────────────

async def _cp_reader_fanout(cp_id, cp_ws, queues):
    """Read from charge point websocket and distribute each message to all backend queues."""
    try:
        async for raw in cp_ws:
            for q in queues:
                await q.put(raw)
    except websockets.exceptions.ConnectionClosed:
        logger.info("CP %s disconnected (reader)", cp_id)
    except Exception as e:
        logger.error("Error reading from CP %s: %s", cp_id, e)
    finally:
        for q in queues:
            await q.put(None)  # sentinel to stop consumers


def _parse_retry_after(exc):
    """Return the server-requested Retry-After delay in seconds, or None.

    Handles both the delta-seconds form (e.g. "120") and the HTTP-date form.
    """
    headers = getattr(exc, "headers", None)
    if not headers:
        return None
    try:
        value = headers.get("Retry-After")
    except Exception:
        value = None
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        delta = (when - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except (TypeError, ValueError):
        return None


def _next_backoff(current, exc=None):
    """Compute the next reconnect delay with exponential growth + jitter.

    If the backend sent a Retry-After header (typically with HTTP 429), honour
    it as a floor so we wait at least as long as the server asked.
    """
    base = min(current * RECONNECT_BACKOFF_FACTOR, RECONNECT_BACKOFF_MAX)
    jitter = base * RECONNECT_BACKOFF_JITTER
    delay = base + random.uniform(-jitter, jitter)
    delay = max(RECONNECT_BACKOFF_INITIAL, delay)
    retry_after = _parse_retry_after(exc) if exc is not None else None
    if retry_after is not None:
        delay = max(delay, retry_after)
    return min(delay, RECONNECT_BACKOFF_MAX)


async def _reconnect_and_forward(cp_id, cp_ws, backend, queue):
    """Queue messages for an offline backend, reconnect when available, replay with 1s delay, then forward normally."""
    backend_name = backend["name"]
    url = backend["url"].rstrip("/")
    upstream_url = f"{url}/{cp_id}"
    offline_buffer = []

    logger.info("Backend '%s' unreachable for %s — queuing messages for replay", backend_name, cp_id)
    _emit("client_event", {"event": "backend_offline", "cp_id": cp_id, "backend": backend_name})

    upstream_ws = None
    backoff = RECONNECT_BACKOFF_INITIAL
    while upstream_ws is None:
        # Try connecting
        try:
            upstream_ws = await websockets.connect(upstream_url, subprotocols=["ocpp1.6"])
            logger.info("Reconnected to backend '%s' (%s) for %s — replaying %d queued messages",
                        backend_name, upstream_url, cp_id, len(offline_buffer))
            break
        except Exception as e:
            status = getattr(e, "status_code", None)
            delay = _next_backoff(backoff, e)
            backoff = delay
            if status == 429:
                logger.warning(
                    "Backend '%s' rate-limited (HTTP 429) for %s — backing off %.0fs",
                    backend_name, cp_id, delay)
            else:
                logger.info(
                    "Backend '%s' still unreachable for %s (%s) — retrying in %.0fs",
                    backend_name, cp_id, status or type(e).__name__, delay)

        # Buffer messages during the backoff window before retrying connection
        deadline = asyncio.get_event_loop().time() + delay
        while True:
            remaining = max(0, deadline - asyncio.get_event_loop().time())
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(queue.get(), timeout=remaining)
                if raw is None:
                    logger.info("CP %s disconnected while backend '%s' offline — discarding %d buffered messages",
                                cp_id, backend_name, len(offline_buffer))
                    return
                offline_buffer.append(raw)
            except asyncio.TimeoutError:
                break

    _emit("client_event", {"event": "backend_reconnected", "cp_id": cp_id, "backend": backend_name})

    # Update connection record
    conn_info = connections.get(cp_id)
    if conn_info:
        conn_info["upstream"].append(upstream_ws)

    # Replay buffered messages with 1-second delay
    for raw in offline_buffer:
        msg_type, uid, action, payload, parsed = parse_ocpp_message(raw)

        # Skip blocked commands
        if msg_type == CALL and action and proxy_db.is_command_blocked(action, cp_id, backend_name):
            continue

        # Apply overrides
        if msg_type == CALL and action == "BootNotification" and parsed:
            payload, modified = _apply_boot_override(cp_id, payload, backend_name)
            if modified:
                parsed[3] = payload
                raw = json.dumps(parsed)
        if msg_type == CALL and action == "StatusNotification" and parsed:
            payload, modified = _apply_revert_override(cp_id, payload, backend_name)
            if modified:
                parsed[3] = payload
                raw = json.dumps(parsed)

        try:
            await upstream_ws.send(raw)
        except websockets.exceptions.ConnectionClosed:
            logger.error("Backend '%s' disconnected during replay for %s", backend_name, cp_id)
            return
        await asyncio.sleep(1)

    logger.info("Replay complete for backend '%s' (%s) — starting normal forwarding", backend_name, cp_id)

    # Start normal bidirectional forwarding
    upstream_task = asyncio.create_task(
        _forward_upstream_to_cp(cp_id, cp_ws, upstream_ws, backend_name=backend_name)
    )

    try:
        await _forward_cp_to_upstream(cp_id, cp_ws, upstream_ws, backend_name=backend_name, queue=queue)
    finally:
        upstream_task.cancel()
        try:
            await upstream_task
        except asyncio.CancelledError:
            pass
        try:
            await upstream_ws.close()
        except Exception:
            pass


async def _forward_cp_to_upstream(cp_id, cp_ws, upstream_ws, backend_name="", queue=None):
    """Forward messages from charge point to upstream central system."""
    label_suffix = f" [{backend_name}]" if backend_name else ""

    async def _message_iter():
        if queue is not None:
            while True:
                raw = await queue.get()
                if raw is None:
                    break
                yield raw
        else:
            async for raw in cp_ws:
                yield raw

    try:
        async for raw in _message_iter():
            msg_type, uid, action, payload, parsed = parse_ocpp_message(raw)

            if msg_type is None:
                # Not parseable — forward as-is
                await upstream_ws.send(raw)
                continue

            type_label = _type_label(msg_type)

            # Drop CallResult/CallError for UIDs that were injected by the
            # proxy itself (e.g. ChangeAvailability via Connector Control) —
            # the backend never sent the original Call, so forwarding the
            # response would confuse its OCPP state machine.
            if msg_type in (CALL_RESULT, CALL_ERROR) and uid in _proxy_injected_uids:
                _proxy_injected_uids.discard(uid)
                resolved_action = _pending_calls.pop(uid, "")
                ts = datetime.now(timezone.utc).isoformat()
                # Still log & show on dashboard
                proxy_db.log_message(cp_id, "CP→Proxy", type_label, resolved_action, payload)
                _emit("ocpp_message", {
                    "timestamp": ts,
                    "cp_id": cp_id,
                    "unique_id": uid,
                    "msg_type_id": msg_type,
                    "direction": "CP→Proxy",
                    "source": cp_id,
                    "target": "Proxy",
                    "type": type_label,
                    "action": resolved_action,
                    "payload": payload,
                    "blocked": False,
                    "modified": False,
                    "backend_name": "",
                })
                logger.info("Consumed proxy-injected %s response for %s (uid=%s)", resolved_action, cp_id, uid)
                continue

            # Track Call UIDs so we can label CallResults later
            if msg_type == CALL and action:
                _pending_calls[uid] = action

            # Capture reported equipment info for the home dashboard
            if msg_type == CALL and action == "BootNotification" and isinstance(payload, dict):
                info = connections.get(cp_id)
                if info is not None:
                    info["vendor"] = payload.get("chargePointVendor", "")
                    info["model"] = payload.get("chargePointModel", "")
                    info["firmware"] = payload.get("firmwareVersion", "")
                    info["serial"] = payload.get("chargePointSerialNumber", "")
                    info["meter_type"] = payload.get("meterType", "")

            resolved_action = action or _pending_calls.get(uid, "")
            ts = datetime.now(timezone.utc).isoformat()

            # Log
            proxy_db.log_message(cp_id, "CP→CS" + label_suffix, type_label, resolved_action, payload)
            if msg_type == CALL:
                proxy_db.record_command(resolved_action or type_label, "CP→CS", cp_id)

            # Leg 1: Station → Proxy (incoming)
            _emit("ocpp_message", {
                "timestamp": ts,
                "cp_id": cp_id,
                "unique_id": uid,
                "msg_type_id": msg_type,
                "direction": "CP→CS" + label_suffix,
                "source": cp_id,
                "target": "Proxy",
                "type": type_label,
                "action": resolved_action,
                "payload": payload,
                "blocked": False,
                "modified": False,
                "backend_name": backend_name,
            })

            # Check blocking
            if msg_type == CALL and action and proxy_db.is_command_blocked(action, cp_id, backend_name):
                logger.info("BLOCKED %s from %s (backend=%s)", action, cp_id, backend_name or "-")
                proxy_db.record_command(action, "CP→CS", cp_id, was_blocked=True)
                proxy_db.log_message(cp_id, "CP→CS", type_label, action, payload, blocked=True)
                # Leg 2: Proxy ✕ Backend (blocked)
                _emit("ocpp_message", {
                    "timestamp": ts,
                    "cp_id": cp_id,
                    "unique_id": uid,
                    "msg_type_id": msg_type,
                    "direction": "BLOCKED",
                    "source": "Proxy",
                    "target": backend_name or "Backend",
                    "type": type_label,
                    "action": action,
                    "payload": payload,
                    "blocked": True,
                    "modified": False,
                    "backend_name": backend_name,
                })
                # Only answer the charge point with a CallError when the command
                # is blocked for *every* connected backend; otherwise another
                # backend will still respond and the CP must get a single reply.
                cp_conn = connections.get(cp_id) or {}
                all_backends = cp_conn.get("backends", [])
                blocked_for_all = all(
                    proxy_db.is_command_blocked(action, cp_id, b) for b in all_backends
                ) if all_backends else True
                if blocked_for_all:
                    err = _make_call_error(uid)
                    await cp_ws.send(err)
                continue

            # Apply BootNotification override
            was_modified = False
            if msg_type == CALL and action == "BootNotification":
                payload, was_modified = _apply_boot_override(cp_id, payload, backend_name)
                if was_modified:
                    parsed[3] = payload
                    raw = json.dumps(parsed)
                    logger.info("MODIFIED BootNotification for %s", cp_id)
                    proxy_db.log_message(cp_id, "CP→CS", type_label, action, payload, modified=True)

            # Restore a persisted Reserved/Unavailable state once a charging
            # session ends (the CP would otherwise report it back as Available).
            if msg_type == CALL and action == "StatusNotification":
                payload, was_modified = _apply_revert_override(
                    cp_id, payload, backend_name, was_modified
                )
                if was_modified:
                    parsed[3] = payload
                    raw = json.dumps(parsed)
                    logger.info("RESTORED connector state for %s → %s",
                                cp_id, payload.get("status"))
                    proxy_db.log_message(cp_id, "CP→CS", type_label, action, payload, modified=True)

            # Leg 2: Proxy → Backend (outgoing)
            _emit("ocpp_message", {
                "timestamp": ts,
                "cp_id": cp_id,
                "unique_id": uid,
                "msg_type_id": msg_type,
                "direction": "CP→CS" + label_suffix,
                "source": "Proxy",
                "target": backend_name or "Backend",
                "type": type_label,
                "action": resolved_action,
                "payload": payload,
                "blocked": False,
                "modified": was_modified,
                "backend_name": backend_name,
            })

            await upstream_ws.send(raw)

            # Event logging: log CALL as new event, pair CallResult/CallError with pending event
            if msg_type == CALL and action and action != "Heartbeat":
                eid = proxy_db.log_event(
                    cp_id, "INFO",
                    f"{action} connector={payload.get('connectorId', '-')}" if isinstance(payload, dict) else action,
                    ocpp_command=action, direction="CP→CS", backend_name=backend_name,
                    ocpp_payload=raw,
                )
                _pending_event_ids[uid] = eid
            elif msg_type in (CALL_RESULT, CALL_ERROR) and uid in _pending_event_ids:
                proxy_db.update_event_response(_pending_event_ids.pop(uid), raw)

    except websockets.exceptions.ConnectionClosed:
        logger.info("CP %s disconnected", cp_id)
    except Exception as e:
        logger.error("Error forwarding CP→CS for %s: %s", cp_id, e)


async def _forward_upstream_to_cp(cp_id, cp_ws, upstream_ws, backend_name=""):
    """Forward messages from upstream central system to charge point."""
    label_suffix = f" [{backend_name}]" if backend_name else ""
    try:
        async for raw in upstream_ws:
            msg_type, uid, action, payload, parsed = parse_ocpp_message(raw)

            if msg_type is None:
                await cp_ws.send(raw)
                continue

            type_label = _type_label(msg_type)

            # Drop CallResult/CallError for StatusNotifications the proxy
            # injected toward this backend (Connector Control). The charge
            # point never sent the original Call, so forwarding the response
            # would confuse its OCPP state machine.
            if msg_type in (CALL_RESULT, CALL_ERROR) and uid in _proxy_injected_backend_uids:
                _proxy_injected_backend_uids.discard(uid)
                resolved_action = _pending_calls.pop(uid, "StatusNotification")
                ts = datetime.now(timezone.utc).isoformat()
                proxy_db.log_message(cp_id, "CS→Proxy" + label_suffix, type_label, resolved_action, payload)
                _emit("ocpp_message", {
                    "timestamp": ts,
                    "cp_id": cp_id,
                    "unique_id": uid,
                    "msg_type_id": msg_type,
                    "direction": "CS→Proxy" + label_suffix,
                    "source": backend_name or "Backend",
                    "target": "Proxy",
                    "type": type_label,
                    "action": resolved_action,
                    "payload": payload,
                    "blocked": False,
                    "modified": False,
                    "backend_name": backend_name,
                })
                continue

            if msg_type == CALL and action:
                _pending_calls[uid] = action

            resolved_action = action or _pending_calls.get(uid, "")

            # Clean up pending call if this is a response
            if msg_type in (CALL_RESULT, CALL_ERROR):
                _pending_calls.pop(uid, None)

            ts = datetime.now(timezone.utc).isoformat()

            proxy_db.log_message(cp_id, "CS→CP" + label_suffix, type_label, resolved_action, payload)
            if msg_type == CALL:
                proxy_db.record_command(resolved_action or type_label, "CS→CP", cp_id)

            # Leg 1: Backend → Proxy (incoming)
            _emit("ocpp_message", {
                "timestamp": ts,
                "cp_id": cp_id,
                "unique_id": uid,
                "msg_type_id": msg_type,
                "direction": "CS→CP" + label_suffix,
                "source": backend_name or "Backend",
                "target": "Proxy",
                "type": type_label,
                "action": resolved_action,
                "payload": payload,
                "blocked": False,
                "modified": False,
                "backend_name": backend_name,
            })

            # Check blocking for CS→CP calls
            if msg_type == CALL and action and proxy_db.is_command_blocked(action, cp_id, backend_name):
                logger.info("BLOCKED %s to %s (backend=%s)", action, cp_id, backend_name or "-")
                proxy_db.record_command(action, "CS→CP", cp_id, was_blocked=True)
                proxy_db.log_message(cp_id, "CS→CP", type_label, action, payload, blocked=True)
                # Leg 2: Proxy ✕ Station (blocked)
                _emit("ocpp_message", {
                    "timestamp": ts,
                    "cp_id": cp_id,
                    "unique_id": uid,
                    "msg_type_id": msg_type,
                    "direction": "BLOCKED",
                    "source": "Proxy",
                    "target": cp_id,
                    "type": type_label,
                    "action": action,
                    "payload": payload,
                    "blocked": True,
                    "modified": False,
                    "backend_name": backend_name,
                })
                err = _make_call_error(uid)
                await upstream_ws.send(err)
                continue

            # Leg 2: Proxy → Station (outgoing)
            _emit("ocpp_message", {
                "timestamp": ts,
                "cp_id": cp_id,
                "unique_id": uid,
                "msg_type_id": msg_type,
                "direction": "CS→CP" + label_suffix,
                "source": "Proxy",
                "target": cp_id,
                "type": type_label,
                "action": resolved_action,
                "payload": payload,
                "blocked": False,
                "modified": False,
                "backend_name": backend_name,
            })

            await cp_ws.send(raw)

            # Event logging: log CS→CP CALL as new event, pair responses with pending event
            if msg_type == CALL and action and action != "Heartbeat":
                eid = proxy_db.log_event(
                    cp_id, "INFO",
                    f"{action} from backend",
                    ocpp_command=action, direction="CS→CP", backend_name=backend_name,
                    ocpp_payload=raw,
                )
                _pending_event_ids[uid] = eid
            elif msg_type in (CALL_RESULT, CALL_ERROR) and uid in _pending_event_ids:
                proxy_db.update_event_response(_pending_event_ids.pop(uid), raw)

    except websockets.exceptions.ConnectionClosed:
        logger.info("Upstream closed for %s", cp_id)
    except Exception as e:
        logger.error("Error forwarding CS→CP for %s: %s", cp_id, e)


async def _connect_backends(cp_id):
    """Connect to all enabled backend servers for a charge point.

    Returns (connected, failed) where connected is a list of (backend_row, ws)
    and failed is a list of backend_rows that could not be reached.
    """
    backends = proxy_db.get_enabled_backends()
    connected = []
    failed = []
    for b in backends:
        url = b["url"].rstrip("/")
        # Append /cp_id if the URL ends with /ocpp or similar path
        upstream_url = f"{url}/{cp_id}"
        try:
            ws = await websockets.connect(upstream_url, subprotocols=["ocpp1.6"])
            connected.append((b, ws))
            logger.info("Connected to backend '%s' (%s) for %s", b["name"], upstream_url, cp_id)
        except Exception as e:
            logger.error("Failed to connect to backend '%s' (%s) for %s: %s", b["name"], upstream_url, cp_id, e)
            failed.append(b)
    return connected, failed


async def handle_charge_point(websocket):
    """Handle a new charge point WebSocket connection."""
    # Extract CP id from URL path: /ocpp/<cp_id>
    try:
        path = websocket.request.path
    except AttributeError:
        path = getattr(websocket, "path", "") or ""

    parts = [p for p in path.split("/") if p]
    cp_id = parts[-1] if len(parts) >= 2 else "UNKNOWN"

    logger.info("Charge point %s connecting via proxy", cp_id)
    _emit("client_event", {"event": "connected", "cp_id": cp_id})

    # Connect to all enabled upstream backends
    connected, failed = await _connect_backends(cp_id)
    if not connected and not failed:
        logger.error("No backends configured for %s — dropping connection", cp_id)
        return
    if not connected:
        logger.error("No backends available for %s — dropping connection", cp_id)
        return

    upstream_ws_list = [ws for _, ws in connected]
    connections[cp_id] = {
        "cp_ws": websocket,
        "upstream": upstream_ws_list,
        "connected_at": datetime.now(timezone.utc).isoformat(),
        "backends": [b["name"] for b, _ in connected],
    }
    _emit("connections_update", {"clients": list(connections.keys())})

    # Replay any persisted operator-configured connector states so they are
    # reflected in the CSMS again after a (re)connection / proxy restart.
    asyncio.create_task(_replay_connector_states(cp_id))

    queues = []
    tasks = []
    try:
        # For each connected backend, create a queue and forwarding tasks
        for b, u_ws in connected:
            name = b["name"]
            q = asyncio.Queue()
            queues.append(q)
            tasks.append(asyncio.create_task(
                _forward_cp_to_upstream(cp_id, websocket, u_ws, backend_name=name, queue=q)
            ))
            tasks.append(asyncio.create_task(
                _forward_upstream_to_cp(cp_id, websocket, u_ws, backend_name=name)
            ))

        # For each failed backend, create a queue and a reconnect task
        for b in failed:
            q = asyncio.Queue()
            queues.append(q)
            tasks.append(asyncio.create_task(
                _reconnect_and_forward(cp_id, websocket, b, q)
            ))

        # Single reader fans out CP messages to all backend queues
        tasks.append(asyncio.create_task(
            _cp_reader_fanout(cp_id, websocket, queues)
        ))

        # Wait for the CP websocket to close (first task finishing means CP disconnected)
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except Exception as e:
        logger.error("Proxy error for %s: %s", cp_id, e)
    finally:
        connections.pop(cp_id, None)
        _emit("client_event", {"event": "disconnected", "cp_id": cp_id})
        _emit("connections_update", {"clients": list(connections.keys())})

        # Close all upstream sockets
        for _, u_ws in connected:
            try:
                await u_ws.close()
            except Exception:
                pass

        logger.info("Charge point %s proxy session ended", cp_id)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

async def _close_all_connections():
    """Gracefully close all active proxy connections."""
    for cp_id in list(connections.keys()):
        info = connections.pop(cp_id, None)
        if not info:
            continue
        try:
            await info["cp_ws"].close()
        except Exception:
            pass
        for u_ws in info.get("upstream", []):
            try:
                await u_ws.close()
            except Exception:
                pass
    logger.info("All proxy connections closed")
    _emit("connections_update", {"clients": []})


async def restart_listener(new_port=None):
    """Stop the current WS server and start listening on *new_port* (or the DB port).

    Each step is bounded by its own timeout so a stuck close/bind fails fast
    with a clear error instead of silently leaving no listener bound at all
    (which is what used to happen when the outer 10s RPC timeout in
    web_app._run_proxy_coro fired while this coroutine was still stuck).
    """
    global _ws_server

    # Close existing server + connections
    if _ws_server is not None:
        old_server, _ws_server = _ws_server, None
        old_server.close()
        try:
            await asyncio.wait_for(old_server.wait_closed(), timeout=5)
        except asyncio.TimeoutError:
            raise RuntimeError("timed out closing the previous OCPP listener")
        logger.info("Old WS listener closed")

    try:
        await asyncio.wait_for(_close_all_connections(), timeout=5)
    except asyncio.TimeoutError:
        raise RuntimeError("timed out closing existing proxy connections")

    port = int(new_port or proxy_db.get_listen_port())
    logger.info("Starting WS listener on ws://%s:%s/ocpp", LISTEN_HOST, port)
    try:
        _ws_server = await asyncio.wait_for(
            websockets.serve(
                handle_charge_point,
                LISTEN_HOST,
                port,
                subprotocols=["ocpp1.6"],
            ),
            timeout=5,
        )
    except asyncio.TimeoutError:
        raise RuntimeError(f"timed out binding OCPP listener to port {port}")
    logger.info("WS listener bound to ws://%s:%s/ocpp", LISTEN_HOST, port)
    _emit("client_event", {"event": "listener_restart", "port": port})


async def reconnect_all_backends():
    """Disconnect all charge points and reconnect them to the (possibly changed) backends."""
    # Closing CP websockets will cause `handle_charge_point` to end.
    # The charge points themselves will reconnect automatically (OCPP spec),
    # and the new `handle_charge_point` call will read fresh backends from DB.
    await _close_all_connections()
    if _ws_server is not None:
        logger.info("Backends changed — waiting for charge points to reconnect")
        _emit("client_event", {"event": "backends_changed"})


async def start_proxy():
    """Start the OCPP WebSocket proxy server."""
    global _ws_server, _proxy_loop

    proxy_db.init_db()
    _proxy_loop = asyncio.get_event_loop()

    port = int(proxy_db.get_listen_port())
    logger.info("OCPP Proxy listening on ws://%s:%s/ocpp", LISTEN_HOST, port)
    _ws_server = await websockets.serve(
        handle_charge_point,
        LISTEN_HOST,
        port,
        subprotocols=["ocpp1.6"],
    )
    asyncio.create_task(_backend_health_loop())
    # Deliberately does NOT await _ws_server.wait_closed() here: this
    # coroutine's completion only starts the health-check task and returns —
    # the event loop itself is kept alive by _run_ws_proxy()'s loop.run_forever()
    # (see ocppproxy.py). If this awaited wait_closed() instead, closing
    # _ws_server from restart_listener() would resolve this await too,
    # finishing start_proxy() and stopping the whole loop out from under any
    # in-flight restart — which is exactly the bug that used to make
    # restart_listener() hang forever after a listener restart.


def get_proxy_loop():
    """Return the asyncio event loop running the proxy, for cross-thread scheduling."""
    return _proxy_loop


# ---------------------------------------------------------------------------
# Backend (CSMS) health checks + home dashboard overview
# ---------------------------------------------------------------------------

async def _probe_backend(url):
    """Return True if the backend (CSMS) endpoint is reachable.

    This performs a plain TCP connection to the backend host/port. A successful
    connection means the CSMS is accepting traffic. We deliberately do NOT open
    an OCPP WebSocket session here: doing so would make the CSMS register a
    phantom charge point (named after the URL's last path segment) on every
    probe. Only connection-level failures (refused / timeout / DNS) are treated
    as offline.
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return False
    port = parsed.port or (443 if parsed.scheme in ("wss", "https") else 80)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=5,
        )
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except (asyncio.TimeoutError, OSError):
        return False
    except Exception:
        return False


async def _backend_health_loop():
    """Periodically probe configured backends and cache their reachability."""
    while True:
        try:
            now = datetime.now(timezone.utc).isoformat()
            for b in proxy_db.get_backends():
                url = b["url"]
                online = await _probe_backend(url) if b.get("enabled") else None
                prev = backend_health.get(url, {})
                changed = prev.get("online") != online or "last_change" not in prev
                backend_health[url] = {
                    "online": online,
                    "checked_at": now,
                    "last_change": now if changed else prev.get("last_change", now),
                }
            _emit("overview_update", get_overview())
        except Exception as e:
            logger.error("Backend health check error: %s", e)
        await asyncio.sleep(HEALTHCHECK_INTERVAL)


def get_overview():
    """Build the home-dashboard overview of charge points and CSMS backends."""
    charge_points = []
    for cp_id, info in connections.items():
        try:
            recent = proxy_db.get_recent_messages(limit=1, cp_id=cp_id)
        except Exception:
            recent = []
        last_msg = recent[0] if recent else {}
        try:
            connectors = proxy_db.get_connector_statuses(cp_id)
        except Exception:
            connectors = []

        # Equipment info: prefer the live BootNotification capture, fall back
        # to the most recent BootNotification logged in the database.
        vendor = info.get("vendor")
        model = info.get("model")
        meter_type = info.get("meter_type")
        if not vendor and not model and not meter_type:
            try:
                boot = proxy_db.get_last_boot_info(cp_id)
            except Exception:
                boot = None
            if boot:
                vendor = vendor or boot.get("vendor")
                model = model or boot.get("model")
                meter_type = meter_type or boot.get("meter")

        charge_points.append({
            "cp_id": cp_id,
            "connected_at": info.get("connected_at"),
            "vendor": vendor or "",
            "model": model or "",
            "meter_type": meter_type or "",
            "backends": info.get("backends", []),
            "backend_count": len(info.get("upstream", [])),
            "last_action": last_msg.get("action"),
            "last_seen": last_msg.get("timestamp"),
            "connectors": connectors,
        })
    charge_points.sort(key=lambda c: c["cp_id"])

    backends = []
    for b in proxy_db.get_backends():
        h = backend_health.get(b["url"], {})
        active = sum(1 for info in connections.values()
                     if b["name"] in info.get("backends", []))
        backends.append({
            "id": b["id"],
            "name": b["name"],
            "url": b["url"],
            "enabled": bool(b["enabled"]),
            "online": h.get("online"),
            "checked_at": h.get("checked_at"),
            # Time the backend was first seen online (status last changed).
            "connected_since": h.get("last_change") if h.get("online") else None,
            "active_chargepoints": active,
        })

    return {
        "chargepoints": charge_points,
        "backends": backends,
        "summary": {
            "chargepoints_connected": len(charge_points),
            "backends_total": len(backends),
            "backends_online": sum(1 for b in backends if b["online"]),
            "listen_port": proxy_db.get_listen_port(),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def send_to_cp(cp_id, action, payload):
    """Inject an OCPP Call message to a connected charge point and emit dashboard events."""
    cp_info = connections.get(cp_id)
    if not cp_info or not cp_info["cp_ws"]:
        raise ValueError(f"Charge point '{cp_id}' is not connected")

    unique_id = str(uuid.uuid4())
    raw = json.dumps([CALL, unique_id, action, payload])
    ts = datetime.now(timezone.utc).isoformat()

    # Leg 1: Proxy → Station
    _emit("ocpp_message", {
        "timestamp": ts,
        "cp_id": cp_id,
        "unique_id": unique_id,
        "msg_type_id": CALL,
        "direction": "CS→CP",
        "source": "Proxy",
        "target": cp_id,
        "type": "Call",
        "action": action,
        "payload": payload,
        "blocked": False,
        "modified": False,
        "backend_name": "",
    })

    proxy_db.log_message(cp_id, "CS→CP (proxy)", "Call", action, payload)
    proxy_db.record_command(action, "CS→CP", cp_id)

    # Track this UID so the CallResult from the station is NOT forwarded
    # to the backend (the backend never sent this Call).
    _proxy_injected_uids.add(unique_id)

    await cp_info["cp_ws"].send(raw)
    logger.info("Sent %s to %s: %s", action, cp_id, payload)
    return unique_id


async def send_status_to_backends(cp_id, connector_id, status, backends="*", persist=True):
    """Inject a StatusNotification toward selected upstream backends of a CP.

    Used by Connector Control so the CSMS reflects a chosen connector status
    without depending on the charge point echoing a StatusNotification back
    (it only does so when its own availability actually changes).

    `backends` selects which CSMS backend(s) receive the update ('*' = all,
    otherwise a comma-separated list / list of backend names).

    When ``persist`` is True the chosen state is stored in the database (so it
    survives a proxy restart and can be replayed) and recorded in the Connector
    Control history. Replays on (re)connection pass ``persist=False``.
    """
    cp_info = connections.get(cp_id)
    if not cp_info or not cp_info.get("upstream"):
        raise ValueError(f"Charge point '{cp_id}' is not connected")

    connector_id = int(connector_id)
    backends = proxy_db.normalize_backends(backends)

    if persist:
        # Persist the operator-configured state and log it for the history panel.
        proxy_db.set_connector_status(cp_id, connector_id, status, backends)
        proxy_db.log_connector_state(cp_id, connector_id, status, backends)

    payload = {
        "connectorId": int(connector_id),
        "errorCode": "NoError",
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    names = cp_info.get("backends", [])
    sent = 0
    for idx, upstream_ws in enumerate(cp_info["upstream"]):
        backend_name = names[idx] if idx < len(names) else ""
        # Skip backends not targeted by this update.
        if not proxy_db.backend_targeted(backends, backend_name):
            continue
        unique_id = str(uuid.uuid4())
        raw = json.dumps([CALL, unique_id, "StatusNotification", payload])
        ts = datetime.now(timezone.utc).isoformat()

        # The backend will reply with a CallResult for this UID; it must not be
        # forwarded to the charge point (which never sent the original Call).
        _proxy_injected_backend_uids.add(unique_id)

        proxy_db.log_message(cp_id, "CP→CS (proxy)", "Call", "StatusNotification", payload)
        proxy_db.record_command("StatusNotification", "CP→CS", cp_id)
        _emit("ocpp_message", {
            "timestamp": ts,
            "cp_id": cp_id,
            "unique_id": unique_id,
            "msg_type_id": CALL,
            "direction": "CP→CS (proxy)",
            "source": "Proxy",
            "target": backend_name or "Backend",
            "type": "Call",
            "action": "StatusNotification",
            "payload": payload,
            "blocked": False,
            "modified": False,
            "backend_name": backend_name,
        })

        try:
            await upstream_ws.send(raw)
            sent += 1
        except Exception as e:
            logger.error("Failed to inject StatusNotification to backend '%s' for %s: %s",
                         backend_name, cp_id, e)

    logger.info("Injected StatusNotification (connector %s = %s) to %d backend(s) for %s",
                connector_id, status, sent, cp_id)
    return sent


async def _replay_connector_states(cp_id):
    """Re-send persisted operator-configured connector states to the backend(s).

    Runs shortly after a charge point (re)connects so that connector states
    configured via Connector Control survive a proxy restart and are reflected
    in the CSMS again. A small delay lets the charge point's BootNotification
    reach the backend first.
    """
    await asyncio.sleep(5)
    try:
        states = proxy_db.get_connector_statuses(cp_id)
    except Exception as e:
        logger.error("Failed to read persisted connector states for %s: %s", cp_id, e)
        return
    if not states:
        return
    if cp_id not in connections:
        return
    logger.info("Replaying %d persisted connector state(s) to backends for %s",
                len(states), cp_id)
    for s in states:
        try:
            await send_status_to_backends(
                cp_id, s["connector_id"], s["status"],
                backends=s.get("backends") or "*", persist=False,
            )
        except Exception as e:
            logger.error("Failed to replay connector %s state for %s: %s",
                         s.get("connector_id"), cp_id, e)
