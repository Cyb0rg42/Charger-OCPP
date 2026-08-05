#!/usr/bin/env python3
"""
Script:   web_app.py

Abstract:
    Flask + Socket.IO web dashboard and REST API for the OCPP Proxy.
    Serves the operator UI (connected charge points, connected backends,
    live message feed, command blocking, boot-notification and connector
    overrides, equipment database) and exposes the endpoints it calls into
    proxy_core.py for — REST commands are scheduled onto the proxy's
    asyncio event loop via _run_proxy_coro() and their results/errors
    turned into JSON responses.

Features:
    - Live updates pushed to the browser over Socket.IO
      (proxy_core.set_emit_callback wires proxy_core's events here).
    - REST endpoints for backend/blocking/override/equipment management.
    - Runs under Flask-SocketIO's threading async_mode (no eventlet).

Usage:
    Normally started via ocppproxy.py (which also starts the WS proxy).
    Can be run standalone for quick UI-only testing:
        python web_app.py
    (dashboard only — no OCPP proxy connections are accepted in this mode).

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
import concurrent.futures
import json
import logging
import secrets
import socket
from datetime import datetime, timezone

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

import proxy_db
import proxy_core

logger = logging.getLogger("ocpp_proxy.web")

app = Flask(__name__)
# Randomly generated per process start. Nothing in this app relies on the
# key staying stable across restarts (no persisted sessions/CSRF tokens),
# so a fresh random value each boot is strictly safer than a fixed one.
app.config["SECRET_KEY"] = secrets.token_hex(32)
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")


@app.context_processor
def inject_system_name():
    """Expose the host name to all templates as `system_name`."""
    return {"system_name": SYSTEM_NAME or socket.gethostname()}


# Proxy name shown in page titles. Set from the config file (--config) at
# startup; falls back to the host name when unset.
SYSTEM_NAME = None


def set_system_name(name):
    """Override the name shown in page titles (from the proxy config file)."""
    global SYSTEM_NAME
    SYSTEM_NAME = name or None


# Wire up proxy_core to emit via socketio
def _socketio_emit(event, data):
    socketio.emit(event, data, namespace="/proxy")

proxy_core.set_emit_callback(_socketio_emit)


# ── Pages ───────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("proxy_home.html")


@app.route("/dashboard")
def dashboard():
    return render_template("proxy_dashboard.html")


# ── REST API: Home overview ─────────────────────────────────────

@app.route("/api/overview")
def api_overview():
    return jsonify(proxy_core.get_overview())


# ── REST API: Connections ───────────────────────────────────────

@app.route("/api/connections")
def api_connections():
    clients = list(proxy_core.connections.keys())
    return jsonify({"clients": clients})


# ── REST API: Messages ──────────────────────────────────────────

@app.route("/api/messages")
def api_messages():
    limit = request.args.get("limit", 200, type=int)
    cp_id = request.args.get("cp_id", None)
    messages = proxy_db.get_recent_messages(limit=min(limit, 1000), cp_id=cp_id)
    return jsonify({"messages": messages})


# ── REST API: Blocked commands ──────────────────────────────────

@app.route("/api/blocked")
def api_blocked():
    return jsonify({"blocked": proxy_db.get_blocked_commands()})


@app.route("/api/blocked", methods=["POST"])
def api_block_command():
    data = request.get_json(force=True)
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "command required"}), 400
    cp_id = data.get("cp_id", "*").strip() or "*"
    reason = data.get("reason", "").strip()
    backends = data.get("backends", "*")
    proxy_db.block_command(command, cp_id, reason, backends)
    socketio.emit("blocked_update", {"blocked": proxy_db.get_blocked_commands()}, namespace="/proxy")
    return jsonify({"ok": True})


@app.route("/api/blocked", methods=["DELETE"])
def api_unblock_command():
    data = request.get_json(force=True)
    command = data.get("command", "").strip()
    if not command:
        return jsonify({"error": "command required"}), 400
    cp_id = data.get("cp_id", "*").strip() or "*"
    proxy_db.unblock_command(command, cp_id)
    socketio.emit("blocked_update", {"blocked": proxy_db.get_blocked_commands()}, namespace="/proxy")
    return jsonify({"ok": True})


# ── REST API: Boot overrides ───────────────────────────────────

@app.route("/api/overrides")
def api_overrides():
    return jsonify({"overrides": proxy_db.get_boot_overrides()})


@app.route("/api/overrides", methods=["POST"])
def api_set_override():
    data = request.get_json(force=True)
    cp_id = data.get("cp_id", "").strip()
    if not cp_id:
        return jsonify({"error": "cp_id required"}), 400
    proxy_db.upsert_boot_override(
        cp_id=cp_id,
        vendor=data.get("charge_point_vendor", "").strip(),
        model=data.get("charge_point_model", "").strip(),
        serial=data.get("charge_point_serial_number", "").strip(),
        firmware=data.get("firmware_version", "").strip(),
        meter_type=data.get("meter_type", "").strip(),
        enabled=data.get("enabled", True),
        backends=data.get("backends", "*"),
    )
    socketio.emit("overrides_update", {"overrides": proxy_db.get_boot_overrides()}, namespace="/proxy")
    return jsonify({"ok": True})


@app.route("/api/overrides/<cp_id>", methods=["DELETE"])
def api_delete_override(cp_id):
    proxy_db.delete_boot_override(cp_id)
    socketio.emit("overrides_update", {"overrides": proxy_db.get_boot_overrides()}, namespace="/proxy")
    return jsonify({"ok": True})


# ── REST API: Stats ─────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    return jsonify({"stats": proxy_db.get_command_stats()})


# ── REST API: OCPP actions list ─────────────────────────────────

@app.route("/api/ocpp-actions")
def api_ocpp_actions():
    return jsonify({"actions": proxy_core.OCPP_ACTIONS})


# ── REST API: Events (paired request/response) ─────────────────

@app.route("/api/events")
def api_events():
    cp_id = request.args.get("cp_id")
    query = request.args.get("q", "").strip()
    ocpp_command = request.args.get("ocpp_command", "").strip()
    return jsonify(proxy_db.get_events(
        cp_id=cp_id or None,
        query=query or None,
        ocpp_command=ocpp_command or None,
    ))


@app.route("/api/ocpp-commands")
def api_ocpp_commands():
    return jsonify(proxy_db.get_ocpp_commands())


# ── REST API: Equipment vendors ─────────────────────────────────

@app.route("/api/vendors")
def api_vendors():
    return jsonify({"vendors": proxy_db.get_vendors()})


@app.route("/api/vendors", methods=["POST"])
def api_add_vendor():
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    v = proxy_db.add_vendor(name)
    return jsonify({"ok": True, "vendor": v})


@app.route("/api/vendors/<int:vendor_id>", methods=["PUT"])
def api_rename_vendor(vendor_id):
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    proxy_db.rename_vendor(vendor_id, name)
    return jsonify({"ok": True})


@app.route("/api/vendors/<int:vendor_id>", methods=["DELETE"])
def api_delete_vendor(vendor_id):
    proxy_db.delete_vendor(vendor_id)
    return jsonify({"ok": True})


# ── REST API: Equipment models ──────────────────────────────────

@app.route("/api/models")
def api_models():
    vendor_id = request.args.get("vendor_id", None, type=int)
    return jsonify({"models": proxy_db.get_models(vendor_id)})


@app.route("/api/models", methods=["POST"])
def api_add_model():
    data = request.get_json(force=True)
    vendor_id = data.get("vendor_id")
    name = data.get("name", "").strip()
    if not vendor_id or not name:
        return jsonify({"error": "vendor_id and name required"}), 400
    m = proxy_db.add_model(vendor_id, name)
    return jsonify({"ok": True, "model": m})


@app.route("/api/models/<int:model_id>", methods=["PUT"])
def api_rename_model(model_id):
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    proxy_db.rename_model(model_id, name)
    return jsonify({"ok": True})


@app.route("/api/models/<int:model_id>", methods=["DELETE"])
def api_delete_model(model_id):
    proxy_db.delete_model(model_id)
    return jsonify({"ok": True})


# ── REST API: Meter types ──────────────────────────────────────

@app.route("/api/meter-types")
def api_meter_types():
    return jsonify({"meter_types": proxy_db.get_meter_types()})


@app.route("/api/meter-types", methods=["POST"])
def api_add_meter_type():
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    mt = proxy_db.add_meter_type(name)
    return jsonify({"ok": True, "meter_type": mt})


@app.route("/api/meter-types/<int:mt_id>", methods=["PUT"])
def api_rename_meter_type(mt_id):
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    proxy_db.rename_meter_type(mt_id, name)
    return jsonify({"ok": True})


@app.route("/api/meter-types/<int:mt_id>", methods=["DELETE"])
def api_delete_meter_type(mt_id):
    proxy_db.delete_meter_type(mt_id)
    return jsonify({"ok": True})


# ── Helper: schedule coroutine on proxy event loop ─────────────

def _run_proxy_coro(coro):
    """Schedule an async coroutine on the proxy's event loop from Flask threads."""
    loop = proxy_core.get_proxy_loop()
    if loop is None or loop.is_closed():
        raise RuntimeError("Proxy event loop not available")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=10)  # block until done (max 10 s)
    except concurrent.futures.TimeoutError:
        # Don't leave the coroutine running forever on the proxy's event loop —
        # cancel it so a stuck call can't orphan state (e.g. leave no listener
        # bound after a failed restart_listener()).
        future.cancel()
        raise RuntimeError("proxy operation timed out after 10s and was cancelled")


# ── REST API: Proxy settings (listen port) ─────────────────────

@app.route("/api/config/port")
def api_get_port():
    return jsonify({"port": proxy_db.get_listen_port()})


@app.route("/api/config/port", methods=["PUT"])
def api_set_port():
    data = request.get_json(force=True)
    port = data.get("port")
    if port is None:
        return jsonify({"error": "port required"}), 400
    try:
        port = int(port)
        if not (1024 <= port <= 65535):
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({"error": "port must be an integer between 1024 and 65535"}), 400
    proxy_db.set_listen_port(port)
    # Restart the WS listener on the new port
    try:
        _run_proxy_coro(proxy_core.restart_listener(port))
    except Exception as e:
        logger.error("Failed to restart listener: %s", e)
        return jsonify({"error": f"Port saved but restart failed: {e}"}), 500
    return jsonify({"ok": True, "port": port})


# ── REST API: Backend servers ──────────────────────────────────

@app.route("/api/backends")
def api_backends():
    return jsonify({"backends": proxy_db.get_backends()})


@app.route("/api/backends", methods=["POST"])
def api_add_backend():
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    url = data.get("url", "").strip()
    if not name or not url:
        return jsonify({"error": "name and url required"}), 400
    enabled = data.get("enabled", True)
    try:
        b = proxy_db.add_backend(name, url, enabled)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    # Reconnect so new backend is picked up
    try:
        _run_proxy_coro(proxy_core.reconnect_all_backends())
    except Exception as e:
        logger.error("Reconnect failed: %s", e)
    return jsonify({"ok": True, "backend": b})


@app.route("/api/backends/<int:backend_id>", methods=["PUT"])
def api_update_backend(backend_id):
    data = request.get_json(force=True)
    proxy_db.update_backend(
        backend_id,
        name=data.get("name"),
        url=data.get("url"),
        enabled=data.get("enabled"),
    )
    # Reconnect to apply changes
    try:
        _run_proxy_coro(proxy_core.reconnect_all_backends())
    except Exception as e:
        logger.error("Reconnect failed: %s", e)
    return jsonify({"ok": True})


@app.route("/api/backends/<int:backend_id>", methods=["DELETE"])
def api_delete_backend(backend_id):
    proxy_db.delete_backend(backend_id)
    # Reconnect with remaining backends
    try:
        _run_proxy_coro(proxy_core.reconnect_all_backends())
    except Exception as e:
        logger.error("Reconnect failed: %s", e)
    return jsonify({"ok": True})


# ── REST API: Change Availability (Connector Control) ──────────

@app.route("/api/change-availability", methods=["POST"])
def api_change_availability():
    data = request.get_json(force=True)
    cp_id = data.get("cp_id", "").strip()
    connector_id = data.get("connector_id")
    avail_type = data.get("type", "").strip()

    valid_statuses = (
        "Available", "Preparing", "Charging", "SuspendedEVSE",
        "SuspendedEV", "Finishing", "Reserved", "Unavailable", "Faulted",
    )
    if not cp_id:
        return jsonify({"ok": False, "error": "cp_id is required"}), 400
    if connector_id is None:
        return jsonify({"ok": False, "error": "connector_id is required"}), 400
    if avail_type not in valid_statuses:
        return jsonify({"ok": False, "error": f"type must be one of {valid_statuses}"}), 400

    backends = data.get("backends", "*")
    try:
        # Inject the StatusNotification straight to the backend(s) so the CSMS
        # reflects the chosen connector status. Relying on the charge point to
        # echo a StatusNotification is unreliable: it only emits one when its
        # own availability actually changes (e.g. it never emits for Reserved).
        sent = _run_proxy_coro(
            proxy_core.send_status_to_backends(
                cp_id, int(connector_id), avail_type, backends=backends
            )
        )
        return jsonify({"ok": True, "backends_notified": sent})
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 404
    except Exception as e:
        logger.error("ChangeAvailability failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/connector-history")
def api_connector_history():
    """Return the most recent operator-set connector states for the dashboard."""
    history = proxy_db.get_connector_state_history(limit=10)
    return jsonify({"history": history})


# ── Socket.IO events ───────────────────────────────────────────

@socketio.on("connect", namespace="/proxy")
def on_connect():
    logger.info("Web client connected")
    socketio.emit("connections_update", {
        "clients": list(proxy_core.connections.keys()),
    }, namespace="/proxy")


@socketio.on("disconnect", namespace="/proxy")
def on_disconnect():
    logger.info("Web client disconnected")


if __name__ == "__main__":
    # Allow running web_app.py directly for quick testing
    # (for production use app.py which also starts the WS proxy)
    proxy_db.init_db()
    print("Starting OCPP Proxy web dashboard on http://0.0.0.0:4000")
    print("NOTE: WebSocket proxy not started — run app.py for full proxy")
    socketio.run(app, host="0.0.0.0", port=4000, debug=True, allow_unsafe_werkzeug=True)
