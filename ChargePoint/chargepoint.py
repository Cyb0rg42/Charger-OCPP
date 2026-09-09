#!/usr/bin/env python3
"""
Script:   chargepoint.py

Abstract:
    Web GUI and REST API for the ChargePoint OCPP 1.6 charge point simulator.
    Serves the station-control dashboard (cable/car connect, start/stop a
    charging session, live power/energy charts) and exposes the JSON API the
    UI polls for state. The actual OCPP wire protocol (BootNotification,
    Heartbeat, StatusNotification, MeterValues, remote commands, ...) is
    handled by cp_sim.py, which this process starts as a background asyncio
    loop alongside the Flask/Socket.IO server.

Features:
    - Flask + Socket.IO dashboard: station control, stats, OCPP event log.
    - REST API for cable/car connect-disconnect and start/stop charging.
    - Loads per-instance config (name, database path, GUI port, log paths)
      from a JSON file via --config, so multiple simulated stations can run
      from the same image with distinct identities.

Usage:
    python chargepoint.py --config /config/config.json

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

import argparse
import asyncio
import json
import secrets
import sys
import threading

from flask import Flask, render_template, jsonify, redirect, url_for
from flask import request
from flask_socketio import SocketIO, emit

# --- Parse --config argument early, before other imports use paths ---

_parser = argparse.ArgumentParser(description="ChargePoint simulator")
_parser.add_argument(
    "--config",
    type=str,
    default=None,
    help="Path to JSON configuration file",
)
_args = _parser.parse_args()

# Load config file and apply paths
import db

if _args.config:
    with open(_args.config, "r") as _f:
        _file_cfg = json.load(_f)
    db.set_db_path(_file_cfg["database"])
else:
    _file_cfg = None

from models import (
    state, lock, connect_cable, disconnect_cable,
    connect_car, disconnect_car,
    start_charging, stop_charging,
    get_config, update_config,
    init_models,
    unlock_connector, soft_reset, hard_reset,
)

# If you want to run cp_sim loop in same process:
from cp_sim import main as cp_main, configure_logging

# Apply chargepoint name from config file
if _file_cfg:
    _cp_name = _file_cfg.get("name")
    if _cp_name:
        from models import config as _model_cfg, config_lock as _cfg_lock
        with _cfg_lock:
            _model_cfg.station_id = _cp_name

# Apply log paths from config file
if _file_cfg:
    _logs = _file_cfg.get("logs", {})
    configure_logging(
        ocpp_log_path=_logs.get("ocpp_log"),
        access_log_path=_logs.get("access_log"),
        error_log_path=_logs.get("error_log"),
    )

app = Flask(__name__)
# Randomly generated per process start. Nothing in this app relies on the
# key staying stable across restarts (no persisted sessions/CSRF tokens),
# so a fresh random value each boot is strictly safer than a fixed one.
app.config["SECRET_KEY"] = secrets.token_hex(32)
socketio = SocketIO(app)

# Expose station name to all templates
_station_name = _file_cfg.get("name", "Cyb0rg42 Station") if _file_cfg else "Cyb0rg42 Station"
_gui_port = _file_cfg.get("port", 9400) if _file_cfg else 9400

@app.context_processor
def inject_station_name():
    return {"station_name": _station_name}

# --- Initialise database and load persisted state ---

init_models()

# Seed the central system URL from the config file when the database has none
# yet. Anything set later in the GUI is stored in the database and wins, so
# this only applies to a fresh instance — where the default `localhost` URL
# would otherwise point a containerised chargepoint at itself.
if _file_cfg:
    _ocpp_url = _file_cfg.get("ocpp_url")
    if _ocpp_url and "ocpp_url" not in db.load_config():
        update_config(ocpp_url=_ocpp_url)

# --- Background OCPP loop in another thread ---

def start_ocpp_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(cp_main())

threading.Thread(target=start_ocpp_loop, daemon=True).start()

# --- Helper to serialize state (thread-safe) ---

def serialize_state():
    with lock:
        return {
            "cable_connected": state.cable_connected,
            "car_connected": state.car_connected,
            "connector_locked": state.connector_locked,
            "charging": state.charging,
            "active_connector_id": state.active_connector_id,
            "total_energy_kwh": state.total_energy(),
            "sessions": [
                {
                    "id": s.id,
                    "started_at": s.started_at.isoformat(),
                    "ended_at": s.ended_at.isoformat() if s.ended_at else None,
                    "energy_kwh": s.energy_kwh,
                    "connector_id": s.connector_id,
                }
                for s in state.sessions
            ],
            "power_history": list(state.power_history),
        }


# --- Background emitter for live UI updates ---

def background_state_emitter():
    while True:
        socketio.sleep(2)
        socketio.emit("state_update", serialize_state())

socketio.start_background_task(background_state_emitter)


# --- Routes ---

@app.route("/")
def index():
    return render_template("index.html", data=serialize_state())

@app.route("/stats")
def stats():
    return render_template("stats.html", data=serialize_state())

@app.route("/api/state")
def api_state():
    return jsonify(serialize_state())

@app.route("/api/cable", methods=["POST"])
def api_cable():
    action = request.json.get("action")
    if action == "connect":
        try:
            connector_id = int(request.json.get("connector_id", 1))
        except (TypeError, ValueError):
            return jsonify(success=False, error="connector_id must be an integer"), 400
        connect_cable(connector_id=connector_id)
    elif action == "disconnect":
        disconnect_cable()
    else:
        return jsonify(success=False, error="unknown action"), 400
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/api/car", methods=["POST"])
def api_car():
    action = request.json.get("action")
    if action == "connect":
        connect_car()
    elif action == "disconnect":
        disconnect_car()
    else:
        return jsonify(success=False, error="unknown action"), 400
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/api/charging", methods=["POST"])
def api_charging():
    action = request.json.get("action")
    if action == "start":
        ok = start_charging()
    elif action == "stop":
        stop_charging()
        ok = True
    else:
        return jsonify(success=False, error="unknown action"), 400
    socketio.emit("state_update", serialize_state())
    return jsonify(success=ok)

@app.route("/api/config", methods=["GET"])
def api_config_get():
    return jsonify(get_config())

@app.route("/api/config", methods=["POST"])
def api_config_set():
    data = request.json or {}
    id_tags = data.get("id_tags")
    if isinstance(id_tags, str):
        id_tags = [t.strip() for t in id_tags.split(",") if t.strip()]
    num_connectors = data.get("num_connectors")
    if num_connectors is not None:
        num_connectors = int(num_connectors)
    update_config(
        ocpp_url=data.get("ocpp_url"),
        max_kwh=data.get("max_kwh"),
        min_kwh=data.get("min_kwh"),
        num_connectors=num_connectors,
        id_tags=id_tags,
        charge_point_vendor=data.get("charge_point_vendor"),
        charge_point_model=data.get("charge_point_model"),
        charge_point_meter_type=data.get("charge_point_meter_type"),
    )
    return jsonify(success=True, config=get_config())

# --- Equipment database (vendors / models) ---

@app.route("/equipment")
def equipment_page():
    return render_template("equipment.html")

@app.route("/meter-types")
def meter_types_page():
    return render_template("meter_types.html")

@app.route("/api/vendors", methods=["GET"])
def api_vendors_get():
    return jsonify(db.get_vendors())

@app.route("/api/vendors", methods=["POST"])
def api_vendors_add():
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify(success=False, error="name required"), 400
    vendor = db.add_vendor(name)
    return jsonify(success=True, vendor=vendor)

@app.route("/api/vendors/<int:vendor_id>", methods=["PUT"])
def api_vendors_rename(vendor_id):
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify(success=False, error="name required"), 400
    db.rename_vendor(vendor_id, name)
    return jsonify(success=True)

@app.route("/api/vendors/<int:vendor_id>", methods=["DELETE"])
def api_vendors_delete(vendor_id):
    db.delete_vendor(vendor_id)
    return jsonify(success=True)

@app.route("/api/models", methods=["GET"])
def api_models_get():
    vendor_id = request.args.get("vendor_id", type=int)
    return jsonify(db.get_models(vendor_id))

@app.route("/api/models", methods=["POST"])
def api_models_add():
    data = request.json or {}
    vendor_id = data.get("vendor_id")
    name = (data.get("name") or "").strip()
    if not vendor_id or not name:
        return jsonify(success=False, error="vendor_id and name required"), 400
    model = db.add_model(int(vendor_id), name)
    return jsonify(success=True, model=model)

@app.route("/api/models/<int:model_id>", methods=["PUT"])
def api_models_rename(model_id):
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify(success=False, error="name required"), 400
    db.rename_model(model_id, name)
    return jsonify(success=True)

@app.route("/api/models/<int:model_id>", methods=["DELETE"])
def api_models_delete(model_id):
    db.delete_model(model_id)
    return jsonify(success=True)

@app.route("/api/meter-types", methods=["GET"])
def api_meter_types_get():
    return jsonify(db.get_meter_types())

@app.route("/api/meter-types", methods=["POST"])
def api_meter_types_add():
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify(success=False, error="name required"), 400
    mt = db.add_meter_type(name)
    return jsonify(success=True, meter_type=mt)

@app.route("/api/meter-types/<int:mt_id>", methods=["PUT"])
def api_meter_types_rename(mt_id):
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify(success=False, error="name required"), 400
    db.rename_meter_type(mt_id, name)
    return jsonify(success=True)

@app.route("/api/meter-types/<int:mt_id>", methods=["DELETE"])
def api_meter_types_delete(mt_id):
    db.delete_meter_type(mt_id)
    return jsonify(success=True)

@app.route("/api/register", methods=["POST"])
def api_register():
    from models import ocpp_events
    ocpp_events.put(("register", None))
    # Also set the asyncio reconnect event so the OCPP loop wakes up
    # immediately even if it's sleeping or in a Pending/Rejected retry.
    from cp_sim import _reconnect_event
    _reconnect_event.set()
    return jsonify(success=True)

@app.route("/api/remote-start", methods=["POST"])
def api_remote_start():
    """Trigger a remote start of the charging session."""
    from models import ocpp_events
    ok = start_charging()
    socketio.emit("state_update", serialize_state())
    if ok:
        return jsonify(success=True)
    else:
        return jsonify(success=False, error="Cannot start charging in current state"), 400

@app.route("/api/remote-stop", methods=["POST"])
def api_remote_stop():
    """Trigger a remote stop of the charging session."""
    stop_charging()
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/api/soft-reset", methods=["POST"])
def api_soft_reset():
    """Trigger a soft reset (stops charging but maintains state)."""
    soft_reset()
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/api/hard-reset", methods=["POST"])
def api_hard_reset():
    """Trigger a hard reset (stops charging and clears all state)."""
    hard_reset()
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/api/unlock-connector", methods=["POST"])
def api_unlock_connector():
    """Unlock a specific connector."""
    try:
        connector_id = int(request.json.get("connector_id", 1)) if request.json else 1
    except (TypeError, ValueError):
        return jsonify(success=False, error="connector_id must be an integer"), 400
    unlock_connector(connector_id=connector_id)
    socketio.emit("state_update", serialize_state())
    return jsonify(success=True)

@app.route("/ocpp")
def ocpp_logs_page():
    return render_template("ocpp.html")

@app.route("/api/ocpp-logs")
def api_ocpp_logs():
    query = request.args.get("q", "").strip()
    logs = db.search_ocpp_logs(query if query else None)
    return jsonify(logs)

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=_gui_port, debug=False, allow_unsafe_werkzeug=True)

