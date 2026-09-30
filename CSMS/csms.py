#!/usr/bin/env python3
"""
Script:   csms.py

Abstract:
    Flask web GUI and REST API for the OCPP 1.6 Central System (CSMS).
    Serves the operator dashboard (connected charge points, transactions,
    events, hardware, Zonneplan tariffs, charging plan, smart schedules,
    RFID/car/equipment management) and drives remote OCPP commands against
    charge points through central_system.py, which this process starts as
    a background asyncio loop alongside the Flask server.

Features:
    - Multi-instance: server name, database path, web/OCPP ports all come
      from a per-instance YAML config, so several backends can run from the
      same image.
    - Remote command endpoints (RemoteStart/Stop, Reset, UnlockConnector,
      ChangeAvailability, GetConfiguration, ChangeConfiguration,
      ClearCache, TriggerMessage) with clean 502/504 error handling instead
      of raw 500s when a charge point rejects or ignores a command.
    - Cheapest-charging-window and cheapest-quarters-with-pause planning
      from stored Zonneplan quarter-hourly tariffs.
    - Smart charging schedule scheduler (start/stop at planned times).

Usage:
    python csms.py --config /config/config.yaml

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
import concurrent.futures
import threading
from datetime import datetime, timedelta, timezone

import yaml
from flask import Flask, render_template, jsonify, request
from ocpp.exceptions import OCPPError

import cs_db
from central_system import connected_cps, heartbeat_offline_cps, start_server as start_ocpp_server, restart_server as restart_ocpp_server

# ── Configuration ───────────────────────────────────────────────

config = {
    "server_name": "Backend",
    "database_path": "/opt/charger/data/csms.db",
    "web_port": 9100,
    "ocpp_port": 9110,
}

app = Flask(__name__)


@app.context_processor
def inject_config():
    return {"server_name": config["server_name"]}


# ── Async helper to call charge point methods from Flask ────────

_loop: asyncio.AbstractEventLoop = None


def _run_async(coro):
    """Schedule a coroutine on the OCPP event loop and wait for result."""
    if _loop is None:
        raise RuntimeError("OCPP event loop not running")
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=10)


# central_system.py's remote-command methods (RemoteStart, Reset,
# GetConfiguration, ...) call the charge point with suppress=False, so a
# CallError (e.g. a charge point that doesn't implement the requested action)
# raises an OCPPError instead of silently returning None — which used to
# crash every one of those routes with an unhandled AttributeError on
# `None.status`/`None.configuration_key`. Turn both failure modes into a
# clean JSON error instead of a bare 500.
@app.errorhandler(OCPPError)
def _handle_ocpp_error(e):
    return jsonify(error=f"Charge point rejected the command: {e}"), 502


@app.errorhandler(concurrent.futures.TimeoutError)
def _handle_command_timeout(e):
    return jsonify(error="Charge point did not respond in time"), 504


def _parse_ts(value):
    if not value:
        return None
    try:
        if "T" not in value:
            if " " in value:
                value = value.replace(" ", "T", 1)
            else:
                value = f"{value}T00:00:00"
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _enrich_charge_session_from_meter(txns):
    """Populate charge session boundaries per transaction from Current.Import meter values."""
    eps = 0.001
    for t in txns:
        charge_start = None
        charge_stop = None
        txn_id = t.get("id")
        if txn_id is not None:
            timeline = cs_db.get_transaction_current_import_timeline(int(txn_id))
            started = False
            last_positive_ts = None
            for p in timeline:
                ts = p.get("timestamp")
                current = p.get("current")
                if current is None:
                    continue
                if not started and current > eps:
                    charge_start = ts
                    started = True
                    last_positive_ts = ts
                    continue
                if started:
                    if current > eps:
                        last_positive_ts = ts
                    elif current <= eps:
                        charge_stop = ts
                        break

            # If no explicit zero-current stop was sampled, use last positive sample.
            if started and charge_stop is None:
                charge_stop = last_positive_ts

        # Fallbacks for legacy transactions lacking detailed current meter values.
        charge_start = charge_start or t.get("charging_start")
        if charge_start:
            charge_stop = charge_stop or t.get("charging_stop")

        t["charge_session_start"] = charge_start
        t["charge_session_stop"] = charge_stop


# ── Pages ───────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("cs_index.html")


@app.route("/chargepoint/<cp_id>")
def chargepoint_detail(cp_id):
    return render_template("cs_chargepoint.html", cp_id=cp_id)


@app.route("/logs")
def logs_page():
    return render_template("cs_logs.html")


@app.route("/events")
def events_page():
    return render_template("cs_events.html")


@app.route("/stats")
def stats_page():
    return render_template("cs_stats.html")


@app.route("/settings")
def settings_page():
    return render_template("cs_settings.html")


@app.route("/rfid")
def rfid_page():
    return render_template("cs_rfid.html")


@app.route("/transactions-mgmt")
def transactions_mgmt_page():
    return render_template("cs_transactions_mgmt.html")


@app.route("/ocpp-config")
def ocpp_config_page():
    return render_template("cs_ocpp_config.html")


@app.route("/mail-config")
def mail_config_page():
    return render_template("cs_mail_config.html")


@app.route("/hardware")
def hardware_page():
    return render_template("cs_hardware.html")


@app.route("/car-config")
def car_config_page():
    return render_template("cs_car_config.html")


@app.route("/chargepoint-config")
def chargepoint_config_page():
    return render_template("cs_chargepoint_config.html")


@app.route("/zonneplan-config")
def zonneplan_config_page():
    return render_template("cs_zonneplan_config.html")


@app.route("/zonneplan")
def zonneplan_page():
    return render_template("cs_zonneplan.html")


@app.route("/charging")
def charging_page():
    return render_template("cs_charging.html")


@app.route("/charge-values")
def charge_values_page():
    return render_template("cs_charge_values.html")


# ── API: Charge Points ─────────────────────────────────────────

@app.route("/api/chargepoints")
def api_chargepoints():
    cps = cs_db.get_chargepoints()
    for cp in cps:
        cp["online"] = cp["cp_id"] in connected_cps and cp["cp_id"] not in heartbeat_offline_cps
        # BootNotification fields are now included directly in cp dict
    return jsonify(cps)


@app.route("/api/chargepoint/<path:cp_id>", methods=["DELETE"])
def api_delete_chargepoint(cp_id):
    if cp_id in connected_cps:
        return jsonify(error="Charge point is still connected. Disconnect it before deleting."), 409

    deleted = cs_db.delete_chargepoint(cp_id)
    if not deleted:
        return jsonify(error="Charge point not found"), 404

    heartbeat_offline_cps.discard(cp_id)
    return jsonify(success=True, deleted=deleted)


@app.route("/api/charging-events")
def api_charging_events():
    cp_id = request.args.get("cp_id")
    start = request.args.get("start")
    end = request.args.get("end")
    query = request.args.get("q", "").strip().lower()
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", type=int, default=0)
    events = cs_db.get_transactions(cp_id)

    def parse_ts(value):
        return _parse_ts(value)

    def normalize_status(value):
        if not value:
            return ""
        return value.replace(".", "").replace("_", "").lower()

    ready_statuses = {"ready", "available"}
    preparing_statuses = {"preparing"}
    charging_statuses = {"charging"}
    charging_stop_statuses = {"suspendedev", "suspendedevse", "faulted", "finishing"}

    start_dt = parse_ts(start) if start else None
    end_dt = parse_ts(end) if end else None
    if end_dt and end and "T" not in end:
        # include full end day when users pick date-only range
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    def in_window(txn):
        started_at = parse_ts(txn.get("started_at"))
        stopped_at = parse_ts(txn.get("stopped_at"))

        # if no date filter, show all
        if not start_dt and not end_dt:
            return True

        # closed session window
        if stopped_at:
            # exclude those completely before range
            if start_dt and stopped_at < start_dt:
                return False
            # exclude those completely after range
            if end_dt and started_at is not None and started_at > end_dt:
                return False
            return True

        # active session window (or missing stopped_at)
        if start_dt and started_at is not None and started_at < start_dt:
            # still running and started before start date; include
            pass
        if end_dt and started_at is not None and started_at > end_dt:
            return False
        return True

    filtered_events = [txn for txn in events if in_window(txn)]
    total = len(filtered_events)
    if limit is not None:
        filtered_events = filtered_events[offset:offset + limit]

    # Attach assigned_to for each id_tag (like in /api/transactions)
    id_tags = list({t["id_tag"] for t in filtered_events if t.get("id_tag")})
    assigned_map = {}
    if id_tags:
        for r in cs_db.get_rfids():
            if r["id_tag"] in id_tags:
                assigned_map[r["id_tag"]] = r.get("assigned_to") or ""
    # Build chargepoint info map for CSV enrichment
    all_cps = {cp["cp_id"]: cp for cp in cs_db.get_chargepoints()}
    for t in filtered_events:
        t["assigned_to"] = assigned_map.get(t["id_tag"], "")
        cp_info = all_cps.get(t.get("cp_id"), {})
        t["charger_model"] = cp_info.get("chargePointModel") or cp_info.get("model") or ""
        t["charger_vendor"] = cp_info.get("chargePointVendor") or cp_info.get("vendor") or ""
        t["charger_serial"] = cp_info.get("chargePointSerialNumber") or ""
        t["charger_firmware"] = cp_info.get("firmwareVersion") or ""
        t["charger_meter_type"] = cp_info.get("meterType") or ""
    # Attach car info for KM calculation
    cars = {c["id"]: c for c in cs_db.get_cars()}
    for t in filtered_events:
        car = cars.get(t.get("car_id"))
        t["car_license_plate"] = car["license_plate"] if car else ""
        t["car_kwh_per_100km"] = car["kwh_per_100km"] if car else None
        # Use stored km (frozen at car-assignment time) if available; fall back
        # to dynamic calculation when it is missing or was frozen as 0 (e.g. the
        # car was assigned before the session had accumulated any energy).
        if not t.get("km"):
            if car and t.get("meter_stop") is not None and t.get("meter_start") is not None:
                energy_kwh = (t["meter_stop"] - t["meter_start"]) / 1000.0
                t["km"] = round(energy_kwh / car["kwh_per_100km"] * 100, 1) if car["kwh_per_100km"] > 0 else None

    _enrich_charge_session_from_meter(filtered_events)

    # Derive timing boundaries from event-sourced status history.
    for t in filtered_events:
        started_dt = parse_ts(t.get("started_at"))
        stopped_dt = parse_ts(t.get("stopped_at"))
        session_start_dt = parse_ts(t.get("charge_session_start")) or started_dt
        session_stop_dt = parse_ts(t.get("charge_session_stop")) or stopped_dt
        if not started_dt:
            t["session_start_at"] = t.get("charge_session_start")
            t["session_end_at"] = t.get("charge_session_stop")
            t["cable_connected_at"] = None
            t["cable_ready_at"] = None
            t["charging_start_at"] = t.get("charging_start")
            t["charging_end_at"] = t.get("charging_stop")
            continue

        cp_id_val = t.get("cp_id")
        connector_id_val = int(t.get("connector_id") or 0)
        base_end = stopped_dt or datetime.now(timezone.utc)
        status_rows = cs_db.get_connector_status_history(
            cp_id_val,
            connector_id_val,
            (started_dt - timedelta(hours=2)).isoformat(),
            (base_end + timedelta(hours=8)).isoformat(),
        )

        latest_preparing = None
        earliest_ready = None
        first_charging = None
        latest_charging_stop = None

        session_anchor = started_dt
        stop_anchor = stopped_dt if stopped_dt and stopped_dt >= started_dt else None

        for row in status_rows:
            ts = parse_ts(row.get("timestamp"))
            if not ts:
                continue
            normalized = normalize_status(row.get("status"))

            if normalized in preparing_statuses and ts <= started_dt:
                if latest_preparing is None or ts > latest_preparing:
                    latest_preparing = ts

            if ts >= session_anchor and normalized in charging_statuses:
                if stop_anchor is None or ts <= stop_anchor:
                    if first_charging is None or ts < first_charging:
                        first_charging = ts

            if ts >= session_anchor and normalized in charging_stop_statuses:
                if stop_anchor is None or ts <= stop_anchor:
                    if latest_charging_stop is None or ts > latest_charging_stop:
                        latest_charging_stop = ts

            if stop_anchor and ts >= stop_anchor and normalized in ready_statuses:
                if earliest_ready is None or ts < earliest_ready:
                    earliest_ready = ts

        # Session duration is derived from Current.Import timeline boundaries.
        if session_start_dt and session_stop_dt and session_stop_dt >= session_start_dt:
            session_end_dt = session_stop_dt
            session_start_out = session_start_dt
        else:
            session_end_dt = stopped_dt if stop_anchor else None
            session_start_out = started_dt

        charging_start_dt = first_charging or parse_ts(t.get("charging_start"))
        charging_end_dt = latest_charging_stop or parse_ts(t.get("charging_stop"))
        if charging_start_dt and charging_end_dt and charging_end_dt < charging_start_dt:
            charging_end_dt = None

        cable_connected_dt = latest_preparing
        cable_ready_dt = earliest_ready
        if cable_connected_dt and cable_ready_dt and cable_ready_dt <= cable_connected_dt:
            cable_connected_dt = None
            cable_ready_dt = None

        # Invariant: charging session cannot be longer than cable-connected window.
        if (
            session_start_out and session_end_dt and
            cable_connected_dt and cable_ready_dt
        ):
            if session_end_dt > cable_ready_dt:
                session_end_dt = cable_ready_dt
            if session_start_out < cable_connected_dt:
                session_start_out = cable_connected_dt
            if session_end_dt <= session_start_out:
                session_start_out = None
                session_end_dt = None

        t["session_start_at"] = session_start_out.isoformat() if session_start_out else None
        t["session_end_at"] = session_end_dt.isoformat() if session_end_dt else None
        t["cable_connected_at"] = cable_connected_dt.isoformat() if cable_connected_dt else None
        t["cable_ready_at"] = cable_ready_dt.isoformat() if cable_ready_dt else None
        t["charging_start_at"] = charging_start_dt.isoformat() if charging_start_dt else None
        t["charging_end_at"] = charging_end_dt.isoformat() if charging_end_dt else None

    # Attach charging cost from Zonneplan tariff
    _enrich_costs(filtered_events)
    return jsonify({
        "events": filtered_events,
        "transactions": filtered_events,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


def _enrich_costs(txns):
    """Use stored cost/tariff from DB. For transactions without stored cost, calculate using hourly tariffs."""
    for t in txns:
        # Already stored in DB — use as-is
        if t.get("cost_eur") is not None and t.get("tariff_eur_per_kwh") is not None:
            continue
        # Transaction with energy data but no stored cost — calculate from hourly tariffs
        if (t.get("meter_stop") is not None and t.get("meter_start") is not None
                and t.get("started_at") and t.get("stopped_at")):
            try:
                conn = cs_db._get_conn()
                result = cs_db._calculate_hourly_cost(
                    conn, t["id"], t["started_at"], t["stopped_at"],
                    t["meter_start"], t["meter_stop"]
                )
                if result["cost_eur"] > 0:
                    t["cost_eur"] = result["cost_eur"]
                    t["tariff_eur_per_kwh"] = result["avg_tariff"]
            except Exception:
                pass


@app.route("/api/load-energy-stats")
def api_load_energy_stats():
    cp_id = request.args.get("cp_id")
    resolution = request.args.get("resolution", "hourly")

    transactions = cs_db.get_transactions(cp_id)
    meter_values = []
    for txn in transactions:
        rows = cs_db.get_meter_values(cp_id=cp_id, transaction_id=txn["id"])
        meter_values.extend(rows)

    total_energy_wh = 0
    peak_power_w = 0
    for mv in meter_values:
        if mv.get("measurand") == "Energy.Active.Import.Register":
            try:
                total_energy_wh += float(mv.get("value", 0))
            except Exception:
                pass
        if mv.get("measurand") == "Power.Active.Import":
            try:
                peak_power_w = max(peak_power_w, float(mv.get("value", 0)))
            except Exception:
                pass

    stats = {
        "total_energy_kwh": round(total_energy_wh / 1000.0, 3),
        "peak_power_kw": round(peak_power_w / 1000.0, 3),
        "transactions": len(transactions),
        "resolution": resolution,
        "chart": []
    }

    # Simulate chart by grouping simplistic values
    if resolution in ["15m", "hourly"]:
        stats["chart"] = [{"label":"Now","energy_kwh":stats["total_energy_kwh"],"peak_kw":stats["peak_power_kw"]}]
    return jsonify(stats)


@app.route("/api/station-settings")
def api_station_settings():
    cps = cs_db.get_chargepoints()
    for cp in cps:
        cp["firmware"] = cp.get("model", "unknown") + "@v1.0"
        cp["fallback_mode"] = "auto"
        cp["last_seen"] = cp.get("last_heartbeat")
        cp["site"] = cp.get("site", "") or ''
    return jsonify(cps)


@app.route("/api/sites")
def api_sites():
    return jsonify(cs_db.get_sites())


@app.route("/api/site", methods=["POST"])
def api_add_site():
    data = request.json or {}
    cs_db.add_site(
        name=data.get("name"),
        street=data.get("street"),
        house_number=data.get("house_number"),
        zip_code=data.get("zip_code"),
        city=data.get("city"),
        country=data.get("country"),
    )
    return jsonify(success=True)


@app.route("/api/site/<int:site_id>", methods=["PUT"])
def api_update_site(site_id):
    data = request.json or {}
    cs_db.update_site(
        site_id,
        name=data.get("name"),
        street=data.get("street"),
        house_number=data.get("house_number"),
        zip_code=data.get("zip_code"),
        city=data.get("city"),
        country=data.get("country"),
    )
    return jsonify(success=True)


@app.route("/api/site/<int:site_id>", methods=["DELETE"])
def api_delete_site(site_id):
    cs_db.delete_site(site_id)
    return jsonify(success=True)


@app.route("/api/site/<int:site_id>/assign-chargepoint", methods=["POST"])
def api_assign_chargepoint(site_id):
    data = request.json or {}
    cp_id = data.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    cs_db.assign_chargepoint_to_site(cp_id, site_id)
    return jsonify(success=True)


@app.route("/api/modbus-registers")
def api_modbus_registers():
    cp_id = request.args.get("cp_id")
    # Minimal mock; in real system this should map connectors and live tags.
    registers = [
        {"connector_id": 1, "register": "0x1001", "value": "OK", "description": "Connector state"},
        {"connector_id": 1, "register": "0x1002", "value": "12345", "description": "Energy (Wh)"},
    ]
    if cp_id:
        return jsonify({"cp_id": cp_id, "registers": registers})
    return jsonify({"registers": registers})


@app.route("/api/rfids")
def api_rfids():
    account = request.args.get("account")
    site = request.args.get("site")
    return jsonify(cs_db.get_rfids(account, site))


@app.route("/api/rfid-upload", methods=["POST"])
def api_rfid_upload():
    data = request.json or {}
    entries = data.get("rfids") or []
    count = cs_db.import_rfids(entries)
    return jsonify({"imported": count})

@app.route("/api/rfid/<int:rfid_id>", methods=["PUT"])
def api_rfid_update(rfid_id):
    data = request.json or {}
    blocked = data.get("blocked")
    if blocked is not None:
        blocked = int(blocked)
    cs_db.update_rfid(
        rfid_id,
        id_tag=data.get("id_tag"),
        assigned_to=data.get("assigned_to"),
        account=data.get("charge_point") or data.get("account"),
        site=data.get("site"),
        blocked=blocked,
    )
    return jsonify(success=True)


@app.route("/api/rfid/<int:rfid_id>", methods=["DELETE"])
def api_rfid_delete(rfid_id):
    cs_db.delete_rfid(rfid_id)
    return jsonify(success=True)

# ── API: Transactions ──────────────────────────────────────────

@app.route("/api/transactions")
def api_transactions():
    cp_id = request.args.get("cp_id")
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", type=int, default=0)
    total = cs_db.get_transactions_count(cp_id)
    txns = cs_db.get_transactions(cp_id, limit=limit, offset=offset)
    # Build id_tag -> assigned_to map for all used id_tags
    id_tags = list({t["id_tag"] for t in txns if t.get("id_tag")})
    assigned_map = {}
    if id_tags:
        for r in cs_db.get_rfids():
            if r["id_tag"] in id_tags:
                assigned_map[r["id_tag"]] = r.get("assigned_to") or ""
    for t in txns:
        t["assigned_to"] = assigned_map.get(t["id_tag"], "")
    # Attach car info for KM calculation
    cars = {c["id"]: c for c in cs_db.get_cars()}
    for t in txns:
        car = cars.get(t.get("car_id"))
        t["car_license_plate"] = car["license_plate"] if car else ""
        t["car_kwh_per_100km"] = car["kwh_per_100km"] if car else None
        if not t.get("km"):
            if car and t.get("meter_stop") is not None and t.get("meter_start") is not None:
                energy_kwh = (t["meter_stop"] - t["meter_start"]) / 1000.0
                t["km"] = round(energy_kwh / car["kwh_per_100km"] * 100, 1) if car["kwh_per_100km"] > 0 else None

    _enrich_charge_session_from_meter(txns)
    _enrich_costs(txns)
    return jsonify({
        "transactions": txns,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@app.route("/api/transaction/<int:txn_id>")
def api_get_transaction(txn_id):
    txn = cs_db.get_transaction(txn_id)
    if not txn:
        return jsonify(error="Transaction not found"), 404
    return jsonify(txn)


@app.route("/api/transaction/<int:txn_id>/cost-detail")
def api_transaction_cost_detail(txn_id):
    txn = cs_db.get_transaction(txn_id)
    if not txn:
        return jsonify(error="Transaction not found"), 404
    if not (txn.get("meter_stop") is not None and txn.get("meter_start") is not None
            and txn.get("started_at") and txn.get("stopped_at")):
        return jsonify(error="Transaction has no meter/time data"), 400
    conn = cs_db._get_conn()
    result = cs_db._calculate_hourly_cost(
        conn, txn_id, txn["started_at"], txn["stopped_at"],
        txn["meter_start"], txn["meter_stop"]
    )
    return jsonify(
        transaction_id=txn_id,
        started_at=txn["started_at"],
        stopped_at=txn["stopped_at"],
        total_energy_wh=txn["meter_stop"] - txn["meter_start"],
        total_cost_eur=result["cost_eur"],
        avg_tariff=result["avg_tariff"],
        hours=result["hours"],
    )


@app.route("/api/transaction/<int:txn_id>", methods=["PUT"])
def api_update_transaction(txn_id):
    data = request.json or {}
    car_id = data.get("car_id")

    # Calculate km and freeze it at assignment time so future changes to
    # kwh_per_100km do not affect already-recorded transactions. Leave km as
    # NULL when the session has no energy yet so it is recalculated later
    # (a frozen 0 would otherwise stick even after energy is recorded).
    km = None
    if "car_id" in data:
        if car_id:
            txn = cs_db.get_transaction(txn_id)
            if txn and txn.get("meter_stop") is not None and txn.get("meter_start") is not None:
                cars_map = {c["id"]: c for c in cs_db.get_cars()}
                car = cars_map.get(int(car_id))
                if car and (car.get("kwh_per_100km") or 0) > 0:
                    energy_kwh = (txn["meter_stop"] - txn["meter_start"]) / 1000.0
                    km = round(energy_kwh / car["kwh_per_100km"] * 100, 1) or None

    update_fields = {
        "cp_id": data.get("cp_id"),
        "connector_id": data.get("connector_id"),
        "id_tag": data.get("id_tag"),
        "meter_start": data.get("meter_start"),
        "meter_stop": data.get("meter_stop"),
        "started_at": data.get("started_at"),
        "stopped_at": data.get("stopped_at"),
        "stop_reason": data.get("stop_reason"),
        "charging_start": data.get("charging_start"),
        "charging_stop": data.get("charging_stop"),
    }

    if "car_id" in data:
        # car_id explicitly cleared -> also clear stored km
        update_fields["car_id"] = car_id
        update_fields["km"] = km

    cs_db.update_transaction(txn_id, **update_fields)
    return jsonify(success=True)


@app.route("/api/transaction/<int:txn_id>", methods=["DELETE"])
def api_delete_transaction(txn_id):
    cs_db.delete_transaction(txn_id)
    return jsonify(success=True)


# ── API: Meter Values ──────────────────────────────────────────

@app.route("/api/meter-values")
def api_meter_values():
    cp_id = request.args.get("cp_id")
    txn_id = request.args.get("transaction_id", type=int)
    return jsonify(cs_db.get_meter_values(cp_id, txn_id))


@app.route("/api/temperature-history")
def api_temperature_history():
    cp_id = request.args.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    return jsonify(cs_db.get_temperature_history(cp_id))


# ── API: Live Stats ────────────────────────────────────────────

@app.route("/api/live-stats")
def api_live_stats():
    cp_id = request.args.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    return jsonify(cs_db.get_latest_meter_values(cp_id))


@app.route("/api/current-session")
def api_current_session():
    cp_id = request.args.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    txn = cs_db.get_active_transaction_for_cp(cp_id)
    if not txn:
        return jsonify({"active": False, "power": 0, "l1": 0, "l2": 0, "l3": 0, "energy": 0})
    mv = cs_db.get_latest_meter_values(cp_id, transaction_id=txn["id"])
    # Fallback: if no meter values found for this transaction, try without filter
    if not mv:
        mv = cs_db.get_latest_meter_values(cp_id)

    def _val(key):
        """Return the value for a measurand key normalised to W / Wh / A, or None."""
        entry = mv.get(key)
        if not entry:
            return None
        try:
            value = float(entry["value"])
        except (TypeError, ValueError):
            return None
        # Charge points may report in kW / kWh; normalise to W / Wh.
        if (entry.get("unit") or "").lower() in ("kw", "kwh"):
            value *= 1000
        return value

    def _phase(measurand, phase):
        # Accept both "L1" and "L1-N" style phase names
        for key in (f"{measurand}.{phase}", f"{measurand}.{phase}-N"):
            v = _val(key)
            if v is not None:
                return v
        return None

    power_val = _val("Power.Active.Import")
    if power_val is None:
        # Some charge points only report power per phase
        phase_powers = [_phase("Power.Active.Import", p) for p in ("L1", "L2", "L3")]
        power_val = sum(p for p in phase_powers if p is not None)
    l1_val = _phase("Current.Import", "L1")
    if l1_val is None:
        # Single-phase / aggregate current without a phase attribute
        l1_val = _val("Current.Import")
    l2_val = _phase("Current.Import", "L2")
    l3_val = _phase("Current.Import", "L3")
    # Total energy delivered = current meter reading - meter_start (both in Wh)
    energy_val = 0
    current_meter = _val("Energy.Active.Import.Register")
    if current_meter is not None:
        meter_start = txn.get("meter_start") or 0
        energy_val = max(0, current_meter - meter_start)
    return jsonify({"active": True, "power": power_val, "l1": l1_val or 0, "l2": l2_val or 0,
                    "l3": l3_val or 0, "energy": energy_val})


@app.route("/api/current-session/debug")
def api_current_session_debug():
    """Debug endpoint to inspect raw current-session data."""
    cp_id = request.args.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    txn = cs_db.get_active_transaction_for_cp(cp_id)
    if not txn:
        return jsonify({"active_transaction": None, "reason": "no active transaction found"})
    txn_dict = dict(txn)
    mv = cs_db.get_latest_meter_values(cp_id, transaction_id=txn_dict["id"])
    mv_no_filter = cs_db.get_latest_meter_values(cp_id)
    return jsonify({
        "active_transaction": txn_dict,
        "meter_values_by_txn": mv,
        "meter_values_no_filter": mv_no_filter,
    })


# ── API: OCPP Config ───────────────────────────────────────────

@app.route("/api/ocpp-config")
def api_get_ocpp_config():
    cfg = cs_db.get_ocpp_config()
    # Include the running OCPP port so the UI can display it
    if "ocpp_port" not in cfg:
        cfg["ocpp_port"] = str(config["ocpp_port"])
    return jsonify(cfg)


@app.route("/api/ocpp-config", methods=["PUT"])
def api_set_ocpp_config():
    data = request.get_json(force=True)
    for key, value in data.items():
        cs_db.set_ocpp_config_value(key, str(value))
    return jsonify(cs_db.get_ocpp_config())


@app.route("/api/ocpp-port/status")
def api_ocpp_port_status():
    """Return the running port and any active sessions/connections."""
    active = cs_db.get_active_transactions()
    return jsonify({
        "running_port": config["ocpp_port"],
        "connected_cps": list(connected_cps.keys()),
        "active_sessions": len(active),
    })


@app.route("/api/ocpp-port/restart", methods=["POST"])
def api_ocpp_port_restart():
    """Change the OCPP WebSocket port at runtime."""
    data = request.get_json(force=True)
    new_port = int(data.get("port", config["ocpp_port"]))
    if not 1 <= new_port <= 65535:
        return jsonify(error="Port must be between 1 and 65535"), 400
    if new_port == config["ocpp_port"]:
        return jsonify(message="Port unchanged")

    cs_db.set_ocpp_config_value("ocpp_port", str(new_port))
    try:
        _run_async(restart_ocpp_server(new_port))
        config["ocpp_port"] = new_port
    except Exception as e:
        return jsonify(error=f"Failed to restart OCPP server: {e}"), 500
    return jsonify(message=f"OCPP server restarted on port {new_port}", port=new_port)

# ── API: Mail Config ─────────────────────────────────────────

@app.route("/api/mail-config")
def api_get_mail_config():
    cfg = cs_db.get_mail_config()
    # Don't expose password to the frontend
    if cfg.get("password"):
        cfg["password"] = "********"
    return jsonify(cfg)


@app.route("/api/mail-config", methods=["PUT"])
def api_set_mail_config():
    data = request.get_json(force=True)
    for key, value in data.items():
        # Don't overwrite password with the masked placeholder
        if key == "password" and value == "********":
            continue
        cs_db.set_mail_config_value(key, str(value))
    return jsonify({"status": "ok"})


# ── API: Cars ──────────────────────────────────────────────────

@app.route("/api/cars")
def api_cars():
    return jsonify(cs_db.get_cars())


@app.route("/api/car", methods=["POST"])
def api_add_car():
    data = request.json or {}
    brand = data.get("brand", "").strip()
    model = data.get("model", "").strip()
    year = data.get("year")
    kwh = data.get("kwh_per_100km")
    license_plate = data.get("license_plate", "").strip()
    battery = data.get("battery_capacity_kwh")
    if not brand or not model or year is None or kwh is None:
        return jsonify(error="All fields are required"), 400
    cs_db.add_car(
        brand,
        model,
        int(year),
        float(kwh),
        license_plate,
        battery_capacity_kwh=float(battery) if battery not in (None, "") else None,
    )
    return jsonify(success=True)


@app.route("/api/car/<int:car_id>", methods=["PUT"])
def api_update_car(car_id):
    data = request.json or {}
    battery = data.get("battery_capacity_kwh")
    cs_db.update_car(
        car_id,
        brand=data.get("brand"),
        model=data.get("model"),
        year=int(data["year"]) if data.get("year") is not None else None,
        kwh_per_100km=float(data["kwh_per_100km"]) if data.get("kwh_per_100km") is not None else None,
        license_plate=data.get("license_plate"),
        battery_capacity_kwh=float(battery) if battery not in (None, "") else None,
    )
    return jsonify(success=True)


@app.route("/api/car/<int:car_id>", methods=["DELETE"])
def api_delete_car(car_id):
    cs_db.delete_car(car_id)
    return jsonify(success=True)


# ── API: App Settings ─────────────────────────────────────────

@app.route("/api/app-settings", methods=["GET"])
def api_get_app_settings():
    return jsonify(cs_db.get_app_config())


@app.route("/api/app-settings", methods=["POST"])
def api_set_app_settings():
    data = request.json or {}
    for key, value in data.items():
        cs_db.set_app_config_value(str(key), "" if value is None else str(value))
    return jsonify(success=True)


# ── API: Chargepoint Charge Values ────────────────────────────

def _max_kw_from_charge_values(cv: dict) -> float | None:
    if not cv:
        return None
    voltage = cv.get("voltage") or 230
    amps = []
    n = int(cv.get("num_phases") or 0)
    if n >= 1 and cv.get("max_amp_l1") is not None:
        amps.append(float(cv["max_amp_l1"]))
    if n >= 2 and cv.get("max_amp_l2") is not None:
        amps.append(float(cv["max_amp_l2"]))
    if n >= 3 and cv.get("max_amp_l3") is not None:
        amps.append(float(cv["max_amp_l3"]))
    if not amps:
        return None
    total_w = sum(a * voltage for a in amps)
    return round(total_w / 1000.0, 3)


@app.route("/api/charge-values", methods=["GET"])
def api_get_all_charge_values():
    cps = cs_db.get_chargepoints()
    cv_list = cs_db.get_chargepoint_charge_values()
    cv_map = {cv["cp_id"]: cv for cv in cv_list}
    out = []
    for cp in cps:
        cv = cv_map.get(cp["cp_id"]) or {
            "cp_id": cp["cp_id"],
            "num_phases": 1,
            "max_amp_l1": None,
            "max_amp_l2": None,
            "max_amp_l3": None,
            "voltage": 230,
            "updated_at": None,
        }
        cv["max_kw"] = _max_kw_from_charge_values(cv)
        out.append(cv)
    return jsonify(out)


@app.route("/api/charge-values/<path:cp_id>", methods=["GET"])
def api_get_charge_values(cp_id):
    cv = cs_db.get_chargepoint_charge_values(cp_id)
    if not cv:
        return jsonify({
            "cp_id": cp_id,
            "num_phases": 1,
            "max_amp_l1": None,
            "max_amp_l2": None,
            "max_amp_l3": None,
            "voltage": 230,
            "max_kw": None,
        })
    cv["max_kw"] = _max_kw_from_charge_values(cv)
    return jsonify(cv)


@app.route("/api/charge-values/<path:cp_id>", methods=["POST", "PUT"])
def api_set_charge_values(cp_id):
    data = request.json or {}
    try:
        num_phases = int(data.get("num_phases", 1))
    except (TypeError, ValueError):
        return jsonify(error="num_phases must be an integer"), 400
    if num_phases < 1 or num_phases > 3:
        return jsonify(error="num_phases must be 1..3"), 400

    def _to_float_or_none(v):
        if v is None or v == "":
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f < 0:
            return None
        return f

    voltage = _to_float_or_none(data.get("voltage"))
    if voltage is None or voltage <= 0:
        voltage = 230.0

    cs_db.set_chargepoint_charge_values(
        cp_id,
        num_phases=num_phases,
        max_amp_l1=_to_float_or_none(data.get("max_amp_l1")),
        max_amp_l2=_to_float_or_none(data.get("max_amp_l2")) if num_phases >= 2 else None,
        max_amp_l3=_to_float_or_none(data.get("max_amp_l3")) if num_phases >= 3 else None,
        voltage=voltage,
    )
    return jsonify(success=True)


@app.route("/api/charge-values/<path:cp_id>", methods=["DELETE"])
def api_delete_charge_values(cp_id):
    cs_db.delete_chargepoint_charge_values(cp_id)
    return jsonify(success=True)


@app.route("/api/charge-values/default-kw", methods=["GET"])
def api_default_charge_kw():
    """Return the maximum kW across all configured charge points (used as
    default for the Charging dashboard's Charging Power)."""
    cv_list = cs_db.get_chargepoint_charge_values()
    best = None
    best_cp = None
    for cv in cv_list:
        kw = _max_kw_from_charge_values(cv)
        if kw is not None and (best is None or kw > best):
            best = kw
            best_cp = cv["cp_id"]
    return jsonify({"default_kw": best, "cp_id": best_cp})


# ── API: Charging Plan ────────────────────────────────────────
#
# Zonneplan publishes tariffs per 15-minute slot (previously per hour), so all
# planning here works in quarter-hour slots. Tariff dicts carry an "hour" key
# for DB/API compatibility, but its value is a quarter-hour timestamp
# ("YYYY-MM-DD HH:MM"), not a whole hour.

_SLOT = timedelta(minutes=15)


def _parse_slot(h):
    try:
        return datetime.strptime(h, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _find_cheapest_consecutive_window(tariffs: list, slots_needed: int):
    """Find the cheapest run of `slots_needed` CONSECUTIVE 15-minute slots.

    Returns {"windows": [ {start_slot, end_slot, slot_count, avg_tariff} ],
    "avg_tariff", "total_window_tariff"} — a single-window result shaped the
    same as _find_cheapest_quarters_allow_pause() so callers can treat both
    strategies uniformly — or None if there isn't a long enough run of
    consecutive future tariff data.
    """
    if slots_needed <= 0 or not tariffs:
        return None
    items = sorted((t for t in tariffs if t.get("tariff_eur_per_kwh") is not None), key=lambda r: r["hour"])
    n = len(items)
    if n < slots_needed:
        return None

    best = None
    # Find runs of consecutive quarter-hour slots, then sliding-window min sum.
    run_start = 0
    for i in range(1, n + 1):
        broken = False
        if i == n:
            broken = True
        else:
            prev_dt = _parse_slot(items[i - 1]["hour"])
            cur_dt = _parse_slot(items[i]["hour"])
            if prev_dt is None or cur_dt is None or (cur_dt - prev_dt) != _SLOT:
                broken = True
        if broken:
            run = items[run_start:i]
            if len(run) >= slots_needed:
                # Sliding window
                window_sum = sum(r["tariff_eur_per_kwh"] for r in run[:slots_needed])
                best_local_sum = window_sum
                best_local_idx = 0
                for j in range(slots_needed, len(run)):
                    window_sum += run[j]["tariff_eur_per_kwh"] - run[j - slots_needed]["tariff_eur_per_kwh"]
                    if window_sum < best_local_sum:
                        best_local_sum = window_sum
                        best_local_idx = j - slots_needed + 1
                start_entry = run[best_local_idx]
                end_entry = run[best_local_idx + slots_needed - 1]
                if best is None or best_local_sum < best[2]:
                    best = (start_entry["hour"], end_entry["hour"], best_local_sum)
            run_start = i
    if best is None:
        return None
    start_slot, end_slot, total = best
    avg = total / slots_needed
    return {
        "windows": [{
            "start_slot": start_slot,
            "end_slot": end_slot,
            "slot_count": slots_needed,
            "avg_tariff": round(avg, 6),
        }],
        "avg_tariff": round(avg, 6),
        "total_window_tariff": round(total, 6),
    }


def _find_cheapest_quarters_allow_pause(tariffs: list, slots_needed: int):
    """Pick the cheapest `slots_needed` 15-minute slots from `tariffs`,
    regardless of whether they're consecutive, then merge adjacent picked
    slots into contiguous charging windows. Charging pauses whenever an
    expensive slot that wasn't picked falls between two picked slots.

    Returns the same shape as _find_cheapest_consecutive_window(), with
    possibly several windows in chronological order, or None if there isn't
    enough future tariff data.
    """
    if slots_needed <= 0 or not tariffs:
        return None
    valid = [t for t in tariffs if t.get("tariff_eur_per_kwh") is not None]
    if len(valid) < slots_needed:
        return None

    # Cheapest N slots; ties broken chronologically for determinism.
    picked = sorted(valid, key=lambda r: (r["tariff_eur_per_kwh"], r["hour"]))[:slots_needed]
    picked.sort(key=lambda r: r["hour"])

    runs = [[picked[0]]]
    for prev, cur in zip(picked, picked[1:]):
        prev_dt = _parse_slot(prev["hour"])
        cur_dt = _parse_slot(cur["hour"])
        if prev_dt is not None and cur_dt is not None and (cur_dt - prev_dt) == _SLOT:
            runs[-1].append(cur)
        else:
            runs.append([cur])

    total = sum(r["tariff_eur_per_kwh"] for r in picked)
    avg = total / slots_needed

    windows = []
    for run in runs:
        run_total = sum(r["tariff_eur_per_kwh"] for r in run)
        windows.append({
            "start_slot": run[0]["hour"],
            "end_slot": run[-1]["hour"],
            "slot_count": len(run),
            "avg_tariff": round(run_total / len(run), 6),
        })

    return {
        "windows": windows,
        "avg_tariff": round(avg, 6),
        "total_window_tariff": round(total, 6),
    }


@app.route("/api/charging-plan")
def api_charging_plan():
    """Compute the cheapest charging plan for a car based on Zonneplan
    quarter-hour tariffs.

    Query params:
        car_id: int (required)
        battery_left_pct: float 0-100 (required)
        charging_power_kw: float (optional, default 11)
        strategy: "consecutive" (default) — one uninterrupted charging block —
            or "allow_pause" — picks the globally cheapest 15-minute slots
            across the whole available forecast (not just one block) and
            pauses charging over any expensive slot in between.
    """
    try:
        car_id = int(request.args.get("car_id", ""))
    except (TypeError, ValueError):
        return jsonify(error="car_id required"), 400
    try:
        left_pct = float(request.args.get("battery_left_pct", ""))
    except (TypeError, ValueError):
        return jsonify(error="battery_left_pct required"), 400
    if left_pct < 0 or left_pct > 100:
        return jsonify(error="battery_left_pct must be 0..100"), 400
    try:
        power_kw = float(request.args.get("charging_power_kw", "11"))
    except (TypeError, ValueError):
        power_kw = 11.0
    if power_kw <= 0:
        return jsonify(error="charging_power_kw must be > 0"), 400
    strategy = request.args.get("strategy", "consecutive")
    if strategy not in ("consecutive", "allow_pause"):
        return jsonify(error="strategy must be 'consecutive' or 'allow_pause'"), 400

    cars = {c["id"]: c for c in cs_db.get_cars()}
    car = cars.get(car_id)
    if not car:
        return jsonify(error="Car not found"), 404
    capacity = car.get("battery_capacity_kwh")
    if capacity is None or capacity <= 0:
        return jsonify(error="Car has no battery_capacity_kwh configured"), 400

    left_kwh = capacity * (left_pct / 100.0)
    kwh_to_load = max(0.0, capacity - left_kwh)

    now_utc = datetime.now(timezone.utc)
    current_slot_key = now_utc.replace(minute=(now_utc.minute // 15) * 15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    # Future tariffs (including the current quarter-hour slot).
    all_tariffs = cs_db.get_zonneplan_tariffs(start=current_slot_key)
    avg_tariff_all = None
    if all_tariffs:
        vals = [t["tariff_eur_per_kwh"] for t in all_tariffs if t.get("tariff_eur_per_kwh") is not None]
        if vals:
            avg_tariff_all = round(sum(vals) / len(vals), 6)

    import math
    if kwh_to_load <= 0:
        plan = {
            "strategy": strategy,
            "slots_needed": 0,
            "duration_hours": 0,
            "start_at": None,
            "end_at": None,
            "avg_tariff_window": None,
            "estimated_cost_eur": 0.0,
            "windows": [],
        }
    else:
        kwh_per_slot = power_kw * 0.25
        slots_needed = max(1, int(math.ceil(kwh_to_load / kwh_per_slot)))
        if strategy == "allow_pause":
            result = _find_cheapest_quarters_allow_pause(all_tariffs, slots_needed)
        else:
            result = _find_cheapest_consecutive_window(all_tariffs, slots_needed)

        if result is None:
            plan = {
                "strategy": strategy,
                "slots_needed": slots_needed,
                "duration_hours": round(slots_needed * 0.25, 2),
                "start_at": None,
                "end_at": None,
                "avg_tariff_window": None,
                "estimated_cost_eur": None,
                "windows": [],
                "warning": ("Not enough consecutive future tariff data to plan" if strategy == "consecutive"
                            else "Not enough future tariff data to plan"),
            }
        else:
            windows_out = []
            for w in result["windows"]:
                start_dt = _parse_slot(w["start_slot"])
                end_dt = _parse_slot(w["end_slot"]) + _SLOT
                windows_out.append({
                    "start_at": start_dt.isoformat(),
                    "end_at": end_dt.isoformat(),
                    "avg_tariff": w["avg_tariff"],
                    "slot_count": w["slot_count"],
                })

            plan = {
                "strategy": strategy,
                "slots_needed": slots_needed,
                "duration_hours": round(slots_needed * 0.25, 2),
                "start_at": windows_out[0]["start_at"],
                "end_at": windows_out[-1]["end_at"],
                "avg_tariff_window": result["avg_tariff"],
                "estimated_cost_eur": round(kwh_to_load * result["avg_tariff"], 4),
                "windows": windows_out,
            }

    return jsonify({
        "now": now_utc.isoformat(),
        "car_id": car["id"],
        "brand": car.get("brand"),
        "model": car.get("model"),
        "license_plate": car.get("license_plate") or "",
        "battery_capacity_kwh": capacity,
        "battery_left_pct": left_pct,
        "battery_left_kwh": round(left_kwh, 3),
        "kwh_to_load": round(kwh_to_load, 3),
        "charging_power_kw": power_kw,
        "avg_tariff_forecast": avg_tariff_all,
        "plan": plan,
    })


# ── API: Smart Schedules ──────────────────────────────────────

@app.route("/api/smart-schedules")
def api_smart_schedules():
    cp_id = request.args.get("cp_id")
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", type=int, default=0)
    total = cs_db.get_smart_schedules_count(cp_id)
    schedules = cs_db.get_smart_schedules(cp_id, limit=limit, offset=offset)
    active_count = len(cs_db.get_active_smart_schedules(cp_id))
    return jsonify({
        "schedules": schedules,
        "total": total,
        "limit": limit,
        "offset": offset,
        "active_count": active_count,
    })


@app.route("/api/smart-schedule/<int:schedule_id>")
def api_get_smart_schedule(schedule_id):
    schedule = cs_db.get_smart_schedule(schedule_id)
    if not schedule:
        return jsonify(error="Schedule not found"), 404
    return jsonify(schedule)


@app.route("/api/smart-schedule", methods=["POST"])
def api_add_smart_schedule():
    data = request.json or {}
    cp_id = data.get("cp_id")
    id_tag = data.get("id_tag")
    start_at = data.get("start_at")
    stop_at = data.get("stop_at")
    if not all([cp_id, id_tag, start_at, stop_at]):
        return jsonify(error="cp_id, id_tag, start_at and stop_at required"), 400
    sid = cs_db.add_smart_schedule(cp_id, id_tag, start_at, stop_at)
    return jsonify(success=True, id=sid)


@app.route("/api/smart-schedule/<int:schedule_id>", methods=["PUT"])
def api_update_smart_schedule(schedule_id):
    data = request.json or {}
    allowed = {k: v for k, v in data.items() if k in ("id_tag", "start_at", "stop_at", "status")}
    if not allowed:
        return jsonify(error="No valid fields to update"), 400
    cs_db.update_smart_schedule(schedule_id, **allowed)
    return jsonify(success=True)


@app.route("/api/smart-schedule/<int:schedule_id>", methods=["DELETE"])
def api_delete_smart_schedule(schedule_id):
    schedule = cs_db.get_smart_schedule(schedule_id)
    if schedule and schedule["status"] == "started" and schedule.get("transaction_id"):
        # Stop the active transaction before deleting
        cp_id = schedule["cp_id"]
        if cp_id in connected_cps:
            try:
                _run_async(connected_cps[cp_id].remote_stop_transaction(schedule["transaction_id"]))
            except Exception:
                pass
    cs_db.delete_smart_schedule(schedule_id)
    return jsonify(success=True)


@app.route("/api/mail-config/test", methods=["POST"])
def api_mail_test():
    import mail_notify
    try:
        mail_notify.send_test_mail()
        return jsonify({"status": "ok", "message": "Test email sent"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── API: Zonneplan ─────────────────────────────────────────────

@app.route("/api/zonneplan/status")
def api_zonneplan_status():
    import zonneplan
    cfg = cs_db.get_zonneplan_config()
    return jsonify({
        "authenticated": zonneplan.is_authenticated(),
        "email": cfg.get("email", ""),
        "auth_status": cfg.get("auth_status", ""),
        "connection_uuid": cfg.get("connection_uuid", ""),
    })


@app.route("/api/zonneplan/login", methods=["POST"])
def api_zonneplan_login():
    import zonneplan
    data = request.json or {}
    email = data.get("email", "").strip()
    if not email:
        return jsonify(error="Email required"), 400
    try:
        uuid = zonneplan.request_login(email)
        return jsonify(success=True, uuid=uuid, message="Verification email sent. Check your inbox and click the link.")
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/api/zonneplan/verify", methods=["POST"])
def api_zonneplan_verify():
    import zonneplan
    try:
        token = zonneplan.check_login()
        if token:
            return jsonify(success=True, message="Authenticated successfully")
        return jsonify(success=False, message="Not yet verified. Check your email and click the link.")
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/api/zonneplan/setup-connection", methods=["POST"])
def api_zonneplan_setup_connection():
    import zonneplan
    try:
        result = zonneplan.setup_connection()
        return jsonify(success=True, **result)
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/api/zonneplan/tariff")
def api_zonneplan_tariff():
    import zonneplan
    if not zonneplan.is_authenticated():
        return jsonify(error="Not authenticated with Zonneplan"), 401
    try:
        tariff = zonneplan.get_current_tariff()
        return jsonify(tariff)
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.route("/api/zonneplan/tariffs")
def api_zonneplan_tariffs():
    start = request.args.get("start")
    end = request.args.get("end")
    limit = request.args.get("limit", type=int)
    tariffs = cs_db.get_zonneplan_tariffs(start=start, end=end)
    if limit and not start and not end:
        tariffs = tariffs[:limit]
    return jsonify(tariffs)


@app.route("/api/zonneplan/debug")
def api_zonneplan_debug():
    import zonneplan
    if not zonneplan.is_authenticated():
        return jsonify(error="Not authenticated with Zonneplan"), 401
    try:
        cfg = zonneplan._get_config()
        connection_uuid = cfg.get("connection_uuid", "")
        if not connection_uuid:
            return jsonify(error="No connection_uuid configured")
        data = zonneplan.get_summary_data(connection_uuid)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        now_key = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
        top_keys = list(data.keys()) if isinstance(data, dict) else str(type(data))
        price_per_hour = data.get("price_per_hour", [])
        sample = price_per_hour[:3] if price_per_hour else []
        return jsonify({
            "now_key": now_key,
            "top_level_keys": top_keys,
            "price_per_hour_count": len(price_per_hour),
            "price_per_hour_sample": sample,
            "data_type": str(type(data)),
            "data_empty": not data,
        })
    except Exception as e:
        import traceback
        return jsonify(error=str(e), trace=traceback.format_exc()), 500


@app.route("/api/zonneplan/disconnect", methods=["POST"])
def api_zonneplan_disconnect():
    import zonneplan
    zonneplan.disconnect()
    return jsonify(success=True)

# ── API: Dashboard Stats ───────────────────────────────────────

@app.route("/api/dashboard-stats")
def api_dashboard_stats():
    active = cs_db.get_active_transactions()
    # If no active transactions, show the most recent completed one
    if not active:
        last = cs_db.get_last_transaction()
        if last:
            active = [last]
        else:
            return jsonify([])
    # Keep only the most recent transaction per charge point
    seen_cps = {}
    for txn in active:
        cp = txn["cp_id"]
        if cp not in seen_cps or txn["id"] > seen_cps[cp]["id"]:
            seen_cps[cp] = txn
    active = list(seen_cps.values())
    # Build id_tag -> assigned_to map for all used id_tags
    id_tags = list({t["id_tag"] for t in active if t.get("id_tag")})
    assigned_map = {}
    if id_tags:
        for r in cs_db.get_rfids():
            if r["id_tag"] in id_tags:
                assigned_map[r["id_tag"]] = r.get("assigned_to") or ""
    results = []
    for txn in active:
        stats = cs_db.get_latest_meter_values(txn["cp_id"], txn["id"])
        # Fallback: if no meter values found for this transaction, try without scoping
        if not stats:
            stats = cs_db.get_latest_meter_values(txn["cp_id"])
        # Use the most recent DB-stored timestamp; fall back to server receive time
        last_update = None
        for v in stats.values():
            ts = v.get("timestamp")
            if ts and (last_update is None or ts > last_update):
                last_update = ts
        # Format as YYYY-MM-DD HH:MM:SS
        if last_update:
            try:
                dt = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                last_update = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        elif stats:
            last_update = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        results.append({
            "cp_id": txn["cp_id"],
            "transaction_id": txn["id"],
            "started_at": txn["started_at"],
            "stopped_at": txn.get("stopped_at"),
            "meter_start": txn["meter_start"],
            "meter_stop": txn.get("meter_stop"),
            "last_update": last_update,
            "energy": stats.get("Energy.Active.Import.Register"),
            "power": stats.get("Power.Active.Import"),
            "current_l1": stats.get("Current.Import.L1"),
            "current_l2": stats.get("Current.Import.L2"),
            "current_l3": stats.get("Current.Import.L3"),
            "id_tag": txn.get("id_tag"),
            "assigned_to": assigned_map.get(txn.get("id_tag"), "")
        })
    return jsonify(results)


# ── API: Event Log ─────────────────────────────────────────────

@app.route("/api/force-close-session", methods=["POST"])
def api_force_close_session():
    data = request.json or {}
    cp_id = data.get("cp_id")
    if not cp_id:
        return jsonify(error="cp_id required"), 400
    txn_id = cs_db.stop_active_transaction_for_cp(cp_id, reason="ForceClose")
    if txn_id is None:
        return jsonify(error="No active session for this charge point"), 404
    cs_db.update_chargepoint_status(cp_id, "Available")
    return jsonify(status="Closed", transaction_id=txn_id)


@app.route("/api/events")
def api_events():
    cp_id = request.args.get("cp_id")
    query = request.args.get("q", "").strip()
    ocpp_command = request.args.get("ocpp_command", "").strip()
    severity = request.args.get("severity", "DEBUG").strip().upper()
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", type=int, default=0)
    total = cs_db.get_events_count(
        cp_id,
        query if query else None,
        ocpp_command if ocpp_command else None,
        severity_min=severity,
    )
    evts = cs_db.get_events(
        cp_id,
        query if query else None,
        ocpp_command if ocpp_command else None,
        severity_min=severity,
        limit=limit,
        offset=offset,
    )
    return jsonify({
        "events": evts,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@app.route("/api/ocpp-commands")
def api_ocpp_commands():
    """Return list of distinct OCPP commands used in the system."""
    commands = cs_db.get_ocpp_commands()
    return jsonify(commands)


# ── API: Remote Commands ───────────────────────────────────────

@app.route("/api/command/<cp_id>/remote-start", methods=["POST"])
def api_remote_start(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    id_tag = data.get("id_tag", "REMOTE")
    connector_id = data.get("connector_id")
    result = _run_async(connected_cps[cp_id].remote_start_transaction(id_tag, connector_id))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/remote-stop", methods=["POST"])
def api_remote_stop(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    txn_id = data.get("transaction_id")
    if txn_id is None:
        return jsonify(error="transaction_id required"), 400
    result = _run_async(connected_cps[cp_id].remote_stop_transaction(txn_id))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/reset", methods=["POST"])
def api_reset(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    reset_type = data.get("type", "Soft")
    result = _run_async(connected_cps[cp_id].reset(reset_type))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/unlock", methods=["POST"])
def api_unlock(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    connector_id = data.get("connector_id", 1)
    result = _run_async(connected_cps[cp_id].unlock_connector(connector_id))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/change-availability", methods=["POST"])
def api_change_availability(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    connector_id = data.get("connector_id", 0)
    av_type = data.get("type", "Operative")
    result = _run_async(connected_cps[cp_id].change_availability(connector_id, av_type))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/get-configuration", methods=["POST"])
def api_get_configuration(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    keys = data.get("keys", [])
    result = _run_async(connected_cps[cp_id].get_configuration(keys if keys else None))
    return jsonify(result)


@app.route("/api/command/<cp_id>/change-configuration", methods=["POST"])
def api_change_configuration(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    key = data.get("key", "")
    value = data.get("value", "")
    if not key:
        return jsonify(error="key required"), 400
    result = _run_async(connected_cps[cp_id].change_configuration(key, value))
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/clear-cache", methods=["POST"])
def api_clear_cache(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    result = _run_async(connected_cps[cp_id].clear_cache())
    return jsonify(status=result)


@app.route("/api/command/<cp_id>/trigger-message", methods=["POST"])
def api_trigger_message(cp_id):
    if cp_id not in connected_cps:
        return jsonify(error="Charge point not connected"), 404
    data = request.json or {}
    msg = data.get("requested_message", "")
    connector_id = data.get("connector_id")
    if not msg:
        return jsonify(error="requested_message required"), 400
    result = _run_async(connected_cps[cp_id].trigger_message(msg, connector_id))
    return jsonify(status=result)


# ── Start everything ───────────────────────────────────────────

def _run_ocpp_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_until_complete(start_ocpp_server(port=config["ocpp_port"]))
    # Keep the event loop running so restart_server can be scheduled on it
    _loop.run_forever()


def main():
    parser = argparse.ArgumentParser(description="OCPP Central System Backend")
    parser.add_argument(
        "-c", "--config",
        type=str,
        default=None,
        help="Path to YAML configuration file",
    )
    args = parser.parse_args()

    file_cfg = {}
    if args.config:
        with open(args.config, "r") as f:
            file_cfg = yaml.safe_load(f) or {}
        config.update(file_cfg)

    cs_db.DB_PATH = config["database_path"]
    cs_db.init_db()
    cs_db.backfill_status_history_from_event_log()

    if "ocpp_port" in file_cfg:
        # An OCPP port set explicitly in the YAML config is authoritative.
        # Persist it so the web UI reflects the configured value.
        config["ocpp_port"] = int(file_cfg["ocpp_port"])
        cs_db.set_ocpp_config_value("ocpp_port", str(config["ocpp_port"]))
    else:
        # No YAML override — use the port saved via the UI (DB), otherwise the default.
        db_port = cs_db.get_ocpp_config_value("ocpp_port")
        if db_port:
            try:
                config["ocpp_port"] = int(db_port)
            except ValueError:
                pass

    threading.Thread(target=_run_ocpp_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=config["web_port"], debug=False)


## ── API: Statistics Dashboard Endpoints ─────────────────────

@app.route("/api/stats/total_energy")
def stats_total_energy():
    txns = cs_db.get_transactions()
    if not txns:
        return jsonify({"total_energy_kwh": 0})
    # Transactions are returned DESC by id; first = newest, last = oldest
    first_txn = txns[-1]  # oldest
    last_txn = txns[0]    # newest
    meter_start = first_txn.get("meter_start") or 0
    meter_stop = last_txn.get("meter_stop") or last_txn.get("meter_start") or 0
    total_wh = max(0, meter_stop - meter_start)
    return jsonify({"total_energy_kwh": round(total_wh / 1000.0, 3)})


@app.route("/api/stats/avg_energy_per_session")
def stats_avg_energy_per_session():
    txns = cs_db.get_transactions()
    energies = []
    for t in txns:
        if t.get("meter_stop") is not None and t.get("meter_start") is not None:
            wh = t["meter_stop"] - t["meter_start"]
            if wh >= 0:
                energies.append(wh / 1000.0)
    avg = round(sum(energies) / len(energies), 2) if energies else 0
    return jsonify({"avg_energy_kwh": avg})


@app.route("/api/stats/avg_session_duration")
def stats_avg_session_duration():
    txns = cs_db.get_transactions()
    durations = []
    for t in txns:
        start = t.get("charging_start") or t.get("started_at")
        stop = t.get("charging_stop") or t.get("stopped_at")
        if start and stop:
            try:
                s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                e = datetime.fromisoformat(stop.replace("Z", "+00:00"))
                durations.append((e - s).total_seconds() / 60.0)
            except Exception:
                pass
    avg = round(sum(durations) / len(durations), 1) if durations else 0
    return jsonify({"avg_duration_minutes": avg})


@app.route("/api/stats/co2_savings")
def stats_co2_savings():
    txns = cs_db.get_transactions()
    total_kwh = 0
    for t in txns:
        if t.get("meter_stop") is not None and t.get("meter_start") is not None:
            wh = t["meter_stop"] - t["meter_start"]
            if wh >= 0:
                total_kwh += wh / 1000.0
    # Average CO2 savings: ~0.4 kg CO2 per kWh (EV vs ICE)
    co2_kg = round(total_kwh * 0.4, 2)
    return jsonify({"co2_kg": co2_kg})


@app.route("/api/stats/error_alert_count")
def stats_error_alert_count():
    events = cs_db.get_events()
    count = sum(1 for e in events if e.get("severity") in ("ERROR", "WARNING"))
    return jsonify({"error_count": count})


@app.route("/api/stats/most_active_hour")
def stats_most_active_hour():
    txns = cs_db.get_transactions()
    hour_counts = {}
    for t in txns:
        started = t.get("started_at")
        if started:
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                h = dt.hour
                hour_counts[h] = hour_counts.get(h, 0) + 1
            except Exception:
                pass
    if hour_counts:
        best = max(hour_counts, key=hour_counts.get)
        return jsonify({"hour": best, "sessions": hour_counts[best]})
    return jsonify({"hour": None, "sessions": 0})


@app.route("/api/stats/unique_users")
def stats_unique_users():
    txns = cs_db.get_transactions()
    id_tags = {t["id_tag"] for t in txns if t.get("id_tag")}
    rfids = cs_db.get_rfids()
    assigned_map = {}
    for r in rfids:
        if r["id_tag"] in id_tags:
            assigned_map[r["id_tag"]] = r.get("assigned_to") or ""
    # Count distinct non-empty assigned_to values
    unique = {v for v in assigned_map.values() if v}
    return jsonify({"unique_users": len(unique)})


@app.route("/api/stats/utilization_rate")
def stats_utilization_rate():
    txns = cs_db.get_transactions()
    if not txns:
        return jsonify({"utilization_percent": 0})
    total_seconds = 0
    for t in txns:
        start = t.get("charging_start") or t.get("started_at")
        stop = t.get("charging_stop") or t.get("stopped_at")
        if start and stop:
            try:
                s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                e = datetime.fromisoformat(stop.replace("Z", "+00:00"))
                total_seconds += (e - s).total_seconds()
            except Exception:
                pass
    # Calculate utilization as % of the last 30 days
    window = 30 * 24 * 3600
    pct = round((total_seconds / window) * 100, 1) if window else 0
    return jsonify({"utilization_percent": min(pct, 100.0)})


# ── Statistics Dashboard Page Routes ─────────────────────────
@app.route("/stats/avg_energy_per_session")
def page_avg_energy_per_session():
    return render_template("stats/avg_energy_per_session.html")

@app.route("/stats/avg_session_duration")
def page_avg_session_duration():
    return render_template("stats/avg_session_duration.html")

@app.route("/stats/co2_savings")
def page_co2_savings():
    return render_template("stats/co2_savings.html")

@app.route("/stats/energy_by_connector")
def page_energy_by_connector():
    return render_template("stats/energy_by_connector.html")

@app.route("/stats/error_alert_count")
def page_error_alert_count():
    return render_template("stats/error_alert_count.html")

@app.route("/stats/most_active_hour")
def page_most_active_hour():
    return render_template("stats/most_active_hour.html")

@app.route("/stats/peak_concurrent_sessions")
def page_peak_concurrent_sessions():
    return render_template("stats/peak_concurrent_sessions.html")

@app.route("/stats/revenue")
def page_revenue():
    return render_template("stats/revenue.html")

@app.route("/stats/unique_users")
def page_unique_users():
    return render_template("stats/unique_users.html")

@app.route("/stats/utilization_rate")
def page_utilization_rate():
    return render_template("stats/utilization_rate.html")
if __name__ == "__main__":
    main()
