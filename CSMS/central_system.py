"""
Script:   central_system.py

Abstract:
    OCPP 1.6-J Central System WebSocket server. Accepts charge point (or
    OCPPProxy) connections, handles inbound OCPP calls (BootNotification,
    Heartbeat, StatusNotification, StartTransaction, StopTransaction,
    MeterValues, Authorize, DataTransfer, ...), and exposes the outbound
    remote-command methods (RemoteStart/Stop, Reset, UnlockConnector,
    ChangeAvailability, GetConfiguration, ChangeConfiguration, ClearCache,
    TriggerMessage) that csms.py's REST API drives. Started as a background
    asyncio loop by csms.py alongside its Flask web GUI.

Features:
    - Tracks connected charge points (connected_cps) and offline detection
      via heartbeat_offline_cps().
    - Persists transactions, meter values, and events through cs_db.py.
    - start_server()/restart_server() to (re)bind the OCPP listen port,
      e.g. after a port change from the GUI.

Usage:
    Imported by csms.py; not intended to be run directly.

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
from datetime import datetime, timezone

import websockets
from ocpp.routing import on
from ocpp.v16 import call, call_result, ChargePoint as Cp
from ocpp.v16.enums import (
    AuthorizationStatus,
    RegistrationStatus,
    Action,
)

import cs_db
import mail_notify

_APP_DIR = "/var/log/charger"
os.makedirs(_APP_DIR, exist_ok=True)
_LOG_PATH = os.path.join(_APP_DIR, "backend.json")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "message": record.getMessage(),
        }
        return json.dumps(log_entry)


_json_formatter = JsonFormatter()

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_json_formatter)

_file_handler = logging.FileHandler(_LOG_PATH, encoding="utf-8")
_file_handler.setFormatter(_json_formatter)

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_root_logger.handlers.clear()
_root_logger.addHandler(_console_handler)
_root_logger.addHandler(_file_handler)

# ── OCPP debug logger → /var/log/charger/ocpp.log ──────────────
_OCPP_LOG_DIR = "/var/log/charger"
os.makedirs(_OCPP_LOG_DIR, exist_ok=True)

_ocpp_logger = logging.getLogger("backend")
_ocpp_logger.setLevel(logging.DEBUG)
_ocpp_logger.propagate = False

_ocpp_wire_handler = logging.FileHandler(
    os.path.join(_OCPP_LOG_DIR, "backendocpp.log"), encoding="utf-8"
)
_ocpp_wire_handler.setFormatter(_json_formatter)
_ocpp_logger.addHandler(_ocpp_wire_handler)

# Connected charge points keyed by id
connected_cps: dict[str, "CentralSystemHandler"] = {}
# CPs that missed heartbeats and are presumed offline
heartbeat_offline_cps: set[str] = set()

HEARTBEAT_TIMEOUT_SECONDS = 300  # 5 minutes

# Reference to the running websocket server (set by start_server)
_ws_server: websockets.WebSocketServer | None = None


class CentralSystemHandler(Cp):
    """Handles a single charge point connection."""

    def __init__(self, cp_id, ws):
        super().__init__(cp_id, ws)
        self.cp_id = cp_id
        self._online_since = datetime.now(timezone.utc)
        self._last_raw_msg = None
        self._last_raw_response = None
        self._last_event_id = None
        self._last_activity = datetime.now(timezone.utc)

    # ── OCPP wire logging ───────────────────────────────────────

    async def _send(self, message):
        _ocpp_logger.debug("[backend][%s] TX %s", self.cp_id, message)
        self._last_raw_response = message
        return await super()._send(message)

    async def route_message(self, raw_msg):
        _ocpp_logger.debug("[backend][%s] RX %s", self.cp_id, raw_msg)
        self._last_raw_msg = raw_msg
        self._last_raw_response = None
        self._last_event_id = None
        result = await super().route_message(raw_msg)
        if self._last_raw_response and self._last_event_id:
            cs_db.update_event_response(self._last_event_id, self._last_raw_response)
        return result

    # ── Incoming from Charge Point ──────────────────────────────

    def _mark_active(self):
        """Update last activity and restore online status if needed."""
        self._last_activity = datetime.now(timezone.utc)
        if self.cp_id in heartbeat_offline_cps:
            heartbeat_offline_cps.discard(self.cp_id)
            cs_db.update_chargepoint_status(self.cp_id, "Online")
            cs_db.log_event(self.cp_id, "INFO", "Charge point back online (heartbeat resumed)")
            logging.info("Charge point %s back online (heartbeat resumed)", self.cp_id)

    @on(Action.boot_notification)
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        self._mark_active()
        # Map BootNotification fields to camelCase for DB
        def map_boot_fields(fields):
            mapping = {
                "chargePointSerialNumber": ["chargePointSerialNumber", "charge_point_serial_number", "serialNumber", "serial_number"],
                "firmwareVersion": ["firmwareVersion", "firmware_version"],
                "meterType": ["meterType", "meter_type"],
            }
            result = {}
            for camel, keys in mapping.items():
                for k in keys:
                    if k in fields:
                        result[camel] = fields[k]
                        break
            return result

        boot_fields = map_boot_fields(kwargs)
        all_fields = {
            "chargePointVendor": charge_point_vendor,
            "chargePointModel": charge_point_model,
            **boot_fields,
        }
        logging.debug(json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": "DEBUG",
            "message": "BootNotification received",
            "cp_id": self.cp_id,
            **all_fields,
        }))
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"BootNotification from {charge_point_vendor} / {charge_point_model}", ocpp_payload=self._last_raw_msg)
        mail_notify.notify("notify_boot", f"[Charger] Boot Notification from {self.cp_id}", f"Charge point {self.cp_id} sent BootNotification.\nVendor: {charge_point_vendor}\nModel: {charge_point_model}")
        # Update chargepoints table with latest info and BootNotification fields
        cs_db.upsert_chargepoint(
            self.cp_id,
            vendor=charge_point_vendor,
            model=charge_point_model,
            status="Available",
            **all_fields
        )
        hb_interval = int(cs_db.get_ocpp_config_value("heartbeat_interval", "30"))
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=hb_interval,
            status=RegistrationStatus.accepted,
        )

    @on(Action.heartbeat)
    async def on_heartbeat(self, **kwargs):
        self._mark_active()
        cs_db.update_chargepoint_heartbeat(self.cp_id)
        return call_result.Heartbeat(
            current_time=datetime.now(timezone.utc).isoformat(),
        )

    @on(Action.status_notification)
    async def on_status_notification(self, connector_id, error_code, status, **kwargs):
        self._mark_active()
        status_ts = kwargs.get("timestamp") or datetime.now(timezone.utc).isoformat()
        self._last_event_id = cs_db.log_event(
            self.cp_id,
            "INFO",
            f"StatusNotification connector={connector_id} status={status} error={error_code}",
            ocpp_payload=self._last_raw_msg,
            event_timestamp=status_ts,
        )

        if status in ("Faulted", "faulted"):
            mail_notify.notify("notify_faulted", f"[Charger] Connector Faulted on {self.cp_id}", f"Charge point {self.cp_id} connector {connector_id} reported Faulted status.\nError code: {error_code}")

        if connector_id == 0:
            # Connector 0 means entire charge point — update all known connectors
            cs_db.update_connector_status(self.cp_id, 0, status, updated_at=status_ts)
            known_ids = cs_db.get_all_connector_ids(self.cp_id)
            for cid in known_ids:
                cs_db.update_connector_status(self.cp_id, cid, status, updated_at=status_ts)
        else:
            cs_db.update_connector_status(self.cp_id, connector_id, status, updated_at=status_ts)

        txn = cs_db.get_active_transaction_for_connector(self.cp_id, connector_id)
        cs_db.insert_connector_status_history(
            self.cp_id,
            connector_id,
            status,
            status_ts,
            transaction_id=(txn["id"] if txn else None),
            source="live",
        )

        # Update last_status on the active transaction
        if txn:
            cs_db.update_transaction_status(txn["id"], status)

        # Track when power actually starts/stops flowing
        if status in ("Charging", "charging"):
            if txn:
                cs_db.record_charging_start(txn["id"], status_ts)

        # Record when power stopped flowing for these statuses.
        if status in ("SuspendedEV", "Suspended.EV", "SuspendedEVSE", "Suspended.EVSE", "Faulted", "faulted", "Finishing", "finishing"):
            if txn:
                cs_db.record_charging_stop(txn["id"], status_ts)
            mail_notify.notify("notify_ev_suspended", f"[Charger] EV Suspended on {self.cp_id}", f"Charge point {self.cp_id} connector {connector_id} reported SuspendedEV status.")

        # SuspendedEV means the car has stopped drawing power (battery full or
        # vehicle-side stop). Treat this as the end of the charge session so
        # the user can assign a car to it without waiting for StopTransaction.
        if status in ("SuspendedEV", "Suspended.EV"):
            closed_id = cs_db.stop_active_transaction_for_cp(self.cp_id, reason="EVSuspended")
            if closed_id:
                self._last_event_id = cs_db.log_event(
                    self.cp_id,
                    "INFO",
                    f"Charge session closed on SuspendedEV (txn_id={closed_id})",
                    ocpp_payload=self._last_raw_msg,
                )
        return call_result.StatusNotification()

    @on(Action.authorize)
    async def on_authorize(self, id_tag, **kwargs):
        auth = cs_db.get_id_tag_auth_status(id_tag)
        if auth == "accepted":
            status = AuthorizationStatus.accepted
            self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"Authorize id_tag={id_tag} \u2192 Accepted", ocpp_payload=self._last_raw_msg)
        elif auth == "blocked":
            status = AuthorizationStatus.blocked
            self._last_event_id = cs_db.log_event(self.cp_id, "WARNING", f"Authorize id_tag={id_tag} \u2192 Blocked", ocpp_payload=self._last_raw_msg)
        else:
            status = AuthorizationStatus.invalid
            self._last_event_id = cs_db.log_event(self.cp_id, "WARNING", f"Authorize id_tag={id_tag} → Invalid (not in whitelist)", ocpp_payload=self._last_raw_msg)
        return call_result.Authorize(
            id_tag_info={"status": status},
        )

    @on(Action.start_transaction)
    async def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        auth = cs_db.get_id_tag_auth_status(id_tag)
        if auth != "accepted":
            status = AuthorizationStatus.blocked if auth == "blocked" else AuthorizationStatus.invalid
            self._last_event_id = cs_db.log_event(self.cp_id, "WARNING", f"StartTransaction REJECTED connector={connector_id} id_tag={id_tag} ({auth})", ocpp_payload=self._last_raw_msg)
            return call_result.StartTransaction(
                transaction_id=0,
                id_tag_info={"status": status},
            )
        txn_id = cs_db.start_transaction(self.cp_id, connector_id, id_tag, meter_start, timestamp)
        # Link transaction to smart schedule if one is active for this CP
        for sched in cs_db.get_active_smart_schedules():
            if sched["cp_id"] == self.cp_id and sched["status"] == "started" and sched["id_tag"] == id_tag:
                cs_db.update_smart_schedule(sched["id"], transaction_id=txn_id)
                break
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"StartTransaction connector={connector_id} id_tag={id_tag} txn_id={txn_id}", ocpp_payload=self._last_raw_msg)
        mail_notify.notify("notify_start_transaction", f"[Charger] Transaction Started on {self.cp_id}", f"Charge point {self.cp_id} started a transaction.\nConnector: {connector_id}\nID Tag: {id_tag}\nTransaction ID: {txn_id}")
        return call_result.StartTransaction(
            transaction_id=txn_id,
            id_tag_info={"status": AuthorizationStatus.accepted},
        )

    @on(Action.stop_transaction)
    async def on_stop_transaction(self, meter_stop, timestamp, transaction_id, **kwargs):
        reason = kwargs.get("reason", "Local")
        cs_db.record_charging_stop(transaction_id, timestamp)
        cs_db.stop_transaction(transaction_id, meter_stop, timestamp, reason)
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"StopTransaction txn_id={transaction_id} meter_stop={meter_stop} reason={reason}", ocpp_payload=self._last_raw_msg)
        mail_notify.notify("notify_end_transaction", f"[Charger] Transaction Ended on {self.cp_id}", f"Charge point {self.cp_id} stopped a transaction.\nTransaction ID: {transaction_id}\nMeter Stop: {meter_stop}\nReason: {reason}")
        cs_db.update_chargepoint_status(self.cp_id, "Available")
        return call_result.StopTransaction(
            id_tag_info={"status": AuthorizationStatus.accepted},
        )

    @on(Action.meter_values)
    async def on_meter_values(self, connector_id, meter_value, transaction_id=None, **kwargs):
        if transaction_id is None:
            transaction_id = kwargs.get("transaction_id")
        # Always resolve to the active transaction's DB id to avoid mismatches
        # (charge point may reference a stale transactionId after server restart or auto-close)
        txn = cs_db.get_active_transaction_for_cp(self.cp_id)
        if txn:
            transaction_id = txn["id"]
        count = 0
        for mv in meter_value:
            ts = mv.get("timestamp", "")
            sampled = mv.get("sampled_value") or mv.get("sampledValue") or []
            for sv in sampled:
                value = sv.get("value", "0")
                measurand = sv.get("measurand", "Energy.Active.Import.Register")
                unit = sv.get("unit", "Wh")
                phase = sv.get("phase")
                logging.info("MeterValue %s txn=%s measurand=%s value=%s unit=%s phase=%s",
                             self.cp_id, transaction_id, measurand, value, unit, phase)
                cs_db.insert_meter_value(self.cp_id, connector_id, transaction_id, ts, measurand, value, unit, phase)
                count += 1
        if count == 0:
            logging.warning("MeterValues %s: no sampled values found. meter_value keys: %s", self.cp_id, [list(mv.keys()) for mv in meter_value])
        self._last_event_id = cs_db.log_event(self.cp_id, "DEBUG", f"MeterValues connector={connector_id} txn={transaction_id} ({count} values)", ocpp_payload=self._last_raw_msg)
        return call_result.MeterValues()

    @on(Action.data_transfer)
    async def on_data_transfer(self, vendor_id, **kwargs):
        message_id = kwargs.get("message_id", "")
        data = kwargs.get("data", "")
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"DataTransfer vendor={vendor_id} msg={message_id}", ocpp_payload=self._last_raw_msg)
        return call_result.DataTransfer(status="Accepted")

    @on(Action.diagnostics_status_notification)
    async def on_diagnostics_status(self, status, **kwargs):
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"DiagnosticsStatusNotification: {status}", ocpp_payload=self._last_raw_msg)
        return call_result.DiagnosticsStatusNotification()

    @on(Action.firmware_status_notification)
    async def on_firmware_status(self, status, **kwargs):
        self._last_event_id = cs_db.log_event(self.cp_id, "INFO", f"FirmwareStatusNotification: {status}", ocpp_payload=self._last_raw_msg)
        return call_result.FirmwareStatusNotification()

    # ── Outgoing to Charge Point (called from web API) ──────────

    def _payload_to_json(self, payload):
        """Serialize an OCPP call/call_result payload to a JSON string."""
        try:
            return json.dumps(payload.__dict__, default=str)
        except Exception:
            return str(payload)

    def _resp_to_json(self, resp):
        """Serialize an OCPP response to a JSON string."""
        try:
            return json.dumps(resp.__dict__, default=str)
        except Exception:
            return str(resp)

    async def remote_start_transaction(self, id_tag, connector_id=None):
        payload = call.RemoteStartTransaction(id_tag=id_tag)
        if connector_id is not None:
            payload.connector_id = connector_id
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"RemoteStartTransaction id_tag={id_tag} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def remote_stop_transaction(self, transaction_id):
        payload = call.RemoteStopTransaction(transaction_id=transaction_id)
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"RemoteStopTransaction txn={transaction_id} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def reset(self, reset_type="Soft"):
        payload = call.Reset(type=reset_type)
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"Reset type={reset_type} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def unlock_connector(self, connector_id=1):
        payload = call.UnlockConnector(connector_id=connector_id)
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"UnlockConnector connector={connector_id} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def change_availability(self, connector_id, av_type):
        payload = call.ChangeAvailability(connector_id=connector_id, type=av_type)
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"ChangeAvailability connector={connector_id} type={av_type} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def get_configuration(self, keys=None):
        payload = call.GetConfiguration(key=keys or [])
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", "GetConfiguration response received", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return {
            "configuration_key": resp.configuration_key or [],
            "unknown_key": resp.unknown_key or [],
        }

    async def change_configuration(self, key, value):
        payload = call.ChangeConfiguration(key=key, value=value)
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"ChangeConfiguration {key}={value} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def clear_cache(self):
        payload = call.ClearCache()
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"ClearCache response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status

    async def trigger_message(self, requested_message, connector_id=None):
        payload = call.TriggerMessage(requested_message=requested_message)
        if connector_id is not None:
            payload.connector_id = connector_id
        resp = await self.call(payload, suppress=False)
        cs_db.log_event(self.cp_id, "INFO", f"TriggerMessage: {requested_message} response: {resp.status}", ocpp_payload=self._payload_to_json(payload), ocpp_response=self._resp_to_json(resp))
        return resp.status


# ── WebSocket server ────────────────────────────────────────────

async def on_connect(websocket):
    """Called for each new charge point connection."""
    try:
        logging.debug(f"Entering on_connect for websocket: {websocket}")
        # Extract CP id from URL path: /ocpp/<cp_id>
        # Support both new websockets (>=13: .request.path) and legacy (<13: .path)
        try:
            path = websocket.request.path
        except AttributeError:
            path = getattr(websocket, 'path', '') or ''

        parts = [p for p in path.split("/") if p]
        # Expected URL: /ocpp/<cp_id> → parts = ["ocpp", "<cp_id>"]
        if len(parts) >= 2:
            cp_id = parts[-1]
        else:
            logging.warning("No charge point ID in path '%s'. "
                            "Charge point must connect to /ocpp/<cp_id>", path)
            cp_id = "UNKNOWN"

        # Verify OCPP subprotocol was negotiated
        subprotocol = getattr(websocket, 'subprotocol', None)
        if subprotocol:
            logging.info("Charge point %s connected (subprotocol=%s)", cp_id, subprotocol)
        else:
            logging.warning("Charge point %s connected WITHOUT ocpp1.6 subprotocol — "
                            "charge point will likely disconnect", cp_id)

        cs_db.upsert_chargepoint(cp_id, status="Online")
        mail_notify.notify("notify_connect", f"[Charger] {cp_id} Connected", f"Charge point {cp_id} connected to the central system.")

        handler = CentralSystemHandler(cp_id, websocket)
        # Close any existing handler for this CP (stale connection from before reboot)
        old_handler = connected_cps.get(cp_id)
        if old_handler is not None and old_handler is not handler:
            logging.info("Replacing stale connection for %s", cp_id)
        connected_cps[cp_id] = handler
        try:
            await handler.start()
        except websockets.exceptions.ConnectionClosed as e:
            logging.info("Charge point %s disconnected: %s", cp_id, e)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logging.error(f"Charge point {cp_id} error: {e}\nTraceback:\n{tb}")
            for h in logging.getLogger().handlers:
                h.flush()
        finally:
            # Only clean up if we are still the active handler (not replaced by a reconnect)
            if connected_cps.get(cp_id) is handler:
                cs_db.update_chargepoint_status(cp_id, "Offline")
                mail_notify.notify("notify_disconnect", f"[Charger] {cp_id} Disconnected", f"Charge point {cp_id} disconnected from the central system.")
                connected_cps.pop(cp_id, None)
                heartbeat_offline_cps.discard(cp_id)
            else:
                logging.info("Charge point %s old connection cleaned up (new connection active)", cp_id)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logging.error(f"Top-level error in on_connect: {e}\nTraceback:\n{tb}")
        for h in logging.getLogger().handlers:
            h.flush()


async def _heartbeat_monitor():
    """Periodically check connected CPs for heartbeat timeout."""
    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)
        for cp_id, handler in list(connected_cps.items()):
            elapsed = (now - handler._last_activity).total_seconds()
            if elapsed > HEARTBEAT_TIMEOUT_SECONDS and cp_id not in heartbeat_offline_cps:
                heartbeat_offline_cps.add(cp_id)
                cs_db.update_chargepoint_status(cp_id, "Offline")
                cs_db.log_event(cp_id, "WARNING", f"No heartbeat for {int(elapsed)}s — presumed offline")
                logging.warning("Charge point %s presumed offline (no heartbeat for %ds)", cp_id, int(elapsed))


async def _smart_schedule_monitor():
    """Periodically check smart charge schedules and trigger remote start/stop."""
    while True:
        await asyncio.sleep(15)
        try:
            schedules = cs_db.get_active_smart_schedules()
        except Exception:
            continue
        now = datetime.now(timezone.utc)
        for s in schedules:
            try:
                start_at = datetime.fromisoformat(s["start_at"])
                stop_at = datetime.fromisoformat(s["stop_at"])
                cp_id = s["cp_id"]

                if s["status"] == "scheduled" and now >= start_at:
                    handler = connected_cps.get(cp_id)
                    if handler:
                        result = await handler.remote_start_transaction(s["id_tag"])
                        if result == "Accepted":
                            cs_db.update_smart_schedule(s["id"], status="started")
                            cs_db.log_event(cp_id, "INFO", f"Smart schedule {s['id']}: remote start sent (id_tag={s['id_tag']})")
                        else:
                            cs_db.log_event(cp_id, "WARNING", f"Smart schedule {s['id']}: remote start rejected ({result})")
                    else:
                        cs_db.log_event(cp_id, "WARNING", f"Smart schedule {s['id']}: charge point not connected, cannot start")

                elif s["status"] == "started" and now >= stop_at:
                    handler = connected_cps.get(cp_id)
                    txn = cs_db.get_active_transaction_for_cp(cp_id)
                    txn_id = s.get("transaction_id") or (txn["id"] if txn else None)
                    if handler and txn_id:
                        result = await handler.remote_stop_transaction(txn_id)
                        cs_db.update_smart_schedule(s["id"], status="completed")
                        cs_db.log_event(cp_id, "INFO", f"Smart schedule {s['id']}: remote stop sent (txn={txn_id}, result={result})")
                    elif not txn_id:
                        cs_db.update_smart_schedule(s["id"], status="completed")
                        cs_db.log_event(cp_id, "WARNING", f"Smart schedule {s['id']}: no active transaction to stop, marking completed")
                    else:
                        cs_db.log_event(cp_id, "WARNING", f"Smart schedule {s['id']}: charge point not connected, cannot stop")
            except Exception as e:
                logging.exception("Smart schedule %s error: %s", s["id"], e)


async def start_server(host="0.0.0.0", port=9000):
    global _ws_server
    cs_db.init_db()
    logging.info("OCPP Central System listening on ws://%s:%s/ocpp", host, port)
    asyncio.create_task(_heartbeat_monitor())
    asyncio.create_task(_smart_schedule_monitor())
    _ws_server = await websockets.serve(
        on_connect,
        host,
        port,
        subprotocols=["ocpp1.6"],
    )


async def restart_server(new_port: int, host="0.0.0.0"):
    """Stop the current WebSocket server and start a new one on *new_port*."""
    global _ws_server
    if _ws_server is not None:
        logging.info("Stopping OCPP server for port change …")
        _ws_server.close()
        await _ws_server.wait_closed()
        _ws_server = None
    logging.info("OCPP Central System restarting on ws://%s:%s/ocpp", host, new_port)
    _ws_server = await websockets.serve(
        on_connect,
        host,
        new_port,
        subprotocols=["ocpp1.6"],
    )
    logging.info("OCPP server now listening on port %s", new_port)
