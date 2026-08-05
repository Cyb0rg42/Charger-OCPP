"""
Script:   cp_sim.py

Abstract:
    OCPP 1.6-J charge-point-side client for the ChargePoint simulator.
    Connects outbound over WebSocket to a Central System (directly, or via
    OCPPProxy), sends BootNotification/Heartbeat/StatusNotification/
    MeterValues/StartTransaction/StopTransaction, and answers CSMS-initiated
    remote commands. Runs as a background asyncio loop started by
    chargepoint.py; reads and mutates the shared simulator state in
    models.py that the web GUI also reads/writes.

Features:
    - Full connect/reconnect loop with automatic retry.
    - Handles RemoteStartTransaction, RemoteStopTransaction, Reset,
      UnlockConnector, ChangeAvailability, GetConfiguration,
      ChangeConfiguration, ClearCache, TriggerMessage, ReserveNow,
      CancelReservation.
    - Simulates a physical connector lock and cable/car presence.
    - Per-connector OCPP status derivation (Available/Preparing/
      SuspendedEV/Charging/Finishing) from the shared EmulatorState.

Usage:
    Imported and driven by chargepoint.py (cp_main / configure_logging);
    not intended to be run directly.

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
import json as _json
import logging
import logging.handlers
import os
import queue
from datetime import datetime, timezone

import websockets
from ocpp.routing import on
from ocpp.v16 import call, call_result
from ocpp.v16 import ChargePoint as Cp
from ocpp.v16.enums import RegistrationStatus, ChargePointStatus

from models import state, lock, ocpp_events, start_charging, stop_charging, config, config_lock, unlock_connector, soft_reset, hard_reset
import db


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        return _json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "message": record.getMessage(),
        })


# ── Default log paths (overridden by configure_logging) ────────
_OCPP_LOG = "/var/log/charger/stationocpp.log"
_ACCESS_LOG = "/var/log/charger/access.log"
_ERROR_LOG = "/var/log/charger/error.log"

_ocpp_logger = logging.getLogger("station")
_station_logger = logging.getLogger("station")


def configure_logging(ocpp_log_path: str | None = None,
                      access_log_path: str | None = None,
                      error_log_path: str | None = None):
    """Set up all loggers.  Call once before the OCPP loop starts."""
    global _OCPP_LOG, _ACCESS_LOG, _ERROR_LOG

    if ocpp_log_path:
        _OCPP_LOG = ocpp_log_path
    if access_log_path:
        _ACCESS_LOG = access_log_path
    if error_log_path:
        _ERROR_LOG = error_log_path

    # Ensure directories exist
    for p in (_OCPP_LOG, _ACCESS_LOG, _ERROR_LOG):
        os.makedirs(os.path.dirname(p), exist_ok=True)

    # ── Root / access logger ───────────────────────────────────
    _access_handler = logging.FileHandler(_ACCESS_LOG, encoding="utf-8")
    _access_handler.setFormatter(_JsonFormatter())
    _access_handler.setLevel(logging.DEBUG)

    # ── Error logger ───────────────────────────────────────────
    _error_handler = logging.FileHandler(_ERROR_LOG, encoding="utf-8")
    _error_handler.setFormatter(_JsonFormatter())
    _error_handler.setLevel(logging.ERROR)

    logging.basicConfig(level=logging.DEBUG, handlers=[
        logging.StreamHandler(),
        _access_handler,
        _error_handler,
    ])

    # ── OCPP wire logger ───────────────────────────────────────
    _ocpp_logger.setLevel(logging.DEBUG)
    _ocpp_logger.propagate = False
    # Remove any existing handlers to avoid duplicates on re-configure
    _ocpp_logger.handlers.clear()

    _ocpp_wire_handler = logging.FileHandler(_OCPP_LOG, encoding="utf-8")
    _ocpp_wire_handler.setFormatter(_JsonFormatter())
    _ocpp_logger.addHandler(_ocpp_wire_handler)

    # ── Station JSON logger (same logger, additional handler) ──
    _station_logger.handlers.clear()
    _station_logger.setLevel(logging.DEBUG)
    _station_logger.propagate = False
    _station_logger.addHandler(_ocpp_wire_handler)


# Apply defaults so the module works even without an explicit call
configure_logging()

# Flag to signal the OCPP loop to reconnect (e.g. after config change)
_reconnect_event = asyncio.Event()


def ocpp_log(severity: str, message: str):
    """Write an OCPP log entry to the database and JSON file."""
    ts = datetime.now(timezone.utc).isoformat()
    db.insert_ocpp_log(ts, severity, message)
    # Also log to JSON file
    level = getattr(logging, severity.upper(), logging.INFO)
    _station_logger.log(level, message)


# ── Simulated OCPP configuration store (GetConfiguration / ChangeConfiguration) ──
# In-memory only (resets on restart, like the rest of this simulator's runtime
# state). NumberOfConnectors is deliberately NOT stored here — it's always
# read live from models.config.num_connectors so it can't drift out of sync;
# see _get_ocpp_config_snapshot().
_ocpp_config = {
    "HeartbeatInterval": {"value": "10", "readonly": False},
    "MeterValueSampleInterval": {"value": "30", "readonly": False},
    "ConnectionTimeOut": {"value": "30", "readonly": False},
    "GetConfigurationMaxKeys": {"value": "50", "readonly": True},
    "SupportedFeatureProfiles": {"value": "Core", "readonly": True},
    "AuthorizeRemoteTxRequests": {"value": "false", "readonly": False},
    "StopTransactionOnEVSideDisconnect": {"value": "true", "readonly": False},
    "UnlockConnectorOnEVSideDisconnect": {"value": "true", "readonly": False},
    "TransactionMessageAttempts": {"value": "3", "readonly": False},
    "TransactionMessageRetryInterval": {"value": "60", "readonly": False},
    "LocalAuthorizeOffline": {"value": "false", "readonly": False},
    "LocalPreAuthorize": {"value": "false", "readonly": False},
}


def _get_ocpp_config_snapshot():
    """Return {key: (value, readonly)} including the live NumberOfConnectors."""
    with config_lock:
        num_connectors = config.num_connectors
    with lock:
        snapshot = {k: (v["value"], v["readonly"]) for k, v in _ocpp_config.items()}
    snapshot["NumberOfConnectors"] = (str(num_connectors), True)
    return snapshot


class ChargePoint(Cp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_transaction_id = None
        self._last_connector_status: dict[int, str] = {}  # track last sent status per connector

    # ── OCPP wire logging ───────────────────────────────────────

    async def _send(self, message):
        _ocpp_logger.debug("[station] TX %s", message)
        return await super()._send(message)

    async def route_message(self, raw_msg):
        _ocpp_logger.debug("[station] RX %s", raw_msg)
        return await super().route_message(raw_msg)

    # --- Outgoing messages ---

    async def send_boot_notification(self):
        with config_lock:
            vendor = config.charge_point_vendor
            model = config.charge_point_model
            meter_type = config.charge_point_meter_type
        boot_kwargs = dict(
            charge_point_model=model,
            charge_point_vendor=vendor,
        )
        if meter_type:
            boot_kwargs["meter_type"] = meter_type
        req = call.BootNotification(**boot_kwargs)
        ocpp_log("INFO", "Sending BootNotification")
        try:
            resp = await self.call(req)
        except Exception as e:
            ocpp_log("ERROR", f"BootNotification failed: {e}")
            return None
        if resp is None:
            ocpp_log("ERROR", "BootNotification returned no response")
            return None
        ocpp_log("INFO", f"BootNotification result: {resp.status}")
        return resp

    async def send_status_notification(self):
        with config_lock:
            num_connectors = config.num_connectors
        with lock:
            if not state.cable_connected:
                status = ChargePointStatus.available
            elif state.cable_connected and not state.car_connected:
                status = ChargePointStatus.preparing
            elif state.session_finishing:
                status = ChargePointStatus.finishing
            elif state.car_connected and not state.charging:
                status = ChargePointStatus.suspended_ev
            else:
                status = ChargePointStatus.charging
        with lock:
            active_connector = state.active_connector_id
            avail = dict(state.connector_availability)
            overrides = dict(state.connector_status_override)
            session_in_progress = state.charging or state.session_finishing

        def _forced_status(cid):
            """Idle status forced for a connector (Unavailable / Reserved),
            or None when no override is configured."""
            if avail.get(cid) == "Inoperative":
                return ChargePointStatus.unavailable
            ov = overrides.get(cid)
            if ov == "Reserved":
                return ChargePointStatus.reserved
            if ov == "Unavailable":
                return ChargePointStatus.unavailable
            return None

        # Connector 0 = the charge point itself (OCPP 1.6 §5.11)
        # Also include any connectors set via ChangeAvailability / ReserveNow
        # that might be beyond the configured num_connectors.
        all_connectors = (
            set(range(0, num_connectors + 1))
            | set(avail.keys())
            | set(overrides.keys())
        )
        for connector_id in sorted(all_connectors):
            forced = _forced_status(connector_id)
            if connector_id != 0 and connector_id == active_connector and session_in_progress:
                # An active charging session takes priority over an
                # Unavailable / Reserved override. The override is restored
                # automatically once the session is stopped or terminated.
                c_status = status
            elif forced is not None:
                c_status = forced
            elif connector_id == 0:
                c_status = ChargePointStatus.available
            elif connector_id == active_connector:
                c_status = status
            else:
                c_status = ChargePointStatus.available
            # Only send if the status for this connector actually changed
            if self._last_connector_status.get(connector_id) == c_status:
                continue
            req = call.StatusNotification(
                connector_id=connector_id,
                error_code="NoError",
                status=c_status,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            ocpp_log("INFO", f"Sending StatusNotification connector {connector_id}: {c_status}")
            await self.call(req)
            self._last_connector_status[connector_id] = c_status

    async def send_heartbeat(self):
        req = call.Heartbeat()
        ocpp_log("DEBUG", "Sending Heartbeat")
        await self.call(req)

    async def send_start_transaction(self, session_id):
        with lock:
            session = next(
                (s for s in state.sessions if s.id == session_id), None
            )
            if not session:
                logging.warning(
                    "StartTransaction: session %s not found", session_id
                )
                ocpp_log("WARNING", f"StartTransaction: session {session_id} not found")
                return
            meter_start = int(session.energy_kwh * 1000)
            connector_id = session.connector_id

        with config_lock:
            id_tag = config.id_tags[0] if config.id_tags else "DEFAULT_TAG"

        req = call.StartTransaction(
            connector_id=connector_id,
            id_tag=id_tag,
            meter_start=meter_start,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        ocpp_log("INFO", f"Sending StartTransaction for session {session_id}")
        resp = await self.call(req)
        ocpp_log("INFO", f"StartTransaction accepted, transaction_id={resp.transaction_id}")
        self._active_transaction_id = resp.transaction_id
        with lock:
            session = next(
                (s for s in state.sessions if s.id == session_id), None
            )
            if session:
                session.transaction_id = resp.transaction_id
                db.update_session(session_id, transaction_id=resp.transaction_id)

    async def send_stop_transaction(self, session_id):
        with lock:
            session = next(
                (s for s in state.sessions if s.id == session_id), None
            )
            if not session:
                logging.warning(
                    "StopTransaction: session %s not found", session_id
                )
                ocpp_log("WARNING", f"StopTransaction: session {session_id} not found")
                return
            energy_wh = int(session.energy_kwh * 1000)
            transaction_id = (
                session.transaction_id or self._active_transaction_id
            )

        if not transaction_id:
            logging.warning(
                "StopTransaction: no transaction_id for session %s",
                session_id,
            )
            ocpp_log("WARNING", f"StopTransaction: no transaction_id for session {session_id}")
            return

        req = call.StopTransaction(
            meter_stop=energy_wh,
            timestamp=datetime.now(timezone.utc).isoformat(),
            transaction_id=transaction_id,
            reason="Local",
        )
        ocpp_log("INFO", f"Sending StopTransaction for session {session_id} (txn {transaction_id})")
        await self.call(req)
        self._active_transaction_id = None

    async def send_meter_values(self):
        with lock:
            if not state.current_session:
                return
            energy_wh = int(state.current_session.energy_kwh * 1000)
            power_kw = state.power_kw
            current_l1 = state.current_a_l1
            current_l2 = state.current_a_l2
            current_l3 = state.current_a_l3
            transaction_id = self._active_transaction_id
            active_connector = state.active_connector_id

        req = call.MeterValues(
            connector_id=active_connector,
            meter_value=[
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "sampled_value": [
                        {
                            "value": str(energy_wh),
                            "measurand": "Energy.Active.Import.Register",
                            "unit": "Wh",
                        },
                        {
                            "value": str(power_kw),
                            "measurand": "Power.Active.Import",
                            "unit": "kW",
                        },
                        {
                            "value": str(current_l1),
                            "measurand": "Current.Import",
                            "unit": "A",
                            "phase": "L1",
                        },
                        {
                            "value": str(current_l2),
                            "measurand": "Current.Import",
                            "unit": "A",
                            "phase": "L2",
                        },
                        {
                            "value": str(current_l3),
                            "measurand": "Current.Import",
                            "unit": "A",
                            "phase": "L3",
                        },
                    ],
                }
            ],
            transaction_id=transaction_id,
        )
        logging.info("Sending MeterValues: %s Wh, %s kW, L1=%s A L2=%s A L3=%s A",
                     energy_wh, power_kw, current_l1, current_l2, current_l3)
        ocpp_log("INFO", f"Sending MeterValues: {energy_wh} Wh, {power_kw} kW, "
                 f"L1={current_l1} A L2={current_l2} A L3={current_l3} A")
        await self.call(req)

    # --- Incoming message handlers ---

    @on("RemoteStartTransaction")
    async def on_remote_start(self, id_tag, connector_id=None, **kwargs):
        ocpp_log("INFO", f"Received RemoteStartTransaction (id_tag={id_tag}, connector_id={connector_id})")
        with lock:
            cable_ok = state.cable_connected
            car_ok = state.car_connected
            already_charging = state.charging
        
        if already_charging:
            ocpp_log("WARNING", "RemoteStartTransaction rejected: already charging")
            return call_result.RemoteStartTransaction(status="Rejected")
        
        if not cable_ok:
            ocpp_log("WARNING", "RemoteStartTransaction rejected: cable not connected")
            return call_result.RemoteStartTransaction(status="Rejected")
        
        if not car_ok:
            ocpp_log("WARNING", "RemoteStartTransaction rejected: car not connected")
            return call_result.RemoteStartTransaction(status="Rejected")
        
        ok = start_charging()
        status = "Accepted" if ok else "Rejected"
        ocpp_log("INFO", f"RemoteStartTransaction -> {status}")
        return call_result.RemoteStartTransaction(status=status)

    @on("RemoteStopTransaction")
    async def on_remote_stop(self, transaction_id, **kwargs):
        ocpp_log("INFO", f"Received RemoteStopTransaction (transaction_id={transaction_id})")
        stop_charging()
        return call_result.RemoteStopTransaction(status="Accepted")

    @on("Reset")
    async def on_reset(self, type, **kwargs):
        ocpp_log("INFO", f"Received Reset (type={type})")
        if type == "Hard":
            ocpp_log("INFO", "Executing Hard Reset - stopping charging and resetting state")
            hard_reset()
            ocpp_events.put(("hard_reset", None))
        elif type == "Soft":
            ocpp_log("INFO", "Executing Soft Reset - stopping charging")
            soft_reset()
            ocpp_events.put(("soft_reset", None))
        return call_result.Reset(status="Accepted")

    @on("UnlockConnector")
    async def on_unlock_connector(self, connector_id, **kwargs):
        ocpp_log("INFO", f"Received UnlockConnector (connector_id={connector_id})")
        unlock_connector(connector_id)
        ocpp_events.put(("unlock", connector_id))
        return call_result.UnlockConnector(status="Unlocked")

    @on("ChangeAvailability")
    async def on_change_availability(self, connector_id, type, **kwargs):
        ocpp_log("INFO", f"Received ChangeAvailability (connector_id={connector_id}, type={type})")
        with config_lock:
            num_connectors = config.num_connectors
        with lock:
            if connector_id == 0:
                # Connector 0 = whole charge point: apply to every connector
                for cid in range(0, num_connectors + 1):
                    state.connector_availability[cid] = type
            else:
                state.connector_availability[connector_id] = type
        ocpp_events.put(("status", None))
        return call_result.ChangeAvailability(status="Accepted")

    @on("GetConfiguration")
    async def on_get_configuration(self, key=None, **kwargs):
        ocpp_log("INFO", f"Received GetConfiguration (key={key})")
        all_keys = _get_ocpp_config_snapshot()

        if key:
            configuration_key = [
                {"key": k, "readonly": all_keys[k][1], "value": all_keys[k][0]}
                for k in key if k in all_keys
            ]
            unknown_key = [k for k in key if k not in all_keys]
        else:
            configuration_key = [
                {"key": k, "readonly": ro, "value": v} for k, (v, ro) in all_keys.items()
            ]
            unknown_key = []

        return call_result.GetConfiguration(configuration_key=configuration_key, unknown_key=unknown_key)

    @on("ChangeConfiguration")
    async def on_change_configuration(self, key, value, **kwargs):
        ocpp_log("INFO", f"Received ChangeConfiguration (key={key}, value={value})")

        if key == "NumberOfConnectors":
            # Reported dynamically from config.num_connectors — not settable
            # via OCPP (there's no ChangeConfiguration-only way to reshape
            # the simulator's connector layout).
            status = "Rejected"
        else:
            with lock:
                entry = _ocpp_config.get(key)
                if entry is None:
                    status = "NotSupported"
                elif entry["readonly"]:
                    status = "Rejected"
                else:
                    entry["value"] = value
                    status = "Accepted"

        ocpp_log("INFO", f"ChangeConfiguration {key}={value} -> {status}")
        return call_result.ChangeConfiguration(status=status)

    @on("ClearCache")
    async def on_clear_cache(self, **kwargs):
        # This simulator doesn't maintain a local authorization cache to
        # actually clear, so there's nothing to do beyond acknowledging —
        # but it must respond, or the CSMS-side call raises a CallError.
        ocpp_log("INFO", "Received ClearCache -> Accepted (no local auth cache is kept)")
        return call_result.ClearCache(status="Accepted")

    @on("TriggerMessage")
    async def on_trigger_message(self, requested_message, connector_id=None, **kwargs):
        ocpp_log("INFO", f"Received TriggerMessage (requested_message={requested_message}, connector_id={connector_id})")
        senders = {
            "BootNotification": self.send_boot_notification,
            "StatusNotification": self.send_status_notification,
            "Heartbeat": self.send_heartbeat,
            "MeterValues": self.send_meter_values,
        }
        fn = senders.get(requested_message)
        if fn is None:
            return call_result.TriggerMessage(status="NotImplemented")
        # The triggered message must be sent as a separate, later Call — not
        # embedded in this CallResult — so schedule it instead of awaiting it.
        asyncio.create_task(fn())
        return call_result.TriggerMessage(status="Accepted")

    @on("ReserveNow")
    async def on_reserve_now(self, connector_id, expiry_date, id_tag, reservation_id, **kwargs):
        ocpp_log("INFO", f"Received ReserveNow (connector_id={connector_id}, reservation_id={reservation_id})")
        with lock:
            state.connector_status_override[connector_id] = "Reserved"
            state.reservations[reservation_id] = connector_id
        ocpp_events.put(("status", None))
        return call_result.ReserveNow(status="Accepted")

    @on("CancelReservation")
    async def on_cancel_reservation(self, reservation_id, **kwargs):
        ocpp_log("INFO", f"Received CancelReservation (reservation_id={reservation_id})")
        with lock:
            connector_id = state.reservations.pop(reservation_id, None)
            if connector_id is None:
                return call_result.CancelReservation(status="Rejected")
            if state.connector_status_override.get(connector_id) == "Reserved":
                del state.connector_status_override[connector_id]
        ocpp_events.put(("status", None))
        return call_result.CancelReservation(status="Accepted")

    # --- Event processing ---

    async def _process_events(self):
        while True:
            try:
                event_type, data = ocpp_events.get_nowait()
            except queue.Empty:
                break
            try:
                if event_type == "start":
                    await self.send_start_transaction(data)
                    # Transition the connector to Charging once the
                    # transaction has started.
                    await self.send_status_notification()
                elif event_type == "stop":
                    await self.send_stop_transaction(data)
                    # Transition Finishing → Available
                    with lock:
                        state.session_finishing = False
                    await self.send_status_notification()
                elif event_type == "status":
                    await self.send_status_notification()
                elif event_type == "register":
                    # Trigger a full reconnect so a fresh
                    # BootNotification is sent with new config.
                    _reconnect_event.set()
                elif event_type == "soft_reset":
                    await self.send_status_notification()
                elif event_type == "hard_reset":
                    await self.send_status_notification()
                elif event_type == "unlock":
                    await self.send_status_notification()
            except Exception:
                logging.exception(
                    "Error processing OCPP event %s", event_type
                )
                ocpp_log("ERROR", f"Error processing OCPP event {event_type}")

    def _drain_register_events(self):
        """Consume any queued register events (used during boot loop)."""
        reconnect = False
        while True:
            try:
                event_type, _ = ocpp_events.get_nowait()
                if event_type == "register":
                    reconnect = True
            except queue.Empty:
                break
        return reconnect

    # --- Main loop ---

    async def run(self):
        # Boot registration loop per OCPP 1.6 §5.2:
        # - Accepted: proceed to normal operation
        # - Pending: retry BootNotification at the given interval,
        #            do not send any other messages
        # - Rejected: retry at the given interval
        while True:
            boot = await self.send_boot_notification()

            if boot is None:
                ocpp_log("WARNING", "BootNotification failed, retrying in 10s")
                interval = 10
            else:
                interval = max(boot.interval, 5)  # safety floor

                if boot.status == RegistrationStatus.accepted:
                    ocpp_log("INFO", "Boot accepted, entering normal operation")
                    break

                if boot.status == RegistrationStatus.pending:
                    ocpp_log("INFO", f"Boot pending, retrying in {interval}s")
                else:
                    ocpp_log("WARNING", f"Boot rejected, retrying in {interval}s")

            # Wait for the server-specified interval, but allow
            # an early exit if a reconnect is requested.
            try:
                await asyncio.wait_for(
                    _reconnect_event.wait(), timeout=interval
                )
                # Reconnect requested — bubble up to main() for a
                # fresh WebSocket connection.
                _reconnect_event.clear()
                raise _ReconnectRequested()
            except asyncio.TimeoutError:
                pass

            # Also check if a register event came in while waiting.
            if self._drain_register_events():
                _reconnect_event.clear()
                raise _ReconnectRequested()

        # --- Accepted: send initial StatusNotification and enter loop ---
        await self.send_status_notification()

        meter_counter = 0
        while True:
            await self._process_events()

            meter_counter += 1
            if meter_counter >= 3:  # Every ~30s
                with lock:
                    is_charging = state.charging
                if is_charging:
                    await self.send_meter_values()
                meter_counter = 0

            await self.send_heartbeat()
            await asyncio.sleep(10)

            # Check if a reconnect was requested (e.g. Register button)
            if _reconnect_event.is_set():
                _reconnect_event.clear()
                raise _ReconnectRequested()


class _ReconnectRequested(Exception):
    """Raised to break out of the run loop and reconnect."""


async def main():
    while True:
        with config_lock:
            url = config.ocpp_url.rstrip("/")
            station_id = config.station_id
        ws_url = f"{url}/{station_id}"
        try:
            logging.info(
                "Connecting to central system at %s", ws_url
            )
            ocpp_log("INFO", f"Connecting to central system at {ws_url}")
            async with websockets.connect(
                ws_url, subprotocols=["ocpp1.6"]
            ) as ws:
                cp = ChargePoint(station_id, ws)
                await asyncio.gather(cp.start(), cp.run())
        except _ReconnectRequested:
            ocpp_log("INFO", "Reconnecting due to registration request...")
            continue
        except (
            websockets.exceptions.ConnectionClosed,
            ConnectionRefusedError,
            OSError,
        ) as e:
            logging.error(
                "OCPP connection failed: %s. Retrying in 5s...", e
            )
            ocpp_log("ERROR", f"OCPP connection failed: {e}. Retrying in 5s...")
            # Check for reconnect event while waiting, to avoid delay
            try:
                await asyncio.wait_for(
                    _reconnect_event.wait(), timeout=5
                )
                _reconnect_event.clear()
            except asyncio.TimeoutError:
                pass


if __name__ == "__main__":
    asyncio.run(main())

