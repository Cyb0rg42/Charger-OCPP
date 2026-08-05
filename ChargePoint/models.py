"""
Script:   models.py

Abstract:
    Shared runtime state and business logic for the ChargePoint simulator.
    Defines the Config and EmulatorState dataclasses (single in-process
    instances: `config`, `state`) plus the functions that mutate them —
    connect/disconnect cable and car, start/stop a charging session, unlock
    the connector, soft/hard reset. Both chargepoint.py's web GUI and
    cp_sim.py's OCPP client read and call into this module, guarded by a
    shared lock, so the two stay consistent.

Features:
    - EmulatorState: cable/car/lock state, active session, energy/power
      history, per-connector availability and status overrides,
      reservations.
    - Config: station identity, OCPP URL, connector count, id tags,
      vendor/model — persisted to and loaded from db.py.
    - Session bookkeeping (start_charging/stop_charging) backed by db.py.

Usage:
    Imported by chargepoint.py and cp_sim.py; not intended to be run
    directly.

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

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple
import json
import queue
import random
import threading
import time

import db


@dataclass
class Config:
    ocpp_url: str = "ws://localhost:9000/ocpp"
    max_kwh: float = 50.0
    min_kwh: float = 0.0
    station_id: str = "CP_001"
    num_connectors: int = 1
    id_tags: List[str] = field(default_factory=lambda: ["DEFAULT_TAG"])
    charge_point_vendor: str = "DemoVendor"
    charge_point_model: str = "PythonEmu"
    charge_point_meter_type: str = ""


config = Config()
config_lock = threading.Lock()


def _load_config_from_db():
    """Populate the in-memory config from the database."""
    stored = db.load_config()
    if "ocpp_url" in stored:
        config.ocpp_url = stored["ocpp_url"]
    if "max_kwh" in stored:
        config.max_kwh = float(stored["max_kwh"])
    if "min_kwh" in stored:
        config.min_kwh = float(stored["min_kwh"])
    if "num_connectors" in stored:
        config.num_connectors = int(stored["num_connectors"])
    if "id_tags" in stored:
        config.id_tags = json.loads(stored["id_tags"])
    if "charge_point_vendor" in stored:
        config.charge_point_vendor = stored["charge_point_vendor"]
    if "charge_point_model" in stored:
        config.charge_point_model = stored["charge_point_model"]
    if "charge_point_meter_type" in stored:
        config.charge_point_meter_type = stored["charge_point_meter_type"]


def get_config() -> dict:
    with config_lock:
        return {
            "ocpp_url": config.ocpp_url,
            "max_kwh": config.max_kwh,
            "min_kwh": config.min_kwh,
            "station_id": config.station_id,
            "num_connectors": config.num_connectors,
            "id_tags": list(config.id_tags),
            "charge_point_vendor": config.charge_point_vendor,
            "charge_point_model": config.charge_point_model,
            "charge_point_meter_type": config.charge_point_meter_type,
        }


def update_config(ocpp_url: Optional[str] = None,
                  max_kwh: Optional[float] = None,
                  min_kwh: Optional[float] = None,
                  num_connectors: Optional[int] = None,
                  id_tags: Optional[List[str]] = None,
                  charge_point_vendor: Optional[str] = None,
                  charge_point_model: Optional[str] = None,
                  charge_point_meter_type: Optional[str] = None):
    with config_lock:
        if ocpp_url is not None:
            config.ocpp_url = ocpp_url
            db.save_config("ocpp_url", ocpp_url)
        if max_kwh is not None:
            config.max_kwh = float(max_kwh)
            db.save_config("max_kwh", str(max_kwh))
        if min_kwh is not None:
            config.min_kwh = float(min_kwh)
            db.save_config("min_kwh", str(min_kwh))
        if num_connectors is not None:
            config.num_connectors = int(num_connectors)
            db.save_config("num_connectors", str(num_connectors))
        if id_tags is not None:
            config.id_tags = list(id_tags)
            db.save_config("id_tags", json.dumps(id_tags))
        if charge_point_vendor is not None:
            config.charge_point_vendor = str(charge_point_vendor)
            db.save_config("charge_point_vendor", charge_point_vendor)
        if charge_point_model is not None:
            config.charge_point_model = str(charge_point_model)
            db.save_config("charge_point_model", charge_point_model)
        if charge_point_meter_type is not None:
            config.charge_point_meter_type = str(charge_point_meter_type)
            db.save_config("charge_point_meter_type", charge_point_meter_type)


@dataclass
class ChargeSession:
    id: int
    started_at: datetime
    connector_id: int = 1
    ended_at: Optional[datetime] = None
    energy_kwh: float = 0.0
    transaction_id: Optional[int] = None


@dataclass
class EmulatorState:
    cable_connected: bool = False
    car_connected: bool = False
    # Physical connector lock: engages when the cable is plugged in, released
    # by an accepted UnlockConnector command (or when the cable is unplugged).
    connector_locked: bool = False
    charging: bool = False
    session_finishing: bool = False
    active_connector_id: int = 1
    current_session: Optional[ChargeSession] = None
    sessions: List[ChargeSession] = field(default_factory=list)
    power_history: List[Tuple[str, float]] = field(default_factory=list)
    power_kw: float = 0.0
    current_a_l1: float = 0.0
    current_a_l2: float = 0.0
    current_a_l3: float = 0.0
    _last_energy_ts: Optional[datetime] = field(default=None, repr=False)
    # Per-connector availability override set by ChangeAvailability.
    # Key = connector_id (0 means whole CP), value = OCPP 1.6 availability type
    # ("Operative" or "Inoperative").  Empty dict = no override.
    connector_availability: dict = field(default_factory=dict)
    # Per-connector status override that must be restored after a charging
    # session ends. Key = connector_id, value = OCPP 1.6 status
    # ("Unavailable" or "Reserved"). Empty dict = no override.
    connector_status_override: dict = field(default_factory=dict)
    # Active reservations: reservation_id → connector_id (set by ReserveNow,
    # cleared by CancelReservation).
    reservations: dict = field(default_factory=dict)

    def total_energy(self) -> float:
        return sum(s.energy_kwh for s in self.sessions)

state = EmulatorState()
lock = threading.Lock()
session_counter = 0


def _load_sessions_from_db():
    """Restore previous sessions from the database."""
    global session_counter
    rows = db.load_sessions()
    for r in rows:
        ended = None
        if r["ended_at"]:
            ended = datetime.fromisoformat(r["ended_at"])
        sess = ChargeSession(
            id=r["id"],
            started_at=datetime.fromisoformat(r["started_at"]),
            ended_at=ended,
            energy_kwh=r["energy_kwh"],
            transaction_id=r["transaction_id"],
        )
        state.sessions.append(sess)
    session_counter = db.get_next_session_id() - 1


def init_models():
    """Initialise DB, then load config and sessions into memory."""
    db.init_db()
    _load_config_from_db()
    _load_sessions_from_db()

# Event queue for OCPP coordination (thread-safe)
ocpp_events: queue.Queue = queue.Queue()

def connect_cable(connector_id: int = 1):
    with lock:
        state.cable_connected = True
        # Connecting the cable also connects the car.
        state.car_connected = True
        # Plugging in engages the physical connector lock.
        state.connector_locked = True
        state.active_connector_id = connector_id
    ocpp_events.put(("status", None))


def disconnect_cable():
    with lock:
        session_id = state.current_session.id if state.current_session else None
        state.cable_connected = False
        state.car_connected = False
        state.connector_locked = False
        _stop_session_locked()
        state.session_finishing = False
    if session_id is not None:
        ocpp_events.put(("stop", session_id))
    ocpp_events.put(("status", None))


def connect_car():
    with lock:
        if state.cable_connected:
            state.car_connected = True


def disconnect_car():
    with lock:
        session_id = state.current_session.id if state.current_session else None
        state.car_connected = False
        _stop_session_locked()
        state.session_finishing = False
    if session_id is not None:
        ocpp_events.put(("stop", session_id))
        ocpp_events.put(("status", None))


def start_charging():
    global session_counter
    with lock:
        if not (state.cable_connected and state.car_connected):
            return False
        if state.charging:
            return True
        session_counter += 1
        sid = session_counter
        now = datetime.now(timezone.utc)
        connector_id = state.active_connector_id
        sess = ChargeSession(id=sid, started_at=now, connector_id=connector_id, energy_kwh=0.0)
        state.current_session = sess
        state.sessions.append(sess)
        state.charging = True
        state.session_finishing = False
        state._last_energy_ts = now
        state.power_history.clear()
        state.power_history.append((now.isoformat(), 0.0))
        db.insert_session(sid, now.isoformat())
    ocpp_events.put(("start", sid))
    return True


def stop_charging():
    with lock:
        session_id = state.current_session.id if state.current_session else None
        _stop_session_locked()
    if session_id is not None:
        ocpp_events.put(("stop", session_id))
        ocpp_events.put(("status", None))


def _stop_session_locked():
    if state.current_session and not state.current_session.ended_at:
        state.current_session.ended_at = datetime.now(timezone.utc)
        db.update_session(
            state.current_session.id,
            ended_at=state.current_session.ended_at.isoformat(),
            energy_kwh=state.current_session.energy_kwh,
        )
    state.current_session = None
    state.charging = False
    state.session_finishing = True


def update_live_energy():
    """Accumulate energy based on simulated power draw."""
    with lock:
        if state.current_session and state.charging and not state.current_session.ended_at:
            with config_lock:
                lo = config.min_kwh
                hi = config.max_kwh
            # Simulate fluctuating power draw (kW)
            state.power_kw = round(random.uniform(lo, hi), 2)
            total_a = state.power_kw * 1000 / 230
            state.current_a_l1 = round(total_a / 3 + random.uniform(-0.5, 0.5), 1)
            state.current_a_l2 = round(total_a / 3 + random.uniform(-0.5, 0.5), 1)
            state.current_a_l3 = round(total_a / 3 + random.uniform(-0.5, 0.5), 1)
            # Accumulate energy: power × elapsed time
            now = datetime.now(timezone.utc)
            if state._last_energy_ts:
                dt_h = (now - state._last_energy_ts).total_seconds() / 3600
                state.current_session.energy_kwh += round(state.power_kw * dt_h, 6)
                state.current_session.energy_kwh = round(state.current_session.energy_kwh, 3)
            state._last_energy_ts = now
            state.power_history.append((now.isoformat(), state.current_session.energy_kwh))
            # Trim history to last 60 seconds
            cutoff = (now - timedelta(seconds=60)).isoformat()
            state.power_history = [
                p for p in state.power_history if p[0] >= cutoff
            ]
            db.update_session(state.current_session.id, energy_kwh=state.current_session.energy_kwh)
        else:
            state.power_kw = 0.0
            state.current_a_l1 = 0.0
            state.current_a_l2 = 0.0
            state.current_a_l3 = 0.0


def _energy_accumulation_loop():
    while True:
        update_live_energy()
        time.sleep(random.randint(1, 10))


def unlock_connector(connector_id: int = 1):
    """Unlock the specified connector (simulated).

    Releases the physical connector lock so the cable can be removed. The
    lock re-engages the next time a cable is plugged in (connect_cable()).
    """
    with lock:
        if state.active_connector_id == connector_id and state.cable_connected:
            state.connector_locked = False
    ocpp_events.put(("status", None))


def soft_reset():
    """Perform a soft reset - stop charging but maintain state."""
    stop_charging()
    ocpp_events.put(("status", None))


def hard_reset():
    """Perform a hard reset - stop charging and clear all state."""
    with lock:
        session_id = state.current_session.id if state.current_session else None
        _stop_session_locked()
        state.cable_connected = False
        state.car_connected = False
        state.connector_locked = False
        state.active_connector_id = 1
        state.power_kw = 0.0
        state.current_a_l1 = 0.0
        state.current_a_l2 = 0.0
        state.current_a_l3 = 0.0
    if session_id is not None:
        ocpp_events.put(("stop", session_id))
    ocpp_events.put(("status", None))


threading.Thread(target=_energy_accumulation_loop, daemon=True).start()

