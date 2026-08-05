"""
Script:   db.py

Abstract:
    SQLite persistence layer for the ChargePoint simulator. Owns a single
    thread-local connection to the station's own database file (path
    overridable via set_db_path(), typically set from the instance's JSON
    config) and provides the schema and CRUD helpers for its runtime
    configuration, charging sessions, and OCPP wire log.

Features:
    - Thread-local SQLite connection with WAL journal mode.
    - Schema creation/migration on init_db().
    - Config key/value store (ocpp_url, station identity, id tags, ...).
    - Charging session persistence and OCPP log storage/search.

Usage:
    Imported by chargepoint.py, cp_sim.py, and models.py; not intended to be
    run directly.

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

import sqlite3
import threading
from typing import Optional, List

DB_PATH = "/opt/charger/data/chargepoint.db"
_local = threading.local()


def set_db_path(path: str):
    """Override the database file path (call before any DB access)."""
    global DB_PATH
    DB_PATH = path


def _get_conn() -> sqlite3.Connection:
    """Return a thread-local database connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
    return _local.conn


def init_db():
    """Create the database and tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id             INTEGER PRIMARY KEY,
            started_at     TEXT    NOT NULL,
            ended_at       TEXT,
            energy_kwh     REAL    NOT NULL DEFAULT 0.0,
            transaction_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS ocpp_logs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT    NOT NULL,
            severity  TEXT    NOT NULL,
            message   TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS equipment_vendors (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS equipment_models (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER NOT NULL REFERENCES equipment_vendors(id) ON DELETE CASCADE,
            name      TEXT NOT NULL,
            UNIQUE(vendor_id, name)
        );

        CREATE TABLE IF NOT EXISTS meter_types (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
    """)
    conn.commit()
    _seed_equipment(conn)


def _seed_equipment(conn):
    """Insert default vendors/models and meter types if those tables are empty."""
    _seed_meter_types(conn)

    row = conn.execute("SELECT COUNT(*) FROM equipment_vendors").fetchone()
    if row[0] > 0:
        return

    vendors_models = {
        "ABB": ["Terra AC W7-T-0", "Terra AC W11-T-0", "Terra AC W22-T-0",
                "Terra DC", "Terra 54 CG", "Terra 124", "Terra 184"],
        "Alfen": ["Eve Single S-line", "Eve Single Pro-line",
                  "Eve Double Pro-line", "Eve Double PG-line"],
        "Amperfied": ["connect.home", "connect.business", "connect.solar"],
        "Autel": ["MaxiCharger AC Wallbox", "MaxiCharger AC Elite",
                  "MaxiCharger DC Compact", "MaxiCharger DC Fast"],
        "Bender": ["CC612", "CC613"],
        "ChargePoint": ["CP4311", "CP4321", "CP6000", "Express Plus"],
        "Compleo": ["eBOX smart", "eBOX touch", "CITO 500"],
        "Delta": ["AC Mini", "AC Mini Plus", "AC Max"],
        "Easee": ["Easee Home", "Easee Charge", "Easee Core"],
        "Ensto": ["Chago eFiller", "Chago eNext", "Chago ePole"],
        "EVBox": ["Elvi", "BusinessLine", "Troniq Modular", "Ultroniq"],
        "EVTEC": ["sospeso:duo", "sospeso:piu"],
        "Fronius": ["Wattpilot Home", "Wattpilot Go"],
        "GARO": ["Entity", "Entity Pro", "LS4 Twin"],
        "go-e": ["Charger Gemini", "Charger Gemini flex", "Controller"],
        "Heidelberg": ["Energy Control", "Wallbox Home Eco"],
        "KEBA": ["KeContact P30 a-series", "KeContact P30 c-series",
                 "KeContact P30 x-series", "KeContact P40"],
        "Mennekes": ["AMTRON Charge Control", "AMTRON Compact 2.0s",
                     "AMTRON Professional", "AMEDIO Professional+"],
        "myenergi": ["zappi v2"],
        "Schneider Electric": ["EVlink Home", "EVlink City", "EVlink Pro AC",
                               "EVlink Parking"],
        "SMA": ["EV Charger 7.4", "EV Charger 22"],
        "Vestel": ["EVC04-AC11", "EVC04-AC22", "EVC04-AC11-T2P"],
        "Wallbe": ["Eco 2.0", "Pro", "Pro Plus"],
        "Webasto": ["Live", "Unite", "Next", "TurboDX"],
        "Zaptec": ["Go", "Pro"],
        "DemoVendor": ["PythonEmu"],
    }

    for vendor_name, models in vendors_models.items():
        conn.execute("INSERT OR IGNORE INTO equipment_vendors (name) VALUES (?)", (vendor_name,))
        vid = conn.execute(
            "SELECT id FROM equipment_vendors WHERE name = ?", (vendor_name,)
        ).fetchone()[0]
        for model_name in models:
            conn.execute(
                "INSERT OR IGNORE INTO equipment_models (vendor_id, name) VALUES (?, ?)",
                (vid, model_name),
            )

    meter_type_list = [
        "AC_1_PHASE", "AC_3_PHASE", "DC",
        "AC_1_PHASE_2_WIRE", "AC_3_PHASE_4_WIRE", "AC_3_PHASE_3_WIRE",
        "DC_2_WIRE", "DC_WITH_CCS", "DC_WITH_CHADEMO",
        "MID_CERTIFIED_AC", "MID_CERTIFIED_DC",
        "EICHRECHT_AC", "EICHRECHT_DC",
    ]
    for mt in meter_type_list:
        conn.execute("INSERT OR IGNORE INTO meter_types (name) VALUES (?)", (mt,))
    conn.commit()


def _seed_meter_types(conn):
    """Insert default meter types if the meter_types table is empty."""
    row = conn.execute("SELECT COUNT(*) FROM meter_types").fetchone()
    if row[0] > 0:
        return
    meter_type_list = [
        "AC_1_PHASE", "AC_3_PHASE", "DC",
        "AC_1_PHASE_2_WIRE", "AC_3_PHASE_4_WIRE", "AC_3_PHASE_3_WIRE",
        "DC_2_WIRE", "DC_WITH_CCS", "DC_WITH_CHADEMO",
        "MID_CERTIFIED_AC", "MID_CERTIFIED_DC",
        "EICHRECHT_AC", "EICHRECHT_DC",
    ]
    for mt in meter_type_list:
        conn.execute("INSERT OR IGNORE INTO meter_types (name) VALUES (?)", (mt,))
    conn.commit()


# ── Equipment vendor helpers ────────────────────────────────────

def get_vendors() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT id, name FROM equipment_vendors ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def add_vendor(name: str) -> Optional[dict]:
    conn = _get_conn()
    conn.execute("INSERT OR IGNORE INTO equipment_vendors (name) VALUES (?)", (name.strip(),))
    conn.commit()
    row = conn.execute(
        "SELECT id, name FROM equipment_vendors WHERE name = ?", (name.strip(),)
    ).fetchone()
    return dict(row) if row else None


def rename_vendor(vendor_id: int, new_name: str):
    conn = _get_conn()
    conn.execute("UPDATE equipment_vendors SET name = ? WHERE id = ?", (new_name.strip(), vendor_id))
    conn.commit()


def delete_vendor(vendor_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM equipment_models WHERE vendor_id = ?", (vendor_id,))
    conn.execute("DELETE FROM equipment_vendors WHERE id = ?", (vendor_id,))
    conn.commit()


# ── Equipment model helpers ─────────────────────────────────────

def get_models(vendor_id: Optional[int] = None) -> list[dict]:
    conn = _get_conn()
    if vendor_id:
        rows = conn.execute(
            "SELECT m.id, m.name, m.vendor_id, v.name AS vendor_name "
            "FROM equipment_models m JOIN equipment_vendors v ON m.vendor_id = v.id "
            "WHERE m.vendor_id = ? ORDER BY m.name",
            (vendor_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT m.id, m.name, m.vendor_id, v.name AS vendor_name "
            "FROM equipment_models m JOIN equipment_vendors v ON m.vendor_id = v.id "
            "ORDER BY v.name, m.name"
        ).fetchall()
    return [dict(r) for r in rows]


def add_model(vendor_id: int, name: str) -> Optional[dict]:
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO equipment_models (vendor_id, name) VALUES (?, ?)",
        (vendor_id, name.strip()),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id, name, vendor_id FROM equipment_models WHERE vendor_id = ? AND name = ?",
        (vendor_id, name.strip()),
    ).fetchone()
    return dict(row) if row else None


def rename_model(model_id: int, new_name: str):
    conn = _get_conn()
    conn.execute("UPDATE equipment_models SET name = ? WHERE id = ?", (new_name.strip(), model_id))
    conn.commit()


def delete_model(model_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM equipment_models WHERE id = ?", (model_id,))
    conn.commit()


# ── Meter type helpers ──────────────────────────────────

def get_meter_types() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT id, name FROM meter_types ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def add_meter_type(name: str) -> Optional[dict]:
    conn = _get_conn()
    conn.execute("INSERT OR IGNORE INTO meter_types (name) VALUES (?)", (name.strip(),))
    conn.commit()
    row = conn.execute(
        "SELECT id, name FROM meter_types WHERE name = ?", (name.strip(),)
    ).fetchone()
    return dict(row) if row else None


def rename_meter_type(mt_id: int, new_name: str):
    conn = _get_conn()
    conn.execute("UPDATE meter_types SET name = ? WHERE id = ?", (new_name.strip(), mt_id))
    conn.commit()


def delete_meter_type(mt_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM meter_types WHERE id = ?", (mt_id,))
    conn.commit()


# ── Config helpers ──────────────────────────────────────────────

def load_config() -> dict:
    """Load all config key/value pairs from the database."""
    conn = _get_conn()
    rows = conn.execute("SELECT key, value FROM config").fetchall()
    return {r["key"]: r["value"] for r in rows}


def save_config(key: str, value: str):
    """Upsert a single config key/value pair."""
    conn = _get_conn()
    conn.execute(
        "INSERT INTO config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── Session helpers ─────────────────────────────────────────────

def load_sessions() -> list[dict]:
    """Return all sessions ordered by id."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, started_at, ended_at, energy_kwh, transaction_id "
        "FROM sessions ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def insert_session(session_id: int, started_at: str):
    """Insert a new session row."""
    conn = _get_conn()
    conn.execute(
        "INSERT INTO sessions (id, started_at, energy_kwh) VALUES (?, ?, 0.0)",
        (session_id, started_at),
    )
    conn.commit()


def update_session(session_id: int,
                   ended_at: Optional[str] = None,
                   energy_kwh: Optional[float] = None,
                   transaction_id: Optional[int] = None):
    """Update fields on an existing session row."""
    fields = []
    params = []
    if ended_at is not None:
        fields.append("ended_at = ?")
        params.append(ended_at)
    if energy_kwh is not None:
        fields.append("energy_kwh = ?")
        params.append(energy_kwh)
    if transaction_id is not None:
        fields.append("transaction_id = ?")
        params.append(transaction_id)
    if not fields:
        return
    params.append(session_id)
    conn = _get_conn()
    conn.execute(
        f"UPDATE sessions SET {', '.join(fields)} WHERE id = ?",
        params,
    )
    conn.commit()


def get_next_session_id() -> int:
    """Return the next available session id."""
    conn = _get_conn()
    row = conn.execute("SELECT MAX(id) AS max_id FROM sessions").fetchone()
    return (row["max_id"] or 0) + 1


# ── OCPP log helpers ────────────────────────────────────────────

def insert_ocpp_log(timestamp: str, severity: str, message: str):
    """Insert a single OCPP log entry."""
    conn = _get_conn()
    conn.execute(
        "INSERT INTO ocpp_logs (timestamp, severity, message) VALUES (?, ?, ?)",
        (timestamp, severity, message),
    )
    conn.commit()


def search_ocpp_logs(query: Optional[str] = None, limit: int = 500) -> List[dict]:
    """Return the most recent OCPP logs (newest first), optionally filtered by
    a search term.

    `limit` caps the result — this table accumulates a heartbeat row roughly
    every 10s and grows unbounded, so an unlimited query here used to return
    the entire table (tens of thousands of rows / several MB of JSON) on every
    3-second poll from the OCPP log page. That was slow enough, with no
    timeout/error handling on the frontend fetch, that the page could appear
    frozen on a stale render — looking like events (e.g. UnlockConnector)
    never arrived, when they were actually logged fine.
    """
    conn = _get_conn()
    if query:
        rows = conn.execute(
            "SELECT id, timestamp, severity, message FROM ocpp_logs "
            "WHERE message LIKE ? OR severity LIKE ? "
            "ORDER BY id DESC LIMIT ?",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, timestamp, severity, message FROM ocpp_logs "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]
