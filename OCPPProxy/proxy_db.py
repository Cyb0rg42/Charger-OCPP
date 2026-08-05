"""
Script:   proxy_db.py

Abstract:
    SQLite persistence layer for the OCPP Proxy. Owns the proxy's database
    (directory via PROXY_DATA_DIR, default /opt/charger/data) and provides
    schema management plus CRUD helpers for backend definitions, command
    blocking rules, boot-notification overrides, connector-state overrides,
    message/event logs, command statistics, and the equipment reference
    database (vendors/models/meter types).

Features:
    - Thread-local SQLite connection with WAL journal mode and schema
      migration on init_db().
    - Seeds a default backend and equipment reference data on first run.
    - backend_targeted(): resolves comma-separated backend-name filters
      used throughout the blocking/override rules.

Usage:
    Imported by proxy_core.py and web_app.py; not intended to be run
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

import os
import sqlite3
import threading
import json
from datetime import datetime, timezone

_APP_DIR = os.environ.get("PROXY_DATA_DIR", "/opt/charger/data")
try:
    os.makedirs(_APP_DIR, exist_ok=True)
except OSError:
    # Fallback to a local data directory when /opt/charger/data is not writable
    _APP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(_APP_DIR, exist_ok=True)
_DB_PATH = os.path.join(_APP_DIR, "ocppproxy.db")

_local = threading.local()


def _conn() -> sqlite3.Connection:
    c = getattr(_local, "conn", None)
    if c is None:
        c = sqlite3.connect(_DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        _local.conn = c
    return c


def init_db():
    c = _conn()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS blocked_commands (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        command     TEXT NOT NULL UNIQUE,
        cp_id       TEXT DEFAULT '*',
        created_at  TEXT NOT NULL,
        reason      TEXT DEFAULT ''
    );

    CREATE TABLE IF NOT EXISTS boot_overrides (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        cp_id                       TEXT NOT NULL UNIQUE,
        charge_point_vendor         TEXT DEFAULT '',
        charge_point_model          TEXT DEFAULT '',
        charge_point_serial_number  TEXT DEFAULT '',
        firmware_version            TEXT DEFAULT '',
        meter_type                  TEXT DEFAULT '',
        backends                    TEXT DEFAULT '*',
        enabled                     INTEGER DEFAULT 1,
        created_at                  TEXT NOT NULL,
        updated_at                  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS command_stats (
        command     TEXT NOT NULL,
        direction   TEXT NOT NULL,
        cp_id       TEXT NOT NULL,
        count       INTEGER DEFAULT 0,
        blocked     INTEGER DEFAULT 0,
        last_seen   TEXT NOT NULL,
        PRIMARY KEY (command, direction, cp_id)
    );

    CREATE TABLE IF NOT EXISTS message_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT NOT NULL,
        cp_id       TEXT NOT NULL,
        direction   TEXT NOT NULL,
        message_type TEXT NOT NULL,
        action      TEXT DEFAULT '',
        payload     TEXT DEFAULT '',
        blocked     INTEGER DEFAULT 0,
        modified    INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS equipment_vendors (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS equipment_models (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_id   INTEGER NOT NULL REFERENCES equipment_vendors(id) ON DELETE CASCADE,
        name        TEXT NOT NULL,
        UNIQUE(vendor_id, name)
    );

    CREATE TABLE IF NOT EXISTS meter_types (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS proxy_config (
        key     TEXT PRIMARY KEY,
        value   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS backend_servers (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT NOT NULL,
        url     TEXT NOT NULL,
        enabled INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS connector_status (
        cp_id           TEXT NOT NULL,
        connector_id    INTEGER NOT NULL,
        status          TEXT NOT NULL DEFAULT 'Operative',
        updated_at      TEXT NOT NULL,
        PRIMARY KEY (cp_id, connector_id)
    );

    CREATE TABLE IF NOT EXISTS connector_state_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        cp_id           TEXT NOT NULL,
        connector_id    INTEGER NOT NULL,
        status          TEXT NOT NULL,
        timestamp       TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS event_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        cp_id           TEXT,
        timestamp       TEXT NOT NULL,
        severity        TEXT NOT NULL,
        message         TEXT NOT NULL,
        ocpp_command    TEXT,
        direction       TEXT,
        backend_name    TEXT,
        ocpp_payload    TEXT,
        ocpp_response   TEXT
    );
    """)
    c.commit()
    _migrate_schema(c)
    _seed_equipment(c)
    _seed_config(c)


def _ensure_column(c, table, column, ddl):
    """Add `column` to `table` if it does not already exist (lightweight migration)."""
    cols = [r["name"] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _migrate_schema(c):
    """Apply in-place schema migrations for existing databases."""
    # Per-CSMS targeting: which backend(s) a rule applies to ('*' = all).
    _ensure_column(c, "blocked_commands", "backends", "backends TEXT DEFAULT '*'")
    _ensure_column(c, "connector_status", "backends", "backends TEXT DEFAULT '*'")
    _ensure_column(c, "connector_state_log", "backends", "backends TEXT DEFAULT '*'")
    _ensure_column(c, "boot_overrides", "backends", "backends TEXT DEFAULT '*'")
    c.commit()


# ── Backend targeting helpers ──────────────────────────────────

def normalize_backends(backends):
    """Normalize a backend target list/string to a stored string.

    Returns '*' when all backends are targeted, otherwise a comma-separated
    list of backend names.
    """
    if backends is None:
        return "*"
    if isinstance(backends, str):
        items = [b.strip() for b in backends.split(",")]
    else:
        items = [str(b).strip() for b in backends]
    items = [b for b in items if b]
    if not items or "*" in items:
        return "*"
    # De-duplicate while preserving order
    seen = []
    for b in items:
        if b not in seen:
            seen.append(b)
    return ",".join(seen)


def backend_targeted(stored, backend_name):
    """True if a rule stored as `stored` applies to `backend_name`.

    `backend_name` of None means "any backend" (used for checks that are not
    backend-specific).
    """
    if stored in (None, "", "*"):
        return True
    if backend_name is None:
        return True
    return backend_name in [b.strip() for b in stored.split(",")]



def _seed_config(c):
    """Insert default proxy config if missing."""
    row = c.execute("SELECT COUNT(*) FROM proxy_config WHERE key = 'listen_port'").fetchone()
    if row[0] == 0:
        c.execute("INSERT OR IGNORE INTO proxy_config (key, value) VALUES ('listen_port', '9100')")
    row = c.execute("SELECT COUNT(*) FROM backend_servers").fetchone()
    if row[0] == 0:
        default_url = os.environ.get("PROXY_DEFAULT_BACKEND_URL", "ws://127.0.0.1:9000/ocpp")
        c.execute("INSERT INTO backend_servers (name, url, enabled) VALUES (?, ?, 1)",
                  ("Default Backend", default_url))
    c.commit()


def _seed_equipment(c):
    """Insert default vendors/models/meter types if tables are empty."""
    row = c.execute("SELECT COUNT(*) FROM equipment_vendors").fetchone()
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
    }

    meter_type_list = [
        "AC_1_PHASE", "AC_3_PHASE", "DC",
        "AC_1_PHASE_2_WIRE", "AC_3_PHASE_4_WIRE", "AC_3_PHASE_3_WIRE",
        "DC_2_WIRE", "DC_WITH_CCS", "DC_WITH_CHADEMO",
        "MID_CERTIFIED_AC", "MID_CERTIFIED_DC",
        "EICHRECHT_AC", "EICHRECHT_DC",
    ]

    for vendor_name, models in vendors_models.items():
        c.execute("INSERT OR IGNORE INTO equipment_vendors (name) VALUES (?)", (vendor_name,))
        vid = c.execute("SELECT id FROM equipment_vendors WHERE name = ?", (vendor_name,)).fetchone()[0]
        for model_name in models:
            c.execute("INSERT OR IGNORE INTO equipment_models (vendor_id, name) VALUES (?, ?)",
                      (vid, model_name))

    for mt in meter_type_list:
        c.execute("INSERT OR IGNORE INTO meter_types (name) VALUES (?)", (mt,))

    c.commit()


# ── Blocked commands ────────────────────────────────────────────

def get_blocked_commands():
    rows = _conn().execute(
        "SELECT command, cp_id, backends, reason, created_at FROM blocked_commands ORDER BY command"
    ).fetchall()
    return [dict(r) for r in rows]


def is_command_blocked(command, cp_id="*", backend_name=None):
    """Whether `command` is blocked for `cp_id` toward `backend_name`.

    `backend_name` of None ignores backend targeting (returns True if blocked
    for any backend).
    """
    rows = _conn().execute(
        "SELECT backends FROM blocked_commands WHERE command = ? AND (cp_id = '*' OR cp_id = ?)",
        (command, cp_id),
    ).fetchall()
    for r in rows:
        if backend_targeted(r["backends"], backend_name):
            return True
    return False


def block_command(command, cp_id="*", reason="", backends="*"):
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute(
        "INSERT OR REPLACE INTO blocked_commands (command, cp_id, backends, reason, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (command, cp_id, normalize_backends(backends), reason, now),
    )
    c.commit()


def unblock_command(command, cp_id="*"):
    c = _conn()
    c.execute(
        "DELETE FROM blocked_commands WHERE command = ? AND cp_id = ?",
        (command, cp_id),
    )
    c.commit()


# ── Boot overrides ──────────────────────────────────────────────

def get_boot_overrides():
    rows = _conn().execute(
        "SELECT * FROM boot_overrides ORDER BY cp_id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_boot_override(cp_id):
    row = _conn().execute(
        "SELECT * FROM boot_overrides WHERE cp_id = ? AND enabled = 1",
        (cp_id,),
    ).fetchone()
    return dict(row) if row else None


def upsert_boot_override(cp_id, vendor="", model="", serial="", firmware="", meter_type="", enabled=True, backends="*"):
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute("""
        INSERT INTO boot_overrides
            (cp_id, charge_point_vendor, charge_point_model,
             charge_point_serial_number, firmware_version, meter_type,
             backends, enabled, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cp_id) DO UPDATE SET
            charge_point_vendor = excluded.charge_point_vendor,
            charge_point_model = excluded.charge_point_model,
            charge_point_serial_number = excluded.charge_point_serial_number,
            firmware_version = excluded.firmware_version,
            meter_type = excluded.meter_type,
            backends = excluded.backends,
            enabled = excluded.enabled,
            updated_at = excluded.updated_at
    """, (cp_id, vendor, model, serial, firmware, meter_type,
          normalize_backends(backends), int(enabled), now, now))
    c.commit()


def delete_boot_override(cp_id):
    c = _conn()
    c.execute("DELETE FROM boot_overrides WHERE cp_id = ?", (cp_id,))
    c.commit()


# ── Command stats ───────────────────────────────────────────────

def record_command(command, direction, cp_id, was_blocked=False):
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute("""
        INSERT INTO command_stats (command, direction, cp_id, count, blocked, last_seen)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(command, direction, cp_id) DO UPDATE SET
            count = count + 1,
            blocked = blocked + ?,
            last_seen = ?
    """, (command, direction, cp_id, int(was_blocked), now, int(was_blocked), now))
    c.commit()


def get_command_stats():
    rows = _conn().execute("""
        SELECT command, direction,
               SUM(count) as total,
               SUM(blocked) as total_blocked,
               MAX(last_seen) as last_seen
        FROM command_stats
        GROUP BY command, direction
        ORDER BY total DESC
    """).fetchall()
    return [dict(r) for r in rows]


def get_command_stats_by_cp(cp_id):
    rows = _conn().execute("""
        SELECT command, direction, count, blocked, last_seen
        FROM command_stats
        WHERE cp_id = ?
        ORDER BY count DESC
    """, (cp_id,)).fetchall()
    return [dict(r) for r in rows]


# ── Message log ─────────────────────────────────────────────────

def log_message(cp_id, direction, message_type, action="", payload="", blocked=False, modified=False):
    # Skip Heartbeat messages from database logging
    if action == "Heartbeat":
        return
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute("""
        INSERT INTO message_log (timestamp, cp_id, direction, message_type, action, payload, blocked, modified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (now, cp_id, direction, message_type, action,
          payload if isinstance(payload, str) else json.dumps(payload),
          int(blocked), int(modified)))
    c.commit()


def get_recent_messages(limit=200, cp_id=None):
    if cp_id:
        rows = _conn().execute(
            "SELECT * FROM message_log WHERE cp_id = ? ORDER BY id DESC LIMIT ?",
            (cp_id, limit),
        ).fetchall()
    else:
        rows = _conn().execute(
            "SELECT * FROM message_log ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_last_boot_info(cp_id):
    """Return {vendor, model, firmware, serial} from the most recent
    BootNotification logged for this charge point, or None if none found."""
    row = _conn().execute(
        "SELECT payload FROM message_log "
        "WHERE cp_id = ? AND action = 'BootNotification' "
        "ORDER BY id DESC LIMIT 1",
        (cp_id,),
    ).fetchone()
    if not row or not row["payload"]:
        return None
    try:
        p = json.loads(row["payload"])
    except (ValueError, TypeError):
        return None
    if not isinstance(p, dict):
        return None
    return {
        "vendor": p.get("chargePointVendor", ""),
        "model": p.get("chargePointModel", ""),
        "firmware": p.get("firmwareVersion", ""),
        "serial": p.get("chargePointSerialNumber", ""),
        "meter": p.get("meterType", ""),
    }


def get_connected_clients():
    """Return distinct cp_ids seen in message log (recent)."""
    rows = _conn().execute("""
        SELECT DISTINCT cp_id FROM message_log
        WHERE timestamp > datetime('now', '-24 hours')
        ORDER BY cp_id
    """).fetchall()
    return [r["cp_id"] for r in rows]


# ── Event log (paired OCPP request/response) ────────────────────

def log_event(cp_id, severity, message, ocpp_command=None, direction=None, backend_name=None, ocpp_payload=None, ocpp_response=None):
    """Insert an event and return its row id."""
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute(
        "INSERT INTO event_log (cp_id, timestamp, severity, message, ocpp_command, direction, backend_name, ocpp_payload, ocpp_response) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (cp_id, now, severity, message, ocpp_command, direction, backend_name,
         ocpp_payload if isinstance(ocpp_payload, str) else (json.dumps(ocpp_payload) if ocpp_payload is not None else None),
         ocpp_response if isinstance(ocpp_response, str) else (json.dumps(ocpp_response) if ocpp_response is not None else None)),
    )
    c.commit()
    return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def update_event_response(event_id, ocpp_response):
    """Attach the OCPP response to an existing event."""
    c = _conn()
    resp = ocpp_response if isinstance(ocpp_response, str) else json.dumps(ocpp_response)
    c.execute("UPDATE event_log SET ocpp_response = ? WHERE id = ?", (resp, event_id))
    c.commit()


def get_events(cp_id=None, query=None, ocpp_command=None, limit=200):
    c = _conn()
    sql = "SELECT * FROM event_log"
    params = []
    clauses = []
    if cp_id:
        clauses.append("cp_id = ?"); params.append(cp_id)
    if query:
        clauses.append("(message LIKE ? OR severity LIKE ?)"); params.extend([f"%{query}%", f"%{query}%"])
    if ocpp_command:
        clauses.append("ocpp_command = ?"); params.append(ocpp_command)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = c.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_ocpp_commands():
    """Return distinct OCPP commands in event_log with counts."""
    c = _conn()
    rows = c.execute(
        "SELECT ocpp_command, COUNT(*) as count FROM event_log "
        "WHERE ocpp_command IS NOT NULL AND ocpp_command != '' "
        "GROUP BY ocpp_command ORDER BY count DESC"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Equipment vendors ───────────────────────────────────────────

def get_vendors():
    rows = _conn().execute("SELECT id, name FROM equipment_vendors ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def add_vendor(name):
    c = _conn()
    c.execute("INSERT OR IGNORE INTO equipment_vendors (name) VALUES (?)", (name.strip(),))
    c.commit()
    row = c.execute("SELECT id, name FROM equipment_vendors WHERE name = ?", (name.strip(),)).fetchone()
    return dict(row) if row else None


def rename_vendor(vendor_id, new_name):
    c = _conn()
    c.execute("UPDATE equipment_vendors SET name = ? WHERE id = ?", (new_name.strip(), vendor_id))
    c.commit()


def delete_vendor(vendor_id):
    c = _conn()
    c.execute("DELETE FROM equipment_vendors WHERE id = ?", (vendor_id,))
    c.commit()


# ── Equipment models ────────────────────────────────────────────

def get_models(vendor_id=None):
    if vendor_id:
        rows = _conn().execute("""
            SELECT m.id, m.name, m.vendor_id, v.name as vendor_name
            FROM equipment_models m JOIN equipment_vendors v ON m.vendor_id = v.id
            WHERE m.vendor_id = ? ORDER BY m.name
        """, (vendor_id,)).fetchall()
    else:
        rows = _conn().execute("""
            SELECT m.id, m.name, m.vendor_id, v.name as vendor_name
            FROM equipment_models m JOIN equipment_vendors v ON m.vendor_id = v.id
            ORDER BY v.name, m.name
        """).fetchall()
    return [dict(r) for r in rows]


def add_model(vendor_id, name):
    c = _conn()
    c.execute("INSERT OR IGNORE INTO equipment_models (vendor_id, name) VALUES (?, ?)",
              (vendor_id, name.strip()))
    c.commit()
    row = c.execute(
        "SELECT id, name, vendor_id FROM equipment_models WHERE vendor_id = ? AND name = ?",
        (vendor_id, name.strip()),
    ).fetchone()
    return dict(row) if row else None


def rename_model(model_id, new_name):
    c = _conn()
    c.execute("UPDATE equipment_models SET name = ? WHERE id = ?", (new_name.strip(), model_id))
    c.commit()


def delete_model(model_id):
    c = _conn()
    c.execute("DELETE FROM equipment_models WHERE id = ?", (model_id,))
    c.commit()


# ── Meter types ─────────────────────────────────────────────────

def get_meter_types():
    rows = _conn().execute("SELECT id, name FROM meter_types ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def add_meter_type(name):
    c = _conn()
    c.execute("INSERT OR IGNORE INTO meter_types (name) VALUES (?)", (name.strip(),))
    c.commit()
    row = c.execute("SELECT id, name FROM meter_types WHERE name = ?", (name.strip(),)).fetchone()
    return dict(row) if row else None


def rename_meter_type(mt_id, new_name):
    c = _conn()
    c.execute("UPDATE meter_types SET name = ? WHERE id = ?", (new_name.strip(), mt_id))
    c.commit()


def delete_meter_type(mt_id):
    c = _conn()
    c.execute("DELETE FROM meter_types WHERE id = ?", (mt_id,))
    c.commit()


# ── Proxy configuration ────────────────────────────────────────

def get_config(key, default=""):
    row = _conn().execute("SELECT value FROM proxy_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_config(key, value):
    c = _conn()
    c.execute("INSERT OR REPLACE INTO proxy_config (key, value) VALUES (?, ?)", (key, str(value)))
    c.commit()


def get_listen_port():
    return int(get_config("listen_port", "9100"))


def set_listen_port(port):
    set_config("listen_port", str(port))


# ── Backend servers ─────────────────────────────────────────────

MAX_BACKENDS = 3


def get_backends():
    rows = _conn().execute("SELECT id, name, url, enabled FROM backend_servers ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def get_enabled_backends():
    rows = _conn().execute(
        "SELECT id, name, url FROM backend_servers WHERE enabled = 1 ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def add_backend(name, url, enabled=True):
    c = _conn()
    count = c.execute("SELECT COUNT(*) FROM backend_servers").fetchone()[0]
    if count >= MAX_BACKENDS:
        return None
    c.execute("INSERT INTO backend_servers (name, url, enabled) VALUES (?, ?, ?)",
              (name.strip(), url.strip(), int(enabled)))
    c.commit()
    return dict(c.execute("SELECT * FROM backend_servers WHERE id = last_insert_rowid()").fetchone())


def update_backend(backend_id, name=None, url=None, enabled=None):
    c = _conn()
    if name is not None:
        c.execute("UPDATE backend_servers SET name = ? WHERE id = ?", (name.strip(), backend_id))
    if url is not None:
        c.execute("UPDATE backend_servers SET url = ? WHERE id = ?", (url.strip(), backend_id))
    if enabled is not None:
        c.execute("UPDATE backend_servers SET enabled = ? WHERE id = ?", (int(enabled), backend_id))
    c.commit()


def delete_backend(backend_id):
    c = _conn()
    c.execute("DELETE FROM backend_servers WHERE id = ?", (backend_id,))
    c.commit()


# ── Connector status overrides ──────────────────────────────────

def set_connector_status(cp_id, connector_id, status, backends="*"):
    """Store the desired connector status and which backend(s) it targets.

    When connector_id == 0 (whole charge point), any existing per-connector
    overrides for this CP are removed so that the connector-0 fallback takes
    effect for every connector.
    """
    now = datetime.now(timezone.utc).isoformat()
    backends = normalize_backends(backends)
    c = _conn()
    if int(connector_id) == 0:
        # Remove stale individual connector overrides so the
        # connector-0 fallback is used consistently.
        c.execute(
            "DELETE FROM connector_status WHERE cp_id = ? AND connector_id != 0",
            (cp_id,),
        )
    c.execute("""
        INSERT INTO connector_status (cp_id, connector_id, status, backends, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cp_id, connector_id) DO UPDATE SET
            status = excluded.status,
            backends = excluded.backends,
            updated_at = excluded.updated_at
    """, (cp_id, int(connector_id), status, backends, now))
    c.commit()


def get_connector_status(cp_id, connector_id):
    """Return the configured status for a specific connector, or None."""
    row = _conn().execute(
        "SELECT status FROM connector_status WHERE cp_id = ? AND connector_id = ?",
        (cp_id, int(connector_id)),
    ).fetchone()
    return row["status"] if row else None


def get_connector_backends(cp_id, connector_id):
    """Return the target backends string for a connector ('*' when unset)."""
    row = _conn().execute(
        "SELECT backends FROM connector_status WHERE cp_id = ? AND connector_id = ?",
        (cp_id, int(connector_id)),
    ).fetchone()
    return (row["backends"] if row and row["backends"] else "*") if row else None



def get_connector_statuses(cp_id=None):
    """Return all connector status overrides, optionally filtered by cp_id."""
    if cp_id:
        rows = _conn().execute(
            "SELECT * FROM connector_status WHERE cp_id = ? ORDER BY connector_id",
            (cp_id,),
        ).fetchall()
    else:
        rows = _conn().execute(
            "SELECT * FROM connector_status ORDER BY cp_id, connector_id"
        ).fetchall()
    return [dict(r) for r in rows]


def delete_connector_status(cp_id, connector_id):
    c = _conn()
    c.execute("DELETE FROM connector_status WHERE cp_id = ? AND connector_id = ?",
             (cp_id, int(connector_id)))
    c.commit()


# ── Connector state history (Connector Control command log) ─────

def log_connector_state(cp_id, connector_id, status, backends="*"):
    """Record an operator-initiated connector state change for the history panel."""
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    c.execute("""
        INSERT INTO connector_state_log (cp_id, connector_id, status, backends, timestamp)
        VALUES (?, ?, ?, ?, ?)
    """, (cp_id, int(connector_id), status, normalize_backends(backends), now))
    c.commit()


def get_connector_state_history(limit=10):
    """Return the most recent connector state changes (newest first)."""
    rows = _conn().execute(
        "SELECT * FROM connector_state_log ORDER BY id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]
