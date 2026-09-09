"""
Script:   cs_db.py

Abstract:
    SQLite persistence layer for the CSMS. Owns the Central System's
    database (path set from the instance's YAML config via DB_PATH) and
    provides schema management plus CRUD helpers for every domain object
    the web GUI and OCPP server touch: charge points, transactions, meter
    values, status/event history, cars, smart charging schedules, Zonneplan
    tariffs, RFID tags, equipment (vendors/models/meter types), and app
    settings.

Features:
    - Thread-local SQLite connection with WAL journal mode and schema
      migration on init_db().
    - Per-quarter-hour Zonneplan tariff cost calculation for finished
      transactions (_calculate_hourly_cost).
    - Cheapest-charging-window helpers backing the Charging Plan page.

Usage:
    Imported by csms.py, central_system.py, and zonneplan.py; not intended
    to be run directly. See test_cs_db.py for the unit tests.

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
import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional, List

_APP_DIR = "/opt/charger/data"
DB_PATH = os.path.join(_APP_DIR, "csms.db")
_local = threading.local()

# Fallback tariff used when no hourly price is available for a given hour.
DEFAULT_TARIFF_EUR_PER_KWH = 0.30


def _get_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.row_factory = sqlite3.Row
        try:
            _local.conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            pass  # WAL not supported on some filesystems (e.g. /mnt/ in WSL)
    return _local.conn


def init_db():
    """Create database and tables if they do not exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS chargepoints (
            cp_id        TEXT PRIMARY KEY,
            vendor       TEXT,
            model        TEXT,
            status       TEXT NOT NULL DEFAULT 'Offline',
            last_heartbeat TEXT,
            site_id      INTEGER,
            created_at   TEXT NOT NULL,
            FOREIGN KEY(site_id) REFERENCES sites(id)
        );

        CREATE TABLE IF NOT EXISTS sites (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL UNIQUE,
            street       TEXT,
            house_number TEXT,
            zip_code     TEXT,
            city         TEXT,
            country      TEXT,
            created_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS connector_status (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id        TEXT NOT NULL,
            connector_id INTEGER NOT NULL,
            status       TEXT NOT NULL DEFAULT 'Offline',
            updated_at   TEXT NOT NULL,
            UNIQUE(cp_id, connector_id),
            FOREIGN KEY (cp_id) REFERENCES chargepoints(cp_id)
        );

        CREATE TABLE IF NOT EXISTS connector_status_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id           TEXT NOT NULL,
            connector_id    INTEGER NOT NULL,
            status          TEXT NOT NULL,
            timestamp       TEXT NOT NULL,
            transaction_id  INTEGER,
            source          TEXT NOT NULL DEFAULT 'live',
            source_event_id INTEGER,
            UNIQUE(cp_id, connector_id, status, timestamp),
            UNIQUE(source_event_id)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id         TEXT NOT NULL,
            connector_id  INTEGER NOT NULL,
            id_tag        TEXT NOT NULL,
            meter_start   INTEGER NOT NULL,
            meter_stop    INTEGER,
            started_at    TEXT NOT NULL,
            stopped_at    TEXT,
            stop_reason   TEXT,
            FOREIGN KEY (cp_id) REFERENCES chargepoints(cp_id)
        );

        CREATE TABLE IF NOT EXISTS meter_values (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id          TEXT NOT NULL,
            connector_id   INTEGER,
            transaction_id INTEGER,
            timestamp      TEXT,
            measurand      TEXT,
            value          TEXT,
            unit           TEXT,
            phase          TEXT
        );

        CREATE TABLE IF NOT EXISTS event_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id        TEXT,
            timestamp    TEXT NOT NULL,
            severity     TEXT NOT NULL,
            message      TEXT NOT NULL,
            ocpp_command TEXT
        );

        CREATE TABLE IF NOT EXISTS rfids (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tag       TEXT NOT NULL,
            assigned_to  TEXT,
            account      TEXT,
            site         TEXT,
            created_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ocpp_config (
            key          TEXT PRIMARY KEY,
            value        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mail_config (
            key          TEXT PRIMARY KEY,
            value        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS zonneplan_config (
            key          TEXT PRIMARY KEY,
            value        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_config (
            key          TEXT PRIMARY KEY,
            value        TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chargepoint_charge_values (
            cp_id        TEXT PRIMARY KEY,
            num_phases   INTEGER NOT NULL DEFAULT 1,
            max_amp_l1   REAL,
            max_amp_l2   REAL,
            max_amp_l3   REAL,
            voltage      REAL NOT NULL DEFAULT 230,
            updated_at   TEXT,
            FOREIGN KEY(cp_id) REFERENCES chargepoints(cp_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS zonneplan_tariffs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            hour             TEXT NOT NULL,
            tariff_eur_per_kwh REAL NOT NULL,
            tariff_group     TEXT,
            fetched_at       TEXT NOT NULL,
            UNIQUE(hour)
        );

        CREATE TABLE IF NOT EXISTS cars (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            brand        TEXT NOT NULL,
            model        TEXT NOT NULL,
            year         INTEGER NOT NULL,
            kwh_per_100km REAL NOT NULL,
            license_plate TEXT NOT NULL DEFAULT '',
            battery_capacity_kwh REAL,
            created_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS smart_schedules (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id        TEXT NOT NULL,
            id_tag       TEXT NOT NULL,
            start_at     TEXT NOT NULL,
            stop_at      TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'scheduled',
            transaction_id INTEGER,
            created_at   TEXT NOT NULL,
            FOREIGN KEY (cp_id) REFERENCES chargepoints(cp_id)
        );
    """)
    conn.commit()

    # Seed default OCPP config values if not present
    defaults = {"heartbeat_interval": "30", "expiration": "1", "ocpp_port": "9110"}
    for k, v in defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO ocpp_config (key, value) VALUES (?, ?)",
            (k, v),
        )
    conn.commit()

    # Seed default app_config values if not present
    app_defaults = {
        "default_car_license_plate": "S-122-GN",
        "default_tariff_eur_per_kwh": str(DEFAULT_TARIFF_EUR_PER_KWH),
    }
    for k, v in app_defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)",
            (k, v),
        )
    conn.commit()

    # Seed default mail config values if not present
    mail_defaults = {
        "enabled": "false",
        "protocol": "smtp",
        "host": "",
        "port": "25",
        "from": "",
        "username": "",
        "password": "",
        "recipients": "",
        "notify_boot": "false",
        "notify_faulted": "false",
        "notify_connect": "false",
        "notify_disconnect": "false",
        "notify_start_transaction": "false",
        "notify_ev_suspended": "false",
        "notify_end_transaction": "false",
    }
    for k, v in mail_defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO mail_config (key, value) VALUES (?, ?)",
            (k, v),
        )
    conn.commit()

    # Migrate: add phase column if missing (existing databases)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(meter_values)").fetchall()]
    if "phase" not in cols:
        conn.execute("ALTER TABLE meter_values ADD COLUMN phase TEXT")
        conn.commit()
    
    # Migrate: add ocpp_command column if missing (existing databases)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(event_log)").fetchall()]
    if "ocpp_command" not in cols:
        conn.execute("ALTER TABLE event_log ADD COLUMN ocpp_command TEXT")
        conn.commit()
    if "ocpp_payload" not in cols:
        conn.execute("ALTER TABLE event_log ADD COLUMN ocpp_payload TEXT")
        conn.commit()
    if "ocpp_response" not in cols:
        conn.execute("ALTER TABLE event_log ADD COLUMN ocpp_response TEXT")
        conn.commit()

    # Migrate: add site_id and BootNotification columns to chargepoints if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(chargepoints)").fetchall()]
    if "site_id" not in cols:
        conn.execute("ALTER TABLE chargepoints ADD COLUMN site_id INTEGER")
        conn.commit()
    for col in ["chargePointModel", "chargePointSerialNumber", "chargePointVendor", "firmwareVersion", "meterType", "boot_received_at"]:
        if col not in cols:
            conn.execute(f"ALTER TABLE chargepoints ADD COLUMN {col} TEXT")
            conn.commit()

    # Migrate: add charging_start / charging_stop to transactions if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "charging_start" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN charging_start TEXT")
        conn.commit()
    if "charging_stop" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN charging_stop TEXT")
        conn.commit()

    # Migrate: add car_id to transactions if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "car_id" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN car_id INTEGER")
        conn.commit()

    # Migrate: add tariff_eur_per_kwh and cost_eur to transactions if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "tariff_eur_per_kwh" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN tariff_eur_per_kwh REAL")
        conn.commit()
    if "cost_eur" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN cost_eur REAL")
        conn.commit()

    # Migrate: add license_plate to cars if missing
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(cars)").fetchall()]
        if cols and "license_plate" not in cols:
            conn.execute("ALTER TABLE cars ADD COLUMN license_plate TEXT NOT NULL DEFAULT ''")
            conn.commit()
    except sqlite3.OperationalError:
        pass

    # Migrate: add battery_capacity_kwh to cars if missing
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(cars)").fetchall()]
        if cols and "battery_capacity_kwh" not in cols:
            conn.execute("ALTER TABLE cars ADD COLUMN battery_capacity_kwh REAL")
            conn.commit()
    except sqlite3.OperationalError:
        pass

    # Migrate: add last_status to transactions if missing
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "last_status" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN last_status TEXT")
        conn.commit()

    # Migrate: add km to transactions if missing (frozen at car-assignment time)
    cols = [row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()]
    if "km" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN km REAL")
        conn.commit()

    # Migrate: add blocked column to rfids if missing
    rfid_cols = [row[1] for row in conn.execute("PRAGMA table_info(rfids)").fetchall()]
    if "blocked" not in rfid_cols:
        conn.execute("ALTER TABLE rfids ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0")
        conn.commit()

    # Migrate: store RFID foreign key id in transactions.id_tag instead of tag text.
    # Keep column type as-is for SQLite compatibility with existing databases.
    conn.execute(
        "UPDATE transactions "
        "SET id_tag = ("
        "  SELECT CAST(r.id AS TEXT) FROM rfids r WHERE r.id_tag = transactions.id_tag LIMIT 1"
        ") "
        "WHERE EXISTS (SELECT 1 FROM rfids r WHERE r.id_tag = transactions.id_tag)"
    )
    conn.commit()

    # Ensure indexes exist for status history lookups/backfills.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_status_hist_cp_conn_ts "
        "ON connector_status_history(cp_id, connector_id, timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_status_hist_txn_ts "
        "ON connector_status_history(transaction_id, timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_status_hist_source_event "
        "ON connector_status_history(source_event_id)"
    )
    conn.commit()


# ── Charge Point helpers ────────────────────────────────────────

def upsert_chargepoint(cp_id: str, vendor: str = None, model: str = None, status: str = None, **boot_fields):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute("SELECT cp_id FROM chargepoints WHERE cp_id = ?", (cp_id,)).fetchone()
    boot_keys = ["chargePointModel", "chargePointSerialNumber", "chargePointVendor", "firmwareVersion", "meterType"]
    boot_values = {k: boot_fields.get(k) for k in boot_keys if boot_fields.get(k) is not None}
    if boot_values:
        boot_values["boot_received_at"] = now
    if existing:
        parts, params = [], []
        if vendor:
            parts.append("vendor = ?"); params.append(vendor)
        if model:
            parts.append("model = ?"); params.append(model)
        if status:
            parts.append("status = ?"); params.append(status)
        for k, v in boot_values.items():
            parts.append(f"{k} = ?"); params.append(v)
        if parts:
            params.append(cp_id)
            conn.execute(f"UPDATE chargepoints SET {', '.join(parts)} WHERE cp_id = ?", params)
    else:
        columns = ["cp_id", "vendor", "model", "status", "created_at"] + list(boot_values.keys())
        values = [cp_id, vendor or "", model or "", status or "Offline", now] + list(boot_values.values())
        placeholders = ", ".join(["?"] * len(columns))
        conn.execute(f"INSERT INTO chargepoints ({', '.join(columns)}) VALUES ({placeholders})", values)
    conn.commit()

def update_chargepoint_status(cp_id: str, status: str):
    conn = _get_conn()
    conn.execute("UPDATE chargepoints SET status = ? WHERE cp_id = ?", (status, cp_id))
    conn.commit()


def update_chargepoint_heartbeat(cp_id: str):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("UPDATE chargepoints SET last_heartbeat = ? WHERE cp_id = ?", (now, cp_id))
    conn.commit()


def update_connector_status(cp_id: str, connector_id: int, status: str, updated_at: Optional[str] = None):
    conn = _get_conn()
    now = updated_at or datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO connector_status (cp_id, connector_id, status, updated_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(cp_id, connector_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at",
        (cp_id, connector_id, status, now),
    )
    conn.commit()


def get_connector_statuses(cp_id: str) -> List[dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT connector_id, status FROM connector_status WHERE cp_id = ? ORDER BY connector_id",
        (cp_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_connector_ids(cp_id: str) -> List[int]:
    """Return all known non-zero connector IDs from both connector_status and transactions."""
    conn = _get_conn()
    ids = set()
    rows = conn.execute(
        "SELECT connector_id FROM connector_status WHERE cp_id = ? AND connector_id != 0",
        (cp_id,),
    ).fetchall()
    for r in rows:
        ids.add(r[0])
    rows = conn.execute(
        "SELECT DISTINCT connector_id FROM transactions WHERE cp_id = ? AND connector_id != 0",
        (cp_id,),
    ).fetchall()
    for r in rows:
        ids.add(r[0])
    return sorted(ids)


def get_chargepoints() -> List[dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT c.*, s.name AS site_name, s.street AS site_street, s.house_number AS site_house_number, s.zip_code AS site_zip_code, s.city AS site_city, s.country AS site_country "
        "FROM chargepoints c LEFT JOIN sites s ON c.site_id = s.id ORDER BY c.cp_id"
    ).fetchall()
    cps = [dict(r) for r in rows]
    for cp in cps:
        if cp.get("site_name"):
            cp["site"] = cp["site_name"]
        else:
            cp["site"] = ''
        connector_rows = conn.execute(
            "SELECT connector_id, status FROM connector_status WHERE cp_id = ? AND connector_id != 0 ORDER BY connector_id",
            (cp["cp_id"],)
        ).fetchall()
        cp["connectors"] = [dict(c) for c in connector_rows]
        # If no connector_status data, fallback to connectors seen in transactions (excluding connector 0)
        if not cp["connectors"]:
            txn_rows = conn.execute(
                "SELECT DISTINCT connector_id FROM transactions WHERE cp_id = ? AND connector_id != 0 ORDER BY connector_id",
                (cp["cp_id"],)
            ).fetchall()
            connectors = []
            for row in txn_rows:
                connector_id = row[0]
                active_txn = conn.execute(
                    "SELECT 1 FROM transactions WHERE cp_id = ? AND connector_id = ? AND stopped_at IS NULL LIMIT 1",
                    (cp["cp_id"], connector_id)
                ).fetchone()
                status = "Charging" if active_txn else "Available"
                connectors.append({"connector_id": connector_id, "status": status})
            cp["connectors"] = connectors
    return cps


# ── Site helpers ─────────────────────────────────────────────────

def get_sites() -> List[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM sites ORDER BY name").fetchall()
    sites = [dict(r) for r in rows]
    for site in sites:
        cprows = conn.execute("SELECT cp_id FROM chargepoints WHERE site_id = ? ORDER BY cp_id", (site['id'],)).fetchall()
        site['chargepoints'] = [r['cp_id'] for r in cprows]
    return sites


def get_site(site_id: int) -> Optional[dict]:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM sites WHERE id = ?", (site_id,)).fetchone()
    if not row:
        return None
    site = dict(row)
    cprows = conn.execute("SELECT cp_id FROM chargepoints WHERE site_id = ? ORDER BY cp_id", (site_id,)).fetchall()
    site['chargepoints'] = [r['cp_id'] for r in cprows]
    return site


def add_site(name: str, street: str = None, house_number: str = None, zip_code: str = None, city: str = None, country: str = None):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO sites (name, street, house_number, zip_code, city, country, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, street or '', house_number or '', zip_code or '', city or '', country or '', now)
    )
    conn.commit()


def update_site(site_id: int, name: str = None, street: str = None, house_number: str = None, zip_code: str = None, city: str = None, country: str = None):
    conn = _get_conn()
    parts, params = [], []
    if name is not None:
        parts.append("name = ?"); params.append(name)
    if street is not None:
        parts.append("street = ?"); params.append(street)
    if house_number is not None:
        parts.append("house_number = ?"); params.append(house_number)
    if zip_code is not None:
        parts.append("zip_code = ?"); params.append(zip_code)
    if city is not None:
        parts.append("city = ?"); params.append(city)
    if country is not None:
        parts.append("country = ?"); params.append(country)
    if parts:
        params.append(site_id)
        conn.execute(f"UPDATE sites SET {', '.join(parts)} WHERE id = ?", params)
        conn.commit()


def delete_site(site_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
    conn.commit()


def assign_chargepoint_to_site(cp_id: str, site_id: int = None):
    conn = _get_conn()
    conn.execute("UPDATE chargepoints SET site_id = ? WHERE cp_id = ?", (site_id, cp_id))
    conn.commit()


def delete_chargepoint(cp_id: str) -> Optional[dict]:
    conn = _get_conn()
    row = conn.execute("SELECT cp_id FROM chargepoints WHERE cp_id = ?", (cp_id,)).fetchone()
    if not row:
        return None

    deleted = {}
    try:
        cur = conn.execute("UPDATE rfids SET account = '' WHERE account = ?", (cp_id,))
        deleted["rfids_cleared"] = max(cur.rowcount, 0)

        for table in (
            "connector_status_history",
            "connector_status",
            "meter_values",
            "transactions",
            "event_log",
            "smart_schedules",
        ):
            cur = conn.execute(f"DELETE FROM {table} WHERE cp_id = ?", (cp_id,))
            deleted[table] = max(cur.rowcount, 0)

        cur = conn.execute("DELETE FROM chargepoints WHERE cp_id = ?", (cp_id,))
        deleted["chargepoints"] = max(cur.rowcount, 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return deleted


# ── Transaction helpers ─────────────────────────────────────────

def _resolve_rfid_fk_value(conn: sqlite3.Connection, id_tag_value) -> str:
    """Return the value to store in transactions.id_tag (RFID row id as text when possible)."""
    if id_tag_value is None:
        return ""

    value = str(id_tag_value).strip()
    if not value:
        return value

    # If this is a known RFID tag string, store its primary key id.
    row = conn.execute("SELECT id FROM rfids WHERE id_tag = ? LIMIT 1", (value,)).fetchone()
    if row:
        return str(row["id"])

    # If this already looks like an RFID id and exists, keep it as-is.
    if value.isdigit():
        row = conn.execute("SELECT id FROM rfids WHERE id = ? LIMIT 1", (int(value),)).fetchone()
        if row:
            return str(row["id"])

    # Fallback for unexpected legacy/invalid values.
    return value


def _normalize_transaction_rows(conn: sqlite3.Connection, rows: List[sqlite3.Row]) -> List[dict]:
    """Resolve stored RFID ids back to tag names for API compatibility."""
    rfids = conn.execute("SELECT id, id_tag FROM rfids").fetchall()
    rfid_by_id = {str(r["id"]): r for r in rfids}
    rfid_by_tag = {r["id_tag"]: r for r in rfids}

    normalized = []
    for row in rows:
        txn = dict(row)
        raw = txn.get("id_tag")
        raw_key = str(raw).strip() if raw is not None else ""

        matched = rfid_by_id.get(raw_key)
        if not matched:
            matched = rfid_by_tag.get(raw_key)

        if matched:
            txn["rfid_id"] = matched["id"]
            txn["id_tag"] = matched["id_tag"]
        else:
            txn["rfid_id"] = int(raw_key) if raw_key.isdigit() else None

        normalized.append(txn)

    return normalized

def get_active_transaction_for_cp(cp_id: str) -> Optional[dict]:
    """Return the active (not yet stopped) transaction for a charge point."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM transactions WHERE cp_id = ? AND stopped_at IS NULL ORDER BY id DESC LIMIT 1",
        (cp_id,),
    ).fetchone()
    if not row:
        return None
    return _normalize_transaction_rows(conn, [row])[0]


def get_active_transaction_for_connector(cp_id: str, connector_id: int) -> Optional[dict]:
    """Return active transaction for a specific connector if possible."""
    conn = _get_conn()
    if connector_id and connector_id > 0:
        row = conn.execute(
            "SELECT * FROM transactions WHERE cp_id = ? AND connector_id = ? AND stopped_at IS NULL ORDER BY id DESC LIMIT 1",
            (cp_id, connector_id),
        ).fetchone()
        if row:
            return _normalize_transaction_rows(conn, [row])[0]
    return get_active_transaction_for_cp(cp_id)


def insert_connector_status_history(
    cp_id: str,
    connector_id: int,
    status: str,
    timestamp: str,
    transaction_id: Optional[int] = None,
    source: str = "live",
    source_event_id: Optional[int] = None,
):
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO connector_status_history "
        "(cp_id, connector_id, status, timestamp, transaction_id, source, source_event_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (cp_id, connector_id, status, timestamp, transaction_id, source, source_event_id),
    )
    conn.commit()


def get_connector_status_history(cp_id: str, connector_id: int, start_ts: str, end_ts: str) -> List[dict]:
    """Return event-sourced status rows for connector plus connector 0 in time window."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT connector_id, status, timestamp, transaction_id "
        "FROM connector_status_history "
        "WHERE cp_id = ? AND timestamp >= ? AND timestamp <= ? "
        "AND (connector_id = ? OR connector_id = 0) "
        "ORDER BY timestamp ASC, id ASC",
        (cp_id, start_ts, end_ts, connector_id),
    ).fetchall()
    return [dict(r) for r in rows]


def backfill_status_history_from_event_log() -> int:
    """Backfill connector_status_history from historical StatusNotification event_log entries."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT COALESCE(MAX(source_event_id), 0) AS max_id "
        "FROM connector_status_history WHERE source = 'event_log_backfill'"
    ).fetchone()
    last_id = int(row["max_id"] or 0)

    rows = conn.execute(
        "SELECT id, cp_id, timestamp, message FROM event_log "
        "WHERE id > ? "
        "AND (ocpp_command = 'StatusNotification' OR message LIKE 'StatusNotification%') "
        "ORDER BY id ASC",
        (last_id,),
    ).fetchall()

    inserted = 0
    for r in rows:
        msg = r["message"] or ""
        m_status = re.search(r"status=([A-Za-z.]+)", msg)
        m_connector = re.search(r"connector=(\d+)", msg)
        if not m_status or not m_connector:
            continue

        cp_id = r["cp_id"]
        connector_id = int(m_connector.group(1))
        status = m_status.group(1)
        ts = r["timestamp"]

        # Best-effort transaction linking by CP/connector and timestamp window.
        txn = None
        if cp_id:
            if connector_id > 0:
                txn = conn.execute(
                    "SELECT id FROM transactions "
                    "WHERE cp_id = ? AND connector_id = ? "
                    "AND started_at <= ? "
                    "AND (stopped_at IS NULL OR stopped_at >= ?) "
                    "ORDER BY id DESC LIMIT 1",
                    (cp_id, connector_id, ts, ts),
                ).fetchone()
            if not txn:
                txn = conn.execute(
                    "SELECT id FROM transactions "
                    "WHERE cp_id = ? AND started_at <= ? "
                    "AND (stopped_at IS NULL OR stopped_at >= ?) "
                    "ORDER BY id DESC LIMIT 1",
                    (cp_id, ts, ts),
                ).fetchone()

        cur = conn.execute(
            "INSERT OR IGNORE INTO connector_status_history "
            "(cp_id, connector_id, status, timestamp, transaction_id, source, source_event_id) "
            "VALUES (?, ?, ?, ?, ?, 'event_log_backfill', ?)",
            (cp_id, connector_id, status, ts, (txn["id"] if txn else None), r["id"]),
        )
        if cur.rowcount and cur.rowcount > 0:
            inserted += 1

    conn.commit()
    return inserted


def record_charging_start(transaction_id: int, timestamp: str):
    """Record when power actually started flowing (first Charging status)."""
    conn = _get_conn()
    conn.execute(
        "UPDATE transactions SET charging_start = ? WHERE id = ? AND charging_start IS NULL",
        (timestamp, transaction_id),
    )
    conn.commit()


def record_charging_stop(transaction_id: int, timestamp: str):
    """Record when power stopped flowing (SuspendedEV / StopTransaction).
    Only records if charging_start is already set (can't stop before starting).
    Overwrites previous value so the last stop time is captured."""
    conn = _get_conn()
    conn.execute(
        "UPDATE transactions SET charging_stop = ? WHERE id = ? AND charging_start IS NOT NULL",
        (timestamp, transaction_id),
    )
    conn.commit()


def start_transaction(cp_id: str, connector_id: int, id_tag: str, meter_start: int, timestamp: str) -> int:
    conn = _get_conn()
    stored_id_tag = _resolve_rfid_fk_value(conn, id_tag)
    row = conn.execute("SELECT MAX(id) FROM transactions").fetchone()
    next_id = (row[0] or 0) + 1
    conn.execute(
        "INSERT INTO transactions (id, cp_id, connector_id, id_tag, meter_start, started_at) VALUES (?, ?, ?, ?, ?, ?)",
        (next_id, cp_id, connector_id, stored_id_tag, meter_start, timestamp),
    )
    conn.commit()
    return next_id


def _calculate_hourly_cost(conn, transaction_id: int, started_at: str, stopped_at: str,
                            meter_start: int, meter_stop: int) -> dict:
    """Calculate charging cost using actual per-quarter-hour tariffs from the database.

    Uses meter values (Energy.Active.Import.Register) to determine energy per
    15-minute slot (Zonneplan publishes prices per quarter-hour, not per hour).
    Falls back to proportional distribution if meter values are sparse.

    Returns dict with 'cost_eur', 'avg_tariff', 'hours' (detail per quarter-hour slot).
    """
    from datetime import datetime, timezone, timedelta
    import dateutil.parser

    total_energy_wh = meter_stop - meter_start
    if total_energy_wh <= 0:
        return {"cost_eur": 0.0, "avg_tariff": None, "hours": []}

    dt_start = dateutil.parser.parse(started_at).astimezone(timezone.utc)
    dt_stop = dateutil.parser.parse(stopped_at).astimezone(timezone.utc)

    if dt_stop <= dt_start:
        return {"cost_eur": 0.0, "avg_tariff": None, "hours": []}

    # Fetch energy meter values for this transaction, ordered by time
    rows = conn.execute(
        "SELECT timestamp, value FROM meter_values "
        "WHERE transaction_id = ? AND measurand = 'Energy.Active.Import.Register' "
        "ORDER BY timestamp ASC",
        (transaction_id,),
    ).fetchall()

    # Build time→energy pairs: start + meter readings + stop
    readings = []
    readings.append((dt_start, float(meter_start)))
    for r in rows:
        try:
            ts = dateutil.parser.parse(r["timestamp"]).astimezone(timezone.utc)
            val = float(r["value"])
            # Only include readings within the session time window
            if ts < dt_start or ts > dt_stop:
                continue
            readings.append((ts, val))
        except (ValueError, TypeError):
            continue
    readings.append((dt_stop, float(meter_stop)))

    # Deduplicate and sort by timestamp
    seen = set()
    unique = []
    for ts, val in readings:
        key = ts.isoformat()
        if key not in seen:
            seen.add(key)
            unique.append((ts, val))
    readings = sorted(unique, key=lambda x: x[0])

    # Build per-quarter-hour energy buckets by interpolating between meter readings
    hour_energy = {}  # "YYYY-MM-DD HH:MM" (quarter-hour slot start) -> Wh consumed in that slot

    for i in range(len(readings) - 1):
        seg_start_ts, seg_start_val = readings[i]
        seg_end_ts, seg_end_val = readings[i + 1]
        seg_energy = seg_end_val - seg_start_val
        seg_duration = (seg_end_ts - seg_start_ts).total_seconds()

        if seg_duration <= 0 or seg_energy < 0:
            continue

        # Walk through quarter-hour boundaries within this segment
        cursor = seg_start_ts
        while cursor < seg_end_ts:
            slot_start = cursor.replace(minute=(cursor.minute // 15) * 15, second=0, microsecond=0)
            hour_key = slot_start.strftime("%Y-%m-%d %H:%M")
            next_quarter = slot_start + timedelta(minutes=15)
            seg_slice_end = min(next_quarter, seg_end_ts)
            slice_seconds = (seg_slice_end - cursor).total_seconds()

            # Proportional energy for this slice of the segment
            slice_energy = seg_energy * (slice_seconds / seg_duration) if seg_duration > 0 else 0

            hour_energy[hour_key] = hour_energy.get(hour_key, 0.0) + slice_energy
            cursor = seg_slice_end

    # Look up tariffs and calculate cost per hour
    total_cost = 0.0
    total_tariffed_energy = 0.0
    hours_detail = []

    for hour_key in sorted(hour_energy.keys()):
        energy_wh = hour_energy[hour_key]
        energy_kwh = energy_wh / 1000.0
        tariff = get_zonneplan_tariff_for_hour(hour_key)
        if tariff is None:
            tariff = get_default_tariff()

        hour_cost = None
        if tariff is not None and energy_kwh > 0:
            hour_cost = round(energy_kwh * tariff, 6)
            total_cost += hour_cost
            total_tariffed_energy += energy_wh

        hours_detail.append({
            "hour": hour_key,
            "energy_wh": round(energy_wh, 1),
            "tariff_eur_per_kwh": tariff,
            "cost_eur": hour_cost,
        })

    avg_tariff = round(total_cost / (total_tariffed_energy / 1000.0), 6) if total_tariffed_energy > 0 else None

    return {
        "cost_eur": round(total_cost, 4),
        "avg_tariff": avg_tariff,
        "hours": hours_detail,
    }


def _store_tariff_on_transaction(conn, transaction_id: int, meter_start: int, meter_stop: int):
    """Calculate hourly cost and persist weighted average tariff + total cost."""
    row = conn.execute(
        "SELECT started_at, stopped_at FROM transactions WHERE id = ?",
        (transaction_id,),
    ).fetchone()
    if not row or not row["started_at"] or not row["stopped_at"]:
        return
    if meter_stop is None or meter_start is None or meter_stop <= meter_start:
        return

    result = _calculate_hourly_cost(
        conn, transaction_id, row["started_at"], row["stopped_at"], meter_start, meter_stop
    )

    conn.execute(
        "UPDATE transactions SET tariff_eur_per_kwh = ?, cost_eur = ? WHERE id = ?",
        (result["avg_tariff"], result["cost_eur"], transaction_id),
    )


def stop_transaction(transaction_id: int, meter_stop: int, timestamp: str, reason: str):
    conn = _get_conn()
    # Get meter_start for cost calculation
    row = conn.execute("SELECT meter_start FROM transactions WHERE id = ?", (transaction_id,)).fetchone()
    conn.execute(
        "UPDATE transactions SET meter_stop = ?, stopped_at = ?, stop_reason = ? WHERE id = ?",
        (meter_stop, timestamp, reason, transaction_id),
    )
    if row:
        _store_tariff_on_transaction(conn, transaction_id, row["meter_start"], meter_stop)
    conn.commit()


def update_transaction_status(transaction_id: int, status: str):
    """Update the last_status field on a transaction from a StatusNotification."""
    conn = _get_conn()
    conn.execute(
        "UPDATE transactions SET last_status = ? WHERE id = ?",
        (status, transaction_id),
    )
    conn.commit()


def stop_active_transaction_for_cp(cp_id: str, reason: str = "EVSuspended") -> Optional[int]:
    """Close the active transaction for a charge point using the latest meter value."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT id, meter_start FROM transactions WHERE cp_id = ? AND stopped_at IS NULL ORDER BY id DESC LIMIT 1",
        (cp_id,),
    ).fetchone()
    if not row:
        return None
    txn_id = row["id"]
    # Get the latest energy meter value for this transaction
    mv = conn.execute(
        "SELECT value FROM meter_values WHERE transaction_id = ? AND measurand = 'Energy.Active.Import.Register' ORDER BY id DESC LIMIT 1",
        (txn_id,),
    ).fetchone()
    meter_stop = int(float(mv["value"])) if mv else row["meter_start"]
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE transactions SET meter_stop = ?, stopped_at = ?, stop_reason = ? WHERE id = ?",
        (meter_stop, now, reason, txn_id),
    )
    _store_tariff_on_transaction(conn, txn_id, row["meter_start"], meter_stop)
    conn.commit()
    return txn_id


def get_transactions(cp_id: Optional[str] = None, limit: Optional[int] = None, offset: int = 0) -> List[dict]:
    conn = _get_conn()
    sql = "SELECT * FROM transactions"
    params = []
    if cp_id:
        sql += " WHERE cp_id = ?"
        params.append(cp_id)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    return _normalize_transaction_rows(conn, rows)


def get_transactions_count(cp_id: Optional[str] = None) -> int:
    conn = _get_conn()
    sql = "SELECT COUNT(*) as cnt FROM transactions"
    params = []
    if cp_id:
        sql += " WHERE cp_id = ?"
        params.append(cp_id)
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else 0


def get_transaction(transaction_id: int) -> Optional[dict]:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM transactions WHERE id = ?", (transaction_id,)).fetchone()
    if not row:
        return None
    return _normalize_transaction_rows(conn, [row])[0]


def update_transaction(transaction_id: int, **fields):
    conn = _get_conn()
    allowed = {"cp_id", "connector_id", "id_tag", "meter_start", "meter_stop",
               "started_at", "stopped_at", "stop_reason", "charging_start", "charging_stop", "car_id",
               "tariff_eur_per_kwh", "cost_eur", "km"}
    nullable = {"km"}  # fields that may be explicitly set to NULL
    parts, params = [], []
    for k, v in fields.items():
        if k in allowed and (v is not None or k in nullable):
            if k == "id_tag":
                v = _resolve_rfid_fk_value(conn, v)
            parts.append(f"{k} = ?")
            params.append(v)
    if parts:
        params.append(transaction_id)
        conn.execute(f"UPDATE transactions SET {', '.join(parts)} WHERE id = ?", params)
        conn.commit()


def delete_transaction(transaction_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM meter_values WHERE transaction_id = ?", (transaction_id,))
    conn.execute("DELETE FROM transactions WHERE id = ?", (transaction_id,))
    conn.commit()


# ── Meter value helpers ─────────────────────────────────────────

def insert_meter_value(cp_id, connector_id, transaction_id, timestamp, measurand, value, unit, phase=None):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO meter_values (cp_id, connector_id, transaction_id, timestamp, measurand, value, unit, phase) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cp_id, connector_id, transaction_id, timestamp, measurand, value, unit, phase),
    )
    conn.commit()


def get_temperature_history(cp_id: str) -> List[dict]:
    """Return temperature meter values for a charge point, ordered by timestamp."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT timestamp, value, unit FROM meter_values "
        "WHERE cp_id = ? AND measurand = 'Temperature' ORDER BY timestamp ASC",
        (cp_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_meter_values(cp_id: Optional[str] = None, transaction_id: Optional[int] = None) -> List[dict]:
    conn = _get_conn()
    sql = "SELECT * FROM meter_values"
    params = []
    clauses = []
    if cp_id:
        clauses.append("cp_id = ?"); params.append(cp_id)
    if transaction_id:
        clauses.append("transaction_id = ?"); params.append(transaction_id)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT 500"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_transaction_current_import_timeline(transaction_id: int) -> List[dict]:
    """Return max Current.Import per timestamp for a transaction, ordered by time."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT timestamp, value FROM meter_values "
        "WHERE transaction_id = ? AND measurand = 'Current.Import' "
        "ORDER BY timestamp ASC, id ASC",
        (transaction_id,),
    ).fetchall()

    grouped = {}
    for r in rows:
        ts = r["timestamp"]
        try:
            val = float(r["value"])
        except (TypeError, ValueError):
            continue
        if ts not in grouped or val > grouped[ts]:
            grouped[ts] = val

    return [{"timestamp": ts, "current": grouped[ts]} for ts in sorted(grouped.keys())]


def get_latest_meter_values(cp_id: str, transaction_id: int = None) -> dict:
    """Return the most recent value for each measurand+phase of a charge point."""
    conn = _get_conn()
    if transaction_id:
        rows = conn.execute(
            "SELECT measurand, phase, value, unit, timestamp FROM meter_values "
            "WHERE cp_id = ? AND transaction_id = ? AND id IN ("
            "  SELECT MAX(id) FROM meter_values "
            "  WHERE cp_id = ? AND transaction_id = ? GROUP BY measurand, phase"
            ")",
            (cp_id, transaction_id, cp_id, transaction_id),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT measurand, phase, value, unit, timestamp FROM meter_values "
            "WHERE cp_id = ? AND id IN ("
            "  SELECT MAX(id) FROM meter_values WHERE cp_id = ? GROUP BY measurand, phase"
            ")",
            (cp_id, cp_id),
        ).fetchall()
    result = {}
    for r in rows:
        key = r["measurand"]
        if r["phase"]:
            key += "." + r["phase"]
        result[key] = {
            "value": r["value"],
            "unit": r["unit"],
            "timestamp": r["timestamp"],
            "phase": r["phase"],
        }
    return result


def get_active_transactions() -> List[dict]:
    """Return transactions that have not yet been stopped."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM transactions WHERE stopped_at IS NULL ORDER BY id DESC"
    ).fetchall()
    return _normalize_transaction_rows(conn, rows)


def get_last_transaction(cp_id: str = None) -> Optional[dict]:
    """Return the most recent transaction (active or completed)."""
    conn = _get_conn()
    if cp_id:
        row = conn.execute(
            "SELECT * FROM transactions WHERE cp_id = ? ORDER BY id DESC LIMIT 1",
            (cp_id,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM transactions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    return _normalize_transaction_rows(conn, [row])[0]


# ── Event log helpers ───────────────────────────────────────────

def log_event(
    cp_id: str,
    severity: str,
    message: str,
    ocpp_command: Optional[str] = None,
    ocpp_payload: Optional[str] = None,
    ocpp_response: Optional[str] = None,
    event_timestamp: Optional[str] = None,
):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    ts_to_store = event_timestamp or now
    
    # Extract OCPP command from message if not provided
    if not ocpp_command:
        # Parse command from message like "BootNotification from..." -> "BootNotification"
        # or "Sending RemoteStartTransaction..." -> "RemoteStartTransaction"
        import re
        match = re.search(r'\b([A-Z][a-z]+(?:[A-Z][a-z]+)*)\b', message)
        if match:
            potential_cmd = match.group(1)
            # Check if it looks like an OCPP command (starts with capital, contains mixed case)
            if potential_cmd[0].isupper():
                ocpp_command = potential_cmd
                # Remove the command from the message for cleaner display
                message = re.sub(r'\b' + re.escape(potential_cmd) + r'\b', '', message).strip()
                # Clean up extra spaces and common prefixes
                message = re.sub(r'^(from|Sending|response|Received)\s+', '', message, flags=re.IGNORECASE).strip()
    
    conn.execute(
        "INSERT INTO event_log (cp_id, timestamp, severity, message, ocpp_command, ocpp_payload, ocpp_response) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (cp_id, ts_to_store, severity, message, ocpp_command, ocpp_payload, ocpp_response),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def update_event_response(event_id: int, ocpp_response: str):
    conn = _get_conn()
    conn.execute("UPDATE event_log SET ocpp_response = ? WHERE id = ?", (ocpp_response, event_id))
    conn.commit()


def get_events(
    cp_id: Optional[str] = None,
    query: Optional[str] = None,
    ocpp_command: Optional[str] = None,
    severity_min: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> List[dict]:
    conn = _get_conn()
    sql = "SELECT * FROM event_log"
    params = []
    clauses = []
    if cp_id:
        clauses.append("cp_id = ?"); params.append(cp_id)
    if query:
        clauses.append("(message LIKE ? OR severity LIKE ?)"); params.extend([f"%{query}%", f"%{query}%"])
    if ocpp_command:
        clauses.append("ocpp_command = ?"); params.append(ocpp_command)
    sev = (severity_min or "").strip().upper()
    if sev and sev != "DEBUG":
        sev_rank = {
            "DEBUG": 0,
            "INFO": 1,
            "WARNING": 2,
            "ERROR": 3,
        }
        min_rank = sev_rank.get(sev)
        if min_rank is not None:
            clauses.append(
                "(CASE UPPER(severity) "
                "WHEN 'DEBUG' THEN 0 "
                "WHEN 'INFO' THEN 1 "
                "WHEN 'WARNING' THEN 2 "
                "WHEN 'ERROR' THEN 3 "
                "ELSE 0 END) >= ?"
            )
            params.append(min_rank)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_events_count(
    cp_id: Optional[str] = None,
    query: Optional[str] = None,
    ocpp_command: Optional[str] = None,
    severity_min: Optional[str] = None,
) -> int:
    conn = _get_conn()
    sql = "SELECT COUNT(*) as cnt FROM event_log"
    params = []
    clauses = []
    if cp_id:
        clauses.append("cp_id = ?"); params.append(cp_id)
    if query:
        clauses.append("(message LIKE ? OR severity LIKE ?)"); params.extend([f"%{query}%", f"%{query}%"])
    if ocpp_command:
        clauses.append("ocpp_command = ?"); params.append(ocpp_command)
    sev = (severity_min or "").strip().upper()
    if sev and sev != "DEBUG":
        sev_rank = {
            "DEBUG": 0,
            "INFO": 1,
            "WARNING": 2,
            "ERROR": 3,
        }
        min_rank = sev_rank.get(sev)
        if min_rank is not None:
            clauses.append(
                "(CASE UPPER(severity) "
                "WHEN 'DEBUG' THEN 0 "
                "WHEN 'INFO' THEN 1 "
                "WHEN 'WARNING' THEN 2 "
                "WHEN 'ERROR' THEN 3 "
                "ELSE 0 END) >= ?"
            )
            params.append(min_rank)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else 0


def get_status_notifications(cp_id: str, connector_id: int, start_ts: str, end_ts: str) -> List[dict]:
    """Compatibility wrapper: return status rows from connector_status_history."""
    return get_connector_status_history(cp_id, connector_id, start_ts, end_ts)


def get_ocpp_commands() -> List[dict]:
    """Return list of distinct OCPP commands used in the system with counts."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT ocpp_command, COUNT(*) as count FROM event_log WHERE ocpp_command IS NOT NULL AND ocpp_command != '' GROUP BY ocpp_command ORDER BY count DESC"
    ).fetchall()
    return [dict(r) for r in rows]


# ── RFID helpers ───────────────────────────────────────────────

def add_rfid(id_tag: str, assigned_to: str = None, account: str = None, site: str = None):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO rfids (id_tag, assigned_to, account, site, created_at) VALUES (?, ?, ?, ?, ?)",
        (id_tag, assigned_to or '', account or '', site or '', now)
    )
    conn.commit()


def get_rfids(account: str = None, site: str = None) -> List[dict]:
    conn = _get_conn()
    sql = "SELECT * FROM rfids"
    params = []
    clauses = []
    if account:
        clauses.append("account = ?"); params.append(account)
    if site:
        clauses.append("site = ?"); params.append(site)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def update_rfid(rfid_id: int, id_tag: str = None, assigned_to: str = None, account: str = None, site: str = None, blocked: int = None):
    conn = _get_conn()
    parts = []
    params = []
    if id_tag is not None:
        parts.append("id_tag = ?")
        params.append(id_tag)
    if assigned_to is not None:
        parts.append("assigned_to = ?")
        params.append(assigned_to)
    if account is not None:
        parts.append("account = ?")
        params.append(account)
    if site is not None:
        parts.append("site = ?")
        params.append(site)
    if blocked is not None:
        parts.append("blocked = ?")
        params.append(blocked)
    if parts:
        params.append(rfid_id)
        conn.execute(f"UPDATE rfids SET {', '.join(parts)} WHERE id = ?", params)
        conn.commit()


def delete_rfid(rfid_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM rfids WHERE id = ?", (rfid_id,))
    conn.commit()


def import_rfids(rfid_list):
    count = 0
    for item in rfid_list:
        id_tag = str(item.get('id_tag', '')).strip()
        if not id_tag:
            continue
        charge_point = item.get('charge_point') or item.get('account') or ''
        add_rfid(id_tag, assigned_to=item.get('assigned_to'), account=charge_point, site=item.get('site'))
        count += 1
    return count


def is_id_tag_authorized(id_tag: str) -> bool:
    """Return True if *id_tag* exists in the rfids whitelist and is not blocked."""
    return get_id_tag_auth_status(id_tag) == "accepted"


def get_id_tag_auth_status(id_tag: str) -> str:
    """Return 'accepted', 'blocked', or 'invalid' for the given id_tag."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT blocked FROM rfids WHERE id_tag = ? LIMIT 1", (id_tag,)
    ).fetchone()
    if row is None:
        return "invalid"
    return "blocked" if row["blocked"] else "accepted"


# ── OCPP Config helpers ─────────────────────────────────────────

def get_ocpp_config() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT key, value FROM ocpp_config").fetchall()
    return {r["key"]: r["value"] for r in rows}


def get_ocpp_config_value(key: str, default: str = None) -> str:
    conn = _get_conn()
    row = conn.execute("SELECT value FROM ocpp_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_ocpp_config_value(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO ocpp_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── Mail Config helpers ──────────────────────────────────────────

def get_mail_config() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT key, value FROM mail_config").fetchall()
    return {r["key"]: r["value"] for r in rows}


def get_mail_config_value(key: str, default: str = None) -> str:
    conn = _get_conn()
    row = conn.execute("SELECT value FROM mail_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_mail_config_value(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO mail_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── Zonneplan Config helpers ───────────────────────────────────────

def get_zonneplan_config() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT key, value FROM zonneplan_config").fetchall()
    return {r["key"]: r["value"] for r in rows}


def set_zonneplan_config_value(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO zonneplan_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── App Config helpers ────────────────────────────────────────────

def get_app_config() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT key, value FROM app_config").fetchall()
    return {r["key"]: r["value"] for r in rows}


def get_app_config_value(key: str, default: str = None) -> str:
    conn = _get_conn()
    row = conn.execute("SELECT value FROM app_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_app_config_value(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO app_config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()


# ── Chargepoint charge values helpers ─────────────────────────────

def get_chargepoint_charge_values(cp_id: str = None):
    conn = _get_conn()
    if cp_id is not None:
        row = conn.execute(
            "SELECT * FROM chargepoint_charge_values WHERE cp_id = ?", (cp_id,)
        ).fetchone()
        return dict(row) if row else None
    rows = conn.execute("SELECT * FROM chargepoint_charge_values").fetchall()
    return [dict(r) for r in rows]


def set_chargepoint_charge_values(cp_id: str, num_phases: int, max_amp_l1: float = None,
                                  max_amp_l2: float = None, max_amp_l3: float = None,
                                  voltage: float = 230):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO chargepoint_charge_values "
        "(cp_id, num_phases, max_amp_l1, max_amp_l2, max_amp_l3, voltage, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(cp_id) DO UPDATE SET "
        "num_phases = excluded.num_phases, "
        "max_amp_l1 = excluded.max_amp_l1, "
        "max_amp_l2 = excluded.max_amp_l2, "
        "max_amp_l3 = excluded.max_amp_l3, "
        "voltage    = excluded.voltage, "
        "updated_at = excluded.updated_at",
        (cp_id, num_phases, max_amp_l1, max_amp_l2, max_amp_l3, voltage, now),
    )
    conn.commit()


def delete_chargepoint_charge_values(cp_id: str):
    conn = _get_conn()
    conn.execute("DELETE FROM chargepoint_charge_values WHERE cp_id = ?", (cp_id,))
    conn.commit()


# ── Zonneplan Tariff helpers ──────────────────────────────────────

def store_zonneplan_tariffs(forecast: list):
    """Store hourly tariff entries. Skips hours that already exist.
    Returns dict with stored/skipped counts and per-entry details."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    stored = 0
    skipped = 0
    details = []
    for entry in forecast:
        hour = entry.get("hour")
        tariff = entry.get("tariff_eur_per_kwh")
        group = entry.get("tariff_group")
        if hour and tariff is not None:
            cur = conn.execute(
                "INSERT OR IGNORE INTO zonneplan_tariffs (hour, tariff_eur_per_kwh, tariff_group, fetched_at) "
                "VALUES (?, ?, ?, ?)",
                (hour, tariff, group, now),
            )
            if cur.rowcount > 0:
                stored += 1
                details.append({"hour": hour, "tariff": tariff, "group": group, "action": "inserted"})
            else:
                skipped += 1
                details.append({"hour": hour, "tariff": tariff, "group": group, "action": "skipped"})
    conn.commit()
    return {"stored": stored, "skipped": skipped, "details": details}


def get_zonneplan_tariffs(start: str = None, end: str = None) -> List[dict]:
    """Get stored tariffs, optionally filtered by hour range."""
    conn = _get_conn()
    query = "SELECT * FROM zonneplan_tariffs"
    params = []
    clauses = []
    if start:
        clauses.append("hour >= ?")
        params.append(start)
    if end:
        clauses.append("hour <= ?")
        params.append(end)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY hour DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_zonneplan_tariff_for_hour(hour: str) -> float | None:
    """Get stored tariff for a specific quarter-hour slot key (e.g. '2026-04-16 18:15')."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT tariff_eur_per_kwh FROM zonneplan_tariffs WHERE hour = ?", (hour,)
    ).fetchone()
    return row["tariff_eur_per_kwh"] if row else None


def get_default_tariff() -> float:
    """Configurable fallback tariff (eur/kWh) used when no hourly price exists."""
    raw = get_app_config_value("default_tariff_eur_per_kwh")
    try:
        if raw is not None and str(raw).strip() != "":
            return float(raw)
    except (ValueError, TypeError):
        pass
    return DEFAULT_TARIFF_EUR_PER_KWH


# ── Car helpers ──────────────────────────────────────────────────

def get_cars() -> List[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM cars ORDER BY brand, model").fetchall()
    return [dict(r) for r in rows]


def add_car(brand: str, model: str, year: int, kwh_per_100km: float, license_plate: str = '', battery_capacity_kwh: float = None):
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO cars (brand, model, year, kwh_per_100km, license_plate, battery_capacity_kwh, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (brand, model, year, kwh_per_100km, license_plate, battery_capacity_kwh, now),
    )
    conn.commit()


def update_car(car_id: int, brand: str = None, model: str = None, year: int = None, kwh_per_100km: float = None, license_plate: str = None, battery_capacity_kwh: float = None):
    conn = _get_conn()
    parts, params = [], []
    if brand is not None:
        parts.append("brand = ?"); params.append(brand)
    if model is not None:
        parts.append("model = ?"); params.append(model)
    if year is not None:
        parts.append("year = ?"); params.append(year)
    if kwh_per_100km is not None:
        parts.append("kwh_per_100km = ?"); params.append(kwh_per_100km)
    if license_plate is not None:
        parts.append("license_plate = ?"); params.append(license_plate)
    if battery_capacity_kwh is not None:
        parts.append("battery_capacity_kwh = ?"); params.append(battery_capacity_kwh)
    if parts:
        params.append(car_id)
        conn.execute(f"UPDATE cars SET {', '.join(parts)} WHERE id = ?", params)
        conn.commit()


def delete_car(car_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM cars WHERE id = ?", (car_id,))
    conn.commit()


# ── Smart Schedule helpers ──────────────────────────────────────

def get_smart_schedules(cp_id: str = None, limit: Optional[int] = None, offset: int = 0) -> List[dict]:
    conn = _get_conn()
    sql = "SELECT * FROM smart_schedules"
    params = []
    if cp_id:
        sql += " WHERE cp_id = ?"
        params.append(cp_id)
    sql += " ORDER BY start_at DESC"
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_smart_schedules_count(cp_id: str = None) -> int:
    conn = _get_conn()
    sql = "SELECT COUNT(*) as cnt FROM smart_schedules"
    params = []
    if cp_id:
        sql += " WHERE cp_id = ?"
        params.append(cp_id)
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else 0


def get_smart_schedule(schedule_id: int) -> Optional[dict]:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM smart_schedules WHERE id = ?", (schedule_id,)).fetchone()
    return dict(row) if row else None


def add_smart_schedule(cp_id: str, id_tag: str, start_at: str, stop_at: str) -> int:
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO smart_schedules (cp_id, id_tag, start_at, stop_at, status, created_at) VALUES (?, ?, ?, ?, 'scheduled', ?)",
        (cp_id, id_tag, start_at, stop_at, now),
    )
    conn.commit()
    return cur.lastrowid


def update_smart_schedule(schedule_id: int, **fields):
    conn = _get_conn()
    allowed = {"id_tag", "start_at", "stop_at", "status", "transaction_id"}
    parts, params = [], []
    for k, v in fields.items():
        if k in allowed:
            parts.append(f"{k} = ?")
            params.append(v)
    if parts:
        params.append(schedule_id)
        conn.execute(f"UPDATE smart_schedules SET {', '.join(parts)} WHERE id = ?", params)
        conn.commit()


def delete_smart_schedule(schedule_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM smart_schedules WHERE id = ?", (schedule_id,))
    conn.commit()


def get_active_smart_schedules(cp_id: str = None) -> List[dict]:
    """Return schedules that are scheduled or started (not completed/cancelled)."""
    conn = _get_conn()
    sql = "SELECT * FROM smart_schedules WHERE status IN ('scheduled', 'started')"
    params = []
    if cp_id:
        sql += " AND cp_id = ?"
        params.append(cp_id)
    sql += " ORDER BY start_at"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
