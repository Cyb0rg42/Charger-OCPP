"""
Script:   test_cs_db.py

Abstract:
    Unit tests for cs_db.py. Each test case runs against a fresh temporary
    SQLite database (created in setUp, removed in tearDown) so tests don't
    touch a real CSMS database.

Features:
    - TestCsDbConnectorZeroFilter: connector 0 (whole-station) rows are
      excluded from per-connector queries.
    - TestCsDbDeleteChargePoint: deleting a charge point cascades to all of
      its related rows (transactions, meter values, events, ...).

Usage:
    python -m unittest test_cs_db.py

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
import tempfile
import unittest
from datetime import datetime, timezone

import cs_db


class TestCsDbConnectorZeroFilter(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        cs_db.DB_PATH = os.path.join(self.tempdir.name, "csms.db")
        cs_db._local.conn = None
        cs_db.init_db()

    def tearDown(self):
        if hasattr(cs_db._local, "conn") and cs_db._local.conn:
            try:
                cs_db._local.conn.close()
            except Exception:
                pass
            cs_db._local.conn = None
        self.tempdir.cleanup()

    def test_connector_zero_is_excluded_from_cp_connectors(self):
        cs_db.upsert_chargepoint("CP001", vendor="DemoVendor", model="DemoModel", status="Available")

        conn = cs_db._get_conn()
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            "INSERT INTO connector_status (cp_id, connector_id, status, updated_at) VALUES (?, ?, ?, ?)",
            ("CP001", 0, "Available", now),
        )
        conn.execute(
            "INSERT INTO connector_status (cp_id, connector_id, status, updated_at) VALUES (?, ?, ?, ?)",
            ("CP001", 1, "Available", now),
        )
        conn.execute(
            "INSERT INTO connector_status (cp_id, connector_id, status, updated_at) VALUES (?, ?, ?, ?)",
            ("CP001", 2, "Charging", now),
        )
        conn.commit()

        cps = cs_db.get_chargepoints()
        self.assertEqual(len(cps), 1)

        connectors = cps[0].get("connectors", [])
        connector_ids = [c.get("connector_id") for c in connectors]

        self.assertEqual(connector_ids, [1, 2])
        self.assertNotIn(0, connector_ids)


class TestCsDbDeleteChargePoint(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        cs_db.DB_PATH = os.path.join(self.tempdir.name, "csms.db")
        cs_db._local.conn = None
        cs_db.init_db()

    def tearDown(self):
        if hasattr(cs_db._local, "conn") and cs_db._local.conn:
            try:
                cs_db._local.conn.close()
            except Exception:
                pass
            cs_db._local.conn = None
        self.tempdir.cleanup()

    def test_delete_chargepoint_removes_all_cp_artifacts(self):
        cp_id = "CP-DELETE"
        now = datetime.now(timezone.utc).isoformat()

        cs_db.upsert_chargepoint(cp_id, vendor="DemoVendor", model="DemoModel", status="Available")
        cs_db.update_connector_status(cp_id, 1, "Charging", now)
        txn_id = cs_db.start_transaction(cp_id, 1, "TAG-1", 1000, now)
        cs_db.insert_meter_value(cp_id, 1, txn_id, now, "Current.Import", "16.0", "A")
        cs_db.log_event(cp_id, "INFO", "Heartbeat received")
        cs_db.insert_connector_status_history(cp_id, 1, "Preparing", now, txn_id, source="test")
        cs_db.add_smart_schedule(cp_id, "TAG-1", now, now)
        cs_db.add_rfid("RFID-1", assigned_to="User", account=cp_id, site="Test Site")

        deleted = cs_db.delete_chargepoint(cp_id)
        self.assertIsNotNone(deleted)
        self.assertEqual(deleted["chargepoints"], 1)
        self.assertEqual(deleted["connector_status"], 1)
        self.assertEqual(deleted["connector_status_history"], 1)
        self.assertEqual(deleted["transactions"], 1)
        self.assertEqual(deleted["meter_values"], 1)
        self.assertEqual(deleted["event_log"], 1)
        self.assertEqual(deleted["smart_schedules"], 1)
        self.assertEqual(deleted["rfids_cleared"], 1)

        conn = cs_db._get_conn()
        self.assertIsNone(conn.execute("SELECT 1 FROM chargepoints WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM connector_status WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM connector_status_history WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM transactions WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM meter_values WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM event_log WHERE cp_id = ?", (cp_id,)).fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM smart_schedules WHERE cp_id = ?", (cp_id,)).fetchone())

        rfid = conn.execute("SELECT account FROM rfids WHERE id_tag = ?", ("RFID-1",)).fetchone()
        self.assertIsNotNone(rfid)
        self.assertEqual(rfid["account"], "")


if __name__ == "__main__":
    unittest.main()
