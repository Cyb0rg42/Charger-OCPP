#!/usr/bin/env python3
"""
Script:   ocppproxy.py

Abstract:
    Entry point for the OCPP Proxy server. Starts the Flask + Socket.IO web
    dashboard (web port, default 9300) and the OCPP WebSocket proxy that
    charge points connect to (proxy_core.py, listen port stored in the
    database, seeded from PROXY_LISTEN_PORT, default 9310), which forwards,
    inspects, logs, and can selectively block OCPP traffic to one or more
    CSMS backends.

Features:
    - JSON logging split across ocpp/access/error log files.
    - Config (name, web port, log paths) via --config (YAML or JSON) or the
      PROXY_CONFIG environment variable (e.g. when launched through
      gunicorn as `ocppproxy:app`).
    - Background proxy thread started once, independent of the web
      server's own lifecycle/reloads.

Usage:
    python ocppproxy.py --config /etc/charger/ocppproxy01.yaml

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
import json
import logging
import os
import threading
from datetime import datetime, timezone

import yaml

import proxy_db
import proxy_core
import web_app
from web_app import app, socketio

logger = logging.getLogger("proxy")

DEFAULT_WEB_PORT = 9300
_proxy_started = False


# ── Logging ─────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        })


def _resolve_log_dir():
    log_dir = os.environ.get("PROXY_LOG_DIR", "/var/log/charger")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError:
        # Fallback to a local log directory when /var/log/charger isn't writable
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log")
        os.makedirs(log_dir, exist_ok=True)
    return log_dir


def configure_logging(logs_cfg=None):
    """Configure the three log files (ocpp, access, error).

    `logs_cfg` is the optional "logs" mapping from the config file; any path it
    omits falls back to a default inside the resolved log directory.
    """
    logs_cfg = logs_cfg or {}
    log_dir = _resolve_log_dir()

    def _path(key, default):
        p = logs_cfg.get(key) or os.path.join(log_dir, default)
        try:
            os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
        except OSError:
            p = os.path.join(log_dir, default)
        return p

    ocpp_log = _path("ocpp_log", "ocppproxy.json")
    access_log = _path("access_log", "ocppproxy-access.json")
    error_log = _path("error_log", "ocppproxy-error.json")

    fmt = JsonFormatter()

    # Root logger → console + OCPP/application log (INFO+) and error log (ERROR+)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    app_handler = logging.FileHandler(ocpp_log, encoding="utf-8")
    app_handler.setFormatter(fmt)
    root.addHandler(app_handler)

    err_handler = logging.FileHandler(error_log, encoding="utf-8")
    err_handler.setFormatter(fmt)
    err_handler.setLevel(logging.ERROR)
    root.addHandler(err_handler)

    # Web request logging (werkzeug) → dedicated access log
    access_logger = logging.getLogger("werkzeug")
    access_logger.setLevel(logging.INFO)
    access_logger.handlers.clear()
    access_handler = logging.FileHandler(access_log, encoding="utf-8")
    access_handler.setFormatter(fmt)
    access_logger.addHandler(access_handler)
    access_logger.propagate = False

    logger.info(
        "Logging configured: ocpp=%s access=%s error=%s",
        ocpp_log, access_log, error_log,
    )


# ── Configuration ───────────────────────────────────────────────

def load_config_file(path):
    """Load a YAML/JSON config file (JSON is valid YAML)."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def apply_config(cfg):
    """Apply a loaded config: name (title), logging. Returns the web port."""
    name = cfg.get("name")
    if name:
        web_app.set_system_name(name)
    configure_logging(cfg.get("logs"))
    return int(cfg.get("port", DEFAULT_WEB_PORT))


# ── Start the async WebSocket proxy in a background thread ──────

def _run_ws_proxy():
    """Run the asyncio WebSocket proxy in its own event loop."""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    proxy_db.init_db()
    loop.run_until_complete(proxy_core.start_proxy())
    # start_proxy() only starts the listener + health-check task and returns;
    # run_forever() is what actually keeps this loop (and thread) alive for
    # the rest of the process's life, independent of the WS server's own
    # lifecycle (see the comment in proxy_core.start_proxy()).
    loop.run_forever()


def start_background_proxy():
    global _proxy_started
    if _proxy_started:
        return
    _proxy_started = True
    t = threading.Thread(target=_run_ws_proxy, daemon=True)
    t.start()
    logger.info("WebSocket proxy thread started")


# ── Import-time setup (e.g. gunicorn `ocppproxy:app`) ───────────
# Only runs when imported as a module (not when executed directly), so that
# `python ocppproxy.py --config ...` configures logging exactly once from the
# config file in main() instead of first creating default-named log files.

if __name__ != "__main__":
    _env_config = os.environ.get("PROXY_CONFIG")
    if _env_config:
        try:
            apply_config(load_config_file(_env_config))
        except Exception:
            configure_logging()
            logger.exception("Failed to load PROXY_CONFIG %s", _env_config)
    else:
        configure_logging()

    start_background_proxy()


# ── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OCPP Proxy Server")
    parser.add_argument(
        "-c", "--config",
        default=os.environ.get("PROXY_CONFIG"),
        help="Path to YAML/JSON configuration file",
    )
    args = parser.parse_args()

    web_port = DEFAULT_WEB_PORT
    if args.config:
        web_port = apply_config(load_config_file(args.config))
    else:
        configure_logging()

    start_background_proxy()
    logger.info("Starting OCPP Proxy web dashboard on port %s", web_port)
    socketio.run(app, host="0.0.0.0", port=web_port, debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()
