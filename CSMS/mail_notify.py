"""
Script:   mail_notify.py

Abstract:
    Email notification utility for the CSMS. Sends SMTP mail (e.g. charge
    point offline/online, transaction alerts) using server settings stored
    in the CSMS database via cs_db.py, over SSL or STARTTLS depending on
    configuration.

Features:
    - send_mail(): builds and sends a MIME text email via smtplib.
    - send_test_mail(): sends a fixed test message for the settings page's
      "Send test email" button.
    - notify(): fire-and-forget wrapper keyed by event type, used by the
      rest of the app to raise notifications without handling SMTP details.

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

import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import cs_db

log = logging.getLogger(__name__)


def _get_config() -> dict:
    return cs_db.get_mail_config()


def send_mail(subject: str, body: str, config: dict = None):
    """Send an email using the stored mail configuration."""
    cfg = config or _get_config()
    if cfg.get("enabled") != "true":
        return
    host = cfg.get("host", "").strip()
    if not host:
        log.warning("Mail notification enabled but no SMTP host configured")
        return
    port = int(cfg.get("port", "25"))
    from_addr = cfg.get("from", "").strip()
    username = cfg.get("username", "").strip()
    password = cfg.get("password", "").strip()
    recipients_raw = cfg.get("recipients", "").strip()
    if not recipients_raw:
        log.warning("Mail notification enabled but no recipients configured")
        return
    recipients = [r.strip() for r in recipients_raw.replace(";", ",").split(",") if r.strip()]
    if not recipients:
        return

    msg = MIMEMultipart()
    msg["From"] = from_addr or f"charger@{host}"
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    protocol = cfg.get("protocol", "smtp").lower()
    try:
        if protocol == "smtps":
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(host, port, context=context, timeout=10)
        else:
            server = smtplib.SMTP(host, port, timeout=10)
            server.ehlo()
            if protocol == "starttls":
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
        if username and password:
            server.login(username, password)
        server.sendmail(msg["From"], recipients, msg.as_string())
        server.quit()
        log.info("Notification email sent: %s", subject)
    except Exception as e:
        log.error("Failed to send notification email: %s", e)


def send_test_mail():
    """Send a test email to verify configuration."""
    cfg = _get_config()
    # Temporarily force enabled for test
    cfg["enabled"] = "true"
    send_mail("[Charger] Test Notification", "This is a test email from the OCPP Central System.", config=cfg)


def notify(event_key: str, subject: str, body: str):
    """Send notification if the given event type is enabled."""
    cfg = _get_config()
    if cfg.get("enabled") != "true":
        return
    if cfg.get(event_key) != "true":
        return
    send_mail(subject, body, config=cfg)
