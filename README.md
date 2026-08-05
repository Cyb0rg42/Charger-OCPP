# Charging-OCPP

A small suite of cooperating services built around OCPP 1.6-J electric-vehicle
charging infrastructure: a Central System (CSMS), a charge-point simulator,
and a transparent OCPP proxy that sits between them.

**CSMS is production-capable.** It's a real OCPP 1.6 Central System you can
self-host to run your own charge points, instead of handing your charging
sessions to a commercial provider — useful if data and infrastructure
sovereignty over your own charging setup matters to you. It has been tested
against the [SmartEVSE](https://www.smartevse.nl/) charge point module as a
real-world charge point, alongside the bundled simulator.

**ChargePoint and OCPPProxy are test/development tooling.** ChargePoint is a
software-only charge point simulator for exercising a CSMS without physical
hardware, and OCPPProxy is a man-in-the-middle proxy for inspecting and
manipulating OCPP traffic during development and troubleshooting — neither is
intended for production charging.

## Components

| Component | Directory | Role | Default ports |
|---|---|---|---|
| **CSMS** | [`CSMS/`](CSMS/) | OCPP 1.6 Central System — production-capable. Accepts charge point connections (tested with SmartEVSE and with the bundled simulator), tracks transactions, remote commands, RFID authorization, Zonneplan tariff-based smart charging | `9000` (OCPP), `9001` (web) |
| **ChargePoint** | [`ChargePoint/`](ChargePoint/) | Virtual charge point / station simulator (test/dev only) — connects out to a CSMS (directly or via the proxy), simulates cable/car connect and charging sessions | `5000` (web) |
| **OCPPProxy** | [`OCPPProxy/`](OCPPProxy/) | Transparent OCPP man-in-the-middle proxy (test/dev only) — a single instance serves *many* charge points at once, each independently fanned out to *one or more* configured CSMS backends; message logging, command blocking, boot/connector overrides | `4000` (web), `9100` (OCPP) |

Each component has its own detailed manual: see `usermanual.mediawiki` in
each directory for configuration options, the REST API reference, and
troubleshooting.

### Architecture

```
ChargePoint 1 ──┐                                  ┌──► CSMS 1
ChargePoint 2 ──┼──►   OCPPProxy   ──────────────►  ├──► CSMS 2
ChargePoint N ──┘  (inspects, logs,                 └──► CSMS N
  (simulators)      can block traffic)                (central systems)

Each component also serves its own web dashboard over HTTP, and every
component can run as multiple independent instances (different name,
ports, and database) from the same Docker image.

CSMS ──► Zonneplan API   (quarter-hourly electricity tariffs, for
                           cheapest-charging-window planning)
CSMS ──► SMTP server     (email notifications)
```

A charge point can talk directly to a CSMS, or through the proxy — the
proxy is optional but is what enables message inspection and selective
command blocking. A single OCPPProxy instance is a genuine many-to-many
router: it accepts connections from any number of charge points at once,
and independently fans each one out to every backend in its own configured
CSMS list — e.g. mirroring live traffic to a second CSMS for testing, or
routing different charge points to different CSMS instances, all through
one proxy.

## Requirements

- [Docker](https://docs.docker.com/get-docker/) and the Docker Compose
  plugin (`docker compose version`) — this is the supported way to run
  the stack.
- For local development without Docker: Python 3.12+ and the
  per-component `requirements.txt`.

## Quick start (Docker Compose)

```bash
git clone <this repo>
cd Charger
docker compose build
docker compose up -d
```

This starts five containers: one `ocppproxy`, two `CSMS` instances
(`csms1`, `csms2`), and two `ChargePoint` instances (`chargepoint1`,
`chargepoint2`) — demonstrating that CSMS and ChargePoint both support
running multiple independent instances from the same image.

That full five-container stack is the demo/development setup. For a
production deployment you'd typically run just a `csms` service (and skip
`chargepoint`/`ocppproxy` entirely), pointing your real charge point
hardware — e.g. a [SmartEVSE](https://www.smartevse.nl/) module — at the
CSMS's OCPP port instead of the simulator.

| Service | URL | Notes |
|---|---|---|
| OCPP Proxy dashboard | http://localhost:4000 | |
| CSMS 1 dashboard | http://localhost:9001 | OCPP listener on `:9000` |
| CSMS 2 dashboard | http://localhost:9011 | OCPP listener on `:9010` |
| ChargePoint 1 dashboard | http://localhost:5001 | |
| ChargePoint 2 dashboard | http://localhost:5002 | |

### First-time wiring

Fresh containers don't know about each other yet — a couple of one-time
steps in the web UIs connect the pieces (Docker containers reach each
other by service name on the compose network, not `localhost`):

1. **OCPPProxy → CSMS**: open the proxy dashboard (`:4000`) →
   backend settings, and confirm/add a backend URL of
   `ws://csms1:9000/ocpp` (the compose file seeds this automatically on a
   fresh `ocppproxy` data volume via `PROXY_DEFAULT_BACKEND_URL`).
2. **ChargePoint → OCPPProxy (or CSMS)**: open a ChargePoint dashboard
   (`:5001`/`:5002`), go to its OCPP/config settings, and set the central
   system URL to `ws://ocppproxy:9100/ocpp` (through the proxy) or
   directly to `ws://csms1:9000/ocpp` (bypassing it). Use the *service
   name*, never `127.0.0.1`/`localhost` — inside a container that means
   the container itself.

### Running more instances

CSMS and ChargePoint are both designed to run as multiple instances from
the same image — each instance is just the image plus its own config file
and port mapping:

- CSMS instances are configured with a small YAML file
  (`server_name`, `database_path`, `web_port`, `ocpp_port`) — see
  [`CSMS/config_instance1.yaml`](CSMS/config_instance1.yaml) /
  [`config_instance2.yaml`](CSMS/config_instance2.yaml).
- ChargePoint instances are configured with a small JSON file
  (`name`, `database`, `port`, `logs`) — see
  [`ChargePoint/config_instance1.json`](ChargePoint/config_instance1.json) /
  [`config_instance2.json`](ChargePoint/config_instance2.json).

To add a third instance of either, copy its service block in
[`docker-compose.yml`](docker-compose.yml), give it a new name/ports, and
point it at a new `config_instanceN.*` file with a unique port and
database path.

### Data and logs

Each container persists its SQLite database and log files to named Docker
volumes (`<service>_data`, `<service>_logs`), so data survives container
rebuilds/restarts. Remove a volume to reset that instance to a clean
state.

## Configuration reference

| App | Format | Key fields |
|---|---|---|
| CSMS | YAML | `server_name`, `database_path`, `web_port`, `ocpp_port` |
| ChargePoint | JSON | `name`, `database`, `port`, `logs.{ocpp_log,access_log,error_log}` |
| OCPPProxy | YAML/JSON | `name`, `port` (web dashboard); `PROXY_LISTEN_PORT` env var sets the OCPP-facing port (default `9100`) |

The container entrypoint for each app takes the config file via
`--config`, mounted read-only at `/config/config.yaml` (or `.json`) — see
each Dockerfile and `docker-compose.yml` for the exact mount.

## Running without Docker (development)

Each app can also run directly with Python 3.12+:

```bash
cd CSMS && pip install -r requirements.txt && python csms.py --config config_instance1.yaml
cd ChargePoint && pip install -r requirements.txt && python chargepoint.py --config config_instance1.json
cd OCPPProxy && pip install -r requirements.txt && python ocppproxy.py --config config_example.yaml
```

Without a container's isolated filesystem, make sure `/opt/charger/data`
and `/var/log/charger` (or whatever paths your config points at) exist
and are writable, or point the config at local paths instead.

## Further reading

Each component has an in-depth manual covering its full configuration
reference, REST API, and troubleshooting:

- [`CSMS/usermanual.mediawiki`](CSMS/usermanual.mediawiki)
- [`ChargePoint/usermanual.mediawiki`](ChargePoint/usermanual.mediawiki)
- [`OCPPProxy/usermanual.mediawiki`](OCPPProxy/usermanual.mediawiki)

## License

GNU General Public License v3.0 or later (GPL-3.0-or-later). See
[`COPYING`](COPYING) for the full license text.

Copyright (C) 2026 Karl Lovink
