# steve-mqtt-bridge

Export EV-charger energy & telemetry from the [SteVe](https://github.com/steve-community/steve)
OCPP CSMS (MariaDB) into [Home Assistant](https://www.home-assistant.io/) over MQTT
with automatic entity discovery.

**Read-only by design.** The bridge only ever `SELECT`s from the SteVe database and
only publishes to MQTT. It never writes to SteVe and never touches the OCPP
connection between charger and CSMS — zero risk to live charging.

## How it works

```
Charger ──OCPP──► SteVe CSMS ──(MariaDB)──► steve-mqtt-bridge ──MQTT──► Home Assistant
```

- Polls the SteVe database every `POLL_INTERVAL_SEC` (default 60s).
- **Dynamic discovery** — the charger list is re-read from the DB every poll, so a
  newly accepted charger needs no bridge config change.
- Publishes HA MQTT-discovery config (retained) so entities are created
  automatically.
- Publishes live power / energy / voltage / current / temperature / status, plus
  the active RFID tag and its friendly user name.
- Reports availability from the newest of the meter-value and status timestamps
  (a charger is marked "offline" only when *both* are older than the threshold).

## Entities per charger

| Entity | Source | Notes |
|---|---|---|
| `sensor.steve_<id>_energy` | `Energy.Active.Import.Register` (Wh→kWh) | `total_increasing` → feeds the HA Energy dashboard |
| `sensor.steve_<id>_power` | `Power.Active.Import` (W) | |
| `sensor.steve_<id>_voltage_l1..l3` | `Voltage` (V) | per phase (dynamic — 1φ chargers get only `_l1`) |
| `sensor.steve_<id>_current_l1..l3` | `Current.Import` (A) | per phase (dynamic) |
| `sensor.steve_<id>_temperature` | `Temperature` (°C) | |
| `sensor.steve_<id>_status` | `connector_status.status` | `Available` / `Charging` / … |
| `sensor.steve_<id>_error_code` | `connector_status.error_code` | |
| `sensor.steve_<id>_session_energy` | transaction start/stop (Wh→kWh) | energy of the current (or last) session |
| `sensor.steve_<id>_active_tag` | `transaction_start.id_tag` | RFID tag currently charging |
| `sensor.steve_<id>_active_user` | `ocpp_tag.note` | friendly name of that tag |

`<id>` is the raw `charge_box_id`, lower-cased for the entity_id (e.g.
`ASAC24071901` → `steve_asac24071901_energy`).

## Prerequisites

1. A running [SteVe](https://github.com/steve-community/steve) CSMS with MariaDB.
2. A dedicated **read-only** DB user — never use root.
3. An MQTT broker (e.g. the Home Assistant Mosquitto addon) with a user for the
   bridge. The broker must **require auth** (anonymous is not supported by the
   bridge's recommended setup).
4. Home Assistant with the MQTT integration and **discovery enabled**
   (default `discovery_prefix: homeassistant`).

### Create the read-only DB user

```sql
CREATE USER 'steve_readonly'@'%' IDENTIFIED BY 'a-strong-password';
GRANT SELECT ON stevedb.* TO 'steve_readonly'@'%';
FLUSH PRIVILEGES;
```

## Quick start (Docker)

```bash
cp .env.example .env
# edit .env with your DB + MQTT values
docker compose up -d --build
```

## Configuration

All via environment variables (see `.env.example` for the full list).

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` / `DB_PORT` | `localhost` / `3306` | SteVe MariaDB |
| `DB_USER` / `DB_PASSWORD` | `steve_readonly` / — | read-only DB credentials |
| `DB_NAME` | `stevedb` | database name |
| `MQTT_HOST` / `MQTT_PORT` | `localhost` / `1883` | MQTT broker |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | — | MQTT credentials |
| `MQTT_CLIENT_ID` | `steve-mqtt-bridge` | MQTT client id |
| `MQTT_BASE` | `steve` | base topic prefix |
| `DISCOVERY_PREFIX` | `homeassistant` | HA discovery prefix |
| `POLL_INTERVAL_SEC` | `60` | DB poll interval (seconds) |
| `STATUS_STALE_SEC` | `900` | offline threshold (seconds) |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |

### MQTT topics

- State: `steve/<charge_box_id>/<metric>` (e.g. `steve/ATAC24062801/energy`)
- Availability: `steve/<charge_box_id>/availability` (`online` / `offline`), shared
  by all entities of that charger
- Discovery: `homeassistant/sensor/steve_<id>_<metric>/config` (retained)

## Availability model

A charger's "last seen" is the **newest** of its meter-value timestamp and its
connector-status timestamp. This matters because neither signal alone is
reliable:

- While charging, the meter value updates every ~1 minute, but `connector_status`
  stays silent (status doesn't change).
- While idle, meter values stop, but `connector_status` still arrives ~every
  10 minutes.

The bridge marks a charger offline only when **both** are older than
`STATUS_STALE_SEC` (default 900s = 15 min). Set the threshold ≥ the charger's
slowest signal cadence (≥10 min in practice); a 5-min threshold will false-positive.

## Naming note (why `object_id` is set)

Home Assistant derives an MQTT entity's `entity_id` from its `name` combined with
the `device.name` — **unless** an `object_id` field is provided in the discovery
payload. `unique_id` alone does *not* override that naming. Without `object_id`,
an entity whose name already contains the device name gets a doubled entity_id
(e.g. `sensor.steve_asac24071901_steve_asac24071901_energy`). This bridge sets
`object_id` explicitly to keep entity_ids clean.

## Timezone note

The SteVe app + DB containers may run in UTC while the host is a different
timezone. The bridge never does time math against the host clock — all
"age" comparisons run inside the DB (`TIMESTAMPDIFF(SECOND, ts, NOW())`), so the
result is always correct regardless of host timezone.

## Security

- `.env` is gitignored — never commit real DB/MQTT credentials.
- Use a dedicated read-only DB user for the bridge.
- Use a dedicated MQTT user (not the HA owner account) for the bridge.
- The bridge runs as a non-root user inside its container.

## License

MIT
