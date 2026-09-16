#!/usr/bin/env python3
"""
steve-mqtt-bridge
=================

Export EV-charger energy/telemetry from the SteVe OCPP CSMS (MariaDB) into
Home Assistant over MQTT (with HA auto-discovery).

Read-only by design: this bridge only ever SELECTs from the SteVe database and
only PUBLISHES to MQTT. It never writes to SteVe and never touches the OCPP
connection between charger and CSMS — zero risk to live charging.

Design decisions (validated against a live SteVe 3.14.1 + HA 2025.10):
- Dynamic discovery: the charger list is re-read from the DB on every poll, so a
  newly accepted charger needs NO bridge config change.
- Availability = GREATEST(meter.value_timestamp, status.status_timestamp).
  Neither heartbeat nor connector_status alone is reliable (status is silent
  during a continuous charge; heartbeat is infrequent), so the newest of BOTH
  is the "last seen" signal. Mark offline only when both are older than the
  threshold.
- All time math is done INSIDE the DB (UTC vs UTC) — never against the host
  clock, which may be a different timezone than the containers.
- Per-phase voltage/current are emitted dynamically (1-phase chargers produce
  only L1; 3-phase produce L1/L2/L3).
- Energy is converted Wh -> kWh and tagged total_increasing so it feeds the HA
  Energy dashboard directly.
"""

import json
import logging
import os
import re
import signal
import sys
import time

import paho.mqtt.client as mqtt
import pymysql

# ---------------------------------------------------------------------------
# Configuration (all via environment variables; no secrets in code)
# ---------------------------------------------------------------------------

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "steve_readonly")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "stevedb")

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")
MQTT_BASE = os.environ.get("MQTT_BASE", "steve")            # base topic prefix
DISCOVERY_PREFIX = os.environ.get("DISCOVERY_PREFIX", "homeassistant")

POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
STATUS_STALE_SEC = int(os.environ.get("STATUS_STALE_SEC", "900"))  # offline threshold

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("steve-mqtt-bridge")

_shutdown = False


def _on_signal(signum, frame):
    global _shutdown
    log.info("Signal %s received, shutting down", signum)
    _shutdown = True


# ---------------------------------------------------------------------------
# DB helpers (read-only)
# ---------------------------------------------------------------------------

def db_connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
        read_timeout=15,
    )


def fetch_all(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def fetch_one(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

def mqtt_connect():
    client = mqtt.Client(
        client_id=os.environ.get("MQTT_CLIENT_ID", "steve-mqtt-bridge"),
        protocol=mqtt.MQTTv311,
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Last-will so HA marks everything unavailable if the bridge dies silently.
    client.will_set(
        f"{MQTT_BASE}/status", "offline", qos=1, retain=True
    )

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def pub(client, topic, payload, retain=False, qos=1):
    """Publish a message; payload may be a str or a dict (JSON-encoded)."""
    if isinstance(payload, (dict, list)):
        payload = json.dumps(payload)
    client.publish(topic, payload, qos=qos, retain=retain)


def _slug(value):
    """Lowercase + collapse non-alphanumerics to _ (matches HA entity_id rules)."""
    return re.sub(r"[^a-z0-9_]+", "_", str(value).lower()).strip("_")


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------

def publish_discovery(client, charge_box_id, metric, discovery_payload):
    """Publish an HA MQTT-discovery config message (retained).

    HA derives entity_id from the entity `name` (+ device name) UNLESS the
    `object_id` field is set explicitly — `unique_id` alone does NOT override
    that naming. So we set `object_id` to the clean slug to force a clean
    entity_id, and keep `unique_id` as the stable identity anchor.
    """
    slug = _slug(charge_box_id)
    oid = f"steve_{slug}_{metric}"
    topic = f"{DISCOVERY_PREFIX}/sensor/{oid}/config"
    payload = dict(discovery_payload)
    payload["object_id"] = oid            # forces clean entity_id
    payload.setdefault("unique_id", oid)  # stable anchor for renames
    pub(client, topic, payload, retain=True)


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def get_chargers(conn):
    """Dynamic discovery: live list of accepted chargers from the DB."""
    sql = """
        SELECT cb.charge_box_id, cb.charge_point_vendor, cb.charge_point_model,
               ev.evse_pk
        FROM charge_box cb
        JOIN evse ev ON ev.charge_box_id = cb.charge_box_id
        WHERE cb.registration_status = 'Accepted'
          AND cb.ocpp_protocol IS NOT NULL
          AND ev.evse_id = 1
        ORDER BY cb.charge_box_id
    """
    return fetch_all(conn, sql)


def get_latest_meter(conn, evse_pk):
    """Latest meter sample (newest value_timestamp) for one EVSE, fanning out
    into per-measurand rows with phase preserved."""
    sql = """
        SELECT measurand, unit, phase, value, value_timestamp
        FROM connector_meter_value
        WHERE evse_pk = %s
          AND value_timestamp = (
              SELECT MAX(value_timestamp)
              FROM connector_meter_value
              WHERE evse_pk = %s
          )
    """
    return fetch_all(conn, sql, (evse_pk, evse_pk))


def get_latest_status(conn, evse_pk):
    sql = """
        SELECT status, error_code, status_timestamp
        FROM connector_status
        WHERE evse_pk = %s
        ORDER BY status_timestamp DESC
        LIMIT 1
    """
    return fetch_one(conn, sql, (evse_pk,))


def get_active_transaction(conn, evse_pk):
    """The currently-open transaction (RFID tag + user), if any."""
    sql = """
        SELECT ts.transaction_pk, ts.id_tag, ot.note AS user_name,
               ts.start_timestamp, ts.start_value
        FROM transaction_start ts
        LEFT JOIN transaction_stop tp ON tp.transaction_pk = ts.transaction_pk
        LEFT JOIN ocpp_tag ot ON ot.id_tag = ts.id_tag
        WHERE ts.evse_pk = %s
          AND tp.transaction_pk IS NULL
        ORDER BY ts.transaction_pk DESC
        LIMIT 1
    """
    return fetch_one(conn, sql, (evse_pk,))


def get_last_seen(conn, evse_pk):
    """Newest timestamp across meter values AND status (UTC, from the DB)."""
    sql = """
        SELECT GREATEST(
                   COALESCE((SELECT MAX(value_timestamp)
                             FROM connector_meter_value
                             WHERE evse_pk = %s), '1970-01-01'),
                   COALESCE((SELECT MAX(status_timestamp)
                             FROM connector_status
                             WHERE evse_pk = %s), '1970-01-01')
               ) AS last_seen,
               TIMESTAMPDIFF(SECOND,
                   GREATEST(
                       COALESCE((SELECT MAX(value_timestamp)
                                 FROM connector_meter_value
                                 WHERE evse_pk = %s), '1970-01-01'),
                       COALESCE((SELECT MAX(status_timestamp)
                                 FROM connector_status
                                 WHERE evse_pk = %s), '1970-01-01')
                   ),
                   NOW()
               ) AS age_s
    """
    return fetch_one(conn, sql, (evse_pk, evse_pk, evse_pk, evse_pk))


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def collect(conn, charger):
    """Gather all state for one charger and return a list of
    (topic_suffix, value, kind) where kind is one of:
      'state'    -> plain numeric/string state
      'attr'     -> ignored (reserved)
    """
    evse_pk = charger["evse_pk"]
    cbid = charger["charge_box_id"]
    out = {"charge_box_id": cbid, "states": {}, "online": False}

    # --- meter values ---
    rows = get_latest_meter(conn, evse_pk)
    for r in rows:
        measurand = r["measurand"]
        phase = r["phase"] or ""  # None -> "" for 1-phase chargers
        val = _as_float(r["value"])
        if measurand == "Energy.Active.Import.Register":
            if val is not None:
                out["states"]["energy"] = round(val / 1000.0, 3)  # Wh -> kWh
        elif measurand == "Power.Active.Import":
            if val is not None:
                out["states"]["power"] = round(val, 1)
        elif measurand == "Temperature":
            if val is not None:
                out["states"]["temperature"] = round(val, 1)
        elif measurand == "Voltage":
            if val is not None:
                out["states"][f"voltage_{_slug(phase) or 'total'}"] = round(val, 1)
        elif measurand == "Current.Import":
            if val is not None:
                out["states"][f"current_{_slug(phase) or 'total'}"] = round(val, 2)

    # --- connector status ---
    st = get_latest_status(conn, evse_pk)
    if st and st.get("status"):
        out["states"]["status"] = st["status"]
        if st.get("error_code"):
            out["states"]["error_code"] = st["error_code"]

    # --- active transaction (RFID tag / user) ---
    tx = get_active_transaction(conn, evse_pk)
    if tx:
        out["states"]["active_tag"] = tx["id_tag"]
        out["states"]["active_user"] = tx.get("user_name") or ""
        if tx.get("start_value") is not None and "energy" in out["states"]:
            # session energy = current register - session start register
            start_wh = _as_float(tx["start_value"])
            cur_wh = out["states"]["energy"] * 1000.0
            if start_wh is not None:
                out["states"]["session_energy"] = round((cur_wh - start_wh) / 1000.0, 3)
    else:
        out["states"]["active_tag"] = ""
        out["states"]["active_user"] = ""

    # --- availability ---
    ls = get_last_seen(conn, evse_pk)
    out["online"] = ls is not None and ls["age_s"] is not None and ls["age_s"] < STATUS_STALE_SEC

    return out


# ---------------------------------------------------------------------------
# Publish helpers
# ---------------------------------------------------------------------------

# Metric -> HA discovery config (device_class, unit, state_class)
METRIC_DEFS = {
    "energy":          {"device_class": "energy",      "unit": "kWh",     "state_class": "total_increasing", "name": "Energy"},
    "power":           {"device_class": "power",       "unit": "W",       "state_class": "measurement",     "name": "Power"},
    "temperature":     {"device_class": "temperature", "unit": "°C",      "state_class": "measurement",     "name": "Temperature"},
    "status":          {"device_class": None,          "unit": None,      "state_class": None,              "name": "Status"},
    "error_code":      {"device_class": None,          "unit": None,      "state_class": None,              "name": "Error Code"},
    "active_tag":      {"device_class": None,          "unit": None,      "state_class": None,              "name": "Active Tag"},
    "active_user":     {"device_class": None,          "unit": None,      "state_class": None,              "name": "Active User"},
    "session_energy":  {"device_class": "energy",      "unit": "kWh",     "state_class": "total",            "name": "Session Energy"},
}


def publish_charger(client, charger, data):
    cbid = charger["charge_box_id"]
    slug = _slug(cbid)
    online = data["online"]

    # availability topic (shared by all entities of this charger)
    avail_topic = f"{MQTT_BASE}/{cbid}/availability"
    pub(client, avail_topic, "online" if online else "offline", retain=True)

    for metric, value in data["states"].items():
        topic = f"{MQTT_BASE}/{cbid}/{metric}"

        # discovery config (retained) so HA creates the entity
        defn = METRIC_DEFS.get(metric)
        if defn is None:
            # dynamic per-phase voltage/current — infer from metric name
            if metric.startswith("voltage_"):
                defn = {"device_class": "voltage", "unit": "V", "state_class": "measurement", "name": f"Voltage {metric.split('_',1)[1].upper()}"}
            elif metric.startswith("current_"):
                defn = {"device_class": "current", "unit": "A", "state_class": "measurement", "name": f"Current {metric.split('_',1)[1].upper()}"}
            else:
                defn = {"device_class": None, "unit": None, "state_class": None, "name": metric}

        payload = {
            "name": defn["name"],
            "state_topic": topic,
            "availability_topic": avail_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
            "device": {
                "identifiers": [f"steve_{slug}"],
                "name": f"SteVe {cbid}",
                "manufacturer": charger.get("charge_point_vendor") or "SteVe",
                "model": charger.get("charge_point_model") or "OCPP",
            },
        }
        if defn.get("unit"):
            payload["unit_of_measurement"] = defn["unit"]
        if defn.get("device_class"):
            payload["device_class"] = defn["device_class"]
        if defn.get("state_class"):
            payload["state_class"] = defn["state_class"]

        publish_discovery(client, cbid, metric, payload)

        # state value (retained so HA shows last-known on restart)
        pub(client, topic, value, retain=True)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_once(client, conn):
    chargers = get_chargers(conn)
    log.info("Discovered %d charger(s)", len(chargers))
    for ch in chargers:
        try:
            data = collect(conn, ch)
            publish_charger(client, ch, data)
            log.info(
                "%s online=%s states=%s",
                ch["charge_box_id"], data["online"], list(data["states"]),
            )
        except Exception as exc:  # noqa: BLE001 - keep one charger failing isolated
            log.exception("Failed processing charger %s: %s", ch["charge_box_id"], exc)


def main():
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("Connecting to DB %s:%s/%s as %s", DB_HOST, DB_PORT, DB_NAME, DB_USER)
    conn = db_connect()

    log.info("Connecting to MQTT %s:%s", MQTT_HOST, MQTT_PORT)
    client = mqtt_connect()
    pub(client, f"{MQTT_BASE}/status", "online", retain=True)

    while not _shutdown:
        try:
            conn.ping(reconnect=True)
        except Exception:
            log.exception("DB connection lost, reconnecting")
            conn = db_connect()

        try:
            run_once(client, conn)
        except Exception:
            log.exception("Poll cycle failed")

        # sleep in small increments so shutdown is responsive
        for _ in range(POLL_INTERVAL_SEC):
            if _shutdown:
                break
            time.sleep(1)

    log.info("Shutting down; marking bridge offline")
    pub(client, f"{MQTT_BASE}/status", "offline", retain=True)
    client.loop_stop()
    client.disconnect()
    conn.close()


if __name__ == "__main__":
    main()
