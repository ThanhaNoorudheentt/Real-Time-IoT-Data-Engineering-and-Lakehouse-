"""MQTT -> Kafka bridge.

Subscribes to sensors/# and forwards each message to Kafka topic iot.raw, keyed by
device_id (taken from the MQTT topic) so per-device ordering is preserved within a partition.
Payloads that are not valid JSON objects go to iot.dlq with a reason header.
Schema/range validation is deliberately NOT done here: bad-but-parseable data must reach
bronze untouched, and silver decides what is invalid.
"""
import json
import logging
import os
import signal
import sys
import time

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "iot.raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "iot.dlq")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bridge")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 20,
    "compression.type": "lz4",
})

stats = {"forwarded": 0, "dlq": 0, "errors": 0}


def on_delivery(err, msg):
    if err is not None:
        stats["errors"] += 1
        log.error("delivery failed: %s", err)


def device_key(topic: str) -> bytes:
    # sensors/{home}/{device_id}/{sensor_type}
    parts = topic.split("/")
    return (parts[2] if len(parts) >= 4 else topic).encode()


def on_connect(client, userdata, flags, reason_code, properties=None):
    log.info("connected to MQTT (%s); subscribing", reason_code)
    client.subscribe("sensors/#", qos=1)


def parse_error(payload: bytes):
    """Return None if payload is a JSON object, else a short reason string."""
    try:
        obj = json.loads(payload)
    except Exception as exc:
        return f"invalid JSON: {exc}"
    if not isinstance(obj, dict):
        return "payload is not a JSON object"
    return None


def produce(topic, key, value, headers):
    # A full local queue is back-pressure, not a bad message: wait for space and retry.
    while True:
        try:
            producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.5)


def on_message(client, userdata, msg):
    key = device_key(msg.topic)
    headers = [("mqtt_topic", msg.topic.encode())]
    reason = parse_error(msg.payload)
    if reason is None:
        produce(RAW_TOPIC, key, msg.payload, headers)
        stats["forwarded"] += 1
    else:
        headers.append(("reason", reason.encode()))
        produce(DLQ_TOPIC, key, msg.payload, headers)
        stats["dlq"] += 1
    producer.poll(0)


def main():
    # Persistent session (clean_session=False) + QoS 1 subscription: the broker keeps messages
    # published while the bridge is down and delivers them when it reconnects.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bridge",
                         protocol=mqtt.MQTTv311, clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message

    def shutdown(*_):
        log.info("shutting down, flushing producer; stats=%s", stats)
        client.disconnect()
        producer.flush(10)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as exc:
            log.warning("MQTT not ready (%s); retrying", exc)
            time.sleep(2)

    client.loop_start()
    while True:
        time.sleep(10)
        log.info("stats=%s", stats)


if __name__ == "__main__":
    main()
