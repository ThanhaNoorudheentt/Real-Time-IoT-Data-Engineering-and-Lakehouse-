"""Smart home telemetry simulator with fault injection and a ground-truth log.

Every publish attempt is recorded in a JSONL ground-truth file so that, later, lakehouse contents
can be compared against what was really sent (duplicates, late events, malformed rows, dropouts).

Faults (all seeded, so a run is exactly reproducible):
  duplicate   re-publish the same payload (same event_id) shortly afterwards
  late        publish after a long delay (default 10 s - 10 min); event_ts stays the original time
  ooo         publish after a short delay (1-5 s), so it arrives behind newer events
  malformed   missing field / wrong type / out-of-range / unknown sensor / truncated JSON
  dropout     a device goes silent for 1-5 minutes

Examples
  python simulator.py --sink stdout --homes 2 --duration 120 --no-sleep --seed 7
  python simulator.py --sink mqtt --mqtt-host localhost --homes 10 --realtime
"""
import argparse
import collections
import heapq
import json
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def local_env_value(key):
    """Read a simple KEY=value from .env when the CLI is run outside Compose."""
    if key in os.environ:
        return os.environ[key]
    if not ENV_FILE.exists():
        return None
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, _, value = stripped.partition("=")
        if name.strip() == key:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            return value
    return None


# room -> list of (device prefix, sensor_type, unit)
HOME_TEMPLATE = {
    "living_room": [("tmp", "temperature", "C"), ("hum", "humidity", "%"), ("co2", "co2", "ppm"),
                    ("mot", "motion", "bool"), ("pwr", "power", "W")],
    "kitchen":     [("tmp", "temperature", "C"), ("pwr", "power", "W"), ("mot", "motion", "bool")],
    "bedroom":     [("tmp", "temperature", "C"), ("hum", "humidity", "%"), ("co2", "co2", "ppm")],
    "bathroom":    [("hum", "humidity", "%"), ("mot", "motion", "bool")],
    "hallway":     [("door", "door", "bool"), ("mot", "motion", "bool")],
}
VALID_RANGES = {"temperature": (-10, 50), "humidity": (0, 100), "co2": (300, 5000),
                "power": (0, 10000), "motion": (0, 1), "door": (0, 1)}
REQUIRED = ["event_id", "home_id", "room", "device_id", "sensor_type", "value", "unit",
            "event_ts", "seq", "fw_version", "produced_at"]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


class Device:
    def __init__(self, rng, home_idx, room, prefix, sensor_type, unit, num):
        self.home_id = f"home-{home_idx:03d}"
        self.room = room
        self.device_id = f"{prefix}-{num:04d}"
        self.sensor_type = sensor_type
        self.unit = unit
        self.fw = rng.choice(["1.4.2", "1.4.2", "1.5.0"])
        self.seq = 0
        self.phase = rng.uniform(0, 2 * math.pi)
        self.state = rng.choice([0, 1]) if sensor_type in ("door", "motion") else 0
        self.next_emit = 0.0
        self.silent_until = 0.0
        self.topic = f"sensors/{self.home_id}/{self.device_id}/{self.sensor_type}"

    def reading(self, rng, t):
        day = math.sin(2 * math.pi * (t % 86400) / 86400 + self.phase)
        s = self.sensor_type
        if s == "temperature":
            return round(21 + 2.5 * day + rng.gauss(0, 0.2), 2)
        if s == "humidity":
            return round(min(100, max(0, 48 + 8 * day + rng.gauss(0, 1.0))), 1)
        if s == "co2":
            return round(min(5000, max(300, 650 + 250 * day + rng.gauss(0, 40))), 0)
        if s == "power":
            base = 120 + 80 * (day + 1)
            spike = rng.random() < 0.03
            return round(base + (rng.uniform(800, 2200) if spike else rng.gauss(0, 10)), 1)
        # motion / door: sticky binary state
        if rng.random() < 0.1:
            self.state = 1 - self.state
        return self.state


def build_fleet(rng, homes):
    fleet, num = [], 1
    for h in range(1, homes + 1):
        for room, specs in HOME_TEMPLATE.items():
            for prefix, st, unit in specs:
                fleet.append(Device(rng, h, room, prefix, st, unit, num))
                num += 1
    return fleet


def corrupt(rng, event):
    """Return (payload_str, fault_name, expected_destination)."""
    kind = rng.choice(["missing_field", "wrong_type", "out_of_range", "unknown_sensor", "truncated"])
    e = dict(event)
    if kind == "missing_field":
        del e[rng.choice([k for k in REQUIRED if k != "event_id"])]
        return json.dumps(e), "missing_field", "quarantine"
    if kind == "wrong_type":
        e["value"] = "n/a"
        return json.dumps(e), "wrong_type", "quarantine"
    if kind == "out_of_range":
        e["value"] = -999
        return json.dumps(e), "out_of_range", "quarantine"
    if kind == "unknown_sensor":
        e["sensor_type"] = "banana"
        return json.dumps(e), "unknown_sensor", "quarantine"
    full = json.dumps(event)
    return full[: len(full) // 2], "truncated_json", "dlq"


def make_sink(args):
    if args.sink == "mqtt":
        import paho.mqtt.client as mqtt
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="simulator")
        if args.mqtt_user:
            c.username_pw_set(args.mqtt_user, args.mqtt_password)
        c.max_inflight_messages_set(200)
        c.connect(args.mqtt_host, args.mqtt_port, keepalive=30)
        c.loop_start()
        recent = collections.deque(maxlen=5000)  # handles of the newest publishes

        def send(topic, payload):
            recent.append(c.publish(topic, payload, qos=1))

        def close():
            # publish() only queues the message. Wait until the broker has acknowledged the
            # newest ones, otherwise the last messages of a run are silently lost.
            for info in recent:
                try:
                    info.wait_for_publish(timeout=30)
                except (RuntimeError, ValueError):
                    pass
            c.disconnect()
            c.loop_stop()

        return send, close
    if args.sink == "file":
        f = open(args.out_file, "w")
        return (lambda topic, payload: f.write(f"{topic}\t{payload}\n")), f.close
    return (lambda topic, payload: print(f"{topic}\t{payload}")), (lambda: None)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--homes", type=int, default=2, help="homes (each has 17 devices)")
    p.add_argument("--interval", type=float, default=5.0, help="seconds between readings per device")
    p.add_argument("--duration", type=float, default=300, help="simulated seconds to run (0 = forever)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-ts", default=None, help="ISO start time (default: now). Fix it for reproducible runs")
    p.add_argument("--realtime", action="store_true", help="follow the wall clock (sleep between ticks)")
    p.add_argument("--no-sleep", action="store_true", help="run on virtual time as fast as possible")
    p.add_argument("--dup-rate", type=float, default=0.02)
    p.add_argument("--late-rate", type=float, default=0.05)
    p.add_argument("--late-min", type=float, default=10)
    p.add_argument("--late-max", type=float, default=600)
    p.add_argument("--ooo-rate", type=float, default=0.03)
    p.add_argument("--malformed-rate", type=float, default=0.02)
    p.add_argument("--dropout-prob", type=float, default=0.002, help="per device per minute")
    p.add_argument("--sink", choices=["mqtt", "stdout", "file"], default="mqtt")
    p.add_argument("--out-file", default="data/simulated_messages.tsv")
    p.add_argument("--mqtt-host", default="localhost")
    p.add_argument("--mqtt-port", type=int, default=1883)
    p.add_argument("--mqtt-user", default=local_env_value("MQTT_SIMULATOR_USER"))
    p.add_argument("--mqtt-password", default=local_env_value("MQTT_SIMULATOR_PASSWORD"))
    p.add_argument("--truth-file", default="data/ground_truth.jsonl")
    args = p.parse_args(argv)
    if args.realtime and args.no_sleep:
        p.error("--realtime and --no-sleep are mutually exclusive")

    rng = random.Random(args.seed)
    fleet = build_fleet(rng, args.homes)
    for d in fleet:
        d.next_emit = rng.uniform(0, args.interval)

    if args.start_ts:
        t0 = datetime.fromisoformat(args.start_ts.replace("Z", "+00:00")).timestamp()
    else:
        t0 = time.time()
    tick = 0.5
    send, close_sink = make_sink(args)
    truth = open(args.truth_file, "w")
    pending = []  # heap of (release_t, n, topic, payload, truth_record)
    counter = 0
    counts = {"generated": 0, "published": 0, "duplicate": 0, "late": 0, "ooo": 0,
              "malformed": 0, "dropouts": 0}

    def publish(now, topic, payload, rec):
        send(topic, payload)
        rec["publish_ts"] = iso(datetime.fromtimestamp(now, timezone.utc))
        truth.write(json.dumps(rec) + "\n")
        counts["published"] += 1

    def schedule(release, topic, payload, rec):
        nonlocal counter
        counter += 1
        heapq.heappush(pending, (release, counter, topic, payload, rec))

    elapsed = 0.0
    wall0 = time.time()
    try:
        while args.duration == 0 or elapsed < args.duration:
            now = t0 + elapsed
            if args.realtime:
                now = time.time()
            # release held (late / ooo / duplicate) messages
            while pending and pending[0][0] <= now:
                _, _, topic, payload, rec = heapq.heappop(pending)
                publish(now, topic, payload, rec)

            for d in fleet:
                if now < d.silent_until:
                    continue
                if rng.random() < args.dropout_prob * tick / 60:
                    dur = rng.uniform(60, 300)
                    d.silent_until = now + dur
                    counts["dropouts"] += 1
                    truth.write(json.dumps({"kind": "dropout", "device_id": d.device_id,
                                            "start": iso(datetime.fromtimestamp(now, timezone.utc)),
                                            "duration_s": round(dur, 1)}) + "\n")
                    continue
                if now < d.next_emit:
                    continue
                d.next_emit = now + args.interval * rng.uniform(0.9, 1.1)

                # decide the delivery delay first: produced_at is the moment the device sends it
                faults, delay = [], 0.0
                if rng.random() < args.late_rate:
                    delay = rng.uniform(args.late_min, args.late_max)
                    faults.append("late")
                    counts["late"] += 1
                elif rng.random() < args.ooo_rate:
                    delay = rng.uniform(1, 5)
                    faults.append("ooo")
                    counts["ooo"] += 1

                ev_dt = datetime.fromtimestamp(now, timezone.utc)
                event = {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                    "home_id": d.home_id, "room": d.room, "device_id": d.device_id,
                    "sensor_type": d.sensor_type, "value": d.reading(rng, now), "unit": d.unit,
                    "event_ts": iso(ev_dt), "seq": d.seq, "fw_version": d.fw,
                    "produced_at": iso(datetime.fromtimestamp(now + delay, timezone.utc)),
                }
                d.seq += 1
                counts["generated"] += 1
                rec = {"kind": "publish", "event_id": event["event_id"], "device_id": d.device_id,
                       "event_ts": event["event_ts"], "faults": faults, "expected": "silver",
                       "lateness_s": round(delay, 2)}
                payload = json.dumps(event)

                if rng.random() < args.malformed_rate:
                    payload, fault, dest = corrupt(rng, event)
                    rec["faults"].append(fault)
                    rec["expected"] = dest
                    counts["malformed"] += 1

                if delay > 0:
                    schedule(now + delay, d.topic, payload, rec)
                else:
                    publish(now, d.topic, payload, rec)

                # duplicates only make sense for parseable payloads
                if rec["expected"] != "dlq" and rng.random() < args.dup_rate:
                    dup = dict(rec, faults=rec["faults"] + ["duplicate_publish"], expected="duplicate")
                    schedule(now + max(delay, 0) + rng.uniform(0.5, 3), d.topic, payload, dup)
                    counts["duplicate"] += 1

            elapsed += tick
            if args.realtime:
                time.sleep(tick)
            elif not args.no_sleep:
                time.sleep(tick / 20)  # gentle default: 20x speed-up
        # flush anything still held
        end = t0 + elapsed + args.late_max + 10
        while pending:
            release, _, topic, payload, rec = heapq.heappop(pending)
            publish(min(release, end), topic, payload, rec)
    except KeyboardInterrupt:
        print("interrupted: stopping", file=sys.stderr)
    finally:
        close_sink()
        truth.close()

    summary = dict(counts, devices=len(fleet), seed=args.seed, simulated_seconds=elapsed,
                   wall_seconds=round(time.time() - wall0, 2))
    with open(args.truth_file.replace(".jsonl", "_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary), file=sys.stderr)


if __name__ == "__main__":
    main()
