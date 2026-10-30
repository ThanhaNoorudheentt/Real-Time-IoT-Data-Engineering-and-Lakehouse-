"""Unit tests for the bridge routing logic and the acceptance-script helpers (no broker needed)."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "bridge"))
sys.path.insert(0, str(ROOT / "scripts"))

pytest.importorskip("confluent_kafka")
pytest.importorskip("paho.mqtt.client")
import bridge  # noqa: E402
import stage1_acceptance as acc  # noqa: E402


class FakeProducer:
    def __init__(self, fail_first=0):
        self.sent, self.fail_first = [], fail_first

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if self.fail_first > 0:
            self.fail_first -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value, dict(headers or [])))

    def poll(self, t=0):
        pass


class Msg:
    def __init__(self, topic, payload):
        self.topic, self.payload = topic, payload


def run(msg, producer):
    bridge.producer = producer
    bridge.on_message(None, None, msg)


def test_device_key_from_topic():
    assert bridge.device_key("sensors/home-001/tmp-0001/temperature") == b"tmp-0001"


def test_valid_json_goes_to_raw_with_topic_header():
    p = FakeProducer()
    run(Msg("sensors/home-001/tmp-0001/temperature", json.dumps({"a": 1}).encode()), p)
    topic, key, _, headers = p.sent[0]
    assert (topic, key) == ("iot.raw", b"tmp-0001")
    assert headers["mqtt_topic"] == b"sensors/home-001/tmp-0001/temperature"


def test_schema_invalid_but_parseable_still_goes_to_raw():
    p = FakeProducer()
    run(Msg("sensors/h/d-0001/x", json.dumps({"value": "n/a"}).encode()), p)
    assert p.sent[0][0] == "iot.raw"


@pytest.mark.parametrize("payload", [b'{"event_id": "abc", "val', b"not json", b"[1, 2]", b""])
def test_unparseable_goes_to_dlq_with_reason(payload):
    p = FakeProducer()
    run(Msg("sensors/h/d-0001/x", payload), p)
    topic, _, value, headers = p.sent[0]
    assert topic == "iot.dlq" and value == payload and headers["reason"]


def test_full_queue_retries_and_does_not_misroute_to_dlq():
    p = FakeProducer(fail_first=3)
    run(Msg("sensors/h/d-0001/x", json.dumps({"a": 1}).encode()), p)
    assert [s[0] for s in p.sent] == ["iot.raw"]


def test_parse_offsets_sums_partitions():
    assert acc.parse_offsets("iot.raw:0:10\niot.raw:1:5\niot.raw:2:0\n") == 15
    assert acc.parse_offsets("garbage\n") == 0


def test_expected_counts_from_truth(tmp_path):
    lines = [
        {"kind": "publish", "expected": "silver"},
        {"kind": "publish", "expected": "duplicate"},
        {"kind": "publish", "expected": "quarantine"},
        {"kind": "publish", "expected": "dlq"},
        {"kind": "dropout"},
    ]
    f = tmp_path / "t.jsonl"
    f.write_text("\n".join(json.dumps(x) for x in lines))
    assert acc.expected_from_truth(f) == {"raw": 3, "dlq": 1}
