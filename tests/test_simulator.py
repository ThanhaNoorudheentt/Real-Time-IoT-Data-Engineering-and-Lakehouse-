import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "simulator"))
import simulator

SIM = ROOT / "simulator" / "simulator.py"
SCHEMA = json.loads((ROOT / "schemas" / "sensor_event.v1.json").read_text())


def run(tmp_path, tag, seed=7, **extra):
    out, truth = tmp_path / f"{tag}.tsv", tmp_path / f"{tag}_truth.jsonl"
    args = [sys.executable, str(SIM), "--sink", "file", "--out-file", str(out), "--truth-file", str(truth),
            "--homes", "2", "--duration", "600", "--no-sleep", "--seed", str(seed),
            "--start-ts", "2026-10-20T00:00:00Z"]
    for k, v in extra.items():
        args += [f"--{k.replace('_', '-')}", str(v)]
    subprocess.run(args, check=True, capture_output=True)
    return out, truth


def test_same_seed_is_reproducible(tmp_path):
    a, _ = run(tmp_path, "a")
    b, _ = run(tmp_path, "b")
    assert a.read_bytes() == b.read_bytes()


def test_different_seed_differs(tmp_path):
    a, _ = run(tmp_path, "a", seed=1)
    b, _ = run(tmp_path, "b", seed=2)
    assert a.read_bytes() != b.read_bytes()


def test_ground_truth_matches_published_messages(tmp_path):
    out, truth = run(tmp_path, "t")
    n_msgs = len(out.read_text().splitlines())
    pubs = [json.loads(l) for l in truth.read_text().splitlines() if '"kind": "publish"' in l]
    assert n_msgs == len(pubs)


def test_no_faults_gives_clean_valid_stream(tmp_path):
    out, truth = run(tmp_path, "clean", dup_rate=0, late_rate=0, ooo_rate=0, malformed_rate=0, dropout_prob=0)
    required = SCHEMA["required"]
    ids = set()
    for line in out.read_text().splitlines():
        _, payload = line.split("\t", 1)
        ev = json.loads(payload)
        assert all(k in ev for k in required)
        lo, hi = (SCHEMA["x-valid-ranges"][ev["sensor_type"]][k] for k in ("min", "max"))
        assert lo <= ev["value"] <= hi
        assert ev["event_id"] not in ids
        ids.add(ev["event_id"])


def test_produced_at_matches_delay(tmp_path):
    from datetime import datetime

    def ts(s):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    out, truth = run(tmp_path, "p", dup_rate=0, malformed_rate=0)
    lateness = {}
    for line in truth.read_text().splitlines():
        r = json.loads(line)
        if r["kind"] == "publish":
            lateness[r["event_id"]] = r["lateness_s"]
    checked = 0
    for line in out.read_text().splitlines():
        ev = json.loads(line.split("\t", 1)[1])
        delta = (ts(ev["produced_at"]) - ts(ev["event_ts"])).total_seconds()
        assert delta >= 0
        assert abs(delta - lateness[ev["event_id"]]) < 0.01
        checked += 1
    assert checked > 100 and any(v > 0 for v in lateness.values())


def test_duplicates_share_event_id(tmp_path):
    _, truth = run(tmp_path, "d", dup_rate=0.2)
    recs = [json.loads(l) for l in truth.read_text().splitlines() if '"kind": "publish"' in l]
    dups = [r for r in recs if "duplicate_publish" in r["faults"]]
    assert dups
    first_ids = {r["event_id"] for r in recs if "duplicate_publish" not in r["faults"]}
    assert all(r["event_id"] in first_ids for r in dups)


def test_local_env_value_reads_credentials_without_shell_execution(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\nMQTT_SIMULATOR_USER='sim user'\nMQTT_SIMULATOR_PASSWORD=abc123\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(simulator, "ENV_FILE", env_file)
    monkeypatch.delenv("MQTT_SIMULATOR_USER", raising=False)
    assert simulator.local_env_value("MQTT_SIMULATOR_USER") == "sim user"
    monkeypatch.setenv("MQTT_SIMULATOR_PASSWORD", "from-environment")
    assert simulator.local_env_value("MQTT_SIMULATOR_PASSWORD") == "from-environment"
