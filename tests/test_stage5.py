import csv
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "streaming"))

import benchmark  # noqa: E402
import collect_metrics  # noqa: E402
import init_env  # noqa: E402
import maintenance  # noqa: E402


def test_init_env_generates_private_file_and_adds_only_missing_keys(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setattr(init_env, "ENV_FILE", env_file)

    assert init_env.main([]) == 0
    before = env_file.read_text()
    assert "MINIO_WAREHOUSE_PASSWORD=" in before
    assert os.stat(env_file).st_mode & 0o777 == 0o600

    with pytest.raises(SystemExit, match="already exists"):
        init_env.main([])

    without_bridge_password = "\n".join(
        line for line in before.splitlines()
        if not line.startswith("MQTT_BRIDGE_PASSWORD=")
    )
    env_file.write_text("KEEP_ME=unchanged\n" + without_bridge_password + "\n")
    original = env_file.read_text()
    assert init_env.main(["--update"]) == 0
    updated = env_file.read_text()
    assert "KEEP_ME=unchanged" in updated
    assert updated.count("MINIO_ROOT_PASSWORD=") == 1
    assert updated.count("MQTT_BRIDGE_PASSWORD=") == 1
    assert updated.startswith(original)


def test_parse_topic_offsets_and_lag_input():
    offsets = collect_metrics.parse_topic_offsets(
        "iot.raw:0:8\niot.raw:1:12\niot.dlq:0:3\ninvalid\n",
        "iot.raw",
    )
    assert offsets == {0: 8, 1: 12}


def test_maintenance_compaction_is_non_destructive_and_expiry_is_explicit():
    statements = maintenance.build_statements(
        ["silver.sensor_events"], "rewrite"
    )
    assert statements == [
        "CALL iceberg_catalog.system.rewrite_data_files(table => 'silver.sensor_events')"
    ]

    with pytest.raises(ValueError, match="requires --expire-before"):
        maintenance.build_statements(["silver.sensor_events"], "expire")

    expiry = maintenance.build_statements(
        ["silver.sensor_events"], "expire", "2025-01-01 00:00:00", retain_last=5
    )
    assert "retain_last => 5" in expiry[0]
    with pytest.raises(ValueError, match="unsupported table"):
        maintenance.build_statements(["silver.sensor_events; DROP TABLE x"], "rewrite")


def test_benchmark_case_measures_deterministic_local_run():
    result = benchmark.run_case(homes=1, duration=5, interval=2.5, seed=7)
    assert result["published_events"] > 0
    assert result["wall_seconds"] > 0
    assert result["events_per_second"] > 0
    assert "excludes MQTT" in result["scope"]


def test_minio_policy_is_bucket_scoped():
    policy_path = ROOT / "minio" / "warehouse-policy.json"
    policy = json.loads(policy_path.read_text())
    resources = [
        resource
        for statement in policy["Statement"]
        for resource in statement["Resource"]
    ]
    assert "arn:aws:s3:::warehouse" in resources
    assert all(":::*" not in resource for resource in resources)
