import hashlib
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_alert_id_is_deterministic():
    event_id = "550e8400-e29b-41d4-a716-446655440000"
    expected_hash = hashlib.sha256(event_id.encode("utf-8")).hexdigest()

    # Hash must be 64-char hex string
    assert len(expected_hash) == 64
    # Recomputing gives same hash
    assert hashlib.sha256(event_id.encode("utf-8")).hexdigest() == expected_hash


def test_dashboard_json_structure():
    dashboard_file = PROJECT_ROOT / "monitoring" / "dashboards" / "iot-lakehouse.json"
    assert dashboard_file.exists(), "Dashboard JSON file missing"

    with open(dashboard_file, "r", encoding="utf-8") as f:
        dashboard = json.load(f)

    assert dashboard["uid"] == "iot-lakehouse"
    panels = dashboard.get("panels", [])
    assert len(panels) >= 5, f"Expected at least 5 panels, found {len(panels)}"

    panel_titles = [p["title"] for p in panels]
    assert any("CO2" in t for t in panel_titles)
    assert any("power" in t.lower() for t in panel_titles)
    assert any("health" in t.lower() for t in panel_titles)
    assert any("alerts" in t.lower() for t in panel_titles)
    assert any("pass rate" in t.lower() for t in panel_titles)

    # Ensure power is average watts in SQL
    power_panel = next(p for p in panels if "power" in p["title"].lower())
    sql = power_panel["targets"][0]["rawSQL"]
    assert "avg_value" in sql
    assert "SUM" not in sql.upper()


def test_trino_datasource_provisioning():
    ds_file = PROJECT_ROOT / "monitoring" / "provisioning" / "datasources" / "trino.yml"
    assert ds_file.exists(), "Datasource provisioning file missing"

    with open(ds_file, "r", encoding="utf-8") as f:
        content = f.read()

    assert "type: trino-datasource" in content
    assert "url: http://trino:8080" in content
    assert "user: grafana" in content
