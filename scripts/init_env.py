#!/usr/bin/env python3
"""Create a local Compose environment with unique development passwords."""
from __future__ import annotations

import os
import secrets
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update",
        action="store_true",
        help="append only missing keys to an existing .env without changing existing values",
    )
    args = parser.parse_args(argv)
    existed = ENV_FILE.exists()
    if existed and not args.update:
        raise SystemExit(f"{ENV_FILE} already exists; refusing to overwrite it.")

    values = {
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": secrets.token_hex(24),
        "MINIO_WAREHOUSE_USER": "warehouse_app",
        "MINIO_WAREHOUSE_PASSWORD": secrets.token_hex(24),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
        "MQTT_SIMULATOR_USER": "simulator",
        "MQTT_SIMULATOR_PASSWORD": secrets.token_hex(24),
        "MQTT_BRIDGE_USER": "bridge",
        "MQTT_BRIDGE_PASSWORD": secrets.token_hex(24),
        "CO2_THRESHOLD": "1500",
        "POSTGRES_USER": "iceberg",
        "POSTGRES_PASSWORD": secrets.token_hex(24),
        "POSTGRES_DB": "postgres",
    }

    if ENV_FILE.exists():
        existing_keys = {
            line.partition("=")[0].strip()
            for line in ENV_FILE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#") and "=" in line
        }
        missing = {key: value for key, value in values.items() if key not in existing_keys}
        with ENV_FILE.open("a", encoding="utf-8") as env_file:
            if missing:
                env_file.write("\n# Added by make update-env; review and rotate local secrets.\n")
                for key, value in missing.items():
                    env_file.write(f"{key}={value}\n")
    else:
        with ENV_FILE.open("x", encoding="utf-8") as env_file:
            env_file.write("# Generated for this local development environment; do not commit.\n")
            for key, value in values.items():
                env_file.write(f"{key}={value}\n")
    os.chmod(ENV_FILE, 0o600)
    action = "Updated missing keys in" if args.update and existed else "Created"
    print(f"{action} {ENV_FILE} with unique passwords (permissions 0600).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
