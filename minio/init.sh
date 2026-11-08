#!/bin/sh
set -eu

mc mb --ignore-existing local/warehouse
if ! mc admin policy info local warehouse-access >/dev/null 2>&1; then
    mc admin policy create local warehouse-access /tmp/warehouse-policy.json
fi
if ! mc admin user info local "$MINIO_WAREHOUSE_USER" >/dev/null 2>&1; then
    mc admin user add local "$MINIO_WAREHOUSE_USER" "$MINIO_WAREHOUSE_PASSWORD"
fi
mc admin policy attach local warehouse-access --user "$MINIO_WAREHOUSE_USER"
