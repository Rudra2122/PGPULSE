#!/usr/bin/env bash
# Local Postgres HA without Docker: 1 etcd + 2 Patroni nodes (primary + synchronous replica).
# Needs on PATH: patroni (pip install "patroni[etcd3]" psycopg2-binary), etcd, and Postgres binaries.
#   macOS: brew install etcd postgresql@16 && pip3 install "patroni[etcd3]" psycopg2-binary
#   ./scripts/pg-ha-local.sh up | down | status
# Then point the app at both nodes:
#   PG_HOSTS=127.0.0.1:5433,127.0.0.1:5434 PG_DATABASE=postgres
set -euo pipefail

DIR="${TMPDIR:-/tmp}/pgpulse-pg-ha"
PG_BIN="${PG_BIN:-$(dirname "$(command -v postgres || command -v pg_ctl)")}"

node_conf() { # name pgport restport
  cat > "$DIR/$1.yml" <<EOF
scope: pgpulse
name: $1
restapi:
  listen: 127.0.0.1:$3
  connect_address: 127.0.0.1:$3
etcd3:
  hosts: 127.0.0.1:2389
bootstrap:
  dcs:
    ttl: 20
    loop_wait: 2
    retry_timeout: 4
    maximum_lag_on_failover: 1048576
    synchronous_mode: true
    postgresql:
      use_pg_rewind: true
      parameters:
        max_connections: 200
        wal_log_hints: "on"
  initdb:
    - encoding: UTF8
    - data-checksums
  pg_hba:
    - local all all trust
    - host all all 127.0.0.1/32 md5
    - host replication replicator 127.0.0.1/32 md5
postgresql:
  listen: 127.0.0.1:$2
  connect_address: 127.0.0.1:$2
  data_dir: $DIR/$1
  bin_dir: $PG_BIN
  pgpass: $DIR/pgpass-$1
  authentication:
    superuser: { username: pgpulse, password: pgpulse }
    replication: { username: replicator, password: replicator }
  parameters:
    unix_socket_directories: '$DIR'
EOF
}

up() {
  down >/dev/null 2>&1 || true
  mkdir -p "$DIR"
  etcd --name pgpulse-etcd --data-dir "$DIR/etcd" \
    --listen-client-urls http://127.0.0.1:2389 --advertise-client-urls http://127.0.0.1:2389 \
    --listen-peer-urls http://127.0.0.1:2390 >"$DIR/etcd.log" 2>&1 &
  echo $! > "$DIR/etcd.pid"
  sleep 2
  node_conf pg1 5433 8018
  node_conf pg2 5434 8019
  patroni "$DIR/pg1.yml" >"$DIR/pg1.log" 2>&1 & echo $! > "$DIR/pg1.pid"
  sleep 8
  patroni "$DIR/pg2.yml" >"$DIR/pg2.log" 2>&1 & echo $! > "$DIR/pg2.pid"
  for i in $(seq 1 40); do
    if curl -s 127.0.0.1:8018/cluster | grep -q '"Sync Standby"'; then break; fi
    sleep 1
  done
  status
}

status() {
  curl -s 127.0.0.1:8018/cluster 2>/dev/null || curl -s 127.0.0.1:8019/cluster
  echo
}

down() {
  for f in pg1 pg2 etcd; do [ -f "$DIR/$f.pid" ] && kill "$(cat "$DIR/$f.pid")" 2>/dev/null || true; done
  sleep 2
  pkill -f "$DIR" 2>/dev/null || true
  rm -rf "$DIR"
}

"${1:-up}"
