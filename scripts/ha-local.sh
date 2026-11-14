#!/usr/bin/env bash
# Local Redis HA setup without Docker: 1 primary, 1 replica, 3 Sentinels.
# Needs redis-server on PATH (macOS: brew install redis).
#   ./scripts/ha-local.sh up     start everything
#   ./scripts/ha-local.sh down   stop everything
# Then run the failover bench with:
#   REDIS_SENTINELS=127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392 node dist/queue/bench.js failover
set -euo pipefail

DIR="${TMPDIR:-/tmp}/pgpulse-ha"
PRIMARY=6390
REPLICA=6391
SENTINELS=(26390 26391 26392)

up() {
  down >/dev/null 2>&1 || true
  mkdir -p "$DIR"
  redis-server --port $PRIMARY --save '' --appendonly no --daemonize yes \
    --pidfile "$DIR/r$PRIMARY.pid" --logfile "$DIR/r$PRIMARY.log" --dir "$DIR"
  redis-server --port $REPLICA --save '' --appendonly no --daemonize yes \
    --replicaof 127.0.0.1 $PRIMARY \
    --pidfile "$DIR/r$REPLICA.pid" --logfile "$DIR/r$REPLICA.log" --dir "$DIR"
  for p in "${SENTINELS[@]}"; do
    cat > "$DIR/sentinel-$p.conf" <<EOF
port $p
daemonize yes
pidfile $DIR/s$p.pid
logfile $DIR/s$p.log
dir $DIR
sentinel monitor mymaster 127.0.0.1 $PRIMARY 2
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 10000
sentinel parallel-syncs mymaster 1
EOF
    redis-server "$DIR/sentinel-$p.conf" --sentinel
  done
  sleep 3
  echo "primary per sentinel: $(redis-cli -p ${SENTINELS[0]} SENTINEL get-master-addr-by-name mymaster | tr '\n' ':')"
  echo "replica role: $(redis-cli -p $REPLICA ROLE | head -1)"
}

down() {
  for p in $PRIMARY $REPLICA "${SENTINELS[@]}"; do redis-cli -p "$p" SHUTDOWN NOSAVE >/dev/null 2>&1 || true; done
  rm -rf "$DIR"
}

"${1:-up}"
