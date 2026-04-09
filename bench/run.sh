#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

SKIP_SKYLR=0
SKIP_COMPOSE=0
for arg in "$@"; do
    case "$arg" in
        -skip-skylr)   SKIP_SKYLR=1 ;;
        -skip-compose) SKIP_COMPOSE=1 ;;
    esac
done

OVERSEER_PID=""
cleanup() {
    if [ -n "$OVERSEER_PID" ]; then
        kill "$OVERSEER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- 1. Конкуренты в Docker ---

if [ "$SKIP_COMPOSE" -eq 0 ]; then
    echo "==> docker compose up"
    docker compose -f "$REPO_ROOT/bench/docker-compose.yml" up -d
    echo ""
fi

# --- 2. Оверсеер в фоне ---

if [ "$SKIP_SKYLR" -eq 0 ]; then
    OVERSEER_BIN="$REPO_ROOT/skylr-overseer/bin/skylr-overseer"
    OVERSEER_CFG="$REPO_ROOT/bench/skylr/config.bench.yaml"

    if [ ! -f "$OVERSEER_BIN" ]; then
        echo "==> building skylr-overseer"
        (cd "$REPO_ROOT/skylr-overseer" && make build)
    fi

    if [ ! -f "$REPO_ROOT/skylr-shard/bin/skylr-shard" ]; then
        echo "==> building skylr-shard"
        (cd "$REPO_ROOT/skylr-shard" && make build)
    fi

    OVERSEER_LOG="$REPO_ROOT/bench/overseer.log"
    echo "==> starting skylr-overseer (logs → bench/overseer.log)"
    # Run overseer from its own directory so relative binary/config paths resolve correctly.
    (cd "$REPO_ROOT/skylr-overseer" && "$OVERSEER_BIN" -config="$OVERSEER_CFG") >"$OVERSEER_LOG" 2>&1 &
    OVERSEER_PID=$!

    echo -n "    waiting for :9000"
    for i in $(seq 1 20); do
        if nc -z localhost 9000 2>/dev/null; then
            echo " ready"
            break
        fi
        echo -n "."
        sleep 0.5
        if [ "$i" -eq 20 ]; then
            echo ""
            echo "ERROR: overseer did not start within 10s" >&2
            exit 1
        fi
    done
    echo ""
fi

# --- 3. Driver (Redis / Valkey / KeyDB / Memcached) ---

echo "==> bench/driver"
(cd "$REPO_ROOT/bench/driver" && go run .)
echo ""

# --- 4. Skylr ---

if [ "$SKIP_SKYLR" -eq 0 ]; then
    echo "==> bench/skylr"
    (cd "$REPO_ROOT/bench/skylr" && go run .)
fi
