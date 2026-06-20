#!/usr/bin/env bash
# Scale verification runner — 2000 points (BACnet 5 devices × 200pts + OPC-UA 1000pts)
# Usage: bash fixtures/scale/run_scale_test.sh [phase]
#   phase: setup | start | metrics | burst | stop (default: metrics)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NATS_URL="nats://127.0.0.1:14222"
GW_METRICS="http://localhost:8080/metrics"
POINT_LIST_FILE="$SCRIPT_DIR/point_list_2000.json"
OPCUA_POINTS_FILE="$SCRIPT_DIR/opcua_points_1000.json"

# BACnet devices: connector_id → (port, device_id)
declare -A BACNET_PORT=(
  [bacnet-01]=47808 [bacnet-02]=47809 [bacnet-03]=47810
  [bacnet-04]=47811 [bacnet-05]=47812
)
declare -A BACNET_DEVICE=(
  [bacnet-01]=1001 [bacnet-02]=1002 [bacnet-03]=1003
  [bacnet-04]=1004 [bacnet-05]=1005
)
BACNET_CONNECTORS=(bacnet-01 bacnet-02 bacnet-03 bacnet-04 bacnet-05)

phase="${1:-metrics}"

# ── helpers ──────────────────────────────────────────────────────────────────
metric() { curl -s "$GW_METRICS" | grep "^$1 " | awk '{print $2}' | head -1; }
header() { echo ""; echo "═══════════════════════════════════════"; echo "  $*"; echo "═══════════════════════════════════════"; }

# ── PHASE: setup ─────────────────────────────────────────────────────────────
if [[ "$phase" == "setup" ]]; then
    header "Phase 1: Setup verification"

    echo "[1] Checking point list..."
    TOTAL=$(python3 -c "import json; d=json.load(open('$POINT_LIST_FILE')); print(len(d))")
    BN=$(python3 -c "import json; d=json.load(open('$POINT_LIST_FILE')); print(sum(1 for p in d if p['protocol']=='bacnet'))")
    UA=$(python3 -c "import json; d=json.load(open('$POINT_LIST_FILE')); print(sum(1 for p in d if p['protocol']=='opcua'))")
    echo "    Total: $TOTAL  (BACnet: $BN, OPC-UA: $UA)"
    [[ "$TOTAL" == "2000" ]] && echo "    ✓ 2000 points" || echo "    ✗ Expected 2000, got $TOTAL"

    echo "[2] Checking per-connector BACnet point files..."
    for cid in "${BACNET_CONNECTORS[@]}"; do
        f="$SCRIPT_DIR/bacnet_points_${cid}.json"
        if [[ -f "$f" ]]; then
            CNT=$(python3 -c "import json; print(len(json.load(open('$f'))))")
            echo "    $cid: $CNT points"
        else
            echo "    ✗ Missing: $f"
        fi
    done

    echo "[3] Checking OPC-UA points..."
    UA_CNT=$(python3 -c "import json; print(len(json.load(open('$OPCUA_POINTS_FILE'))))")
    echo "    opcua-01: $UA_CNT points"

    echo "[4] BACnet port plan:"
    for cid in "${BACNET_CONNECTORS[@]}"; do
        echo "    $cid → port ${BACNET_PORT[$cid]}, device_id=${BACNET_DEVICE[$cid]}"
    done

    echo ""
    echo "Start multi-device BACnet simulator:"
    echo "  python3 simulator/bacnet/device_multi.py"
    echo "Start OPC-UA simulator:"
    echo "  python3 simulator/opcua/server_scale.py"
    echo "Then run: bash fixtures/scale/run_scale_test.sh start"
fi

# ── PHASE: start ─────────────────────────────────────────────────────────────
if [[ "$phase" == "start" ]]; then
    header "Phase 2: Start containers"

    echo "[1] Stopping old containers..."
    docker stop nexus-gateway opcua-connector 2>/dev/null || true
    docker rm   nexus-gateway opcua-connector 2>/dev/null || true
    for cid in "${BACNET_CONNECTORS[@]}"; do
        docker stop "bacnet-connector-${cid#bacnet-}" 2>/dev/null || true
        docker rm   "bacnet-connector-${cid#bacnet-}" 2>/dev/null || true
    done

    echo "[2] Starting 5 BACnet connectors..."
    for cid in "${BACNET_CONNECTORS[@]}"; do
        PORT="${BACNET_PORT[$cid]}"
        DID="${BACNET_DEVICE[$cid]}"
        CNUM="${cid#bacnet-}"
        BPTS=$(cat "$SCRIPT_DIR/bacnet_points_${cid}.json")
        docker run -d \
          --name "bacnet-connector-${CNUM}" \
          --network host \
          -e "CONNECTOR_ID=${cid}" \
          -e "NATS_URL=$NATS_URL" \
          -e "BACNET_ADDRESS=127.0.0.1:${PORT}" \
          -e "BACNET_DEVICE_ID=${DID}" \
          -e BACNET_LOCAL_ADDRESS=0.0.0.0 \
          -e BACNET_POLL_INTERVAL=30 \
          -e "BACNET_POINTS=${BPTS}" \
          nexus-gateway-bacnet-connector:latest
        echo "    Started $cid (device $DID on port $PORT)"
    done

    echo "[3] Starting OPC-UA connector (1000 points)..."
    OPTS=$(cat "$OPCUA_POINTS_FILE")
    docker run -d \
      --name opcua-connector \
      --network host \
      -e CONNECTOR_ID=opcua-01 \
      -e "NATS_URL=$NATS_URL" \
      -e OPCUA_ENDPOINT=opc.tcp://127.0.0.1:4840 \
      -e "OPCUA_POINTS=${OPTS}" \
      nexus-gateway-opcua-connector:latest

    echo "[4] Starting gateway (2000-point fixture)..."
    docker run -d \
      --name nexus-gateway \
      --network host \
      -e GATEWAY_ID=gw-001 \
      -e "NATS_URL=$NATS_URL" \
      -e BOS_INGRESS_ADDR=192.168.0.18:5051 \
      -e BOS_EGRESS_ADDR=192.168.0.18:5052 \
      -e BOS_INSECURE=true \
      -e POINT_LIST_FILE=/fixtures/scale/point_list_2000.json \
      -e POINT_LIST_PERSIST=/data/point_list.json \
      -e ADMIN_ADDR=:8080 \
      -e SF_DB=/data/storeforward.db \
      -v "$REPO_ROOT/fixtures:/fixtures:ro" \
      -v "$REPO_ROOT/data:/data" \
      nexus-gateway-gateway:latest

    echo "Waiting 20s for startup..."
    sleep 20

    echo "[5] Metrics check..."
    UNRES=$(metric "normalizer_unresolved_total" 2>/dev/null || echo "n/a")
    WRITTEN=$(metric storefwd_written_total 2>/dev/null || echo "n/a")
    echo "    normalizer_unresolved: ${UNRES}"
    echo "    storefwd_written:      ${WRITTEN}"
    echo ""
    echo "Run 'metrics' phase next to collect steady-state readings."
fi

# ── PHASE: metrics ────────────────────────────────────────────────────────────
if [[ "$phase" == "metrics" ]]; then
    header "Phase 3: Steady-state metrics snapshot"
    echo "Timestamp: $(date '+%Y-%m-%dT%H:%M:%S%z')"
    echo ""

    WRITTEN=$(metric storefwd_written_total)
    SENT=$(metric storefwd_sent_total)
    DEPTH=$(metric storefwd_buffer_depth)
    CKPT=$(metric storefwd_checkpoint_total)
    UNRES=$(metric "normalizer_unresolved_total" 2>/dev/null || echo "0")
    HEALTH=$(curl -s http://localhost:8080/health 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "n/a")

    echo "Gateway:"
    echo "  health:           $HEALTH"
    echo "  storefwd_written: ${WRITTEN:-0}"
    echo "  storefwd_sent:    ${SENT:-0}"
    echo "  buffer_depth:     ${DEPTH:-0}"
    echo "  checkpoints:      ${CKPT:-0}"
    echo "  unresolved:       ${UNRES:-0}"
    echo ""

    echo "Container resources:"
    CNAMES=(nexus-gateway opcua-connector)
    for cid in "${BACNET_CONNECTORS[@]}"; do
        CNAMES+=("bacnet-connector-${cid#bacnet-}")
    done
    docker stats --no-stream "${CNAMES[@]}" \
      --format "  {{.Name}}: CPU={{.CPUPerc}} MEM={{.MemUsage}}" 2>/dev/null \
      || echo "  (some containers not running)"

    echo ""
    echo "Host:"
    uptime | awk '{print "  load:", $0}'
    free -h | awk '/^Mem/ {printf "  memory: used=%s / total=%s\n", $3, $2}'

    echo ""
    echo "Acceptance criteria:"
    [[ "${UNRES:-1}" == "0" ]] \
      && echo "  ✓ normalizer_unresolved = 0" \
      || echo "  ✗ normalizer_unresolved = ${UNRES} (expect 0)"
    DEPTH_INT=${DEPTH%.*}; DEPTH_INT=${DEPTH_INT:-0}
    [[ "$DEPTH_INT" -lt 10000 ]] \
      && echo "  ✓ buffer_depth < 10,000" \
      || echo "  ✗ buffer_depth = $DEPTH (expect < 10,000)"
fi

# ── PHASE: burst ─────────────────────────────────────────────────────────────
if [[ "$phase" == "burst" ]]; then
    header "Phase 4: Burst test — restart simulators"

    BEFORE_WRITTEN=$(metric storefwd_written_total)
    echo "Before burst: written=$BEFORE_WRITTEN"

    echo "Restarting simulators to trigger initial subscription flood..."
    pkill -f "server_scale.py" 2>/dev/null || true
    pkill -f "device_multi.py" 2>/dev/null || true
    sleep 2

    python3 "$REPO_ROOT/simulator/opcua/server_scale.py" &
    python3 "$REPO_ROOT/simulator/bacnet/device_multi.py" &

    echo "Measuring peak buffer depth over 30s..."
    PEAK_DEPTH=0
    for i in $(seq 1 6); do
        sleep 5
        D=$(metric storefwd_buffer_depth)
        D_INT=${D%.*}; D_INT=${D_INT:-0}
        [[ "$D_INT" -gt "$PEAK_DEPTH" ]] && PEAK_DEPTH=$D_INT
        echo "  t=$((i*5))s: buffer_depth=$D"
    done

    AFTER_WRITTEN=$(metric storefwd_written_total)
    AFTER_UNRES=$(metric "normalizer_unresolved_total" 2>/dev/null || echo "0")
    DELTA=$(( ${AFTER_WRITTEN%.*} - ${BEFORE_WRITTEN%.*} ))

    echo ""
    echo "Burst results:"
    echo "  frames written:    $DELTA"
    echo "  peak buffer depth: $PEAK_DEPTH"
    echo "  unresolved:        $AFTER_UNRES  (expect 0)"
fi

# ── PHASE: stop ──────────────────────────────────────────────────────────────
if [[ "$phase" == "stop" ]]; then
    header "Stopping scale test"
    for cid in "${BACNET_CONNECTORS[@]}"; do
        docker stop "bacnet-connector-${cid#bacnet-}" 2>/dev/null || true
        docker rm   "bacnet-connector-${cid#bacnet-}" 2>/dev/null || true
    done
    docker stop nexus-gateway opcua-connector 2>/dev/null || true
    docker rm   nexus-gateway opcua-connector 2>/dev/null || true
    pkill -f "server_scale.py" 2>/dev/null || true
    pkill -f "device_multi.py" 2>/dev/null || true
    echo "Stopped."
fi
