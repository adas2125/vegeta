#!/usr/bin/env bash
set -euo pipefail

# target configuration for the server and attack
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
METHOD="${METHOD:-GET}"
PATH_PREFIX="${PATH_PREFIX:-/}"

# attack configuration
DURATION="${DURATION:-15s}"
RATE="${RATE:-400}"
COOLDOWN="${COOLDOWN:-3}"

# more specification for the server's behavior to create the misleading throughput scenario
SERVER_FAST_DELAY="${SERVER_FAST_DELAY:-10ms}"
SERVER_SLOW_DELAY="${SERVER_SLOW_DELAY:-400ms}"
SERVER_CYCLE="${SERVER_CYCLE:-4s}"
SERVER_SPIKE="${SERVER_SPIKE:-1500ms}"
BURST_SERVER_BIN="${BURST_SERVER_BIN:-./vegeta-burst-server}"

## number of workers and connections for the constrained and well-provisioned cases
CONSTRAINED_WORKERS="${CONSTRAINED_WORKERS:-20}"
CONSTRAINED_CONNECTIONS="${CONSTRAINED_CONNECTIONS:-20}"

# 400 req/s * 400 ms spike implies ~160 concurrent in-flight requests at peak.
WELL_PROVISIONED_WORKERS="${WELL_PROVISIONED_WORKERS:-220}"
WELL_PROVISIONED_CONNECTIONS="${WELL_PROVISIONED_CONNECTIONS:-220}"

# creating a unique output directory for this run based on the timestamp
STAMP="$(date +%Y%m%d_%H%M%S)"
OUTDIR="${OUTDIR:-misleading_results/run_${STAMP}}"

# Creates a temp file w/ GET http://127.0.0.1:8080/
TARGETS_FILE="$(mktemp)"
SERVER_PID=""

mkdir -p "$OUTDIR"
printf '%s http://%s:%s%s\n' "$METHOD" "$HOST" "$PORT" "$PATH_PREFIX" > "$TARGETS_FILE"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

## Cleanup logic
cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -f "$TARGETS_FILE"
}
trap cleanup EXIT

# starting up the bursty server with retries to ensure it's ready before the attack starts
start_server() {
  local log_file=$1
  local attempt

  for attempt in 1 2 3 4 5; do
    echo "[$(timestamp)] Starting burst server (attempt ${attempt}/5)"
    : > "$log_file"
    "$BURST_SERVER_BIN" \
      -addr ":${PORT}" \
      -fast-delay "${SERVER_FAST_DELAY}" \
      -slow-delay "${SERVER_SLOW_DELAY}" \
      -cycle "${SERVER_CYCLE}" \
      -spike "${SERVER_SPIKE}" \
      >"$log_file" 2>&1 &

    SERVER_PID=$!
    sleep 1

    if kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 0
    fi

    SERVER_PID=""
    echo "[$(timestamp)] Burst server not ready yet; retrying in 1s"
    sleep 1
  done

  echo "[$(timestamp)] Burst server failed to start after retries. Check ${log_file}" >&2
  exit 1
}

# gracefully stops the server
stop_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "[$(timestamp)] Stopping burst server"
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
  sleep 1
}

# builds the burst server binary if it doesn't exist
ensure_burst_server_binary() {
  echo "[$(timestamp)] Building burst server binary: ${BURST_SERVER_BIN}"
  mkdir -p .gocache .gomodcache
  GOCACHE="$PWD/.gocache" GOMODCACHE="$PWD/.gomodcache" \
    go build -o "$BURST_SERVER_BIN" ./cmd/burst_server
}

run_case() {
  local case_name=$1
  local workers=$2
  local connections=$3

  local case_dir="${OUTDIR}/${case_name}"
  mkdir -p "$case_dir"

  # creates per-case outputs
  local metrics_csv="${case_dir}/${case_name}_metrics.csv"
  local window_csv="${case_dir}/${case_name}_window_results.csv"
  local window_samples_csv="${case_dir}/${case_name}_window_samples.csv"
  local results_bin="${case_dir}/${case_name}_results.bin"
  local report_json="${case_dir}/${case_name}_report.json"
  local report_text="${case_dir}/${case_name}_report.txt"
  local server_log="${case_dir}/${case_name}_server.log"
  local run_meta="${case_dir}/${case_name}_run_config.txt"

  cat > "$run_meta" <<EOF
case_name=${case_name}
target=${METHOD} http://${HOST}:${PORT}${PATH_PREFIX}
rate=${RATE}
duration=${DURATION}
workers=${workers}
max_workers=${workers}
connections=${connections}
server_fast_delay=${SERVER_FAST_DELAY}
server_slow_delay=${SERVER_SLOW_DELAY}
server_cycle=${SERVER_CYCLE}
server_spike=${SERVER_SPIKE}
EOF

  echo "[$(timestamp)] ======================================"
  echo "[$(timestamp)] Running case=${case_name}"
  echo "[$(timestamp)] rate=${RATE} duration=${DURATION} workers=${workers} connections=${connections}"
  echo "[$(timestamp)] Outputs: ${case_dir}"
  echo "[$(timestamp)] ======================================"

  start_server "$server_log"

  # runs the server attack
  ./vegeta attack \
    -targets="$TARGETS_FILE" \
    -rate="$RATE" \
    -duration="$DURATION" \
    -workers="$workers" \
    -max-workers="$workers" \
    -connections="$connections" \
    -keepalive=true \
    -http2=false \
    -metrics-csv="$metrics_csv" \
    -window-csv="$window_csv" \
    -window-samples-csv="$window_samples_csv" \
    -output="$results_bin"

  ./vegeta report -type=json "$results_bin" > "$report_json"
  ./vegeta report "$results_bin" > "$report_text"

  # stops server + cooldown
  stop_server

  echo "[$(timestamp)] Completed case=${case_name}"
  echo "[$(timestamp)] Cooling down ${COOLDOWN}s"
  sleep "$COOLDOWN"
}

echo "[$(timestamp)] Starting misleading-throughput comparison run"
echo "[$(timestamp)] Output directory: ${OUTDIR}"
echo "[$(timestamp)] Target: ${METHOD} http://${HOST}:${PORT}${PATH_PREFIX}"
echo "[$(timestamp)] Server profile: fast=${SERVER_FAST_DELAY} slow=${SERVER_SLOW_DELAY} cycle=${SERVER_CYCLE} spike=${SERVER_SPIKE}"
echo "[$(timestamp)] Attack profile: rate=${RATE} duration=${DURATION}"

ensure_burst_server_binary

run_case "constrained" "$CONSTRAINED_WORKERS" "$CONSTRAINED_CONNECTIONS"
run_case "well_provisioned" "$WELL_PROVISIONED_WORKERS" "$WELL_PROVISIONED_CONNECTIONS"

echo "[$(timestamp)] All runs completed!"
