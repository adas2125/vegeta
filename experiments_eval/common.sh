#!/usr/bin/env bash

# Shared helpers for the XLG Inspector evaluation scripts.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output}"
TOOLS_DIR="${TOOLS_DIR:-${OUTPUT_ROOT}/tools}"

VEGETA_BIN="${VEGETA_BIN:-${REPO_ROOT}/vegeta}"
VEGETA_LOGICAL_CPUS="${VEGETA_LOGICAL_CPUS:-4}"
SIMPLE_SERVER_BIN="${SIMPLE_SERVER_BIN:-${TOOLS_DIR}/simple_server}"
WINDOW_S="${WINDOW_S:-1}"
WINDOW_SAMPLE_RETENTION="${WINDOW_SAMPLE_RETENTION:-1.0}"

SERVER_HOST="${SERVER_HOST:-127.0.0.1}"
SERVER_PORT="${SERVER_PORT:-18080}"
TARGET_METHOD="${TARGET_METHOD:-GET}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-5}"
CPU_STRESS_WARMUP="${CPU_STRESS_WARMUP:-2}"

HEALTHY_WORKERS="${HEALTHY_WORKERS:-10}"
HEALTHY_MAX_WORKERS="${HEALTHY_MAX_WORKERS:-10000}"
HEALTHY_CONNECTIONS="${HEALTHY_CONNECTIONS:-10000}"
HEALTHY_MAX_CONNECTIONS="${HEALTHY_MAX_CONNECTIONS:-0}"

SERVER_PID=""
STRESS_PIDS=()

log() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $*"
}

start_simple_server() {
  local log_path="$1"
  local delay_mode="$2"
  local delay_value="$3"

  stop_simple_server

  mkdir -p "$(dirname "$log_path")"
  log "server ${delay_mode}:${delay_value} port=${SERVER_PORT}"

  local server_args=(
    "$SIMPLE_SERVER_BIN"
    -addr ":${SERVER_PORT}"
    -delay-mode "$delay_mode"
  )

  if [[ "$delay_mode" == "fixed" ]]; then
    server_args+=(-delay "$delay_value")
  elif [[ "$delay_mode" == "exp" ]]; then
    server_args+=(-mean-delay "$delay_value")
  else
    echo "Unknown delay mode: $delay_mode" >&2
    exit 1
  fi

  "${server_args[@]}" > "$log_path" 2>&1 &
  SERVER_PID=$!
  sleep "${SERVER_WARMUP:-1}"
}

stop_simple_server() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
}

start_cpu_stress() {
  local jobs="$1"
  local log_path="$2"

  if (( jobs <= 0 )); then
    return 0
  fi

  mkdir -p "$(dirname "$log_path")"
  log "cpu stress yes jobs=${jobs}"
  : > "$log_path"
  local i
  for ((i = 0; i < jobs; i++)); do
    yes > /dev/null 2>> "$log_path" &
    STRESS_PIDS+=("$!")
  done
  sleep "$CPU_STRESS_WARMUP"
}

stop_cpu_stress() {
  local pid
  for pid in "${STRESS_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  STRESS_PIDS=()
}

run_attack_to_dir() {
  local case_dir="$1"
  local label="$2"
  local rate="$3"
  local duration="$4"
  local targets_file="$5"
  local reference_csv="$6"
  local workers="$7"
  local max_workers="$8"
  local connections="$9"
  local max_connections="${10}"
  shift 10

  mkdir -p "$case_dir"

  local metrics_csv="${case_dir}/metrics_rps${rate}.csv"
  local window_csv="${case_dir}/window_results_rps${rate}.csv"
  local samples_csv="${case_dir}/window_samples_rps${rate}.csv"
  local results_bin="${case_dir}/results_rps${rate}.bin"
  local xlg_log="${case_dir}/xlg_windows_rps${rate}.log"
  local report_txt="${case_dir}/report_rps${rate}.txt"
  local report_json="${case_dir}/report_rps${rate}.json"

  log "run ${label}: ${rate}rps ${duration} workers=${workers}/${max_workers} conns=${connections}/${max_connections}"

  local attack_args=(
    attack
    -targets="$targets_file"
    -rate="$rate"
    -duration="$duration"
    -workers="$workers"
    -max-workers="$max_workers"
    -connections="$connections"
    -max-connections="$max_connections"
    -keepalive=true
    -http2=false
    -metrics-interval="${WINDOW_S}s"
    -metrics-csv="$metrics_csv"
    -window-csv="$window_csv"
    -window-samples-csv="$samples_csv"
    -window-sample-retention="$WINDOW_SAMPLE_RETENTION"
    -output="$results_bin"
  )

  if [[ -n "$reference_csv" ]]; then
    attack_args+=(-reference-csv-path="$reference_csv")
  fi

  attack_args+=("$@")
  "$VEGETA_BIN" -cpus="$VEGETA_LOGICAL_CPUS" "${attack_args[@]}" > "$xlg_log"
  "$VEGETA_BIN" -cpus="$VEGETA_LOGICAL_CPUS" report "$results_bin" > "$report_txt"
  "$VEGETA_BIN" -cpus="$VEGETA_LOGICAL_CPUS" report -type=json "$results_bin" > "$report_json"

  cat > "${case_dir}/run_meta.env" <<EOF
label=${label}
rate=${rate}
duration=${duration}
workers=${workers}
max_workers=${max_workers}
connections=${connections}
max_connections=${max_connections}
reference_csv=${reference_csv}
vegeta_logical_cpus=${VEGETA_LOGICAL_CPUS}
window_s=${WINDOW_S}
EOF

  log "done ${label}: ${case_dir}"
  sleep "$SLEEP_BETWEEN_RUNS"
}

json_value() {
  local path="$1"
  local query="$2"
  python3 - "$path" "$query" <<'PY'
import json
import sys

value = json.loads(open(sys.argv[1]).read())
for part in sys.argv[2].split("."):
    if part:
        value = value[part]
print(value)
PY
}
