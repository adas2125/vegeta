#!/usr/bin/env bash

# Shared helpers for the XLG Inspector evaluation scripts.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output}"

VEGETA_BIN="${VEGETA_BIN:-${REPO_ROOT}/vegeta}"
VEGETA_LOGICAL_CPUS="${VEGETA_LOGICAL_CPUS:-4}"
WINDOW_S="${WINDOW_S:-1}"
WINDOW_SAMPLE_RETENTION="${WINDOW_SAMPLE_RETENTION:-1.0}"

SERVER_HOST="${SERVER_HOST:-}"
SERVER_PORT="${SERVER_PORT:-}"
TARGET_METHOD="${TARGET_METHOD:-GET}"
NETEM_IFACE="${NETEM_IFACE:-}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-5}"
CPU_STRESS_WARMUP="${CPU_STRESS_WARMUP:-2}"

HEALTHY_WORKERS="${HEALTHY_WORKERS:-10}"
HEALTHY_MAX_WORKERS="${HEALTHY_MAX_WORKERS:-10000}"
HEALTHY_CONNECTIONS="${HEALTHY_CONNECTIONS:-10000}"
HEALTHY_MAX_CONNECTIONS="${HEALTHY_MAX_CONNECTIONS:-0}"

STRESS_PIDS=()

log() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $*"
}

sut_target_url() {
  echo "http://${SERVER_HOST}:${SERVER_PORT}/"
}

require_external_sut() {
  if [[ -z "$SERVER_HOST" || -z "$SERVER_PORT" ]]; then
    echo "SERVER_HOST and SERVER_PORT are required for external-SUT experiments." >&2
    echo "Example: SERVER_HOST=<sut-vm-ip> SERVER_PORT=<sut-port> experiments_eval/run_full_pipeline.sh" >&2
    exit 1
  fi
}

write_targets_file() {
  local targets_file="$1"

  require_external_sut
  mkdir -p "$(dirname "$targets_file")"
  printf '%s %s\n' "$TARGET_METHOD" "$(sut_target_url)" > "$targets_file"
  log "target ${TARGET_METHOD} $(sut_target_url)"
}

client_netem_iface() {
  if [[ -n "$NETEM_IFACE" ]]; then
    echo "$NETEM_IFACE"
    return 0
  fi

  if ! command -v ip >/dev/null 2>&1; then
    echo "ip command not found; set NETEM_IFACE explicitly." >&2
    exit 1
  fi

  ip route get "$SERVER_HOST" | awk '{
    for (i = 1; i <= NF; i++) {
      if ($i == "dev") {
        print $(i + 1)
        exit
      }
    }
  }'
}

set_client_network_delay() {
  local delay="$1"
  local iface

  require_external_sut
  iface="$(client_netem_iface)"
  if [[ -z "$iface" ]]; then
    echo "Could not infer client network interface for SERVER_HOST=${SERVER_HOST}; set NETEM_IFACE." >&2
    exit 1
  fi

  log "client netem delay=${delay} iface=${iface}"
  sudo tc qdisc replace dev "$iface" root netem delay "$delay"
}

clear_client_network_delay() {
  local iface

  if [[ -z "$SERVER_HOST" && -z "$NETEM_IFACE" ]]; then
    return 0
  fi

  iface="$(client_netem_iface 2>/dev/null || true)"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  log "client netem clear iface=${iface}"
  sudo tc qdisc del dev "$iface" root 2>/dev/null || true
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
