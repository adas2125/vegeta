#!/usr/bin/env bash

# Shared helpers for the XLG Inspector evaluation scripts.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output}"

VEGETA_BIN="${VEGETA_BIN:-${REPO_ROOT}/vegeta}"
TARGETS_FILE="${TARGETS_FILE:-${REPO_ROOT}/targets.txt}"

NETEM_IFACE="${NETEM_IFACE:-}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-5}"
CPU_STRESS_WARMUP="${CPU_STRESS_WARMUP:-2}"
SUDO_KEEPALIVE_INTERVAL_S="${SUDO_KEEPALIVE_INTERVAL_S:-60}"

HEALTHY_WORKERS="${HEALTHY_WORKERS:-10}"
HEALTHY_MAX_WORKERS="${HEALTHY_MAX_WORKERS:-10000}"
HEALTHY_CONNECTIONS="${HEALTHY_CONNECTIONS:-10000}"
HEALTHY_MAX_CONNECTIONS="${HEALTHY_MAX_CONNECTIONS:-0}"

STRESS_PIDS=()
NETWORK_RAMP_PIDS=()
CLIENT_NETWORK_RAMP_LAST_PID=""
SUDO_KEEPALIVE_PID="${SUDO_KEEPALIVE_PID:-}"
SUDO_KEEPALIVE_OWNED=""

log() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $*"
}

require_targets_file() {
  local targets_file="${1:-$TARGETS_FILE}"

  if [[ ! -s "$targets_file" ]]; then
    echo "Missing or empty targets file: ${targets_file}" >&2
    exit 1
  fi
}

start_sudo_keepalive() {
  if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]] && kill -0 "$SUDO_KEEPALIVE_PID" 2>/dev/null; then
    return 0
  fi

  SUDO_KEEPALIVE_PID=""

  log "acquiring sudo credentials for client netem"
  sudo -v

  (
    while true; do
      sleep "$SUDO_KEEPALIVE_INTERVAL_S"
      sudo -n true >/dev/null 2>&1 || exit 0
    done
  ) &

  SUDO_KEEPALIVE_PID="$!"
  SUDO_KEEPALIVE_OWNED="1"
  export SUDO_KEEPALIVE_PID
}

stop_sudo_keepalive() {
  if [[ "${SUDO_KEEPALIVE_OWNED:-}" != "1" ]]; then
    return 0
  fi

  if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]]; then
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  fi

  SUDO_KEEPALIVE_PID=""
  SUDO_KEEPALIVE_OWNED=""
  unset SUDO_KEEPALIVE_PID
}

set_client_network_delay() {
  local delay="$1"

  start_sudo_keepalive
  log "client netem delay=${delay} iface=${NETEM_IFACE}"
  sudo -n tc qdisc replace dev "$NETEM_IFACE" root netem delay "$delay"
}

add_time_values() {
  python3 "${REPO_ROOT}/scripts_eval/time_values.py" add "$1" "$2"
}

subtract_time_values() {
  python3 "${REPO_ROOT}/scripts_eval/time_values.py" subtract "$1" "$2"
}

client_network_delay_ramp() {
  local start_delay="$1"
  local end_delay="$2"
  local duration="$3"
  local steps="${4:-10}"
  local delay
  local sleep_s

  while read -r delay sleep_s; do
    set_client_network_delay "$delay"
    case "$sleep_s" in
      0|0.0|0.000000) ;;
      *) sleep "$sleep_s" ;;
    esac
  done < <(
    python3 "${REPO_ROOT}/scripts_eval/time_values.py" ramp \
      "$start_delay" \
      "$end_delay" \
      "$duration" \
      "$steps"
  )
}

start_client_network_delay_ramp() {
  CLIENT_NETWORK_RAMP_LAST_PID=""
  client_network_delay_ramp "$@" &
  CLIENT_NETWORK_RAMP_LAST_PID="$!"
  NETWORK_RAMP_PIDS+=("$CLIENT_NETWORK_RAMP_LAST_PID")
}

stop_client_network_delay_ramps() {
  local pid
  for pid in "${NETWORK_RAMP_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  NETWORK_RAMP_PIDS=()
  CLIENT_NETWORK_RAMP_LAST_PID=""
}

clear_client_network_delay() {
  log "client netem clear iface=${NETEM_IFACE}"
  sudo -n tc qdisc del dev "$NETEM_IFACE" root 2>/dev/null || true
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

  require_targets_file "$targets_file"
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
    -http2=false
    -metrics-csv="$metrics_csv"
    -window-csv="$window_csv"
    -window-samples-csv="$samples_csv"
    -output="$results_bin"
  )

  if [[ -n "$reference_csv" ]]; then
    attack_args+=(-reference-csv-path="$reference_csv")
  fi

  attack_args+=("$@")
  "$VEGETA_BIN" "${attack_args[@]}" > "$xlg_log"
  "$VEGETA_BIN" report "$results_bin" > "$report_txt"
  "$VEGETA_BIN" report -type=json "$results_bin" > "$report_json"

  cat > "${case_dir}/run_meta.env" <<EOF
label=${label}
rate=${rate}
duration=${duration}
workers=${workers}
max_workers=${max_workers}
connections=${connections}
max_connections=${max_connections}
reference_csv=${reference_csv}
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
