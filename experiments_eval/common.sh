#!/usr/bin/env bash

# Shared helpers for the XLG Inspector evaluation scripts.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_ROOT="${OUTPUT_ROOT:-${SCRIPT_DIR}/output}"

VEGETA_BIN="${VEGETA_BIN:-${REPO_ROOT}/vegeta}"
TARGETS_FILE="${TARGETS_FILE:-${REPO_ROOT}/targets.txt}"
VEGETA_LOGICAL_CPUS="${VEGETA_LOGICAL_CPUS:-4}"
WINDOW_S="${WINDOW_S:-1}"
WINDOW_SAMPLE_RETENTION="${WINDOW_SAMPLE_RETENTION:-1.0}"

SERVER_HOST="${SERVER_HOST:-}"
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
    echo "Generate one with: python3 scripts/generate_targets.py --output targets.txt" >&2
    exit 1
  fi
}

target_host() {
  local targets_file="${1:-$TARGETS_FILE}"

  if [[ -n "$SERVER_HOST" ]]; then
    echo "$SERVER_HOST"
    return 0
  fi

  require_targets_file "$targets_file"
  python3 - "$targets_file" <<'PY'
import sys
from urllib.parse import urlparse

targets_file = sys.argv[1]
with open(targets_file, encoding="utf-8") as handle:
    for raw_line in handle:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue
        host = urlparse(parts[1]).hostname
        if host:
            print(host)
            sys.exit(0)

print(f"Could not infer target host from {targets_file}; set SERVER_HOST or NETEM_IFACE.", file=sys.stderr)
sys.exit(1)
PY
}

client_netem_iface() {
  local host="${1:-}"

  if [[ -n "$NETEM_IFACE" ]]; then
    echo "$NETEM_IFACE"
    return 0
  fi

  if ! command -v ip >/dev/null 2>&1; then
    echo "ip command not found; set NETEM_IFACE explicitly." >&2
    exit 1
  fi

  if [[ -z "$host" ]]; then
    host="$(target_host)"
  fi

  ip route get "$host" | awk '{
    for (i = 1; i <= NF; i++) {
      if ($i == "dev") {
        print $(i + 1)
        exit
      }
    }
  }'
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
  local host
  local iface

  if [[ -n "$NETEM_IFACE" ]]; then
    iface="$NETEM_IFACE"
  else
    host="$(target_host)"
    iface="$(client_netem_iface "$host")"
  fi

  if [[ -z "$iface" ]]; then
    echo "Could not infer client network interface for target host ${host}; set NETEM_IFACE." >&2
    exit 1
  fi

  start_sudo_keepalive
  log "client netem delay=${delay} iface=${iface}"
  sudo -n tc qdisc replace dev "$iface" root netem delay "$delay"
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
  local iface

  iface="$(client_netem_iface 2>/dev/null || true)"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  log "client netem clear iface=${iface}"
  sudo -n tc qdisc del dev "$iface" root 2>/dev/null || true
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
