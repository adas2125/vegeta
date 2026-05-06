#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

HOST="127.0.0.1"
PORT="18080"
RATE="2000"
DURATION="16s"
WARMUP_SECONDS="4"
STALL_DURATION="5s"
DELAYS=(5ms 10ms 20ms 30ms)
CAPS=(80 120 200 400 800)
ADAPTIVE_WORKERS="10"
CAPPED_WORKERS="10"

VEGETA_BIN="${REPO_ROOT}/vegeta"
SERVER_BIN="${REPO_ROOT}/manual_stall_server"
OUT_DIR="${SCRIPT_DIR}/runs"
BASE_URL="http://${HOST}:${PORT}"

start_stall() {
  curl -fsS -X POST "${BASE_URL}/start-stall" >/dev/null
}

run_case() {
  local delay="$1"
  local label="$2"
  local workers="$3"
  local max_workers="$4"
  local delay_label="${delay/./_}"
  local case_dir="${OUT_DIR}/delay_${delay_label}/${label}"
  local targets_file="${case_dir}/targets.txt"
  local server_log="${case_dir}/server.log"

  rm -rf "${case_dir}"
  mkdir -p "${case_dir}"
  printf "GET %s/\n" "${BASE_URL}" > "${targets_file}"

  # starting the server
  echo "[server ${label} delay=${delay}]"
  "${SERVER_BIN}" \
    -addr ":${PORT}" \
    -delay "${delay}" \
    -stall-duration "${STALL_DURATION}" \
    > "${server_log}" 2>&1 &
  local server_pid="$!"

  # sleep a bit to allow for startup
  sleep 1.0

  local attack_cmd=(
    "${VEGETA_BIN}" attack
    -targets="${targets_file}"
    -rate="${RATE}/s"
    -duration="${DURATION}"
    -workers="${workers}"
    -keepalive=true
    -http2=false
    -xlg-inspector=false
    -output=/dev/null
  )
  if [[ -n "${max_workers}" ]]; then
    attack_cmd+=(-max-workers="${max_workers}")
  fi

  echo "[attack ${label} delay=${delay} workers=${workers} max_workers=${max_workers:-adaptive}]"
  "${attack_cmd[@]}" > "${case_dir}/vegeta.stdout" 2> "${case_dir}/vegeta.stderr" &
  local attack_pid="$!"

  sleep "${WARMUP_SECONDS}"
  start_stall

  set +e
  wait "${attack_pid}"
  local attack_status="$?"
  kill "${server_pid}" 2>/dev/null
  wait "${server_pid}" 2>/dev/null
  set -e

  if [[ "${attack_status}" -ne 0 ]]; then
    echo "attack failed for ${label} delay=${delay}; see ${case_dir}/vegeta.stderr" >&2
    exit "${attack_status}"
  fi
}

mkdir -p "${OUT_DIR}"

for delay in "${DELAYS[@]}"; do
  run_case "${delay}" "adaptive" "${ADAPTIVE_WORKERS}" ""
  for cap in "${CAPS[@]}"; do
    run_case "${delay}" "cap_${cap}" "${CAPPED_WORKERS}" "${cap}"
  done
done

echo "Wrote raw logs under ${OUT_DIR}"
