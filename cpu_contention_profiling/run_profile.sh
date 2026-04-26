#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

VEGETA_BIN="${VEGETA_BIN:-${REPO_ROOT}/vegeta}"
TARGETS_FILE="${TARGETS_FILE:-${REPO_ROOT}/targets.txt}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

RPS_LIST="${RPS_LIST:-1000 2000 3000}"
JOB_LIST="${JOB_LIST:-20 40 80 120}"
RUNS_PER_POINT="${RUNS_PER_POINT:-1}"
DURATION="${DURATION:-20s}"

CPU_STRESS_WARMUP="${CPU_STRESS_WARMUP:-2}"
SLEEP_BETWEEN_POINTS="${SLEEP_BETWEEN_POINTS:-2}"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RUN_DIR:-${SCRIPT_DIR}/output/run_${STAMP}}"

STRESS_PIDS=()

log() {
  echo "[$(date +"%Y-%m-%d %H:%M:%S")] $*"
}

stop_cpu_stress() {
  local pid
  for pid in "${STRESS_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  STRESS_PIDS=()
}

start_cpu_stress() {
  local jobs="$1"
  local i

  stop_cpu_stress

  if (( jobs <= 0 )); then
    return 0
  fi

  log "starting ${jobs} background yes jobs"
  for ((i = 0; i < jobs; i++)); do
    yes > /dev/null 2>&1 &
    STRESS_PIDS+=("$!")
  done
  sleep "$CPU_STRESS_WARMUP"
}

cleanup() {
  stop_cpu_stress
}

run_point() {
  local rps="$1"
  local jobs="$2"
  local run_idx="$3"
  local point_dir="${RUN_DIR}/rps_${rps}/jobs_${jobs}/run_$(printf '%02d' "$run_idx")"
  local results_bin="${point_dir}/results_rps${rps}.bin"
  local report_json="${point_dir}/report_rps${rps}.json"
  local attack_log="${point_dir}/attack_rps${rps}.log"

  mkdir -p "$point_dir"

  log "run rps=${rps} jobs=${jobs} run=${run_idx}"
  start_cpu_stress "$jobs"

  "$VEGETA_BIN" attack \
    -targets="$TARGETS_FILE" \
    -rate="$rps" \
    -duration="$DURATION" \
    -keepalive=true \
    -http2=false \
    -output="$results_bin" \
    > "$attack_log"

  "$VEGETA_BIN" report -type=json "$results_bin" > "$report_json"

  stop_cpu_stress
  sleep "$SLEEP_BETWEEN_POINTS"
}

write_run_config() {
  cat > "${RUN_DIR}/run_config.env" <<EOF
vegeta_bin=${VEGETA_BIN}
targets_file=${TARGETS_FILE}
rps_list=${RPS_LIST}
job_list=${JOB_LIST}
runs_per_point=${RUNS_PER_POINT}
duration=${DURATION}
cpu_stress_warmup=${CPU_STRESS_WARMUP}
sleep_between_points=${SLEEP_BETWEEN_POINTS}
EOF
}

main() {
  local rps
  local jobs
  local run_idx

  trap cleanup EXIT INT TERM

  mkdir -p "$RUN_DIR"
  write_run_config

  log "profiling output: ${RUN_DIR}"

  for rps in $RPS_LIST; do
    for jobs in $JOB_LIST; do
      for run_idx in $(seq 1 "$RUNS_PER_POINT"); do
        run_point "$rps" "$jobs" "$run_idx"
      done
    done
  done

  "$PYTHON_BIN" "${SCRIPT_DIR}/select_jobs.py" --run-dir "$RUN_DIR"
  log "done: ${RUN_DIR}/cpu_jobs.json"
}

main "$@"
