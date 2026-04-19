#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

DURATION="${DURATION:-15s}"
RATE="${RATE:-${TARGET_RPS:-${EVAL_RATE:-${BASELINE_RPS:-3000}}}}"
NUM_BASELINE_RUNS="${NUM_BASELINE_RUNS:-2}"
BASELINE_MEAN_DELAY="${BASELINE_MEAN_DELAY:-10ms}"

STAMP="$(date +%Y%m%d_%H%M%S)"
STAGE_B_DIR="${STAGE_B_DIR:-${OUTPUT_ROOT}/stage_b_variable/run_${STAMP}}"
TARGETS_FILE="${STAGE_B_DIR}/targets.txt"

trap 'stop_cpu_stress; stop_simple_server' EXIT

mkdir -p "$STAGE_B_DIR"
printf '%s http://%s:%s/\n' "$TARGET_METHOD" "$SERVER_HOST" "$SERVER_PORT" > "$TARGETS_FILE"

start_simple_server "${STAGE_B_DIR}/server_exp_${BASELINE_MEAN_DELAY}.log" exp "$BASELINE_MEAN_DELAY"

mkdir -p "${STAGE_B_DIR}/baseline_healthy"
for run_idx in $(seq 1 "$NUM_BASELINE_RUNS"); do
  run_attack_to_dir \
    "${STAGE_B_DIR}/baseline_healthy/run_$(printf '%02d' "$run_idx")" \
    "stage_b_baseline_run_${run_idx}" \
    "$RATE" \
    "$DURATION" \
    "$TARGETS_FILE" \
    "" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS"
done

cat > "${STAGE_B_DIR}/run_config.env" <<EOF
stage=stage_b_variable_baseline
rate=${RATE}
duration=${DURATION}
baseline_mean_delay=${BASELINE_MEAN_DELAY}
num_baseline_runs=${NUM_BASELINE_RUNS}
output_dir=${STAGE_B_DIR}
EOF

log "stage B baseline complete: ${STAGE_B_DIR}"
