#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

DURATION="${DURATION:-15s}"
BASELINE_MEAN_DELAY="${BASELINE_MEAN_DELAY:-${FIXED_DELAY:-10ms}}"
NUM_MILD_RUNS="${NUM_MILD_RUNS:-3}"

STAGE_A_ROOT="${STAGE_A_ROOT:-${OUTPUT_ROOT}/stage_a_fixed}"
STAGE_A_DIR="${STAGE_A_DIR:-$(ls -d "$STAGE_A_ROOT"/run_* | sort | tail -n 1)}"
COUNTS_JSON="${COUNTS_JSON:-${STAGE_A_DIR}/stage_a_counts.json}"
MILD_DIR="${MILD_DIR:-${STAGE_A_DIR}/mild_calibration}"
TARGETS_FILE="${STAGE_A_DIR}/targets.txt"

trap 'stop_cpu_stress; stop_simple_server' EXIT

mkdir -p "$MILD_DIR"

RATE="$(json_value "$COUNTS_JSON" "rate")"
WORKERS_MILD="$(json_value "$COUNTS_JSON" "severity.workers.mild")"
CONNECTIONS_MILD="$(json_value "$COUNTS_JSON" "severity.connections.mild")"
CPU_MILD="$(json_value "$COUNTS_JSON" "severity.cpu.mild")"
REFERENCE_CSV="$(ls "${STAGE_A_DIR}"/reference/window_results_rps*.csv | sort | head -n 1)"

start_simple_server "${MILD_DIR}/server_exp_${BASELINE_MEAN_DELAY}.log" exp "$BASELINE_MEAN_DELAY"

mkdir -p "${MILD_DIR}/cpu_mild"
for run_idx in $(seq 1 "$NUM_MILD_RUNS"); do
  start_cpu_stress "$CPU_MILD" "${MILD_DIR}/cpu_mild/stress_run_$(printf '%02d' "$run_idx").log"
  run_attack_to_dir \
    "${MILD_DIR}/cpu_mild/run_$(printf '%02d' "$run_idx")" \
    "stage_a_cpu_mild_run_${run_idx}" \
    "$RATE" \
    "$DURATION" \
    "$TARGETS_FILE" \
    "$REFERENCE_CSV" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS"
  stop_cpu_stress
done

mkdir -p "${MILD_DIR}/workers_mild"
for run_idx in $(seq 1 "$NUM_MILD_RUNS"); do
  run_attack_to_dir \
    "${MILD_DIR}/workers_mild/run_$(printf '%02d' "$run_idx")" \
    "stage_a_workers_mild_run_${run_idx}" \
    "$RATE" \
    "$DURATION" \
    "$TARGETS_FILE" \
    "$REFERENCE_CSV" \
    "$WORKERS_MILD" \
    "$WORKERS_MILD" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS"
done

mkdir -p "${MILD_DIR}/connections_mild"
for run_idx in $(seq 1 "$NUM_MILD_RUNS"); do
  run_attack_to_dir \
    "${MILD_DIR}/connections_mild/run_$(printf '%02d' "$run_idx")" \
    "stage_a_connections_mild_run_${run_idx}" \
    "$RATE" \
    "$DURATION" \
    "$TARGETS_FILE" \
    "$REFERENCE_CSV" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$CONNECTIONS_MILD" \
    "$CONNECTIONS_MILD"
done

cat > "${MILD_DIR}/run_config.env" <<EOF
stage=stage_a_fixed_mild_calibration
rate=${RATE}
duration=${DURATION}
baseline_mean_delay=${BASELINE_MEAN_DELAY}
num_mild_runs=${NUM_MILD_RUNS}
workers_mild=${WORKERS_MILD}
connections_mild=${CONNECTIONS_MILD}
cpu_mild=${CPU_MILD}
reference_csv=${REFERENCE_CSV}
output_dir=${MILD_DIR}
EOF

log "stage A mild complete: ${MILD_DIR}"
