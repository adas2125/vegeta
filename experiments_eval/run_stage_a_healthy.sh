#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# load functions and variables from common.sh
source "${SCRIPT_DIR}/common.sh"

# these are the defaults
DURATION="${DURATION:-15s}"
RATE="${RATE:-${BASELINE_RPS:-4000}}"
NUM_HEALTHY_RUNS="${NUM_HEALTHY_RUNS:-2}"
BASELINE_MEAN_DELAY="${BASELINE_MEAN_DELAY:-${FIXED_DELAY:-10ms}}"

# creating output directory for this experiment
STAMP="$(date +%Y%m%d_%H%M%S)"
STAGE_A_DIR="${STAGE_A_DIR:-${OUTPUT_ROOT}/stage_a_fixed/run_${STAMP}}"

# storing the payload for vegeta
TARGETS_FILE="${STAGE_A_DIR}/targets.txt"

# when this exits, we want to stop the server and any stress processes
trap 'stop_cpu_stress; stop_simple_server' EXIT

mkdir -p "$STAGE_A_DIR"
printf '%s http://%s:%s/\n' "$TARGET_METHOD" "$SERVER_HOST" "$SERVER_PORT" > "$TARGETS_FILE"

# Start test server with exponential delay; log output.
start_simple_server "${STAGE_A_DIR}/server_exp_${BASELINE_MEAN_DELAY}.log" exp "$BASELINE_MEAN_DELAY"

# using healthy resources, create the reference CSV
run_attack_to_dir \
  "${STAGE_A_DIR}/reference" \
  "stage_a_reference" \
  "$RATE" \
  "$DURATION" \
  "$TARGETS_FILE" \
  "" \
  "$HEALTHY_WORKERS" \
  "$HEALTHY_MAX_WORKERS" \
  "$HEALTHY_CONNECTIONS" \
  "$HEALTHY_MAX_CONNECTIONS"

# path to the reference CSV, which will be used for the healthy runs
REFERENCE_CSV="${STAGE_A_DIR}/reference/window_results_rps${RATE}.csv"

# running multiple healthy runs
mkdir -p "${STAGE_A_DIR}/healthy"
for run_idx in $(seq 1 "$NUM_HEALTHY_RUNS"); do
  # running the attack with the healthy configuration, and storing results in a separate directory for each run
  run_attack_to_dir \
    "${STAGE_A_DIR}/healthy/run_$(printf '%02d' "$run_idx")" \
    "stage_a_healthy_run_${run_idx}" \
    "$RATE" \
    "$DURATION" \
    "$TARGETS_FILE" \
    "$REFERENCE_CSV" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS"
done

# saving experiment configuration for stage A in the output directory
cat > "${STAGE_A_DIR}/run_config.env" <<EOF
stage=stage_a_fixed
rate=${RATE}
duration=${DURATION}
baseline_mean_delay=${BASELINE_MEAN_DELAY}
num_healthy_runs=${NUM_HEALTHY_RUNS}
reference_csv=${REFERENCE_CSV}
output_dir=${STAGE_A_DIR}
EOF

log "stage A healthy complete: ${STAGE_A_DIR}"
