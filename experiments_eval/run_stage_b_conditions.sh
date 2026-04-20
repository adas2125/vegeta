#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# default parameters
DURATION="${DURATION:-15s}"
NUM_EVAL_RUNS="${NUM_EVAL_RUNS:-1}"
STAGE_B_SEVERITIES="${STAGE_B_SEVERITIES:-mild mod severe}"

# getting the latest stage A and stage B runs
STAGE_A_ROOT="${STAGE_A_ROOT:-${OUTPUT_ROOT}/stage_a_fixed}"
STAGE_A_DIR="${STAGE_A_DIR:-$(ls -d "$STAGE_A_ROOT"/run_* | sort | tail -n 1)}"
STAGE_B_ROOT="${STAGE_B_ROOT:-${OUTPUT_ROOT}/stage_b_variable}"
STAGE_B_DIR="${STAGE_B_DIR:-$(ls -d "$STAGE_B_ROOT"/run_* | sort | tail -n 1)}"
STAGE_B_SETTINGS_JSON="${STAGE_B_SETTINGS_JSON:-${REFERENCE_JSON:-${STAGE_B_DIR}/stage_b_reference.json}}"
CONDITIONS_DIR="${CONDITIONS_DIR:-${STAGE_B_DIR}/conditions}"
TARGETS_FILE="${STAGE_B_DIR}/targets.txt"

trap 'stop_cpu_stress; clear_client_network_delay' EXIT

mkdir -p "$CONDITIONS_DIR"
write_targets_file "$TARGETS_FILE"

# obtaining the rate from the settings json
RATE="$(json_value "$STAGE_B_SETTINGS_JSON" "rate")"
EVAL_RATE="${EVAL_RATE:-${TARGET_RPS:-$RATE}}"

# obtaining the reference csv from stage A
STAGE_A_REFERENCE_CSV="${STAGE_A_REFERENCE_CSV:-$(ls "${STAGE_A_DIR}"/reference/window_results_rps*.csv | sort | head -n 1)}"

run_case() {
  # encapsulates the logic to run a single case with the given parameters, including optional CPU stress
  local base_dir="$1"
  local label="$2"
  local workers="$3"
  local max_workers="$4"
  local connections="$5"
  local max_connections="$6"
  local cpu_jobs="${7:-0}"

  mkdir -p "$base_dir"
  for run_idx in $(seq 1 "$NUM_EVAL_RUNS"); do
  # if CPU stress is requested, start it before the attack and stop it after
    if (( cpu_jobs > 0 )); then
      start_cpu_stress "$cpu_jobs" "${base_dir}/stress_run_$(printf '%02d' "$run_idx").log"
    fi
    # run the attack with reference to the stage A results and the specified parameters
    run_attack_to_dir \
      "${base_dir}/run_$(printf '%02d' "$run_idx")" \
      "${label}_run_${run_idx}" \
      "$EVAL_RATE" \
      "$DURATION" \
      "$TARGETS_FILE" \
      "$STAGE_A_REFERENCE_CSV" \
      "$workers" \
      "$max_workers" \
      "$connections" \
      "$max_connections"
      # after the attack completes, stop the CPU stress if it was started
    if (( cpu_jobs > 0 )); then
      stop_cpu_stress
    fi
  done
}

# run the normal and resource-fault conditions first
set_client_network_delay 5ms
run_case \
  "${CONDITIONS_DIR}/NORMAL" \
  "stage_b_NORMAL" \
  "$HEALTHY_WORKERS" \
  "$HEALTHY_MAX_WORKERS" \
  "$HEALTHY_CONNECTIONS" \
  "$HEALTHY_MAX_CONNECTIONS"

# run CPU contention conditions with varying severity
for severity in $STAGE_B_SEVERITIES; do
  run_case \
    "${CONDITIONS_DIR}/CPU_CONTENTION/${severity}" \
    "stage_b_CPU_CONTENTION_${severity}" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS" \
    "$(json_value "$STAGE_B_SETTINGS_JSON" "severity.cpu.${severity}")"
done

# run worker condition with varying severity
for severity in $STAGE_B_SEVERITIES; do
  workers="$(json_value "$STAGE_B_SETTINGS_JSON" "severity.workers.${severity}")"
  run_case \
    "${CONDITIONS_DIR}/FEW_WORKERS/${severity}" \
    "stage_b_FEW_WORKERS_${severity}" \
    "$workers" \
    "$workers" \
    "$HEALTHY_CONNECTIONS" \
    "$HEALTHY_MAX_CONNECTIONS"
done

# run connection condition with varying severity
for severity in $STAGE_B_SEVERITIES; do
  connections="$(json_value "$STAGE_B_SETTINGS_JSON" "severity.connections.${severity}")"
  run_case \
    "${CONDITIONS_DIR}/FEW_CONNECTIONS/${severity}" \
    "stage_b_FEW_CONNECTIONS_${severity}" \
    "$HEALTHY_WORKERS" \
    "$HEALTHY_MAX_WORKERS" \
    "$connections" \
    "$connections"
done

# run degraded with higher client-side network latency
set_client_network_delay 10ms
run_case \
  "${CONDITIONS_DIR}/SUT_DEGRADED" \
  "stage_b_SUT_DEGRADED" \
  "$HEALTHY_WORKERS" \
  "$HEALTHY_MAX_WORKERS" \
  "$HEALTHY_CONNECTIONS" \
  "$HEALTHY_MAX_CONNECTIONS"

# run faster with client-side network latency removed
clear_client_network_delay
run_case \
  "${CONDITIONS_DIR}/SUT_FASTER" \
  "stage_b_SUT_FASTER" \
  "$HEALTHY_WORKERS" \
  "$HEALTHY_MAX_WORKERS" \
  "$HEALTHY_CONNECTIONS" \
  "$HEALTHY_MAX_CONNECTIONS"

# save the configuration for this stage in the conditions directory for reference
cat > "${CONDITIONS_DIR}/run_config.env" <<EOF
stage=stage_b_variable_conditions
rate=${RATE}
eval_rate=${EVAL_RATE}
duration=${DURATION}
num_eval_runs=${NUM_EVAL_RUNS}
stage_b_severities=${STAGE_B_SEVERITIES}
server_host=${SERVER_HOST}
server_port=${SERVER_PORT}
target_url=$(sut_target_url)
normal_network_delay=5ms
degraded_network_delay=10ms
faster_network_delay=0ms
stage_a_dir=${STAGE_A_DIR}
stage_a_reference_csv=${STAGE_A_REFERENCE_CSV}
stage_b_settings_json=${STAGE_B_SETTINGS_JSON}
output_dir=${CONDITIONS_DIR}
EOF

log "stage B conditions complete: ${CONDITIONS_DIR}"
