#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

trap 'clear_client_network_delay; stop_sudo_keepalive' EXIT

PYTHON_BIN="${PYTHON_BIN:-python3}"
STAMP="$(date +%Y%m%d_%H%M%S)"

# setting the RPS variables (baseline and target are separate, usually baseline <= target)
BASELINE_RPS="${BASELINE_RPS:-${RATE:-}}"
TARGET_RPS="${TARGET_RPS:-${EVAL_RATE:-}}"
# usually set to the target RPS
STAGE_B_RPS="${STAGE_B_RPS:-${TARGET_RPS:-${BASELINE_RPS:-}}}"

# creating output directories
STAGE_A_DIR="${STAGE_A_DIR:-${OUTPUT_ROOT}/stage_a_fixed/run_${STAMP}}"
STAGE_B_DIR="${STAGE_B_DIR:-${OUTPUT_ROOT}/stage_b_variable/run_${STAMP}}"
export OUTPUT_ROOT STAGE_A_DIR STAGE_B_DIR
start_sudo_keepalive

run_with_optional_rate() {
  local rate="$1"
  shift
  if [[ -n "$rate" ]]; then
    RATE="$rate" "$@"
  else
    "$@"
  fi
}

run_with_optional_eval_rate() {
  local rate="$1"
  shift
  if [[ -n "$rate" ]]; then
    EVAL_RATE="$rate" "$@"
  else
    "$@"
  fi
}

log "stage A healthy"
# for stage a, we run the healthy configuration at the baseline RPS
run_with_optional_rate "$BASELINE_RPS" "${SCRIPT_DIR}/run_stage_a_healthy.sh"
log "stage A counts"
"$PYTHON_BIN" "${REPO_ROOT}/scripts_eval/stage_a_fixed_counts.py" \
  --stage-a-dir "$STAGE_A_DIR"
log "stage A thresholds"
"$PYTHON_BIN" "${REPO_ROOT}/scripts_eval/stage_a_thresholds.py" \
  --stage-a-dir "$STAGE_A_DIR"

log "stage B baseline"
run_with_optional_rate "$STAGE_B_RPS" "${SCRIPT_DIR}/run_stage_b_baseline.sh"
log "stage B fault settings"
"$PYTHON_BIN" "${REPO_ROOT}/scripts_eval/stage_b_reference.py" \
  --stage-b-dir "$STAGE_B_DIR"
log "stage B conditions"
run_with_optional_eval_rate "$TARGET_RPS" "${SCRIPT_DIR}/run_stage_b_conditions.sh"
log "stage B evaluation"
"$PYTHON_BIN" "${REPO_ROOT}/scripts_eval/stage_b_evaluate.py" \
  --stage-b-dir "$STAGE_B_DIR" \
  --stage-a-thresholds "${STAGE_A_DIR}/stage_a_thresholds.json"

log "pipeline complete"
echo "Stage A: ${STAGE_A_DIR}"
echo "Stage B: ${STAGE_B_DIR}"
