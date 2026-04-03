#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_HOST="${TARGET_HOST:-"130.127.134.71"}"
TARGET_PORT="${TARGET_PORT:-5000}"
TARGET_BASE_URL="${TARGET_BASE_URL:-http://${TARGET_HOST}:${TARGET_PORT}}"
TARGETS_FILE="${REPO_ROOT}/targets.txt"
# TARGET_GENERATOR="${REPO_ROOT}/scripts/generate_targets.py"
# TARGET_COUNT="${TARGET_COUNT:-1800000}"
# TARGET_SEED="${TARGET_SEED:-42}"
VEGETA_BIN="${VEGETA_BIN:-}"
REF_DIR=refs_DSB_new_interop
SAMPLES_DIR=samples_DSB_new_interop
DURATION=15s
RPS=2000
NUM_YES=1000
NUM_RUNS="${NUM_RUNS:-5}"

# Settling times
SLEEP_BETWEEN_RUNS=5
SLEEP_AFTER_YES_START=5
SLEEP_AFTER_YES_STOP=8

mkdir -p "$REF_DIR" "$SAMPLES_DIR"

if [[ -z "$VEGETA_BIN" ]]; then
  if [[ -x "${REPO_ROOT}/vegeta" ]]; then
    VEGETA_BIN="${REPO_ROOT}/vegeta"
  elif [[ -x "${REPO_ROOT}/vegeta_local" ]]; then
    VEGETA_BIN="${REPO_ROOT}/vegeta_local"
  fi
fi

if [[ -z "$VEGETA_BIN" || ! -x "$VEGETA_BIN" ]]; then
  echo "Missing vegeta binary. Expected one of:" >&2
  echo "  - ${REPO_ROOT}/vegeta" >&2
  echo "  - ${REPO_ROOT}/vegeta_local" >&2
  echo "Build/setup it first, e.g.: ./scripts/setup_vegeta.sh" >&2
  exit 1
fi

echo "Using vegeta binary: $VEGETA_BIN"

# if [[ ! -f "$TARGET_GENERATOR" ]]; then
#   echo "Missing target generator script: $TARGET_GENERATOR" >&2
#   exit 1
# fi

# echo "Generating targets file..."
# echo "Base URL: $TARGET_BASE_URL"
# python3 "$TARGET_GENERATOR" \
#   --base-url "$TARGET_BASE_URL" \
#   --count "$TARGET_COUNT" \
#   --seed "$TARGET_SEED" \
#   --output "$TARGETS_FILE"

cleanup() {
  pkill -x yes || true
}
trap cleanup EXIT

run_attack() {
  local label="$1"
  shift

  echo
  echo "============================================================"
  echo "Running: $label"
  echo "============================================================"
  echo "Targets: $TARGETS_FILE"

  "$VEGETA_BIN" attack \
    -targets="$TARGETS_FILE" \
    -rate="$RPS" \
    -duration="$DURATION" \
    "$@" \
  | "$VEGETA_BIN" report

  echo
  echo "Finished: $label"
  echo "Sleeping ${SLEEP_BETWEEN_RUNS}s to let things settle..."
  sleep "$SLEEP_BETWEEN_RUNS"
}

# 1) Reference CSV for baseline latency under healthy conditions (run once)
run_attack "reference baseline" \
  -window-csv="${REF_DIR}/eval_rps${RPS}.csv" \
  -window-samples-csv="${REF_DIR}/eval_samples_rps${RPS}.csv"

# Run the experiments NUM_RUNS times with indexed outputs
for run_idx in $(seq 1 "$NUM_RUNS"); do
  echo
  echo "========================================"
  echo "Starting run $run_idx of $NUM_RUNS"
  echo "========================================"

  # 2) Normal reference run
  run_attack "normal (run $run_idx)" \
    -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
    -window-samples-csv="${SAMPLES_DIR}/window_samples_normal_run${run_idx}.csv" \
    -window-csv="${SAMPLES_DIR}/window_results_normal_run${run_idx}.csv"

  # 3) Too few workers
  run_attack "few workers (run $run_idx)" \
    -workers=5 \
    -max-workers=5 \
    -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
    -window-samples-csv="${SAMPLES_DIR}/window_samples_few_workers_run${run_idx}.csv" \
    -window-csv="${SAMPLES_DIR}/window_results_few_workers_run${run_idx}.csv"

  # 4) Too few connections
  run_attack "few connections (run $run_idx)" \
    -max-connections=10 \
    -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
    -window-samples-csv="${SAMPLES_DIR}/window_samples_few_conns_run${run_idx}.csv" \
    -window-csv="${SAMPLES_DIR}/window_results_few_conns_run${run_idx}.csv"

  # 5) CPU contention
  echo
  echo "Starting ${NUM_YES} background yes processes..."
  for i in $(seq 1 "$NUM_YES"); do
    yes > /dev/null &
  done

  echo "Sleeping ${SLEEP_AFTER_YES_START}s to let CPU contention build..."
  sleep "$SLEEP_AFTER_YES_START"

  run_attack "cpu contention (run $run_idx)" \
    -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
    -window-samples-csv="${SAMPLES_DIR}/window_samples_cpu_contention_run${run_idx}.csv" \
    -window-csv="${SAMPLES_DIR}/window_results_cpu_contention_run${run_idx}.csv"

  echo "Stopping background yes processes..."
  pkill -x yes || true

  echo "Sleeping ${SLEEP_AFTER_YES_STOP}s to let the machine recover..."
  sleep "$SLEEP_AFTER_YES_STOP"
done

echo
echo "All experiments complete ($NUM_RUNS runs)."