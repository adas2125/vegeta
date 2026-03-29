#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_HOST="${TARGET_HOST:-localhost}"
TARGET_PORT="${TARGET_PORT:-5001}"
TARGET_BASE_URL="${TARGET_BASE_URL:-http://${TARGET_HOST}:${TARGET_PORT}}"
TARGETS_FILE="${REPO_ROOT}/targets.txt"
TARGET_GENERATOR="${REPO_ROOT}/scripts/generate_targets.py"
TARGET_COUNT="${TARGET_COUNT:-1800000}"
TARGET_SEED="${TARGET_SEED:-42}"
REF_DIR=refs_new
SAMPLES_DIR=samples_new
DURATION=15s
RPS=3000
NUM_YES=100

# Settling times
SLEEP_BETWEEN_RUNS=5
SLEEP_AFTER_YES_START=5
SLEEP_AFTER_YES_STOP=8

mkdir -p "$REF_DIR" "$SAMPLES_DIR"

if [[ ! -f "$TARGET_GENERATOR" ]]; then
  echo "Missing target generator script: $TARGET_GENERATOR" >&2
  exit 1
fi

echo "Generating targets file..."
echo "Base URL: $TARGET_BASE_URL"
python3 "$TARGET_GENERATOR" \
  --base-url "$TARGET_BASE_URL" \
  --count "$TARGET_COUNT" \
  --seed "$TARGET_SEED" \
  --output "$TARGETS_FILE"

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

  ./vegeta attack \
    -targets="$TARGETS_FILE" \
    -rate="$RPS" \
    -duration="$DURATION" \
    "$@" \
  | ./vegeta report

  echo
  echo "Finished: $label"
  echo "Sleeping ${SLEEP_BETWEEN_RUNS}s to let things settle..."
  sleep "$SLEEP_BETWEEN_RUNS"
}

# 1) Reference CSV for baseline latency under healthy conditions
run_attack "reference baseline" \
  -workers=150 \
  -max-workers=150 \
  -window-csv="${REF_DIR}/eval_rps${RPS}.csv"

# 2) Normal reference run
run_attack "normal" \
  -workers=150 \
  -max-workers=150 \
  -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
  -window-samples-csv="${SAMPLES_DIR}/window_samples_normal.csv" \
  -window-csv="${SAMPLES_DIR}/window_results_normal.csv"

# 3) Too few workers
run_attack "few workers" \
  -workers=110 \
  -max-workers=110 \
  -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
  -window-samples-csv="${SAMPLES_DIR}/window_samples_few_workers.csv" \
  -window-csv="${SAMPLES_DIR}/window_results_few_workers.csv"

# 4) Too few connections
run_attack "few connections" \
  -workers=150 \
  -max-workers=150 \
  -max-connections=40 \
  -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
  -window-samples-csv="${SAMPLES_DIR}/window_samples_few_conns.csv" \
  -window-csv="${SAMPLES_DIR}/window_results_few_conns.csv"

# 5) CPU contention
echo
echo "Starting ${NUM_YES} background yes processes..."
for i in $(seq 1 "$NUM_YES"); do
  yes > /dev/null &
done

echo "Sleeping ${SLEEP_AFTER_YES_START}s to let CPU contention build..."
sleep "$SLEEP_AFTER_YES_START"

run_attack "cpu contention" \
  -workers=150 \
  -max-workers=150 \
  -reference-csv-path="${REF_DIR}/eval_rps${RPS}.csv" \
  -window-samples-csv="${SAMPLES_DIR}/window_samples_cpu_contention.csv" \
  -window-csv="${SAMPLES_DIR}/window_results_cpu_contention.csv"

echo "Stopping background yes processes..."
pkill -x yes || true

echo "Sleeping ${SLEEP_AFTER_YES_STOP}s to let the machine recover..."
sleep "$SLEEP_AFTER_YES_STOP"

echo
echo "All experiments complete."
