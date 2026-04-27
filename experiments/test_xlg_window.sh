#!/usr/bin/env bash
set -euo pipefail

# sets the directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Calibrate the live XLG consumer from a steady regime-shift server, then pipe
# a live reference-backed attack into the same Stage B state machine.

URL="${URL:-GET http://localhost:8080/}"
RATE="${RATE:-2000}"
BASELINE_DURATION="${BASELINE_DURATION:-15s}"
LIVE_DURATION="${LIVE_DURATION:-120s}"
TRIM_S="${TRIM_S:-5.0}"
WINDOW_S="${WINDOW_S:-1}"
NUM_HEALTHY_RUNS="${NUM_HEALTHY_RUNS:-2}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-5}"

# Configurations for the attack
WORKERS="${WORKERS:-150}"
MAX_WORKERS="${MAX_WORKERS:-$WORKERS}"
CONNECTIONS="${CONNECTIONS:-10000}"
MAX_CONNECTIONS="${MAX_CONNECTIONS:-0}"
HTTP2="${HTTP2:-false}"
OUT_ROOT="${OUT_ROOT:-xlg_window_test}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${OUT_DIR:-${OUT_ROOT}/run_${STAMP}}"
STAGE_A_DIR="${OUT_DIR}/stage_a_fixed"
TARGETS_FILE="${OUT_DIR}/targets.txt"
FIFO_PATH="${OUT_DIR}/telemetry.pipe"
CONSUMER_PID=""

# the reference CSV
REFERENCE_DIR="${STAGE_A_DIR}/reference"
REFERENCE_CSV="${REFERENCE_DIR}/window_results_rps${RATE}.csv"

# outputs for the live run and the Stage A analysis
THRESHOLDS_JSON="${STAGE_A_DIR}/stage_a_thresholds.json"
LIVE_DIR="${OUT_DIR}/live"
LIVE_RESULTS="${LIVE_DIR}/results_rps${RATE}.bin"
LIVE_METRICS_CSV="${LIVE_DIR}/metrics_rps${RATE}.csv"
LIVE_WINDOW_CSV="${LIVE_DIR}/window_results_rps${RATE}.csv"
LIVE_SAMPLES_CSV="${LIVE_DIR}/window_samples_rps${RATE}.csv"
LIVE_XLG_LOG="${LIVE_DIR}/xlg_windows_rps${RATE}.log"

mkdir -p "$REFERENCE_DIR" "${STAGE_A_DIR}/healthy" "$LIVE_DIR"
printf '%s\n' "$URL" > "$TARGETS_FILE"
# creates the FIFO for the XLG consumer to read from
rm -f "$FIFO_PATH"; mkfifo "$FIFO_PATH"

cleanup() {
  if [[ -n "$CONSUMER_PID" ]]; then
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  rm -f "$FIFO_PATH"
}
trap cleanup EXIT

run_attack() {
  local run_dir="$1"
  local duration="$2"
  local reference_csv="$3"

  mkdir -p "$run_dir"
  local metrics_csv="${run_dir}/metrics_rps${RATE}.csv"
  local window_csv="${run_dir}/window_results_rps${RATE}.csv"
  local samples_csv="${run_dir}/window_samples_rps${RATE}.csv"
  local results_bin="${run_dir}/results_rps${RATE}.bin"
  local xlg_log="${run_dir}/xlg_windows_rps${RATE}.log"

  local args=(
    attack
    -targets="$TARGETS_FILE"
    -rate="$RATE"
    -duration="$duration"
    -workers="$WORKERS"
    -max-workers="$MAX_WORKERS"
    -connections="$CONNECTIONS"
    -max-connections="$MAX_CONNECTIONS"
    -http2="$HTTP2"
    -metrics-interval="${WINDOW_S}s"
    -metrics-csv="$metrics_csv"
    -window-csv="$window_csv"
    -window-samples-csv="$samples_csv"
    -output="$results_bin"
  )

  if [[ -n "$reference_csv" ]]; then
    args+=(-reference-csv-path="$reference_csv")
  fi

  "${REPO_ROOT}/vegeta" "${args[@]}" > "$xlg_log"
}

echo "========================================"
echo "Step 1: Collecting steady Stage A reference and healthy runs"
echo "========================================"

run_attack "$REFERENCE_DIR" "$BASELINE_DURATION" ""

# run up to NUM_HEALTHY_RUNS healthy runs (default: 2)
for run_idx in $(seq 1 "$NUM_HEALTHY_RUNS"); do
  run_attack \
    "${STAGE_A_DIR}/healthy/run_$(printf '%02d' "$run_idx")" \
    "$BASELINE_DURATION" \
    "$REFERENCE_CSV"
done

printf 'rate=%s\n' "$RATE" > "${STAGE_A_DIR}/run_config.env"
# running the stage A scripts to determine thresholds for the live attack
python3 "${REPO_ROOT}/scripts_eval/stage_a_fixed_counts.py" \
  --stage-a-dir "$STAGE_A_DIR" \
  --trim-s "$TRIM_S"

python3 "${REPO_ROOT}/scripts_eval/stage_a_thresholds.py" \
  --stage-a-dir "$STAGE_A_DIR" \
  --trim-s "$TRIM_S"

echo
echo "========================================"
echo "Baseline calibration complete. Sleeping ${SLEEP_BETWEEN_RUNS}s before the live attack..."
echo "========================================"
sleep "$SLEEP_BETWEEN_RUNS"
echo
echo "========================================"
echo "Step 2: Running live attack"
echo "========================================"

# starts the XLG consumer in the background, consuming from the FIFO and using the Stage A thresholds
python3 "${REPO_ROOT}/scripts/consume_xlg_window.py" \
  --stage-a-thresholds "$THRESHOLDS_JSON" \
  --trim-s "$TRIM_S" \
  < "$FIFO_PATH" &
CONSUMER_PID=$!

# pipes the live attack results into the FIFO for the XLG consumer, while also saving the full output to a log file
"${REPO_ROOT}/vegeta" attack \
  -targets="$TARGETS_FILE" \
  -rate="$RATE" \
  -duration="$LIVE_DURATION" \
  -workers="$WORKERS" \
  -max-workers="$MAX_WORKERS" \
  -connections="$CONNECTIONS" \
  -max-connections="$MAX_CONNECTIONS" \
  -http2="$HTTP2" \
  -metrics-interval="${WINDOW_S}s" \
  -metrics-csv="$LIVE_METRICS_CSV" \
  -window-csv="$LIVE_WINDOW_CSV" \
  -window-samples-csv="$LIVE_SAMPLES_CSV" \
  -output="$LIVE_RESULTS" \
  -reference-csv-path="$REFERENCE_CSV" \
  | tee "$LIVE_XLG_LOG" > "$FIFO_PATH"

wait "$CONSUMER_PID"

echo
echo "========================================"
echo "Step 3: Vegeta report for the live run"
echo "========================================"
"${REPO_ROOT}/vegeta" report "$LIVE_RESULTS"

echo
echo "Finished."
echo "Output: $OUT_DIR"
