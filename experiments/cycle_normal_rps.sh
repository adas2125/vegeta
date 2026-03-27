#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-localhost}"
PORT="${PORT:-8080}"
METHOD="${METHOD:-POST}"
PATH_PREFIX="${PATH_PREFIX:-/}"
DURATION="${DURATION:-30s}"
WORKERS="${WORKERS:-200}"
MAX_WORKERS="${MAX_WORKERS:-200}"
RUNS="${RUNS:-2}"
COOLDOWN="${COOLDOWN:-10}"
RATES=(4000 6000 8000 10000 12000)
LAST_RATE_INDEX=$((${#RATES[@]} - 1))
LAST_RATE="${RATES[$LAST_RATE_INDEX]}"

# Healthy regime assumptions for this sweep:
# - server delay fixed at 10 ms
# - protocol HTTP/1.1
REGIME_NAME="healthy_fixed10ms_http1"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUTDIR="experiments/out/${REGIME_NAME}_cycle_${STAMP}"

mkdir -p "$OUTDIR"

TARGETS_FILE="$(mktemp)"
printf '%s http://%s:%s%s\n' "$METHOD" "$HOST" "$PORT" "$PATH_PREFIX" > "$TARGETS_FILE"

cleanup() {
  rm -f "$TARGETS_FILE"
}
trap cleanup EXIT

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

echo "[$(timestamp)] Starting healthy RPS cycle"
echo "[$(timestamp)] Output directory: $OUTDIR"
echo "[$(timestamp)] Assumed server regime: fixed 10 ms delay, HTTP/1.1"
echo "[$(timestamp)] Target: ${METHOD} http://${HOST}:${PORT}${PATH_PREFIX}"
echo "[$(timestamp)] Duration=${DURATION} workers=${WORKERS} max-workers=${MAX_WORKERS} runs-per-rps=${RUNS}"

for RATE in "${RATES[@]}"; do
  RATE_DIR="${OUTDIR}/rps_${RATE}"
  mkdir -p "$RATE_DIR"

  for RUN in $(seq 1 "$RUNS"); do
    echo "[$(timestamp)] ======================================"
    echo "[$(timestamp)] Running healthy baseline RATE=${RATE} RUN=${RUN}"
    echo "[$(timestamp)] Saving outputs under ${RATE_DIR}"
    echo "[$(timestamp)] ======================================"

    ./vegeta attack \
      -targets="$TARGETS_FILE" \
      -rate="$RATE" \
      -duration="$DURATION" \
      -workers="$WORKERS" \
      -max-workers="$MAX_WORKERS" \
      -http2=false \
      -metrics-csv="${RATE_DIR}/metrics_run${RUN}.csv" \
      -window-csv="${RATE_DIR}/window_results_run${RUN}.csv" \
      -window-samples-csv="${RATE_DIR}/window_samples_run${RUN}.csv" \
      -output="${RATE_DIR}/results_run${RUN}.bin"

    ./vegeta report -type=json "${RATE_DIR}/results_run${RUN}.bin"

    echo "[$(timestamp)] Completed RATE=${RATE} RUN=${RUN}"

    if [[ "$RUN" -lt "$RUNS" || "$RATE" != "$LAST_RATE" ]]; then
      echo "[$(timestamp)] Cooling down for ${COOLDOWN}s"
      sleep "$COOLDOWN"
    fi
  done
done

echo "[$(timestamp)] All healthy baseline cycles completed."
