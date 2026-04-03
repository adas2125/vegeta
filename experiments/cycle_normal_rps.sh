#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-"130.127.134.71"}"
PORT="${PORT:-5000}"
PATH_PREFIX="${PATH_PREFIX:-/}"
DURATION="${DURATION:-10s}"
RUNS="${RUNS:-2}"
COOLDOWN="${COOLDOWN:-10}"
RATES=(1000 2000 3000)
LAST_RATE_INDEX=$((${#RATES[@]} - 1))
LAST_RATE="${RATES[$LAST_RATE_INDEX]}"

# Healthy regime assumptions for this sweep:
# - server delay fixed at 10 ms
# - protocol HTTP/1.1
REGIME_NAME="healthy_HotelReservation_http1"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUTDIR="experiments/out/${REGIME_NAME}_cycle_${STAMP}"

mkdir -p "$OUTDIR"

TARGETS_FILE="targets.txt"


timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

echo "[$(timestamp)] Starting healthy RPS cycle"
echo "[$(timestamp)] Output directory: $OUTDIR"
echo "[$(timestamp)] Duration=${DURATION} runs-per-rps=${RUNS}"

for RATE in "${RATES[@]}"; do
  RATE_DIR="${OUTDIR}/rps_${RATE}"
  mkdir -p "$RATE_DIR"

  for RUN in $(seq 1 "$RUNS"); do
    echo "[$(timestamp)] ======================================"
    echo "[$(timestamp)] Running healthy baseline RATE=${RATE} RUN=${RUN}"
    echo "[$(timestamp)] Saving outputs under ${RATE_DIR}"
    echo "[$(timestamp)] ======================================"

    ./vegeta_local attack \
      -targets="$TARGETS_FILE" \
      -rate="$RATE" \
      -duration="$DURATION" \
      -http2=false \
      -metrics-csv="${RATE_DIR}/metrics_run${RUN}.csv" \
      -window-csv="${RATE_DIR}/window_results_run${RUN}.csv" \
      -window-samples-csv="${RATE_DIR}/window_samples_run${RUN}.csv" \
      -output="${RATE_DIR}/results_run${RUN}.bin"

    ./vegeta_local report -type=json "${RATE_DIR}/results_run${RUN}.bin"

    echo "[$(timestamp)] Completed RATE=${RATE} RUN=${RUN}"

    if [[ "$RUN" -lt "$RUNS" || "$RATE" != "$LAST_RATE" ]]; then
      echo "[$(timestamp)] Cooling down for ${COOLDOWN}s"
      sleep "$COOLDOWN"
    fi
  done
done

echo "[$(timestamp)] All healthy baseline cycles completed."