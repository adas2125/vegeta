#!/bin/bash
set -euo pipefail

URL="POST http://localhost:8080/"
DURATION="10s"

# RPS bands
RATES=(1000 2000 4000 8000 12000)
FIXED_WORKERS=150

# runs per band
RUNS=15

# cooldowns
RUN_COOLDOWN=10

# output dirs
REFERENCE_DIR="references"
BASELINE_DIR="baseline_ref"

mkdir -p "$REFERENCE_DIR" "$BASELINE_DIR"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}


# --------------------------------------
# Phase 1: Generate per-RPS reference CSVs
# --------------------------------------
for RATE in "${RATES[@]}"; do
  echo "[$(timestamp)] ======================================"
  echo "[$(timestamp)] Generating reference CSV for RATE=$RATE"
  echo "[$(timestamp)] ======================================"

  echo "$URL" | ./vegeta attack \
    -rate="$RATE" \
    -duration="$DURATION" \
    -workers="$FIXED_WORKERS" \
    -max-workers="$FIXED_WORKERS" \
    -window-csv="${REFERENCE_DIR}/rps${RATE}.csv" \
    | ./vegeta report

  echo "[$(timestamp)] Reference run complete for RATE=$RATE"
  echo "[$(timestamp)] Cooling down for ${RUN_COOLDOWN}s"
  sleep "$RUN_COOLDOWN"
done

# --------------------------------------
# Phase 2: Generate baseline runs
# --------------------------------------
for RATE in "${RATES[@]}"; do
  for RUN in $(seq 1 "$RUNS"); do
    echo "[$(timestamp)] ======================================"
    echo "[$(timestamp)] Running baseline RATE=$RATE RUN=$RUN"
    echo "[$(timestamp)] ======================================"

    echo "$URL" | ./vegeta attack \
      -rate="$RATE" \
      -duration="$DURATION" \
      -workers="$FIXED_WORKERS" \
      -max-workers="$FIXED_WORKERS" \
      -window-csv="${BASELINE_DIR}/baseline_rps${RATE}_run${RUN}.csv" \
      -reference-csv-path="${REFERENCE_DIR}/rps${RATE}.csv" \
      | ./vegeta report

    echo "[$(timestamp)] Baseline run complete RATE=$RATE RUN=$RUN"
    echo "[$(timestamp)] Cooling down for ${RUN_COOLDOWN}s"
    sleep "$RUN_COOLDOWN"
  done
done

echo "[$(timestamp)] All baseline runs completed."