#!/bin/bash
set -euo pipefail

URL="POST http://localhost:8080/"
DURATION="30s"
WORKERS=15000
BASELINE_REF="baseline_plots/baseline_reference.csv"

# Seen (baseline anchor) RPS
GOOD_RATES=(1000 4000 10000)
BAD_RATES=(16000 20000) 

RUNS=2
COOLDOWN=10

OUTDIR="eval"
mkdir -p "$OUTDIR"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

run_eval() {
  local RATE=$1
  local RUN=$2
  local TAG=$3

  echo "[$(timestamp)] ======================================"
  echo "[$(timestamp)] Running $TAG RPS=$RATE RUN=$RUN"
  echo "[$(timestamp)] ======================================"

  echo "$URL" | ./vegeta attack \
    -rate=$RATE \
    -duration=$DURATION \
    -workers=$WORKERS \
    -max-workers=$WORKERS \
    -window-csv="${OUTDIR}/${TAG}_rps${RATE}_run${RUN}.csv" \
    -baseline-reference-csv="$BASELINE_REF" \
    | ./vegeta report

  echo "[$(timestamp)] Done $TAG RPS=$RATE RUN=$RUN"
  echo "[$(timestamp)] Cooling down ${COOLDOWN}s"
  sleep $COOLDOWN
}

# # ------------------------
# # GOOD RPS evaluation
# # ------------------------
# for RATE in "${GOOD_RATES[@]}"; do
#   for RUN in $(seq 1 $RUNS); do
#     run_eval "$RATE" "$RUN" "seen"
#   done
# done

# ------------------------
# BAD RPS evaluation
# ------------------------
for RATE in "${BAD_RATES[@]}"; do
  for RUN in $(seq 1 $RUNS); do
    run_eval "$RATE" "$RUN" "unseen"
  done
done

echo "[$(timestamp)] All evaluation runs completed."