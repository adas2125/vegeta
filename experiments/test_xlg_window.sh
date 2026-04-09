#!/bin/bash
set -euo pipefail

# This script does two things:
# 1) run a short baseline attack to collect the reference CSV used for rho
# 2) run another attack that emits XLG-WINDOW telemetry and feed it into the
#    Python consumer so we can see the parsed values printed to the terminal

URL="POST http://localhost:8080/"
RATE=10000
DURATION="10s"
DURATION_TEST="20s"
SLEEP_BETWEEN_RUNS=5
WORKERS=200
OUT_DIR="xlg_window_test"
REF_CSV="${OUT_DIR}/reference_rps${RATE}.csv"
REF_SAMPLES_CSV="${OUT_DIR}/reference_samples_rps${RATE}.csv"
REF_RESULTS="${OUT_DIR}/reference_rps${RATE}.bin"
RUN_CSV="${OUT_DIR}/telemetry_rps${RATE}.csv"
RUN_RESULTS="${OUT_DIR}/telemetry_rps${RATE}.bin"
FIFO_PATH="${OUT_DIR}/telemetry.pipe"
CONSUMER_PID=""

mkdir -p "$OUT_DIR"
rm -f "$FIFO_PATH"
mkfifo "$FIFO_PATH"

cleanup() {
  if [[ -n "$CONSUMER_PID" ]]; then
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  rm -f "$FIFO_PATH"
}
trap cleanup EXIT

echo "========================================"
echo "Step 1: Collecting reference CSV"
echo "This gives vegeta a baseline latency for rho and a baseline sample set for EMD."
echo "Reference CSV: $REF_CSV"
echo "Reference samples CSV: $REF_SAMPLES_CSV"
echo "========================================"

echo "$URL" | ./vegeta attack \
  -rate="$RATE" \
  -duration="$DURATION" \
  -workers="$WORKERS" \
  -max-workers="$WORKERS" \
  -output="$REF_RESULTS" \
  -window-csv="$REF_CSV" \
  -window-samples-csv="$REF_SAMPLES_CSV" \
  > /dev/null

echo
echo "========================================"
echo "Baseline vegeta report"
echo "========================================"
./vegeta report "$REF_RESULTS"

echo "Reference CSV collected, sleeping ${SLEEP_BETWEEN_RUNS}s before the next attack..."
sleep "$SLEEP_BETWEEN_RUNS"

echo
echo "========================================"
echo "Step 2: Running another attack and piping XLG-WINDOW telemetry"
echo "into the Python consumer script."
echo "You should see rho plus EMD values for pacer, scheduler, and connection delays below."
echo "========================================"

# Start the consumer in the background, loading the baseline delay samples once.
python3 scripts/consume_xlg_window.py "$REF_SAMPLES_CSV" < "$FIFO_PATH" &
CONSUMER_PID=$!

# Write Vegeta's normal attack results to a file so we can print a report after the run.
# The Go emitter still writes XLG-WINDOW lines to stdout, and those go into the FIFO.
echo "$URL" | ./vegeta -profile="cpu" attack \
  -rate="$RATE" \
  -duration="$DURATION_TEST" \
  -workers="$WORKERS" \
  -max-workers="$WORKERS" \
  -output="$RUN_RESULTS" \
  -window-csv="$RUN_CSV" \
  -reference-csv-path="$REF_CSV" \
  > "$FIFO_PATH"

# Give the consumer a brief moment to print the last payload, then stop it.
sleep 1
kill "$CONSUMER_PID" 2>/dev/null || true
wait "$CONSUMER_PID" 2>/dev/null || true
CONSUMER_PID=""

echo
echo "========================================"
echo "Step 3: Vegeta report for the reference-backed run"
echo "========================================"
./vegeta report "$RUN_RESULTS"

echo
echo "Finished."
echo "Reference CSV: $REF_CSV"
echo "Reference samples: $REF_SAMPLES_CSV"
echo "Reference results: $REF_RESULTS"
echo "Run CSV: $RUN_CSV"
echo "Run results: $RUN_RESULTS"
