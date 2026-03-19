#!/usr/bin/env bash
set -euo pipefail

# USAGE: ./run_exp.sh > sweep.log 2>&1 & echo $! > sweep.pid

HOST=localhost
PORT=8080
OUT_DIR=out
DURATION=30s
SAMPLING_INTERVAL=10ms
DELAY=0ms

mkdir -p "$OUT_DIR"

for rps in 500 1000 2000 4000 8000 16000 64000
do

    echo "========================================"
    echo "Running vegeta with -rate $rps"
    echo "========================================"

    metrics_file="$OUT_DIR/metrics_rps_${rps}_delay_${DELAY}.csv"
    window_file="$OUT_DIR/window_results_rps_${rps}_delay_${DELAY}.csv"
    echo "GET http://$HOST:$PORT/" | ./vegeta attack -rate=$rps -duration=$DURATION -workers=1 --metrics-csv="$metrics_file" --window-csv="$window_file" --metrics-interval=$SAMPLING_INTERVAL | ./vegeta report -type=json > "$OUT_DIR/report_rps_${rps}_delay_${DELAY}.json"

    echo
    echo "Sleeping 5 seconds before next run..."
    sleep 5
done
