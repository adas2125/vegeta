#!/usr/bin/env bash
set -euo pipefail

# Simple fixed-delay scan runner.
# Assumes the server is already running before you launch this script.
#
# Example server:
# go run ./cmd/simple_server -addr :8080 -delay-mode fixed -delay 10ms

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
DURATION="${DURATION:-10s}"
COOLDOWN="${COOLDOWN:-5}"
HTTP2="${HTTP2:-false}"
RATES=(${RATES:-1000 3000 5000 7000 9000 11000 13000})
LAST_RATE_INDEX=$((${#RATES[@]} - 1))
LAST_RATE="${RATES[$LAST_RATE_INDEX]}"

STAMP="$(date +%Y%m%d_%H%M%S)"
RUN_ROOT="${RUN_ROOT:-fixed_delay_low_load_runs}"
OUTDIR="${OUTDIR:-${RUN_ROOT}/run_${STAMP}}"
SUMMARY_CSV="${OUTDIR}/summary.csv"
LATEST_SUMMARY="${RUN_ROOT}/latest_summary.csv"
TARGETS_FILE="${OUTDIR}/targets.txt"

mkdir -p "$OUTDIR"

# Vegeta targets file for this run.
printf 'GET http://%s:%s/\n' "$HOST" "$PORT" >"$TARGETS_FILE"

# One summary row per load level.
printf 'target_rps,requests,achieved_throughput_rps,mean_latency_ms,p50_latency_ms,p95_latency_ms,max_latency_ms,success\n' >"$SUMMARY_CSV"

echo "Writing run outputs to: $OUTDIR"
echo "Rates: ${RATES[*]}"
echo "Duration per rate: $DURATION"

for RATE in "${RATES[@]}"; do
  RATE_DIR="${OUTDIR}/rps_${RATE}"
  mkdir -p "$RATE_DIR"

  echo
  echo "Running ${RATE} RPS"

  # Attack outputs for this rate.
  ./vegeta_local attack \
    -targets="$TARGETS_FILE" \
    -rate="$RATE" \
    -duration="$DURATION" \
    -http2="$HTTP2" \
    -metrics-csv="${RATE_DIR}/metrics.csv" \
    -window-csv="${RATE_DIR}/window_results.csv" \
    -output="${RATE_DIR}/results.bin"

  # Save both machine-readable and human-readable reports.
  ./vegeta_local report -type=json "${RATE_DIR}/results.bin" >"${RATE_DIR}/report.json"
  ./vegeta_local report "${RATE_DIR}/results.bin" >"${RATE_DIR}/report.txt"

  # Append the main numbers we care about to the run summary.
  python3 - "$RATE" "${RATE_DIR}/report.json" "$SUMMARY_CSV" <<'PY'
import csv
import json
import sys
from pathlib import Path

target_rps = int(sys.argv[1])
report_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

report = json.loads(report_path.read_text())
latencies = report["latencies"]

row = {
    "target_rps": target_rps,
    "requests": int(report["requests"]),
    "achieved_throughput_rps": float(report["throughput"]),
    "mean_latency_ms": float(latencies["mean"]) / 1e6,
    "p50_latency_ms": float(latencies["50th"]) / 1e6,
    "p95_latency_ms": float(latencies["95th"]) / 1e6,
    "max_latency_ms": float(latencies["max"]) / 1e6,
    "success": float(report["success"]),
}

with summary_path.open("a", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
    writer.writerow(row)

print(
    f"target={target_rps} achieved={row['achieved_throughput_rps']:.2f} "
    f"mean_ms={row['mean_latency_ms']:.3f} p95_ms={row['p95_latency_ms']:.3f}"
)
PY

  # Brief pause between rates so runs do not blend together.
  if [[ "$RATE" != "$LAST_RATE" ]]; then
    sleep "$COOLDOWN"
  fi
done

# Refresh a stable pointer for the curve-fit script.
cp "$SUMMARY_CSV" "$LATEST_SUMMARY"

echo
echo "Summary saved to: $SUMMARY_CSV"
echo "Latest summary copy: $LATEST_SUMMARY"
cat "$SUMMARY_CSV"
