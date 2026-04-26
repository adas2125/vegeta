#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from xlg_eval_common import (
    read_json,
    read_rate,
    retained_windows,
    round_count,
    run_dirs,
    write_json,
)

CAP_OFFSETS_MS = {
    "mild": 6.0,
    "mod": 4.0,
    "severe": 2.0,
}
CPU_JOBS_JSON = Path(
    "/home/amitdas3/vegeta/cpu_contention_profiling/output/run_20260424_170744/cpu_jobs.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Stage B fault injection settings.")
    parser.add_argument("--stage-b-dir", type=Path, required=True)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def baseline_mean_total_latency_ms(run_list: list[Path], trim_s: float) -> float:
    total = 0.0
    count = 0
    for run_dir in run_list:
        latencies = retained_windows(run_dir, trim_s=trim_s)["avg_total_latency_ms"]
        total += float(latencies.sum())
        count += int(latencies.count())
    if count == 0:
        raise ValueError("Stage B healthy runs produced no total-latency windows")
    return total / count


def caps_from_latency(rate: int, baseline_latency_ms: float) -> dict[str, int]:
    return {
        severity: round_count(rate * (baseline_latency_ms + offset_ms) / 1000.0)
        for severity, offset_ms in CAP_OFFSETS_MS.items()
    }


def cpu_jobs_from_profile(rate: int) -> dict[str, int]:
    payload = read_json(CPU_JOBS_JSON)
    jobs_by_rps = payload["jobs"]
    selected = jobs_by_rps.get(str(rate))
    if selected is None:
        raise KeyError(f"missing CPU jobs for rate {rate} in {CPU_JOBS_JSON}")
    return {severity: int(selected[severity]) for severity in ["mild", "mod", "severe"]}


def main() -> None:
    args = parse_args()
    stage_dir = args.stage_b_dir
    output = stage_dir / "stage_b_reference.json"

    # get the healthy runs for Stage B
    healthy_runs = run_dirs(stage_dir / "baseline_healthy")

    # obtain the rate
    rate = read_rate(stage_dir)
    baseline_latency_ms = baseline_mean_total_latency_ms(healthy_runs, trim_s=args.trim_s)
    caps = caps_from_latency(rate, baseline_latency_ms)
    print(f"baseline latency: {baseline_latency_ms:.2f} ms, caps: {caps}, rate: {rate} rps, CPU jobs: {cpu_jobs_from_profile(rate)}")

    payload = {
        "rate": rate,
        "severity": {
            "connections": caps.copy(),
            "cpu": cpu_jobs_from_profile(rate),
            "workers": caps.copy(),
        },
    }
    write_json(output, payload)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
