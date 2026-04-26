#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path


MILD_RATE_FACTOR = 0.90
JOB_STEP = 10
VERIFY_TOLERANCE = 0.01


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select CPU job counts from Vegeta report JSON files."
    )
    parser.add_argument("--run-dir", type=Path, default=None)
    return parser.parse_args()


def script_root() -> Path:
    return Path(__file__).resolve().parent


def latest_run_dir() -> Path:
    candidates = sorted((script_root() / "output").glob("run_*"))
    if not candidates:
        raise FileNotFoundError("no run_* directories found under cpu_contention_profiling/output")
    return candidates[-1]


def load_report(path: Path) -> dict[str, float]:
    data = json.loads(path.read_text())

    rate = float(data["rate"])
    requests = float(data["requests"])
    duration_ns = float(data["duration"])

    if not math.isfinite(rate) or rate <= 0:
        raise ValueError(f"invalid Vegeta rate in {path}")
    if not math.isfinite(duration_ns) or duration_ns <= 0:
        raise ValueError(f"invalid duration in {path}")
    if not math.isfinite(requests) or requests < 0:
        raise ValueError(f"invalid requests count in {path}")

    derived_rate = requests / (duration_ns / 1e9)
    relative_error = abs(rate - derived_rate) / rate
    if relative_error > VERIFY_TOLERANCE:
        raise ValueError(
            f"rate mismatch in {path}: rate={rate:.6f}, "
            f"derived_rate={derived_rate:.6f}, rel_error={relative_error:.4f}"
        )

    throughput = float(data.get("throughput", math.nan))
    return {
        "rate": rate,
        "derived_rate": derived_rate,
        "throughput": throughput,
    }


def collect_rates(run_dir: Path) -> tuple[dict[int, dict[int, list[float]]], bool]:
    rates_by_rps: dict[int, dict[int, list[float]]] = {}
    had_error = False

    for path in sorted(run_dir.glob("rps_*/jobs_*/run_*/report_rps*.json")):
        try:
            rps = int(path.parents[2].name.removeprefix("rps_"))
            jobs = int(path.parents[1].name.removeprefix("jobs_"))
            report = load_report(path)
        except Exception as exc:
            print(f"[select_jobs] {exc}", file=sys.stderr)
            had_error = True
            continue

        rates_by_rps.setdefault(rps, {}).setdefault(jobs, []).append(report["rate"])

    if not rates_by_rps:
        raise FileNotFoundError(f"no report_rps*.json files found under {run_dir}")

    return rates_by_rps, had_error


def first_job_below(job_rates: dict[int, float], limit: float) -> int | None:
    for jobs in sorted(job_rates):
        if job_rates[jobs] < limit:
            return jobs
    return None


def select_jobs(job_rates: dict[int, float], rps: int) -> dict[str, int | None]:
    mild = first_job_below(job_rates, MILD_RATE_FACTOR * rps)
    if mild is None:
        return {"mild": None, "mod": None, "severe": None}

    return {
        "mild": mild,
        "mod": mild + JOB_STEP,
        "severe": mild + (2 * JOB_STEP),
    }


def main() -> None:
    args = parse_args()
    run_dir = args.run_dir or latest_run_dir()

    rates_by_rps, had_error = collect_rates(run_dir)

    jobs_payload: dict[str, dict[str, int | None]] = {}
    selection_failed = had_error

    for rps in sorted(rates_by_rps):
        job_rates = {
            jobs: statistics.median(values)
            for jobs, values in rates_by_rps[rps].items()
            if values
        }
        selected = select_jobs(job_rates, rps)
        jobs_payload[str(rps)] = selected

        if any(value is None for value in selected.values()):
            selection_failed = True

    payload = {
        "metric": "vegeta_rate",
        "rule": {
            "mild_rate_below_fraction_of_target": MILD_RATE_FACTOR,
            "job_step": JOB_STEP,
        },
        "jobs": jobs_payload,
    }

    output_path = run_dir / "cpu_jobs.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {output_path}")

    if selection_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
