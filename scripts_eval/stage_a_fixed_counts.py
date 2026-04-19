#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from xlg_eval_common import (
    baseline_concurrency,
    median,
    quantile,
    read_rate,
    rho_values,
    run_dirs,
    severity_from_count,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Stage A fixed-delay counts.")
    parser.add_argument("--stage-a-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stage_dir = args.stage_a_dir
    output = args.output or stage_dir / "stage_a_counts.json"

    healthy_runs = run_dirs(stage_dir / "healthy")
    if not healthy_runs:
        raise FileNotFoundError(f"no healthy run_* directories under {stage_dir / 'healthy'}")

    # obtain the median rhos, how much it deviates from the center, and the concurrency at the baseline
    rhos = rho_values(healthy_runs, trim_s=args.trim_s)
    rho_center = median(rhos)
    epsilon = quantile([abs(value - rho_center) for value in rhos], 0.95)
    rate = read_rate(stage_dir, "healthy")
    baseline_count = baseline_concurrency(healthy_runs, rate, trim_s=args.trim_s)

    # saves the payload including the rate, the center rho, the epsilon, and the severity values for
    # fault injection based on baseline count (workers, connections). CPU contention is fixed
    payload = {
        "rate": rate,
        "rho_center_fixed": rho_center,
        "epsilon_fixed": epsilon,
        "severity": severity_from_count(baseline_count),
    }
    write_json(output, payload)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
