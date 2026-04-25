#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from xlg_eval_common import (
    median,
    quantile,
    read_rate,
    rho_values,
    run_dirs,
    write_json,
)


def parse_args() -> argparse.Namespace:
    """
    Arguments:
        - stage-a-dir: direcotry consisting of healthy runs
        - trim-s: how many seconds to trim from the start of each run
    """
    parser = argparse.ArgumentParser(description="Compute Stage A fixed-delay counts.")
    parser.add_argument("--stage-a-dir", type=Path, required=True)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stage_dir = args.stage_a_dir
    output = stage_dir / "stage_a_counts.json"

    # obtain the healthy runs
    healthy_runs = run_dirs(stage_dir / "healthy")

    # obtain the healthy rho center and band width
    rhos = rho_values(healthy_runs, trim_s=args.trim_s)
    rho_center = median(rhos)
    epsilon = quantile([abs(value - rho_center) for value in rhos], 0.95)
    rate = read_rate(stage_dir)

    # save the Stage A calibration values needed by thresholding and evaluation
    payload = {
        "rate": rate,
        "rho_center_fixed": rho_center,
        "epsilon_fixed": epsilon,
    }
    write_json(output, payload)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
