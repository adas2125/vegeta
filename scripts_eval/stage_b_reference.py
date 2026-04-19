#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from xlg_eval_common import (
    baseline_concurrency,
    read_rate,
    run_dirs,
    severity_from_count,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Stage B fault injection settings.")
    parser.add_argument("--stage-b-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stage_dir = args.stage_b_dir
    output = args.output or stage_dir / "stage_b_reference.json"
    healthy_runs = run_dirs(stage_dir / "baseline_healthy")
    if not healthy_runs:
        raise ValueError("at least one Stage B healthy run is needed")

    rate = read_rate(stage_dir, "baseline_healthy")
    baseline_count = baseline_concurrency(healthy_runs, rate, trim_s=args.trim_s)

    payload = {
        "rate": rate,
        "severity": severity_from_count(baseline_count),
    }
    write_json(output, payload)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
