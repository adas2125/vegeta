#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from xlg_eval_common import (
    cheap_signal_quantiles,
    finite_values,
    leave_one_out_normalizers,
    pooled_reference,
    quantile,
    read_json,
    run_dirs,
    window_scores,
    write_json,
)

DEFAULT_THRESHOLD_QUANTILE = 0.90


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Stage A EMD thresholds.")
    parser.add_argument("--stage-a-dir", type=Path, required=True)
    parser.add_argument("--counts-json", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--trim-s", type=float, default=5.0)
    parser.add_argument("--threshold-quantile", type=float, default=DEFAULT_THRESHOLD_QUANTILE)
    return parser.parse_args()


def healthy_window_scores(
    healthy_runs: list[Path],
    normalizers: dict[str, float],
    trim_s: float,
) -> pd.DataFrame:
    """Score healthy windows against leave-one-out healthy baselines."""
    frames: list[pd.DataFrame] = []
    for run_dir in healthy_runs:
        # obtain path name for the other healthy runs
        reference_runs = [path for path in healthy_runs if path != run_dir]

        # collect the references for the reference runs and pool them together
        reference = pooled_reference(reference_runs, trim_s=trim_s)
        
        windows = window_scores(
            run_dir=run_dir,
            reference=reference,
            normalizers=normalizers,
            trim_s=trim_s,
        )
        frames.append(windows)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def score_quantile(values: pd.Series, q: float) -> float:
    """Return a robust finite-score threshold from healthy baseline windows."""
    vals = finite_values(values)
    if not vals:
        raise ValueError("healthy baseline windows produced no finite scores")
    return quantile(vals, q)


def main() -> None:
    args = parse_args()
    # obtaining the directories from stage a, output file will be 'stage_a_thresholds.json'
    # reading in the counts from stage a
    stage_dir = args.stage_a_dir
    output = args.output or stage_dir / "stage_a_thresholds.json"
    counts_json = args.counts_json or stage_dir / "stage_a_counts.json"

    if not 0 < args.threshold_quantile <= 1:
        raise ValueError("--threshold-quantile must be in (0, 1]")

    counts = read_json(counts_json)
    # these are the path names of the healthy runs
    healthy_runs = run_dirs(stage_dir / "healthy")
    if len(healthy_runs) < 2:
        raise ValueError("at least two healthy runs are needed for leave-one-out EMD")
    

    # compute the normalizers using a leave-out strategy
    # normalizers now computed run-level (leave_one_out makes sense if there are more baselines)
    normalizers = leave_one_out_normalizers(healthy_runs, trim_s=args.trim_s)

    # returns for each window, the scheduler scores w/ the normalizers applied, as well as rho values
    healthy_windows = healthy_window_scores(
        healthy_runs=healthy_runs,
        normalizers=normalizers,
        trim_s=args.trim_s,
    )

    # obtain scheduler thresholds based on the provided quantile of the healthy window scores aggregated across all healthy runs
    scheduler_threshold = score_quantile(healthy_windows["scheduler_score"], args.threshold_quantile)
    cheap_quantiles = cheap_signal_quantiles(healthy_runs, trim_s=args.trim_s)
    
    thresholds = {
        "T_cpu": scheduler_threshold,
        "T_worker": scheduler_threshold,
    }

    # save the payload
    payload = {
        "stage_a_dir": stage_dir,
        "rho_center_fixed": counts["rho_center_fixed"],
        "epsilon_fixed": counts["epsilon_fixed"],
        "normalizers": normalizers,
        "thresholds": thresholds,
        "cheap_signal_rule": "scheduler_mean_or_connection_p25_gt_healthy_p95",
        "cheap_signal_quantiles": cheap_quantiles,
    }
    write_json(output, payload)
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
