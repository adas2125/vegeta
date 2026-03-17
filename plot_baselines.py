#!/usr/bin/env python3
"""
Build baseline summaries and plots from per-window CSV output.

This script expects a directory of baseline CSV files whose names encode the
intended request rate and run number, for example:

    baseline_rps1000_run1.csv

The overall flow is:
1. Load each CSV and trim warmup / partial windows.
2. Compute one mean row per run for plotting and quick sanity checks.
3. Pool all trimmed windows by RPS to build the baseline reference table.
4. Save summary CSVs and one plot per tracked metric.
5. Optionally compare a new run's window-level results to the nearest baseline.

The implementation below keeps the logic straightforward and favors explicit
dataframe transformations so it is easy to inspect intermediate outputs.

Usage example:
    python3 plot_baselines.py --drop-last-window

"""

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Baseline files are expected to encode both target RPS and run number.
# Example: baseline_rps1000_run2.csv
FILENAME_RE = re.compile(r".*rps(\d+)_run(\d+)\.csv$")

# Smaller subset used when building the exported reference CSV and when
# comparing a new run against the nearest baseline.
PRIMARY_BASELINE_METRICS = [
    "valid_achieved_rate", "avg_scheduler_delay_ms",
    "avg_conn_delay_ms", "avg_first_byte_rtt_ms",
    "avg_total_latency_ms", "avg_in_flight",
]

# Human-friendly axis labels for the generated plots.
METRICS_TO_PLOT = {
    "valid_achieved_rate": "Valid achieved rate (RPS)",
    "avg_scheduler_delay_ms": "Avg scheduler delay (ms)",
    "avg_dispatch_delay_ms": "Avg dispatch delay (ms)",
    "avg_conn_delay_ms": "Avg conn delay (ms)",
    "avg_write_delay_ms": "Avg write delay (ms)",
    "avg_first_byte_rtt_ms": "Avg first-byte RTT (ms)",
    "avg_first_byte_delay_ms": "Avg first-byte delay (ms)",
    "avg_total_latency_ms": "Avg total latency (ms)",
    "avg_in_flight": "Avg in-flight",
    "observed_R": "Observed R",
}

def parse_args():
    """Parse CLI arguments."""
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", type=str, default="baseline_ref")
    p.add_argument("--output-dir", type=str, default="baseline_plots")
    p.add_argument("--warmup-windows", type=int, default=2)
    p.add_argument("--drop-last-window", action="store_true")
    p.add_argument("--duration-threshold-frac", type=float, default=0.8)
    return p.parse_args()

def infer_rps_and_run(path: Path):
    """Extract `(rps, run)` from a baseline CSV filename."""
    m = FILENAME_RE.match(path.name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

def load_and_trim_csv(path: Path, warmup_windows: int, drop_last_window: bool, duration_threshold_frac: float):
    """
    Load one CSV and drop windows that should not influence the baseline.

    Trimming rules:
    - Remove a configurable number of initial warmup windows.
    - Optionally remove the final window, which is often partial.
    - Drop unusually short windows compared with the file's median duration.

    The returned dataframe preserves the original columns and row order.
    """
    df = pd.read_csv(path)

    if df.empty:
        return df

    # Early windows often include startup effects and are excluded from the
    # baseline to better represent steady-state behavior.
    if warmup_windows > 0 and len(df) > warmup_windows:
        df = df.iloc[warmup_windows:].copy()

    # The last window can be truncated when the attack ends mid-interval.
    if drop_last_window and len(df) > 1:
        df = df.iloc[:-1].copy()

    if df.empty:
        return df

    # Filter out any windows that are materially shorter than the typical
    # window length for this file, since they can skew per-window averages.
    if "window_duration_ms" in df.columns:
        median_dur = df["window_duration_ms"].median()
        min_ok = duration_threshold_frac * median_dur
        df = df[df["window_duration_ms"] >= min_ok].copy()

    return df

def build_run_level_table(input_dir: Path, args) -> pd.DataFrame:
    """
    Build one summary row per `(rps, run)` baseline file.

    Each metric is the mean across the trimmed windows for that single run.
    This is mainly useful for plotting per-run points on top of the aggregated
    baseline curve and for quick sanity checks across runs.
    """
    rows = []

    for path in sorted(input_dir.glob("*.csv")):
        parsed = infer_rps_and_run(path)
        if parsed is None:
            continue

        rps, run = parsed
        df = load_and_trim_csv(
            path,
            warmup_windows=args.warmup_windows,
            drop_last_window=args.drop_last_window,
            duration_threshold_frac=args.duration_threshold_frac,
        )

        if df.empty:
            continue

        row = {
            "file": path.name,
            "rps": rps,
            "run": run,
            "num_windows_used": len(df),
        }

        # Compute one mean value per metric for this run, which we can later plot
        for metric in METRICS_TO_PLOT.keys():
            if metric in df.columns:
                row[metric] = df[metric].mean()

        rows.append(row)

    if not rows:
        raise RuntimeError("No matching baseline CSV files found or all became empty after trimming.")

    # one row per run, sorted by RPS and run number for easy visual inspection
    return pd.DataFrame(rows).sort_values(["rps", "run"]).reset_index(drop=True)

def build_window_level_table(input_dir: Path, args) -> pd.DataFrame:
    """
    Pool all trimmed windows from all baseline CSV files into one table.

    Extra columns are added so each window still knows which run and source
    file it came from after concatenation.
    """
    frames = []

    for path in sorted(input_dir.glob("*.csv")):
        parsed = infer_rps_and_run(path)
        if parsed is None:
            continue

        rps, run = parsed
        df = load_and_trim_csv(
            path,
            warmup_windows=args.warmup_windows,
            drop_last_window=args.drop_last_window,
            duration_threshold_frac=args.duration_threshold_frac,
        )

        if df.empty:
            continue

        df = df.copy()
        df["source_file"] = path.name
        df["rps"] = rps
        df["run"] = run
        frames.append(df)

    if not frames:
        raise RuntimeError("No baseline windows found after trimming.")

    # concatenate all windows into one big table, sorted by RPS and run for easy visual inspection
    return pd.concat(frames, ignore_index=True)

def build_rps_summary_from_windows(window_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate pooled windows into one baseline summary row per RPS.

    For each metric we compute:
    - mean and sample standard deviation
    - 99th and 1st percentiles to capture the typical range of variation across windows
    """
    records = []

    for rps, grp in window_df.groupby("rps", sort=True):
        record = {
            "rps": rps,
            "num_runs": grp["run"].nunique(),
            "num_windows": len(grp),
        }

        for metric in METRICS_TO_PLOT.keys():
            if metric not in grp.columns:
                continue

            vals = grp[metric].dropna()
            if len(vals) == 0:
                continue

            # Use sample stddev (`ddof=1`) when we have more than one window so
            # the summary reflects run-to-run/window-to-window variation.
            mean = vals.mean()
            std = vals.std(ddof=1) if len(vals) > 1 else 0.0

            record[f"{metric}_mean"] = mean
            record[f"{metric}_std"] = std

        if "observed_R" in grp.columns:
            observed_r_vals = grp["observed_R"].dropna()
            if len(observed_r_vals) > 0:
                record["p01_normal_r"] = observed_r_vals.quantile(0.01)
                record["p99_normal_r"] = observed_r_vals.quantile(0.99)

        records.append(record)

    return pd.DataFrame(records).sort_values("rps").reset_index(drop=True)

def safe_filename(metric: str) -> str:
    """Convert a metric name into a filesystem-friendly plot filename."""
    return metric.replace("/", "_").replace(" ", "_")

def plot_metric(run_df: pd.DataFrame, summary_df: pd.DataFrame, metric: str, output_dir: Path):
    """
    Plot one metric across baseline RPS values.

    The line and error bars come from pooled window-level statistics, while the
    overlaid scatter points show the run-level means for individual baseline
    runs at that same target RPS.
    """
    metric_mean_col = f"{metric}_mean"
    metric_std_col = f"{metric}_std"

    if metric_mean_col not in summary_df.columns:
        return

    x = summary_df["rps"].to_list()
    y = summary_df[metric_mean_col].to_list()
    yerr = summary_df[metric_std_col].fillna(0.0).to_list()

    plt.figure(figsize=(8, 5))
    plt.errorbar(x, y, yerr=yerr, fmt="-o", capsize=5)

    # Overlay one point per run so you can see how tightly the individual runs
    # cluster around the pooled baseline mean.
    for rps in sorted(run_df["rps"].unique()):
        vals = run_df.loc[run_df["rps"] == rps, metric].dropna().to_list()
        if vals:
            plt.scatter([rps] * len(vals), vals, alpha=0.7)

    plt.xlabel("Target RPS")
    plt.ylabel(METRICS_TO_PLOT.get(metric, metric))
    plt.title(f"{metric} vs baseline RPS")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    out_path = output_dir / f"{safe_filename(metric)}.png"
    plt.savefig(out_path, dpi=160)
    plt.close()

def main():
    """Run the full baseline summarization, plotting."""
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # One row per baseline file after trimming. This gives us per-run means.
    run_df = build_run_level_table(input_dir, args)

    # Pooled window data is the basis for the baseline reference, because it
    # preserves the natural variation across all retained windows.
    window_df = build_window_level_table(input_dir, args)
    rps_summary_df = build_rps_summary_from_windows(window_df)

    # Save the final tables
    run_df.to_csv(output_dir / "run_level_summary.csv", index=False)
    window_df.to_csv(output_dir / "window_level_trimmed.csv", index=False)
    rps_summary_df.to_csv(output_dir / "baseline_reference.csv", index=False)

    # Generate one PNG per metric for quick visual inspection.
    for metric in METRICS_TO_PLOT.keys():
        plot_metric(run_df, rps_summary_df, metric, output_dir)

    print(f"Saved plots and summaries to: {output_dir}")



if __name__ == "__main__":
    main()
