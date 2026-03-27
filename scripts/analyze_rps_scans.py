#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(".matplotlib").resolve()))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DEFAULT_RESULTS_DIR = Path("experiments/out")
DEFAULT_SCAN = "healthy_fixed10ms_http1_cycle_20260324_181942"
DEFAULT_RPS = [4000, 6000, 8000, 10000, 12000]
DEFAULT_RUNS = [1, 2]
DEFAULT_SKIP_FIRST = 2
DEFAULT_SKIP_LAST = 1

WINDOW_METRICS = [
    "avg_scheduler_delay_ms",
    "avg_dispatch_delay_ms",
    "avg_conn_delay_ms",
    "avg_first_byte_rtt_ms",
    "avg_response_tail_time_ms",
    "avg_total_latency_ms",
    "reuse_frac",
    "fresh_conn_frac",
    "was_idle_given_reused",
    "avg_conn_idle_time_ms",
]

SAMPLE_METRICS = [
    "scheduler_delay",
    "total_latency",
    "conn_delay",
    "response_tail_time",
    "first_byte_rtt",
    "fire_to_dispatch_delay",
    "conn_idle_time",
    "first_byte_delay",
    "write_delay",
    "dispatch_delay",
    "pacer_wait",
]

PERCENTILES = {
    "p05": 5,
    "p25": 25,
    "p50": 50,
    "p75": 75,
    "p95": 95,
    "p99": 99,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Analyze a timestamped RPS scan under experiments/out by trimming edge "
            "windows, computing percentile summaries, and saving plots."
        )
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--scan", default=DEFAULT_SCAN)
    parser.add_argument("--rps", type=int, nargs="+", default=DEFAULT_RPS)
    parser.add_argument("--runs", type=int, nargs="+", default=DEFAULT_RUNS)
    parser.add_argument("--skip-first", type=int, default=DEFAULT_SKIP_FIRST)
    parser.add_argument("--skip-last", type=int, default=DEFAULT_SKIP_LAST)
    return parser.parse_args()


def safe_metric_name(metric):
    return metric.replace("/", "_").replace(" ", "_")


def summarize_series(values):
    """
    Inputs:
        - values: a pandas Series containing numeric values
    Output:     
        - summary: a dictionary containing count, mean, std, median, min, max, and additional percentiles for the input values. Returns None if there are no valid numeric values.
    """
    clean = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
    if clean.empty:
        return None

    # get a summary for al the values
    summary = {
        "count": int(clean.shape[0]),
        "mean": float(clean.mean()),
        "std": float(clean.std(ddof=1)) if clean.shape[0] > 1 else 0.0,
        "median": float(clean.median()),
        "min": float(clean.min()),
        "max": float(clean.max()),
    }

    # add the requested percentiles to the summary
    for label, percentile in PERCENTILES.items():
        summary[label] = float(np.percentile(clean.to_numpy(), percentile))

    return summary


def load_trimmed_window_results(path, skip_first, skip_last):
    """
    Inputs: 
        - path: path to the window results CSV file
        - skip_first: number of initial windows to skip
        - skip_last: number of final windows to skip
    Outputs:
        - trimmed: DataFrame containing only the rows corresponding to the kept windows
        - order: DataFrame containing the unique window_start and window_end pairs in their original order
    """

    df = pd.read_csv(path)
    if df.empty:
        return df.copy(), df.copy()

    order = df[["window_start", "window_end"]].drop_duplicates().reset_index(drop=True)

    # obtaining the split locations
    start = min(skip_first, len(order))
    stop = len(order) - min(skip_last, max(len(order) - start, 0))
    kept = order.iloc[start:stop].copy()

    if kept.empty:
        return df.iloc[0:0].copy(), order

    # perform an inner merge to keep only the rows corresponding to the kept windows
    trimmed = df.merge(kept, on=["window_start", "window_end"], how="inner")
    return trimmed, order


def load_trimmed_window_samples(path, kept_windows):
    """
    Inputs:
        - path: path to the window samples CSV file
        - kept_windows: DataFrame containing the unique window_start and window_end pairs for the windows that were kept after trimming
    Output:
        - trimmed_samples: DataFrame containing only the rows from the window samples CSV that correspond to the kept windows
    """

    df = pd.read_csv(path)
    if df.empty or kept_windows.empty:
        return df.iloc[0:0].copy()
    return df.merge(kept_windows, on=["window_start", "window_end"], how="inner")


def ensure_expected_columns(df, columns, path):
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {path}")


def collect_scan_data(scan_dir, rps_values, run_ids, skip_first, skip_last):
    sample_rows = []
    window_rows = []
    console_blocks = []

    for rps in rps_values:
        for run in run_ids:
            run_dir = scan_dir / f"rps_{rps}"

            # get the paths for the window results and samples CSV files
            window_results_path = run_dir / f"window_results_run{run}.csv"
            window_samples_path = run_dir / f"window_samples_run{run}.csv"

            if not window_results_path.exists():
                raise FileNotFoundError(f"Missing file: {window_results_path}")
            if not window_samples_path.exists():
                raise FileNotFoundError(f"Missing file: {window_samples_path}")

            # obtaining the trimmed windows and samples dataframes
            trimmed_windows, all_windows = load_trimmed_window_results(
                window_results_path, skip_first, skip_last
            )
            kept_windows = trimmed_windows[["window_start", "window_end"]].drop_duplicates()
            trimmed_samples = load_trimmed_window_samples(window_samples_path, kept_windows)

            ensure_expected_columns(trimmed_windows, WINDOW_METRICS, window_results_path)
            ensure_expected_columns(trimmed_samples, ["metric_name", "value_ms"], window_samples_path)

            # adding console lines for this run
            console_lines = [
                f"RPS={rps} RUN={run}",
                (
                    f"  windows kept={len(kept_windows)} of {len(all_windows)} "
                    f"(skip_first={skip_first}, skip_last={skip_last})"
                ),
            ]

            for metric in SAMPLE_METRICS:
                metric_df = trimmed_samples.loc[trimmed_samples["metric_name"] == metric, "value_ms"]
                summary = summarize_series(metric_df)
                if summary is None:
                    continue

                # adding rps/run/metric and its summary (count, percentiles, etc.) to sample_rows
                sample_rows.append(
                    {
                        "rps": rps,
                        "run": run,
                        "metric": metric,
                        **summary,
                    }
                )

                # adding output lines for this metric to the console summary
                console_lines.append(
                    (
                        f"  sample {metric}: count={summary['count']} "
                        f"median={summary['median']:.4f} "
                        f"p05={summary['p05']:.4f} p25={summary['p25']:.4f} "
                        f"p50={summary['p50']:.4f} p75={summary['p75']:.4f} "
                        f"p95={summary['p95']:.4f} p99={summary['p99']:.4f}"
                    )
                )

            for metric in WINDOW_METRICS:

                # obtain summary for this metric across all the trimmed windows and add it to window_rows with rps/run/metric info
                summary = summarize_series(trimmed_windows[metric])
                if summary is None:
                    continue

                window_rows.append(
                    {
                        "rps": rps,
                        "run": run,
                        "metric": metric,
                        **summary,
                    }
                )

            console_blocks.append("\n".join(console_lines))

    # returning dataframes with sample and window-level summaries, as well as the console summary blocks for each run
    sample_df = pd.DataFrame(sample_rows).sort_values(["metric", "rps", "run"]).reset_index(drop=True)
    window_df = pd.DataFrame(window_rows).sort_values(["metric", "rps", "run"]).reset_index(drop=True)
    return sample_df, window_df, console_blocks


def write_console_summary(console_blocks, output_path):
    text = "\n\n".join(console_blocks) + "\n"
    output_path.write_text(text)
    print(text, end="")


def plot_percentiles(summary_df, metrics, save_dir, rps_values, run_ids, file_suffix):
    """
    Inputs:
    - summary_df: DataFrame containing per-(rps, run, metric) summaries
    - metrics: ordered list of metrics to plot
    - save_dir: directory where the generated plots should be saved
    - rps_values: list of RPS values to include in the plots
    - run_ids: list of run IDs to include in the plots
    - file_suffix: suffix appended to the output filename
    """
    if summary_df.empty:
        return

    percentiles_to_plot = ["p05", "p25", "p50", "p75", "p95", "p99"]
    x = np.arange(len(rps_values))
    width = 0.35 if len(run_ids) <= 2 else 0.8 / max(len(run_ids), 1)

    for metric in metrics:
        metric_df = summary_df[summary_df["metric"] == metric].copy()
        if metric_df.empty:
            continue

        fig, axes = plt.subplots(2, 3, figsize=(16, 8), sharex=True)
        axes = axes.flatten()

        for ax, percentile in zip(axes, percentiles_to_plot):
            for run_index, run in enumerate(run_ids):
                run_df = metric_df[metric_df["run"] == run].set_index("rps")
                heights = [run_df.at[rps, percentile] if rps in run_df.index else np.nan for rps in rps_values]
                offset = (run_index - (len(run_ids) - 1) / 2.0) * width

                ax.bar(
                    x + offset,
                    heights,
                    width=width,
                    label=f"run{run}",
                    alpha=0.85,
                )

            ax.set_title(percentile)
            ax.set_xticks(x)
            ax.set_xticklabels([str(rps) for rps in rps_values], rotation=0)
            ax.set_xlabel("RPS")
            ax.set_ylabel(metric)
            ax.grid(axis="y", alpha=0.25)

        handles, labels = axes[0].get_legend_handles_labels()
        if handles:
            fig.legend(handles, labels, loc="upper center", ncol=len(run_ids))
        fig.suptitle(f"{metric} percentiles by RPS", y=0.98)
        fig.tight_layout(rect=[0, 0, 1, 0.95])
        fig.savefig(save_dir / f"{safe_metric_name(metric)}_{file_suffix}.png", dpi=200)
        plt.close(fig)


def main():
    args = parse_args()
    scan_dir = args.results_dir / args.scan
    if not scan_dir.exists():
        raise FileNotFoundError(f"Scan directory not found: {scan_dir}")

    Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    analysis_dir = scan_dir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    sample_df, window_df, console_blocks = collect_scan_data(
        scan_dir=scan_dir,
        rps_values=args.rps,
        run_ids=args.runs,
        skip_first=args.skip_first,
        skip_last=args.skip_last,
    )

    print(f"sample_df:\n {sample_df}")
    print(f"window_df:\n {window_df}")
    exit()

    sample_csv = analysis_dir / "sample_percentiles.csv"
    window_csv = analysis_dir / "window_percentiles.csv"
    console_txt = analysis_dir / "sample_summary.txt"


    sample_df.to_csv(sample_csv, index=False)
    window_df.to_csv(window_csv, index=False)
    write_console_summary(console_blocks, console_txt)
    plot_percentiles(
        window_df,
        WINDOW_METRICS,
        analysis_dir,
        args.rps,
        args.runs,
        "window_percentiles",
    )
    plot_percentiles(
        sample_df,
        SAMPLE_METRICS,
        analysis_dir,
        args.rps,
        args.runs,
        "sample_percentiles",
    )

    print(f"\nSaved sample summary CSV: {sample_csv}")
    print(f"Saved window summary CSV: {window_csv}")
    print(f"Saved text summary: {console_txt}")
    print(f"Saved plots under: {analysis_dir}")


if __name__ == "__main__":
    main()
