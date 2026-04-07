#!/usr/bin/env python3


import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance
from matplotlib.ticker import AutoMinorLocator

from utils import normalized_emd, safe_filename, trim_window_margins, latest_run_dir

DEFAULT_ROOT_DIR = Path("misleading_results")
DEFAULT_CASE_BASELINE = "well_provisioned"
DEFAULT_CASE_ABNORMAL = "constrained"
DEFAULT_TRIM_START_WINDOWS = 0
DEFAULT_TRIM_END_WINDOWS = 1
DEFAULT_REQUESTS_OUTPUT = "lg_requests_sent_comparison.pdf"
DEFAULT_CUMULATIVE_OUTPUT = "lg_requests_sent_cumulative_only.pdf"

DISTRIBUTION_METRICS = [
    "pacer_wait", "scheduler_delay", "fire_to_dispatch_delay",
    "dispatch_delay", "conn_delay", "first_byte_rtt", "first_byte_delay", 
    "response_tail_time", "total_latency", "write_delay",
]
CASE_STYLES = {
    "baseline": {
        "color": "tab:blue",
        "linestyle": "-",
        "marker": "o",
        "hatch": "////",
    },
    "abnormal": {
        "color": "tab:red",
        "linestyle": "--",
        "marker": "s",
        "hatch": "\\\\\\\\",
    },
}

BASE_FONT_SIZE = 23
AXIS_LABEL_SIZE = 27
TITLE_SIZE = 28
TICK_LABEL_SIZE = 21
LEGEND_SIZE = 21
SUPTITLE_SIZE = 24


def style_axis_grid(ax):
    ax.set_axisbelow(True)
    ax.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(which="major", axis="both", color="#cfd6df", linewidth=0.9, alpha=0.9)
    ax.grid(which="minor", axis="both", color="#e8edf3", linewidth=0.6, alpha=0.9)


def configure_plot_style():
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": BASE_FONT_SIZE,
            "axes.labelsize": AXIS_LABEL_SIZE,
            "axes.titlesize": TITLE_SIZE,
            "xtick.labelsize": TICK_LABEL_SIZE,
            "ytick.labelsize": TICK_LABEL_SIZE,
            "legend.fontsize": LEGEND_SIZE,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.linewidth": 1.0,
            "xtick.major.width": 1.0,
            "ytick.major.width": 1.0,
            "xtick.minor.width": 0.8,
            "ytick.minor.width": 0.8,
            "xtick.major.size": 5,
            "ytick.major.size": 5,
            "xtick.minor.size": 3,
            "ytick.minor.size": 3,
        }
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description=("Compare constrained vs well-provisioned misleading-throughput runs")
    )
    parser.add_argument("--root-dir", type=Path, default=DEFAULT_ROOT_DIR)
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Specific misleading_results/run_* directory. Defaults to the latest run under --root-dir.",
    )
    parser.add_argument("--baseline-case", default=DEFAULT_CASE_BASELINE)
    parser.add_argument("--abnormal-case", default=DEFAULT_CASE_ABNORMAL)
    parser.add_argument("--trim-start-windows", type=int, default=DEFAULT_TRIM_START_WINDOWS)
    parser.add_argument("--trim-end-windows", type=int, default=DEFAULT_TRIM_END_WINDOWS)
    parser.add_argument(
        "--requests-output",
        type=Path,
        default=None,
        help=(
            "Path for the requests-sent comparison figure. Defaults to "
            f"<run-dir>/{DEFAULT_REQUESTS_OUTPUT}."
        ),
    )
    parser.add_argument(
        "--cumulative-only",
        action="store_true",
        help="Only generate the cumulative sent-requests plot on its own.",
    )
    parser.add_argument(
        "--cumulative-output",
        type=Path,
        default=None,
        help=(
            "Path for the cumulative-only figure. Defaults to "
            f"<run-dir>/{DEFAULT_CUMULATIVE_OUTPUT}."
        ),
    )
    return parser.parse_args()

def load_case(run_dir, case_name, trim_start, trim_end):
    """
    Inputs:
        - run_dir: Path to the run_* directory containing the case subdirectories.
        - case_name: Name of the case subdirectory to load (e.g. "well_provisioned" or "constrained").
        - trim_start: Number of initial windows to trim from the window results (to remove warmup).
        - trim_end: Number of final windows to trim from the window results (to remove cooldown).
    Outputs:   A dictionary with the following keys and corresponding values:
        - "case_dir": Path to the case directory.
        - "results": DataFrame of the raw results with sent rate computations.
            - Columns: timestamp  elapsed_ms  workers  connections  in_flight  completions  elapsed_s  sent_cumulative  delta_elapsed_s  delta_sent  sent_rate_rps
        - "window_results": DataFrame of the window-level results with elapsed time and window index.
            - Columns: window_start, window_end, window_duration_ms, total_latency_count, valid_achieved_rate  ...  
            ll_violation, window_start_dt, window_end_dt, elapsed_s, window_index
        - "window_samples": DataFrame of the window-level samples filtered to the kept windows and with numeric values.
            - Columns: window_start, window_end, metric_name, value_ms
    """

    # loading all the files for the case
    case_dir = run_dir / case_name
    results_path = case_dir / f"{case_name}_metrics.csv"
    window_results_path = case_dir / f"{case_name}_window_results.csv"
    window_samples_path = case_dir / f"{case_name}_window_samples.csv"

    # some cleanup of column types
    results_df = pd.read_csv(results_path)  # has 'timestamp', 'elapsed_ms', 'in_flight', 'completions'
    results_df["timestamp"] = pd.to_datetime(results_df["timestamp"], utc=True)
    results_df["elapsed_s"] = pd.to_numeric(results_df["elapsed_ms"], errors="coerce") / 1000.0
    results_df["in_flight"] = pd.to_numeric(results_df["in_flight"], errors="coerce")
    results_df["completions"] = pd.to_numeric(results_df["completions"], errors="coerce")

    # drop rows with missing critical values, sort by elapsed time, and compute sent totals and rates
    results_df = results_df.dropna(subset=["elapsed_s", "in_flight", "completions"]).sort_values("elapsed_s").reset_index(drop=True)
    results_df["sent_cumulative"] = results_df["completions"] + results_df["in_flight"]
    results_df["delta_elapsed_s"] = results_df["elapsed_s"].diff()
    results_df["delta_sent"] = results_df["sent_cumulative"].diff()
    results_df.loc[0, "delta_elapsed_s"] = results_df.loc[0, "elapsed_s"]
    results_df.loc[0, "delta_sent"] = results_df.loc[0, "sent_cumulative"]
    valid_dt = results_df["delta_elapsed_s"] > 0
    results_df["sent_rate_rps"] = np.nan
    results_df.loc[valid_dt, "sent_rate_rps"] = (
        results_df.loc[valid_dt, "delta_sent"] / results_df.loc[valid_dt, "delta_elapsed_s"]
    )

    # load and trim the window results, parse datetimes, and compute elapsed seconds
    window_results_df = pd.read_csv(window_results_path) # has 'window_start', 'window_end'
    window_results_df = trim_window_margins(
        window_results_df,
        start_windows=trim_start,
        end_windows=trim_end,
    ).reset_index(drop=True)
    window_results_df["window_start_dt"] = pd.to_datetime(window_results_df["window_start"], utc=True)
    window_results_df["window_end_dt"] = pd.to_datetime(window_results_df["window_end"], utc=True)
    origin = window_results_df["window_start_dt"].iloc[0]
    window_results_df["elapsed_s"] = (window_results_df["window_start_dt"] - origin).dt.total_seconds()
    window_results_df["window_index"] = np.arange(len(window_results_df))

    # load the window samples, filter to only the kept windows, and ensure value_ms is numeric
    kept_windows = window_results_df[["window_start", "window_end"]].drop_duplicates()
    window_samples_df = pd.read_csv(window_samples_path) # has 'window_start', 'window_end', 'metric_name', 'value_ms'
    window_samples_df["value_ms"] = pd.to_numeric(window_samples_df["value_ms"], errors="coerce")
    window_samples_df = window_samples_df.dropna(subset=["value_ms"])
    window_samples_df = pd.merge(
        window_samples_df,
        kept_windows,
        on=["window_start", "window_end"],
        how="inner",
    )

    return {
        "case_dir": case_dir,
        "results": results_df,
        "window_results": window_results_df,
        "window_samples": window_samples_df,
    }


def save_requests_overlay(output_path, run_dir, baseline_name, baseline_results, abnormal_name, abnormal_results):
    """
    Inputs:
        - run_dir: Path to the run_* directory where the figure will be saved.
        - baseline_name: Name of the baseline case (e.g. "well_provisioned").
        - baseline_results: DataFrame of the baseline case results with 'elapsed_s', 'sent_cumulative', and 'sent_rate_rps' columns.
        - abnormal_name: Name of the abnormal case (e.g. "constrained").
        - abnormal_results: DataFrame of the abnormal case results with 'elapsed_s', 'sent_cumulative', and 'sent_rate_rps' columns.
     Outputs:
        - Saves a figure comparing the cumulative sent requests and sent rate
        over time for the baseline and abnormal cases on the same axes
    """
    configure_plot_style()

    fig, axes = plt.subplots(3, 1, figsize=(13.0, 11.2), sharex=True, constrained_layout=True)
    baseline_style = CASE_STYLES["baseline"]
    abnormal_style = CASE_STYLES["abnormal"]

    # plotting the sent cumulative for both abnormal and normal cases on same axes
    axes[0].plot(
        baseline_results["elapsed_s"],
        baseline_results["sent_cumulative"],
        linewidth=3.0,
        label=baseline_name,
        color=baseline_style["color"],
        linestyle=baseline_style["linestyle"],
        marker=baseline_style["marker"],
        markersize=6.0,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(baseline_results) // 12, 1),
    )
    axes[0].plot(
        abnormal_results["elapsed_s"],
        abnormal_results["sent_cumulative"],
        linewidth=3.0,
        label=abnormal_name,
        color=abnormal_style["color"],
        linestyle=abnormal_style["linestyle"],
        marker=abnormal_style["marker"],
        markersize=6.0,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(abnormal_results) // 12, 1),
    )
    # axes[0].set_title("Load Generator Requests Sent Over Time", loc="left", pad=10)
    axes[0].set_ylabel("Cumulative sent")
    style_axis_grid(axes[0])
    axes[0].legend(frameon=False, handlelength=3.0)

    # plotting the sent rate for both abnormal and normal cases on same axes
    axes[1].plot(
        baseline_results["elapsed_s"],
        baseline_results["sent_rate_rps"],
        linewidth=2.8,
        label=f"{baseline_name} sent rate",
        color=baseline_style["color"],
        linestyle=baseline_style["linestyle"],
        marker=baseline_style["marker"],
        markersize=5.8,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(baseline_results) // 12, 1),
    )
    axes[1].plot(
        abnormal_results["elapsed_s"],
        abnormal_results["sent_rate_rps"],
        linewidth=2.8,
        label=f"{abnormal_name} sent rate",
        color=abnormal_style["color"],
        linestyle=abnormal_style["linestyle"],
        marker=abnormal_style["marker"],
        markersize=5.8,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(abnormal_results) // 12, 1),
    )
    # axes[1].set_title("Load Generator Send Rate Over Time", loc="left", pad=10)
    axes[1].set_ylabel("Sent rate (req/s)")
    axes[1].set_xlabel("Elapsed time (s)")
    style_axis_grid(axes[1])
    axes[1].legend(frameon=False, handlelength=3.0)

    # plotting the number of requests received by the server for both abnormal and normal cases on same axes
    server_log_baseline = run_dir / f"{baseline_name}" / f"{baseline_name}_server.log"
    server_log_abnormal = run_dir / f"{abnormal_name}" / f"{abnormal_name}_server.log"
    if server_log_baseline.exists() and server_log_abnormal.exists():
        def load_server_counts(log_path):
            """Parses the server log"""
            raw = pd.read_csv(log_path, names=["line"], header=None)
            parsed = raw["line"].str.extract(
                r"^(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*served=(?P<received>\d+)"
            )
            parsed = parsed.dropna(subset=["timestamp", "received"]).copy()
            if parsed.empty:
                return parsed
            parsed["timestamp"] = pd.to_datetime(parsed["timestamp"], format="%Y/%m/%d %H:%M:%S")
            parsed["received"] = pd.to_numeric(parsed["received"], errors="coerce")
            parsed = parsed.dropna(subset=["received"]).sort_values("timestamp").reset_index(drop=True)
            parsed["elapsed_s"] = (parsed["timestamp"] - parsed["timestamp"].iloc[0]).dt.total_seconds()
            return parsed

        baseline_server = load_server_counts(server_log_baseline)
        abnormal_server = load_server_counts(server_log_abnormal)

        print(f"baseline_server:\n{baseline_server.head()}\n")
        print(f"abnormal_server:\n{abnormal_server.head()}\n")

        if not baseline_server.empty and not abnormal_server.empty:
            axes[2].plot(
                baseline_server["elapsed_s"],
                baseline_server["received"],
                linewidth=2.8,
                label=f"{baseline_name} received",
                color=baseline_style["color"],
                linestyle=baseline_style["linestyle"],
                marker=baseline_style["marker"],
                markersize=5.8,
                markerfacecolor="white",
                markeredgewidth=1.2,
                markevery=max(len(baseline_server) // 12, 1),
            )
            axes[2].plot(
                abnormal_server["elapsed_s"],
                abnormal_server["received"],
                linewidth=2.8,
                label=f"{abnormal_name} received",
                color=abnormal_style["color"],
                linestyle=abnormal_style["linestyle"],
                marker=abnormal_style["marker"],
                markersize=5.8,
                markerfacecolor="white",
                markeredgewidth=1.2,
                markevery=max(len(abnormal_server) // 12, 1),
            )
            axes[2].set_ylabel("Received by server")
            axes[2].set_xlabel("Elapsed time (s)")
            axes[2].set_title("Requests Received by Server Over Time", loc="left", pad=10)
            style_axis_grid(axes[2])
            axes[2].legend(frameon=False, handlelength=3.0)


    # finalizing and saving the figure
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


def save_cumulative_sent_plot(output_path, baseline_name, baseline_results, abnormal_name, abnormal_results):
    """Save just the cumulative sent requests plot as a standalone figure."""
    configure_plot_style()

    fig, ax = plt.subplots(1, 1, figsize=(8.4, 5.8), constrained_layout=True)
    baseline_style = CASE_STYLES["baseline"]
    abnormal_style = CASE_STYLES["abnormal"]
    ax.plot(
        baseline_results["elapsed_s"],
        baseline_results["sent_cumulative"],
        linewidth=3.0,
        label=baseline_name,
        color=baseline_style["color"],
        linestyle=baseline_style["linestyle"],
        marker=baseline_style["marker"],
        markersize=6.0,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(baseline_results) // 12, 1),
    )
    ax.plot(
        abnormal_results["elapsed_s"],
        abnormal_results["sent_cumulative"],
        linewidth=3.0,
        label=abnormal_name,
        color=abnormal_style["color"],
        linestyle=abnormal_style["linestyle"],
        marker=abnormal_style["marker"],
        markersize=6.0,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(abnormal_results) // 12, 1),
    )
    # ax.set_title("Load Generator Requests Sent Over Time", loc="left", pad=10)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Cumulative sent")
    style_axis_grid(ax)
    ax.legend(frameon=False, handlelength=3.0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


def choose_selected_window(abnormal_windows):
    candidate_metric = "avg_fire_to_dispatch_delay_ms"
    if candidate_metric in abnormal_windows.columns:
        return int(pd.to_numeric(abnormal_windows[candidate_metric], errors="coerce").fillna(-np.inf).idxmax())
    return int(abnormal_windows.index[0])


def build_window_order(window_results_df):
    ordered = window_results_df[["window_start", "window_end", "elapsed_s", "window_index"]].copy()
    if "ll_violation" in window_results_df.columns:
        ordered["ll_violation"] = window_results_df["ll_violation"].fillna(False).astype(bool)
    else:
        ordered["ll_violation"] = False
    return ordered


def save_distribution_comparisons(run_dir, baseline_name, baseline_data, abnormal_name, abnormal_data):
    """
    Inputs:
        - run_dir: Path to the run_* directory where the figures and EMD scores will be saved.
        - baseline_name: Name of the baseline case (e.g. "well_provisioned").
        - baseline_data: Dictionary containing the baseline case data w/ keys "case_dir", "results", "window_results", and "window_samples".
        - abnormal_name: Name of the abnormal case (e.g. "constrained").
        - abnormal_data: Dictionary containing the abnormal case data w/ keys "case_dir", "results", "window_results", and "window_samples".
     Outputs:
        - For each metric in DISTRIBUTION_METRICS, saves "{safe_filename(metric)}_distribution_comparison.png" 
        in the run_dir that compares the distributions of the metric values across all windows for the baseline and abnormal cases. Each figure includes 
        a histogram and a pooled ECDF plot for the baseline and abnormal cases, along with the EMD and normalized EMD values in the title.
    """

    configure_plot_style()

    # obtaining the window samples for both cases
    baseline_samples = baseline_data["window_samples"]
    abnormal_samples = abnormal_data["window_samples"]

    for metric in DISTRIBUTION_METRICS:
        # obtaining the metric values for both cases, skipping if either case has no values for the metric
        baseline_values = baseline_samples.loc[
            baseline_samples["metric_name"] == metric, "value_ms"
        ].to_numpy(dtype=float)
        abnormal_values = abnormal_samples.loc[
            abnormal_samples["metric_name"] == metric, "value_ms"
        ].to_numpy(dtype=float)
        if baseline_values.size == 0 or abnormal_values.size == 0:
            continue

        # computing the EMD and normalized EMD between the two distributions of metric values
        emd_value = wasserstein_distance(baseline_values, abnormal_values)
        normalized_emd_value = normalized_emd(baseline_values, abnormal_values)

        # calculating the bins for histogram 
        all_values = np.concatenate([baseline_values, abnormal_values])
        lower = np.percentile(all_values, 1)
        upper = np.percentile(all_values, 99)
        if lower == upper:
            lower -= 0.5
            upper += 0.5
        bins = np.linspace(lower, upper, 40)

        # clipping the values to the 1st and 99th percentiles for better visualization in the histogram
        baseline_clipped = np.clip(baseline_values, lower, upper)
        abnormal_clipped = np.clip(abnormal_values, lower, upper)

        fig = plt.figure(figsize=(16.8, 6.8))
        gs = fig.add_gridspec(1, 3, width_ratios=[1, 1, 1.2])
        ax_baseline = fig.add_subplot(gs[0, 0])
        ax_abnormal = fig.add_subplot(gs[0, 1], sharey=ax_baseline)
        ax_ecdf = fig.add_subplot(gs[0, 2])

        # plotting the histograms for both abnormal and normal cases on same axes
        _, _, baseline_patches = ax_baseline.hist(
            baseline_clipped,
            bins=bins,
            density=True,
            alpha=0.55,
            color=CASE_STYLES["baseline"]["color"],
            edgecolor="black",
            linewidth=0.8,
        )
        for patch in baseline_patches:
            patch.set_hatch(CASE_STYLES["baseline"]["hatch"])
        ax_baseline.set_title(f"{baseline_name} pooled", loc="left", pad=10)
        ax_baseline.set_xlabel("Value (ms)")
        ax_baseline.set_ylabel("Density")
        style_axis_grid(ax_baseline)

        _, _, abnormal_patches = ax_abnormal.hist(
            abnormal_clipped,
            bins=bins,
            density=True,
            alpha=0.55,
            color=CASE_STYLES["abnormal"]["color"],
            edgecolor="black",
            linewidth=0.8,
        )
        for patch in abnormal_patches:
            patch.set_hatch(CASE_STYLES["abnormal"]["hatch"])
        ax_abnormal.set_title(f"{abnormal_name} pooled", loc="left", pad=10)
        ax_abnormal.set_xlabel("Value (ms)")
        style_axis_grid(ax_abnormal)

        # plotting the ECDFs for both abnormal and normal cases on same axes
        baseline_ecdf_x = np.sort(baseline_values)
        baseline_ecdf_y = np.arange(1, len(baseline_ecdf_x) + 1) / len(baseline_ecdf_x)
        abnormal_ecdf_x = np.sort(abnormal_values)
        abnormal_ecdf_y = np.arange(1, len(abnormal_ecdf_x) + 1) / len(abnormal_ecdf_x)
        ax_ecdf.plot(
            baseline_ecdf_x,
            baseline_ecdf_y,
            color=CASE_STYLES["baseline"]["color"],
            linewidth=2.8,
            linestyle=CASE_STYLES["baseline"]["linestyle"],
            marker=CASE_STYLES["baseline"]["marker"],
            markersize=5.8,
            markerfacecolor="white",
            markeredgewidth=1.2,
            markevery=max(len(baseline_ecdf_x) // 12, 1),
            label=baseline_name,
        )
        ax_ecdf.plot(
            abnormal_ecdf_x,
            abnormal_ecdf_y,
            color=CASE_STYLES["abnormal"]["color"],
            linewidth=2.8,
            linestyle=CASE_STYLES["abnormal"]["linestyle"],
            marker=CASE_STYLES["abnormal"]["marker"],
            markersize=5.8,
            markerfacecolor="white",
            markeredgewidth=1.2,
            markevery=max(len(abnormal_ecdf_x) // 12, 1),
            label=abnormal_name,
        )
        ax_ecdf.set_title("Pooled ECDF", loc="left", pad=10)
        ax_ecdf.set_xlabel("Value (ms)")
        ax_ecdf.set_ylabel("Probability")
        style_axis_grid(ax_ecdf)
        ax_ecdf.legend(frameon=False, handlelength=3.0)

        fig.suptitle(
            f"{metric} distribution comparison\nEMD = {emd_value:.6f} | normalized EMD = {normalized_emd_value:.6f}",
            fontsize=SUPTITLE_SIZE,
        )
        fig.savefig(
            run_dir / f"{safe_filename(metric)}_distribution_comparison.png",
            dpi=180,
            bbox_inches="tight",
            pad_inches=0.04,
        )
        plt.close(fig)


def main():
    args = parse_args()
    run_dir = args.run_dir if args.run_dir is not None else latest_run_dir(args.root_dir)
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    requests_output = args.requests_output if args.requests_output is not None else run_dir / DEFAULT_REQUESTS_OUTPUT
    cumulative_output = args.cumulative_output if args.cumulative_output is not None else run_dir / DEFAULT_CUMULATIVE_OUTPUT

    # loading the baseline and abnormal cases
    baseline_data = load_case(
        run_dir,
        args.baseline_case,
        trim_start=args.trim_start_windows,
        trim_end=args.trim_end_windows,
    )
    abnormal_data = load_case(
        run_dir,
        args.abnormal_case,
        trim_start=args.trim_start_windows,
        trim_end=args.trim_end_windows,
    )

    # plotting the load generator requests sent over time for both cases on the same axes
    if args.cumulative_only:
        save_cumulative_sent_plot(
            cumulative_output,
            "Well Provisioned",
            baseline_data["results"],
            "Constrained",
            abnormal_data["results"],
        )
        print(f"Saved cumulative-only plot to {cumulative_output}")
        return

    save_requests_overlay(
        requests_output,
        run_dir,
        args.baseline_case,
        baseline_data["results"],
        args.abnormal_case,
        abnormal_data["results"],
    )

    # plotting the distributions of window-level metric samples for both cases and saving the EMD scores
    save_distribution_comparisons(
        run_dir,
        args.baseline_case,
        baseline_data,
        args.abnormal_case,
        abnormal_data,
    )

if __name__ == "__main__":
    main()
