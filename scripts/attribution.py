from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance

from utils import trim_window_margins

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# constants
CATEGORIES = ["normal", "cpu_contention", "few_workers", "few_conns"]
ROOT_DIR = Path("samples_DSB_new_interop")
REFERENCE_DIR = Path("refs_DSB_new_interop")
REFERENCE_RESULTS_PATH = REFERENCE_DIR / "eval_rps2000.csv"
REFERENCE_SAMPLES_PATH = REFERENCE_DIR / "eval_samples_rps2000.csv"
REFERENCE_FILENAME_PREFIX = "eval"
OUTPUT_ROOT = ROOT_DIR / "attribution_out"
TRIM_START_WINDOWS = 1
TRIM_END_WINDOWS = 1
ABNORMAL_POOL_WINDOWS = 5   # how many windows to include in the "abnormal" pool for each run (after trimming margins)
SAMPLE_METRICS = [
    "pacer_wait",
    "scheduler_delay",
    # "dispatch_delay",
    "conn_delay",
    "conn_idle_time",
    # "write_delay",
]

# For plotting purposes
METRIC_COLORS = {
    "pacer_wait": "#4C78A8",
    "scheduler_delay": "#F58518",
    # "dispatch_delay": "#E45756",
    "conn_delay": "#72B7B2",
    "conn_idle_time": "#54A24B",
    # "write_delay": "#EECA3B",
}
METRIC_LABELS = {
    "pacer_wait": "Pacer",
    "scheduler_delay": "Scheduler",
    # "dispatch_delay": "dispatch_delay",
    "conn_delay": "Conn Delay",
    "conn_idle_time": "Conn Idle",
    # "write_delay": "write_delay",
}
METRIC_HATCHES = {
    "pacer_wait": "////",
    "scheduler_delay": "\\\\\\\\",
    "conn_delay": "....",
    "conn_idle_time": "xx",
}
CATEGORY_COLORS = {
    "normal": "#4C78A8",
    "cpu_contention": "#E45756",
    "few_workers": "#54A24B",
    "few_conns": "#F58518",
}
CATEGORY_LINE_STYLES = {
    "normal": "-",
    "cpu_contention": "--",
    "few_workers": ":",
    "few_conns": "-.",
}
CATEGORY_MARKERS = {
    "normal": "o",
    "cpu_contention": "s",
    "few_workers": "^",
    "few_conns": "D",
}
CATEGORY_BAND_HATCHES = {
    "normal": "////",
    "cpu_contention": "\\\\\\\\",
    "few_workers": "....",
    "few_conns": "xx",
}
CATEGORY_TITLES = {
    "normal": "Normal",
    "cpu_contention": "CPU Contention",
    "few_workers": "Few Workers",
    "few_conns": "Few Connections",
}
OVERVIEW_CATEGORIES = ["cpu_contention", "few_workers", "few_conns"]
BASE_FONT_SIZE = 26
AXIS_LABEL_SIZE = 30
TITLE_SIZE = 32
SUPTITLE_SIZE = 34
TICK_LABEL_SIZE = 24
LEGEND_SIZE = 24
PANEL_LABEL_SIZE = 28
X_TICK_LABEL_SIZE = 30
X_AXIS_LABEL_SIZE = 34


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
        }
    )


def load_run_samples(category, run_id, trim=True):
    """
    Load the samples for a specific run and category.
    """

    # loading the results and samples
    results_path = ROOT_DIR / f"window_results_{category}_run{run_id}.csv"
    samples_path = ROOT_DIR / f"window_samples_{category}_run{run_id}.csv"

    window_results = pd.read_csv(results_path)
    window_samples = pd.read_csv(samples_path)
    window_samples["value_ms"] = pd.to_numeric(window_samples["value_ms"], errors="coerce")

    if not trim:
        return window_results, window_samples

    # trimming the windows to remove potential startup/shutdown artifacts
    trimmed_results = trim_window_margins(
        window_results,
        start_windows=TRIM_START_WINDOWS,
        end_windows=TRIM_END_WINDOWS,
    )
    trimmed_keys = trimmed_results[["window_start", "window_end"]].drop_duplicates()
    trimmed_samples = trimmed_keys.merge(
        window_samples,
        on=["window_start", "window_end"],
        how="left",
    )
    return trimmed_results, trimmed_samples


def load_reference_samples(trim=True):
    """
    Load the reference samples for the reference run w/ trimming.
    """
    return load_reference_samples_for_paths(REFERENCE_RESULTS_PATH, REFERENCE_SAMPLES_PATH, trim=trim)


def load_reference_samples_for_paths(reference_results_path, reference_samples_path, trim=True):
    """
    Load reference results and samples from explicit CSV paths.
    """
    # loading the reference results
    reference_results = pd.read_csv(reference_results_path)
    reference_samples = pd.read_csv(reference_samples_path)
    reference_samples["value_ms"] = pd.to_numeric(reference_samples["value_ms"], errors="coerce")

    if not trim:
        return reference_results, reference_samples

    trimmed_results = trim_window_margins(
        reference_results,
        start_windows=TRIM_START_WINDOWS,
        end_windows=TRIM_END_WINDOWS,
    )
    trimmed_keys = trimmed_results[["window_start", "window_end"]].drop_duplicates()
    trimmed_samples = trimmed_keys.merge(
        reference_samples,
        on=["window_start", "window_end"],
        how="left",
    )
    return trimmed_results, trimmed_samples


def load_reference_samples_for_rps(rps, trim=True, reference_dir=REFERENCE_DIR):
    """
    Load reference results and samples for a specific RPS level.
    """
    reference_results_path = reference_dir / f"{REFERENCE_FILENAME_PREFIX}_rps{rps}.csv"
    reference_samples_path = reference_dir / f"{REFERENCE_FILENAME_PREFIX}_samples_rps{rps}.csv"
    return load_reference_samples_for_paths(reference_results_path, reference_samples_path, trim=trim)


def pooled_sample_values(window_samples, metric_name, pooled_keys):
    """
    For a given metric, pool the sample values across all windows defined in pooled_keys.
    """
    pooled_metric = pooled_keys.merge(
        window_samples[window_samples["metric_name"] == metric_name],
        on=["window_start", "window_end"],
        how="left",
    )
    return pooled_metric["value_ms"].dropna()


def metric_window_keys(window_samples, metric_name):
    """
    Get the unique window keys for a specific metric.
    """
    return (
        window_samples[window_samples["metric_name"] == metric_name][["window_start", "window_end"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def abnormal_pool_keys(trimmed_results):
    """
    Get the keys for the "abnormal" pool of windows.
    """
    return (
        trimmed_results[["window_start", "window_end", "window_start_dt"]]
        .drop_duplicates()
        .sort_values(["window_start_dt", "window_end"])
        .head(ABNORMAL_POOL_WINDOWS)[["window_start", "window_end"]]
        .reset_index(drop=True)
    )


def summarize_run_metric(category, run_id, reference_keys, reference_samples):
    """
    Summarizes the metrics for a given category and run_id.
        - computes mean and EMD for each metric compared to the reference samples
    """

    # loading the run for the category and run_id
    category_results, category_samples = load_run_samples(category, run_id)

    # should be window_start and window_end
    category_keys = abnormal_pool_keys(category_results)

    rows = []
    for metric_name in SAMPLE_METRICS:
        # get the reference values for the metric
        reference_values = pooled_sample_values(reference_samples, metric_name, reference_keys)

        # get the category values for the metric for this run (pooled across the abnormal windows)
        category_values = pooled_sample_values(category_samples, metric_name, category_keys)
        used_metric_fallback = False
        forced_zero_emd = False

        # handling connection idle time separately
        if metric_name == "conn_idle_time" and category_values.empty:
            _, full_category_samples = load_run_samples(category, run_id, trim=False)
            full_metric_keys = metric_window_keys(full_category_samples, metric_name)
            if not full_metric_keys.empty and not reference_values.empty:
                category_values = pd.Series([0.0], dtype=float) # setting to 0 for default
                forced_zero_emd = True

        if reference_values.empty or category_values.empty:
            continue

        # save the summary for this metric and run
        rows.append(
            {
                "category": category,
                "run_id": run_id,
                "metric_name": metric_name,
                "emd": 0.0 if forced_zero_emd else wasserstein_distance(reference_values, category_values),
                "reference_mean": reference_values.mean(),
                "reference_std": reference_values.std(ddof=1),
                "category_mean": category_values.mean(),
                "category_std": category_values.std(ddof=1) if len(category_values) > 1 else 0.0,
                "reference_count": len(reference_values),
                "category_count": len(category_values),
                "used_metric_fallback": used_metric_fallback,
                "forced_zero_emd": forced_zero_emd,
            }
        )

    return pd.DataFrame(rows)


def aggregate_metric_emd(summary_df):
    """
    From the summary, calculates average EMD and stddev for each metric across runs
    """

    if summary_df.empty:
        return pd.DataFrame()

    agg_df = (
        summary_df.groupby("metric_name", as_index=False)
        .agg(
            emd_mean=("emd", "mean"),   # average EMD across runs for this metric
            emd_std=("emd", lambda vals: vals.std(ddof=1) if len(vals) > 1 else 0.0),   # stddev of EMD across runs for this metric
            run_count=("run_id", "nunique"),    # how many runs contributed to this metric's EMD calculation
        )
        .set_index("metric_name")
        .reindex(SAMPLE_METRICS)
        .dropna(subset=["emd_mean"])
        .reset_index()
    )

    return agg_df


def draw_category_panel(ax, summary_df, category, show_ylabel):
    # obtain the summary
    metric_df = aggregate_metric_emd(summary_df)
    x = np.arange(len(metric_df), dtype=float)

    # plotting
    bars = ax.bar(
        x,
        metric_df["emd_mean"],
        width=0.68,
        color=[METRIC_COLORS[metric_name] for metric_name in metric_df["metric_name"]],
        edgecolor="black",
        linewidth=1.0,
        yerr=metric_df["emd_std"],
        capsize=3.0,
        error_kw={"elinewidth": 0.9, "alpha": 0.85, "capthick": 0.9},
    )
    for bar, metric_name in zip(bars, metric_df["metric_name"]):
        bar.set_hatch(METRIC_HATCHES.get(metric_name, ""))

    ax.set_xticks(x)
    ax.set_xticklabels(
        [METRIC_LABELS.get(metric_name, metric_name) for metric_name in metric_df["metric_name"]],
        rotation=0,
        ha="center",
        fontsize=X_TICK_LABEL_SIZE,
        fontweight="bold",
    )
    ax.tick_params(axis="x", pad=10)
    if show_ylabel:
        ax.set_ylabel("Mean EMD vs reference", fontsize=AXIS_LABEL_SIZE)
    ax.set_title(CATEGORY_TITLES.get(category, category), fontsize=TITLE_SIZE, pad=12)
    ax.grid(True, axis="y", alpha=0.22, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def plot_combined_overview(category_frames, output_path):
    configure_plot_style()
    available_categories = [
        category
        for category in OVERVIEW_CATEGORIES
        if category in category_frames and not category_frames[category].empty
    ]
    if not available_categories:
        return

    fig, axes = plt.subplots(
        1,
        len(available_categories),
        figsize=(10.5 * len(available_categories), 9.2),
        constrained_layout=True,
        sharex=True,
        sharey=True,
    )
    if len(available_categories) == 1:
        axes = [axes]

    for idx, (ax, category) in enumerate(zip(axes, available_categories)):
        draw_category_panel(
            ax,
            category_frames[category],
            category,
            show_ylabel=(idx == 0),
        )
        ax.text(
            0.01,
            1.03,
            f"{chr(ord('a') + idx)})",
            transform=ax.transAxes,
            fontsize=PANEL_LABEL_SIZE,
            fontweight="bold",
        )
    fig.savefig(output_path)
    plt.close(fig)


def build_r_timeseries(category, run_ids):
    """
    For a given category, builds a timeseries of observed R
    """
    rows = []
    for run_id in run_ids:
        # load the trimmed results for a specific category and run_id
        trimmed_results, _ = load_run_samples(category, run_id)
        if trimmed_results.empty or "observed_R" not in trimmed_results.columns:
            continue

        run_results = trimmed_results.copy()

        # getting the observed R
        run_results["observed_R"] = pd.to_numeric(run_results["observed_R"], errors="coerce")
        run_results = run_results.dropna(subset=["observed_R", "window_start_dt"]).reset_index(drop=True)
        if run_results.empty:
            continue

        # building up the elapsed time
        start_time = run_results["window_start_dt"].iloc[0]
        run_results["elapsed_s"] = (
            (run_results["window_start_dt"] - start_time).dt.total_seconds()
        )
        run_results["window_idx"] = np.arange(len(run_results), dtype=int)

        # for a specific run
        rows.append(
            run_results[["window_idx", "elapsed_s", "observed_R"]].assign(
                category=category,
                run_id=run_id,
            )
        )

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)


def plot_observed_r_over_time(category_run_ids, output_path):
    """Plots the observed R over time for each category, averaged across runs with variability bands."""
    configure_plot_style()
    category_series = {}
    for category in CATEGORIES:
        # construct the timeseries for this category across all its runs and save if available
        series_df = build_r_timeseries(category, category_run_ids.get(category, []))
        if not series_df.empty:
            # store the timeseries for this category if it has valid data
            category_series[category] = series_df

    if not category_series:
        return

    fig, ax = plt.subplots(figsize=(11.0, 6.8), constrained_layout=True)

    for category in CATEGORIES:
        if category not in category_series:
            continue

        # get the mean and stddev of observed R for this category across runs in each time window
        summary_df = (
            category_series[category]
            .groupby("window_idx", as_index=False)
            .agg(
                elapsed_s=("elapsed_s", "mean"),
                observed_R_mean=("observed_R", "mean"),
                observed_R_std=("observed_R", lambda vals: vals.std(ddof=1) if len(vals) > 1 else 0.0),
            )
            .sort_values("window_idx")
        )

        color = CATEGORY_COLORS.get(category, "#444444")
        line_style = CATEGORY_LINE_STYLES.get(category, "-")
        marker = CATEGORY_MARKERS.get(category, "o")
        # plot the trendline
        ax.plot(
            summary_df["elapsed_s"],
            summary_df["observed_R_mean"],
            label=CATEGORY_TITLES.get(category, category),
            color=color,
            linewidth=2.0,
            linestyle=line_style,
            marker=marker,
            markersize=5.5,
            markerfacecolor="white",
            markeredgewidth=1.2,
            markevery=max(len(summary_df) // 10, 1),
        )
        # plot the variability band if we have stddev information
        if summary_df["observed_R_std"].gt(0).any():
            lower = summary_df["observed_R_mean"] - summary_df["observed_R_std"]
            upper = summary_df["observed_R_mean"] + summary_df["observed_R_std"]
            ax.fill_between(
                summary_df["elapsed_s"],
                lower,
                upper,
                facecolor="white",
                edgecolor=color,
                hatch=CATEGORY_BAND_HATCHES.get(category, "////"),
                linewidth=0.0,
                alpha=0.12,
            )

    ax.set_xlabel("Time since trimmed run start (s)")
    ax.set_ylabel("Observed R")
    ax.set_title("Observed R over time across categories", fontsize=TITLE_SIZE, pad=12)
    ax.grid(True, alpha=0.22, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(frameon=False, ncol=2, fontsize=LEGEND_SIZE)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def analyze_category(category, run_ids):

    # get the reference samples and keys once for all runs in this category
    reference_results, reference_samples = load_reference_samples()
    reference_keys = reference_results[["window_start", "window_end"]].drop_duplicates()

    run_frames = []
    for run_id in run_ids:
        # get the summary
        run_df = summarize_run_metric(category, run_id, reference_keys, reference_samples)
        if run_df.empty:
            print(f"Skipping {category} run{run_id}: no sample metrics were available.")
            continue
        
        # for a category and each metric, consists of the EMD, mean, and stddev for a specific run compared to the reference
        run_df = run_df.sort_values("emd", ascending=False).reset_index(drop=True)
        run_frames.append(run_df)

    if not run_frames:
        return pd.DataFrame()

    # concatenating the summary for all runs in this category
    summary_df = pd.concat(run_frames, ignore_index=True)
    return summary_df


def detect_run_ids():
    """
    Returns dictionary mapping each category to the list of run IDs for which we have both samples and results available.
    """
    run_ids_by_category = {}
    for category in CATEGORIES:
        sample_runs = {
            int(path.stem.split("run")[-1]) for path in ROOT_DIR.glob(f"window_samples_{category}_run*.csv")
        }
        result_runs = {
            int(path.stem.split("run")[-1]) for path in ROOT_DIR.glob(f"window_results_{category}_run*.csv")
        }
        run_ids_by_category[category] = sorted(sample_runs & result_runs)
    return run_ids_by_category


def main():
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    run_ids_by_category = detect_run_ids()

    category_frames = {}    # to consist of full summary DataFrame for each category to be used for plotting and analysis
    for category in CATEGORIES:
        category_frames[category] = analyze_category(category, run_ids_by_category.get(category, []))

    plot_combined_overview(category_frames, OUTPUT_ROOT / "paper_attribution_overview.pdf")
    plot_observed_r_over_time(run_ids_by_category, OUTPUT_ROOT / "observed_r_over_time.pdf")


if __name__ == "__main__":
    main()
