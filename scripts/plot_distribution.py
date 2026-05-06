import os
import random

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance

from utils import safe_filename, trim_window_margins

# constants
CATEGORIES = ["cpu_contention", "few_workers", "few_conns"]
ROOT_DIR = "Slides - Chirag/samples"    # renamed to fixed_delay_exp
TRIM_START_WINDOWS = 0
TRIM_END_WINDOWS = 1
CSV_PATH_NORMAL = "normal"
OUTPUT_ROOT = "Slides - Chirag/dist_out"
ABNORMAL_WINDOW_INDEX = 0
TRANSPORT_SCALARS = ["reuse_frac", "fresh_conn_frac", "was_idle_given_reused"]
RANDOM_SEED = 42


def load_window_tables(category):
    window_results_csv = os.path.join(ROOT_DIR, f"window_results_{category}.csv")
    window_samples_csv = os.path.join(ROOT_DIR, f"window_samples_{category}.csv")

    window_results = pd.read_csv(window_results_csv)
    window_samples = pd.read_csv(window_samples_csv)
    window_samples["value_ms"] = window_samples["value_ms"].astype(float)

    window_results = trim_window_margins(
        window_results,
        start_windows=TRIM_START_WINDOWS,
        end_windows=TRIM_END_WINDOWS,
    )

    return window_results, window_samples


def pick_normal_window(df_normal, rng):
    normal_windows = list(
        df_normal[df_normal["ll_violation"] == False][["window_start", "window_end"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    if not normal_windows:
        raise ValueError("No windows found in normal run.")

    normal_start, normal_end = rng.choice(normal_windows)
    normal_window_df = df_normal[
        (df_normal["window_start"] == normal_start) &
        (df_normal["window_end"] == normal_end)
    ]
    return normal_start, normal_end, normal_window_df


def pick_abnormal_window(window_results_abnormal, df_abnormal):
    abnormal_window_rows = (
        window_results_abnormal[window_results_abnormal["ll_violation"] == True]
        .sort_values(["window_start_dt", "window_end"])
    )

    if abnormal_window_rows.empty:
        print("No LL violations found in abnormal run, selecting a random window instead.")
        abnormal_windows = list(
            df_abnormal[["window_start", "window_end"]]
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        if not abnormal_windows:
            raise ValueError("No windows found in abnormal run.")
        abnormal_start, abnormal_end = random.choice(abnormal_windows)
    else:
        selected_abnormal_idx = min(ABNORMAL_WINDOW_INDEX, len(abnormal_window_rows) - 1)
        selected_abnormal = abnormal_window_rows.iloc[selected_abnormal_idx]
        abnormal_start = selected_abnormal["window_start"]
        abnormal_end = selected_abnormal["window_end"]
        print(f"Selected ABNORMAL window #{selected_abnormal_idx}:\n{abnormal_start} → {abnormal_end}")

    abnormal_window_df = df_abnormal[
        (df_abnormal["window_start"] == abnormal_start) &
        (df_abnormal["window_end"] == abnormal_end)
    ]
    return abnormal_start, abnormal_end, abnormal_window_df


def plot_metric_distributions(
    output_dir,
    metric,
    emd,
    normal_values,
    abnormal_values,
    metric_drift,
    abnormal_start,
    abnormal_end,
):
    all_values = np.concatenate([normal_values, abnormal_values])
    lower = np.percentile(all_values, 1)
    upper = np.percentile(all_values, 99)

    normal_clipped = np.clip(normal_values, lower, upper)
    abnormal_clipped = np.clip(abnormal_values, lower, upper)
    bins = np.linspace(lower, upper, 40)

    fig = plt.figure(figsize=(16, 5))
    gs = fig.add_gridspec(1, 3, width_ratios=[1, 1, 1.4])
    ax_normal = fig.add_subplot(gs[0, 0])
    ax_abnormal = fig.add_subplot(gs[0, 1], sharey=ax_normal)
    ax_trend = fig.add_subplot(gs[0, 2])

    ax_normal.hist(normal_clipped, bins=bins, density=True, alpha=0.7)
    ax_normal.set_title("Normal")
    ax_normal.set_xlabel("Value (ms)")
    ax_normal.set_ylabel("Density")

    ax_abnormal.hist(abnormal_clipped, bins=bins, density=True, alpha=0.7)
    ax_abnormal.set_title("Selected abnormal")
    ax_abnormal.set_xlabel("Value (ms)")

    violating = metric_drift[metric_drift["ll_violation"]]
    non_violating = metric_drift[~metric_drift["ll_violation"]]
    ax_trend.plot(metric_drift["elapsed_s"], metric_drift["drift"], color="tab:blue", linewidth=1.5)

    if not non_violating.empty:
        ax_trend.scatter(
            non_violating["elapsed_s"],
            non_violating["drift"],
            s=20,
            alpha=0.7,
            color="tab:blue",
            label="No LL violation",
        )
    if not violating.empty:
        ax_trend.scatter(
            violating["elapsed_s"],
            violating["drift"],
            s=28,
            color="tab:red",
            label="LL violation",
        )

    selected_point = metric_drift[
        (metric_drift["window_start"] == abnormal_start) &
        (metric_drift["window_end"] == abnormal_end)
    ].iloc[0]
    ax_trend.scatter(
        [selected_point["elapsed_s"]],
        [selected_point["drift"]],
        s=90,
        facecolors="none",
        edgecolors="black",
        linewidths=1.5,
        label="Selected window",
        zorder=5,
    )
    ax_trend.set_title("Abnormal run over time")
    ax_trend.set_xlabel("Elapsed time from run start (s)")
    ax_trend.set_ylabel("Wasserstein distance vs healthy baseline")
    ax_trend.grid(True, alpha=0.3)
    ax_trend.legend()

    fig.suptitle(f"{metric} distribution comparison\nEMD = {emd:.6f}", fontsize=12)
    plt.tight_layout()
    fig.savefig(
        os.path.join(output_dir, f"{safe_filename(metric)}_distribution.png"),
        dpi=160,
        bbox_inches="tight",
    )
    plt.close(fig)


def plot_transport_scalars(
    output_dir,
    window_results_normal,
    window_results_abnormal,
    abnormal_window_order,
    abnormal_start,
    abnormal_end,
):
    healthy_results_only = window_results_normal[window_results_normal["ll_violation"] == False]
    for scalar_metric in TRANSPORT_SCALARS:
        if scalar_metric not in window_results_abnormal.columns or scalar_metric not in healthy_results_only.columns:
            print(f"Skipping scalar plot for {scalar_metric}: metric not found in both result tables.")
            continue

        baseline_median = healthy_results_only[scalar_metric].median()
        scalar_series = abnormal_window_order.merge(
            window_results_abnormal[["window_start", "window_end", scalar_metric]],
            on=["window_start", "window_end"],
            how="left",
        )

        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.plot(
            scalar_series["elapsed_s"],
            scalar_series[scalar_metric],
            color="tab:green",
            linewidth=1.5,
        )

        violating = scalar_series[scalar_series["ll_violation"]]
        non_violating = scalar_series[~scalar_series["ll_violation"]]

        if not non_violating.empty:
            ax.scatter(
                non_violating["elapsed_s"],
                non_violating[scalar_metric],
                s=20,
                alpha=0.7,
                color="tab:green",
                label="No LL violation",
            )
        if not violating.empty:
            ax.scatter(
                violating["elapsed_s"],
                violating[scalar_metric],
                s=28,
                color="tab:red",
                label="LL violation",
            )

        ax.axhline(
            baseline_median,
            color="tab:gray",
            linestyle="--",
            linewidth=1.2,
            label=f"Healthy median = {baseline_median:.3f}",
        )

        selected_scalar_point = scalar_series[
            (scalar_series["window_start"] == abnormal_start) &
            (scalar_series["window_end"] == abnormal_end)
        ].iloc[0]
        ax.scatter(
            [selected_scalar_point["elapsed_s"]],
            [selected_scalar_point[scalar_metric]],
            s=90,
            facecolors="none",
            edgecolors="black",
            linewidths=1.5,
            label="Selected window",
            zorder=5,
        )

        ax.set_title(f"{scalar_metric} over abnormal run")
        ax.set_xlabel("Elapsed time from run start (s)")
        ax.set_ylabel(scalar_metric)
        ax.grid(True, alpha=0.3)
        ax.legend()
        plt.tight_layout()
        fig.savefig(
            os.path.join(output_dir, f"{safe_filename(scalar_metric)}_timeseries.png"),
            dpi=160,
            bbox_inches="tight",
        )
        plt.close(fig)


def analyze_category(
    category,
    window_results_normal,
    window_samples_normal,
    rng,
):
    output_dir = os.path.join(OUTPUT_ROOT, category)
    os.makedirs(output_dir, exist_ok=True)

    window_results_abnormal, window_samples_abnormal = load_window_tables(category)

    df_normal = pd.merge(
        window_samples_normal,
        window_results_normal,
        on=["window_start", "window_end"],
        how="inner",
    )
    df_abnormal = pd.merge(
        window_samples_abnormal,
        window_results_abnormal,
        on=["window_start", "window_end"],
        how="inner",
    )

    normal_start, normal_end, normal_window_df = pick_normal_window(df_normal, rng)
    print(f"Selected NORMAL window:\n{normal_start} → {normal_end}")

    abnormal_start, abnormal_end, abnormal_window_df = pick_abnormal_window(
        window_results_abnormal,
        df_abnormal,
    )

    common_metrics = sorted(
        set(normal_window_df["metric_name"].unique()) &
        set(abnormal_window_df["metric_name"].unique())
    )

    healthy_window_keys = (
        window_results_normal[window_results_normal["ll_violation"] == False][["window_start", "window_end"]]
        .drop_duplicates()
    )
    healthy_baseline_df = pd.merge(
        window_samples_normal,
        healthy_window_keys,
        on=["window_start", "window_end"],
        how="inner",
    )

    abnormal_window_order = (
        window_results_abnormal[["window_start", "window_end", "window_start_dt", "ll_violation"]]
        .drop_duplicates()
        .sort_values(["window_start_dt", "window_end"])
        .reset_index(drop=True)
    )
    abnormal_window_order["elapsed_s"] = (
        abnormal_window_order["window_start_dt"] - abnormal_window_order["window_start_dt"].iloc[0]
    ).dt.total_seconds()

    emd_rows = []
    for metric in common_metrics:
        normal_values = normal_window_df[
            normal_window_df["metric_name"] == metric
        ]["value_ms"].to_numpy()
        abnormal_values = abnormal_window_df[
            abnormal_window_df["metric_name"] == metric
        ]["value_ms"].to_numpy()
        baseline_values = healthy_baseline_df[
            healthy_baseline_df["metric_name"] == metric
        ]["value_ms"].to_numpy()

        emd = wasserstein_distance(normal_values, abnormal_values)

        drift_rows = []
        for row in abnormal_window_order.itertuples(index=False):
            window_values = df_abnormal[
                (df_abnormal["window_start"] == row.window_start) &
                (df_abnormal["window_end"] == row.window_end) &
                (df_abnormal["metric_name"] == metric)
            ]["value_ms"].to_numpy()
            if len(window_values) == 0 or len(baseline_values) == 0:
                drift = np.nan
            else:
                drift = wasserstein_distance(baseline_values, window_values)

            drift_rows.append(
                {
                    "window_start": row.window_start,
                    "window_end": row.window_end,
                    "elapsed_s": row.elapsed_s,
                    "ll_violation": row.ll_violation,
                    "drift": drift,
                }
            )

        metric_drift = pd.DataFrame(drift_rows)
        plot_metric_distributions(
            output_dir,
            metric,
            emd,
            normal_values,
            abnormal_values,
            metric_drift,
            abnormal_start,
            abnormal_end,
        )

        emd_rows.append(
            {
                "category": category,
                "metric_name": metric,
                "emd": emd,
                "normal_window_start": normal_start,
                "normal_window_end": normal_end,
                "abnormal_window_start": abnormal_start,
                "abnormal_window_end": abnormal_end,
            }
        )

    plot_transport_scalars(
        output_dir,
        window_results_normal,
        window_results_abnormal,
        abnormal_window_order,
        abnormal_start,
        abnormal_end,
    )

    emd_df = pd.DataFrame(emd_rows).sort_values("emd", ascending=False)
    emd_df.to_csv(os.path.join(output_dir, "emd_results.csv"), index=False)

    print(f"\nEMD / Wasserstein distance by metric for {category}:")
    for row in emd_df.itertuples(index=False):
        print(f"{row.metric_name}: {row.emd:.6f}")
    print(f"\nSaved plots and EMD summary to: {output_dir}")


def main():
    random.seed(RANDOM_SEED)
    rng = random.Random(RANDOM_SEED)

    window_results_normal, window_samples_normal = load_window_tables(CSV_PATH_NORMAL)
    for category in CATEGORIES:
        print(f"\n=== Analyzing category: {category} ===")
        analyze_category(category, window_results_normal, window_samples_normal, rng)


if __name__ == "__main__":
    main()
