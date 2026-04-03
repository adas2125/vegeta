#!/usr/bin/env python3

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from utils import safe_metric_name

DEFAULT_RESULTS_DIR = Path("experiments/out")
DEFAULT_SCAN = "healthy_HotelReservation_http1_cycle_20260401_215615"

# RPS levels for healthy LG runs
DEFAULT_RPS = [1000, 2000, 3000]
DEFAULT_RUNS = [1, 2]

# trimming window parameters
DEFAULT_SKIP_FIRST = 2
DEFAULT_SKIP_LAST = 1

# how many percentile knots to use when interpolating across RPS
DEFAULT_K_VALUE = 99
DEFAULT_EVAL_GRID_SIZE = 1000

# metrics to monitor (client-side)
LG_SAMPLE_METRICS = [
    "scheduler_delay", "conn_delay",
    "response_tail_time", "conn_idle_time",
    "write_delay", "dispatch_delay", "pacer_wait",
]

def parse_args():
    """
    Command-line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--scan", default=DEFAULT_SCAN)
    parser.add_argument("--rps", type=int, nargs="+", default=DEFAULT_RPS)
    parser.add_argument("--runs", type=int, nargs="+", default=DEFAULT_RUNS)
    parser.add_argument("--metrics", nargs="+", default=LG_SAMPLE_METRICS)
    parser.add_argument("--skip-first", type=int, default=DEFAULT_SKIP_FIRST)
    parser.add_argument("--skip-last", type=int, default=DEFAULT_SKIP_LAST)
    parser.add_argument("--k-value", type=int, default=DEFAULT_K_VALUE)
    parser.add_argument("--eval-grid-size", type=int, default=DEFAULT_EVAL_GRID_SIZE)
    return parser.parse_args()


def make_percentile_grid(k: int) -> np.ndarray:
    """
    Create a percentile grid for the quantiles
    Args:
        k: Number of grid points

    Returns:
        np.ndarray: A 1-D array of shape (k,) spanning percentiles from
        0.01 to 0.99 inclusive.
    """
    return np.linspace(0.01, 0.99, k)


def interpolate_in_rps(
    r_target: int,
    r_lo: int,
    q_lo: np.ndarray,
    r_hi: int,
    q_hi: np.ndarray,
) -> np.ndarray:
    """
    Linearly interpolate a target quantile curve between two RPS anchors.

    Args:
        r_target (int): RPS value to predict.
        r_lo (int): Lower neighboring RPS anchor.
        q_lo (np.ndarray (shape (k,))): Quantile values for r_lo on a shared percentile grid.
        r_hi (int): Upper neighboring RPS anchor.
        q_hi (np.ndarray (shape (k,))): Quantile values for r_hi on the same percentile grid.

    Returns:
        np.ndarray (shape (k,)): Predicted quantile values for r_target on that same
        percentile grid.
    """
    alpha = (r_target - r_lo) / float(r_hi - r_lo)
    r_target_quantile_vals = (1.0 - alpha) * q_lo + alpha * q_hi
    return r_target_quantile_vals


def trapezoid_integral(y: np.ndarray, x: np.ndarray) -> float:
    """
    Compute a trapezoidal integral over the eval-grid percentiles

    Args:
        y (np.ndarray (shape (eval-grid-size,))): Function values to integrate.
        x (np.ndarray (shape (eval-grid-size,))): Monotonic sample locations associated with y.

    Returns:
        float: Approximate integral of y with respect to x.
    """
    return float(np.trapz(y, x))


def wasserstein1_from_quantiles(
    q_true: np.ndarray,
    q_hat: np.ndarray,
    percentiles: np.ndarray,
) -> float:
    """
    Compute Wasserstein-1 / EMD between two quantile curves.

    Args:
        q_true (np.ndarray (shape (eval-grid-size,))): Quantile values for the actual distribution.
        q_hat (np.ndarray (shape (eval-grid-size,))): Quantile values for the predicted or baseline distribution.
        percentiles (np.ndarray (shape (eval-grid-size,))): Percentile grid on which both curves are defined.

    Returns:
        float: Estimated Wasserstein-1 distance
    """
    # this is the wassetein distance between the two distributions
    return trapezoid_integral(np.abs(q_true - q_hat), percentiles)


def empirical_quantile_curve(samples: np.ndarray, percentiles: np.ndarray) -> np.ndarray:
    """
    Evaluate the empirical quantile curve for one set of samples.

    Args:
        samples: Raw sample values for one run of one metric.
        percentiles (np.ndarray (shape (grid-size,))): Percentile locations where the empirical quantile function
        should be evaluated.

    Returns:
        np.ndarray (shape (grid-size,)): Quantile values at each requested percentile.
    """
    samples = np.asarray(samples, dtype=float)
    # return values at the requested percentiles, e.g. 0.01, 0.02, ..., 0.99
    return np.quantile(samples, percentiles)


def pointwise_reference_curve(run_samples: List[np.ndarray], percentiles: np.ndarray) -> np.ndarray:
    """
    Aggregate multiple runs into one reference quantile curve.

    Args:
        run_samples: List of per-run sample arrays for the same metric and RPS.
        percentiles (np.ndarray (shape (grid-size,))): Percentile grid where each run's quantile curve should be
        evaluated.

    Returns:
        np.ndarray (shape (grid-size,)): A pointwise median quantile curve across all non-empty runs.
    """
    curves = [empirical_quantile_curve(samples, percentiles) for samples in run_samples if samples.size > 0]
    curves_stacked = np.vstack(curves)  # shape (3, grid-size) if there are 3 runs
    return np.median(curves_stacked, axis=0)


def predict_curve_from_neighbors(
    target_rps: int,
    low_rps: int,
    low_curve: np.ndarray,
    high_rps: int,
    high_curve: np.ndarray,
    knot_grid: np.ndarray,
    eval_grid: np.ndarray,
) -> np.ndarray:
    """
    Predict a target quantile curve from neighboring RPS curves.

    Args:
        target_rps (int): RPS value to predict.
        low_rps (int): Lower neighboring RPS anchor.
        low_curve (np.ndarray (shape (grid-size,))): Quantile curve for low_rps evaluated on knot_grid.
        high_rps (int): Upper neighboring RPS anchor.
        high_curve (np.ndarray (shape (grid-size,))): Quantile curve for high_rps evaluated on knot_grid.
        knot_grid (np.ndarray (shape (k-value,))): Coarse percentile grid used for interpolation.
        eval_grid (np.ndarray (shape (eval-grid-size,))): Dense percentile grid used for final comparison and plots.

    Returns:
        np.ndarray (shape (eval-grid-size,)): Predicted target quantile curve evaluated on eval_grid.
    """

    # interpolate the two neighboring curves at the knot grid percentiles to get the predicted curve at the knots
    interpolated_knots = interpolate_in_rps(target_rps, low_rps, low_curve, high_rps, high_curve)

    # extend the predicted curve from the knot grid to the eval grid using linear interpolation of the quantiles
    return np.interp(eval_grid, knot_grid, interpolated_knots)


def nearest_neighbor_rps(target_rps: int, candidate_rps: List[int]) -> int:
    """
    Pick the nearest neighboring RPS for a baseline predictor.

    Args:
        target_rps (int): RPS value being predicted.
        candidate_rps (List[int]): Candidate anchor RPS values, typically the immediate
        lower and upper neighbors.

    Returns:
        int: The nearest candidate RPS, preferring the lower candidate on ties.
    """
    return min(candidate_rps, key=lambda rps: (abs(rps - target_rps), rps > target_rps, rps))


def load_trimmed_window_results(path: Path, skip_first: int, skip_last: int) -> pd.DataFrame:
    """
    Load window-level results and trim edge windows.
    Returns:     pd.DataFrame:
        - The trimmed window-results table containing only kept windows.
    """
    df = pd.read_csv(path)

    order = df[["window_start", "window_end"]].drop_duplicates().reset_index(drop=True)
    start = min(skip_first, len(order))
    stop = len(order) - min(skip_last, max(len(order) - start, 0))
    kept = order.iloc[start:stop].copy()

    trimmed = df.merge(kept, on=["window_start", "window_end"], how="inner")
    return trimmed


def load_trimmed_metric_samples(
    path: Path,
    kept_windows: pd.DataFrame,
    metrics: List[str],
) -> Dict[str, np.ndarray]:
    """
    Load trimmed sample values for each requested metric.
    Returns:
        Dict[str, np.ndarray]: Mapping from metric name to a 1-D numpy array of
        retained sample values in milliseconds.
    """

    # read the CSV
    df = pd.read_csv(
        path,
        usecols=["window_start", "window_end", "metric_name", "value_ms"],
    )

    # filter to only the requested metrics and the windows that were kept after trimming
    kept_index = pd.MultiIndex.from_frame(
        kept_windows[["window_start", "window_end"]].drop_duplicates()
    )
    metric_set = set(metrics)

    df = df[df["metric_name"].isin(metric_set)]

    keys = pd.MultiIndex.from_frame(df[["window_start", "window_end"]])
    df = df[keys.isin(kept_index)]

    df["value_ms"] = pd.to_numeric(df["value_ms"], errors="coerce")
    df = df.dropna(subset=["value_ms"])

    output = {}
    for metric in metrics:
        output[metric] = df.loc[df["metric_name"] == metric, "value_ms"].to_numpy(dtype=float)

    return output


def collect_run_samples(
    scan_dir: Path,
    rps_values: List[int],
    run_ids: List[int],
    metrics: List[str],
    skip_first: int,
    skip_last: int,
) -> Dict[str, Dict[int, Dict[int, np.ndarray]]]:
    """
    Collect trimmed per-run metric samples across RPS levels.
    Returns:
        Dict[str, Dict[int, Dict[int, np.ndarray]]]: Nested mapping
        metric -> rps -> run_id -> samples where each leaf is a 1-D numpy
        array of trimmed sample values.
    """
    data: Dict[str, Dict[int, Dict[int, np.ndarray]]] = {metric: {} for metric in metrics}

    for rps in rps_values:
        for metric in metrics:
            data[metric][rps] = {}

        run_dir = scan_dir / f"rps_{rps}"
        for run in run_ids:
            window_results_path = run_dir / f"window_results_run{run}.csv"
            window_samples_path = run_dir / f"window_samples_run{run}.csv"

            # load the window results, trim edge windows, and identify the kept windows
            trimmed_windows = load_trimmed_window_results(
                window_results_path,
                skip_first,
                skip_last,
            )
            kept_windows = trimmed_windows[["window_start", "window_end"]].drop_duplicates()
            # loading the metric samples for the kept windows and requested metrics, resulting in a mapping from metric name to sample array
            metric_samples = load_trimmed_metric_samples(
                window_samples_path,
                kept_windows,
                metrics,
            )

            for metric in metrics:
                data[metric][rps][run] = metric_samples[metric]

    return data


def plot_metric_scores(metric_fold_df: pd.DataFrame, output_path: Path) -> None:
    """
    Plot per-target EMD for interpolation and nearest-neighbor baseline.
    Args:
        metric_fold_df: Fold-level results for one metric.
        output_path: File path where the PNG plot should be written.
    """

    ordered = metric_fold_df.sort_values("target_rps")

    fig, ax = plt.subplots(figsize=(9, 5))
    # plot interpolation emds
    ax.plot(
        ordered["target_rps"],
        ordered["interpolation_emd_ms"],
        marker="o",
        linewidth=2,
        label="Quantile interpolation",
    )
    # plot nearest neighbor emds
    ax.plot(
        ordered["target_rps"],
        ordered["nearest_neighbor_emd_ms"],
        marker="s",
        linewidth=2,
        label="Nearest-neighbor baseline",
    )
    ax.set_xlabel("Target RPS")
    ax.set_ylabel("EMD / Wasserstein-1 (ms)")
    ax.set_title("Actual vs predicted distribution by target RPS")
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def plot_metric_quantile_examples(
    metric: str,
    percentile_df: pd.DataFrame,
    output_path: Path,
) -> None:
    """
    Plot actual and predicted quantile curves for each target RPS.
    Args:
        metric: Metric name used in the plot title.
        percentile_df: Percentile-level results for one metric across RPS levels.
        output_path: File path where the PNG plot should be written.
    """

    targets = sorted(percentile_df["target_rps"].unique())

    fig, axes = plt.subplots(
        len(targets),
        1,
        figsize=(9, max(3.5 * len(targets), 4.0)),
        sharex=True,
    )
    if len(targets) == 1:
        axes = [axes]

    # plot actual, interpolated, and nearest neighbor quantile curves for each target RPS
    for ax, target_rps in zip(axes, targets):
        subset = percentile_df[percentile_df["target_rps"] == target_rps].sort_values("percentile")
        ax.plot(subset["percentile"], subset["actual_ms"], linewidth=2, label="Actual")
        ax.plot(subset["percentile"], subset["interpolated_ms"], linewidth=2, label="Interpolated")
        ax.plot(
            subset["percentile"],
            subset["nearest_neighbor_ms"],
            linewidth=2,
            linestyle="--",
            label="Nearest neighbor",
        )
        ax.set_ylabel(f"{target_rps} RPS\nvalue (ms)")
        ax.grid(True, alpha=0.25)

    axes[0].set_title(f"{metric}: actual vs predicted quantile curves")
    axes[-1].set_xlabel("Percentile")
    axes[0].legend()

    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def validate_metric(
    metric: str,
    metric_data: Dict[int, Dict[int, np.ndarray]],
    k_value: int,
    eval_grid: np.ndarray,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Evaluate interpolation quality for one metric across target RPS levels.

    Args:
        metric: Metric name being validated.
        metric_data: Nested mapping ``rps -> run_id -> samples`` for this metric.
        k_value: Number of percentile knots used for interpolation between
            neighboring RPS levels.
        eval_grid: Dense percentile grid used to compare actual and predicted
            curves and to populate the output tables.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            - summary_df: One-row summary for the metric with aggregate EMD
              statistics.
            - folds_df: One row per target RPS containing interpolation and
              nearest-neighbor EMD scores.
            - percentile_df: One row per percentile per target RPS with the
              actual, interpolated, and nearest-neighbor quantile values.
    """
    usable_rps = sorted(
        rps for rps, runs in metric_data.items() if sum(samples.size > 0 for samples in runs.values()) > 0
    )

    # so we can properly define a lower and upper neighbor
    if len(usable_rps) < 3:
        raise ValueError(
            f"Metric {metric} has only {len(usable_rps)} usable RPS levels; need at least 3 for leave-one-RPS-out interpolation"
        )

    knot_grid = make_percentile_grid(k_value)
    true_curves_eval = {}
    true_curves_knot = {}
    for rps in usable_rps:
        # for the given RPS, get the samples and compute the quantile curve on eval grid and knot grid
        run_samples = [samples for samples in metric_data[rps].values() if samples.size > 0]
        true_curves_eval[rps] = pointwise_reference_curve(run_samples, eval_grid)
        true_curves_knot[rps] = pointwise_reference_curve(run_samples, knot_grid)

    fold_rows = []
    percentile_rows = []

    # go over the RPS levels we can interpolate at
    for idx in range(1, len(usable_rps) - 1):

        # get the rps and its neighbors
        target_rps = usable_rps[idx]
        low_rps = usable_rps[idx - 1]
        high_rps = usable_rps[idx + 1]

        # get the actual curve for the target on the eval grid
        actual_curve = true_curves_eval[target_rps]

        # get the interpolated curve
        interpolated_curve = predict_curve_from_neighbors(
            target_rps=target_rps,
            low_rps=low_rps,
            low_curve=true_curves_knot[low_rps],
            high_rps=high_rps,
            high_curve=true_curves_knot[high_rps],
            knot_grid=knot_grid,
            eval_grid=eval_grid,
        )

        # get the nearest neighbor curve as a baseline predictor
        nn_rps = nearest_neighbor_rps(target_rps, [low_rps, high_rps])
        nearest_neighbor_curve = true_curves_eval[nn_rps]

        # calculate the wasserstein distance between the actual curve and the predicted curves, and store the results for this fold
        interpolation_emd = wasserstein1_from_quantiles(actual_curve, interpolated_curve, eval_grid)
        nearest_neighbor_emd = wasserstein1_from_quantiles(actual_curve, nearest_neighbor_curve, eval_grid)

        # add the results for this fold
        fold_rows.append(
            {
                "metric": metric,
                "target_rps": target_rps,
                "low_rps": low_rps,
                "high_rps": high_rps,
                "nearest_neighbor_rps": nn_rps,
                "interpolation_emd_ms": interpolation_emd,
                "nearest_neighbor_emd_ms": nearest_neighbor_emd,
                "emd_improvement_ms": nearest_neighbor_emd - interpolation_emd,
            }
        )

        for percentile, actual_ms, interpolated_ms, nearest_neighbor_ms in zip(
            eval_grid,
            actual_curve,
            interpolated_curve,
            nearest_neighbor_curve,
        ):
            # add the results for this percentile of this fold at the RPS level
            percentile_rows.append(
                {
                    "metric": metric,
                    "target_rps": target_rps,
                    "percentile": percentile,
                    "actual_ms": actual_ms,
                    "interpolated_ms": interpolated_ms,
                    "nearest_neighbor_ms": nearest_neighbor_ms,
                }
            )

    folds_df = pd.DataFrame(fold_rows)
    percentile_df = pd.DataFrame(percentile_rows)
    summary_df = pd.DataFrame(
        [
            {
                "metric": metric,
                "num_target_rps": int(folds_df["target_rps"].nunique()),
                "mean_interpolation_emd_ms": folds_df["interpolation_emd_ms"].mean(),
                "mean_nearest_neighbor_emd_ms": folds_df["nearest_neighbor_emd_ms"].mean(),
                "mean_emd_improvement_ms": folds_df["emd_improvement_ms"].mean(),
                "median_interpolation_emd_ms": folds_df["interpolation_emd_ms"].median(),
                "median_nearest_neighbor_emd_ms": folds_df["nearest_neighbor_emd_ms"].median(),
                "median_emd_improvement_ms": folds_df["emd_improvement_ms"].median(),
            }
        ]
    )
    return summary_df, folds_df, percentile_df


if __name__ == "__main__":
    args = parse_args()
    scan_dir = args.results_dir / args.scan
    analysis_dir = scan_dir / "analysis" / "quantile_interpolation_validation"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    eval_grid = np.linspace(0.001, 0.999, args.eval_grid_size)

    # obtain the nested mapping of metric -> rps -> run_id -> samples
    run_samples = collect_run_samples(
        scan_dir=scan_dir,
        rps_values=sorted(args.rps),
        run_ids=sorted(args.runs),
        metrics=args.metrics,
        skip_first=args.skip_first,
        skip_last=args.skip_last
    )

    summary_frames, fold_frames, percentile_frames = [], [], []
    for metric in args.metrics:
        # for a given metric
        metric_summary_df, metric_folds_df, metric_percentile_df = validate_metric(
            metric=metric,
            metric_data=run_samples[metric],
            k_value=args.k_value,
            eval_grid=eval_grid,
        )
        summary_frames.append(metric_summary_df)
        fold_frames.append(metric_folds_df)
        percentile_frames.append(metric_percentile_df)

        # plotting the results for this metric
        safe_name = safe_metric_name(metric)
        plot_metric_scores(
            metric_folds_df,
            analysis_dir / f"{safe_name}_emd_vs_target_rps.png",
        )
        plot_metric_quantile_examples(
            metric,
            metric_percentile_df,
            analysis_dir / f"{safe_name}_quantile_examples.png",
        )

    # save the results as CSVs
    summary_df = pd.concat(summary_frames, ignore_index=True)
    folds_df = pd.concat(fold_frames, ignore_index=True)
    percentile_df = pd.concat(percentile_frames, ignore_index=True)
    summary_df.to_csv(analysis_dir / "summary.csv", index=False)
    folds_df.to_csv(analysis_dir / "folds.csv", index=False)
    percentile_df.to_csv(analysis_dir / "percentile_curves.csv", index=False)
