from pathlib import Path

import numpy as np
import pandas as pd

import attribution as base
from validate_rps_quantile_interpolation import (
    collect_run_samples,
    empirical_quantile_curve,
    make_percentile_grid,
    pointwise_reference_curve,
    predict_curve_from_neighbors,
    wasserstein1_from_quantiles,
)

OUTPUT_ROOT = base.ROOT_DIR / "attribution_out_interopolate"
# for interpolation purposes
REFERENCE_SCAN_DIR = Path("experiments/out/healthy_HotelReservation_http1_cycle_20260401_215615")

# for our grid sizes
DEFAULT_K_VALUE = 99
DEFAULT_EVAL_GRID_SIZE = 1000

# the bands we use to interpolate the 2K reference curve, and the runs we use for each band
TARGET_RPS = 2000
LOW_REFERENCE_RPS = 1000
HIGH_REFERENCE_RPS = 3000
REFERENCE_RUN_IDS = [1, 2]

# Trimming constants
REFERENCE_SKIP_FIRST = 2
REFERENCE_SKIP_LAST = 1
INTERPOLATED_EXCLUDED_METRICS = {"conn_idle_time"}
INTERPOLATED_METRIC_LABEL_OVERRIDES = {
    "conn_delay": "Connection Delay",
}
INTERPOLATED_METRIC_COLOR = "#4C78A8"
INTERPOLATED_AXIS_LABEL_SIZE = 36
INTERPOLATED_TICK_LABEL_SIZE = 28
INTERPOLATED_X_TICK_LABEL_SIZE = 34


def build_interpolated_reference_curve(
    low_reference_runs,
    high_reference_runs,
    k_value: int = DEFAULT_K_VALUE,
    eval_grid_size: int = DEFAULT_EVAL_GRID_SIZE,
):
    """
    Build the predicted 2K reference curve from healthy 1K and 3K neighbor runs.
    """

    # make the grids
    knot_grid = make_percentile_grid(k_value)
    eval_grid = np.linspace(0.001, 0.999, eval_grid_size)

    # collecting the sames from the bands
    low_run_samples = [samples for samples in low_reference_runs.values() if samples.size > 0]
    high_run_samples = [samples for samples in high_reference_runs.values() if samples.size > 0]

    # median aggregation across runs in each band
    low_reference_knots = pointwise_reference_curve(low_run_samples, knot_grid)
    high_reference_knots = pointwise_reference_curve(high_run_samples, knot_grid)

    # we now can get the predicted 2K curve by interpolating between the 1K and 3K curves
    interpolated_reference_curve = predict_curve_from_neighbors(
        target_rps=TARGET_RPS,
        low_rps=LOW_REFERENCE_RPS,
        low_curve=low_reference_knots,
        high_rps=HIGH_REFERENCE_RPS,
        high_curve=high_reference_knots,
        knot_grid=knot_grid,
        eval_grid=eval_grid,
    )

    return interpolated_reference_curve, eval_grid


def interpolated_emd_from_neighbor_curve(
    interpolated_reference_curve: np.ndarray,
    eval_grid: np.ndarray,
    category_values: pd.Series,
) -> float:
    """
    Compare a category sample pool against the interpolated 2K reference curve.
    """
    category_curve = empirical_quantile_curve(category_values, eval_grid)
    return wasserstein1_from_quantiles(category_curve, interpolated_reference_curve, eval_grid)


def interpolated_reference_scale(interpolated_reference_curve: np.ndarray, eval_grid: np.ndarray) -> float:
    """
    Compute a robust scale from the interpolated reference quantile curve.
    """
    q25 = float(np.interp(0.25, eval_grid, interpolated_reference_curve))
    q75 = float(np.interp(0.75, eval_grid, interpolated_reference_curve))
    iqr = q75 - q25
    if iqr > base.EPSILON:
        return iqr

    std = float(np.std(interpolated_reference_curve, ddof=1)) if len(interpolated_reference_curve) > 1 else 0.0
    if std > base.EPSILON:
        return std

    mean = float(np.mean(interpolated_reference_curve))
    if abs(mean) > base.EPSILON:
        return abs(mean)

    max_value = float(np.max(np.abs(interpolated_reference_curve)))
    if max_value > base.EPSILON:
        return max_value

    return 1.0


def summarize_run_metric(
    category,
    run_id,
    interpolated_reference_by_metric,
):
    """
    Outputs a summary dataframe for a given run and category, comparing each metric's sample pool against the 
    interpolated reference curve for that metric.
    """
    # load the samples for this run and category
    category_results, category_samples = base.load_run_samples(category, run_id)
    category_keys = base.abnormal_pool_keys(category_results)

    rows = []
    for metric_name in base.SAMPLE_METRICS:
        # get the interpolated reference curve for this metric
        interpolated_reference_curve, eval_grid = interpolated_reference_by_metric[metric_name]
        category_values = base.pooled_sample_values(category_samples, metric_name, category_keys)
        used_metric_fallback = False
        forced_zero_emd = False

        if metric_name == "conn_idle_time" and category_values.empty:
            _, full_category_samples = base.load_run_samples(category, run_id, trim=False)
            full_metric_keys = base.metric_window_keys(full_category_samples, metric_name)
            if not full_metric_keys.empty and interpolated_reference_curve is not None:
                category_values = pd.Series([0.0], dtype=float)
                forced_zero_emd = True

        if interpolated_reference_curve is None or category_values.empty:
            continue

        emd = (
            0.0
            if forced_zero_emd
            else interpolated_emd_from_neighbor_curve(
                interpolated_reference_curve,
                eval_grid,
                category_values,
            )
        )
        reference_scale = interpolated_reference_scale(interpolated_reference_curve, eval_grid)

        rows.append(
            {
                "category": category,
                "run_id": run_id,
                "metric_name": metric_name,
                "emd": emd,
                "normalized_emd": emd / reference_scale,
                "reference_scale": reference_scale,
                "category_mean": category_values.mean(),
                "category_std": category_values.std(ddof=1) if len(category_values) > 1 else 0.0,
                "category_count": len(category_values),
                "used_metric_fallback": used_metric_fallback,
                "forced_zero_emd": forced_zero_emd,
            }
        )

    return pd.DataFrame(rows)


def analyze_category(category, run_ids):

    # we get the lower and higher reference band mapping per metric, rps, and run
    reference_metric_runs = collect_run_samples(
        scan_dir=REFERENCE_SCAN_DIR,
        rps_values=[LOW_REFERENCE_RPS, HIGH_REFERENCE_RPS],
        run_ids=REFERENCE_RUN_IDS,
        metrics=base.SAMPLE_METRICS,
        skip_first=REFERENCE_SKIP_FIRST,
        skip_last=REFERENCE_SKIP_LAST,
    )

    interpolated_reference_by_metric = {}

    for metric_name in base.SAMPLE_METRICS:
        metric_reference_runs = reference_metric_runs[metric_name]
        low_reference_runs = metric_reference_runs[LOW_REFERENCE_RPS]
        high_reference_runs = metric_reference_runs[HIGH_REFERENCE_RPS]
        low_arrays = [samples for samples in low_reference_runs.values() if samples.size > 0]
        high_arrays = [samples for samples in high_reference_runs.values() if samples.size > 0]
        if not low_arrays or not high_arrays:
            interpolated_reference_by_metric[metric_name] = (None, None)
            continue

        # build the interpolated reference curve for this metric
        interpolated_reference_by_metric[metric_name] = build_interpolated_reference_curve(
            low_reference_runs,
            high_reference_runs,
        )

    # accross all runs, metrics, consists of emd, 
    run_frames = []
    for run_id in run_ids:
        run_df = summarize_run_metric(
            category,
            run_id,
            interpolated_reference_by_metric,
        )
        if run_df.empty:
            print(f"Skipping {category} run{run_id}: no sample metrics were available.")
            continue
        run_df = run_df.sort_values("emd", ascending=False).reset_index(drop=True)
        run_frames.append(run_df)

    if not run_frames:
        return pd.DataFrame()

    return pd.concat(run_frames, ignore_index=True)


def build_plot_ready_frames(category_frames):
    """
    Filter out metrics we do not want to display in the interpolated overview.
    """
    plot_frames = {}
    for category, frame in category_frames.items():
        if frame.empty:
            plot_frames[category] = frame
            continue
        filtered_frame = frame[~frame["metric_name"].isin(INTERPOLATED_EXCLUDED_METRICS)].copy()
        plot_frames[category] = filtered_frame
    return plot_frames


if __name__ == "__main__":
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    run_ids_by_category = base.detect_run_ids()

    category_frames = {}
    for category in base.CATEGORIES:
        # get the summary stats for a specific category
        category_frames[category] = analyze_category(category, run_ids_by_category.get(category, []))

    plot_frames = build_plot_ready_frames(category_frames)

    base.METRIC_LABELS.update(INTERPOLATED_METRIC_LABEL_OVERRIDES)
    for metric_name in base.SAMPLE_METRICS:
        base.METRIC_COLORS[metric_name] = INTERPOLATED_METRIC_COLOR
        base.METRIC_HATCHES[metric_name] = ""
    base.AXIS_LABEL_SIZE = INTERPOLATED_AXIS_LABEL_SIZE
    base.TICK_LABEL_SIZE = INTERPOLATED_TICK_LABEL_SIZE
    base.X_TICK_LABEL_SIZE = INTERPOLATED_X_TICK_LABEL_SIZE

    # plotting
    base.plot_combined_overview(
        plot_frames,
        OUTPUT_ROOT / "paper_attribution_overview_interpolated.pdf",
    )
    base.plot_combined_overview_for_column(
        plot_frames,
        OUTPUT_ROOT / "paper_attribution_overview_interpolated_normalized.pdf",
        value_column="normalized_emd",
        ylabel="Normalized EMD",
    )
