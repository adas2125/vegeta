import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance


def safe_median(x):
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        return np.nan
    return float(np.median(x))

def normalized_emd(baseline_values, current_values):
    baseline_values = np.asarray(baseline_values, dtype=float)
    current_values = np.asarray(current_values, dtype=float)

    if baseline_values.size == 0 or current_values.size == 0:
        return np.nan

    emd = wasserstein_distance(baseline_values, current_values)
    scale = max(abs(safe_median(baseline_values)), 1.0)  # 1 ms floor
    return emd / scale

def scalar_shift(current_value, baseline_series):
    """
    Absolute deviation from baseline median.
    No normalization for bounded or near-zero metrics.
    """
    baseline_vals = pd.to_numeric(baseline_series, errors="coerce").dropna().values
    if baseline_vals.size == 0 or pd.isna(current_value):
        return np.nan

    baseline_med = float(np.median(baseline_vals))
    return abs(float(current_value) - baseline_med)

def safe_filename(name):
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in name)

def trim_window_margins(window_results_df, start_windows=0, end_windows=0):
    ordered = (
        window_results_df.assign(window_start_dt=pd.to_datetime(window_results_df["window_start"]))
        .sort_values(["window_start_dt", "window_end"])
        .reset_index(drop=True)
    )

    start_idx = min(start_windows, len(ordered))
    end_idx = len(ordered) - end_windows if end_windows > 0 else len(ordered)
    end_idx = max(start_idx, end_idx)
    return ordered.iloc[start_idx:end_idx].copy()

def latest_run_dir(root_dir):
    candidates = [path for path in root_dir.glob("run_*") if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No run_* directories found under {root_dir}")
    return sorted(candidates)[-1]

def safe_metric_name(metric: str) -> str:
    return metric.replace("/", "_").replace(" ", "_")
