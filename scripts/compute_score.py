import random
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance

ROOT_DIR = "samples"

csv_path_normal = "normal"
csv_path_abnormal = "few_conns"

window_results_csv_normal = os.path.join(ROOT_DIR, f"window_results_{csv_path_normal}.csv")
window_results_csv_abnormal = os.path.join(ROOT_DIR, f"window_results_{csv_path_abnormal}.csv")
window_samples_csv_normal = os.path.join(ROOT_DIR, f"window_samples_{csv_path_normal}.csv")
window_samples_csv_abnormal = os.path.join(ROOT_DIR, f"window_samples_{csv_path_abnormal}.csv")

# -----------------------------
# Config
# -----------------------------
EPS = 1e-9

# EMD metric groups
CLIENT_METRICS = ["scheduler_delay", "fire_to_dispatch_delay", "dispatch_delay"]
TRANSPORT_METRICS = ["conn_delay"]
SERVER_METRICS = ["first_byte_rtt", "response_tail_time"]
TRANSPORT_SCALARS = ["reuse_frac", "fresh_conn_frac", "was_idle_given_reused", "avg_conn_idle_time_ms"]

# Weights (these will be learned later)
CLIENT_WEIGHTS = {
    "scheduler_delay": 2.0,
    "fire_to_dispatch_delay": 1.5,
    "dispatch_delay": 1.0,
}

TRANSPORT_WEIGHTS = {
    "conn_delay": 2.0,
    "reuse_frac": 1.0,
    "fresh_conn_frac": 1.5,
    "was_idle_given_reused": 1.0,
    "avg_conn_idle_time_ms": 1.0,
}

SERVER_WEIGHTS = {
    "first_byte_rtt": 2.0,
    "response_tail_time": 1.5,
}

MIXED_RATIO_THRESHOLD = 0.70  # if second-best >= 70% of best => Mixed

# -----------------------------
# Load data
# -----------------------------
window_samples_normal = pd.read_csv(window_samples_csv_normal)
window_samples_abnormal = pd.read_csv(window_samples_csv_abnormal)
window_results_normal = pd.read_csv(window_results_csv_normal)
window_results_abnormal = pd.read_csv(window_results_csv_abnormal)
window_samples_normal["value_ms"] = window_samples_normal["value_ms"].astype(float)
window_samples_abnormal["value_ms"] = window_samples_abnormal["value_ms"].astype(float)

# Merge samples with window-level results
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


# Select first abnormal window
abnormal_window_rows = (
    window_results_abnormal[window_results_abnormal["ll_violation"] == True]
    .sort_values(["window_start", "window_end"])
)

if abnormal_window_rows.empty:
    raise ValueError("No abnormal windows found where ll_violation == True.")

first_abnormal = abnormal_window_rows.iloc[0]
abnormal_start = first_abnormal["window_start"]
abnormal_end = first_abnormal["window_end"]

abnormal_window_df = df_abnormal[
    (df_abnormal["window_start"] == abnormal_start) &
    (df_abnormal["window_end"] == abnormal_end)
]

abnormal_window_result = window_results_abnormal[
    (window_results_abnormal["window_start"] == abnormal_start) &
    (window_results_abnormal["window_end"] == abnormal_end)
].iloc[0]

print(f"Selected FIRST ABNORMAL window:\n{abnormal_start} -> {abnormal_end}")

# Build pooled healthy baseline distributions
healthy_window_keys = window_results_normal[window_results_normal["ll_violation"] == False][
    ["window_start", "window_end"]
].drop_duplicates()
healthy_baseline_df = pd.merge(
    window_samples_normal,
    healthy_window_keys,
    on=["window_start", "window_end"],
    how="inner",
)

common_metrics = sorted(
    set(healthy_baseline_df["metric_name"].unique()) &
    set(abnormal_window_df["metric_name"].unique())
)

print(f"\nCommon metrics in healthy baseline vs abnormal window: {common_metrics}")

if not common_metrics:
    raise ValueError("No common metrics found between healthy baseline and abnormal window.")

# -----------------------------
# Helper functions
# -----------------------------
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

def weighted_sum(score_dict, weight_dict):
    print(f"Score dict: {score_dict}")
    print(f"Weight dict: {weight_dict}")
    total = 0.0
    used = {}
    for k, w in weight_dict.items():
        v = score_dict.get(k, np.nan)
        if pd.notna(v):
            total += w * v
            used[k] = (v, w, w * v)
    return total, used

# Compute per-metric normalized EMDs
emd_scores = {}
for metric in common_metrics:
    baseline_values = healthy_baseline_df[
        healthy_baseline_df["metric_name"] == metric
    ]["value_ms"].values
    print(f"\nBaseline values for {metric} (n={len(baseline_values)}): median={safe_median(baseline_values):.3f} ms")

    abnormal_values = abnormal_window_df[
        abnormal_window_df["metric_name"] == metric
    ]["value_ms"].values
    print(f"Abnormal window values for {metric} (n={len(abnormal_values)}): median={safe_median(abnormal_values):.3f} ms")
    emd_scores[metric] = normalized_emd(baseline_values, abnormal_values)

# -----------------------------
# Compute scalar transport deviations
# -----------------------------
transport_scalar_scores = {}
healthy_results_only = window_results_normal[window_results_normal["ll_violation"] == False]
for col in TRANSPORT_SCALARS:
    if col in healthy_results_only.columns and col in abnormal_window_result.index:
        # difference btw current value and baseline median (no normalization)
        transport_scalar_scores[col] = scalar_shift(
            abnormal_window_result[col],
            healthy_results_only[col]
        )

# Build category scores
client_feature_scores = {m: emd_scores.get(m, np.nan) for m in CLIENT_METRICS}
transport_feature_scores = {m: emd_scores.get(m, np.nan) for m in TRANSPORT_METRICS}
transport_feature_scores.update(transport_scalar_scores)    # Add scalar deviations to transport features
server_feature_scores = {m: emd_scores.get(m, np.nan) for m in SERVER_METRICS}

client_score, client_used = weighted_sum(client_feature_scores, CLIENT_WEIGHTS)
transport_score, transport_used = weighted_sum(transport_feature_scores, TRANSPORT_WEIGHTS)
server_score, server_used = weighted_sum(server_feature_scores, SERVER_WEIGHTS)

category_scores = {
    "Client": client_score,
    "Transport": transport_score,
    "Server": server_score,
}

sorted_scores = sorted(category_scores.items(), key=lambda kv: kv[1], reverse=True)
best_label, best_score = sorted_scores[0]
second_label, second_score = sorted_scores[1]

if best_score <= 0:
    final_label = "Unclear"
elif second_score / max(best_score, EPS) >= MIXED_RATIO_THRESHOLD:
    final_label = "Mixed"
else:
    final_label = best_label

# Print score breakdown
print("\nNormalized EMD scores by metric:")
for metric in sorted(emd_scores):
    print(f"  {metric}: {emd_scores[metric]:.6f}")

print("\nTransport scalar deviation scores:")
for k, v in transport_scalar_scores.items():
    print(f"  {k}: {v:.6f}")

print("\nClient score breakdown:")
for k, (v, w, contrib) in client_used.items():
    print(f"  {k}: value={v:.6f}, weight={w:.2f}, contribution={contrib:.6f}")
print(f"  TOTAL Client Score = {client_score:.6f}")

print("\nTransport score breakdown:")
for k, (v, w, contrib) in transport_used.items():
    print(f"  {k}: value={v:.6f}, weight={w:.2f}, contribution={contrib:.6f}")
print(f"  TOTAL Transport Score = {transport_score:.6f}")

print("\nServer score breakdown:")
for k, (v, w, contrib) in server_used.items():
    print(f"  {k}: value={v:.6f}, weight={w:.2f}, contribution={contrib:.6f}")
print(f"  TOTAL Server Score = {server_score:.6f}")

print("\nCategory scores:")
for k, v in sorted_scores:
    print(f"  {k}: {v:.6f}")

print(f"\nFinal label: {final_label}")
