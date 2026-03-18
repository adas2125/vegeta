import random
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance
import os
import numpy as np

ROOT_DIR = "samples"

csv_path_normal = "normal"
csv_path_abnormal = "cpu_contention"

window_results_csv_normal = os.path.join(ROOT_DIR, f"window_results_{csv_path_normal}.csv")
window_results_csv_abnormal = os.path.join(ROOT_DIR, f"window_results_{csv_path_abnormal}.csv")
window_samples_csv_normal = os.path.join(ROOT_DIR, f"window_samples_{csv_path_normal}.csv")
window_samples_csv_abnormal = os.path.join(ROOT_DIR, f"window_samples_{csv_path_abnormal}.csv")

# Read data
window_samples_normal = pd.read_csv(window_samples_csv_normal)
window_samples_abnormal = pd.read_csv(window_samples_csv_abnormal)
window_results_normal = pd.read_csv(window_results_csv_normal)
window_results_abnormal = pd.read_csv(window_results_csv_abnormal)

# Ensure numeric
window_samples_normal["value_ms"] = window_samples_normal["value_ms"].astype(float)
window_samples_abnormal["value_ms"] = window_samples_abnormal["value_ms"].astype(float)

# Merge samples with per-window results
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

# -----------------------------
# Pick a random normal window
# -----------------------------
normal_windows = list(
    df_normal[["window_start", "window_end"]]
    .drop_duplicates()
    .itertuples(index=False, name=None)
)

if not normal_windows:
    raise ValueError("No windows found in normal run.")

normal_start, normal_end = random.choice(normal_windows)

normal_window_df = df_normal[
    (df_normal["window_start"] == normal_start) &
    (df_normal["window_end"] == normal_end)
]

# -----------------------------------------
# Pick the first abnormal window (LL true)
# -----------------------------------------
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

print(f"Selected NORMAL window:\n{normal_start} → {normal_end}")
print(f"Selected FIRST ABNORMAL window:\n{abnormal_start} → {abnormal_end}")

# Metrics present in both selected windows
common_metrics = sorted(
    set(normal_window_df["metric_name"].unique()) &
    set(abnormal_window_df["metric_name"].unique())
)

if not common_metrics:
    raise ValueError("No common metrics found between the selected windows.")

emd_results = {}

for metric in common_metrics:
    normal_values = normal_window_df[
        normal_window_df["metric_name"] == metric
    ]["value_ms"].values

    abnormal_values = abnormal_window_df[
        abnormal_window_df["metric_name"] == metric
    ]["value_ms"].values

    # Compute EMD / Wasserstein distance
    emd = wasserstein_distance(normal_values, abnormal_values)
    emd_results[metric] = emd

    # Convert to arrays
    normal_values = np.asarray(normal_values)
    abnormal_values = np.asarray(abnormal_values)

    # Clip outliers using a shared range so both plots stay comparable
    all_values = np.concatenate([normal_values, abnormal_values])
    lower = np.percentile(all_values, 1)    # change if you want more/less clipping
    upper = np.percentile(all_values, 99)

    normal_clipped = np.clip(normal_values, lower, upper)
    abnormal_clipped = np.clip(abnormal_values, lower, upper)

    # Same bins for both histograms
    bins = np.linspace(lower, upper, 40)

    # Plot separate subplots
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)

    axes[0].hist(normal_clipped, bins=bins, density=True, alpha=0.7)
    axes[0].set_title("Normal")
    axes[0].set_xlabel("Value (ms)")
    axes[0].set_ylabel("Density")

    axes[1].hist(abnormal_clipped, bins=bins, density=True, alpha=0.7)
    axes[1].set_title("Abnormal")
    axes[1].set_xlabel("Value (ms)")

    fig.suptitle(f"{metric} distribution comparison\nEMD = {emd:.6f}", fontsize=12)
    plt.tight_layout()
    plt.show()

print("\nEMD / Wasserstein distance by metric:")
for metric, emd in emd_results.items():
    print(f"{metric}: {emd:.6f}")