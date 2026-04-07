#!/usr/bin/env python3

import argparse
import math
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(".matplotlib").resolve()))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit


DEFAULT_SUMMARY = Path("fixed_delay_low_load_runs/latest_summary.csv")
DEFAULT_OUTPUT_ROOT = Path("fixed_delay_curve_fit")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Fit queueing-style latency curves against observed Vegeta throughput "
            "and report basic fit quality."
        )
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=DEFAULT_SUMMARY,
        help="CSV with target_rps, achieved_throughput_rps, and mean_latency_ms columns.",
    )
    parser.add_argument(
        "--output-plot",
        type=Path,
        default=None,
        help="Where to save the fitted plot. Overrides --output-dir for the plot file only.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for generated fit artifacts. Defaults under experiments/fixed_delay_curve_fit.",
    )
    return parser.parse_args()


def queueing_delay_general(lam, a, mu, b, k):
    epsilon = 1e-9
    return (a / np.power(np.maximum(mu - lam, epsilon), k)) + b


def queueing_delay_k1(lam, a, mu, b):
    epsilon = 1e-9
    return (a / np.maximum(mu - lam, epsilon)) + b


def rmse(y_true, y_pred):
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def r2(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    if math.isclose(ss_tot, 0.0):
        return float("nan")
    return 1.0 - ss_res / ss_tot


def default_output_dir(summary_csv: Path) -> Path:
    run_name = summary_csv.parent.name
    if summary_csv.stem == "latest_summary":
        run_name = summary_csv.stem
    elif run_name in {"", "."}:
        run_name = summary_csv.stem
    return DEFAULT_OUTPUT_ROOT / run_name


def fit_general_model(throughput, latency):
    initial_guess = [100.0, max(throughput) * 1.5, min(latency), 1.0]
    popt, _ = curve_fit(
        queueing_delay_general,
        throughput,
        latency,
        p0=initial_guess,
        bounds=([0.0, max(throughput) + 1.0, 0.0, 0.1], [np.inf, np.inf, np.inf, 5.0]),
        maxfev=20000,
    )
    predicted = queueing_delay_general(throughput, *popt)
    return popt, predicted


def fit_k1_model(throughput, latency):
    initial_guess = [100.0, max(throughput) * 1.5, min(latency)]
    popt, _ = curve_fit(
        queueing_delay_k1,
        throughput,
        latency,
        p0=initial_guess,
        bounds=([0.0, max(throughput) + 1.0, 0.0], [np.inf, np.inf, np.inf]),
        maxfev=20000,
    )
    predicted = queueing_delay_k1(throughput, *popt)
    return popt, predicted


def leave_one_out_k1(throughput, latency):
    if len(throughput) < 4:
        return None

    rows = []
    for idx in range(len(throughput)):
        train_mask = np.ones(len(throughput), dtype=bool)
        train_mask[idx] = False

        train_x = throughput[train_mask]
        train_y = latency[train_mask]
        test_x = throughput[idx]
        test_y = latency[idx]

        popt, _ = curve_fit(
            queueing_delay_k1,
            train_x,
            train_y,
            p0=[100.0, max(train_x) * 1.5, min(train_y)],
            bounds=([0.0, max(train_x) + 1.0, 0.0], [np.inf, np.inf, np.inf]),
            maxfev=20000,
        )
        pred_y = float(queueing_delay_k1(np.array([test_x]), *popt)[0])
        rows.append(
            {
                "achieved_throughput_rps": float(test_x),
                "actual_mean_latency_ms": float(test_y),
                "predicted_mean_latency_ms": pred_y,
                "absolute_error_ms": abs(pred_y - float(test_y)),
            }
        )

    return pd.DataFrame(rows)


def plot_fit(throughput, latency, general_popt, k1_popt, output_plot: Path):
    x_max = max(float(np.max(throughput)) * 1.1, 1.0)
    x_upper = min(general_popt[1] - 1.0, k1_popt[1] - 1.0, x_max)
    if x_upper <= float(np.min(throughput)):
        x_upper = x_max

    x_vals = np.linspace(0.0, x_upper, 300)

    plt.figure(figsize=(8, 5))
    plt.scatter(throughput, latency, color="tab:red", label="Observed data")
    plt.plot(x_vals, queueing_delay_general(x_vals, *general_popt), label="General fit")
    plt.plot(x_vals, queueing_delay_k1(x_vals, *k1_popt), linestyle="--", label="k=1 fit")
    plt.axvline(general_popt[1], color="tab:gray", linestyle=":", label=f"General mu={general_popt[1]:.1f}")
    plt.axvline(k1_popt[1], color="tab:blue", linestyle=":", label=f"k=1 mu={k1_popt[1]:.1f}")
    plt.xlabel("Achieved throughput (RPS)")
    plt.ylabel("Mean latency (ms)")
    plt.title("Latency curve fit vs achieved throughput")
    plt.legend()
    plt.tight_layout()
    output_plot.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_plot, dpi=160)
    plt.close()


def main():
    args = parse_args()
    if not args.summary_csv.exists():
        raise FileNotFoundError(f"Missing summary CSV: {args.summary_csv}")

    df = pd.read_csv(args.summary_csv)
    required = {"achieved_throughput_rps", "mean_latency_ms"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Summary CSV is missing columns: {sorted(missing)}")

    fit_df = (
        df[["target_rps", "achieved_throughput_rps", "mean_latency_ms"]]
        .copy()
        .sort_values("achieved_throughput_rps")
        .reset_index(drop=True)
    )

    throughput = fit_df["achieved_throughput_rps"].to_numpy(dtype=float)
    latency = fit_df["mean_latency_ms"].to_numpy(dtype=float)

    if len(fit_df) < 3:
        raise ValueError("Need at least 3 rows to fit the latency curve.")

    general_popt, general_pred = fit_general_model(throughput, latency)
    k1_popt, k1_pred = fit_k1_model(throughput, latency)

    fit_df["general_predicted_latency_ms"] = general_pred
    fit_df["general_abs_error_ms"] = np.abs(general_pred - latency)
    fit_df["k1_predicted_latency_ms"] = k1_pred
    fit_df["k1_abs_error_ms"] = np.abs(k1_pred - latency)

    output_dir = args.output_dir or default_output_dir(args.summary_csv)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_plot = args.output_plot or (output_dir / "curve_fit.png")
    plot_fit(throughput, latency, general_popt, k1_popt, output_plot)

    fit_details_path = output_dir / "fit_details.csv"
    fit_df.to_csv(fit_details_path, index=False)

    loo_df = leave_one_out_k1(throughput, latency)
    loo_path = output_dir / "loo_k1.csv"
    if loo_df is not None:
        loo_df.to_csv(loo_path, index=False)

    print("Observed points:")
    print(fit_df.to_string(index=False, float_format=lambda value: f"{value:.4f}"))
    print()

    print("General model fit:")
    print(
        f"a={general_popt[0]:.6f} mu={general_popt[1]:.4f} "
        f"b={general_popt[2]:.6f} k={general_popt[3]:.6f}"
    )
    print(
        f"rmse_ms={rmse(latency, general_pred):.6f} "
        f"r2={r2(latency, general_pred):.6f}"
    )
    print()

    print("k=1 model fit:")
    print(f"a={k1_popt[0]:.6f} mu={k1_popt[1]:.4f} b={k1_popt[2]:.6f}")
    print(
        f"rmse_ms={rmse(latency, k1_pred):.6f} "
        f"r2={r2(latency, k1_pred):.6f}"
    )
    print()

    if loo_df is not None:
        loo_rmse = rmse(
            loo_df["actual_mean_latency_ms"].to_numpy(dtype=float),
            loo_df["predicted_mean_latency_ms"].to_numpy(dtype=float),
        )
        print("Leave-one-out validation for k=1 model:")
        print(loo_df.to_string(index=False, float_format=lambda value: f"{value:.4f}"))
        print(f"loo_rmse_ms={loo_rmse:.6f}")
        print()
    else:
        print("Leave-one-out validation for k=1 model: skipped because fewer than 4 points are available.")
        print()

    print(f"Saved plot to {output_plot}")
    print(f"Saved fit details to {fit_details_path}")
    if loo_df is not None:
        print(f"Saved leave-one-out details to {loo_path}")
    print(
        "Note: the fully general model has four free parameters, so with only four load levels "
        "it can fit very tightly without proving predictive power."
    )


if __name__ == "__main__":
    main()
