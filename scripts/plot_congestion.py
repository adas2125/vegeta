#!/usr/bin/env python3
"""Plot HTTP/1 and HTTP/2 metrics side by side over time."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.ticker import AutoMinorLocator

SERIES_STYLES = {
    "http1": {
        "color": "#1f4e79",
        "linestyle": "-",
        "marker": "o",
    },
    "http2": {
        "color": "#c75b12",
        "linestyle": "--",
        "marker": "s",
    },
    "baseline": {
        "color": "#4c7c2f",
        "linestyle": ":",
        "marker": "^",
    },
}

BASE_FONT_SIZE = 23
AXIS_LABEL_SIZE = 27
TITLE_SIZE = 28
TICK_LABEL_SIZE = 21
LEGEND_SIZE = 21


def style_axis_grid(ax: plt.Axes) -> None:
    ax.set_axisbelow(True)
    ax.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(which="major", axis="both", color="#cfd6df", linewidth=0.9, alpha=0.9)
    ax.grid(which="minor", axis="both", color="#e8edf3", linewidth=0.6, alpha=0.9)


def configure_plot_style() -> None:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read http1/http2 metrics CSV files and plot workers, connections, "
            "and completions over time."
        )
    )
    parser.add_argument("--http1-csv", default="congestion_experiments/http1_metrics.csv", help="Path to the HTTP/1 metrics CSV.")
    parser.add_argument("--http2-csv", default="congestion_experiments/http2_metrics.csv", help="Path to the HTTP/2 metrics CSV.")
    parser.add_argument(
        "--output",
        default="congestion_experiments/http1_vs_http2_metrics.pdf",
        help="Path to the output PDF.",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=14.0,
        help="Only plot samples with elapsed time up to this many seconds.",
    )
    parser.add_argument(
        "--baseline-rps",
        type=float,
        default=5000.0,
        help="Target request rate used for the cumulative sends baseline.",
    )
    parser.add_argument(
        "--completions-only",
        action="store_true",
        help="Only plot completions over time instead of the full 3-panel figure.",
    )
    return parser.parse_args()


def read_metrics(path: Path, max_seconds: float) -> dict[str, list[float]]:
    data = {
        "elapsed_s": [],
        "workers": [],
        "connections": [],
        "completions": [],
    }

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            elapsed_s = float(row["elapsed_ms"]) / 1000.0
            if elapsed_s > max_seconds:
                continue

            data["elapsed_s"].append(elapsed_s)
            data["workers"].append(float(row["workers"]))
            data["connections"].append(float(row["connections"]))
            data["completions"].append(float(row["completions"]))

    return data


def plot_series(
    http1: dict[str, list[float]],
    http2: dict[str, list[float]],
    output: Path,
    max_seconds: float,
    baseline_rps: float,
) -> None:
    configure_plot_style()
    fig = plt.figure(figsize=(11.2, 8.6), constrained_layout=True)
    grid = GridSpec(2, 2, figure=fig, width_ratios=[1.0, 1.2])
    ax_workers = fig.add_subplot(grid[0, 0])
    ax_connections = fig.add_subplot(grid[1, 0], sharex=ax_workers)
    ax_completions = fig.add_subplot(grid[:, 1])

    left_panel_specs = [
        (ax_workers, "workers", "Total Workers"),
        (ax_connections, "connections", "Total Connections"),
    ]

    for ax, key, ylabel in left_panel_specs:
        ax.plot(
            http1["elapsed_s"],
            http1[key],
            label="HTTP/1",
            color=SERIES_STYLES["http1"]["color"],
            linewidth=3.0,
            linestyle=SERIES_STYLES["http1"]["linestyle"],
            marker=SERIES_STYLES["http1"]["marker"],
            markersize=6.3,
            markerfacecolor="white",
            markeredgewidth=1.3,
            markevery=max(len(http1["elapsed_s"]) // 10, 1),
        )
        ax.plot(
            http2["elapsed_s"],
            http2[key],
            label="HTTP/2",
            color=SERIES_STYLES["http2"]["color"],
            linewidth=3.0,
            linestyle=SERIES_STYLES["http2"]["linestyle"],
            marker=SERIES_STYLES["http2"]["marker"],
            markersize=6.3,
            markerfacecolor="white",
            markeredgewidth=1.3,
            markevery=max(len(http2["elapsed_s"]) // 10, 1),
        )
        ax.set_ylabel(ylabel)
        ax.set_xlim(0, max_seconds)
        style_axis_grid(ax)
        ax.margins(x=0.01)

    plt.setp(ax_workers.get_xticklabels(), visible=False)
    ax_connections.set_xlabel("Elapsed Time (s)")
    ax_connections.set_xticks([0, 2, 4, 6, 8, 10, 12, 14])

    line_http1, = ax_completions.plot(
        http1["elapsed_s"],
        http1["completions"],
        label="HTTP/1",
        color=SERIES_STYLES["http1"]["color"],
        linewidth=3.0,
        linestyle=SERIES_STYLES["http1"]["linestyle"],
        marker=SERIES_STYLES["http1"]["marker"],
        markersize=6.3,
        markerfacecolor="white",
        markeredgewidth=1.3,
        markevery=max(len(http1["elapsed_s"]) // 10, 1),
    )
    line_http2, = ax_completions.plot(
        http2["elapsed_s"],
        http2["completions"],
        label="HTTP/2",
        color=SERIES_STYLES["http2"]["color"],
        linewidth=3.0,
        linestyle=SERIES_STYLES["http2"]["linestyle"],
        marker=SERIES_STYLES["http2"]["marker"],
        markersize=6.3,
        markerfacecolor="white",
        markeredgewidth=1.3,
        markevery=max(len(http2["elapsed_s"]) // 10, 1),
    )
    baseline_values = [baseline_rps * x for x in http1["elapsed_s"]]
    baseline, = ax_completions.plot(
        http1["elapsed_s"],
        baseline_values,
        label=f"Send Rate ({baseline_rps:,.0f} req/s)",
        color=SERIES_STYLES["baseline"]["color"],
        linestyle=SERIES_STYLES["baseline"]["linestyle"],
        linewidth=2.5,
        marker=SERIES_STYLES["baseline"]["marker"],
        markersize=5.8,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(http1["elapsed_s"]) // 10, 1),
    )
    ax_completions.set_xlabel("Elapsed Time (s)")
    ax_completions.set_ylabel("Total Completions")
    ax_completions.set_xlim(0, max_seconds)
    ax_completions.set_xticks([0, 2, 4, 6, 8, 10, 12, 14])
    style_axis_grid(ax_completions)
    ax_completions.margins(x=0.01)
    fig.legend(
        [line_http1, line_http2, baseline],
        ["HTTP/1", "HTTP/2", f"Send Rate ({baseline_rps:,.0f} req/s)"],
        loc="upper center",
        ncol=3,
        frameon=False,
        handlelength=3.1,
        columnspacing=1.6,
        bbox_to_anchor=(0.54, 1.08),
    )

    plt.savefig(output, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


def plot_completions_only(
    http1: dict[str, list[float]],
    http2: dict[str, list[float]],
    output: Path,
    max_seconds: float,
    baseline_rps: float,
) -> None:
    configure_plot_style()
    fig, ax = plt.subplots(1, 1, figsize=(7.6, 5.4), constrained_layout=True)

    line_http1, = ax.plot(
        http1["elapsed_s"],
        http1["completions"],
        label="HTTP/1",
        color=SERIES_STYLES["http1"]["color"],
        linewidth=2.8,
        linestyle=SERIES_STYLES["http1"]["linestyle"],
        marker=SERIES_STYLES["http1"]["marker"],
        markersize=5.9,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(http1["elapsed_s"]) // 10, 1),
    )
    line_http2, = ax.plot(
        http2["elapsed_s"],
        http2["completions"],
        label="HTTP/2",
        color=SERIES_STYLES["http2"]["color"],
        linewidth=2.8,
        linestyle=SERIES_STYLES["http2"]["linestyle"],
        marker=SERIES_STYLES["http2"]["marker"],
        markersize=5.9,
        markerfacecolor="white",
        markeredgewidth=1.2,
        markevery=max(len(http2["elapsed_s"]) // 10, 1),
    )
    baseline_values = [baseline_rps * x for x in http1["elapsed_s"]]
    baseline, = ax.plot(
        http1["elapsed_s"],
        baseline_values,
        label=f"Baseline ({baseline_rps:,.0f} req/s)",
        color=SERIES_STYLES["baseline"]["color"],
        linestyle=SERIES_STYLES["baseline"]["linestyle"],
        linewidth=2.2,
        marker=SERIES_STYLES["baseline"]["marker"],
        markersize=5.4,
        markerfacecolor="white",
        markeredgewidth=1.1,
        markevery=max(len(http1["elapsed_s"]) // 10, 1),
    )

    # ax.set_title("Completions")
    ax.set_xlabel("Elapsed Time (s)")
    ax.set_ylabel("Count")
    ax.set_xlim(0, max_seconds)
    ax.set_xticks([0, 2, 4, 6, 8, 10, 12, 14])
    style_axis_grid(ax)
    ax.margins(x=0.01)
    ax.legend(
        [line_http1, line_http2, baseline],
        ["HTTP/1", "HTTP/2", f"Baseline ({baseline_rps:,.0f} req/s)"],
        loc="upper left",
        frameon=False,
        handlelength=2.8,
        borderaxespad=0.2,
    )

    plt.savefig(output, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


def main() -> None:
    args = parse_args()

    http1_path = Path(args.http1_csv)
    http2_path = Path(args.http2_csv)
    output_path = Path(args.output)

    http1 = read_metrics(http1_path, args.max_seconds)
    http2 = read_metrics(http2_path, args.max_seconds)

    if args.completions_only:
        plot_completions_only(
            http1,
            http2,
            output_path,
            args.max_seconds,
            args.baseline_rps,
        )
    else:
        plot_series(http1, http2, output_path, args.max_seconds, args.baseline_rps)
    print(f"Saved plot to {output_path}")


if __name__ == "__main__":
    main()
