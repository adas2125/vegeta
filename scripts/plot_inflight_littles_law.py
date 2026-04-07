#!/usr/bin/env python3

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


RPS = 3000
MEAN_DELAY_S = 0.050
BASE_FONT_SIZE = 23
AXIS_LABEL_SIZE = 27
TICK_LABEL_SIZE = 21
LEGEND_SIZE = 21

ROOT = Path("archive")
FILES = {
    "Exponential": ROOT / "results_exp.csv",
    # "Fixed": ROOT / "results_fixed.csv",
    # "Failed case": ROOT / "results_bad.csv",
}


def configure_plot_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": BASE_FONT_SIZE,
            "axes.labelsize": AXIS_LABEL_SIZE,
            "xtick.labelsize": TICK_LABEL_SIZE,
            "ytick.labelsize": TICK_LABEL_SIZE,
            "legend.fontsize": LEGEND_SIZE,
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )


def load_series(path: Path) -> tuple[list[float], list[int]]:
    elapsed_s: list[float] = []
    in_flight: list[int] = []

    with path.open(newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            elapsed_s.append(float(row["elapsed_ms"]) / 1000.0)
            in_flight.append(int(row["in_flight"]))

    return elapsed_s, in_flight


def main() -> None:
    configure_plot_style()

    # Little's Law expected in-flight count
    expected_in_flight = RPS * MEAN_DELAY_S

    # For an open-loop stable system with exponential service times,
    # in-flight is approximately Poisson with:
    # mean = expected_in_flight
    # std  = sqrt(expected_in_flight)
    sigma_in_flight = expected_in_flight ** 0.5

    # Statistical reference bands for exponential service time
    exp_lower_2sigma = max(expected_in_flight - 2 * sigma_in_flight, 0.0)
    exp_upper_2sigma = expected_in_flight + 2 * sigma_in_flight
    exp_lower_3sigma = max(expected_in_flight - 3 * sigma_in_flight, 0.0)
    exp_upper_3sigma = expected_in_flight + 3 * sigma_in_flight

    plt.figure(figsize=(12, 6))

    for label, path in FILES.items():
        elapsed_s, in_flight = load_series(path)
        plt.plot(elapsed_s, in_flight, label=label, linewidth=1.5)

    # Mean target from Little's Law
    plt.axhline(
        expected_in_flight,
        color="black",
        linestyle="--",
        linewidth=1.5,
        label=f"Little's Law target ({expected_in_flight:.0f})",
    )

    # Exponential-service reference bands
    plt.axhline(
        exp_lower_2sigma,
        color="tab:orange",
        linestyle="--",
        linewidth=1.2,
        label=f"Exp approx -2σ ({exp_lower_2sigma:.1f})",
    )
    plt.axhline(
        exp_upper_2sigma,
        color="tab:orange",
        linestyle="--",
        linewidth=1.2,
        label=f"Exp approx +2σ ({exp_upper_2sigma:.1f})",
    )

    # plt.title("In-Flight Requests Over Time")
    plt.xlabel("Elapsed time (s)")
    plt.ylabel("In-flight requests")
    plt.grid(True, alpha=0.3)
    # set the y-axis limits to show the reference bands clearly
    plt.ylim(0, exp_upper_2sigma * 1.5)
    plt.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        frameon=False,
        ncol=2,
    )
    plt.tight_layout()
    # save as pdf for high-quality vector output
    plt.savefig("inflight_littles_law.pdf")


if __name__ == "__main__":
    main()
