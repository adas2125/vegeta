from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import AutoMinorLocator

# constants
DEFAULT_ROOT_DIR = Path("misleading_results")
DEFAULT_CASE_BASELINE = "well_provisioned"
DEFAULT_CASE_ABNORMAL = "constrained"
DEFAULT_OUTPUT = "lg_requests_sent_cumulative_only.pdf"
DEFAULT_RUN_DIR = "run_20260328_173813"

# for plotting
CASE_STYLES = {
    "baseline": {
        "color": "tab:blue",
        "linestyle": "-",
        "marker": "o",
    },
    "abnormal": {
        "color": "tab:red",
        "linestyle": "--",
        "marker": "s",
    },
}

BASE_FONT_SIZE = 23
AXIS_LABEL_SIZE = 27
TITLE_SIZE = 28
TICK_LABEL_SIZE = 21
LEGEND_SIZE = 21

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


def style_axis_grid(ax):
    ax.set_axisbelow(True)
    ax.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(which="major", axis="both", color="#cfd6df", linewidth=0.9, alpha=0.9)
    ax.grid(which="minor", axis="both", color="#e8edf3", linewidth=0.6, alpha=0.9)


def case_label(case_name):
    return case_name.replace("_", " ").title()


def load_sent_cumulative(run_dir, case_name):

    # loading the metrics file for the run
    results_path = run_dir / case_name / f"{case_name}_metrics.csv"
    results_df = pd.read_csv(results_path)

    # loading the time, in-flight requests, and completions
    results_df["elapsed_s"] = pd.to_numeric(results_df["elapsed_ms"], errors="coerce") / 1000.0
    results_df["in_flight"] = pd.to_numeric(results_df["in_flight"], errors="coerce")
    results_df["completions"] = pd.to_numeric(results_df["completions"], errors="coerce")

    results_df = (
        results_df.dropna(subset=["elapsed_s", "in_flight", "completions"])
        .sort_values("elapsed_s")
        .reset_index(drop=True)
    )

    # obtaining the cumulative sends using in flight and completions
    results_df["sent_cumulative"] = results_df["completions"] + results_df["in_flight"]
    return results_df[["elapsed_s", "sent_cumulative"]]


def plot_cumulative_sent(output_path, baseline_name, baseline_results, abnormal_name, abnormal_results):
    configure_plot_style()

    fig, ax = plt.subplots(1, 1, figsize=(8.4, 5.8), constrained_layout=True)

    for name, results, style in (
        (baseline_name, baseline_results, CASE_STYLES["baseline"]),
        (abnormal_name, abnormal_results, CASE_STYLES["abnormal"]),
    ):
        ax.plot(
            results["elapsed_s"],
            results["sent_cumulative"],
            linewidth=3.0,
            label=case_label(name),
            color=style["color"],
            linestyle=style["linestyle"],
            marker=style["marker"],
            markersize=6.0,
            markerfacecolor="white",
            markeredgewidth=1.2,
            markevery=max(len(results) // 12, 1),
        )

    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Cumulative sent")
    style_axis_grid(ax)
    ax.legend(frameon=False, handlelength=3.0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


if __name__ == "__main__":
    # loading the constrainted and under-provisioned results
    baseline_results = load_sent_cumulative(DEFAULT_ROOT_DIR / DEFAULT_RUN_DIR, DEFAULT_CASE_BASELINE)
    abnormal_results = load_sent_cumulative(DEFAULT_ROOT_DIR / DEFAULT_RUN_DIR, DEFAULT_CASE_ABNORMAL)
    output_path = DEFAULT_ROOT_DIR / DEFAULT_RUN_DIR / DEFAULT_OUTPUT

    plot_cumulative_sent(
        output_path,
        DEFAULT_CASE_BASELINE,
        baseline_results,
        DEFAULT_CASE_ABNORMAL,
        abnormal_results,
    )
