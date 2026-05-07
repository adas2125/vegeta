import csv
import json
from collections import defaultdict
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

# Configuration Constants
TARGET_RPS = 1500
ROOT = Path("HotelReservation_results/hotelreservation_rps_1500")
STAGE_B = ROOT / "stage_b_variable"
FIGURE_OUTPUT = Path("hotelreservation_rps1500_delay_ecdf.png")
PDF_OUTPUT = Path("hotelreservation_rps1500_delay_ecdf.pdf")
TABLE_OUTPUT = Path("output.csv")
TABLE_RPS = (1000, 1500, 2000)
TABLE_SUTS = ("HotelReservation", "SocialNetwork")

TRIM_START = 5
TRIM_END = 1
PERCENTILES = (5, 25, 50, 75, 95, 99)
X_AXIS_MIN_PERCENTILE = 0.1
X_AXIS_MAX_PERCENTILE = 99.9
LOG_PREFIX = "XLG-WINDOW:"

# For plotting metrics
METRICS = (
    ("scheduler", "SchedulerDelays", "Scheduler"),
    ("connection", "ConnectionDelays", "Connection"),
)

PANELS_BY_METRIC = {
    "scheduler": (
        ("Few Workers", {"NORMAL", "FEW_WORKERS"}),
        ("CPU Contention", {"NORMAL", "CPU_CONTENTION"}),
    ),
    "connection": (
        ("Few Connections", {"NORMAL", "FEW_CONNECTIONS"}),
        ("SUT/Path Changes", {"NORMAL", "SUT_DEGRADED", "SUT_FASTER"}),
    ),
}

MODE_NAMES = {
    "NORMAL": "Normal",
    "FEW_CONNECTIONS": "FewConnections",
    "FEW_WORKERS": "FewWorkers",
    "CPU_CONTENTION": "CpuContention",
    "SUT_DEGRADED": "SuTDegraded",
    "SUT_FASTER": "SuTFaster",
}

MODE_COLORS = {
    "NORMAL": "#4C78A8",
    "FEW_CONNECTIONS": "#F58518",
    "FEW_WORKERS": "#54A24B",
    "CPU_CONTENTION": "#E45756",
    "SUT_DEGRADED": "#B279A2",
    "SUT_FASTER": "#72B7B2",
}

MODE_LINESTYLES = {
    "NORMAL": "-",
    "FEW_CONNECTIONS": "--",
    "FEW_WORKERS": "-.",
    "CPU_CONTENTION": ":",
    "SUT_DEGRADED": (0, (5, 1)),
    "SUT_FASTER": (0, (3, 1, 1, 1)),
}

MODE_MARKERS = {
    "NORMAL": "o",
    "FEW_CONNECTIONS": "s",
    "FEW_WORKERS": "^",
    "CPU_CONTENTION": "D",
    "SUT_DEGRADED": "v",
    "SUT_FASTER": "P",
}

MILD_ONLY_MODES = {"FEW_CONNECTIONS", "FEW_WORKERS", "CPU_CONTENTION"}
MODE_ORDER = {mode: index for index, mode in enumerate(MODE_NAMES)}
OMIT_SEVERITY_FROM_LABEL = {"FEW_CONNECTIONS", "FEW_WORKERS", "CPU_CONTENTION"}


def payloads(path):
    """
    Loads the xlg windows log file and extracts the relevant rows.
    """
    rows = [
        json.loads(line[len(LOG_PREFIX) :])
        for line in path.open(encoding="utf-8")
        if line.startswith(LOG_PREFIX)
    ]
    return rows[TRIM_START:-TRIM_END]


def condition_for(path):
    """
    Parses the failure mode and severity from the log file path structure.
    """
    condition_index = path.parts.index("conditions")
    mode = path.parts[condition_index + 1]
    severity = path.parts[condition_index + 2]
    return mode, "none" if severity.startswith("run_") else severity


def collect_condition_values():
    """
    Collects scheduler and connection delay values from logs, organized by failure mode and severity.
    """
    # initialize dictionary w/ keys 'scheduler' and 'connection'
    groups = {name: defaultdict(list) for name, *_ in METRICS}

    # obtaining all the log paths for specified RPS in stage B across different conditions
    paths = sorted(STAGE_B.rglob(f"conditions/**/xlg_windows_rps{TARGET_RPS}.log"))

    for path in paths:
        # obtaining the failure mode and severity from the path structure
        mode, severity = condition_for(path)
        if mode in MILD_ONLY_MODES and severity != "mild":
            continue

        # obtaining rows for each window
        all_rows = payloads(path)
        for row in all_rows:
            # for a given row, populate groups dict
            for metric_name, log_key, _ in METRICS:
                # e.g. metric_name = 'scheduler', log_key = 'SchedulerDelays'
                groups[metric_name][(mode, severity)].extend(row[log_key])

    return groups


def label(mode, severity):
    """
    Generates a label for the legend given mode & severity.
    """
    name = MODE_NAMES[mode]
    if severity == "none" or mode in OMIT_SEVERITY_FROM_LABEL:
        return name
    return f"{name} ({severity})"


def draw_ecdf(ax, mode, severity, values):
    xs = sorted(values)
    ys = [(index + 1) / len(xs) for index in range(len(xs))]
    color = MODE_COLORS[mode]
    marker = MODE_MARKERS[mode]

    ax.step(
        xs,
        ys,
        where="post",
        color=color,
        linestyle=MODE_LINESTYLES[mode],
        linewidth=2.8,
        marker=marker,
        markevery=max(1, len(xs) // 12),
        markersize=4.8,
        markerfacecolor="white",
        markeredgecolor=color,
        markeredgewidth=1.4,
        label=label(mode, severity),
    )


def sort_key(item):
    """
    Defines a sorting key for ordering the legend entries by mode.
    """
    mode, _ = item[0]
    return MODE_ORDER[mode]


def x_limits(groups, panels):
    plotted_modes = set().union(*(modes for _, modes in panels))
    all_values = [
        value
        for (mode, _), values in groups.items()
        if mode in plotted_modes
        for value in values
    ]
    return (
        np.percentile(all_values, X_AXIS_MIN_PERCENTILE) * 0.8,
        np.percentile(all_values, X_AXIS_MAX_PERCENTILE) * 1.05,
    )


def plot_panel(ax, groups, panel_title, modes, x_min, x_max):
    # consists of mode, severity, and corresponding values for metric_name
    panel_groups = [
        (key, values)
        for key, values in sorted(groups.items(), key=sort_key)
        if key[0] in modes
    ]

    # drawing the ecdf
    for (mode, severity), values in panel_groups:
        # e.g. mode = 'FEW_CONNECTIONS', severity = 'mild', values = [12.3, 15.6, ...]
        draw_ecdf(ax, mode, severity, values)

    ax.set_xscale("log")
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(0, 1.01)
    ax.set_title(panel_title, fontweight="semibold", pad=12)
    ax.grid(alpha=0.25)
    ax.legend(loc="lower right", frameon=True)
    ax.tick_params(axis="both", which="major", labelsize=18)
    ax.tick_params(axis="both", which="minor", labelsize=15)
    ax.tick_params(axis="y", labelleft=True)


def plot_delays(groups_by_metric):
    fig, axes = plt.subplots(
        len(METRICS),
        2,
        figsize=(12, 7.5),
        sharex="row",
        sharey=True,
        constrained_layout=True,
    )
    fig.supxlabel("Delay (ms)", fontsize=22, fontweight="semibold")
    fig.supylabel("ECDF", fontsize=22, fontweight="semibold")

    for row, (metric_name, _, metric_title) in enumerate(METRICS):
        metric_groups = groups_by_metric[metric_name]
        panels = PANELS_BY_METRIC[metric_name]
        x_min, x_max = x_limits(metric_groups, panels)

        for col, (panel_title, modes) in enumerate(panels):
            title = f"{metric_title}: {panel_title}"
            plot_panel(
                axes[row, col],
                metric_groups,
                title,
                modes,
                x_min,
                x_max,
            )

    fig.savefig(FIGURE_OUTPUT, dpi=180)
    fig.savefig(PDF_OUTPUT)
    plt.close(fig)
    return FIGURE_OUTPUT, PDF_OUTPUT


def healthy_logs_for(sut, rps):
    if sut == "HotelReservation":
        root = Path(f"HotelReservation_results/hotelreservation_rps_{rps}/stage_a_fixed")
    else:
        root = Path("SocialNetwork_results/stage_a_fixed")

    return sorted(root.rglob(f"healthy/run_*/xlg_windows_rps{rps}.log"))


def write_healthy_table():
    rows = []

    for sut in TABLE_SUTS:
        for rps in TABLE_RPS:
            # for a given SUT and RPS, obtain paths for healthy logs from stage A
            healthy_logs = healthy_logs_for(sut, rps)

            # for a metric and its key in the log
            for metric_name, log_key, _ in METRICS:
                values = []

                # go through all the healthy logs and obtain values for the metric key, e.g. 'SchedulerDelays'
                for path in healthy_logs:
                    for row in payloads(path):
                        values.extend(row[log_key])

                # add the different percentile values
                rows.append(
                    {
                        "sut": sut,
                        "rps": rps,
                        "metric": metric_name,
                        **{
                            f"p{percent:02d}": f"{np.percentile(values, percent):.3f}"
                            for percent in PERCENTILES
                        },
                    }
                )

    # write the rows
    fieldnames = ["sut", "rps", "metric"] + [f"p{percent:02d}" for percent in PERCENTILES]
    with TABLE_OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return TABLE_OUTPUT


if __name__ == "__main__":

    # matplotlib configuration for better aesthetics
    plt.rcParams.update(
        {
            "font.size": 18,
            "axes.titlesize": 20,
            "axes.labelsize": 21,
            "xtick.labelsize": 18,
            "ytick.labelsize": 18,
            "legend.fontsize": 13,
        }
    )

    groups = collect_condition_values()
    for metric_name, groups_by_metric in groups.items():
        for (mode, severity), values in groups_by_metric.items():
            print(f"{metric_name} - {mode} ({severity}): {len(values)} values")

    # plotting scheduler and connection delays into one figure
    plot_delays(groups)
    write_healthy_table()
