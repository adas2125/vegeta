#!/usr/bin/env python3
"""Plot peak server completions during recovery for the worker-cap sweep."""

import argparse
import re
from collections import defaultdict
from pathlib import Path
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D


LOG_RE = re.compile(r"mode=(?P<mode>\w+).*completions_1s=(?P<completions>\d+)")
COLORS = {
    "5ms": "#1f4e79",
    "10ms": "#2f7d32",
    "20ms": "#b35c1e",
    "30ms": "#8f2d2d",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Plot peak recovery server completions.")
    parser.add_argument("--root", default="worker_cap_sweep_new/runs")
    parser.add_argument("--output", default="worker_cap_sweep_new/peak_server_completions_recovery.pdf")
    return parser.parse_args()


def delay_key(path):
    label = path.name.replace("delay_", "").replace("ms", "")
    return float(label)


def cap_key(path):
    return int(path.name.replace("cap_", ""))


def recovery_peak(server_log):
    saw_stall = False
    peaks = []
    for line in server_log.read_text(encoding="utf-8").splitlines():
        match = LOG_RE.search(line)
        if not match:
            continue
        mode = match.group("mode")
        if mode == "stalled":
            saw_stall = True
        elif saw_stall and mode == "healthy":
            peaks.append(int(match.group("completions")))
    return max(peaks) if peaks else 0


def load_rows(root):
    rows = []
    for delay_dir in sorted(Path(root).glob("delay_*"), key=delay_key):
        delay = delay_dir.name.replace("delay_", "")
        if delay == "5ms":
            continue
        adaptive_peak = recovery_peak(delay_dir / "adaptive" / "server.log")
        for cap_dir in sorted(delay_dir.glob("cap_*"), key=cap_key):
            rows.append(
                {
                    "delay": delay,
                    "cap": cap_key(cap_dir),
                    "adaptive_peak": adaptive_peak,
                    "capped_peak": recovery_peak(cap_dir / "server.log"),
                }
            )
    return rows


def main():
    args = parse_args()
    rows = load_rows(args.root)

    plt.rcParams.update(
        {
            "font.family": "serif",
            "axes.spines.top": False,
            "axes.spines.right": False,
        }
    )

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["delay"]].append(row)

    fig, ax = plt.subplots(figsize=(8.4, 5.4))
    for delay in sorted(grouped, key=lambda item: float(item.replace("ms", ""))):
        delay_rows = sorted(grouped[delay], key=lambda row: row["cap"])
        caps = [row["cap"] for row in delay_rows]
        capped = [row["capped_peak"] for row in delay_rows]
        adaptive = [row["adaptive_peak"] for row in delay_rows]
        color = COLORS.get(delay)

        ax.plot(caps, capped, marker="o", linewidth=2.2, color=color, label=f"{delay} capped")
        ax.plot(caps, adaptive, linestyle="--", linewidth=1.5, color=color, alpha=0.75)

    ax.set_title("Peak Server Completions During Recovery")
    ax.set_xlabel("Worker cap")
    ax.set_ylabel("Peak completions/s")
    ax.grid(True, color="#d8dee6", linewidth=0.8)

    handles, labels = ax.get_legend_handles_labels()
    handles.append(Line2D([0], [0], color="#555555", linestyle="--", linewidth=1.5))
    labels.append("adaptive baseline")
    ax.legend(handles, labels, frameon=False)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output)
    print(f"wrote {output}")


if __name__ == "__main__":
    main()
