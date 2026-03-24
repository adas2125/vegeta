#!/usr/bin/env python3
"""Plot Vegeta experiment outputs from metrics/report files."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


METRICS_RE = re.compile(r"^metrics_rps_(\d+)_delay_(.+)\.csv$")
REPORT_RE = re.compile(r"^report_rps_(\d+)_delay_(.+)\.json$")


@dataclass(frozen=True)
class SampleKey:
    rps: int
    delay: str

    @property
    def label(self) -> str:
        return f"rps={self.rps}, delay={self.delay}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read Vegeta outputs in an out/ directory and generate plots for "
            "selected rps/delay samples."
        )
    )
    parser.add_argument("--out-dir", default="out", help="Directory with metrics/report files.")
    parser.add_argument(
        "--plot-dir",
        default="out/plots",
        help="Directory where generated PNG files will be written.",
    )
    parser.add_argument(
        "--rps",
        default="all",
        help='Comma-separated RPS values to include, e.g. "500,1000". Default: all.',
    )
    parser.add_argument(
        "--delays",
        default="all",
        help='Comma-separated delay values to include, e.g. "10ms,1s". Default: all.',
    )
    return parser.parse_args()


def parse_list_arg(value: str) -> set[str] | None:
    if value.strip().lower() == "all":
        return None
    entries = {part.strip() for part in value.split(",") if part.strip()}
    return entries if entries else None


def parse_duration_to_ms(value: str) -> float:
    value = value.strip()
    units = {"ms": 1.0, "s": 1000.0, "m": 60000.0, "h": 3600000.0}
    for unit, factor in units.items():
        if value.endswith(unit):
            number = value[: -len(unit)]
            try:
                return float(number) * factor
            except ValueError as exc:
                raise ValueError(f"invalid duration value: {value}") from exc
    raise ValueError(f"unsupported duration unit in: {value}")


def discover_samples(out_dir: Path) -> tuple[dict[SampleKey, Path], dict[SampleKey, Path]]:
    metrics: dict[SampleKey, Path] = {}
    reports: dict[SampleKey, Path] = {}

    for path in out_dir.iterdir():
        if not path.is_file():
            continue
        m = METRICS_RE.match(path.name)
        if m:
            metrics[SampleKey(rps=int(m.group(1)), delay=m.group(2))] = path
            continue
        r = REPORT_RE.match(path.name)
        if r:
            reports[SampleKey(rps=int(r.group(1)), delay=r.group(2))] = path

    return metrics, reports


def filter_samples(
    all_keys: set[SampleKey], selected_rps: set[str] | None, selected_delays: set[str] | None
) -> list[SampleKey]:
    filtered: list[SampleKey] = []
    for key in sorted(all_keys, key=lambda s: (s.rps, parse_duration_to_ms(s.delay))):
        if selected_rps is not None and str(key.rps) not in selected_rps:
            continue
        if selected_delays is not None and key.delay not in selected_delays:
            continue
        filtered.append(key)
    return filtered


def read_metrics_csv(path: Path) -> dict[str, list[float]]:
    data = {
        "elapsed_s": [],
        "workers": [],
        "connections": [],
        "send_delay_ms": [],
        "in_flight": [],
        "completions": [],
    }
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data["elapsed_s"].append(float(row["elapsed_ms"]) / 1000.0)
            data["workers"].append(float(row["workers"]))
            data["connections"].append(float(row["connections"]))
            data["send_delay_ms"].append(float(row["send_delay_ms"]))
            data["in_flight"].append(float(row["in_flight"]))
            data["completions"].append(float(row["completions"]))
    return data


def plot_time_series(sample: SampleKey, metrics_path: Path, plot_dir: Path) -> None:
    series = read_metrics_csv(metrics_path)
    x = series["elapsed_s"]
    expected_in_flight = sample.rps * (parse_duration_to_ms(sample.delay) / 1000.0)

    fig, axes = plt.subplots(4, 1, figsize=(12, 14), sharex=True)
    fig.suptitle(f"Vegeta Time Series ({sample.label})", fontsize=14)

    axes[0].plot(x, series["workers"], label="workers", linewidth=1.2)
    axes[0].plot(x, series["connections"], label="connections", linewidth=1.2)
    axes[0].axhline(
        expected_in_flight,
        label=f"Little's Law expected (L=λW)={expected_in_flight:.1f}",
        color="tab:purple",
        linestyle="--",
        linewidth=1.2,
    )
    axes[0].set_ylabel("count")
    axes[0].set_title("Workers / Connections")
    axes[0].grid(alpha=0.25)
    axes[0].legend(loc="best")

    axes[1].plot(x, series["send_delay_ms"], color="tab:orange", linewidth=1.2)
    axes[1].set_ylabel("ms")
    axes[1].set_title("Send Delay")
    axes[1].grid(alpha=0.25)

    axes[2].plot(x, series["in_flight"], color="tab:green", linewidth=1.2)
    axes[2].set_ylabel("count")
    axes[2].set_title("In Flight")
    axes[2].grid(alpha=0.25)

    axes[3].plot(x, series["completions"], color="tab:red", linewidth=1.2)
    axes[3].set_ylabel("count")
    axes[3].set_title("Completions")
    axes[3].set_xlabel("Elapsed Time (s)")
    axes[3].grid(alpha=0.25)

    fig.tight_layout(rect=(0, 0, 1, 0.97))
    output = plot_dir / f"timeseries_rps_{sample.rps}_delay_{sample.delay}.png"
    fig.savefig(output, dpi=160)
    plt.close(fig)


def compute_error_requests(report: dict[str, Any]) -> int:
    requests = int(report.get("requests", 0))
    success = report.get("success")
    if success is not None:
        return max(0, int(round(requests * (1.0 - float(success)))))

    status_codes = report.get("status_codes", {})
    ok = 0
    for code, count in status_codes.items():
        if str(code).startswith("2"):
            ok += int(count)
    return max(0, requests - ok)


def read_report_metrics(sample: SampleKey, report_path: Path) -> dict[str, float]:
    with report_path.open("r") as f:
        report = json.load(f)

    target_ms = parse_duration_to_ms(sample.delay)
    lat = report.get("latencies", {})

    def error_from_percentile(key: str) -> float:
        ns = float(lat.get(key, 0.0))
        observed_ms = ns / 1_000_000.0
        return observed_ms - target_ms

    return {
        "p50_err_ms": error_from_percentile("50th"),
        "p90_err_ms": error_from_percentile("90th"),
        "p95_err_ms": error_from_percentile("95th"),
        "p99_err_ms": error_from_percentile("99th"),
        "errors": float(compute_error_requests(report)),
        "rate": float(report.get("rate", 0.0)),
        "throughput": float(report.get("throughput", 0.0)),
        "requests": float(report.get("requests", 0.0)),
    }


def plot_report_overview(samples: list[SampleKey], reports: dict[SampleKey, Path], plot_dir: Path) -> bool:
    if not samples:
        return False

    # Remove stale outputs from older script versions.
    for stale in (
        "report_overview_selected_samples.png",
        "report_rate_selected_samples.png",
        "report_throughput_selected_samples.png",
        "report_requests_selected_samples.png",
    ):
        stale_path = plot_dir / stale
        if stale_path.exists():
            stale_path.unlink()

    full_metrics = [(s, read_report_metrics(s, reports[s])) for s in samples]
    all_labels = [f"{s.rps}/{s.delay}" for s, _ in full_metrics]
    all_x = list(range(len(full_metrics)))
    all_rate = [m["rate"] for _, m in full_metrics]
    # Keep only cases where at least one percentile deviates by > 500ms from target.
    filtered = [
        (s, m)
        for s, m in full_metrics
        if max(abs(m["p50_err_ms"]), abs(m["p90_err_ms"]), abs(m["p95_err_ms"]), abs(m["p99_err_ms"]))
        > 500.0
    ]
    if not filtered:
        print("No report plots generated: no samples with latency deviation > 500ms.")
        # Still generate rate plot for all selected samples.
        fig, ax = plt.subplots(1, 1, figsize=(max(12, len(full_metrics) * 1.2), 4))
        ax.plot(all_x, all_rate, marker="o", linewidth=1.4)
        ax.set_title("Rate (all selected samples)")
        ax.set_ylabel("req/s")
        ax.set_xticks(all_x, all_labels, rotation=45, ha="right")
        ax.grid(alpha=0.25)
        fig.tight_layout()
        fig.savefig(plot_dir / "report_rate_all_selected_samples.png", dpi=160)
        plt.close(fig)
        return True

    plot_samples = [s for s, _ in filtered]
    metrics = [m for _, m in filtered]
    labels = [f"{s.rps}/{s.delay}" for s in plot_samples]
    x = list(range(len(plot_samples)))
    width = 0.18

    fig, ax = plt.subplots(1, 1, figsize=(max(12, len(plot_samples) * 1.2), 5))
    ax.bar([i - 1.5 * width for i in x], [m["p50_err_ms"] for m in metrics], width, label="p50")
    ax.bar([i - 0.5 * width for i in x], [m["p90_err_ms"] for m in metrics], width, label="p90")
    ax.bar([i + 0.5 * width for i in x], [m["p95_err_ms"] for m in metrics], width, label="p95")
    ax.bar([i + 1.5 * width for i in x], [m["p99_err_ms"] for m in metrics], width, label="p99")
    ax.axhline(0.0, color="black", linewidth=0.8, alpha=0.7)
    ax.set_ylabel("Observed - Target (ms)")
    ax.set_title("Latency Error vs Target Delay (|error| > 500ms)")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(loc="best")
    ax.set_xticks(x, labels, rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(plot_dir / "report_latency_error_selected_samples.png", dpi=160)
    plt.close(fig)

    def plot_single_metric(key: str, ylabel: str, title: str, filename: str) -> None:
        fig, ax = plt.subplots(1, 1, figsize=(max(12, len(plot_samples) * 1.2), 4))
        ax.plot(x, [m[key] for m in metrics], marker="o", linewidth=1.4)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x, labels, rotation=45, ha="right")
        ax.grid(alpha=0.25)
        fig.tight_layout()
        fig.savefig(plot_dir / filename, dpi=160)
        plt.close(fig)

    plot_single_metric("errors", "count", "Errors (|latency error| > 500ms cases)", "report_errors_selected_samples.png")
    # Rate should include all selected combinations to make outliers visible.
    fig, ax = plt.subplots(1, 1, figsize=(max(12, len(full_metrics) * 1.2), 4))
    ax.plot(all_x, all_rate, marker="o", linewidth=1.4)
    ax.set_title("Rate (all selected samples)")
    ax.set_ylabel("req/s")
    ax.set_xticks(all_x, all_labels, rotation=45, ha="right")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(plot_dir / "report_rate_all_selected_samples.png", dpi=160)
    plt.close(fig)
    return True


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    plot_dir = Path(args.plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    if not out_dir.exists() or not out_dir.is_dir():
        raise SystemExit(f"out dir does not exist: {out_dir}")

    selected_rps = parse_list_arg(args.rps)
    selected_delays = parse_list_arg(args.delays)

    metrics, reports = discover_samples(out_dir)
    sample_keys = set(metrics.keys()) & set(reports.keys())
    selected = filter_samples(sample_keys, selected_rps, selected_delays)

    if not selected:
        raise SystemExit(
            "no matching samples found. check --rps/--delays and available files in out dir."
        )

    for sample in selected:
        plot_time_series(sample, metrics[sample], plot_dir)

    report_plots_generated = plot_report_overview(selected, reports, plot_dir)

    print(f"Generated {len(selected)} time-series plot(s) in {plot_dir}")
    if report_plots_generated:
        print(f"Generated report plots in {plot_dir}")


if __name__ == "__main__":
    main()
