#!/usr/bin/env python3
"""Plot measured-vs-ideal latency/throughput deltas for VM run binaries."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


@dataclass(frozen=True)
class Scenario:
    name: str
    measured_throughput_rps: float
    measured_latency_ms: float
    ideal_throughput_rps: float
    ideal_latency_ms: float

    @property
    def throughput_delta_rps(self) -> float:
        return self.measured_throughput_rps - self.ideal_throughput_rps

    @property
    def latency_delta_ms(self) -> float:
        return self.measured_latency_ms - self.ideal_latency_ms


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one plot for measured-vs-ideal VM latency and throughput deltas."
    )
    parser.add_argument("--vm1", default="vm1.bin", help="Path to vm1 vegeta binary results file.")
    parser.add_argument("--vm2", default="vm2.bin", help="Path to vm2 vegeta binary results file.")
    parser.add_argument(
        "--vm1-only", default="vm1_only.bin", help="Path to single-machine vegeta binary results file."
    )
    parser.add_argument("--ideal-latency-ms", type=float, default=1000.0, help="Ideal latency in ms.")
    parser.add_argument(
        "--ideal-rps-combined",
        type=float,
        default=8000.0,
        help="Ideal throughput when vm1 and vm2 run together.",
    )
    parser.add_argument(
        "--output",
        default="out/plots/vm_measured_vs_ideal_deltas.png",
        help="Output PNG path.",
    )
    return parser.parse_args()


def read_report_from_bin(bin_path: Path) -> dict:
    cmd = ["./vegeta", "report", "-type=json", str(bin_path)]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def to_scenario(name: str, report: dict, ideal_rps: float, ideal_latency_ms: float) -> Scenario:
    mean_latency_ms = float(report.get("latencies", {}).get("mean", 0.0)) / 1_000_000.0
    throughput_rps = float(report.get("throughput", 0.0))
    return Scenario(
        name=name,
        measured_throughput_rps=throughput_rps,
        measured_latency_ms=mean_latency_ms,
        ideal_throughput_rps=ideal_rps,
        ideal_latency_ms=ideal_latency_ms,
    )


def main() -> None:
    args = parse_args()
    vm1_path = Path(args.vm1)
    vm2_path = Path(args.vm2)
    vm1_only_path = Path(args.vm1_only)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    vm1_report = read_report_from_bin(vm1_path)
    vm2_report = read_report_from_bin(vm2_path)
    vm1_only_report = read_report_from_bin(vm1_only_path)

    # vm1/vm2 were part of a combined 8000 RPS run, so each machine's ideal is half.
    per_vm_ideal_rps = float(args.ideal_rps_combined) / 2.0
    ideal_latency_ms = float(args.ideal_latency_ms)

    vm1 = to_scenario("vm1", vm1_report, per_vm_ideal_rps, ideal_latency_ms)
    vm2 = to_scenario("vm2", vm2_report, per_vm_ideal_rps, ideal_latency_ms)
    vm1_only = to_scenario("vm1_only", vm1_only_report, float(args.ideal_rps_combined), ideal_latency_ms)

    req1 = float(vm1_report.get("requests", 0.0))
    req2 = float(vm2_report.get("requests", 0.0))
    total_req = req1 + req2
    combined_latency_ms = 0.0
    if total_req > 0:
        combined_latency_ms = (vm1.measured_latency_ms * req1 + vm2.measured_latency_ms * req2) / total_req
    vm1_vm2 = Scenario(
        name="vm1+vm2",
        measured_throughput_rps=vm1.measured_throughput_rps + vm2.measured_throughput_rps,
        measured_latency_ms=combined_latency_ms,
        ideal_throughput_rps=float(args.ideal_rps_combined),
        ideal_latency_ms=ideal_latency_ms,
    )

    scenarios = [vm1, vm2, vm1_vm2, vm1_only]
    labels = [s.name for s in scenarios]
    x = list(range(len(scenarios)))

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    fig.suptitle("VM Measured vs Ideal Deltas", fontsize=13)

    axes[0].bar(x, [s.latency_delta_ms for s in scenarios], color="tab:blue", alpha=0.85)
    axes[0].axhline(0.0, color="black", linewidth=0.8, alpha=0.7)
    axes[0].set_ylabel("ms")
    axes[0].set_title("Latency Delta (measured mean - ideal)")
    axes[0].grid(axis="y", alpha=0.25)

    axes[1].bar(x, [s.throughput_delta_rps for s in scenarios], color="tab:orange", alpha=0.85)
    axes[1].axhline(0.0, color="black", linewidth=0.8, alpha=0.7)
    axes[1].set_ylabel("req/s")
    axes[1].set_title("Throughput Delta (measured - ideal)")
    axes[1].set_xticks(x, labels)
    axes[1].grid(axis="y", alpha=0.25)

    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
