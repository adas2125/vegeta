#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import queue
import sys
import threading
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts_eval"))

from xlg_eval_common import (
    DiagnosisState,
    cheap_signal_trigger,
    is_worker_cap_near,
    mean,
    median,
    normalized_score,
    pooled_reference,
    quantile,
    raw_emd,
    read_json,
    run_dirs,
    should_compute_emd,
    transition_window,
)


WINDOW_PREFIX = "XLG-WINDOW:"
QUEUE_MAXSIZE = 100

payload_queue: "queue.Queue[dict[str, Any] | None]" = queue.Queue(maxsize=QUEUE_MAXSIZE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume live XLG windows with Stage B logic.")
    parser.add_argument("--stage-a-thresholds", type=Path, required=True)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def stage_a_dir_from_thresholds(thresholds_path: Path, payload: dict[str, Any]) -> Path:
    raw = payload["stage_a_dir"]
    stage_a_dir = Path(str(raw))
    if stage_a_dir.exists():
        return stage_a_dir

    candidate = thresholds_path.parent
    if (candidate / "healthy").exists():
        return candidate


def finite_float(value: Any) -> float:
    try:
        current = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return current if math.isfinite(current) else float("nan")


def score_payload(
    payload: dict[str, Any],
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    cheap_quantiles: dict[str, dict[str, float]],
    rho_center: float,
    epsilon: float,
) -> dict[str, Any]:
    rho = finite_float(payload.get("rho", float("nan")))
    if rho == -1:
        rho = float("nan")

    scheduler_delays = payload.get("SchedulerDelays") or []
    connection_delays = payload.get("ConnectionDelays") or []

    scheduler_mean = mean(scheduler_delays)
    connection_mean = mean(connection_delays)
    scheduler_median = median(scheduler_delays)
    connection_p25 = quantile(connection_delays, 0.25)
    worker_cap_near = is_worker_cap_near(
        payload.get("AvgInFlight"),
        payload.get("MaxWorkers"),
    )

    # we use the cheap signals to determine whether to compute the EMD-based scores for this window, which are more expensive to compute
    scheduler_quantile_trigger = cheap_signal_trigger(
        scheduler_median,
        cheap_quantiles["scheduler_delay"],
    )
    connection_quantile_trigger = cheap_signal_trigger(
        connection_p25,
        cheap_quantiles["connection_delay"],
    )
    emd_computed, emd_reason = should_compute_emd(
        rho=rho,
        rho_center=rho_center,
        epsilon=epsilon,
        scheduler_quantile_trigger=scheduler_quantile_trigger,
        connection_quantile_trigger=connection_quantile_trigger,
        worker_cap_near=worker_cap_near,
    )

    if emd_computed:
        scheduler_score = normalized_score(
            raw_emd(scheduler_delays, reference["scheduler_delay"]),
            normalizers["scheduler_delay"],
        )
    else:
        scheduler_score = 0.0

    connection_score = float("nan")

    return {
        "rho": rho,
        "scheduler_score": scheduler_score,
        "connection_score": connection_score,
        "scheduler_mean_ms": scheduler_mean,
        "connection_mean_ms": connection_mean,
        "scheduler_median_ms": scheduler_median,
        "connection_p25_ms": connection_p25,
        "scheduler_mean_gt_healthy_p95": scheduler_quantile_trigger,
        "connection_p25_gt_healthy_p95": connection_quantile_trigger,
        "worker_cap_near": worker_cap_near,
        "emd_computed": emd_computed,
        "emd_reason": emd_reason,
    }


def format_metric(value: Any) -> str:
    try:
        current = float(value)
    except (TypeError, ValueError):
        return "nan"
    if not math.isfinite(current):
        return "nan"
    return f"{current:.6f}"


def emit_window_result(
    window_idx: int,
    elapsed_s: float,
    score: dict[str, Any],
    label: str,
    terminal: bool,
    reason: str,
) -> None:
    print(
        "window={window} elapsed_s={elapsed} rho={rho} label={label} terminal={terminal} "
        "reason={reason} scheduler_score={scheduler_score} connection_score={connection_score} "
        "emd_computed={emd_computed} emd_reason={emd_reason} "
        "scheduler_trigger={scheduler_trigger} connection_trigger={connection_trigger}".format(
            window=window_idx,
            elapsed=format_metric(elapsed_s),
            rho=format_metric(score["rho"]),
            label=label,
            terminal=str(terminal).lower(),
            reason=reason,
            scheduler_score=format_metric(score["scheduler_score"]),
            connection_score=format_metric(score["connection_score"]),
            emd_computed=str(bool(score["emd_computed"])).lower(),
            emd_reason=score["emd_reason"],
            scheduler_trigger=str(bool(score["scheduler_mean_gt_healthy_p95"])).lower(),
            connection_trigger=str(bool(score["connection_p25_gt_healthy_p95"])).lower(),
        )
    )
    sys.stdout.flush()


def stdin_producer() -> None:
    """Producer thread that reads lines from stdin, extracts XLG window payloads, and enqueues them for processing."""
    def enqueue(payload: dict[str, Any] | None) -> None:
        """adds the payload to the queue, evicting old entries if the queue is full"""
        try:
            payload_queue.put_nowait(payload)
        except queue.Full:
            try:
                payload_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                payload_queue.put_nowait(payload)
            except queue.Full:
                pass

    for line in sys.stdin:
        # ignoring packets that don't start with the expected prefix
        if not line.startswith(WINDOW_PREFIX):
            continue

        raw_payload = line[len(WINDOW_PREFIX) :].strip()
        if not raw_payload:
            continue

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            enqueue(payload)

    enqueue(None)


def main() -> None:
    args = parse_args()
    stage_a_thresholds = read_json(args.stage_a_thresholds)
    stage_a_dir = stage_a_dir_from_thresholds(args.stage_a_thresholds, stage_a_thresholds)

    # obtain the healthy runs
    healthy_runs = run_dirs(stage_a_dir / "healthy")

    # obtain the reference distribution and thresholds
    reference = pooled_reference(healthy_runs, trim_s=args.trim_s)
    thresholds = stage_a_thresholds["thresholds"]
    normalizers = stage_a_thresholds["normalizers"]
    rho_center = float(stage_a_thresholds["rho_center_fixed"])
    epsilon = float(stage_a_thresholds["epsilon_fixed"])
    cheap_quantiles = stage_a_thresholds["cheap_signal_quantiles"]
    state = DiagnosisState()

    producer = threading.Thread(target=stdin_producer, daemon=True)
    producer.start()

    first_window_start_ms: float | None = None
    window_idx = 0
    pending_payload: dict[str, Any] | None = None

    while True:
        # obtain the payload
        payload = payload_queue.get()
        if payload is None:
            break

        # Keep one payload buffered so EOF drops the final artifact-prone window,
        # matching scripts_eval.xlg_eval_common.trim_payloads.
        if pending_payload is None:
            pending_payload = payload
            continue

        current_payload = pending_payload
        pending_payload = payload

        window_start_ms = finite_float(current_payload.get("window_start", 0.0))
        if first_window_start_ms is None:
            first_window_start_ms = window_start_ms
        elapsed_s = (window_start_ms - first_window_start_ms) / 1000.0

        # we ignore the first payloads up to trim
        if elapsed_s < args.trim_s:
            continue

        window_idx += 1

        # computes EMD-based scores and other metrics for this window based on the payload and the reference distribution
        score = score_payload(
            payload=current_payload,
            reference=reference,
            normalizers=normalizers,
            cheap_quantiles=cheap_quantiles,
            rho_center=rho_center,
            epsilon=epsilon,
        )

        # apply the Stage B logic to obtain a diagnosis for this window
        label, terminal, reason, _ = transition_window(
            state=state,
            rho=float(score["rho"]),
            scheduler_score=float(score["scheduler_score"]),
            scheduler_quantile_trigger=bool(score["scheduler_mean_gt_healthy_p95"]),
            connection_quantile_trigger=bool(score["connection_p25_gt_healthy_p95"]),
            worker_cap_near=bool(score["worker_cap_near"]),
            emd_reason=str(score["emd_reason"]),
            thresholds=thresholds,
            rho_center=rho_center,
            epsilon=epsilon,
        )

        # just a print statement
        emit_window_result(
            window_idx=window_idx,
            elapsed_s=elapsed_s,
            score=score,
            label=label,
            terminal=terminal,
            reason=reason,
        )


if __name__ == "__main__":
    main()
