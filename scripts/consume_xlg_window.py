#!/usr/bin/env python3
import json
import math
import queue
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import pandas as pd

# User-defined functions
from utils import normalized_emd, trim_window_margins

# prefix we are filtering for in the incoming telemetry stream
WINDOW_PREFIX = "XLG-WINDOW:"
QUEUE_MAXSIZE = 100

# a bounded capacity queue to hold incoming windows until the consumer can process them
payload_queue: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)

# the metrics we are after
METRIC_CONFIG = {
    "pacer_emd": ("pacer_wait", "PacerDelays"),
    "scheduler_emd": ("scheduler_delay", "SchedulerDelays"),
    "connection_emd": ("conn_delay", "ConnectionDelays"),
}
TRIM_START_WINDOWS = 1
TRIM_END_WINDOWS = 1
SKIP_INITIAL_WINDOWS = 2


# defining the states of the state machine
STATE_NORMAL = 0
STATE_LG_SUCCESS_SUT_DEGRADED = 1
STATE_LG_SUCCESS_SUT_FASTER = 2
STATE_FAILED_FEW_WORKERS = 3
STATE_FAILED_FEW_CONNECTIONS = 4

STATE_LABELS = {
    STATE_NORMAL: "NORMAL",
    STATE_LG_SUCCESS_SUT_DEGRADED: "LG_SUCCESS_SUT_DEGRADED",
    STATE_LG_SUCCESS_SUT_FASTER: "LG_SUCCESS_SUT_FASTER",
    STATE_FAILED_FEW_WORKERS: "FAILED_FEW_WORKERS",
    STATE_FAILED_FEW_CONNECTIONS: "FAILED_FEW_CONNECTIONS",
}

# Constants (Thresholds)
DEFAULT_EPSILON = 0.05
DEFAULT_BAND = 0.15
DEFAULT_SCHED_MAX = 2.0
DEFAULT_CONN_MAX = 5.0


@dataclass
class AnalyzerState:
    # `anchor` tracks the rho value that defines the current regime's
    # suppression band.
    current_state: int = STATE_NORMAL
    anchor: float = 1.0


@dataclass
class TransitionResult:
    previous_state: int
    current_state: int
    anchor: float
    reason: str
    emd_suppressed: bool
    terminal: bool


def is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def is_healthy_emd(emd_sched: float, emd_conn: float, sched_max: float, conn_max: float) -> bool:
    return (
        is_finite_number(emd_sched)
        and is_finite_number(emd_conn)
        and float(emd_sched) < sched_max
        and float(emd_conn) < conn_max
    )


def transition_state(
    analysis_state: AnalyzerState,
    current: float,
    emd_sched: float,
    emd_conn: float,
    epsilon: float,
    sched_max: float,
    conn_max: float,
) -> TransitionResult:
    """Handling the state transitions"""
    previous_state = analysis_state.current_state

    # Terminal LG failures take priority over any regime classification.
    if previous_state in (STATE_NORMAL, STATE_LG_SUCCESS_SUT_DEGRADED, STATE_LG_SUCCESS_SUT_FASTER):
        if is_finite_number(emd_sched) and float(emd_sched) >= sched_max:
            analysis_state.current_state = STATE_FAILED_FEW_WORKERS
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="terminal_sched_max",
                emd_suppressed=False,
                terminal=True,
            )

        if is_finite_number(emd_conn) and float(emd_conn) >= conn_max:
            analysis_state.current_state = STATE_FAILED_FEW_CONNECTIONS
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="terminal_conn_max",
                emd_suppressed=False,
                terminal=True,
            )

    emd_is_healthy = is_healthy_emd(emd_sched, emd_conn, sched_max, conn_max)
    lower_baseline = 1.0 - epsilon
    upper_baseline = 1.0 + epsilon

    # State 0 only exits when rho leaves the calibration band and LG internals
    # still look healthy.
    if previous_state == STATE_NORMAL and emd_is_healthy:
        if current > upper_baseline:
            analysis_state.current_state = STATE_LG_SUCCESS_SUT_DEGRADED
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="enter_degraded",
                emd_suppressed=False,
                terminal=False,
            )
        if current < lower_baseline:
            analysis_state.current_state = STATE_LG_SUCCESS_SUT_FASTER
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="enter_faster",
                emd_suppressed=False,
                terminal=False,
            )

    if previous_state in (STATE_LG_SUCCESS_SUT_DEGRADED, STATE_LG_SUCCESS_SUT_FASTER) and emd_is_healthy:
        if lower_baseline <= current <= upper_baseline:
            analysis_state.current_state = STATE_NORMAL
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="recover_normal",
                emd_suppressed=False,
                terminal=False,
            )

        if previous_state == STATE_LG_SUCCESS_SUT_DEGRADED and current < lower_baseline:
            analysis_state.current_state = STATE_LG_SUCCESS_SUT_FASTER
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="swap_to_faster",
                emd_suppressed=False,
                terminal=False,
            )

        if previous_state == STATE_LG_SUCCESS_SUT_FASTER and current > upper_baseline:
            analysis_state.current_state = STATE_LG_SUCCESS_SUT_DEGRADED
            analysis_state.anchor = current
            return TransitionResult(
                previous_state=previous_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="swap_to_degraded",
                emd_suppressed=False,
                terminal=False,
            )

        # Intra-state drift: the regime name did not change, but rho moved far
        # enough to establish a new "normal" anchor for suppression.
        analysis_state.anchor = current
        return TransitionResult(
            previous_state=previous_state,
            current_state=analysis_state.current_state,
            anchor=analysis_state.anchor,
            reason="reanchor_same_state",
            emd_suppressed=False,
            terminal=False,
        )

    return TransitionResult(
        previous_state=previous_state,
        current_state=analysis_state.current_state,
        anchor=analysis_state.anchor,
        reason="no_transition",
        emd_suppressed=False,
        terminal=analysis_state.current_state in (
            STATE_FAILED_FEW_WORKERS,
            STATE_FAILED_FEW_CONNECTIONS,
        ),
    )

def load_baseline_samples(samples_path: Path) -> dict[str, list[float]]:
    """
    Load baseline samples once so each incoming window can be compared against
    the same reference distribution.

    We trim the first and last baseline windows to reduce startup/shutdown
    artifacts, matching the approach used in attribution.py.
    """
    samples_df = pd.read_csv(samples_path)
    samples_df["value_ms"] = pd.to_numeric(samples_df["value_ms"], errors="coerce")
    samples_df["window_start"] = pd.to_datetime(samples_df["window_start"], errors="coerce")
    samples_df["window_end"] = pd.to_datetime(samples_df["window_end"], errors="coerce")

    window_results = (
        samples_df[["window_start", "window_end"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["window_start", "window_end"])
        .reset_index(drop=True)
    )
    trimmed_results = trim_window_margins(
        window_results,
        start_windows=TRIM_START_WINDOWS,
        end_windows=TRIM_END_WINDOWS,
    )
    trimmed_keys = trimmed_results[["window_start", "window_end"]].drop_duplicates()
    trimmed_samples = trimmed_keys.merge(
        samples_df,
        on=["window_start", "window_end"],
        how="left",
    )

    baseline_samples: dict[str, list[float]] = {}
    for metric_name, _ in METRIC_CONFIG.values():
        metric_values = (
            trimmed_samples[trimmed_samples["metric_name"] == metric_name]["value_ms"]
            .dropna()
            .astype(float)
            .tolist()
        )
        baseline_samples[metric_name] = metric_values

    return baseline_samples


def stdin_producer() -> None:
    """
    Read telemetry from stdin and keep only the newest windows if the consumer
    falls behind.
    """
    while True:

        # read the line
        line = sys.stdin.readline()
        if line == "":
            break

        if not line.startswith(WINDOW_PREFIX):
            continue

        raw_payload = line[len(WINDOW_PREFIX) :].strip()
        if not raw_payload:
            continue

        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue

        if not isinstance(payload, dict):
            continue

        # if the queue is full, drop the oldest payload to make room for the new one
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


def compute_window_emd(
    payload: dict[str, Any], baseline_samples: dict[str, list[float]]
) -> dict[str, float]:
    """
    Compute normalized EMD for the delay distributions carried by one window.
    """
    metrics: dict[str, float] = {}
    for output_name, (baseline_metric, payload_key) in METRIC_CONFIG.items():
        current_values = payload.get(payload_key) or []
        metrics[output_name] = normalized_emd(
            baseline_samples.get(baseline_metric, []),
            current_values,
        )
    return metrics


def emit_window_result(
    current: float,
    pacer_emd: Optional[float],
    scheduler_emd: Optional[float],
    connection_emd: Optional[float],
    transition: TransitionResult,
) -> None:
    def format_metric(value: Optional[float]) -> str:
        if value is None or not is_finite_number(value):
            return "nan"
        return f"{float(value):.6f}"

    print(
        "rho={rho:.6f} state={state} prev_state={prev_state} anchor={anchor:.6f} "
        "reason={reason} emd_suppressed={emd_suppressed} terminal={terminal} "
        "pacer_emd={pacer_emd} scheduler_emd={scheduler_emd} connection_emd={connection_emd}".format(
            rho=float(current),
            state=STATE_LABELS[transition.current_state],
            prev_state=STATE_LABELS[transition.previous_state],
            anchor=float(transition.anchor),
            reason=transition.reason,
            emd_suppressed=str(transition.emd_suppressed).lower(),
            terminal=str(transition.terminal).lower(),
            pacer_emd=format_metric(pacer_emd),
            scheduler_emd=format_metric(scheduler_emd),
            connection_emd=format_metric(connection_emd),
        )
    )
    sys.stdout.flush()


def main() -> None:
    if len(sys.argv) != 2:
        print(
            "usage: python3 scripts/consume_xlg_window.py <baseline_samples_csv>",
            file=sys.stderr,
        )
        sys.exit(1)

    baseline_samples = load_baseline_samples(Path(sys.argv[1]))
    analysis_state = AnalyzerState()
    seen_windows = 0

    producer = threading.Thread(target=stdin_producer, daemon=True)
    producer.start()

    while True:
        payload = payload_queue.get()

        rho = payload.get("rho", -1)
        if rho == -1:
            continue
        if not is_finite_number(rho):
            continue

        seen_windows += 1
        if seen_windows <= SKIP_INITIAL_WINDOWS:
            continue

        current = float(rho)

        # Once terminal, stop consuming further windows for this process.
        if analysis_state.current_state in (
            STATE_FAILED_FEW_WORKERS,
            STATE_FAILED_FEW_CONNECTIONS,
        ):
            break

        # Suppress EMD while rho stays inside the anomaly regime's anchor band
        # to avoid extra CPU work during stable periods.
        if analysis_state.current_state in (
            STATE_LG_SUCCESS_SUT_DEGRADED,
            STATE_LG_SUCCESS_SUT_FASTER,
        ) and (analysis_state.anchor - DEFAULT_BAND) <= current <= (analysis_state.anchor + DEFAULT_BAND):
            transition = TransitionResult(
                previous_state=analysis_state.current_state,
                current_state=analysis_state.current_state,
                anchor=analysis_state.anchor,
                reason="suppressed_within_band",
                emd_suppressed=True,
                terminal=False,
            )
            emit_window_result(
                current=current,
                pacer_emd=None,
                scheduler_emd=None,
                connection_emd=None,
                transition=transition,
            )
            continue

        emd_metrics = compute_window_emd(payload, baseline_samples)
        transition = transition_state(
            analysis_state=analysis_state,
            current=current,
            emd_sched=float(emd_metrics["scheduler_emd"]),
            emd_conn=float(emd_metrics["connection_emd"]),
            epsilon=DEFAULT_EPSILON,
            sched_max=DEFAULT_SCHED_MAX,
            conn_max=DEFAULT_CONN_MAX,
        )
        emit_window_result(
            current=current,
            pacer_emd=float(emd_metrics["pacer_emd"]),
            scheduler_emd=float(emd_metrics["scheduler_emd"]),
            connection_emd=float(emd_metrics["connection_emd"]),
            transition=transition,
        )

        if transition.terminal:
            break


if __name__ == "__main__":
    main()
