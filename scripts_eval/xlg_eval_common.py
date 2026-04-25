#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance


METRIC_SOURCES = {
    "scheduler_delay": "scheduler_delay",
    "connection_delay": "conn_delay",
}
XLG_WINDOW_PREFIX = "XLG-WINDOW:"
XLG_PAYLOAD_COLUMNS = {
    "scheduler_delay": "scheduler_delays",
    "conn_delay": "connection_delays",
}
LABEL_ORDER = [
    "FEW_CONNECTIONS",
    "FEW_WORKERS",
    "CPU_CONTENTION",
    "SUT_DEGRADED",
    "SUT_FASTER",
    "NORMAL",
]
TERMINAL_LABELS = {"FEW_CONNECTIONS", "FEW_WORKERS", "CPU_CONTENTION"}
CPU_STRESS_JOBS = {"mild": 50, "mod": 100, "severe": 150}
TERMINAL_CONFIRMATION_WINDOWS = 3
WORKER_CAP_NEAR_RATIO = 0.95


def json_default(value: Any) -> Any:
    """Convert numpy and path values for JSON output."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a stable JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON object."""
    return json.loads(path.read_text())


def newest_match(root: Path, pattern: str) -> Path:
    """Return the last sorted file matching a run output pattern."""
    matches = sorted(root.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"missing {pattern} under {root}")
    return matches[-1]


def run_dirs(root: Path) -> list[Path]:
    """Find run_* directories in a flat case directory."""
    return sorted(path for path in root.glob("run_*") if path.is_dir())


def read_rate(stage_dir: Path) -> int:
    """Read an experiment RPS from run_config.env."""
    config = stage_dir / "run_config.env"
    for line in config.read_text().splitlines():
        key, _, value = line.partition("=")
        if key == "rate":
            return int(value)
    raise ValueError(f"missing rate in {config}")


def read_windows(path: Path) -> pd.DataFrame:
    """Load window summaries with parsed timestamps."""
    df = pd.read_csv(path)
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    numeric_cols = [
        "window_duration_ms",
        "total_latency_count",
        "avg_scheduler_delay_ms",
        "avg_conn_delay_ms",
        "avg_total_latency_ms",
        "avg_in_flight",
        "observed_R",
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    return df


def read_xlg_payloads(path: Path) -> pd.DataFrame:
    """Load XLG anomaly payloads emitted by Vegeta."""
    rows: list[dict[str, Any]] = []
    for line in path.open():
        if not line.startswith(XLG_WINDOW_PREFIX):
            raise ValueError(f"unexpected line in {path}: {line[:80].rstrip()}")

        payload = json.loads(line[len(XLG_WINDOW_PREFIX) :])
        rho = float(payload["rho"])
        if rho == -1:
            rho = float("nan")

        rows.append(
            {
                "window_start": pd.to_datetime(
                    payload["window_start"],
                    unit="ms",
                    utc=True,
                ),
                "rho": rho,
                "avg_in_flight": finite_number(payload.get("AvgInFlight")),
                "max_workers": finite_number(payload.get("MaxWorkers")),
                "scheduler_delays": finite_values(payload["SchedulerDelays"] or []),
                "connection_delays": finite_values(payload["ConnectionDelays"] or []),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "window_start",
                "rho",
                "avg_in_flight",
                "max_workers",
                "scheduler_delays",
                "connection_delays",
            ]
        )
    return pd.DataFrame(rows)


def trim_windows(df: pd.DataFrame, trim_s: float = 5.0) -> pd.DataFrame:
    """Drop startup windows and the final artifact-prone window."""
    if df.empty:
        return df.copy()
    ordered = df.sort_values(["window_start", "window_end"]).reset_index(drop=True)
    cutoff = ordered["window_start"].min() + pd.to_timedelta(trim_s, unit="s")
    trimmed = ordered[ordered["window_start"] >= cutoff].copy()
    if len(trimmed) > 0:
        trimmed = trimmed.iloc[:-1].copy()
    return trimmed.reset_index(drop=True)


def trim_payloads(df: pd.DataFrame, trim_s: float = 5.0) -> pd.DataFrame:
    """Drop startup payloads and the final artifact-prone payload."""
    if df.empty:
        return df.copy()
    ordered = df.sort_values("window_start").reset_index(drop=True)
    cutoff = ordered["window_start"].min() + pd.to_timedelta(trim_s, unit="s")
    trimmed = ordered[ordered["window_start"] >= cutoff].copy()
    if len(trimmed) > 0:
        trimmed = trimmed.iloc[:-1].copy()
    return trimmed.reset_index(drop=True)


def retained_windows(run_dir: Path, trim_s: float = 5.0) -> pd.DataFrame:
    """Load retained window rows for one run."""
    return trim_windows(read_windows(newest_match(run_dir, "window_results_rps*.csv")), trim_s=trim_s)


def retained_xlg_payloads(run_dir: Path, trim_s: float = 5.0) -> pd.DataFrame:
    """Load retained XLG anomaly payloads for one run."""
    return trim_payloads(read_xlg_payloads(newest_match(run_dir, "xlg_windows_rps*.log")), trim_s=trim_s)


def finite_values(values: Iterable[Any]) -> list[float]:
    """Return finite floats from an iterable."""
    out: list[float] = []
    for value in values:
        current = float(value)
        if math.isfinite(current):
            out.append(current)
    return out


def finite_number(value: Any) -> float:
    """Return a finite float, or NaN if the scalar is missing/invalid."""
    try:
        current = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return current if math.isfinite(current) else float("nan")


def is_worker_cap_near(avg_in_flight: Any, max_workers: Any) -> bool:
    """Use average in-flight requests as a lightweight proxy for worker pressure."""
    current = finite_number(avg_in_flight)
    cap = finite_number(max_workers)
    return math.isfinite(current) and math.isfinite(cap) and cap > 0 and current >= WORKER_CAP_NEAR_RATIO * cap


def quantile(values: Iterable[Any], q: float) -> float:
    """Compute a quantile while ignoring missing values."""
    vals = finite_values(values)
    if not vals:
        return float("nan")
    return float(np.quantile(vals, q))


def median(values: Iterable[Any]) -> float:
    """Compute a median while ignoring missing values."""
    vals = finite_values(values)
    if not vals:
        return float("nan")
    return float(np.median(vals))


def mean(values: Iterable[Any]) -> float:
    """Compute a mean while ignoring missing values."""
    vals = finite_values(values)
    if not vals:
        return float("nan")
    return float(np.mean(vals))


def round_count(value: float) -> int:
    """Round a capacity count and keep it usable."""
    return max(1, int(np.rint(float(value))))


def raw_emd(left: Iterable[Any], right: Iterable[Any]) -> float:
    """Compute raw Wasserstein distance in milliseconds."""
    left_vals = finite_values(left)
    right_vals = finite_values(right)
    if not left_vals or not right_vals:
        return float("nan")
    return float(wasserstein_distance(left_vals, right_vals))


def normalized_score(raw: float, normalizer: float) -> float:
    """Normalize a raw EMD score while preserving missing values."""
    return raw / normalizer if math.isfinite(raw) else float("nan")


def cheap_signal_quantiles(run_list: list[Path], trim_s: float = 5.0) -> dict[str, dict[str, float]]:
    """Compute healthy p95 thresholds for cheap delay signals."""

    # obtain values from the healthy runs and pool them together by metric (e.g. scheduler, connection delay)
    values_by_metric = {
        metric: pooled_metric_values(run_list, source, trim_s=trim_s)
        for metric, source in METRIC_SOURCES.items()
    }

    # obtain the p95 quantiles for each metric and return them in a dictionary
    quantiles: dict[str, dict[str, float]] = {}
    for metric, values in values_by_metric.items():
        if not values:
            raise ValueError(f"no healthy cheap-signal values for {metric}")
        quantiles[metric] = {
            "healthy_p95_ms": quantile(values, 0.95),
        }

    return quantiles


def cheap_signal_trigger(current_ms: float, quantiles: dict[str, float]) -> bool:
    """Return whether the current cheap signal is above the healthy p95 threshold."""
    healthy_p95_ms = float(quantiles["healthy_p95_ms"])
    return math.isfinite(current_ms) and math.isfinite(healthy_p95_ms) and current_ms > healthy_p95_ms


def should_compute_emd(
    rho: float,
    rho_center: float,
    epsilon: float,
    scheduler_quantile_trigger: bool,
    connection_quantile_trigger: bool,
    worker_cap_near: bool = False,
) -> tuple[bool, str]:
    """Decide whether rho or cheap quantile signals require scheduler EMD."""

    if worker_cap_near:
        return True, "worker_cap_near"

    # recompute emd if we are outside the healthy band
    if math.isfinite(rho) and (rho > rho_center + epsilon or rho < rho_center - epsilon):
        return True, "rho_outside_band"

    # compute emd if one of the triggers is activated
    if scheduler_quantile_trigger:
        return True, "scheduler_mean_gt_healthy_p95"
    if connection_quantile_trigger:
        return True, "connection_p25_gt_healthy_p95"

    return False, "cheap_signals_within_band"


def run_metric_values(run_dir: Path, metric: str, trim_s: float = 5.0) -> list[float]:
    """Load one metric distribution from one run."""
    values: list[float] = []
    payloads = retained_xlg_payloads(run_dir, trim_s=trim_s)
    for current in payloads[XLG_PAYLOAD_COLUMNS[metric]]:
        values.extend(current)
    return values


def pooled_metric_values(run_list: list[Path], metric: str, trim_s: float = 5.0) -> list[float]:
    """Pool one metric across many trimmed runs."""
    values: list[float] = []
    for run_dir in run_list:
        values.extend(run_metric_values(run_dir, metric, trim_s=trim_s))
    return values


def pooled_reference(run_list: list[Path], trim_s: float = 5.0) -> dict[str, list[float]]:
    """Build pooled healthy reference distributions."""
    return {
        metric: pooled_metric_values(run_list, source, trim_s=trim_s)
        for metric, source in METRIC_SOURCES.items()
    }


def leave_one_out_normalizers(
    run_list: list[Path],
    trim_s: float = 5.0,
) -> dict[str, float]:
    """Compute q95 healthy EMD normalizers from leave-one-out runs."""
    raw_by_metric: dict[str, list[float]] = {"scheduler_delay": []}
    for run_dir in run_list:
        others = [path for path in run_list if path != run_dir]
        current = run_metric_values(run_dir, "scheduler_delay", trim_s=trim_s)
        reference = pooled_metric_values(others, "scheduler_delay", trim_s=trim_s)
        raw_by_metric["scheduler_delay"].append(raw_emd(current, reference))
    
    normalizers: dict[str, float] = {}
    for metric, values in raw_by_metric.items():
        q95 = quantile(values, 0.95)
        normalizers[metric] = q95 if math.isfinite(q95) and q95 > 0 else 1.0
    return normalizers


def rho_values(run_list: list[Path], trim_s: float = 5.0) -> list[float]:
    """Collect trimmed rho windows across runs."""
    values: list[float] = []
    for run_dir in run_list:
        windows = retained_windows(run_dir, trim_s=trim_s)
        values.extend(finite_values(windows["observed_R"]))
    return values


@dataclass
class DiagnosisState:
    """Online diagnosis state retained across replayed windows."""

    # initializes in the normal regime with no previous rho and not terminal
    label: str = "NORMAL"
    previous_rho: float = float("nan")
    pending_terminal_label: str = ""
    pending_terminal_count: int = 0
    terminal: bool = False


def score_elevated(score: float, threshold: float) -> bool:
    """Return whether one normalized EMD score is above its threshold."""
    return math.isfinite(score) and score > threshold


def reset_terminal_confirmation(state: DiagnosisState) -> None:
    """Clear pending terminal evidence after a non-terminal window."""
    state.pending_terminal_label = ""
    state.pending_terminal_count = 0


def confirm_terminal_candidate(
    state: DiagnosisState,
    candidate_label: str,
    candidate_reason: str,
    fallback_label: str,
    reference_rho: float,
) -> tuple[str, bool, str, float]:
    """Latch a terminal label only after consecutive matching candidates."""
    if state.pending_terminal_label == candidate_label:
        # increase the terminal count
        state.pending_terminal_count += 1
    else:
        # reset the pending terminal label and count to start confirming the new candidate
        state.pending_terminal_label = candidate_label
        state.pending_terminal_count = 1

    # mark terminal once we have seen consecutive windows
    if state.pending_terminal_count >= TERMINAL_CONFIRMATION_WINDOWS:
        state.label = candidate_label
        state.terminal = True
        return state.label, True, candidate_reason, reference_rho

    # use a non-terminal fallback label until we have confirmed the candidate
    state.label = fallback_label
    reason = f"{candidate_reason}_pending_{state.pending_terminal_count}_of_{TERMINAL_CONFIRMATION_WINDOWS}"
    return state.label, False, reason, reference_rho


def transition_window(
    state: DiagnosisState,
    rho: float,
    scheduler_score: float,
    scheduler_quantile_trigger: bool,
    connection_quantile_trigger: bool,
    emd_reason: str,
    thresholds: dict[str, float],
    rho_center: float,
    epsilon: float,
    worker_cap_near: bool = False,
) -> tuple[str, bool, str, float]:
    """Advance the online diagnosis state using only the current window.

    The replay starts from NORMAL, uses the Stage A rho center for the baseline
    band, and keeps the previous rho to decide whether an already-shifted regime
    moved far enough to re-check terminal evidence. Terminal diagnoses latch and
    stop the evaluation logically.
    """

    # get the previous rho if there is any
    previous_rho = state.previous_rho

    # handles the edge case of the initial window having an invalid rho by treating the center as the reference until a valid rho is observed
    reference_rho = previous_rho if math.isfinite(previous_rho) else rho_center

    # if the state is already terminal, it remains in that state regardless of the current window's rho or scores
    if state.terminal:
        return state.label, True, "terminal_latched", reference_rho

    # the rho value is invalid
    if not math.isfinite(rho):
        return state.label, False, "invalid_rho", reference_rho

    # obtain the thresholds
    worker_threshold = thresholds["T_worker"]
    cpu_threshold = thresholds["T_cpu"] # for now, worker and cpu share the same threshold

    # determine whether the current window's scheduler score is elevated compared to the thresholds
    worker_sched_elevated = score_elevated(scheduler_score, worker_threshold)
    cpu_sched_elevated = score_elevated(scheduler_score, cpu_threshold)

    # compare rho to the healthy center band defined by rho_center +/- epsilon
    rho_high = rho > rho_center + epsilon
    rho_low = rho < rho_center - epsilon

    # compare rho to previous values of rho
    state.previous_rho = rho

    # since we are near the worker cap and scheduler is elevated, we check for FEW_WORKERS
    if worker_cap_near and worker_sched_elevated and scheduler_quantile_trigger:
        if rho_high:
            fallback_label = "SUT_DEGRADED"
        elif rho_low:
            fallback_label = "SUT_FASTER"
        else:
            fallback_label = "NORMAL"
        return confirm_terminal_candidate(
            state,
            "FEW_WORKERS",
            "worker_cap_scheduler_delay",
            fallback_label,
            reference_rho,
        )

    if rho_high:
        # we check for CPU_CONTENTION or FEW_CONNECTIONS
        if connection_quantile_trigger:
            return confirm_terminal_candidate(
                state,
                "FEW_CONNECTIONS",
                "rho_high_connection_delay",
                "SUT_DEGRADED",
                reference_rho,
            )
        if cpu_sched_elevated and scheduler_quantile_trigger:
            return confirm_terminal_candidate(
                state,
                "CPU_CONTENTION",
                "rho_high_scheduler_delay",
                "SUT_DEGRADED",
                reference_rho,
            )

        # no terminal conditions, but SUT is degraded if rho is high, reset terminal label
        reset_terminal_confirmation(state)
        state.label = "SUT_DEGRADED"
        reason = "rho_high"
        return state.label, False, reason, reference_rho

    if rho_low:
        # SUT is faster if rho is low but no terminal conditions are met; worker bottleneck
        # checked earlier w/ worker cap being nearly saturated
        reason = "hold_sut_faster" if state.label == "SUT_FASTER" else "rho_low"
        reset_terminal_confirmation(state)
        state.label = "SUT_FASTER"
        return state.label, False, reason, reference_rho

    # if rho is within the healthy band, we return NORMAL
    reset_terminal_confirmation(state)
    state.label = "NORMAL"
    return state.label, False, "rho_within_band", reference_rho


WINDOW_SCORE_COLUMNS = [
    "rho",
    "scheduler_score",
    "connection_score",
    "scheduler_mean_ms",
    "connection_mean_ms",
    "scheduler_median_ms",
    "connection_p25_ms",
    "scheduler_mean_gt_healthy_p95",
    "connection_p25_gt_healthy_p95",
    "worker_cap_near",
    "emd_computed",
    "emd_reason",
]


def window_scores(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    cheap_quantiles: dict[str, dict[str, float]] | None = None,
    rho_center: float = float("nan"),
    epsilon: float = float("nan"),
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Score retained XLG anomaly payloads for one run."""
    payloads = retained_xlg_payloads(run_dir, trim_s=trim_s)

    rows: list[dict[str, Any]] = []
    scheduler_normalizer = normalizers["scheduler_delay"]

    for _, row in payloads.reset_index(drop=True).iterrows():
        scheduler_mean, connection_mean = mean(row["scheduler_delays"]), mean(row["connection_delays"])
        scheduler_median = median(row["scheduler_delays"])
        connection_p25 = quantile(row["connection_delays"], 0.25)
        worker_cap_near = is_worker_cap_near(row["avg_in_flight"], row["max_workers"])

        if cheap_quantiles is None:
            scheduler_quantile_trigger = False
            connection_quantile_trigger = False
            emd_computed = True
            emd_reason = "cheap_quantiles_unavailable"
        else:
            # comparing the scheduler_mean with healthy p95 for scheduler delay
            scheduler_quantile_trigger = cheap_signal_trigger(
                scheduler_mean,
                cheap_quantiles["scheduler_delay"],
            )

            # comparing the connection_p25 with healthy p95 for connection delay
            connection_quantile_trigger = cheap_signal_trigger(
                connection_p25,
                cheap_quantiles["connection_delay"],
            )

            # we decide whether to compute EMD
            emd_computed, emd_reason = should_compute_emd(
                rho=float(row["rho"]),
                rho_center=rho_center,
                epsilon=epsilon,
                scheduler_quantile_trigger=scheduler_quantile_trigger,
                connection_quantile_trigger=connection_quantile_trigger,
                worker_cap_near=worker_cap_near,
            )

        if emd_computed:
            # compute the scheduler EMD against the pooled healthy reference and normalize it
            sched_raw = raw_emd(row["scheduler_delays"], reference["scheduler_delay"])
            scheduler_score = normalized_score(sched_raw, scheduler_normalizer)
        else:
            scheduler_score = 0.0

        # Connection EMD is no longer used for diagnosis; keep the column for compatibility.
        connection_score = float("nan")

        rows.append(
            {
                "rho": float(row["rho"]),
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
        )
    return pd.DataFrame(rows, columns=WINDOW_SCORE_COLUMNS)


def window_predictions(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    thresholds: dict[str, float],
    rho_center: float,
    epsilon: float,
    cheap_quantiles: dict[str, dict[str, float]] | None = None,
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Replay retained windows through the online diagnosis state machine."""

    # outputs a dataframe for each window consisting of rho, scheduler_score,
    # and the cheap connection trigger along with some other metrics
    scores = window_scores(
        run_dir=run_dir,
        reference=reference,
        normalizers=normalizers,
        cheap_quantiles=cheap_quantiles,
        rho_center=rho_center,
        epsilon=epsilon,
        trim_s=trim_s,
    )

    labels: list[str] = []
    terminals: list[bool] = []
    reasons: list[str] = []
    reference_rhos: list[float] = []
    state = DiagnosisState()    # initializing the state

    # we are simulating the transition behavior
    for _, row in scores.iterrows():
        # calling the transition_window on the row
        label, terminal, reason, reference_rho = transition_window(
            state=state,
            rho=float(row["rho"]),
            scheduler_score=float(row["scheduler_score"]),
            scheduler_quantile_trigger=bool(row["scheduler_mean_gt_healthy_p95"]),
            connection_quantile_trigger=bool(row["connection_p25_gt_healthy_p95"]),
            worker_cap_near=bool(row["worker_cap_near"]),
            emd_reason=str(row["emd_reason"]),
            thresholds=thresholds,
            rho_center=rho_center,
            epsilon=epsilon,
        )

        # add the labels, terminals, reasons, and reference rhos to the scores dataframe
        labels.append(label)
        terminals.append(terminal)
        reasons.append(reason)
        reference_rhos.append(reference_rho)

    # saving the predictions, terminals, reasons, and reference rhos in the scores dataframe
    scores["window_prediction"] = labels
    scores["terminal"] = terminals
    scores["transition_reason"] = reasons
    scores["rho_reference"] = reference_rhos
    return scores


def mode_label(labels: Iterable[str]) -> str:
    """Return the modal label with stable label-order tie breaking."""
    labels = list(labels)
    if not labels:
        return "UNKNOWN"
    counts = Counter(labels)
    best_count = max(counts.values())
    for label in LABEL_ORDER:
        if counts.get(label, 0) == best_count:
            return label
    return counts.most_common(1)[0][0]


def run_prediction_from_windows(df: pd.DataFrame) -> tuple[str, bool]:
    """Return the online run-level state after replaying retained windows."""
    if df.empty:
        return "UNKNOWN", False
    
    # if there are terminal windows, return the first one; otherwise, return the last window's prediction and whether it's terminal
    terminal_rows = df[df["terminal"]]
    if not terminal_rows.empty:
        label = str(terminal_rows.iloc[0]["window_prediction"])
        return label, True
    label = str(df.iloc[-1]["window_prediction"])
    return label, label in TERMINAL_LABELS
