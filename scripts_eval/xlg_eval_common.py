#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
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
XLG_PAYLOAD_METRICS = {
    "scheduler_delay": "SchedulerDelays",
    "conn_delay": "ConnectionDelays",
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
SEVERITY_FACTORS = {"mild": 0.80, "mod": 0.65, "severe": 0.50}
CONNECTION_SEVERITY_FACTORS = {"mild": 0.90, "mod": 0.85, "severe": 0.80}
CPU_STRESS_JOBS = {"mild": 25, "mod": 50, "severe": 75}


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


def read_rate(stage_dir: Path, fallback_runs_subdir: str) -> int:
    """Read an experiment RPS from run_config.env or output filenames."""
    config = stage_dir / "run_config.env"
    if config.exists():
        for line in config.read_text().splitlines():
            key, _, value = line.partition("=")
            if key == "rate":
                return int(value)

    matches = sorted((stage_dir / fallback_runs_subdir).glob("run_*/window_results_rps*.csv"))
    if not matches:
        raise FileNotFoundError(f"could not infer rate under {stage_dir / fallback_runs_subdir}")

    match = re.search(r"rps(\d+)", matches[-1].name)
    if not match:
        raise ValueError(f"could not infer rate from {matches[-1]}")
    return int(match.group(1))


def read_windows(path: Path) -> pd.DataFrame:
    """Load window summaries with parsed timestamps."""
    df = pd.read_csv(path)
    df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce", utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce", utc=True)
    numeric_cols = [
        "window_duration_ms",
        "total_latency_count",
        "avg_scheduler_delay_ms",
        "avg_conn_delay_ms",
        "avg_total_latency_ms",
        "avg_in_flight",
        "observed_R",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["window_start", "window_end"])


def read_samples(path: Path) -> pd.DataFrame:
    """Load per-window distribution samples."""
    df = pd.read_csv(path)
    df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce", utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce", utc=True)
    df["value_ms"] = pd.to_numeric(df["value_ms"], errors="coerce")
    return df.dropna(subset=["window_start", "window_end", "metric_name", "value_ms"])


def read_xlg_payloads(path: Path) -> pd.DataFrame:
    """Load XLG anomaly payloads emitted by Vegeta."""
    rows: list[dict[str, Any]] = []
    for line in path.open():
        if not line.startswith(XLG_WINDOW_PREFIX):
            continue
        raw_payload = line[len(XLG_WINDOW_PREFIX) :].strip()
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue

        try:
            rho = float(payload.get("rho", float("nan")))
        except (TypeError, ValueError):
            rho = float("nan")
        if rho == -1:
            rho = float("nan")

        rows.append(
            {
                "window_start": pd.to_datetime(
                    payload.get("window_start"),
                    unit="ms",
                    errors="coerce",
                    utc=True,
                ),
                "rho": rho,
                "scheduler_delays": finite_values(payload.get("SchedulerDelays") or []),
                "connection_delays": finite_values(payload.get("ConnectionDelays") or []),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=["window_start", "rho", "scheduler_delays", "connection_delays"]
        )
    return pd.DataFrame(rows).dropna(subset=["window_start"])


def trim_windows(df: pd.DataFrame, trim_s: float = 5.0, drop_last: bool = True) -> pd.DataFrame:
    """Drop startup windows and the final artifact-prone window."""
    if df.empty:
        return df.copy()
    ordered = df.sort_values(["window_start", "window_end"]).reset_index(drop=True)
    cutoff = ordered["window_start"].min() + pd.to_timedelta(trim_s, unit="s")
    trimmed = ordered[ordered["window_start"] >= cutoff].copy()
    if drop_last and len(trimmed) > 0:
        trimmed = trimmed.iloc[:-1].copy()
    return trimmed.reset_index(drop=True)


def trim_payloads(df: pd.DataFrame, trim_s: float = 5.0, drop_last: bool = True) -> pd.DataFrame:
    """Drop startup payloads and the final artifact-prone payload."""
    if df.empty:
        return df.copy()
    ordered = df.sort_values("window_start").reset_index(drop=True)
    cutoff = ordered["window_start"].min() + pd.to_timedelta(trim_s, unit="s")
    trimmed = ordered[ordered["window_start"] >= cutoff].copy()
    if drop_last and len(trimmed) > 0:
        trimmed = trimmed.iloc[:-1].copy()
    return trimmed.reset_index(drop=True)


def trim_samples_for_windows(samples: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    """Keep sample rows that belong to retained windows."""
    keys = windows[["window_start", "window_end"]].drop_duplicates()
    if keys.empty:
        return samples.iloc[0:0].copy()
    return keys.merge(samples, on=["window_start", "window_end"], how="left")


def retained_windows(run_dir: Path, trim_s: float = 5.0) -> pd.DataFrame:
    """Load retained window rows for one run."""
    return trim_windows(read_windows(newest_match(run_dir, "window_results_rps*.csv")), trim_s=trim_s)


def retained_samples(run_dir: Path, windows: pd.DataFrame) -> pd.DataFrame:
    """Load samples belonging to retained windows."""
    samples = read_samples(newest_match(run_dir, "window_samples_rps*.csv"))
    return trim_samples_for_windows(samples, windows)


def retained_xlg_payloads(run_dir: Path, trim_s: float = 5.0) -> pd.DataFrame:
    """Load retained XLG anomaly payloads for one run."""
    return trim_payloads(read_xlg_payloads(newest_match(run_dir, "xlg_windows_rps*.log")), trim_s=trim_s)


def finite_values(values: Iterable[Any]) -> list[float]:
    """Return finite floats from an iterable."""
    out: list[float] = []
    for value in values:
        try:
            current = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(current):
            out.append(current)
    return out


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


def round_count(value: float) -> int:
    """Round a capacity count and keep it usable."""
    if not math.isfinite(float(value)):
        return 1
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
    return raw / (normalizer or 1.0) if math.isfinite(raw) else float("nan")


def metric_values(samples: pd.DataFrame, metric: str) -> list[float]:
    """Extract one sample distribution from a samples dataframe."""
    return finite_values(samples[samples["metric_name"] == metric]["value_ms"])


def run_metric_values(run_dir: Path, metric: str, trim_s: float = 5.0) -> list[float]:
    """Load one metric distribution from one run."""
    payload_key = XLG_PAYLOAD_METRICS.get(metric)
    if payload_key is not None:
        try:
            values: list[float] = []
            payloads = retained_xlg_payloads(run_dir, trim_s=trim_s)
            column = "scheduler_delays" if payload_key == "SchedulerDelays" else "connection_delays"
            for current in payloads[column]:
                values.extend(finite_values(current))
            if values:
                return values
        except FileNotFoundError:
            pass

    windows = retained_windows(run_dir, trim_s=trim_s)
    samples = retained_samples(run_dir, windows)
    return metric_values(samples, metric)


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
    raw_by_metric: dict[str, list[float]] = {metric: [] for metric in METRIC_SOURCES}
    for run_dir in run_list:
        # print(f"computing normalizers with leave-out run {run_dir}")
        others = [path for path in run_list if path != run_dir]
        # print(f"The other runs are {others}")
        for metric, source in METRIC_SOURCES.items():
            # print(f"computing normalizer for metric {metric} with source {source}")
            current = run_metric_values(run_dir, source, trim_s=trim_s)
            reference = pooled_metric_values(others, source, trim_s=trim_s)
            # print(f"Total values for current: {len(current)}, Total values for reference: {len(reference)}")
            raw_by_metric[metric].append(raw_emd(current, reference))
    
    normalizers: dict[str, float] = {}
    for metric, values in raw_by_metric.items():
        # print(f"raw EMD values for metric {metric}: {values}")
        q95 = quantile(values, 0.95)
        # print(f"95th percentile for metric {metric}: {q95}")
        normalizers[metric] = q95 if math.isfinite(q95) and q95 > 0 else 1.0
    return normalizers


def baseline_concurrency(run_list: list[Path], configured_rate: int, trim_s: float = 5.0) -> float:
    """Estimate mean concurrency as configured rate times measured latency."""
    values: list[float] = []

    for run_dir in run_list:
        windows = retained_windows(run_dir, trim_s=trim_s)
        if "avg_total_latency_ms" not in windows.columns:
            continue
        for _, window in windows.iterrows():
            current_latency_ms = float(window["avg_total_latency_ms"])
            if not math.isfinite(current_latency_ms):
                continue
            values.append(configured_rate * current_latency_ms / 1000.0)

    return float(np.mean(values)) if values else float("nan")


def rho_values(run_list: list[Path], trim_s: float = 5.0) -> list[float]:
    """Collect trimmed rho windows across runs."""
    values: list[float] = []
    for run_dir in run_list:
        windows = retained_windows(run_dir, trim_s=trim_s)
        if "observed_R" in windows.columns:
            values.extend(finite_values(windows["observed_R"]))
    return values


def capacity_levels(
    baseline_count: float,
    factors: dict[str, float] = SEVERITY_FACTORS,
) -> dict[str, int]:
    """Build mild/mod/severe caps from one healthy baseline count."""
    return {
        severity: round_count(factor * baseline_count)
        for severity, factor in factors.items()
    }


def severity_from_count(baseline_count: float) -> dict[str, dict[str, int]]:
    """Build fault-injection settings from one healthy baseline count."""
    return {
        "workers": capacity_levels(baseline_count),
        "connections": capacity_levels(baseline_count, CONNECTION_SEVERITY_FACTORS),
        "cpu": CPU_STRESS_JOBS.copy(),
    }


@dataclass
class DiagnosisState:
    """Online diagnosis state retained across replayed windows."""

    # initializes in the normal regime with no previous rho and not terminal
    label: str = "NORMAL"
    previous_rho: float = float("nan")
    terminal: bool = False


def score_elevated(score: float, threshold: float) -> bool:
    """Return whether one normalized EMD score is above its threshold."""
    return math.isfinite(score) and score > threshold


def transition_window(
    state: DiagnosisState,
    rho: float,
    scheduler_score: float,
    connection_score: float,
    thresholds: dict[str, float],
    rho_center: float,
    epsilon: float,
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

    # determine whether the current window's scheduler and connection scores are elevated compared to the thresholds
    conn_elevated = score_elevated(connection_score, thresholds["T_conn"])
    worker_sched_elevated = score_elevated(scheduler_score, worker_threshold)
    cpu_sched_elevated = score_elevated(scheduler_score, cpu_threshold)

    # compare rho to the healthy center band defined by rho_center +/- epsilon
    rho_high = rho > rho_center + epsilon
    rho_low = rho < rho_center - epsilon

    # compare rho to previous values of rho
    rho_rising = math.isfinite(previous_rho) and rho > previous_rho + epsilon
    rho_falling = math.isfinite(previous_rho) and rho < previous_rho - epsilon
    state.previous_rho = rho

    if rho_high:
        # we compute emd's when the SUT is not degraded or rho is rising
        check_terminal = state.label != "SUT_DEGRADED" or rho_rising

        # due to the rising rho, we check for CPU_CONTENTION or FEW_CONNECTIONS, not FEW_WORKERS
        if check_terminal and cpu_sched_elevated:
            state.label = "CPU_CONTENTION"
            state.terminal = True
            return state.label, True, "rho_high_scheduler_delay", reference_rho
        if check_terminal and conn_elevated:
            state.label = "FEW_CONNECTIONS"
            state.terminal = True
            return state.label, True, "rho_high_connection_delay", reference_rho

        # no terminal conditions, but SUT is degraded if rho is high
        state.label = "SUT_DEGRADED"
        reason = "rho_high" if check_terminal else "hold_sut_degraded"
        return state.label, False, reason, reference_rho

    if rho_low:
        # due to a lower rho, we compute EMD if the rho is falling or if we haven't already labeled SUT_FASTER
        check_terminal = state.label != "SUT_FASTER" or rho_falling
        if check_terminal and worker_sched_elevated:
            state.label = "FEW_WORKERS"
            state.terminal = True
            return state.label, True, "rho_low_scheduler_delay", reference_rho
        
        # SUT is faster if rho is low but no terminal conditions are met
        state.label = "SUT_FASTER"
        reason = "rho_low" if check_terminal else "hold_sut_faster"
        return state.label, False, reason, reference_rho

    # if rho is within the healthy band, we return NORMAL
    if state.label not in TERMINAL_LABELS:
        state.label = "NORMAL"
    return state.label, False, "rho_within_band", reference_rho


WINDOW_SCORE_COLUMNS = ["rho", "scheduler_score", "connection_score"]


def xlg_payload_window_scores(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Score retained XLG anomaly payloads for one run."""
    # generates dataframe w/ window_star, rho, scheduler_delays, and connection_delays columns
    payloads = retained_xlg_payloads(run_dir, trim_s=trim_s)

    rows: list[dict[str, Any]] = []
    scheduler_normalizer = normalizers.get("scheduler_delay", 1.0) or 1.0
    connection_normalizer = normalizers.get("connection_delay", 1.0) or 1.0

    for _, row in payloads.reset_index(drop=True).iterrows():
        sched_raw = raw_emd(row["scheduler_delays"], reference["scheduler_delay"])
        conn_raw = raw_emd(row["connection_delays"], reference["connection_delay"])
        rows.append(
            {
                "rho": float(row["rho"]),
                "scheduler_score": normalized_score(sched_raw, scheduler_normalizer),
                "connection_score": normalized_score(conn_raw, connection_normalizer),
            }
        )
    return pd.DataFrame(rows, columns=WINDOW_SCORE_COLUMNS)


def csv_window_scores(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Score retained CSV windows for one run."""
    windows = retained_windows(run_dir, trim_s=trim_s)
    samples = retained_samples(run_dir, windows)
    grouped = {
        key: group
        for key, group in samples.groupby(["window_start", "window_end"], sort=False)
    }

    rows: list[dict[str, Any]] = []
    scheduler_normalizer = normalizers.get("scheduler_delay", 1.0) or 1.0
    connection_normalizer = normalizers.get("connection_delay", 1.0) or 1.0
    for _, row in windows.reset_index(drop=True).iterrows():
        key = (row["window_start"], row["window_end"])
        sample_df = grouped.get(key, samples.iloc[0:0])
        sched_raw = raw_emd(metric_values(sample_df, "scheduler_delay"), reference["scheduler_delay"])
        conn_raw = raw_emd(metric_values(sample_df, "conn_delay"), reference["connection_delay"])
        rows.append(
            {
                "rho": float(row.get("observed_R", float("nan"))),
                "scheduler_score": normalized_score(sched_raw, scheduler_normalizer),
                "connection_score": normalized_score(conn_raw, connection_normalizer),
            }
        )
    return pd.DataFrame(rows, columns=WINDOW_SCORE_COLUMNS)


def window_scores(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Score retained windows, preferring Vegeta XLG anomaly payloads."""
    try:
        scores = xlg_payload_window_scores(run_dir, reference, normalizers, trim_s=trim_s)
        if not scores.empty:
            return scores
    except FileNotFoundError:
        print(f"No XLG payloads found for {run_dir}, falling back to CSV scoring.")
    return csv_window_scores(run_dir, reference, normalizers, trim_s=trim_s)


def window_predictions(
    run_dir: Path,
    reference: dict[str, list[float]],
    normalizers: dict[str, float],
    thresholds: dict[str, float],
    rho_center: float,
    epsilon: float,
    trim_s: float = 5.0,
) -> pd.DataFrame:
    """Replay retained windows through the online diagnosis state machine."""

    # outputs a dataframe for each window consisting of rho, scheduler_score, connection_score
    scores = window_scores(run_dir, reference, normalizers, trim_s=trim_s)

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
            connection_score=float(row["connection_score"]),
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


def confusion_matrix(rows: pd.DataFrame) -> pd.DataFrame:
    """Build an actual-by-predicted confusion matrix."""
    labels = LABEL_ORDER.copy()
    extra = sorted(
        set(rows["actual_label"]).union(rows["predicted_label"]).difference(labels)
    )
    labels.extend(extra)
    matrix = pd.crosstab(rows["actual_label"], rows["predicted_label"])
    return matrix.reindex(index=labels, columns=labels, fill_value=0)
