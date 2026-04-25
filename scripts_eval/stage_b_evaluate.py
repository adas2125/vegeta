#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from xlg_eval_common import (
    LABEL_ORDER,
    mode_label,
    pooled_reference,
    read_json,
    run_dirs,
    run_prediction_from_windows,
    window_predictions,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Stage B condition runs.")
    # specifying stage b directory and thresholds from stage a
    parser.add_argument("--stage-b-dir", type=Path, required=True)
    parser.add_argument("--stage-a-thresholds", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--trim-s", type=float, default=5.0)
    return parser.parse_args()


def repo_root() -> Path:
    """Return the repository root from this script location."""
    return Path(__file__).resolve().parents[1]


def latest_stage_a_thresholds() -> Path:
    """Find the latest Stage A threshold file."""
    root = repo_root() / "experiments_eval" / "output" / "stage_a_fixed"
    candidates = sorted(root.glob("run_*/stage_a_thresholds.json"))
    if not candidates:
        raise FileNotFoundError("no stage_a_thresholds.json found; pass --stage-a-thresholds")
    return candidates[-1]


def stage_a_dir_from_thresholds(thresholds_path: Path, payload: dict[str, object]) -> Path:
    """Resolve the Stage A run directory recorded in a thresholds file."""
    raw = payload.get("stage_a_dir")
    if raw is None:
        raise KeyError(f"{thresholds_path} is missing stage_a_dir")

    stage_a_dir = Path(str(raw))
    if stage_a_dir.exists():
        return stage_a_dir

    candidate = thresholds_path.parent
    if (candidate / "healthy").exists():
        return candidate

    raise FileNotFoundError(f"could not resolve Stage A directory from {thresholds_path}")


def discover_condition_runs(stage_b_dir: Path) -> list[dict[str, object]]:
    """Find all Stage B condition run directories."""
    conditions_dir = stage_b_dir / "conditions"
    rows: list[dict[str, object]] = []
    for actual in LABEL_ORDER:
        case_dir = conditions_dir / actual
        if not case_dir.exists():
            continue

        direct_runs = run_dirs(case_dir)
        if direct_runs:
            for run_dir in direct_runs:
                rows.append({"actual_label": actual, "severity": "", "run_dir": run_dir})
            continue

        for severity in ["mild", "mod", "severe"]:
            for run_dir in run_dirs(case_dir / severity):
                rows.append(
                    {
                        "actual_label": actual,
                        "severity": severity,
                        "run_dir": run_dir,
                    }
                )
    return rows


def write_heatmap(path: Path, matrix: pd.DataFrame) -> None:
    """Save the confusion matrix as an annotated heatmap."""
    os.environ.setdefault("MPLCONFIGDIR", str((repo_root() / ".matplotlib").resolve()))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    values = matrix.to_numpy(dtype=float)
    max_value = max(float(values.max()) if values.size else 0.0, 1.0)

    fig_width = max(8.0, 1.05 * len(matrix.columns) + 2.5)
    fig_height = max(6.0, 0.8 * len(matrix.index) + 2.0)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    image = ax.imshow(values, cmap="Blues", vmin=0, vmax=max_value)

    ax.set_title("XLG Inspector Confusion Matrix")
    ax.set_xlabel("Predicted label")
    ax.set_ylabel("Actual label")
    ax.set_xticks(range(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=35, ha="right")
    ax.set_yticks(range(len(matrix.index)))
    ax.set_yticklabels(matrix.index)

    for row_idx, actual in enumerate(matrix.index):
        for col_idx, predicted in enumerate(matrix.columns):
            value = int(matrix.loc[actual, predicted])
            color = "white" if value > max_value / 2 else "black"
            ax.text(col_idx, row_idx, str(value), ha="center", va="center", color=color)

    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04, label="runs")
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)

def confusion_matrix(rows: pd.DataFrame) -> pd.DataFrame:
    """Build an actual-by-predicted confusion matrix."""
    labels = LABEL_ORDER.copy()
    extra = sorted(
        set(rows["actual_label"]).union(rows["predicted_label"]).difference(labels)
    )
    labels.extend(extra)
    matrix = pd.crosstab(rows["actual_label"], rows["predicted_label"])
    return matrix.reindex(index=labels, columns=labels, fill_value=0)

def main() -> None:
    args = parse_args()
    stage_b_dir = args.stage_b_dir
    stage_a_thresholds_path = args.stage_a_thresholds or latest_stage_a_thresholds()

    # creating the output directory
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or stage_b_dir / "evaluation" / f"run_{stamp}"
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing output dir: {output_dir}")
    output_dir.mkdir(parents=True)

    # reads the thresholds from stage a
    stage_a_thresholds = read_json(stage_a_thresholds_path)

    # obtaining the stage a directory from thresholds file
    stage_a_dir = stage_a_dir_from_thresholds(stage_a_thresholds_path, stage_a_thresholds)

    # reading the thresholds, normalizers, rho_center, epsilon from rho_center, from stage a thresholds file
    thresholds = stage_a_thresholds["thresholds"]
    normalizers = stage_a_thresholds["normalizers"]
    rho_center = float(stage_a_thresholds["rho_center_fixed"])
    epsilon = float(stage_a_thresholds["epsilon_fixed"])
    cheap_quantiles = stage_a_thresholds["cheap_signal_quantiles"]

    # identifies the healthy runs from stage a
    healthy_runs = run_dirs(stage_a_dir / "healthy")
    if not healthy_runs:
        raise FileNotFoundError(f"no Stage A healthy runs found under {stage_a_dir / 'healthy'}")
    
    # pools all the healthy runs into a single reference for evaluation
    reference = pooled_reference(healthy_runs, trim_s=args.trim_s)

    # discovers the condition runs from stage b conditions directory
    # returns a list of dicts with keys: actual_label, severity, run_dir
    condition_runs = discover_condition_runs(stage_b_dir)
    if not condition_runs:
        raise FileNotFoundError(f"no condition runs found under {stage_b_dir / 'conditions'}")

    run_rows: list[dict[str, object]] = []
    for item in condition_runs:
        run_dir = Path(item["run_dir"])
        # print(f"[INFO] Evaluating {run_dir} with actual label {item['actual_label']} and severity {item['severity']}")
        # Replay retained windows through the same online state machine used by the live diagnoser.
        windows = window_predictions(
            run_dir=run_dir,
            reference=reference,
            normalizers=normalizers,
            thresholds=thresholds,
            rho_center=rho_center,
            epsilon=epsilon,
            cheap_quantiles=cheap_quantiles,
            trim_s=args.trim_s,
        )
        
        predicted, terminal = run_prediction_from_windows(windows)
        if windows.empty:
            raise ValueError(f"{run_dir} produced no retained XLG windows; check duration, trim_s, or XLG log")
        else:
            # If any window is terminal, the decision is the first terminal window. Otherwise, it's the last window.
            terminal_windows = windows[windows["terminal"]]
            decision = terminal_windows.iloc[0] if terminal else windows.iloc[-1]
            decision_window = int(decision.name) + 1
            decision_reason = str(decision["transition_reason"])
            rho_decision = float(decision["rho"])
            scheduler_score_decision = float(decision["scheduler_score"])
            connection_score_decision = float(decision["connection_score"])
            scheduler_mean_decision = float(decision["scheduler_mean_ms"])
            connection_mean_decision = float(decision["connection_mean_ms"])
            scheduler_median_decision = float(decision["scheduler_median_ms"])
            connection_p25_decision = float(decision["connection_p25_ms"])
            scheduler_mean_gt_healthy_p95_decision = bool(decision["scheduler_mean_gt_healthy_p95"])
            connection_p25_gt_healthy_p95_decision = bool(decision["connection_p25_gt_healthy_p95"])
            emd_computed_decision = bool(decision["emd_computed"])
            emd_reason_decision = str(decision["emd_reason"])
            emd_computed_windows = int(windows["emd_computed"].fillna(False).astype(bool).sum())
            emd_total_windows = int(len(windows))
            emd_computed_fraction = emd_computed_windows / emd_total_windows

        run_rows.append(
            {
                "actual_label": item["actual_label"],
                "severity": item["severity"],
                "run_dir": str(run_dir),
                "predicted_label": predicted,
                "terminal": terminal,
                "window_mode": mode_label(windows["window_prediction"]) if not windows.empty else "UNKNOWN",
                "decision_window": decision_window,
                "decision_reason": decision_reason,
                "rho_decision": rho_decision,
                "scheduler_score_decision": scheduler_score_decision,
                "connection_score_decision": connection_score_decision,
                "scheduler_mean_decision": scheduler_mean_decision,
                "connection_mean_decision": connection_mean_decision,
                "scheduler_median_decision": scheduler_median_decision,
                "connection_p25_decision": connection_p25_decision,
                "scheduler_mean_gt_healthy_p95_decision": scheduler_mean_gt_healthy_p95_decision,
                "connection_p25_gt_healthy_p95_decision": connection_p25_gt_healthy_p95_decision,
                "emd_computed_decision": emd_computed_decision,
                "emd_reason_decision": emd_reason_decision,
                "emd_computed_windows": emd_computed_windows,
                "emd_total_windows": emd_total_windows,
                "emd_computed_fraction": emd_computed_fraction,
            }
        )

    run_df = pd.DataFrame(run_rows)
    matrix = confusion_matrix(run_df)

    # saves the output files of the predictions including confusion matrix
    run_predictions_csv = output_dir / "run_predictions.csv"
    confusion_heatmap = output_dir / "confusion_matrix_heatmap.png"
    summary_json = output_dir / "evaluation_summary.json"

    run_df.to_csv(run_predictions_csv, index=False)
    write_heatmap(confusion_heatmap, matrix)

    write_json(
        summary_json,
        {
            "stage_b_dir": stage_b_dir,
            "stage_a_thresholds": stage_a_thresholds_path,
            "run_predictions_csv": run_predictions_csv,
            "confusion_matrix_heatmap": confusion_heatmap,
        },
    )

    print(f"Wrote {output_dir}")


if __name__ == "__main__":
    main()
