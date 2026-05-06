from pathlib import Path
import pandas as pd

from stage_b_evaluate import discover_condition_runs, stage_a_dir_from_thresholds
from xlg_eval_common import (
    LABEL_ORDER,
    pooled_reference,
    read_json,
    run_dirs,
    run_prediction_from_windows,
    window_predictions,
)

TRIM_S = 5.0
THRESHOLDS_FILENAME = "stage_a_thresholds.json"
OUTPUT_PATH = Path("cross_rps_confusion_matrices_3_servers.png")
SERVER_CONFIGS = {
    "ExpServer": {
        "stage_a_paths": {
            1000: Path(f"data copy/ExpServer_results/stage_a_fixed/run_20260426_011652/{THRESHOLDS_FILENAME}"),
            2000: Path(f"data copy/ExpServer_results/stage_a_fixed/run_20260426_013506/{THRESHOLDS_FILENAME}"),
            3000: Path(f"data copy/ExpServer_results/stage_a_fixed/run_20260426_015103/{THRESHOLDS_FILENAME}"),
        },
        "stage_b_paths": {
            1000: Path("data copy/ExpServer_results/stage_b_variable/run_20260426_011652"),
            2000: Path("data copy/ExpServer_results/stage_b_variable/run_20260426_013506"),
            3000: Path("data copy/ExpServer_results/stage_b_variable/run_20260426_015103"),
        },
    },
    "HotelReservation": {
        "stage_a_paths": {
            1000: Path(f"data copy/HotelReservation_results/hotelreservation_rps_1000/stage_a_fixed/run_20260426_180606/{THRESHOLDS_FILENAME}"),
            1500: Path(f"data copy/HotelReservation_results/hotelreservation_rps_1500/stage_a_fixed/run_20260426_182130/{THRESHOLDS_FILENAME}"),
            2000: Path(f"data copy/HotelReservation_results/hotelreservation_rps_2000/stage_a_fixed/run_20260426_190347/{THRESHOLDS_FILENAME}"),
        },
        "stage_b_paths": {
            1000: Path("data copy/HotelReservation_results/hotelreservation_rps_1000/stage_b_variable/run_20260426_180606"),
            1500: Path("data copy/HotelReservation_results/hotelreservation_rps_1500/stage_b_variable/run_20260426_182130"),
            2000: Path("data copy/HotelReservation_results/hotelreservation_rps_2000/stage_b_variable/run_20260426_190347"),
        },
    },
    "SocialNetwork": {
        "stage_a_paths": {
            1000: Path(f"data copy/SocialNetwork_results/stage_a_fixed/run_20260502_205729/{THRESHOLDS_FILENAME}"),
            1500: Path(f"data copy/SocialNetwork_results/stage_a_fixed/run_20260502_211916/{THRESHOLDS_FILENAME}"),
            2000: Path(f"data copy/SocialNetwork_results/stage_a_fixed/run_20260502_210830/{THRESHOLDS_FILENAME}"),
        },
        "stage_b_paths": {
            1000: Path("data copy/SocialNetwork_results/stage_b_variable/run_20260502_205729"),
            1500: Path("data copy/SocialNetwork_results/stage_b_variable/run_20260502_211916"),
            2000: Path("data copy/SocialNetwork_results/stage_b_variable/run_20260502_210830"),
        },
    },
}

import numpy as np
import matplotlib.pyplot as plt

PAPER_LABELS = {
    "FEW_CONNECTIONS": "Few\nconnections",
    "FEW_WORKERS": "Few\nworkers",
    "CPU_CONTENTION": "CPU\ncontention",
    "SUT_DEGRADED": "SUT\ndegraded",
    "SUT_FASTER": "SUT\nfaster",
    "NORMAL": "Normal",
}


def paper_label(raw_label):
    return PAPER_LABELS.get(raw_label, raw_label.replace("_", " ").title())


def plot_confusion_matrix_on_axis(ax, confusion_df, title):
    counts = confusion_df.to_numpy(dtype=float)

    row_sums = counts.sum(axis=1, keepdims=True)
    norm = np.divide(counts, row_sums, out=np.zeros_like(counts), where=row_sums > 0)
    display_columns = [paper_label(label) for label in confusion_df.columns]
    display_index = [paper_label(label) for label in confusion_df.index]

    im = ax.imshow(norm, cmap="Blues", vmin=0.0, vmax=1.0)

    ax.set_xticks(range(len(confusion_df.columns)))
    ax.set_yticks(range(len(confusion_df.index)))
    ax.set_xticklabels(display_columns, fontsize=11)
    ax.set_yticklabels(display_index, fontsize=11)
    ax.tick_params(length=0)

    for i in range(len(confusion_df.index)):
        for j in range(len(confusion_df.columns)):
            count = int(counts[i, j])
            pct = 100.0 * norm[i, j]
            color = "white" if norm[i, j] > 0.5 else "black"
            ax.text(
                j,
                i,
                f"{count}\n{pct:.0f}%",
                ha="center",
                va="center",
                color=color,
                fontsize=10,
                fontweight="semibold",
            )

    ax.set_title(title, fontsize=16, fontweight="bold", pad=12)
    return im


def next_available_path(path):
    if not path.exists():
        return path

    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.stem}_{idx}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise FileExistsError(f"no available output path for {path}")


def plot_confusion_matrices_paper(confusions_by_server, out_path=None):
    fig, axes = plt.subplots(
        1,
        len(confusions_by_server),
        figsize=(19.5, 6.5),
        sharey=True,
        constrained_layout=True,
    )
    axes = np.atleast_1d(axes)

    im = None
    for ax, (server_name, confusion_df) in zip(axes, confusions_by_server.items()):
        im = plot_confusion_matrix_on_axis(ax, confusion_df, server_name)

    for ax in axes[1:]:
        ax.tick_params(labelleft=False)

    fig.supxlabel("Predicted diagnosis", fontsize=16, fontweight="bold")
    fig.supylabel("Actual diagnosis", fontsize=16, fontweight="bold")
    fig.suptitle("Cross-RPS Diagnosis Confusion Matrices", fontsize=18, fontweight="bold")

    cbar = fig.colorbar(im, ax=axes, fraction=0.018, pad=0.012)
    cbar.set_label("Row-normalized fraction", fontsize=13, fontweight="bold")
    cbar.ax.tick_params(labelsize=11)

    if out_path is None:
        plt.show()
        return None

    fig.savefig("cross_rps_confusion_matrix.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    return "cross_rps_confusion_matrix.png"


def evaluate_stage_pair(stage_a_rate, stage_a_thresholds, stage_b_rate, stage_b_dir):
    stage_a_payload = read_json(stage_a_thresholds)
    stage_a_dir = stage_a_dir_from_thresholds(stage_a_thresholds, stage_a_payload)

    # obtain the reference and other parameters from stage A, then apply to stage B runs
    reference = pooled_reference(run_dirs(stage_a_dir / "healthy"), trim_s=TRIM_S)
    thresholds = stage_a_payload["thresholds"]
    normalizers = stage_a_payload["normalizers"]
    rho_center = stage_a_payload["rho_center_fixed"]
    epsilon = stage_a_payload["epsilon_fixed"]
    cheap_quantiles = stage_a_payload["cheap_signal_quantiles"]

    run_rows = []
    for item in discover_condition_runs(stage_b_dir):
        # run the stage B prediction using the stage A parameters and reference
        windows = window_predictions(
            run_dir=Path(item["run_dir"]),
            reference=reference,
            normalizers=normalizers,
            thresholds=thresholds,
            rho_center=rho_center,
            epsilon=epsilon,
            cheap_quantiles=cheap_quantiles,
            trim_s=TRIM_S,
        )
        predicted_label, _ = run_prediction_from_windows(windows)

        # add the result to the list of rows for this stage pair
        run_rows.append(
            {
                "stage_a_rate": stage_a_rate,
                "stage_b_rate": stage_b_rate,
                "actual_label": item["actual_label"],
                "predicted_label": predicted_label,
            }
        )

    return pd.DataFrame(run_rows)


def cross_rps_confusion(stage_a_paths, stage_b_paths, server_name):
    pair_rows_cross_rps = []
    for stage_a_rate in stage_a_paths.keys():
        for stage_b_rate in stage_b_paths.keys():
            if stage_a_rate < stage_b_rate:
                print(f"{server_name}: Stage A {stage_a_rate} --> Stage B {stage_b_rate}")
                run_preds_df = evaluate_stage_pair(
                    stage_a_rate,
                    stage_a_paths[stage_a_rate],
                    stage_b_rate,
                    stage_b_paths[stage_b_rate],
                )

                pair_rows_cross_rps.append(run_preds_df)

    cross_rps_df = pd.concat(pair_rows_cross_rps, ignore_index=True)
    cross_rps_confusion = pd.crosstab(
        cross_rps_df["actual_label"],
        cross_rps_df["predicted_label"],
    )
    cross_rps_confusion = cross_rps_confusion.reindex(
        index=LABEL_ORDER,
        columns=LABEL_ORDER,
        fill_value=0,
    )
    return cross_rps_confusion


if __name__ == "__main__":
    confusions_by_server = {}
    for server_name, config in SERVER_CONFIGS.items():
        confusions_by_server[server_name] = cross_rps_confusion(
            config["stage_a_paths"],
            config["stage_b_paths"],
            server_name,
        )

    saved_path = plot_confusion_matrices_paper(confusions_by_server, out_path=OUTPUT_PATH)
    if saved_path is not None:
        print(f"Wrote {saved_path}")
