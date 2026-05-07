from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from stage_b_evaluate import discover_condition_runs, stage_a_dir_from_thresholds
from xlg_eval_common import (
    LABEL_ORDER,
    pooled_reference,
    read_json,
    run_dirs,
    run_prediction_from_windows,
    window_predictions,
)

# Constants and path configurations for the eval setup
TRIM_S = 5.0
THRESHOLDS_FILENAME = "stage_a_thresholds.json"
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

# labels for the figure
PAPER_LABELS = {
    "FEW_CONNECTIONS": "FewConnections",
    "FEW_WORKERS": "FewWorkers",
    "CPU_CONTENTION": "CpuContention",
    "SUT_DEGRADED": "SuTDegraded",
    "SUT_FASTER": "SuTFaster",
    "NORMAL": "Normal",
}

SEVERITIES = ['mild', 'mod', 'severe']
TERMINALS = ['FEW_CONNECTIONS', 'FEW_WORKERS', 'CPU_CONTENTION']


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
    ax.set_xticklabels(
        display_columns,
        fontsize=13,
        rotation=35,
        ha="right",
        rotation_mode="anchor",
    )
    ax.set_yticklabels(display_index, fontsize=13)
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
                fontsize=12,
                fontweight="semibold",
            )

    ax.set_title(title, fontsize=20, fontweight="bold", pad=14)
    return im


def plot_confusion_matrices_paper(confusions_by_server):
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

    fig.supxlabel("Predicted diagnosis", fontsize=19, fontweight="bold")
    fig.supylabel("Actual diagnosis", fontsize=19, fontweight="bold")

    cbar = fig.colorbar(im, ax=axes, fraction=0.018, pad=0.012)
    cbar.set_label("Row-normalized fraction", fontsize=15, fontweight="bold")
    cbar.ax.tick_params(labelsize=13)

    fig.savefig("cross_rps_confusion_matrix.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


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

    when_latched = {}   # to track when the terminal label is latched on

    # initializer when_latched with terminal labels and severities
    for terminal in TERMINALS:
        when_latched[terminal] = {severity: np.inf for severity in SEVERITIES}

    run_rows = []
    for item in discover_condition_runs(stage_b_dir):       
        severity = item["severity"]     
        actual_label = item["actual_label"]
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

        # find the index of the row where "transition_reason" is "terminal_latched"
        if predicted_label in TERMINALS:
            latched_rows = windows[windows["transition_reason"] == "terminal_latched"]
            if not latched_rows.empty:
                # organize by actual label and severity, then take the index of the first latched row
                when_latched[actual_label][severity] = latched_rows.index[0]
            else:
                when_latched[actual_label][severity] = np.inf

        # add the result to the list of rows for this stage pair
        run_rows.append(
            {
                "stage_a_rate": stage_a_rate,
                "stage_b_rate": stage_b_rate,
                "actual_label": item["actual_label"],
                "predicted_label": predicted_label,
            }
        )

    return pd.DataFrame(run_rows), when_latched


def cross_rps_confusion(stage_a_paths, stage_b_paths, server_name):
    pair_rows_cross_rps = []
    all_when_latched = {}
    for stage_a_rate in stage_a_paths.keys():
        for stage_b_rate in stage_b_paths.keys():
            # only compare pairs where the stage A calibration rate is lower than the stage B rate
            if stage_a_rate < stage_b_rate:
                print(f"{server_name}: Stage A {stage_a_rate} --> Stage B {stage_b_rate}")
                run_preds_df, when_latched = evaluate_stage_pair(
                    stage_a_rate,
                    stage_a_paths[stage_a_rate],
                    stage_b_rate,
                    stage_b_paths[stage_b_rate],
                )
                pair_rows_cross_rps.append(run_preds_df)
                all_when_latched[(stage_a_rate, stage_b_rate)] = when_latched
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
    return cross_rps_confusion, all_when_latched

def output_stats(when_latched_by_server):
    """
    Returns the average index of when the terminal label is latched on for each server, severity, and terminal type.
    """
    for server_name, when_latched in when_latched_by_server.items():
        print(f"Against {server_name}:")
        for terminal in TERMINALS:
            for severity in SEVERITIES:
                latched_indices = []
                for (stage_a_rate, stage_b_rate), latched_dict in when_latched.items():
                    print(f"  Stage A {stage_a_rate} --> Stage B {stage_b_rate}: {terminal} - {severity} latched at index {latched_dict[terminal][severity]}")
                    latched_index = latched_dict[terminal][severity]
                    if np.isfinite(latched_index):
                        latched_indices.append(latched_index)
                if latched_indices:
                    avg_index = np.mean(latched_indices)
                    print(f"  {terminal} - {severity}: Average index of terminal latch = {avg_index:.2f}")
                else:
                    print(f"  {terminal} - {severity}: No terminal latch observed")


if __name__ == "__main__":
    confusions_by_server = {}
    when_latched_by_server = {}
    for server_name, config in SERVER_CONFIGS.items():
        cm, when_latched = cross_rps_confusion(
            config["stage_a_paths"],
            config["stage_b_paths"],
            server_name,
        )
        confusions_by_server[server_name] = cm
        when_latched_by_server[server_name] = when_latched

    plot_confusion_matrices_paper(confusions_by_server)
    output_stats(when_latched_by_server)
