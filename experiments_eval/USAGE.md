# XLG Inspector Two-Stage Experiment

Collection scripts live in `experiments_eval`, analysis scripts live in `scripts_eval`, and outputs go under `experiments_eval/output`.

## Requirements

- Build or keep a Vegeta binary at `./vegeta`, or set `VEGETA_BIN`.
- Start the SUT server on a separate VM before running the scripts.
- Set `NETEM_IFACE` for every collection run. The scripts do not start or stop a local server.
- Install `tc` on the load-generator VM and run with sudo access for client-side netem.
- Python needs `pandas`, `numpy`, `scipy`, and `matplotlib`.

Defaults:

- run duration: `15s`
- XLG trim: first `5s` plus the final window
- window size: `1s`
- metrics: `scheduler_delay`, `connection_delay`, `rho`
- normal client-side network delay: `5ms`
- degraded client-side network delay: `10ms`
- faster client-side network delay: netem removed
- CPU contention: background `yes` processes; defaults are mild/mod/severe = `150/200/250` jobs
- Worker and connection bottlenecks: caps come from Stage B baseline latency plus mild/mod/severe offsets of `6ms/4ms/2ms`; Stage B ramps network delay on top of normal delay during these runs

The shell scripts now print concise progress only. Per-run details are kept in each output directory.

The collection scripts set client-side netem automatically on the load-generator VM using `NETEM_IFACE`. Netem is removed automatically on script exit.

## Full Pipeline

Run everything in order:

```bash
NETEM_IFACE=<iface> \
BASELINE_RPS=3000 \
TARGET_RPS=3000 \
experiments_eval/run_full_pipeline.sh
```

That runs:

1. Stage A healthy collection
2. Stage A count analysis
3. Stage A threshold analysis
4. Stage B baseline collection
5. Stage B fault-setting analysis
6. Stage B condition collection
7. Stage B evaluation

It creates paired timestamped directories:

```text
experiments_eval/output/stage_a_fixed/run_<timestamp>/
experiments_eval/output/stage_b_variable/run_<timestamp>/
```

Useful quick-run override:

```bash
NETEM_IFACE=<iface> \
DURATION=15s \
NUM_HEALTHY_RUNS=3 \
NUM_BASELINE_RUNS=3 \
NUM_EVAL_RUNS=1 \
BASELINE_RPS=400 \
TARGET_RPS=500 \
STAGE_B_SEVERITIES="mod severe" \
experiments_eval/run_full_pipeline.sh
```

`BASELINE_RPS` is used for Stage A calibration. `TARGET_RPS` is used for the Stage B healthy baseline, Stage B fault settings, and Stage B condition/fault runs. If `TARGET_RPS` is not set, Stage B falls back to `BASELINE_RPS`. The older names `RATE` and `EVAL_RATE` still work as aliases.

## Manual Steps

Collect Stage A healthy external-SUT runs:

```bash
NETEM_IFACE=<iface> experiments_eval/run_stage_a_healthy.sh
```

Compute Stage A counts and severity settings:

```bash
python3 scripts_eval/stage_a_fixed_counts.py \
  --stage-a-dir experiments_eval/output/stage_a_fixed/run_<timestamp>
```

Compute Stage A thresholds:

```bash
python3 scripts_eval/stage_a_thresholds.py \
  --stage-a-dir experiments_eval/output/stage_a_fixed/run_<timestamp>
```

Collect Stage B healthy external-SUT baseline runs:

```bash
NETEM_IFACE=<iface> TARGET_RPS=5000 experiments_eval/run_stage_b_baseline.sh
```

Compute Stage B fault injection settings:

```bash
python3 scripts_eval/stage_b_reference.py \
  --stage-b-dir experiments_eval/output/stage_b_variable/run_<timestamp>
```

Collect Stage B condition runs:

```bash
NETEM_IFACE=<iface> \
STAGE_A_DIR=experiments_eval/output/stage_a_fixed/run_<timestamp> \
STAGE_B_DIR=experiments_eval/output/stage_b_variable/run_<timestamp> \
  experiments_eval/run_stage_b_conditions.sh
```

Evaluate Stage B:

```bash
python3 scripts_eval/stage_b_evaluate.py \
  --stage-b-dir experiments_eval/output/stage_b_variable/run_<timestamp> \
  --stage-a-thresholds experiments_eval/output/stage_a_fixed/run_<timestamp>/stage_a_thresholds.json
```

## Outputs

Stage A count analysis creates:

- `stage_a_counts.json`
- `rate`
- `rho_center_fixed`, `epsilon_fixed`

Stage A threshold analysis creates:

- `stage_a_thresholds.json`
- EMD normalizers
- `T_conn`, `T_cpu`, `T_worker` from the healthy-window score percentile (`--threshold-quantile`, default `0.90`)

The EMD reference, normalizers, thresholds, and Stage B replay use Vegeta's `XLG-WINDOW` anomaly payloads from `xlg_windows_rps*.log`. Window summary CSVs are still used for setup fields such as latency and observed R.

Stage B fault-setting analysis creates `stage_b_reference.json` with the target rate and mild/mod/severe CPU, worker, and connection settings used for Stage B fault injection.

Stage B conditions create:

- `NORMAL`
- `SUT_DEGRADED` with client-side network delay `10ms`
- `SUT_FASTER` with client-side network delay removed
- `CPU_CONTENTION/mild`, `CPU_CONTENTION/mod`, `CPU_CONTENTION/severe`
- `FEW_WORKERS/mild`, `FEW_WORKERS/mod`, `FEW_WORKERS/severe` with fixed Stage B worker caps and a network-delay ramp from normal delay to normal plus `BOTTLENECK_RAMP_EXTRA_DELAY`
- `FEW_CONNECTIONS/mild`, `FEW_CONNECTIONS/mod`, `FEW_CONNECTIONS/severe` with fixed Stage B connection caps and the same network-delay ramp

Stage B evaluation writes a fresh directory under `evaluation/` with:

- `run_predictions.csv`
- `confusion_matrix_heatmap.png`
- `evaluation_summary.json`

Stage B evaluation uses Stage A as the calibration source:

- EMD reference distributions come from Stage A healthy runs
- EMD normalizers and thresholds come from `stage_a_thresholds.json`
- rho center and epsilon come from Stage A baseline calibration
- Stage B `stage_b_reference.json` supplies fault-injection settings, not evaluation calibration

Actual labels in the confusion matrix are ordered as:

```text
FEW_CONNECTIONS, FEW_WORKERS, CPU_CONTENTION, SUT_DEGRADED, SUT_FASTER, NORMAL
```

## Notes

The scripts create directories with `mkdir -p`. If you reuse an output directory, files from matching run names can be replaced.

One extra reference run is collected in Stage A. Stage B condition collection reuses that Stage A reference CSV so `observed_R` is calibrated against Stage A. Stage B baseline runs feed the Stage B reference/settings step. In the full pipeline, Stage B baseline and condition collection use `TARGET_RPS` when it is set.

For `FEW_WORKERS` and `FEW_CONNECTIONS`, Stage B starts at `NORMAL_NETWORK_DELAY` and applies repeated `tc qdisc replace` calls during the Vegeta run until reaching `NORMAL_NETWORK_DELAY + BOTTLENECK_RAMP_EXTRA_DELAY`. The attack is not paused or restarted while the delay changes.

Run-level prediction replays retained windows through the online diagnosis state machine. The replay starts in `NORMAL` after trimming, uses Stage A rho center for the baseline band, advances one window at a time, and latches the first terminal diagnosis. `FEW_CONNECTIONS` uses high/rising rho with elevated connection delay, `FEW_WORKERS` uses low/falling rho with elevated scheduler delay, and `CPU_CONTENTION` uses high/rising rho with strongly elevated scheduler delay. No whole-run aggregate override is used.

Useful overrides:

Assume `NETEM_IFACE` is set in the environment for these examples.

```bash
VEGETA_BIN=./vegeta_local experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3500 experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3000 TARGET_RPS=5000 experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3500 experiments_eval/run_stage_a_healthy.sh
TARGET_RPS=5000 experiments_eval/run_stage_b_baseline.sh
NETEM_IFACE=ens33 experiments_eval/run_full_pipeline.sh
STAGE_B_SEVERITIES="mod severe" NUM_EVAL_RUNS=1 experiments_eval/run_stage_b_conditions.sh
TARGET_RPS=500 experiments_eval/run_stage_b_conditions.sh
SLEEP_BETWEEN_RUNS=10 experiments_eval/run_stage_b_conditions.sh
```
