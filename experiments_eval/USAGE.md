# XLG Inspector Two-Stage Experiment

Collection scripts live in `experiments_eval`, analysis scripts live in `scripts_eval`, and outputs go under `experiments_eval/output`.

## Requirements

- Build or keep a Vegeta binary at `./vegeta`, or set `VEGETA_BIN`.
- Build the synthetic server binary used by the scripts:

```bash
mkdir -p experiments_eval/output/tools
go build -o experiments_eval/output/tools/simple_server ./cmd/simple_server
```

- Python needs `pandas`, `numpy`, `scipy`, and `matplotlib`.

Defaults:

- run duration: `15s`
- XLG trim: first `5s` plus the final window
- window size: `1s`
- Vegeta logical CPUs: `VEGETA_LOGICAL_CPUS=4`
- metrics: `scheduler_delay`, `connection_delay`, `rho`
- Stage A server: exponential delay with `BASELINE_MEAN_DELAY=10ms`
- CPU contention: background `yes` processes; defaults are mild/mod/severe = `25/50/75` jobs
- Worker and connection bottlenecks: mild/mod/severe use `0.80/0.65/0.50` of baseline concurrency, estimated as configured baseline RPS times measured mean latency from trimmed baseline windows

The shell scripts now print concise progress only. Per-run details are kept in each output directory.

## Full Pipeline

Run everything in order:

```bash
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
DURATION=15s \
NUM_HEALTHY_RUNS=3 \
NUM_BASELINE_RUNS=3 \
NUM_EVAL_RUNS=1 \
BASELINE_RPS=400 \
TARGET_RPS=500 \
STAGE_B_SEVERITIES="mod severe" \
VEGETA_LOGICAL_CPUS=1 \
experiments_eval/run_full_pipeline.sh
```

`BASELINE_RPS` is used for Stage A calibration. `TARGET_RPS` is used for the Stage B healthy baseline, Stage B fault sizing, and Stage B condition/fault runs. If `TARGET_RPS` is not set, Stage B falls back to `BASELINE_RPS`. The older names `RATE` and `EVAL_RATE` still work as aliases.

## Manual Steps

Collect Stage A healthy exponential-delay runs:

```bash
experiments_eval/run_stage_a_healthy.sh
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

Collect Stage B healthy variable-delay baseline runs:

```bash
TARGET_RPS=5000 experiments_eval/run_stage_b_baseline.sh
```

Compute Stage B fault injection settings:

```bash
python3 scripts_eval/stage_b_reference.py \
  --stage-b-dir experiments_eval/output/stage_b_variable/run_<timestamp>
```

Collect Stage B condition runs:

```bash
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
- mild/mod/severe worker and connection counts

Stage A threshold analysis creates:

- `stage_a_thresholds.json`
- EMD normalizers
- `T_conn`, `T_cpu`, `T_worker` from the healthy-window score percentile (`--threshold-quantile`, default `0.90`)

The EMD reference, normalizers, thresholds, and Stage B replay prefer Vegeta's `XLG-WINDOW` anomaly payloads from `xlg_windows_rps*.log`. The older `window_samples_rps*.csv` path is still used as a fallback when no retained payload values are available.

Stage B fault-setting analysis creates `stage_b_reference.json` with the target rate and mild/mod/severe settings used for Stage B fault injection.

Stage B conditions create:

- `NORMAL`
- `SUT_DEGRADED` with exponential mean `20ms`
- `SUT_FASTER` with exponential mean `5ms`
- `CPU_CONTENTION/mild`, `CPU_CONTENTION/mod`, `CPU_CONTENTION/severe`
- `FEW_WORKERS/mild`, `FEW_WORKERS/mod`, `FEW_WORKERS/severe`
- `FEW_CONNECTIONS/mild`, `FEW_CONNECTIONS/mod`, `FEW_CONNECTIONS/severe`

Stage B evaluation writes a fresh directory under `evaluation/` with:

- `run_predictions.csv`
- `confusion_matrix_heatmap.png`
- `evaluation_summary.json`

Stage B evaluation uses Stage A as the calibration source:

- EMD reference distributions come from Stage A healthy runs
- EMD normalizers and thresholds come from `stage_a_thresholds.json`
- rho center and epsilon come from Stage A baseline calibration
- Stage B `stage_b_reference.json` is used by condition collection for fault magnitudes, not by evaluation calibration

Actual labels in the confusion matrix are ordered as:

```text
FEW_CONNECTIONS, FEW_WORKERS, CPU_CONTENTION, SUT_DEGRADED, SUT_FASTER, NORMAL
```

## Notes

The scripts create directories with `mkdir -p`. If you reuse an output directory, files from matching run names can be replaced.

One extra reference run is collected in Stage A. Stage B condition collection reuses that Stage A reference CSV so `observed_R` is calibrated against Stage A while the Stage B baseline runs are used only to size injected faults. In the full pipeline, Stage B baseline and condition collection use `TARGET_RPS` when it is set.

`run_stage_a_mild.sh` is still available for optional mild diagnostic runs, but threshold calibration no longer depends on it.

Run-level prediction replays retained windows through the online diagnosis state machine. The replay starts in `NORMAL` after trimming, uses Stage A rho center for the baseline band, advances one window at a time, and latches the first terminal diagnosis. `FEW_CONNECTIONS` uses high/rising rho with elevated connection delay, `FEW_WORKERS` uses low/falling rho with elevated scheduler delay, and `CPU_CONTENTION` uses high/rising rho with strongly elevated scheduler delay. No whole-run aggregate override is used.

Useful overrides:

```bash
VEGETA_LOGICAL_CPUS=1 experiments_eval/run_full_pipeline.sh
VEGETA_BIN=./vegeta_local experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3500 experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3000 TARGET_RPS=5000 experiments_eval/run_full_pipeline.sh
BASELINE_RPS=3500 experiments_eval/run_stage_a_healthy.sh
SERVER_PORT=18180 experiments_eval/run_stage_b_baseline.sh
STAGE_B_SEVERITIES="mod severe" NUM_EVAL_RUNS=1 experiments_eval/run_stage_b_conditions.sh
TARGET_RPS=500 experiments_eval/run_stage_b_conditions.sh
SLEEP_BETWEEN_RUNS=10 experiments_eval/run_stage_b_conditions.sh
```
