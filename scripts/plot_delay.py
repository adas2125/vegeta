import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------
# Load data
# ---------------------------
results_normal = pd.read_csv("results_3000rps.csv")
results_abnormal = pd.read_csv("results_30000rps.csv")

# Convert elapsed time to seconds
results_normal["t"] = results_normal["elapsed_ms"] / 1000
results_abnormal["t"] = results_abnormal["elapsed_ms"] / 1000

results_normal = results_normal.sort_values("t")
results_abnormal = results_abnormal.sort_values("t")

print("Max abnormal time:", results_abnormal["t"].max())

# ---------------------------
# Plot helper
# ---------------------------
def plot_metric(column, ylabel):

    fig, axes = plt.subplots(2, 1, figsize=(10,6), sharex=True)

    # Normal load
    axes[0].plot(results_normal["t"], results_normal[column])
    axes[0].set_ylabel(ylabel)
    axes[0].set_title(f"{column} (Normal Load)")
    axes[0].grid(True)

    # Abnormal load
    axes[1].plot(results_abnormal["t"], results_abnormal[column])
    axes[1].set_ylabel(ylabel)
    axes[1].set_title(f"{column} (High Load)")
    axes[1].set_xlabel("Time (s)")
    axes[1].grid(True)

    plt.tight_layout()
    plt.show()

# ---------------------------
# Core LG behavior
# ---------------------------
plot_metric("workers", "Workers")
plot_metric("connections", "Connections")
plot_metric("in_flight", "In-flight Requests")
plot_metric("completions", "Completed Requests")

# ---------------------------
# Scheduling behavior
# ---------------------------
plot_metric("send_delay_ms", "Send Delay (ms)")
plot_metric("conn_delay_ms", "Connection Delay (ms)")
plot_metric("write_delay_ms", "Write Delay (ms)")

# ---------------------------
# Latency breakdown
# ---------------------------
plot_metric("first_byte_rtt_ms", "First Byte RTT (ms)")
plot_metric("first_byte_delay_ms", "First Byte Delay (ms)")
plot_metric("total_latency_ms", "Total Latency (ms)")