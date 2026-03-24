import matplotlib.pyplot as plt
import pandas as pd

HTTP1_CSV = "http1.csv"
HTTP2_CSV = "http2.csv"

http1_df = pd.read_csv(HTTP1_CSV)
http2_df = pd.read_csv(HTTP2_CSV)

t1 = http1_df["elapsed_ms"] / 1000.0
t2 = http2_df["elapsed_ms"] / 1000.0

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# --- Workers subplot ---
ax1.plot(t1, http1_df["workers"], label="HTTP/1 Workers", linewidth=1.5)
ax1.plot(t2, http2_df["workers"], label="HTTP/2 Workers", linewidth=1.5)

ax1.set_ylabel("Workers")
ax1.set_title("Workers Over Time")
ax1.grid(True, alpha=0.3)
ax1.legend()

# --- Completions subplot ---
ax2.plot(t1, http1_df["completions"], label="HTTP/1 Completions", linewidth=1.5)
ax2.plot(t2, http2_df["completions"], label="HTTP/2 Completions", linewidth=1.5)

ax2.set_ylabel("Requests Completed")
ax2.set_xlabel("Elapsed time (s)")
ax2.set_title("Requests Completed Over Time")
ax2.grid(True, alpha=0.3)
ax2.legend()

plt.tight_layout()
plt.show()