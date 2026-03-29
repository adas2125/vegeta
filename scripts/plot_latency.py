import json
import matplotlib.pyplot as plt
from datetime import datetime

rps_values = [1000, 2000, 4000, 8000]

plt.figure()

for rps in rps_values:
    times = []
    latencies = []

    with open(f"./results_{rps}_full.json") as f:
        for line in f:
            data = json.loads(line)
            
            # Vegeta timestamp is an RFC3339 string
            t = datetime.fromisoformat(data["timestamp"]).timestamp()
            latency = data["latency"] / 1e9  # convert to seconds
            
            times.append(t)
            latencies.append(latency)

    # Normalize time (start at 0)
    t0 = times[0]
    times = [t - t0 for t in times]

    plt.plot(times, latencies, label=f"{rps} RPS")
    print(sum(latencies) / len(latencies))

plt.xlabel("Time (seconds)")
plt.ylabel("Latency (s)")
plt.title("Latency vs Time at Different RPS")
plt.legend()
plt.grid()

plt.show()