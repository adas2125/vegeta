package prom

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// Metrics encapsulates Prometheus metrics of an attack.
type Metrics struct {
	requestLatencyHistogram *prometheus.HistogramVec
	requestBytesInCounter   *prometheus.CounterVec
	requestBytesOutCounter  *prometheus.CounterVec
	requestFailCounter      *prometheus.CounterVec
}

// NewMetrics returns a new Metrics instance that must be
// registered in a Prometheus registry with Register.
func NewMetrics() *Metrics {
	baseLabels := []string{"method", "url", "status"}
	return &Metrics{
		requestLatencyHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "request_seconds",
			Help:    "Request latency",
			Buckets: prometheus.DefBuckets,
		}, baseLabels),
		requestBytesInCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "request_bytes_in",
			Help: "Bytes received from servers as response to requests",
		}, baseLabels),
		requestBytesOutCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "request_bytes_out",
			Help: "Bytes sent to servers during requests",
		}, baseLabels),
		requestFailCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "request_fail_count",
			Help: "Count of failed requests",
		}, append(baseLabels[:len(baseLabels):len(baseLabels)], "message")),
	}
}

// Register registers all Prometheus metrics in r.
func (pm *Metrics) Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{
		pm.requestLatencyHistogram,
		pm.requestBytesInCounter,
		pm.requestBytesOutCounter,
		pm.requestFailCounter,
	} {
		if err := r.Register(c); err != nil {
			return fmt.Errorf("failed to register metric %v: %w", c, err)
		}
	}
	return nil
}

// Observe metrics given a vegeta.Result.
func (pm *Metrics) Observe(res *vegeta.Result) {
	code := strconv.FormatUint(uint64(res.Code), 10)
	pm.requestBytesInCounter.WithLabelValues(res.Method, res.URL, code).Add(float64(res.BytesIn))
	pm.requestBytesOutCounter.WithLabelValues(res.Method, res.URL, code).Add(float64(res.BytesOut))
	pm.requestLatencyHistogram.WithLabelValues(res.Method, res.URL, code).Observe(res.Latency.Seconds())
	if res.Error != "" {
		pm.requestFailCounter.WithLabelValues(res.Method, res.URL, code, res.Error)
	}
}

// NewHandler returns a new http.Handler that exposes Prometheus
// metrics registed in r in the OpenMetrics format.
func NewHandler(r *prometheus.Registry, startTime time.Time) http.Handler {
	return promhttp.HandlerFor(r, promhttp.HandlerOpts{
		Registry:          r,
		EnableOpenMetrics: true,
		ProcessStartTime:  startTime,
	})
}

// DiagnosticMetrics exposes runtime counters and per-window delay metrics from
// the self-diagnosis pipeline as Prometheus Gauges.
type DiagnosticMetrics struct {
	// runtime state — updated every metricsInterval
	Workers     prometheus.Gauge
	Connections prometheus.Gauge
	InFlight    prometheus.Gauge
	Completions prometheus.Gauge // monotonically increasing within a run; exposed as Gauge so Set() works

	// per-window aggregates — updated at every window flush
	AchievedRate          prometheus.Gauge
	SchedulerDelayMs      prometheus.Gauge
	FireToDispatchDelayMs prometheus.Gauge
	DispatchDelayMs       prometheus.Gauge
	ConnDelayMs           prometheus.Gauge
	WriteDelayMs          prometheus.Gauge
	FirstByteRTTMs        prometheus.Gauge
	FirstByteDelayMs      prometheus.Gauge
	ResponseTailTimeMs    prometheus.Gauge
	TotalLatencyMs        prometheus.Gauge
	AvgInFlight           prometheus.Gauge
	ObservedR             prometheus.Gauge
	LLViolation           prometheus.Gauge
}

// NewDiagnosticMetrics creates a DiagnosticMetrics instance. Call Register to
// add it to a Prometheus registry before use.
func NewDiagnosticMetrics() *DiagnosticMetrics {
	return &DiagnosticMetrics{
		Workers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_workers",
			Help: "Current number of worker goroutines",
		}),
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_active_connections",
			Help: "Number of open TCP connections",
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_in_flight",
			Help: "Number of requests currently in flight (dispatched but not yet completed)",
		}),
		Completions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_completions_total",
			Help: "Total number of completed requests since the attack started",
		}),
		AchievedRate: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_achieved_rate",
			Help: "Valid completed requests per second in the current window",
		}),
		SchedulerDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_scheduler_delay_ms",
			Help: "Average scheduler delay in the current window (ms) — time between scheduled fire and worker wake-up; indicates client CPU/OS jitter",
		}),
		FireToDispatchDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_fire_to_dispatch_delay_ms",
			Help: "Average delay from scheduled fire time to HTTP request dispatch in the current window (ms)",
		}),
		DispatchDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_dispatch_delay_ms",
			Help: "Average dispatch delay in the current window (ms) — time between worker wake-up and HTTP request dispatch",
		}),
		ConnDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_conn_delay_ms",
			Help: "Average connection acquisition delay in the current window (ms) — time to obtain a TCP connection",
		}),
		WriteDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_write_delay_ms",
			Help: "Average request write delay in the current window (ms) — time to write request bytes after getting connection",
		}),
		FirstByteRTTMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_first_byte_rtt_ms",
			Help: "Average time from request write completion to first response byte in the current window (ms) — server processing + network RTT",
		}),
		FirstByteDelayMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_first_byte_delay_ms",
			Help: "Average time from request dispatch to first response byte in the current window (ms)",
		}),
		ResponseTailTimeMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_response_tail_time_ms",
			Help: "Average time from first response byte to request completion in the current window (ms)",
		}),
		TotalLatencyMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_total_latency_ms",
			Help: "Average total request latency in the current window (ms) — worker wake-up to response complete",
		}),
		AvgInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_avg_in_flight",
			Help: "Average number of in-flight requests sampled across the current window",
		}),
		ObservedR: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_observed_r",
			Help: "Little's Law R = AvgInFlight / (targetRPS * baselineLatency); should be ~1.0 in healthy open-loop",
		}),
		LLViolation: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vegeta_window_ll_violation",
			Help: "1 if the current window violates the Little's Law bounds, 0 otherwise",
		}),
	}
}

// Register adds all DiagnosticMetrics collectors to the given Prometheus registry.
func (dm *DiagnosticMetrics) Register(r prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		dm.Workers, dm.Connections, dm.InFlight, dm.Completions,
		dm.AchievedRate,
		dm.SchedulerDelayMs,
		dm.FireToDispatchDelayMs,
		dm.DispatchDelayMs,
		dm.ConnDelayMs,
		dm.WriteDelayMs,
		dm.FirstByteRTTMs,
		dm.FirstByteDelayMs,
		dm.ResponseTailTimeMs,
		dm.TotalLatencyMs,
		dm.AvgInFlight,
		dm.ObservedR,
		dm.LLViolation,
	}
	for _, c := range collectors {
		if err := r.Register(c); err != nil {
			return fmt.Errorf("failed to register diagnostic metric %v: %w", c, err)
		}
	}
	return nil
}

// ObserveRuntime updates the runtime Gauges from an attacker snapshot.
func (dm *DiagnosticMetrics) ObserveRuntime(workers, connections, inFlight, completions uint64) {
	dm.Workers.Set(float64(workers))
	dm.Connections.Set(float64(connections))
	dm.InFlight.Set(float64(inFlight))
	dm.Completions.Set(float64(completions))
}

// ObserveWindow updates all per-window Gauges from a completed window summary.
// Pass math.NaN() for observedR when no baseline reference is available; the
// gauge will be set to -1 in that case to distinguish from a true zero.
func (dm *DiagnosticMetrics) ObserveWindow(
	achievedRate float64,
	schedulerMs, fireToDispatchMs, dispatchMs, connMs, writeMs float64,
	fbRTTMs, fbDelayMs, responseTailMs, totalLatMs float64,
	avgInFlight, observedR float64,
	llViolation bool,
) {
	dm.AchievedRate.Set(achievedRate)
	dm.SchedulerDelayMs.Set(schedulerMs)
	dm.FireToDispatchDelayMs.Set(fireToDispatchMs)
	dm.DispatchDelayMs.Set(dispatchMs)
	dm.ConnDelayMs.Set(connMs)
	dm.WriteDelayMs.Set(writeMs)
	dm.FirstByteRTTMs.Set(fbRTTMs)
	dm.FirstByteDelayMs.Set(fbDelayMs)
	dm.ResponseTailTimeMs.Set(responseTailMs)
	dm.TotalLatencyMs.Set(totalLatMs)
	dm.AvgInFlight.Set(avgInFlight)

	if observedR != observedR { // NaN check
		dm.ObservedR.Set(-1)
	} else {
		dm.ObservedR.Set(observedR)
	}

	if llViolation {
		dm.LLViolation.Set(1)
	} else {
		dm.LLViolation.Set(0)
	}
}
