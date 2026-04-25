package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsenart/vegeta/v12/internal/resolver"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	prom "github.com/tsenart/vegeta/v12/lib/prom"
)

// AnomalyPayload is the struct of the payload emitted at the end of each window
type AnomalyPayload struct {
	Rho              float64   `json:"rho"`
	WindowStart      int       `json:"window_start"`
	MaxWorkers       uint64    `json:"MaxWorkers"`
	AvgInFlight      float64   `json:"AvgInFlight"`
	PacerDelays      []float64 `json:"PacerDelays"`
	SchedulerDelays  []float64 `json:"SchedulerDelays"`
	ConnectionDelays []float64 `json:"ConnectionDelays"`
}

// emits the anomaly payload to a channel
var anomalyPayloadCh = make(chan AnomalyPayload, 128)

func init() {
	go func() {
		for payload := range anomalyPayloadCh {

			// reads payload from the channel, marshal to JSON, emit to stdout
			body, err := json.Marshal(payload)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to marshal anomaly payload: %v\n", err)
				continue
			}
			fmt.Fprintf(os.Stdout, "XLG-WINDOW:%s\n", body)
		}
	}()
}

func emitAnomalyPayload(payload AnomalyPayload) {
	// non-blocking send to the channel
	select {
	case anomalyPayloadCh <- payload:
	default:
		// if the channel is full, we drop the payload to avoid blocking the attack processing
	}
}

func cloneFloat64s(values []float64) []float64 {
	if len(values) == 0 {
		return nil
	}

	// safe copy of slices
	cloned := make([]float64, len(values))
	copy(cloned, values)
	return cloned
}

func emitWindowAnomalyPayload(window *WindowStats, summary windowSummary, maxWorkers uint64) {

	// obtain the rho value from the window summary
	rho := summary.ObservedR

	// if it's not defined, set to -1
	if math.IsNaN(rho) || math.IsInf(rho, 0) {
		rho = -1
	}

	// emit the payload
	emitAnomalyPayload(AnomalyPayload{
		Rho:              rho,
		WindowStart:      int(window.Start.UnixMilli()),
		MaxWorkers:       maxWorkers,
		AvgInFlight:      summary.AvgInFlight,
		PacerDelays:      cloneFloat64s(window.PacerWaitSamples),
		SchedulerDelays:  cloneFloat64s(window.SchedulerDelaySamples),
		ConnectionDelays: cloneFloat64s(window.ConnDelaySamples),
	})
}

func attackCmd() command {
	fs := flag.NewFlagSet("vegeta attack", flag.ExitOnError)
	opts := &attackOpts{
		headers:      headers{http.Header{}},
		proxyHeaders: headers{http.Header{}},
		laddr:        localAddr{&vegeta.DefaultLocalAddr},
		rate:         vegeta.Rate{Freq: 50, Per: time.Second},
		maxBody:      vegeta.DefaultMaxBody,
		promAddr:     "0.0.0.0:8880",
	}
	fs.StringVar(&opts.name, "name", "", "Attack name")
	fs.StringVar(&opts.targetsf, "targets", "stdin", "Targets file")
	fs.StringVar(&opts.format, "format", vegeta.HTTPTargetFormat,
		fmt.Sprintf("Targets format [%s]", strings.Join(vegeta.TargetFormats, ", ")))
	fs.StringVar(&opts.outputf, "output", "stdout", "Output file")
	fs.StringVar(&opts.bodyf, "body", "", "Requests body file")
	fs.BoolVar(&opts.chunked, "chunked", false, "Send body with chunked transfer encoding")
	fs.StringVar(&opts.certf, "cert", "", "TLS client PEM encoded certificate file")
	fs.StringVar(&opts.keyf, "key", "", "TLS client PEM encoded private key file")
	fs.Var(&opts.rootCerts, "root-certs", "TLS root certificate files (comma separated list)")
	fs.BoolVar(&opts.http2, "http2", true, "Send HTTP/2 requests when supported by the server")
	fs.BoolVar(&opts.h2c, "h2c", false, "Send HTTP/2 requests without TLS encryption")
	fs.BoolVar(&opts.insecure, "insecure", false, "Ignore invalid server TLS certificates")
	fs.BoolVar(&opts.lazy, "lazy", false, "Read targets lazily")
	fs.DurationVar(&opts.duration, "duration", 0, "Duration of the test [0 = forever]")
	fs.DurationVar(&opts.timeout, "timeout", vegeta.DefaultTimeout, "Requests timeout")
	fs.Uint64Var(&opts.workers, "workers", vegeta.DefaultWorkers, "Initial number of workers")
	fs.Uint64Var(&opts.maxWorkers, "max-workers", vegeta.DefaultMaxWorkers, "Maximum number of workers")
	fs.IntVar(&opts.connections, "connections", vegeta.DefaultConnections, "Max open idle connections per target host")
	fs.IntVar(&opts.maxConnections, "max-connections", vegeta.DefaultMaxConnections, "Max connections per target host")
	fs.IntVar(&opts.redirects, "redirects", vegeta.DefaultRedirects, "Number of redirects to follow. -1 will not follow but marks as success")
	fs.Var(&maxBodyFlag{&opts.maxBody}, "max-body", "Maximum number of bytes to capture from response bodies. [-1 = no limit]")
	fs.Var(&rateFlag{&opts.rate}, "rate", "Number of requests per time unit [0 = infinity]")
	fs.Var(&opts.headers, "header", "Request header")
	fs.Var(&opts.proxyHeaders, "proxy-header", "Proxy CONNECT header")
	fs.Var(&opts.laddr, "laddr", "Local IP address")
	fs.BoolVar(&opts.keepalive, "keepalive", true, "Use persistent connections")
	fs.StringVar(&opts.unixSocket, "unix-socket", "", "Connect over a unix socket. This overrides the host address in target URLs")
	fs.StringVar(&opts.promAddr, "prometheus-addr", "", "Prometheus exporter listen address [empty = disabled]. Example: 0.0.0.0:8880")
	fs.Var(&dnsTTLFlag{&opts.dnsTTL}, "dns-ttl", "Cache DNS lookups for the given duration [-1 = disabled, 0 = forever]")
	fs.BoolVar(&opts.sessionTickets, "session-tickets", false, "Enable TLS session resumption using session tickets")
	fs.Var(&connectToFlag{&opts.connectTo}, "connect-to", "A mapping of (ip|host):port to use instead of a target URL's (ip|host):port. Can be repeated multiple times.\nIdentical src:port with different dst:port will round-robin over the different dst:port pairs.\nExample: google.com:80:localhost:6060")
	// custom added flags
	fs.StringVar(&opts.metricsCSV, "metrics-csv", "results.csv", "CSV file path for runtime attack metrics (e.g. workers, connections, in-flight, completions over time)")
	fs.StringVar(&opts.windowCSV, "window-csv", "window_results.csv", "CSV file path for windowed trace metrics including delay & latency metrics, achieved rate, observed R, and Little's Law violation flag computed for each window")
	fs.StringVar(&opts.windowSamplesCSV, "window-samples-csv", "", "CSV file path for windowed trace metric samples used to plot distributions [empty = disabled]")
	fs.StringVar(&opts.referenceCSVPath, "reference-csv-path", "", "CSV file path for computed baseline latency for R metric computation")
	fs.BoolVar(&opts.xlgInspector, "xlg-inspector", true, "Emit XLG-WINDOW telemetry to stdout for the XLG Inspector")
	fs.DurationVar(&opts.metricsInterval, "metrics-interval", time.Second, "Sampling interval for runtime metrics CSV")
	fs.DurationVar(&opts.sampleInterval, "sample-interval", 10*time.Millisecond, "Sampling interval for windowed trace metrics (in-flight), must be less than or equal to metrics-interval")
	fs.Float64Var(&opts.windowSampleRetention, "window-sample-retention", 1.0, "Fraction of request trace samples to retain for per-window distribution outputs [0.0-1.0]")
	systemSpecificFlags(fs, opts)

	return command{fs, func(args []string) error {
		fs.Parse(args)
		return attack(opts)
	}}
}

var (
	errZeroRate = errors.New("rate frequency and time unit must be bigger than zero")
	errBadCert  = errors.New("bad certificate")
)

// attackOpts aggregates the attack function command options
type attackOpts struct {
	name                  string
	targetsf              string
	format                string
	outputf               string
	bodyf                 string
	certf                 string
	keyf                  string
	rootCerts             csl
	http2                 bool
	h2c                   bool
	insecure              bool
	lazy                  bool
	chunked               bool
	duration              time.Duration
	timeout               time.Duration
	rate                  vegeta.Rate
	workers               uint64
	maxWorkers            uint64
	connections           int
	maxConnections        int
	redirects             int
	maxBody               int64
	headers               headers
	proxyHeaders          headers
	laddr                 localAddr
	keepalive             bool
	resolvers             csl
	unixSocket            string
	promAddr              string
	metricsCSV            string
	windowCSV             string
	windowSamplesCSV      string
	metricsInterval       time.Duration
	sampleInterval        time.Duration
	windowSampleRetention float64
	referenceCSVPath      string
	xlgInspector          bool
	dnsTTL                time.Duration
	sessionTickets        bool
	connectTo             map[string][]string
}

type WindowStats struct {
	Start time.Time
	End   time.Time

	// valid counts for each metric
	PacerWaitValidCount      int
	SchedulerDelayValidCount int
	DispatchDelayValidCount  int
	ConnDelayValidCount      int
	WriteDelayValidCount     int
	FirstByteRTTValidCount   int
	FirstByteDelayValidCount int
	ResponseTailValidCount   int
	TotalLatencyValidCount   int

	SumPacerWait       time.Duration
	SumSchedulerDelay  time.Duration
	SumDispatchDelay   time.Duration
	SumConnDelay       time.Duration
	SumWriteDelay      time.Duration
	SumFirstByteRTT    time.Duration
	SumFirstByteDelay  time.Duration
	SumResponseTail    time.Duration
	SumTotalLatency    time.Duration
	SumInFlightSamples float64
	NumInFlightSamples int64

	// to plot distributions
	PacerWaitSamples      []float64
	SchedulerDelaySamples []float64
	DispatchDelaySamples  []float64
	ConnDelaySamples      []float64
	WriteDelaySamples     []float64
	FirstByteRTTSamples   []float64
	FirstByteDelaySamples []float64
	ResponseTailSamples   []float64
	TotalLatencySamples   []float64

	// connection-state counts
	GotConnCount           int
	ReusedConnCount        int
	FreshConnCount         int
	ReusedIdleConnCount    int
	ConnIdleTimeValidCount int
	SumConnIdleTime        time.Duration
	ConnIdleTimeSamples    []float64
}

type windowSummary struct {
	// diagnostic counts for each metric
	PacerWaitCount      int
	SchedulerDelayCount int
	DispatchDelayCount  int
	ConnDelayCount      int
	WriteDelayCount     int
	FirstByteRTTCount   int
	FirstByteDelayCount int
	ResponseTailCount   int
	TotalLatencyCount   int

	// connection-state counts
	GotConnCount        int
	ReusedConnCount     int
	FreshConnCount      int
	ReusedIdleConnCount int
	ConnIdleTimeCount   int
	AvgConnIdleTime     time.Duration
	ReuseFrac           float64
	FreshConnFrac       float64
	WasIdleGivenReused  float64

	// averages
	Duration          time.Duration
	AchievedRate      float64
	AvgPacerWait      time.Duration
	AvgSchedulerDelay time.Duration
	AvgDispatchDelay  time.Duration
	AvgConnDelay      time.Duration
	AvgWriteDelay     time.Duration
	AvgFirstByteRTT   time.Duration
	AvgFirstByteDelay time.Duration
	AvgResponseTail   time.Duration
	AvgTotalLatency   time.Duration
	AvgInFlight       float64

	// for violations
	ObservedR          float64
	LittleLawViolation bool
}

type windowSamplesCSVWriter struct {
	file *os.File
	csv  *csv.Writer
}

type littleLawReference struct {
	targetRPS       float64
	baselineLatency time.Duration
}

// attack validates the attack arguments, sets up the
// required resources, launches the attack and writes the results
func attack(opts *attackOpts) (err error) {
	if opts.maxWorkers == vegeta.DefaultMaxWorkers && opts.rate.Freq == 0 {
		return fmt.Errorf("-rate=0 requires setting -max-workers")
	}
	if opts.windowSampleRetention < 0 || opts.windowSampleRetention > 1 {
		return fmt.Errorf("-window-sample-retention must be between 0.0 and 1.0")
	}

	if len(opts.resolvers) > 0 {
		res, err := resolver.NewResolver(opts.resolvers)
		if err != nil {
			return err
		}
		net.DefaultResolver = res
	}

	net.DefaultResolver.PreferGo = true

	files := map[string]io.Reader{}
	for _, filename := range []string{opts.targetsf, opts.bodyf} {
		if filename == "" {
			continue
		}
		f, err := file(filename, false)
		if err != nil {
			return fmt.Errorf("error opening %s: %s", filename, err)
		}
		defer f.Close()
		files[filename] = f
	}

	var body []byte
	if bodyf, ok := files[opts.bodyf]; ok {
		if body, err = io.ReadAll(bodyf); err != nil {
			return fmt.Errorf("error reading %s: %s", opts.bodyf, err)
		}
	}

	var (
		tr       vegeta.Targeter
		src      = files[opts.targetsf]
		hdr      = opts.headers.Header
		proxyHdr = opts.proxyHeaders.Header
	)

	switch opts.format {
	case vegeta.JSONTargetFormat:
		tr = vegeta.NewJSONTargeter(src, body, hdr)
	case vegeta.HTTPTargetFormat:
		tr = vegeta.NewHTTPTargeter(src, body, hdr)
	default:
		return fmt.Errorf("format %q isn't one of [%s]",
			opts.format, strings.Join(vegeta.TargetFormats, ", "))
	}

	if !opts.lazy {
		targets, err := vegeta.ReadAllTargets(tr)
		if err != nil {
			return err
		}
		tr = vegeta.NewStaticTargeter(targets...)
	}

	out, err := file(opts.outputf, true)
	if err != nil {
		return fmt.Errorf("error opening %s: %s", opts.outputf, err)
	}
	defer out.Close()

	tlsc, err := tlsConfig(opts.insecure, opts.certf, opts.keyf, opts.rootCerts)
	if err != nil {
		return err
	}

	var pm *prom.Metrics
	var dm *prom.DiagnosticMetrics
	if opts.promAddr != "" {
		pm = prom.NewMetrics()
		dm = prom.NewDiagnosticMetrics()

		r := prometheus.NewRegistry()
		if err := pm.Register(r); err != nil {
			return fmt.Errorf("error registering prometheus metrics: %s", err)
		}
		if err := dm.Register(r); err != nil {
			return fmt.Errorf("error registering diagnostic metrics: %s", err)
		}

		srv := http.Server{
			Addr:    opts.promAddr,
			Handler: prom.NewHandler(r, time.Now().UTC()),
		}

		defer srv.Close()
		go srv.ListenAndServe()
	}

	targetRPS := targetRatePerSecond(opts.rate)

	// If a reference CSV is provided, use the average window latency from that
	// file as the baseline latency for ObservedR calculations.
	var llRef *littleLawReference
	if opts.referenceCSVPath != "" && targetRPS > 0 {
		baselineLatency, err := loadAverageLatencyFromReferenceCSV(opts.referenceCSVPath)
		if err != nil {
			return err
		}

		// creating a llref, which can be used for the LL check
		if llRef == nil {
			llRef = &littleLawReference{targetRPS: targetRPS}
		}
		llRef.baselineLatency = baselineLatency
	}

	atk := vegeta.NewAttacker(
		vegeta.Redirects(opts.redirects),
		vegeta.Timeout(opts.timeout),
		vegeta.LocalAddr(*opts.laddr.IPAddr),
		vegeta.TLSConfig(tlsc),
		vegeta.Workers(opts.workers),
		vegeta.MaxWorkers(opts.maxWorkers),
		vegeta.KeepAlive(opts.keepalive),
		vegeta.Connections(opts.connections),
		vegeta.MaxConnections(opts.maxConnections),
		vegeta.HTTP2(opts.http2),
		vegeta.H2C(opts.h2c),
		vegeta.MaxBody(opts.maxBody),
		vegeta.UnixSocket(opts.unixSocket),
		vegeta.ProxyHeader(proxyHdr),
		vegeta.ChunkedBody(opts.chunked),
		vegeta.DNSCaching(opts.dnsTTL),
		vegeta.ConnectTo(opts.connectTo),
		vegeta.SessionTickets(opts.sessionTickets),
	)

	// setting up the CSV writers for runtime metrics and windowed trace metrics
	var mw *metricsCSVWriter
	if opts.metricsCSV != "" {
		mw, err = newMetricsCSVWriter(opts.metricsCSV)
		if err != nil {
			return fmt.Errorf("error creating %s: %s", opts.metricsCSV, err)
		}
		defer mw.Close()
	}

	var sw *windowSamplesCSVWriter
	if opts.windowSamplesCSV != "" {
		sw, err = newWindowSamplesCSVWriter(opts.windowSamplesCSV)
		if err != nil {
			return fmt.Errorf("error creating %s: %s", opts.windowSamplesCSV, err)
		}
		defer sw.Close()
	}

	var ww *windowCSVWriter
	if opts.windowCSV != "" {
		ww, err = newWindowCSVWriter(opts.windowCSV)
		if err != nil {
			return fmt.Errorf("error creating %s: %s", opts.windowCSV, err)
		}
		defer ww.Close()
	}

	res := atk.Attack(tr, opts.rate, opts.duration, opts.name)
	traces := atk.TraceRecords()
	enc := vegeta.NewEncoder(out)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	return processAttack(atk, res, enc, sig, pm, dm, mw, sw, ww, llRef, opts.metricsInterval, opts.sampleInterval, opts.windowSampleRetention, opts.xlgInspector, opts.maxWorkers, traces)
}

func hasAnyValidSamples(window *WindowStats) bool {
	return window.TotalLatencyValidCount > 0 ||
		window.SchedulerDelayValidCount > 0 ||
		window.DispatchDelayValidCount > 0 ||
		window.ConnDelayValidCount > 0 ||
		window.WriteDelayValidCount > 0 ||
		window.FirstByteRTTValidCount > 0 ||
		window.FirstByteDelayValidCount > 0 ||
		window.ResponseTailValidCount > 0
}

func processAttack(
	atk *vegeta.Attacker,
	res <-chan *vegeta.Result,
	enc vegeta.Encoder,
	sig <-chan os.Signal,
	pm *prom.Metrics,
	dm *prom.DiagnosticMetrics,
	mw *metricsCSVWriter,
	sw *windowSamplesCSVWriter,
	ww *windowCSVWriter,
	llRef *littleLawReference,
	metricsInterval time.Duration,
	sampleInterval time.Duration,
	windowSampleRetention float64,
	xlgInspector bool,
	maxWorkers uint64,
	traces <-chan *vegeta.RequestRecord,
) error {

	// creating tickers for runtime metrics and windowed trace metrics
	if metricsInterval <= 0 {
		metricsInterval = time.Second
	}
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	if sampleInterval <= 0 {
		sampleInterval = 100 * time.Millisecond
	}
	sampleTicker := time.NewTicker(sampleInterval)
	defer sampleTicker.Stop()

	// initializing the window stats for the first interval
	window := &WindowStats{Start: time.Now()}
	sampleRNG := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-sig:
			if stopSent := atk.Stop(); !stopSent {
				// Exit immediately on second signal.
				return nil
			}
		case r, ok := <-res:
			if !ok {
				// write the final runtime metrics to the CSV file if provided
				if mw != nil {
					if err := mw.Write(atk.RuntimeMetrics()); err != nil {
						return err
					}
				}
				res = nil
				if res == nil && traces == nil {
					// write the final windowed trace metrics to the CSV file if provided
					window.End = time.Now()
					// compute the average metrics for the final window
					summary := window.Summary()
					applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last ticker tick
					applyLittleLawCheck(&summary, llRef)

					hasAnySamples := hasAnyValidSamples(window)
					if !hasAnySamples {
						return nil
					}

					// on a window tick, report the anomaly payload for the window
					if xlgInspector {
						emitWindowAnomalyPayload(window, summary, maxWorkers)
					}

					// write the window samples to the CSV file if provided
					if sw != nil {
						if err := sw.WriteWindowSamples(window); err != nil {
							return err
						}
					}

					// write the final windowed trace metrics to the CSV file if provided
					if ww != nil {
						if err := ww.Write(window, summary); err != nil {
							return err
						}
					}
					return nil
				}
				// continue to process any remaining traces as traces may still be coming
				continue
			}

			if pm != nil {
				pm.Observe(r)
			}

			if err := enc.Encode(r); err != nil {
				return err
			}

		case rec, ok := <-traces:
			if !ok {
				traces = nil
				if res == nil && traces == nil {
					// write the final windowed trace metrics to the CSV file if provided
					window.End = time.Now()
					summary := window.Summary()
					applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last ticker tick
					applyLittleLawCheck(&summary, llRef)

					hasAnySamples := hasAnyValidSamples(window)
					if !hasAnySamples {
						return nil
					}

					// to ensure no data loss at the end, emit for final window
					if xlgInspector {
						emitWindowAnomalyPayload(window, summary, maxWorkers)
					}

					// write the window samples to the CSV file if provided
					if sw != nil {
						if err := sw.WriteWindowSamples(window); err != nil {
							return err
						}
					}

					if ww != nil {
						if err := ww.Write(window, summary); err != nil {
							return err
						}
					}
					return nil
				}
				continue // continue to process any remaining results as results may still be coming
			}

			// update the window stats on receipt of a valid record
			retainSamples := sampleRNG.Float64() < windowSampleRetention

			if rec.PacerWaitValid {
				window.PacerWaitValidCount++
				window.SumPacerWait += rec.PacerWait
				if retainSamples {
					window.PacerWaitSamples = append(window.PacerWaitSamples, float64(rec.PacerWait)/float64(time.Millisecond))
				}
			}
			if rec.SchedulerDelayValid {
				window.SchedulerDelayValidCount++
				window.SumSchedulerDelay += rec.SchedulerDelay
				if retainSamples {
					window.SchedulerDelaySamples = append(window.SchedulerDelaySamples, float64(rec.SchedulerDelay)/float64(time.Millisecond))
				}
			}
			if rec.DispatchDelayValid {
				window.DispatchDelayValidCount++
				window.SumDispatchDelay += rec.DispatchDelay
				if retainSamples {
					window.DispatchDelaySamples = append(window.DispatchDelaySamples, float64(rec.DispatchDelay)/float64(time.Millisecond))
				}
			}
			if rec.ConnDelayValid {
				window.ConnDelayValidCount++
				window.SumConnDelay += rec.ConnDelay
				if retainSamples {
					window.ConnDelaySamples = append(window.ConnDelaySamples, float64(rec.ConnDelay)/float64(time.Millisecond))
				}
			}
			if rec.WriteDelayValid {
				window.WriteDelayValidCount++
				window.SumWriteDelay += rec.WriteDelay
				if retainSamples {
					window.WriteDelaySamples = append(window.WriteDelaySamples, float64(rec.WriteDelay)/float64(time.Millisecond))
				}
			}
			if rec.FirstByteRTTValid {
				window.FirstByteRTTValidCount++
				window.SumFirstByteRTT += rec.FirstByteRTT
				if retainSamples {
					window.FirstByteRTTSamples = append(window.FirstByteRTTSamples, float64(rec.FirstByteRTT)/float64(time.Millisecond))
				}
			}
			if rec.FirstByteDelayValid {
				window.FirstByteDelayValidCount++
				window.SumFirstByteDelay += rec.FirstByteDelay
				if retainSamples {
					window.FirstByteDelaySamples = append(window.FirstByteDelaySamples, float64(rec.FirstByteDelay)/float64(time.Millisecond))
				}
			}
			if rec.ResponseTailValid {
				window.ResponseTailValidCount++
				window.SumResponseTail += rec.ResponseTailTime
				if retainSamples {
					window.ResponseTailSamples = append(window.ResponseTailSamples, float64(rec.ResponseTailTime)/float64(time.Millisecond))
				}
			}
			if rec.TotalLatencyValid {
				window.TotalLatencyValidCount++
				window.SumTotalLatency += rec.TotalLatency
				if retainSamples {
					window.TotalLatencySamples = append(window.TotalLatencySamples, float64(rec.TotalLatency)/float64(time.Millisecond))
				}
			}

			// updating connection-state counts
			if rec.GotConnValid {
				window.GotConnCount++

				if rec.ConnReused {
					window.ReusedConnCount++
				} else {
					window.FreshConnCount++
				}

				if rec.ConnReused && rec.ConnWasIdle {
					window.ReusedIdleConnCount++
				}

				if rec.ConnIdleTimeValid {
					window.ConnIdleTimeValidCount++
					window.SumConnIdleTime += rec.ConnIdleTime
					if retainSamples {
						window.ConnIdleTimeSamples = append(
							window.ConnIdleTimeSamples,
							float64(rec.ConnIdleTime)/float64(time.Millisecond),
						)
					}
				}
			}

		case <-sampleTicker.C:
			// sample the current runtime metrics and update the window stats
			metrics := atk.RuntimeMetrics()
			inFlight := float64(metrics.InFlight)
			window.SumInFlightSamples += inFlight
			window.NumInFlightSamples++

		case <-ticker.C:
			// whenever the ticker fires, snapshot runtime metrics and write to CSV / Prometheus
			rm := atk.RuntimeMetrics()
			if mw != nil {
				if err := mw.Write(rm); err != nil {
					return err
				}
			}
			if dm != nil {
				dm.ObserveRuntime(rm.Workers, rm.Connections, rm.InFlight, rm.Completions)
			}

			// close the current window and compute the average metrics for the window on receipt of the ticker signal
			window.End = time.Now()

			// compute the average metrics for the window
			summary := window.Summary()
			applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last sample ticker tick
			applyLittleLawCheck(&summary, llRef)

			hasAnySamples := hasAnyValidSamples(window)
			if !hasAnySamples {
				// reset the window stats for the next interval
				window = &WindowStats{Start: time.Now()}
				continue
			}

			if xlgInspector {
				emitWindowAnomalyPayload(window, summary, maxWorkers)
			}

			// write the window samples to the CSV file if provided
			if sw != nil {
				if err := sw.WriteWindowSamples(window); err != nil {
					return err
				}
			}

			if ww != nil {
				if err := ww.Write(window, summary); err != nil {
					return err
				}
				if dm != nil {
					dm.ObserveWindow(
						summary.AchievedRate,
						msFloat(summary.AvgSchedulerDelay),
						msFloat(summary.AvgDispatchDelay),
						msFloat(summary.AvgConnDelay),
						msFloat(summary.AvgWriteDelay),
						msFloat(summary.AvgFirstByteRTT),
						msFloat(summary.AvgFirstByteDelay),
						msFloat(summary.AvgResponseTail),
						msFloat(summary.AvgTotalLatency),
						summary.AvgInFlight,
						summary.ObservedR,
						summary.LittleLawViolation,
					)
				}
			}

			// reset the window stats for the next interval
			window = &WindowStats{Start: time.Now()}

		}
	}
}

func (w *WindowStats) Summary() windowSummary {
	summary := windowSummary{
		PacerWaitCount:      w.PacerWaitValidCount,
		SchedulerDelayCount: w.SchedulerDelayValidCount,
		DispatchDelayCount:  w.DispatchDelayValidCount,
		ConnDelayCount:      w.ConnDelayValidCount,
		WriteDelayCount:     w.WriteDelayValidCount,
		FirstByteRTTCount:   w.FirstByteRTTValidCount,
		FirstByteDelayCount: w.FirstByteDelayValidCount,
		ResponseTailCount:   w.ResponseTailValidCount,
		TotalLatencyCount:   w.TotalLatencyValidCount,

		GotConnCount:        w.GotConnCount,
		ReusedConnCount:     w.ReusedConnCount,
		FreshConnCount:      w.FreshConnCount,
		ReusedIdleConnCount: w.ReusedIdleConnCount,
		ConnIdleTimeCount:   w.ConnIdleTimeValidCount,

		Duration: w.End.Sub(w.Start),
	}

	// compute valid achieved rate for the window
	if summary.Duration > 0 {
		summary.AchievedRate = float64(w.TotalLatencyValidCount) / summary.Duration.Seconds()
	}

	// compute average metrics for the window
	if w.PacerWaitValidCount > 0 {
		summary.AvgPacerWait = w.SumPacerWait / time.Duration(w.PacerWaitValidCount)
	}
	if w.SchedulerDelayValidCount > 0 {
		summary.AvgSchedulerDelay = w.SumSchedulerDelay / time.Duration(w.SchedulerDelayValidCount)
	}
	if w.DispatchDelayValidCount > 0 {
		summary.AvgDispatchDelay = w.SumDispatchDelay / time.Duration(w.DispatchDelayValidCount)
	}
	if w.ConnDelayValidCount > 0 {
		summary.AvgConnDelay = w.SumConnDelay / time.Duration(w.ConnDelayValidCount)
	}
	if w.WriteDelayValidCount > 0 {
		summary.AvgWriteDelay = w.SumWriteDelay / time.Duration(w.WriteDelayValidCount)
	}
	if w.FirstByteRTTValidCount > 0 {
		summary.AvgFirstByteRTT = w.SumFirstByteRTT / time.Duration(w.FirstByteRTTValidCount)
	}
	if w.FirstByteDelayValidCount > 0 {
		summary.AvgFirstByteDelay = w.SumFirstByteDelay / time.Duration(w.FirstByteDelayValidCount)
	}
	if w.ResponseTailValidCount > 0 {
		summary.AvgResponseTail = w.SumResponseTail / time.Duration(w.ResponseTailValidCount)
	}
	if w.TotalLatencyValidCount > 0 {
		summary.AvgTotalLatency = w.SumTotalLatency / time.Duration(w.TotalLatencyValidCount)
	}
	if w.ConnIdleTimeValidCount > 0 {
		summary.AvgConnIdleTime = w.SumConnIdleTime / time.Duration(w.ConnIdleTimeValidCount)
	}

	if w.GotConnCount > 0 {
		summary.ReuseFrac = float64(w.ReusedConnCount) / float64(w.GotConnCount)
		summary.FreshConnFrac = float64(w.FreshConnCount) / float64(w.GotConnCount)
	}

	if w.ReusedConnCount > 0 {
		summary.WasIdleGivenReused = float64(w.ReusedIdleConnCount) / float64(w.ReusedConnCount)
	}

	if w.NumInFlightSamples > 0 {
		// compute average in-flight for the window
		n := float64(w.NumInFlightSamples)
		summary.AvgInFlight = w.SumInFlightSamples / n
	}

	return summary
}

// msFloat converts a time.Duration to a float64 number of milliseconds.
func msFloat(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func targetRatePerSecond(rate vegeta.Rate) float64 {
	if rate.Freq == 0 || rate.Per <= 0 {
		return 0
	}
	return float64(rate.Freq) / rate.Per.Seconds()
}

func extractRPS(path string) (int, error) {
	base := filepath.Base(path) // eval_rps10000.csv

	re := regexp.MustCompile(`rps(\d+)`)
	m := re.FindStringSubmatch(base)
	if len(m) < 2 {
		return 0, fmt.Errorf("could not extract rps from path: %s", path)
	}

	rps, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, fmt.Errorf("invalid rps in path %s: %w", path, err)
	}

	return rps, nil
}

func loadAverageLatencyFromReferenceCSV(path string) (time.Duration, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("error opening reference CSV %s: %s", path, err)
	}
	defer f.Close()

	rps, err := extractRPS(path)
	if err != nil {
		return 0, fmt.Errorf("error extracting RPS from reference CSV path %s: %s", path, err)
	}

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return 0, fmt.Errorf("error reading reference CSV %s: %s", path, err)
	}
	if len(rows) < 2 {
		return 0, fmt.Errorf("reference CSV %s has no data rows", path)
	}

	header := map[string]int{}
	for i, col := range rows[0] {
		header[col] = i
	}

	latencyIdx, ok := header["avg_total_latency_ms"]
	if !ok {
		return 0, fmt.Errorf("reference CSV %s is missing required column %q", path, "avg_total_latency_ms")
	}
	countIdx, ok := header["total_latency_count"]
	if !ok {
		return 0, fmt.Errorf("reference CSV %s is missing required column %q", path, "total_latency_count")
	}

	var (
		sum   float64
		count int
	)

	minCount := 0.9 * float64(rps) // only consider rows where the count is at least 90% of the reference RPS

	// Average only rows whose observed count stays close to the requested RPS so the baseline reflects steady-state windows.
	for _, row := range rows[1:] {
		if latencyIdx >= len(row) || strings.TrimSpace(row[latencyIdx]) == "" {
			continue
		}
		if countIdx >= len(row) || strings.TrimSpace(row[countIdx]) == "" {
			continue
		}
		rowCount, err := strconv.ParseFloat(row[countIdx], 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing count %q in %s: %s", row[countIdx], path, err)
		}
		if rowCount <= minCount {
			continue
		}
		latencyMS, err := strconv.ParseFloat(row[latencyIdx], 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing avg_total_latency_ms %q in %s: %s", row[latencyIdx], path, err)
		}
		sum += latencyMS
		count++
	}

	if count == 0 {
		return 0, fmt.Errorf("reference CSV %s has no usable avg_total_latency_ms values with count > 90%% of target RPS", path)
	}

	return time.Duration((sum / float64(count)) * float64(time.Millisecond)), nil
}

func applyObservedR(summary *windowSummary, llRef *littleLawReference) {
	if llRef == nil {
		summary.ObservedR = math.NaN()
		return
	}

	inflightExpected := llRef.targetRPS * llRef.baselineLatency.Seconds()
	if inflightExpected <= 0 {
		summary.ObservedR = math.NaN()
		return
	}

	summary.ObservedR = summary.AvgInFlight / inflightExpected
}

func applyLittleLawCheck(summary *windowSummary, llRef *littleLawReference) {
	if math.IsNaN(summary.ObservedR) {
		return
	}

	if llRef == nil {
		lower := 0.95
		upper := 1.10
		fmt.Fprintf(
			os.Stderr,
			"No valid reference data available, using default envelope [%.2f, %.2f]\n",
			lower,
			upper,
		)

		if !math.IsNaN(summary.ObservedR) {
			if summary.ObservedR < lower || summary.ObservedR > upper {
				fmt.Fprintf(
					os.Stderr,
					"Little's Law violation: observed R=%.6f outside [%.2f, %.2f]\n",
					summary.ObservedR,
					lower,
					upper,
				)
				summary.LittleLawViolation = true
			} else {
				fmt.Fprintf(
					os.Stderr,
					"Little's Law check passed: observed R=%.6f within [%.2f, %.2f]\n",
					summary.ObservedR,
					lower,
					upper,
				)
				summary.LittleLawViolation = false
			}
		}
		return
	}

	summary.LittleLawViolation = false
}

// defining writer structs for metrics CSV and windowed trace CSV
type metricsCSVWriter struct {
	file      *os.File
	csv       *csv.Writer
	startTime time.Time
}

type windowCSVWriter struct {
	file *os.File
	csv  *csv.Writer
}

func newWindowCSVWriter(path string) (*windowCSVWriter, error) {

	// creating the CSV file for windowed trace metrics and writing the header row
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)
	if err := w.Write([]string{
		"window_start",
		"window_end",
		"window_duration_ms",
		"total_latency_count",
		"valid_achieved_rate",
		"avg_pacer_wait_ms",
		"avg_scheduler_delay_ms",
		"avg_dispatch_delay_ms",
		"avg_conn_delay_ms",
		"got_conn_count",
		"reused_conn_count",
		"fresh_conn_count",
		"reused_idle_conn_count",
		"conn_idle_time_count",
		"reuse_frac",
		"fresh_conn_frac",
		"was_idle_given_reused",
		"avg_conn_idle_time_ms",
		"avg_write_delay_ms",
		"avg_first_byte_rtt_ms",
		"avg_first_byte_delay_ms",
		"avg_response_tail_time_ms",
		"avg_total_latency_ms",
		"avg_in_flight",
		"observed_R",
		"ll_violation",
	}); err != nil {
		f.Close()
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return nil, err
	}

	return &windowCSVWriter{file: f, csv: w}, nil
}

func newWindowSamplesCSVWriter(path string) (*windowSamplesCSVWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)
	if err := w.Write([]string{"window_start", "window_end", "metric_name", "value_ms"}); err != nil {
		f.Close()
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return nil, err
	}

	return &windowSamplesCSVWriter{file: f, csv: w}, nil
}

func (w *windowCSVWriter) Close() error {
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

func (w *windowCSVWriter) Write(window *WindowStats, summary windowSummary) error {
	rec := []string{
		window.Start.UTC().Format(time.RFC3339Nano),
		window.End.UTC().Format(time.RFC3339Nano),
		strconv.FormatFloat(float64(summary.Duration)/float64(time.Millisecond), 'f', 3, 64),
		strconv.Itoa(summary.TotalLatencyCount),
		strconv.FormatFloat(summary.AchievedRate, 'f', 6, 64),
		strconv.FormatFloat(float64(summary.AvgPacerWait)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgSchedulerDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgDispatchDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgConnDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.Itoa(summary.GotConnCount),
		strconv.Itoa(summary.ReusedConnCount),
		strconv.Itoa(summary.FreshConnCount),
		strconv.Itoa(summary.ReusedIdleConnCount),
		strconv.Itoa(summary.ConnIdleTimeCount),
		strconv.FormatFloat(summary.ReuseFrac, 'f', 6, 64),
		strconv.FormatFloat(summary.FreshConnFrac, 'f', 6, 64),
		strconv.FormatFloat(summary.WasIdleGivenReused, 'f', 6, 64),
		strconv.FormatFloat(float64(summary.AvgConnIdleTime)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgWriteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteRTT)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgResponseTail)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgTotalLatency)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(summary.AvgInFlight, 'f', 6, 64),
		strconv.FormatFloat(summary.ObservedR, 'f', 6, 64),
		strconv.FormatBool(summary.LittleLawViolation),
	}
	if err := w.csv.Write(rec); err != nil {
		return err
	}
	w.csv.Flush()
	return w.csv.Error()
}

func (w *windowSamplesCSVWriter) Close() error {
	w.csv.Flush()
	if err := w.csv.Error(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

func newMetricsCSVWriter(path string) (*metricsCSVWriter, error) {

	// creating the CSV file for runtime metrics and writing the header row
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	w := csv.NewWriter(f)
	if err := w.Write([]string{
		"timestamp",
		"elapsed_ms",
		"workers",
		"connections",
		"in_flight",
		"completions",
	}); err != nil {
		f.Close()
		return nil, err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return nil, err
	}

	return &metricsCSVWriter{file: f, csv: w}, nil
}

func (w *windowSamplesCSVWriter) WriteWindowSamples(window *WindowStats) error {
	start := window.Start.UTC().Format(time.RFC3339Nano)
	end := window.End.UTC().Format(time.RFC3339Nano)

	writeMetric := func(name string, vals []float64) error {
		for _, v := range vals {
			rec := []string{
				start,
				end,
				name,
				strconv.FormatFloat(v, 'f', 6, 64),
			}
			if err := w.csv.Write(rec); err != nil {
				return err
			}
		}
		return nil
	}

	if err := writeMetric("pacer_wait", window.PacerWaitSamples); err != nil {
		return err
	}
	if err := writeMetric("scheduler_delay", window.SchedulerDelaySamples); err != nil {
		return err
	}
	if err := writeMetric("dispatch_delay", window.DispatchDelaySamples); err != nil {
		return err
	}
	if err := writeMetric("conn_delay", window.ConnDelaySamples); err != nil {
		return err
	}
	if err := writeMetric("write_delay", window.WriteDelaySamples); err != nil {
		return err
	}
	if err := writeMetric("first_byte_rtt", window.FirstByteRTTSamples); err != nil {
		return err
	}
	if err := writeMetric("first_byte_delay", window.FirstByteDelaySamples); err != nil {
		return err
	}
	if err := writeMetric("response_tail_time", window.ResponseTailSamples); err != nil {
		return err
	}
	if err := writeMetric("total_latency", window.TotalLatencySamples); err != nil {
		return err
	}
	if err := writeMetric("conn_idle_time", window.ConnIdleTimeSamples); err != nil {
		return err
	}

	w.csv.Flush()
	return w.csv.Error()
}

func (m *metricsCSVWriter) Close() error {
	m.csv.Flush()
	if err := m.csv.Error(); err != nil {
		_ = m.file.Close()
		return err
	}
	return m.file.Close()
}

func (m *metricsCSVWriter) Write(metrics vegeta.RuntimeMetrics) error {
	if m.startTime.IsZero() {
		m.startTime = metrics.Timestamp
	}
	elapsedMS := metrics.Timestamp.Sub(m.startTime).Milliseconds()

	rec := []string{
		metrics.Timestamp.UTC().Format(time.RFC3339Nano),
		strconv.FormatInt(elapsedMS, 10),
		strconv.FormatUint(metrics.Workers, 10),
		strconv.FormatUint(metrics.Connections, 10),
		strconv.FormatUint(metrics.InFlight, 10),
		strconv.FormatUint(metrics.Completions, 10),
	}
	if err := m.csv.Write(rec); err != nil {
		return err
	}
	m.csv.Flush()
	return m.csv.Error()
}

// tlsConfig builds a *tls.Config from the given options.
func tlsConfig(insecure bool, certf, keyf string, rootCerts []string) (*tls.Config, error) {
	var err error
	files := map[string][]byte{}
	filenames := append([]string{certf, keyf}, rootCerts...)
	for _, f := range filenames {
		if f != "" {
			if files[f], err = os.ReadFile(f); err != nil {
				return nil, err
			}
		}
	}

	c := tls.Config{InsecureSkipVerify: insecure}
	if cert, ok := files[certf]; ok {
		key, ok := files[keyf]
		if !ok {
			key = cert
		}

		certificate, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}

		c.Certificates = append(c.Certificates, certificate)
		c.BuildNameToCertificate()
	}

	if len(rootCerts) > 0 {
		c.RootCAs = x509.NewCertPool()
		for _, f := range rootCerts {
			if !c.RootCAs.AppendCertsFromPEM(files[f]) {
				return nil, errBadCert
			}
		}
	}

	return &c, nil
}
