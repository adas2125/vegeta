package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsenart/vegeta/v12/internal/resolver"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	prom "github.com/tsenart/vegeta/v12/lib/prom"
)

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
	fs.StringVar(&opts.baselineReferenceCSV, "baseline-reference-csv", "", "CSV file path for baseline Little's Law reference data; must be provided for Little's Law check to be performed")
	fs.StringVar(&opts.referenceCSVPath, "reference-csv-path", "", "CSV file path for computed baseline latency for R metric computation")
	fs.DurationVar(&opts.metricsInterval, "metrics-interval", time.Second, "Sampling interval for runtime metrics CSV")
	fs.DurationVar(&opts.sampleInterval, "sample-interval", 100*time.Millisecond, "Sampling interval for windowed trace metrics (in-flight), must be less than or equal to metrics-interval")
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
	name                 string
	targetsf             string
	format               string
	outputf              string
	bodyf                string
	certf                string
	keyf                 string
	rootCerts            csl
	http2                bool
	h2c                  bool
	insecure             bool
	lazy                 bool
	chunked              bool
	duration             time.Duration
	timeout              time.Duration
	rate                 vegeta.Rate
	workers              uint64
	maxWorkers           uint64
	connections          int
	maxConnections       int
	redirects            int
	maxBody              int64
	headers              headers
	proxyHeaders         headers
	laddr                localAddr
	keepalive            bool
	resolvers            csl
	unixSocket           string
	promAddr             string
	metricsCSV           string
	windowCSV            string
	baselineReferenceCSV string
	metricsInterval      time.Duration
	sampleInterval       time.Duration
	referenceCSVPath     string
	dnsTTL               time.Duration
	sessionTickets       bool
	connectTo            map[string][]string
}

type WindowStats struct {
	Start time.Time
	End   time.Time

	ValidCount int

	SumSchedulerDelay  time.Duration
	SumDispatchDelay   time.Duration
	SumConnDelay       time.Duration
	SumWriteDelay      time.Duration
	SumFirstByteRTT    time.Duration
	SumFirstByteDelay  time.Duration
	SumTotalLatency    time.Duration
	SumInFlightSamples float64
	NumInFlightSamples int64
}

type windowSummary struct {
	ValidCount         int
	Duration           time.Duration
	AchievedRate       float64
	AvgSchedulerDelay  time.Duration
	AvgDispatchDelay   time.Duration
	AvgConnDelay       time.Duration
	AvgWriteDelay      time.Duration
	AvgFirstByteRTT    time.Duration
	AvgFirstByteDelay  time.Duration
	AvgTotalLatency    time.Duration
	AvgInFlight        float64
	ObservedR          float64
	LittleLawViolation bool
}

type littleLawReference struct {
	targetRPS       float64
	baselineRPS     float64
	baselineLatency time.Duration
	p01NormalR      float64 // p01NormalR & p99NormalR are computed from reference-csv-path (referenceCSVPath)
	p99NormalR      float64
}

// attack validates the attack arguments, sets up the
// required resources, launches the attack and writes the results
func attack(opts *attackOpts) (err error) {
	if opts.maxWorkers == vegeta.DefaultMaxWorkers && opts.rate.Freq == 0 {
		return fmt.Errorf("-rate=0 requires setting -max-workers")
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

	// loading the baseline reference CSV for Little's Law check if provided
	var llRef *littleLawReference
	targetRPS := targetRatePerSecond(opts.rate)
	if opts.baselineReferenceCSV != "" && opts.rate.Freq > 0 {
		llRef, err = loadLittleLawReference(opts.baselineReferenceCSV, targetRPS)
		if err != nil {
			return err
		}
	}

	// If a reference CSV is provided, use the average window latency from that
	// file as the baseline latency for ObservedR calculations.
	if opts.referenceCSVPath != "" && targetRPS > 0 {
		baselineLatency, err := loadAverageLatencyFromReferenceCSV(opts.referenceCSVPath, targetRPS)
		if err != nil {
			return err
		}
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

	return processAttack(atk, res, enc, sig, pm, dm, mw, ww, llRef, opts.metricsInterval, opts.sampleInterval, traces)
}

func processAttack(
	atk *vegeta.Attacker,
	res <-chan *vegeta.Result,
	enc vegeta.Encoder,
	sig <-chan os.Signal,
	pm *prom.Metrics,
	dm *prom.DiagnosticMetrics,
	mw *metricsCSVWriter,
	ww *windowCSVWriter,
	llRef *littleLawReference,
	metricsInterval time.Duration,
	sampleInterval time.Duration,
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
					if window.ValidCount > 0 {
						// compute the average metrics for the final window
						summary := window.Summary()
						applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last ticker tick
						applyLittleLawCheck(&summary, llRef)
						// write the final windowed trace metrics to the CSV file if provided
						if ww != nil {
							if err := ww.Write(window, summary); err != nil {
								return err
							}
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
					if window.ValidCount > 0 {
						summary := window.Summary()
						applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last ticker tick
						applyLittleLawCheck(&summary, llRef)
						if ww != nil {
							if err := ww.Write(window, summary); err != nil {
								return err
							}
						}
					}
					return nil
				}
				continue // continue to process any remaining results as results may still be coming
			}

			// update the window stats on receipt of a valid record only if none of the valids are False for rec
			allTrue := rec.SchedulerDelayValid && rec.DispatchDelayValid &&
				rec.ConnDelayValid && rec.WriteDelayValid &&
				rec.FirstByteRTTValid && rec.FirstByteDelayValid &&
				rec.TotalLatencyValid
			if !allTrue {
				continue
			}

			window.ValidCount++
			window.SumSchedulerDelay += rec.SchedulerDelay
			window.SumDispatchDelay += rec.DispatchDelay
			window.SumConnDelay += rec.ConnDelay
			window.SumWriteDelay += rec.WriteDelay
			window.SumFirstByteRTT += rec.FirstByteRTT
			window.SumFirstByteDelay += rec.FirstByteDelay
			window.SumTotalLatency += rec.TotalLatency

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
			if window.ValidCount > 0 {
				summary := window.Summary()
				applyObservedR(&summary, llRef) // update observed R as in-flight samples may have changed since the last sample ticker tick
				applyLittleLawCheck(&summary, llRef)
				if ww != nil {
					if err := ww.Write(window, summary); err != nil {
						return err
					}
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
		ValidCount: w.ValidCount,
		Duration:   w.End.Sub(w.Start),
	}

	// compute valid achieved rate for the window
	if summary.Duration > 0 {
		summary.AchievedRate = float64(w.ValidCount) / summary.Duration.Seconds()
	}

	// compute average metrics for the window
	if w.ValidCount > 0 {
		summary.AvgSchedulerDelay = w.SumSchedulerDelay / time.Duration(w.ValidCount)
		summary.AvgDispatchDelay = w.SumDispatchDelay / time.Duration(w.ValidCount)
		summary.AvgConnDelay = w.SumConnDelay / time.Duration(w.ValidCount)
		summary.AvgWriteDelay = w.SumWriteDelay / time.Duration(w.ValidCount)
		summary.AvgFirstByteRTT = w.SumFirstByteRTT / time.Duration(w.ValidCount)
		summary.AvgFirstByteDelay = w.SumFirstByteDelay / time.Duration(w.ValidCount)
		summary.AvgTotalLatency = w.SumTotalLatency / time.Duration(w.ValidCount)
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

func loadLittleLawReference(path string, targetRPS float64) (*littleLawReference, error) {
	/*
		Loads the baseline reference CSV for Little's Law check and returns the closest matching row to the target RPS
	*/

	// opening the provided CSV
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening baseline reference CSV %s: %s", path, err)
	}
	defer f.Close()

	// reading the CSV and validating its structure
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading baseline reference CSV %s: %s", path, err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("baseline reference CSV %s has no data rows", path)
	}

	// mapping the header columns to their indices
	header := map[string]int{}
	for i, col := range rows[0] {
		header[col] = i
	}

	// validating that the required columns are present in the CSV
	required := []string{"rps", "avg_total_latency_ms_mean", "p01_normal_r", "p99_normal_r"}
	for _, col := range required {
		if _, ok := header[col]; !ok {
			return nil, fmt.Errorf("baseline reference CSV %s is missing required column %q", path, col)
		}
	}

	var (
		best         *littleLawReference
		bestDistance = math.MaxFloat64
		globalLower  = math.MaxFloat64
		globalUpper  = -math.MaxFloat64
	)

	for _, row := range rows[1:] {

		// obtaining the RPS, average total latency, and average in-flight standard deviation from the CSV row
		rps, err := strconv.ParseFloat(row[header["rps"]], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing rps %q in %s: %s", row[header["rps"]], path, err)
		}
		// latency is the average across all the windows for the given RPS
		latencyMS, err := strconv.ParseFloat(row[header["avg_total_latency_ms_mean"]], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing avg_total_latency_ms_mean %q in %s: %s", row[header["avg_total_latency_ms_mean"]], path, err)
		}
		p01R, err := strconv.ParseFloat(row[header["p01_normal_r"]], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing p01_normal_r %q in %s: %s", row[header["p01_normal_r"]], path, err)
		}
		p99R, err := strconv.ParseFloat(row[header["p99_normal_r"]], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing p99_normal_r %q in %s: %s", row[header["p99_normal_r"]], path, err)
		}

		// distance from the target RPS to the current row's RPS
		distance := math.Abs(rps - targetRPS)
		if best == nil || distance < bestDistance {
			best = &littleLawReference{
				targetRPS:       targetRPS,
				baselineRPS:     rps,
				baselineLatency: time.Duration(latencyMS * float64(time.Millisecond)),
			}
			bestDistance = distance
		}

		// --- global envelope (GLOBAL) ---
		if p01R < globalLower {
			globalLower = p01R
		}
		if p99R > globalUpper {
			globalUpper = p99R
		}
	}

	if best == nil {
		return nil, fmt.Errorf("baseline reference CSV %s has no usable baseline rows", path)
	}

	best.p01NormalR = globalLower
	best.p99NormalR = globalUpper
	return best, nil
}

func loadAverageLatencyFromReferenceCSV(path string, targetRPS float64) (time.Duration, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("error opening reference CSV %s: %s", path, err)
	}
	defer f.Close()

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
	countIdx, ok := header["count"]
	if !ok {
		return 0, fmt.Errorf("reference CSV %s is missing required column %q", path, "count")
	}

	var (
		sum   float64
		count int
	)

	minCount := 0.9 * targetRPS

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
	if llRef == nil || math.IsNaN(summary.ObservedR) {
		return
	}

	// experimentally set values
	lower := math.Min(llRef.p01NormalR, 0.95)
	upper := math.Max(llRef.p99NormalR, 1.10)

	summary.LittleLawViolation = summary.ObservedR < lower || summary.ObservedR > upper

	if summary.LittleLawViolation {
		fmt.Fprintf(
			os.Stderr,
			"Little's Law violation: observed R=%.6f outside [%.6f, %.6f]\n",
			summary.ObservedR,
			lower,
			upper,
		)
	} else {
		fmt.Fprintf(
			os.Stderr,
			"Little's Law check passed: observed R=%.6f within [%.6f, %.6f]\n",
			summary.ObservedR,
			lower,
			upper,
		)
	}
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
		"count",
		"valid_achieved_rate",
		"avg_scheduler_delay_ms",
		"avg_dispatch_delay_ms",
		"avg_conn_delay_ms",
		"avg_write_delay_ms",
		"avg_first_byte_rtt_ms",
		"avg_first_byte_delay_ms",
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
		strconv.Itoa(summary.ValidCount),
		strconv.FormatFloat(summary.AchievedRate, 'f', 6, 64),
		strconv.FormatFloat(float64(summary.AvgSchedulerDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgDispatchDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgConnDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgWriteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteRTT)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteDelay)/float64(time.Millisecond), 'f', 3, 64),
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
