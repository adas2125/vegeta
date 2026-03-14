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
	fs.StringVar(&opts.metricsCSV, "metrics-csv", "results.csv", "CSV file path for runtime attack metrics over time [empty = disabled]")
	fs.DurationVar(&opts.metricsInterval, "metrics-interval", time.Second, "Sampling interval for runtime metrics CSV")
	fs.DurationVar(&opts.sampleInterval, "sample-interval", 100*time.Millisecond, "Sampling interval")
	fs.Var(&dnsTTLFlag{&opts.dnsTTL}, "dns-ttl", "Cache DNS lookups for the given duration [-1 = disabled, 0 = forever]")
	fs.BoolVar(&opts.sessionTickets, "session-tickets", false, "Enable TLS session resumption using session tickets")
	fs.Var(&connectToFlag{&opts.connectTo}, "connect-to", "A mapping of (ip|host):port to use instead of a target URL's (ip|host):port. Can be repeated multiple times.\nIdentical src:port with different dst:port will round-robin over the different dst:port pairs.\nExample: google.com:80:localhost:6060")
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
	name            string
	targetsf        string
	format          string
	outputf         string
	bodyf           string
	certf           string
	keyf            string
	rootCerts       csl
	http2           bool
	h2c             bool
	insecure        bool
	lazy            bool
	chunked         bool
	duration        time.Duration
	timeout         time.Duration
	rate            vegeta.Rate
	workers         uint64
	maxWorkers      uint64
	connections     int
	maxConnections  int
	redirects       int
	maxBody         int64
	headers         headers
	proxyHeaders    headers
	laddr           localAddr
	keepalive       bool
	resolvers       csl
	unixSocket      string
	promAddr        string
	metricsCSV      string
	metricsInterval time.Duration
	sampleInterval  time.Duration
	dnsTTL          time.Duration
	sessionTickets  bool
	connectTo       map[string][]string
}

type WindowStats struct {
	Start time.Time
	End   time.Time

	Count int

	SumSchedulerDelay    time.Duration
	SumDispatchDelay     time.Duration
	SumConnDelay         time.Duration
	SumWriteDelay        time.Duration
	SumFirstByteRTT      time.Duration
	SumFirstByteDelay    time.Duration
	SumTotalLatency      time.Duration
	SumSqSchedulerDelay  float64
	SumSqDispatchDelay   float64
	SumSqConnDelay       float64
	SumSqWriteDelay      float64
	SumSqFirstByteRTT    float64
	SumSqFirstByteDelay  float64
	SumSqTotalLatency    float64
	SumInFlightSamples   float64
	SumSqInFlightSamples float64
	NumInFlightSamples   int64
}

type windowSummary struct {
	Count                int
	Duration             time.Duration
	AchievedRate         float64
	AvgSchedulerDelay    time.Duration
	StdDevSchedulerDelay time.Duration
	AvgDispatchDelay     time.Duration
	StdDevDispatchDelay  time.Duration
	AvgConnDelay         time.Duration
	StdDevConnDelay      time.Duration
	AvgWriteDelay        time.Duration
	StdDevWriteDelay     time.Duration
	AvgFirstByteRTT      time.Duration
	StdDevFirstByteRTT   time.Duration
	AvgFirstByteDelay    time.Duration
	StdDevFirstByteDelay time.Duration
	AvgTotalLatency      time.Duration
	StdDevTotalLatency   time.Duration
	AvgInFlight          float64
	StdDevInFlight       float64
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
	if opts.promAddr != "" {
		pm = prom.NewMetrics()

		r := prometheus.NewRegistry()
		if err := pm.Register(r); err != nil {
			return fmt.Errorf("error registering prometheus metrics: %s", err)
		}

		srv := http.Server{
			Addr:    opts.promAddr,
			Handler: prom.NewHandler(r, time.Now().UTC()),
		}

		defer srv.Close()
		go srv.ListenAndServe()
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

	var mw *metricsCSVWriter
	if opts.metricsCSV != "" {
		mw, err = newMetricsCSVWriter(opts.metricsCSV)
		if err != nil {
			return fmt.Errorf("error creating %s: %s", opts.metricsCSV, err)
		}
		defer mw.Close()
	}

	ww, err := newWindowCSVWriter("window_results.csv")
	if err != nil {
		return fmt.Errorf("error creating %s: %s", "window_results.csv", err)
	}
	defer ww.Close()

	res := atk.Attack(tr, opts.rate, opts.duration, opts.name)
	traces := atk.TraceRecords()
	enc := vegeta.NewEncoder(out)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	return processAttack(atk, res, enc, sig, pm, mw, ww, opts.metricsInterval, opts.sampleInterval, traces)
}

func processAttack(
	atk *vegeta.Attacker,
	res <-chan *vegeta.Result,
	enc vegeta.Encoder,
	sig <-chan os.Signal,
	pm *prom.Metrics,
	mw *metricsCSVWriter,
	ww *windowCSVWriter,
	metricsInterval time.Duration,
	sampleInterval time.Duration,
	traces <-chan *vegeta.RequestRecord,
) error {
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

	if mw != nil {
		if err := mw.Write(atk.RuntimeMetrics()); err != nil {
			return err
		}
	}

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
				if mw != nil {
					if err := mw.Write(atk.RuntimeMetrics()); err != nil {
						return err
					}
				}
				res = nil
				if res == nil && traces == nil {
					window.End = time.Now()
					if window.Count > 0 && ww != nil {
						summary := window.Summary()
						if err := ww.Write(window, summary); err != nil {
							return err
						}
					}
					return nil
				}
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
					window.End = time.Now()
					if window.Count > 0 && ww != nil {
						summary := window.Summary()
						if err := ww.Write(window, summary); err != nil {
							return err
						}
					}
					return nil
				}
				continue
			}

			// update the window stats
			window.Count++
			window.SumSchedulerDelay += rec.SchedulerDelay
			window.SumDispatchDelay += rec.DispatchDelay
			window.SumConnDelay += rec.ConnDelay
			window.SumWriteDelay += rec.WriteDelay
			window.SumFirstByteRTT += rec.FirstByteRTT
			window.SumFirstByteDelay += rec.FirstByteDelay
			window.SumTotalLatency += rec.TotalLatency
			window.SumSqSchedulerDelay += durationSquare(rec.SchedulerDelay)
			window.SumSqDispatchDelay += durationSquare(rec.DispatchDelay)
			window.SumSqConnDelay += durationSquare(rec.ConnDelay)
			window.SumSqWriteDelay += durationSquare(rec.WriteDelay)
			window.SumSqFirstByteRTT += durationSquare(rec.FirstByteRTT)
			window.SumSqFirstByteDelay += durationSquare(rec.FirstByteDelay)
			window.SumSqTotalLatency += durationSquare(rec.TotalLatency)

		case <-sampleTicker.C:
			// sample the current runtime metrics and update the window stats
			metrics := atk.RuntimeMetrics()
			inFlight := float64(metrics.InFlight)
			window.SumInFlightSamples += inFlight
			window.SumSqInFlightSamples += inFlight * inFlight
			window.NumInFlightSamples++

		case <-ticker.C:
			// whenever the ticker fires, write the current runtime metrics to the CSV file
			if mw != nil {
				if err := mw.Write(atk.RuntimeMetrics()); err != nil {
					return err
				}
			}

			window.End = time.Now()

			// compute the average metrics for the window
			if window.Count > 0 {
				summary := window.Summary()

				fmt.Fprintf(os.Stderr, "Window [%s - %s]: Count=%d, AvgSchedulerDelay=%s, StdDevSchedulerDelay=%s, AvgDispatchDelay=%s, StdDevDispatchDelay=%s, AvgConnDelay=%s, StdDevConnDelay=%s, AvgWriteDelay=%s, StdDevWriteDelay=%s, AvgFirstByteRTT=%s, StdDevFirstByteRTT=%s, AvgFirstByteDelay=%s, StdDevFirstByteDelay=%s, AvgTotalLatency=%s, StdDevTotalLatency=%s, AchievedRate=%.2f, AvgInFlight=%.2f, StdDevInFlight=%.2f\n",
					window.Start.Format(time.RFC3339Nano), window.End.Format(time.RFC3339Nano),
					summary.Count,
					summary.AvgSchedulerDelay, summary.StdDevSchedulerDelay,
					summary.AvgDispatchDelay, summary.StdDevDispatchDelay,
					summary.AvgConnDelay, summary.StdDevConnDelay,
					summary.AvgWriteDelay, summary.StdDevWriteDelay,
					summary.AvgFirstByteRTT, summary.StdDevFirstByteRTT,
					summary.AvgFirstByteDelay, summary.StdDevFirstByteDelay,
					summary.AvgTotalLatency, summary.StdDevTotalLatency,
					summary.AchievedRate, summary.AvgInFlight, summary.StdDevInFlight)

				if ww != nil {
					if err := ww.Write(window, summary); err != nil {
						return err
					}
				}
			}

			// reset the window stats for the next interval
			window = &WindowStats{Start: time.Now()}

		}
	}
}

func (w *WindowStats) Summary() windowSummary {
	summary := windowSummary{
		Count:    w.Count,
		Duration: w.End.Sub(w.Start),
	}

	if summary.Duration > 0 {
		summary.AchievedRate = float64(w.Count) / summary.Duration.Seconds()
	}

	if w.Count > 0 {
		n := float64(w.Count)
		summary.AvgSchedulerDelay = w.SumSchedulerDelay / time.Duration(w.Count)
		summary.StdDevSchedulerDelay = stdDevDuration(w.SumSchedulerDelay, w.SumSqSchedulerDelay, n)
		summary.AvgDispatchDelay = w.SumDispatchDelay / time.Duration(w.Count)
		summary.StdDevDispatchDelay = stdDevDuration(w.SumDispatchDelay, w.SumSqDispatchDelay, n)
		summary.AvgConnDelay = w.SumConnDelay / time.Duration(w.Count)
		summary.StdDevConnDelay = stdDevDuration(w.SumConnDelay, w.SumSqConnDelay, n)
		summary.AvgWriteDelay = w.SumWriteDelay / time.Duration(w.Count)
		summary.StdDevWriteDelay = stdDevDuration(w.SumWriteDelay, w.SumSqWriteDelay, n)
		summary.AvgFirstByteRTT = w.SumFirstByteRTT / time.Duration(w.Count)
		summary.StdDevFirstByteRTT = stdDevDuration(w.SumFirstByteRTT, w.SumSqFirstByteRTT, n)
		summary.AvgFirstByteDelay = w.SumFirstByteDelay / time.Duration(w.Count)
		summary.StdDevFirstByteDelay = stdDevDuration(w.SumFirstByteDelay, w.SumSqFirstByteDelay, n)
		summary.AvgTotalLatency = w.SumTotalLatency / time.Duration(w.Count)
		summary.StdDevTotalLatency = stdDevDuration(w.SumTotalLatency, w.SumSqTotalLatency, n)
	}

	if w.NumInFlightSamples > 0 {
		n := float64(w.NumInFlightSamples)
		summary.AvgInFlight = w.SumInFlightSamples / n
		summary.StdDevInFlight = stdDevFloat(w.SumInFlightSamples, w.SumSqInFlightSamples, n)
	}

	return summary
}

func durationSquare(d time.Duration) float64 {
	v := float64(d)
	return v * v
}

func stdDevDuration(sum time.Duration, sumSq float64, count float64) time.Duration {
	return time.Duration(stdDevFloat(float64(sum), sumSq, count))
}

func stdDevFloat(sum float64, sumSq float64, count float64) float64 {
	if count <= 0 {
		return 0
	}

	mean := sum / count
	variance := (sumSq / count) - (mean * mean)
	if variance < 0 {
		variance = 0
	}

	return math.Sqrt(variance)
}

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
		"achieved_rate",
		"avg_scheduler_delay_ms",
		"stddev_scheduler_delay_ms",
		"avg_dispatch_delay_ms",
		"stddev_dispatch_delay_ms",
		"avg_conn_delay_ms",
		"stddev_conn_delay_ms",
		"avg_write_delay_ms",
		"stddev_write_delay_ms",
		"avg_first_byte_rtt_ms",
		"stddev_first_byte_rtt_ms",
		"avg_first_byte_delay_ms",
		"stddev_first_byte_delay_ms",
		"avg_total_latency_ms",
		"stddev_total_latency_ms",
		"avg_in_flight",
		"stddev_in_flight",
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
		strconv.Itoa(summary.Count),
		strconv.FormatFloat(summary.AchievedRate, 'f', 6, 64),
		strconv.FormatFloat(float64(summary.AvgSchedulerDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevSchedulerDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgDispatchDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevDispatchDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgConnDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevConnDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgWriteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevWriteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteRTT)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevFirstByteRTT)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgFirstByteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevFirstByteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.AvgTotalLatency)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(summary.StdDevTotalLatency)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(summary.AvgInFlight, 'f', 6, 64),
		strconv.FormatFloat(summary.StdDevInFlight, 'f', 6, 64),
	}
	if err := w.csv.Write(rec); err != nil {
		return err
	}
	w.csv.Flush()
	return w.csv.Error()
}

func newMetricsCSVWriter(path string) (*metricsCSVWriter, error) {
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
		"send_delay_ms",
		"in_flight",
		"completions",
		"scheduler_delay_ms",
		"conn_delay_ms",
		"write_delay_ms",
		"first_byte_rtt_ms",
		"first_byte_delay_ms",
		"total_latency_ms",
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
		strconv.FormatFloat(float64(metrics.SendDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatUint(metrics.InFlight, 10),
		strconv.FormatUint(metrics.Completions, 10),
		strconv.FormatFloat(float64(metrics.SchedulerDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(metrics.ConnDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(metrics.WriteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(metrics.FirstByteRTT)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(metrics.FirstByteDelay)/float64(time.Millisecond), 'f', 3, 64),
		strconv.FormatFloat(float64(metrics.TotalLatency)/float64(time.Millisecond), 'f', 3, 64),
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
