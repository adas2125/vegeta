package vegeta

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/dnscache"
	"golang.org/x/net/http2"
)

// Attacker is an attack executor which wraps an http.Client
type Attacker struct {
	dialer     *net.Dialer
	client     http.Client
	stopch     chan struct{}
	stopOnce   sync.Once
	workers    uint64
	maxWorkers uint64
	maxBody    int64
	redirects  int
	seqmu      sync.Mutex
	seq        uint64
	began      time.Time
	chunked    bool

	currentWorkers int64
	activeConns    int64
	inFlight       int64
	completions    int64
	traceCh        chan *RequestRecord
}

// creating a struct to hold information related to each request
type RequestRecord struct {
	ID                  uint64
	FireTime            time.Time
	PacerWait           time.Duration
	PacerWaitValid      bool
	WakeTime            time.Time
	DispatchStart       time.Time
	GotConnTime         time.Time
	WroteReqTime        time.Time
	FirstByteTime       time.Time
	DoneTime            time.Time
	DispatchDelay       time.Duration
	DispatchDelayValid  bool
	ConnDelay           time.Duration
	ConnDelayValid      bool
	WriteDelay          time.Duration
	WriteDelayValid     bool
	FirstByteRTT        time.Duration
	FirstByteRTTValid   bool
	FirstByteDelay      time.Duration
	FirstByteDelayValid bool
	ResponseTailTime    time.Duration
	ResponseTailValid   bool
	TotalLatency        time.Duration
	TotalLatencyValid   bool
	SchedulerDelay      time.Duration
	SchedulerDelayValid bool

	// connection related fields
	ConnReused        bool
	ConnWasIdle       bool
	ConnIdleTime      time.Duration
	ConnIdleTimeValid bool
	GotConnValid      bool
}

const (
	// DefaultRedirects is the default number of times an Attacker follows
	// redirects.
	DefaultRedirects = 10
	// DefaultTimeout is the default amount of time an Attacker waits for a request
	// before it times out.
	DefaultTimeout = 30 * time.Second
	// DefaultConnections is the default amount of max open idle connections per
	// target host.
	DefaultConnections = 10000
	// DefaultMaxConnections is the default amount of connections per target
	// host.
	DefaultMaxConnections = 0
	// DefaultWorkers is the default initial number of workers used to carry an attack.
	DefaultWorkers = 10
	// DefaultMaxWorkers is the default maximum number of workers used to carry an attack.
	DefaultMaxWorkers = math.MaxUint64
	// DefaultMaxBody is the default max number of bytes to be read from response bodies.
	// Defaults to no limit.
	DefaultMaxBody = int64(-1)
	// NoFollow is the value when redirects are not followed but marked successful
	NoFollow = -1
)

var (
	// DefaultLocalAddr is the default local IP address an Attacker uses.
	DefaultLocalAddr = net.IPAddr{IP: net.IPv4zero}
	// DefaultTLSConfig is the default tls.Config an Attacker uses.
	DefaultTLSConfig = &tls.Config{InsecureSkipVerify: false}
)

// RuntimeMetrics holds point-in-time attacker internals useful for time-series tracking.
type RuntimeMetrics struct {
	Timestamp   time.Time
	Workers     uint64
	Connections uint64
	InFlight    uint64
	Completions uint64
}

type FireEvent struct {
	FireTime  time.Time
	PacerWait time.Duration
}

func (a *Attacker) TraceRecords() <-chan *RequestRecord {
	return a.traceCh
}

// NewAttacker returns a new Attacker with default options which are overridden
// by the optionally provided opts.
func NewAttacker(opts ...func(*Attacker)) *Attacker {
	a := &Attacker{
		stopch:     make(chan struct{}),
		stopOnce:   sync.Once{},
		workers:    DefaultWorkers,
		maxWorkers: DefaultMaxWorkers,
		maxBody:    DefaultMaxBody,
	}

	a.dialer = &net.Dialer{
		LocalAddr: &net.TCPAddr{IP: DefaultLocalAddr.IP, Zone: DefaultLocalAddr.Zone},
		KeepAlive: 30 * time.Second,
	}

	a.client = http.Client{
		Timeout: DefaultTimeout,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         a.trackDialContext(a.dialer.DialContext),
			TLSClientConfig:     DefaultTLSConfig,
			MaxIdleConnsPerHost: DefaultConnections,
			MaxConnsPerHost:     DefaultMaxConnections,
		},
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Workers returns a functional option which sets the initial number of workers
// an Attacker uses to hit its targets. More workers may be spawned dynamically
// to sustain the requested rate in the face of slow responses and errors.
func Workers(n uint64) func(*Attacker) {
	return func(a *Attacker) { a.workers = n }
}

// MaxWorkers returns a functional option which sets the maximum number of workers
// an Attacker can use to hit its targets.
func MaxWorkers(n uint64) func(*Attacker) {
	return func(a *Attacker) { a.maxWorkers = n }
}

// Connections returns a functional option which sets the number of maximum idle
// open connections per target host.
func Connections(n int) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		tr.MaxIdleConnsPerHost = n
	}
}

// MaxConnections returns a functional option which sets the number of maximum
// connections per target host.
func MaxConnections(n int) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		tr.MaxConnsPerHost = n
	}
}

// ChunkedBody returns a functional option which makes the attacker send the
// body of each request with the chunked transfer encoding.
func ChunkedBody(b bool) func(*Attacker) {
	return func(a *Attacker) { a.chunked = b }
}

// Redirects returns a functional option which sets the maximum
// number of redirects an Attacker will follow.
func Redirects(n int) func(*Attacker) {
	return func(a *Attacker) {
		a.redirects = n
		a.client.CheckRedirect = func(_ *http.Request, via []*http.Request) error {
			switch {
			case n == NoFollow:
				return http.ErrUseLastResponse
			case n < len(via):
				return fmt.Errorf("stopped after %d redirects", n)
			default:
				return nil
			}
		}
	}
}

// Proxy returns a functional option which sets the `Proxy` field on
// the http.Client's Transport
func Proxy(proxy func(*http.Request) (*url.URL, error)) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		tr.Proxy = proxy
	}
}

// Timeout returns a functional option which sets the maximum amount of time
// an Attacker will wait for a request to be responded to and completely read.
func Timeout(d time.Duration) func(*Attacker) {
	return func(a *Attacker) {
		a.client.Timeout = d
	}
}

// LocalAddr returns a functional option which sets the local address
// an Attacker will use with its requests.
func LocalAddr(addr net.IPAddr) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		a.dialer.LocalAddr = &net.TCPAddr{IP: addr.IP, Zone: addr.Zone}
		tr.DialContext = a.trackDialContext(a.dialer.DialContext)
	}
}

// KeepAlive returns a functional option which toggles KeepAlive
// connections on the dialer and transport.
func KeepAlive(keepalive bool) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		tr.DisableKeepAlives = !keepalive
		if !keepalive {
			a.dialer.KeepAlive = 0
			tr.DialContext = a.trackDialContext(a.dialer.DialContext)
		}
	}
}

// TLSConfig returns a functional option which sets the *tls.Config for a
// Attacker to use with its requests.
func TLSConfig(c *tls.Config) func(*Attacker) {
	return func(a *Attacker) {
		tr := a.client.Transport.(*http.Transport)
		tr.TLSClientConfig = c
	}
}

// HTTP2 returns a functional option which enables or disables HTTP/2 support
// on requests performed by an Attacker.
func HTTP2(enabled bool) func(*Attacker) {
	return func(a *Attacker) {
		if tr := a.client.Transport.(*http.Transport); enabled {
			http2.ConfigureTransport(tr)
		} else {
			tr.ForceAttemptHTTP2 = false
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
	}
}

// H2C returns a functional option which enables H2C support on requests
// performed by an Attacker
func H2C(enabled bool) func(*Attacker) {
	return func(a *Attacker) {
		if tr := a.client.Transport.(*http.Transport); enabled {
			a.client.Transport = &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
					return tr.DialContext(ctx, network, addr)
				},
			}
		}
	}
}

// MaxBody returns a functional option which limits the max number of bytes
// read from response bodies. Set to -1 to disable any limits.
func MaxBody(n int64) func(*Attacker) {
	return func(a *Attacker) { a.maxBody = n }
}

// UnixSocket changes the dialer for the attacker to use the specified unix socket file
func UnixSocket(socket string) func(*Attacker) {
	return func(a *Attacker) {
		if tr, ok := a.client.Transport.(*http.Transport); socket != "" && ok {
			tr.DialContext = a.trackDialContext(func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			})
		}
	}
}

// SessionTickets returns a functional option which configures usage of session
// tickets for TLS session resumption.
func SessionTickets(enabled bool) func(*Attacker) {
	return func(a *Attacker) {
		if enabled {
			cf := a.client.Transport.(*http.Transport).TLSClientConfig
			cf.SessionTicketsDisabled = false
			cf.ClientSessionCache = tls.NewLRUClientSessionCache(0)
		}
	}
}

// Client returns a functional option that allows you to bring your own http.Client
func Client(c *http.Client) func(*Attacker) {
	return func(a *Attacker) { a.client = *c }
}

// ProxyHeader returns a functional option that allows you to add your own
// Proxy CONNECT headers
func ProxyHeader(h http.Header) func(*Attacker) {
	return func(a *Attacker) {
		if tr, ok := a.client.Transport.(*http.Transport); ok {
			tr.ProxyConnectHeader = h
		}
	}
}

// ConnectTo returns a functional option which makes the attacker use the
// passed in map to translate target addr:port pairs. When used with DNSCaching,
// it must be used after it.
func ConnectTo(addrMap map[string][]string) func(*Attacker) {
	return func(a *Attacker) {
		if len(addrMap) == 0 {
			return
		}

		tr, ok := a.client.Transport.(*http.Transport)
		if !ok {
			return
		}

		dial := tr.DialContext
		if dial == nil {
			dial = a.dialer.DialContext
		}

		type roundRobin struct {
			addrs []string
			n     int
		}

		connectTo := make(map[string]*roundRobin, len(addrMap))
		for k, v := range addrMap {
			connectTo[k] = &roundRobin{addrs: v}
		}

		tr.DialContext = a.trackDialContext(func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cm, ok := connectTo[addr]; ok {
				cm.n = (cm.n + 1) % len(cm.addrs)
				addr = cm.addrs[cm.n]
			}
			return dial(ctx, network, addr)
		})
	}
}

// DNSCaching returns a functional option that enables DNS caching for
// the given ttl. When ttl is zero cached entries will never expire.
// When ttl is non-zero, this will start a refresh go-routine that updates
// the cache every ttl interval. This go-routine will be stopped when the
// attack is stopped.
// When the ttl is negative, no caching will be performed.
func DNSCaching(ttl time.Duration) func(*Attacker) {
	return func(a *Attacker) {
		if ttl < 0 {
			return
		}

		if tr, ok := a.client.Transport.(*http.Transport); ok {
			dial := tr.DialContext
			if dial == nil {
				dial = a.dialer.DialContext
			}

			resolver := &dnscache.Resolver{}

			if ttl != 0 {
				go func() {
					refresh := time.NewTicker(ttl)
					defer refresh.Stop()
					for {
						select {
						case <-refresh.C:
							resolver.Refresh(true)
						case <-a.stopch:
							return
						}
					}
				}()
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

			tr.DialContext = a.trackDialContext(func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}

				ips, err := resolver.LookupHost(ctx, host)
				if err != nil {
					return nil, err
				}

				if len(ips) == 0 {
					return nil, &net.DNSError{Err: "no such host", Name: addr}
				}

				// Pick a random IP from each IP family and dial each concurrently.
				// The first that succeeds wins, the other gets canceled.

				rng.Shuffle(len(ips), func(i, j int) { ips[i], ips[j] = ips[j], ips[i] })

				ips = firstOfEachIPFamily(ips)

				type result struct {
					conn net.Conn
					err  error
				}

				ch := make(chan result, len(ips))
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				for _, ip := range ips {
					go func(ip string) {
						conn, err := dial(ctx, network, net.JoinHostPort(ip, port))
						if err == nil {
							cancel()
						}
						ch <- result{conn, err}
					}(ip)
				}

				for i := 0; i < cap(ch); i++ {
					if r := <-ch; conn == nil {
						conn, err = r.conn, r.err
					}
				}

				return conn, err
			})
		}
	}
}

// firstOfEachIPFamily returns the first IP of each IP family in the input slice.
func firstOfEachIPFamily(ips []string) []string {
	if len(ips) == 0 {
		return ips
	}

	var (
		lastV4 bool
		each   = ips[:0]
	)

	for i := 0; i < len(ips) && len(each) < 2; i++ {
		ip := net.ParseIP(ips[i])
		if ip == nil {
			continue
		}

		isV4 := ip.To4() != nil
		if len(each) == 0 || isV4 != lastV4 {
			each = append(each, ips[i])
			lastV4 = isV4
		}
	}

	return each
}

type attack struct {
	name  string
	began time.Time

	seqmu sync.Mutex
	seq   uint64
}

// Attack reads its Targets from the passed Targeter and attacks them at
// the rate specified by the Pacer. When the duration is zero the attack
// runs until Stop is called. Results are sent to the returned channel as soon
// as they arrive and will have their Attack field set to the given name.
func (a *Attacker) Attack(tr Targeter, p Pacer, du time.Duration, name string) <-chan *Result {
	var wg sync.WaitGroup

	workers := a.workers
	if workers > a.maxWorkers {
		workers = a.maxWorkers
	}

	atk := &attack{
		name:  name,
		began: time.Now(),
	}

	results := make(chan *Result)
	a.traceCh = make(chan *RequestRecord, 1024)
	ticks := make(chan *FireEvent)

	for i := uint64(0); i < workers; i++ {
		wg.Add(1)
		go a.attack(tr, atk, &wg, ticks, results)
	}

	go func() {
		defer func() {
			close(ticks)
			wg.Wait()
			close(a.traceCh)
			close(results)
			a.Stop()
		}()

		count := uint64(0)
		for {
			elapsed := time.Since(atk.began)
			if du > 0 && elapsed > du {
				return
			}

			wait, stop := p.Pace(elapsed, count)
			if stop {
				return
			}

			// target: the time at which the request should be fired
			// beforeSleep := time.Now()
			// target := beforeSleep.Add(wait)
			time.Sleep(wait)
			fire_time := time.Now()

			if workers < a.maxWorkers {
				select {
				// send the firetick struct to the ticks channel to signal a worker to fire a request
				case ticks <- &FireEvent{FireTime: fire_time, PacerWait: wait}:
					count++
					continue
				case <-a.stopch:
					return
				default:
					// all workers are blocked. start one more and try again
					workers++
					wg.Add(1)
					go a.attack(tr, atk, &wg, ticks, results)
				}
			}

			select {
			case ticks <- &FireEvent{FireTime: fire_time, PacerWait: wait}:
				count++
			case <-a.stopch:
				return
			}
		}
	}()

	return results
}

// Stop stops the current attack. The return value indicates whether this call
// has signalled the attack to stop (`true` for the first call) or whether it
// was a noop because it has been previously signalled to stop (`false` for any
// subsequent calls).
func (a *Attacker) Stop() bool {
	select {
	case <-a.stopch:
		return false
	default:
		a.stopOnce.Do(func() { close(a.stopch) })
		return true
	}
}

func (a *Attacker) attack(tr Targeter, atk *attack, workers *sync.WaitGroup, ticks <-chan *FireEvent, results chan<- *Result) {
	defer workers.Done()
	atomic.AddInt64(&a.currentWorkers, 1)
	defer atomic.AddInt64(&a.currentWorkers, -1)

	for fire := range ticks {
		atomic.AddInt64(&a.inFlight, 1)

		// create a new RequestRecord for this request
		rec := &RequestRecord{}
		rec.FireTime = fire.FireTime
		rec.PacerWait = fire.PacerWait
		rec.PacerWaitValid = true
		rec.WakeTime = time.Now() // record the time when the worker wakes up to process the request

		res := a.hit(tr, atk, rec)

		// update inflight, completions, add the result
		atomic.AddInt64(&a.inFlight, -1)
		atomic.AddInt64(&a.completions, 1)
		select {
		case a.traceCh <- rec:
		default:
			// drop trace record if consumer is behind
		}
		results <- res
	}
}

// RuntimeMetrics returns a snapshot of attacker internals.
func (a *Attacker) RuntimeMetrics() RuntimeMetrics {
	workers := atomic.LoadInt64(&a.currentWorkers)
	if workers < 0 {
		workers = 0
	}
	connections := atomic.LoadInt64(&a.activeConns)
	if connections < 0 {
		connections = 0
	}
	inFlight := atomic.LoadInt64(&a.inFlight)
	if inFlight < 0 {
		inFlight = 0
	}
	completions := atomic.LoadInt64(&a.completions)
	if completions < 0 {
		completions = 0
	}

	return RuntimeMetrics{
		Timestamp:   time.Now(),
		Workers:     uint64(workers),
		Connections: uint64(connections),
		InFlight:    uint64(inFlight),
		Completions: uint64(completions),
	}
}

func (a *Attacker) trackDialContext(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if _, ok := conn.(*trackedConn); ok {
			return conn, nil
		}
		// increment active connections
		atomic.AddInt64(&a.activeConns, 1)
		// returns a trackedConn that embeds the original connection and decrements the active connections counter when closed
		return &trackedConn{
			Conn: conn,
			onClose: func() {
				atomic.AddInt64(&a.activeConns, -1)
			},
		}, nil
	}
}

type trackedConn struct {
	net.Conn
	once    sync.Once
	onClose func()
}

func (c *trackedConn) Close() error {
	/* Overrides net.Conn's Close method */
	err := c.Conn.Close()
	c.once.Do(c.onClose)
	return err
}

func checkValidTime(t1 time.Time, t2 time.Time) bool {
	if t1.IsZero() || t2.IsZero() {
		return false
	}
	if t2.Before(t1) {
		return false
	}
	return true
}

func (a *Attacker) hit(tr Targeter, atk *attack, rec *RequestRecord) *Result {
	var (
		res = Result{Attack: atk.name}
		tgt Target
		err error

		traceMu    sync.Mutex
		tDispatch  time.Time
		tGotConn   time.Time
		tWroteReq  time.Time
		tFirstByte time.Time
	)

	//
	// Subtleness ahead! We need to compute the result timestamp in
	// the same critical section that protects the increment of the sequence
	// number because we want the same total ordering of timestamps and sequence
	// numbers. That is, we wouldn't want two results A and B where A.seq > B.seq
	// but A.timestamp < B.timestamp.
	//
	// Additionally, we calculate the result timestamp based on the same beginning
	// timestamp using the Add method, which will use monotonic time calculations.
	//
	atk.seqmu.Lock()
	res.Timestamp = atk.began.Add(time.Since(atk.began))
	res.Seq = atk.seq
	atk.seq++
	atk.seqmu.Unlock()

	rec.ID = res.Seq

	defer func() {

		tDone := time.Now()

		// update record
		rec.DoneTime = tDone
		if checkValidTime(rec.WakeTime, rec.DispatchStart) {
			// dispatch delay: the time between when the worker wakes up to process the request and when it actually makes the HTTP request (dispatch start)
			rec.DispatchDelay = rec.DispatchStart.Sub(rec.WakeTime)
			rec.DispatchDelayValid = true
		} else {
			rec.DispatchDelay = 0
			rec.DispatchDelayValid = false
		}

		if checkValidTime(rec.DispatchStart, rec.GotConnTime) {
			// connection delay: the time between when the HTTP request is dispatched and when a connection is obtained
			rec.ConnDelay = rec.GotConnTime.Sub(rec.DispatchStart)
			rec.ConnDelayValid = true
		} else {
			rec.ConnDelay = 0
			rec.ConnDelayValid = false
		}

		if checkValidTime(rec.GotConnTime, rec.WroteReqTime) {
			// write delay: how long it took for the client to write the request after obtaining a connection
			rec.WriteDelay = rec.WroteReqTime.Sub(rec.GotConnTime)
			rec.WriteDelayValid = true
		} else {
			rec.WriteDelay = 0
			rec.WriteDelayValid = false
		}

		if checkValidTime(rec.WroteReqTime, rec.FirstByteTime) {
			// first byte RTT: the time between when the request was written and when the first byte of the response was received
			rec.FirstByteRTT = rec.FirstByteTime.Sub(rec.WroteReqTime)
			rec.FirstByteRTTValid = true
		} else {
			rec.FirstByteRTT = 0
			rec.FirstByteRTTValid = false
		}

		if checkValidTime(rec.DispatchStart, rec.FirstByteTime) {
			// first byte delay: the time between when the request was dispatched and when the first response byte was received
			rec.FirstByteDelay = rec.FirstByteTime.Sub(rec.DispatchStart)
			rec.FirstByteDelayValid = true
		} else {
			rec.FirstByteDelay = 0
			rec.FirstByteDelayValid = false
		}

		if checkValidTime(rec.FirstByteTime, rec.DoneTime) {
			// response tail time: the time between the first response byte and request completion
			rec.ResponseTailTime = rec.DoneTime.Sub(rec.FirstByteTime)
			rec.ResponseTailValid = true
		} else {
			rec.ResponseTailTime = 0
			rec.ResponseTailValid = false
		}

		if checkValidTime(rec.WakeTime, rec.DoneTime) {
			// total latency: the time between when the worker wakes up to process the request and when the request is fully completed (response received)
			rec.TotalLatency = rec.DoneTime.Sub(rec.WakeTime)
			rec.TotalLatencyValid = true
		} else {
			rec.TotalLatency = 0
			rec.TotalLatencyValid = false
		}

		if checkValidTime(rec.FireTime, rec.WakeTime) {
			// scheduler delay: the time between when the request was fired and when the worker woke up to process it
			rec.SchedulerDelay = rec.WakeTime.Sub(rec.FireTime)
			rec.SchedulerDelayValid = true
		} else {
			rec.SchedulerDelay = 0
			rec.SchedulerDelayValid = false
		}

		res.Latency = time.Since(res.Timestamp)
		if err != nil {
			res.Error = err.Error()
		}
	}()

	if err = tr(&tgt); err != nil {
		a.Stop()
		return &res
	}

	res.Method = tgt.Method
	res.URL = tgt.URL

	req, err := tgt.Request()
	if err != nil {
		return &res
	}

	if atk.name != "" {
		req.Header.Set("X-Vegeta-Attack", atk.name)
	}

	req.Header.Set("X-Vegeta-Seq", strconv.FormatUint(res.Seq, 10))

	if a.chunked {
		req.TransferEncoding = append(req.TransferEncoding, "chunked")
	}

	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			traceMu.Lock()
			tGotConn = time.Now()
			rec.GotConnTime = tGotConn
			rec.GotConnValid = true
			rec.ConnReused = info.Reused
			rec.ConnWasIdle = info.WasIdle
			if info.WasIdle {
				rec.ConnIdleTime = info.IdleTime
				rec.ConnIdleTimeValid = true
			} else {
				rec.ConnIdleTime = 0
				rec.ConnIdleTimeValid = false
			}
			traceMu.Unlock()
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				traceMu.Lock()
				tWroteReq = time.Now()
				rec.WroteReqTime = tWroteReq
				traceMu.Unlock()
			}
		},
		GotFirstResponseByte: func() {
			traceMu.Lock()
			tFirstByte = time.Now()
			rec.FirstByteTime = tFirstByte
			traceMu.Unlock()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	traceMu.Lock()
	tDispatch = time.Now()
	rec.DispatchStart = tDispatch
	traceMu.Unlock()
	r, err := a.client.Do(req)
	if err != nil {
		return &res
	}
	defer r.Body.Close()

	body := io.Reader(r.Body)
	if a.maxBody >= 0 {
		body = io.LimitReader(r.Body, a.maxBody)
	}

	if res.Body, err = io.ReadAll(body); err != nil {
		return &res
	} else if _, err = io.Copy(io.Discard, r.Body); err != nil {
		return &res
	}

	res.BytesIn = uint64(len(res.Body))

	if req.ContentLength != -1 {
		res.BytesOut = uint64(req.ContentLength)
	}

	if res.Code = uint16(r.StatusCode); res.Code < 200 || res.Code >= 400 {
		res.Error = r.Status
	}

	res.Headers = r.Header

	return &res
}
