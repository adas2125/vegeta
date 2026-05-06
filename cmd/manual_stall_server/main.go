package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type stallGate struct {
	mu sync.RWMutex

	// indicates if server is currently stalled
	stalled bool

	waitCh chan struct{}

	// how long to stall when Start is called
	duration time.Duration
}

func newStallGate(duration time.Duration) *stallGate {
	// constructor initializing a new stallGate (above struct)
	return &stallGate{
		waitCh:   make(chan struct{}),
		duration: duration,
	}
}

func (g *stallGate) Start() bool {
	// acquires lock to check if stalled
	g.mu.Lock()
	if g.stalled {
		// if already stalled, release lock and return false
		g.mu.Unlock()
		return false
	}

	// set stalled to true and create a new waitCh for this stall period
	g.stalled = true
	g.waitCh = make(chan struct{})
	waitCh := g.waitCh
	duration := g.duration
	g.mu.Unlock()

	// starts a goroutine that will sleep for the specified duration and then unstalls the server
	go func() {
		time.Sleep(duration)
		g.mu.Lock()
		if g.stalled && g.waitCh == waitCh {
			g.stalled = false

			// close the waitCh to unblock any waiting requests
			close(g.waitCh)
		}
		g.mu.Unlock()
	}()
	return true
}

func (g *stallGate) Snapshot() (bool, <-chan struct{}) {
	// acquires read lock to safely read the stalled state and waitCh
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stalled, g.waitCh
}

func (g *stallGate) Mode() string {
	// returns the state as a string for health check responses
	stalled, _ := g.Snapshot()
	if stalled {
		return "stalled"
	}
	return "healthy"
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	delay := flag.Duration("delay", 10*time.Millisecond, "fixed healthy response delay")
	stallDuration := flag.Duration("stall-duration", 5*time.Second, "timed stall duration")
	flag.Parse()

	// creates a new stallGate with the specified stall duration
	gate := newStallGate(*stallDuration)
	var started uint64
	var completed uint64
	var inFlight int64

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&started, 1)
		atomic.AddInt64(&inFlight, 1)
		defer atomic.AddInt64(&inFlight, -1)

		// checks if the server is currently stalled and waits if it is
		if stalled, waitCh := gate.Snapshot(); stalled {
			<-waitCh
		}

		// sleeps for the specified delay to simulate processing time for healthy responses
		if *delay > 0 {
			time.Sleep(*delay)
		}

		// request processing is complete, increment completed counter and respond with 200 OK
		atomic.AddUint64(&completed, 1)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// responds with the current mode (stalled or healthy) for health checks
		fmt.Fprintf(w, "ok mode=%s\n", gate.Mode())
	})

	mux.HandleFunc("/start-stall", func(w http.ResponseWriter, r *http.Request) {
		// attempts to start a stall
		gate.Start()
		w.WriteHeader(http.StatusOK)
	})

	// starts a goroutine to log stats about arrivals, completions, and in-flight requests every second
	go logStats(gate, &started, &completed, &inFlight)

	log.Fatal(http.ListenAndServe(*addr, mux))
}

func logStats(gate *stallGate, started, completed *uint64, inFlight *int64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastStarted uint64
	var lastCompleted uint64
	for range ticker.C {
		currentStarted := atomic.LoadUint64(started)
		currentCompleted := atomic.LoadUint64(completed)

		// logs the current mode, number of arrivals and completions in the last second, and the current number of in-flight requests
		log.Printf(
			"mode=%s arrivals_1s=%d completions_1s=%d inflight=%d",
			gate.Mode(),
			currentStarted-lastStarted,
			currentCompleted-lastCompleted,
			atomic.LoadInt64(inFlight),
		)
		
		lastStarted = currentStarted
		lastCompleted = currentCompleted
	}
}
