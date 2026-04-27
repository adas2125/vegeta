package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type phase struct {
	Name        string        `json:"name"`
	Delay       time.Duration `json:"delay"`
	Description string        `json:"description"`
}

var phases = []phase{
	{
		Name:        "baseline",
		Delay:       10 * time.Millisecond,
		Description: "Respond instantly enough to represent the healthy baseline.",
	},
	{
		Name:        "degraded_100ms",
		Delay:       25 * time.Millisecond,
		Description: "Moderate slowdown to simulate a degraded service.",
	},
	{
		Name:        "cache_warm_5ms",
		Delay:       5 * time.Millisecond,
		Description: "Speed-up phase that mimics a warm cache.",
	},
	{
		Name:        "worker_cap_500ms",
		Delay:       100 * time.Millisecond,
		Description: "Severe slowdown intended to push Vegeta toward worker-cap pressure.",
	},
}

type controller struct {
	mu        sync.RWMutex
	index     int
	startedAt time.Time
}

func newController() *controller {
	return &controller{
		startedAt: time.Now(),
	}
}

func (c *controller) currentPhase() phase {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return phases[c.index]
}

func (c *controller) currentIndex() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.index
}

func (c *controller) setPhase(index int) phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = normalizePhaseIndex(index)
	return phases[c.index]
}

func (c *controller) nextPhase() phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = normalizePhaseIndex(c.index + 1)
	return phases[c.index]
}

func (c *controller) previousPhase() phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = normalizePhaseIndex(c.index - 1)
	return phases[c.index]
}

func normalizePhaseIndex(index int) int {
	if len(phases) == 0 {
		return 0
	}
	for index < 0 {
		index += len(phases)
	}
	return index % len(phases)
}

type response struct {
	Now          time.Time     `json:"now"`
	Phase        string        `json:"phase"`
	PhaseIndex   int           `json:"phase_index"`
	AppliedDelay time.Duration `json:"applied_delay"`
	TotalServed  uint64        `json:"total_served"`
	CurrentInFly int64         `json:"current_in_flight"`
	Description  string        `json:"description"`
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	ctrl := newController()
	var totalServed uint64
	var totalStarted uint64
	var inFlight int64

	go readConsole(ctrl)
	go logStats(ctrl, &totalStarted, &totalServed, &inFlight)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := ctrl.currentPhase()

		atomic.AddUint64(&totalStarted, 1)
		currentInFlight := atomic.AddInt64(&inFlight, 1)
		defer atomic.AddInt64(&inFlight, -1)

		if p.Delay > 0 {
			time.Sleep(p.Delay)
		}

		served := atomic.AddUint64(&totalServed, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response{
			Now:          time.Now(),
			Phase:        p.Name,
			PhaseIndex:   ctrl.currentIndex(),
			AppliedDelay: p.Delay,
			TotalServed:  served,
			CurrentInFly: currentInFlight,
			Description:  p.Description,
		})
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		p := ctrl.currentPhase()
		_, _ = fmt.Fprintf(w, "ok phase=%s delay=%s\n", p.Name, p.Delay)
	})

	mux.HandleFunc("/phase", func(w http.ResponseWriter, r *http.Request) {
		p := ctrl.currentPhase()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Phase      phase `json:"phase"`
			PhaseIndex int   `json:"phase_index"`
		}{
			Phase:      p,
			PhaseIndex: ctrl.currentIndex(),
		})
	})

	log.Printf("regime shift server listening on %s", *addr)
	log.Printf("phases: 0=%s 1=%s 2=%s 3=%s", phases[0].Delay, phases[1].Delay, phases[2].Delay, phases[3].Delay)
	log.Printf("controls: 'n' next, 'p' previous, '0'..'3' jump to phase, 'q' stop reading stdin")
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func logStats(ctrl *controller, totalStarted, totalServed *uint64, inFlight *int64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastStarted uint64
	var lastServed uint64
	for range ticker.C {
		p := ctrl.currentPhase()
		started := atomic.LoadUint64(totalStarted)
		served := atomic.LoadUint64(totalServed)
		currentInFlight := atomic.LoadInt64(inFlight)
		log.Printf(
			"phase=%s delay=%s started=%d served=%d arrivals_1s=%d completions_1s=%d inflight=%d uptime=%s",
			p.Name,
			p.Delay,
			started,
			served,
			started-lastStarted,
			served-lastServed,
			currentInFlight,
			time.Since(ctrl.startedAt).Truncate(time.Second),
		)
		lastStarted = started
		lastServed = served
	}
}

func readConsole(ctrl *controller) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := strings.ToLower(strings.TrimSpace(scanner.Text()))
		switch command {
		case "n":
			p := ctrl.nextPhase()
			log.Printf("manual advance to phase=%s delay=%s", p.Name, p.Delay)
		case "p":
			p := ctrl.previousPhase()
			log.Printf("manual rewind to phase=%s delay=%s", p.Name, p.Delay)
		case "0", "1", "2", "3":
			index := int(command[0] - '0')
			p := ctrl.setPhase(index)
			log.Printf("manual jump to phase=%s delay=%s", p.Name, p.Delay)
		case "q":
			log.Printf("console loop exiting")
			return
		case "":
		default:
			log.Printf("unknown command %q; use 'n', 'p', '0'..'3', or 'q'", command)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("stdin scanner stopped: %v", err)
	}
}
