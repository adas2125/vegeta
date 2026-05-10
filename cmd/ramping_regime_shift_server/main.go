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

type phaseSpec struct {
	Name         string
	BaseDelay    time.Duration
	Description  string
	Step         time.Duration
	StepInterval time.Duration
	MinDelay     time.Duration
	MaxDelay     time.Duration
}

var phaseSpecs = []phaseSpec{
	{
		Name:        "baseline",
		BaseDelay:   10 * time.Millisecond,
		Description: "Respond instantly enough to represent the healthy baseline.",
	},
	{
		Name:         "degraded_100ms",
		BaseDelay:    10 * time.Millisecond,
		Description:  "Moderate slowdown ramp: starts at 10ms and adds 1ms every second, capped at 100ms.",
		Step:         time.Millisecond,
		StepInterval: time.Second,
		MinDelay:     10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
	},
	{
		Name:         "cache_warm_5ms",
		BaseDelay:    10 * time.Millisecond,
		Description:  "Speed-up ramp that mimics a warm cache: starts at 10ms and subtracts 1ms every second, capped at 5ms.",
		Step:         -time.Millisecond,
		StepInterval: time.Second,
		MinDelay:     5 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
	},
	{
		Name:        "worker_cap_100ms",
		BaseDelay:   100 * time.Millisecond,
		Description: "Severe slowdown intended to push Vegeta toward worker-cap pressure.",
	},
}

type controller struct {
	mu             sync.RWMutex
	index          int
	startedAt      time.Time
	phaseStartedAt time.Time
}

func newController() *controller {
	now := time.Now()
	return &controller{
		startedAt:      now,
		phaseStartedAt: now,
	}
}

func (c *controller) currentPhase() phase {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return currentPhaseLocked(c.index, time.Since(c.phaseStartedAt))
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
	c.phaseStartedAt = time.Now()
	return currentPhaseLocked(c.index, 0)
}

func (c *controller) nextPhase() phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = normalizePhaseIndex(c.index + 1)
	c.phaseStartedAt = time.Now()
	return currentPhaseLocked(c.index, 0)
}

func (c *controller) previousPhase() phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index = normalizePhaseIndex(c.index - 1)
	c.phaseStartedAt = time.Now()
	return currentPhaseLocked(c.index, 0)
}

func currentPhaseLocked(index int, elapsed time.Duration) phase {
	spec := phaseSpecs[index]
	return phase{
		Name:        spec.Name,
		Delay:       spec.delay(elapsed),
		Description: spec.Description,
	}
}

func (p phaseSpec) delay(elapsed time.Duration) time.Duration {
	delay := p.BaseDelay
	if p.Step != 0 && p.StepInterval > 0 && elapsed > 0 {
		delay += time.Duration(int64(elapsed/p.StepInterval)) * p.Step
	}
	if p.MinDelay > 0 && delay < p.MinDelay {
		return p.MinDelay
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func normalizePhaseIndex(index int) int {
	if len(phaseSpecs) == 0 {
		return 0
	}
	for index < 0 {
		index += len(phaseSpecs)
	}
	return index % len(phaseSpecs)
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

	log.Printf("ramping regime shift server listening on %s", *addr)
	log.Printf("phases: 0=10ms fixed 1=10ms+1ms/s cap 100ms 2=10ms-1ms/s floor 5ms 3=100ms fixed")
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
