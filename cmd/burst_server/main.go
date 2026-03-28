package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type response struct {
	Now          time.Time     `json:"now"` 			// when the request was processed (after the delay)
	Phase        string        `json:"phase"` 			// current phase of the server (fast or slow)
	AppliedDelay time.Duration `json:"applied_delay"` 	// delay applied to the request
	TotalCount   uint64        `json:"total_count"` 	// total number of requests served
}

func main() {
	// command-line flags to configure the server behavior
	addr := flag.String("addr", ":8080", "listen address")
	fastDelay := flag.Duration("fast-delay", 10*time.Millisecond, "delay outside the spike window")
	slowDelay := flag.Duration("slow-delay", 400*time.Millisecond, "delay inside the spike window")
	cycle := flag.Duration("cycle", 4*time.Second, "full duration of the repeating latency pattern")
	spike := flag.Duration("spike", 1500*time.Millisecond, "time spent in the slow phase within each cycle")
	flag.Parse()

	if *cycle <= 0 {
		log.Fatal("cycle must be > 0")
	}
	if *spike < 0 || *spike > *cycle {
		log.Fatal("spike must be between 0 and cycle")
	}

	start := time.Now()
	var total uint64	// total number of requests served, updated atomically

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		elapsed := now.Sub(start)
		// position within the current cycle determines the delay applied to this request
		offset := elapsed % *cycle

		// start with fast delay
		delay := *fastDelay
		phase := "fast"

		// if we're within the spike window, switch to slow delay
		if offset < *spike {
			delay = *slowDelay
			phase = "slow"
		}

		// increment the total count of arrivals
		count := atomic.AddUint64(&total, 1)

		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response{
			Now:          now,
			Phase:        phase,
			AppliedDelay: delay,
			TotalCount:   count,
		})
	})

	// background goroutine to log the total count and recent throughput every second
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var last uint64
		for range ticker.C {
			// logging the total number of requests served and the number served in the last second
			totalNow := atomic.LoadUint64(&total)
			log.Printf("served=%d last_1s=%d\n", totalNow, totalNow-last)
			last = totalNow
		}
	}()

	log.Printf("listening on %s fast=%s slow=%s cycle=%s spike=%s\n", *addr, *fastDelay, *slowDelay, *cycle, *spike)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
