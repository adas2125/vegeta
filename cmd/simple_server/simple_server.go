// package main

// import (
// 	"flag"
// 	"net/http"
// 	"time"
// )

// // Example:
// // go run simple_server.go -addr :8080 -base-delay 20ms -peak-delay 500ms -ramp-up 20s -ramp-down 20s

// func main() {
// 	addr := flag.String("addr", ":8080", "listen address")

// 	baseDelay := flag.Duration("base-delay", 20*time.Millisecond, "starting/ending delay")
// 	peakDelay := flag.Duration("peak-delay", 500*time.Millisecond, "maximum delay at peak")
// 	rampUp := flag.Duration("ramp-up", 20*time.Second, "time to ramp from base to peak")
// 	rampDown := flag.Duration("ramp-down", 20*time.Second, "time to ramp from peak back to base")

// 	flag.Parse()

// 	start := time.Now()

// 	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
// 		elapsed := time.Since(start)
// 		delay := currentDelay(elapsed, *baseDelay, *peakDelay, *rampUp, *rampDown)

// 		if delay > 0 {
// 			time.Sleep(delay)
// 		}

// 		w.WriteHeader(http.StatusOK)
// 		_, _ = w.Write([]byte("ok"))
// 	})

// 	_ = http.ListenAndServe(*addr, nil)
// }

// func currentDelay(elapsed, base, peak, rampUp, rampDown time.Duration) time.Duration {
// 	// Ramp up: base -> peak
// 	if elapsed <= rampUp {
// 		return lerpDuration(base, peak, float64(elapsed)/float64(rampUp))
// 	}

// 	// Ramp down: peak -> base
// 	downElapsed := elapsed - rampUp
// 	if downElapsed <= rampDown {
// 		return lerpDuration(peak, base, float64(downElapsed)/float64(rampDown))
// 	}

// 	// After full cycle, stay at base
// 	return base
// }

// func lerpDuration(a, b time.Duration, t float64) time.Duration {
// 	if t < 0 {
// 		t = 0
// 	}
// 	if t > 1 {
// 		t = 1
// 	}
// 	return a + time.Duration(float64(b-a)*t)
// }

package main

import (
	"flag"
	"net/http"
	"time"
)

// Usage: go run simple_server.go -addr localhost:8080 -delay 200ms

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	delay := flag.Duration("delay", 0, "response delay (e.g. 50ms, 200ms)")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *delay > 0 {
			time.Sleep(*delay)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	_ = http.ListenAndServe(*addr, nil)
}
