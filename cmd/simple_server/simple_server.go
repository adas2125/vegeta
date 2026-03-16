package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")

	// Delay mode: fixed, uniform, exp
	delayMode := flag.String("delay-mode", "fixed", "delay mode: fixed, uniform, exp")

	// Fixed delay
	delay := flag.Duration("delay", 0, "fixed response delay (e.g. 50ms, 200ms)")

	// Uniform delay parameters
	minDelay := flag.Duration("min-delay", 0, "minimum delay for uniform mode")
	maxDelay := flag.Duration("max-delay", 0, "maximum delay for uniform mode")

	// Exponential delay parameter
	meanDelay := flag.Duration("mean-delay", 0, "mean delay for exp mode")

	flag.Parse()

	// file, err := os.Create("output.csv")
	// if err != nil {
	// 	panic(err)
	// }
	// defer file.Close()

	// writer := csv.NewWriter(file)
	// defer writer.Flush()

	var totalCount uint64
	// var mu sync.Mutex

	// RNG seeded once
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// _ = writer.Write([]string{"timestamp", "total_requests"})
	// writer.Flush()

	// go func() {
	// 	for {
	// 		time.Sleep(1 * time.Second)
	// 		current := atomic.LoadUint64(&totalCount)

	// 		mu.Lock()
	// 		_ = writer.Write([]string{
	// 			time.Now().Format(time.RFC3339),
	// 			fmt.Sprintf("%d", current),
	// 		})
	// 		writer.Flush()
	// 		mu.Unlock()
	// 	}
	// }()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		d := sampleDelay(rng, *delayMode, *delay, *minDelay, *maxDelay, *meanDelay)
		if d > 0 {
			time.Sleep(d)
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		atomic.AddUint64(&totalCount, 1)
	})

	fmt.Println("server listening on", *addr)
	fmt.Println("delay mode:", *delayMode)
	_ = http.ListenAndServe(*addr, nil)
}

func sampleDelay(
	rng *rand.Rand,
	mode string,
	fixed time.Duration,
	minDelay time.Duration,
	maxDelay time.Duration,
	meanDelay time.Duration,
) time.Duration {
	switch mode {
	case "fixed":
		return fixed

	case "uniform":
		if maxDelay < minDelay {
			minDelay, maxDelay = maxDelay, minDelay
		}
		if maxDelay == minDelay {
			return minDelay
		}

		span := maxDelay - minDelay
		// random integer in [0, span]
		return minDelay + time.Duration(rng.Int63n(int64(span)+1))

	case "exp":
		if meanDelay <= 0 {
			return 0
		}
		// ExpFloat64 returns exponential with mean 1.0
		return time.Duration(rng.ExpFloat64() * float64(meanDelay))

	default:
		fmt.Printf("unknown delay mode %q, defaulting to fixed\n", mode)
		return fixed
	}
}
